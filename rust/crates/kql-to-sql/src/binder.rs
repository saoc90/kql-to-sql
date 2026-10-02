//! Translation context, name environments and tabular expressions.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use kql_parser::ast::{self, Expr, LetValue, Operator, ParamType, Statement};

use crate::dialect::SqlDialect;
use crate::expr::{Scope, TExpr};
use crate::sql::{quote_ident, From, Item, Query, Select};
use crate::{err, Catalog, Column, KqlType, Result};

/// A relation under construction: a SELECT plus its typed output schema.
#[derive(Debug, Clone)]
pub(crate) struct Rel {
    pub sel: Select,
    pub cols: Vec<Column>,
    /// Logical row order as SQL ORDER BY items over output columns. It is emitted only where it
    /// matters (LIMIT, window functions, the final result), never inside plain subqueries.
    pub order: Vec<OrderSpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OrderSpec {
    pub col: String,
    pub desc: bool,
    pub nulls_first: bool,
}

impl OrderSpec {
    pub fn sql(&self) -> String {
        format!(
            "{} {} NULLS {}",
            quote_ident(&self.col),
            if self.desc { "DESC" } else { "ASC" },
            if self.nulls_first { "FIRST" } else { "LAST" }
        )
    }
}

impl Rel {
    pub fn from_select(sel: Select, cols: Vec<Column>) -> Rel {
        Rel { sel, cols, order: Vec::new() }
    }

    /// Wraps the current select as a subquery: `SELECT * FROM (<sel>) AS _q`.
    /// Wraps the select keeping its ORDER BY (for physically ordered relations).
    pub fn wrap_keep_order(self, ctx: &mut Ctx) -> Rel {
        let alias = ctx.alias();
        Rel { sel: Select::star_from(From::Query(Box::new(Query::Select(Box::new(self.sel))), alias)), cols: self.cols, order: self.order }
    }

    pub fn wrap(mut self, ctx: &mut Ctx) -> Rel {
        // A logical order is re-applied by the consumer; a physical-only order (expression sort
        // keys) must stay in the subquery.
        if self.sel.limit.is_none() && !self.order.is_empty() {
            self.sel.order_by.clear();
        }
        let alias = ctx.alias();
        Rel { sel: Select::star_from(From::Query(Box::new(Query::Select(Box::new(self.sel))), alias)), cols: self.cols, order: self.order }
    }

    /// Ensures the select outputs exactly its FROM's columns, so new clauses can reference them.
    pub fn passthrough(self, ctx: &mut Ctx) -> Rel {
        if self.sel.is_passthrough() {
            self
        } else {
            self.wrap(ctx)
        }
    }

    pub fn order_sql(&self) -> Vec<String> {
        self.order.iter().map(OrderSpec::sql).collect()
    }

    /// Replaces the projection with explicit items (wrapping first if needed).
    pub fn project(self, ctx: &mut Ctx, items: Vec<Item>, cols: Vec<Column>) -> Rel {
        let mut r = self.passthrough(ctx);
        // keep logical order only if its columns survive unchanged
        let kept: Vec<OrderSpec> = r
            .order
            .iter()
            .filter(|o| items.iter().any(|i| i.alias == o.col && i.sql == quote_ident(&o.col)))
            .cloned()
            .collect();
        if kept.len() != r.order.len() {
            r.order.clear();
        } else {
            r.order = kept;
        }
        r.sel.items = Some(items);
        r.cols = cols;
        r
    }

    /// Items that re-select every column unchanged.
    pub fn identity_items(&self) -> Vec<Item> {
        self.cols.iter().map(|c| Item { sql: quote_ident(&c.name), alias: c.name.clone() }).collect()
    }

    pub fn col(&self, name: &str) -> Option<&Column> {
        self.cols.iter().find(|c| c.name == name)
    }

    /// Renders as a query, applying the logical order.
    pub fn into_query(mut self) -> Query {
        if self.sel.order_by.is_empty() && !self.order.is_empty() {
            self.sel.order_by = self.order_sql();
        }
        Query::Select(Box::new(self.sel))
    }
}

/// A tabular `let` binding or table: a FROM item and its schema.
#[derive(Debug, Clone)]
pub(crate) struct TableRef {
    pub from: String,
    pub cols: Vec<Column>,
    pub order: Vec<OrderSpec>,
}

#[derive(Clone)]
pub(crate) enum Binding {
    Scalar(TExpr),
    Tabular(TableRef),
    Function(Rc<UserFunction>),
}

pub(crate) struct UserFunction {
    pub def: ast::Function,
    pub env: Env,
}

/// Lexical environment of `let` names.
#[derive(Clone, Default)]
pub(crate) struct Env {
    vars: HashMap<String, Binding>,
}

impl Env {
    pub fn get(&self, name: &str) -> Option<&Binding> {
        self.vars.get(name)
    }

    pub fn with(&self, name: &str, b: Binding) -> Env {
        let mut e = self.clone();
        e.vars.insert(name.to_string(), b);
        e
    }

    pub fn set(&mut self, name: &str, b: Binding) {
        self.vars.insert(name.to_string(), b);
    }
}

/// Per-translation state. A new context is created for every `translate` call, so translation
/// is free of shared mutable state.
pub(crate) struct Ctx<'a> {
    pub d: &'static dyn SqlDialect,
    pub catalog: &'a Catalog,
    pub ctes: Vec<(String, String, bool)>,
    /// Names bound by the `as` operator (visible to the rest of the query).
    pub as_names: Vec<(String, TableRef)>,
    /// Chart instructions from `| render`.
    pub render: Option<crate::RenderInfo>,
    cte_names: HashSet<String>,
    /// Hidden window columns collected while compiling an operator's row expressions
    /// (`None` outside such operators); see `window::begin`.
    pub window_hoists: Option<Vec<crate::window::Hoist>>,
    next_alias: usize,
    depth: usize,
}

const MAX_DEPTH: usize = 64;

impl<'a> Ctx<'a> {
    pub fn new(d: &'static dyn SqlDialect, catalog: &'a Catalog) -> Ctx<'a> {
        Ctx { d, catalog, ctes: Vec::new(), as_names: Vec::new(), render: None, cte_names: HashSet::new(), window_hoists: None, next_alias: 0, depth: 0 }
    }

    pub fn alias(&mut self) -> String {
        self.next_alias += 1;
        format!("_q{}", self.next_alias)
    }

    /// Registers a CTE and returns its (quoted) name.
    pub fn add_cte(&mut self, base: &str, rel: Rel, materialized: bool) -> TableRef {
        let mut name = base.to_string();
        let mut n = 1;
        while self.cte_names.contains(&name.to_ascii_lowercase()) || self.catalog.table(&name).is_some() {
            name = format!("{base}_{n}");
            n += 1;
        }
        self.cte_names.insert(name.to_ascii_lowercase());
        let order = rel.order.clone();
        let cols = rel.cols.clone();
        let mut sel = rel.sel;
        if sel.limit.is_none() {
            sel.order_by.clear();
        }
        self.ctes.push((quote_ident(&name), sel.render(), materialized));
        TableRef { from: quote_ident(&name), cols, order }
    }

    // ------------------------------------------------------------------ statements

    /// Processes statements; returns the last expression statement, if any.
    pub fn statements<'s>(&mut self, stmts: &'s [Statement], env: &mut Env) -> Result<Option<&'s Expr>> {
        let mut result = None;
        for s in stmts {
            match s {
                Statement::Let { name, value } => self.bind_let(name, value, env)?,
                Statement::Set { .. } => {}
                Statement::Expr(e) => {
                    if result.is_some() {
                        return err("multiple result expressions are not supported");
                    }
                    result = Some(e);
                }
            }
        }
        Ok(result)
    }

    fn bind_let(&mut self, name: &str, value: &LetValue, env: &mut Env) -> Result<()> {
        let b = match value {
            LetValue::Function(f) => {
                if f.params.is_empty() && f.is_view {
                    // a view behaves like a tabular let
                    Binding::Function(Rc::new(UserFunction { def: f.clone(), env: env.clone() }))
                } else {
                    Binding::Function(Rc::new(UserFunction { def: f.clone(), env: env.clone() }))
                }
            }
            LetValue::Expr(e) => {
                if self.is_tabular(e, env) {
                    let (inner, materialized) = match e {
                        Expr::Call { name: f, args } if f.eq_ignore_ascii_case("materialize") && args.len() == 1 => (&args[0].expr, true),
                        _ => (e, false),
                    };
                    let rel = self.tabular(inner, env)?;
                    Binding::Tabular(self.add_cte(name, rel, materialized))
                } else {
                    let t = self.expr(e, &Scope::empty(), env)?;
                    Binding::Scalar(t)
                }
            }
        };
        env.set(name, b);
        Ok(())
    }

    // ------------------------------------------------------------------ tabular expressions

    /// Syntactic test for tabular expressions.
    pub fn is_tabular(&self, e: &Expr, env: &Env) -> bool {
        match e {
            Expr::Pipe { .. } | Expr::Source(_) => true,
            Expr::Paren(x) => self.is_tabular(x, env),
            Expr::Name(n) => match env.get(n) {
                Some(Binding::Tabular(_)) => true,
                Some(Binding::Function(f)) => f.def.is_view || (f.def.params.is_empty() && self.body_is_tabular(f)),
                Some(Binding::Scalar(_)) => false,
                None => self.catalog.table(n).is_some(),
            },
            Expr::Call { name, args } => match env.get(name) {
                Some(Binding::Function(f)) => self.body_is_tabular(f),
                _ => {
                    let l = name.to_ascii_lowercase();
                    matches!(l.as_str(), "materialize" | "table" | "view")
                        || (l == "materialize" && args.len() == 1)
                }
            },
            _ => false,
        }
    }

    fn body_is_tabular(&self, f: &UserFunction) -> bool {
        let mut env = f.env.clone();
        for p in &f.def.params {
            if let ParamType::Tabular { .. } = p.ty {
                env.set(&p.name, Binding::Tabular(TableRef { from: String::new(), cols: Vec::new(), order: Vec::new() }));
            } else {
                env.set(&p.name, Binding::Scalar(TExpr::new("NULL", KqlType::Dynamic)));
            }
        }
        for s in &f.def.body {
            match s {
                Statement::Let { name, value } => {
                    let tab = match value {
                        LetValue::Expr(e) => self.is_tabular(e, &env),
                        LetValue::Function(_) => false,
                    };
                    let b = if tab {
                        Binding::Tabular(TableRef { from: String::new(), cols: Vec::new(), order: Vec::new() })
                    } else {
                        Binding::Scalar(TExpr::new("NULL", KqlType::Dynamic))
                    };
                    env.set(name, b);
                }
                Statement::Expr(e) => return self.is_tabular(e, &env),
                Statement::Set { .. } => {}
            }
        }
        false
    }

    pub fn tabular(&mut self, e: &Expr, env: &Env) -> Result<Rel> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            self.depth -= 1;
            return err("query is nested too deeply");
        }
        let r = self.tabular_inner(e, env);
        self.depth -= 1;
        r
    }

    fn tabular_inner(&mut self, e: &Expr, env: &Env) -> Result<Rel> {
        match e {
            Expr::Paren(x) => self.tabular(x, env),
            Expr::Name(n) => self.table_ref(n, env),
            Expr::Pipe { input, op } => {
                let rel = self.tabular(input, env)?;
                if let Operator::Evaluate { params, name, args } = &**op {
                    // plugins whose output schema depends on constant input data inspect it
                    return crate::op_evaluate::evaluate_with_input(self, rel, input, params, name, args, env);
                }
                self.apply_operator(rel, op, env)
            }
            Expr::Source(op) => self.source_operator(op, env),
            Expr::Call { name, args } => {
                if let Some(Binding::Function(f)) = env.get(name).cloned() {
                    return match self.call_user_function(&f, args, &Scope::empty(), env)? {
                        FnResult::Tabular(r) => Ok(r),
                        FnResult::Scalar(_) => err(format!("function '{name}' does not return a table")),
                    };
                }
                match name.to_ascii_lowercase().as_str() {
                    "materialize" if args.len() == 1 => {
                        let rel = self.tabular(&args[0].expr, env)?;
                        let t = self.add_cte("materialized", rel, true);
                        Ok(self.rel_from_tableref(&t))
                    }
                    "table" | "view" if args.len() == 1 => {
                        let n = match &args[0].expr {
                            Expr::Literal(ast::Literal::String(s)) => s.clone(),
                            _ => return err("table() requires a constant table name"),
                        };
                        self.table_ref(&n, env)
                    }
                    _ => err(format!("'{name}(...)' is not a tabular expression")),
                }
            }
            _ => err("expected a tabular expression"),
        }
    }

    pub fn rel_from_tableref(&mut self, t: &TableRef) -> Rel {
        let mut r = Rel::from_select(Select::star_from(From::Table(t.from.clone())), t.cols.clone());
        r.order = t.order.clone();
        r
    }

    pub fn table_ref(&mut self, name: &str, env: &Env) -> Result<Rel> {
        match env.get(name).cloned() {
            Some(Binding::Tabular(t)) => return Ok(self.rel_from_tableref(&t)),
            Some(Binding::Function(f)) if f.def.params.is_empty() => {
                return match self.call_user_function(&f, &[], &Scope::empty(), env)? {
                    FnResult::Tabular(r) => Ok(r),
                    FnResult::Scalar(_) => err(format!("'{name}' is not tabular")),
                };
            }
            Some(_) => return err(format!("'{name}' is not a table")),
            None => {}
        }
        if let Some((_, t)) = self.as_names.iter().rev().find(|(n, _)| n == name) {
            let t = t.clone();
            return Ok(self.rel_from_tableref(&t));
        }
        let Some((tname, cols)) = self.catalog.table(name) else {
            return err(format!("unknown table '{name}'"));
        };
        let cols = cols.clone();
        let quoted = quote_ident(tname);
        let mut rel = Rel::from_select(Select::star_from(From::Table(quoted)), cols.clone());
        // Stored INTERVAL columns become ticks; stored types otherwise match the representation.
        if cols.iter().any(|c| c.ty == KqlType::TimeSpan) {
            let items = cols
                .iter()
                .map(|c| {
                    let q = quote_ident(&c.name);
                    let sql = if c.ty == KqlType::TimeSpan { self.d.ticks_from_interval(&q) } else { q };
                    Item { sql, alias: c.name.clone() }
                })
                .collect();
            rel.sel.items = Some(items);
        }
        Ok(rel)
    }

    // ------------------------------------------------------------------ user functions

    pub fn call_user_function(&mut self, f: &UserFunction, args: &[ast::Arg], scope: &Scope, env: &Env) -> Result<FnResult> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            self.depth -= 1;
            return err("function calls are nested too deeply (recursion is not supported)");
        }
        let r = self.call_user_function_inner(f, args, scope, env);
        self.depth -= 1;
        r
    }

    fn call_user_function_inner(&mut self, f: &UserFunction, args: &[ast::Arg], scope: &Scope, env: &Env) -> Result<FnResult> {
        let mut fenv = f.env.clone();
        if args.len() > f.def.params.len() {
            return err(format!("too many arguments: expected at most {}", f.def.params.len()));
        }
        for (i, p) in f.def.params.iter().enumerate() {
            let arg = args.iter().find(|a| a.name.as_deref() == Some(p.name.as_str())).or_else(|| args.get(i).filter(|a| a.name.is_none()));
            match (&p.ty, arg) {
                (ParamType::Tabular { .. }, Some(a)) => {
                    let rel = self.tabular(&a.expr, env)?;
                    let t = self.add_cte(&p.name, rel, false);
                    fenv.set(&p.name, Binding::Tabular(t));
                }
                (ParamType::Scalar(ty), Some(a)) => {
                    let v = self.expr(&a.expr, scope, env)?;
                    let v = match KqlType::from_name(ty) {
                        Some(t) if t != v.ty && !(t.is_numeric() && v.ty.is_numeric() && t == KqlType::Long) => self.convert(v, t),
                        _ => v,
                    };
                    fenv.set(&p.name, Binding::Scalar(v));
                }
                (_, None) => match &p.default {
                    Some(d) => {
                        let v = self.expr(d, &Scope::empty(), &f.env)?;
                        fenv.set(&p.name, Binding::Scalar(v));
                    }
                    None => return err(format!("missing argument '{}'", p.name)),
                },
            }
        }
        let Some(result) = self.statements(&f.def.body, &mut fenv)? else {
            return err("function body has no result expression");
        };
        if self.is_tabular(result, &fenv) {
            Ok(FnResult::Tabular(self.tabular(result, &fenv)?))
        } else {
            Ok(FnResult::Scalar(self.expr(result, scope, &fenv)?))
        }
    }

    pub fn source_operator(&mut self, op: &Operator, env: &Env) -> Result<Rel> {
        crate::ops::source(self, op, env)
    }

    pub fn apply_operator(&mut self, rel: Rel, op: &Operator, env: &Env) -> Result<Rel> {
        crate::ops::apply(self, rel, op, env)
    }
}

pub(crate) enum FnResult {
    Scalar(TExpr),
    Tabular(Rel),
}
