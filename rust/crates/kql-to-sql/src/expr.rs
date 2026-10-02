//! Typed expression compilation.
//!
//! Every compiled expression is a [`TExpr`]: SQL text plus its Kusto type. The SQL text is always
//! atomic (safe to embed anywhere without extra parentheses).

use kql_parser::ast::{self, BinaryOp, Expr, InKind, Literal, StringOp, UnaryOp};

use crate::binder::{Binding, Ctx, Env};
use crate::datetime;
use crate::sql::{quote_ident, quote_str};
use crate::types::widest;
use crate::{err, Column, KqlType, Result};

/// A compile-time constant value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Const {
    Null,
    Bool(bool),
    Long(i64),
    Real(f64),
    Str(String),
    /// Microseconds since the epoch.
    DateTime(i64),
    /// Ticks.
    TimeSpan(i64),
    Dynamic(serde_json::Value),
}

#[derive(Debug, Clone)]
pub(crate) struct TExpr {
    pub sql: String,
    pub ty: KqlType,
    pub konst: Option<Const>,
    /// Contains an aggregate function call.
    pub agg: bool,
    /// Contains a window function (row_number, prev, ...).
    pub window: bool,
    /// A comparison: Kusto defines it as false (never null) when an operand is null, while SQL
    /// yields NULL. Consumers that can observe the difference (NOT, projections) coalesce.
    pub cmp: bool,
}

impl TExpr {
    pub fn new(sql: impl Into<String>, ty: KqlType) -> TExpr {
        TExpr { sql: sql.into(), ty, konst: None, agg: false, window: false, cmp: false }
    }

    pub fn konst(sql: impl Into<String>, ty: KqlType, c: Const) -> TExpr {
        TExpr { sql: sql.into(), ty, konst: Some(c), agg: false, window: false, cmp: false }
    }

    /// Derives a new expression from `parts`, inheriting their aggregate/window flags.
    pub fn derived(sql: impl Into<String>, ty: KqlType, parts: &[&TExpr]) -> TExpr {
        TExpr {
            sql: sql.into(),
            ty,
            konst: None,
            agg: parts.iter().any(|p| p.agg),
            window: parts.iter().any(|p| p.window),
            cmp: false,
        }
    }

    /// Marks a comparison result (see [`TExpr::cmp`]).
    pub fn comparison(mut self) -> TExpr {
        self.cmp = true;
        self
    }

    /// The SQL of a boolean with Kusto's never-null comparison semantics.
    pub fn bool_sql(&self) -> String {
        if self.cmp {
            format!("COALESCE({}, false)", self.sql)
        } else {
            self.sql.clone()
        }
    }

    pub fn is_null_const(&self) -> bool {
        matches!(self.konst, Some(Const::Null))
    }

    pub fn str_const(&self) -> Option<&str> {
        match &self.konst {
            Some(Const::Str(s)) => Some(s),
            _ => None,
        }
    }

    pub fn long_const(&self) -> Option<i64> {
        match &self.konst {
            Some(Const::Long(v)) => Some(*v),
            _ => None,
        }
    }
}

/// `$left` / `$right` bindings inside a join condition.
pub(crate) struct JoinSides<'s> {
    pub left: &'s [Column],
    pub right: &'s [Column],
    pub left_alias: &'s str,
    pub right_alias: &'s str,
}

/// The row context an expression is compiled in.
pub(crate) struct Scope<'s> {
    pub cols: &'s [Column],
    /// Qualifier prepended to column references (e.g. a join alias).
    pub qual: Option<&'s str>,
    pub join: Option<JoinSides<'s>>,
    pub aggregates: bool,
    /// The serialized row order (SQL ORDER BY items), for window functions.
    pub order: &'s [String],
    /// Extra columns visible to the expression (e.g. `scan` state) as (name, sql, type).
    pub extra: &'s [(String, String, KqlType)],
}

impl<'s> Scope<'s> {
    pub fn rows(cols: &'s [Column]) -> Scope<'s> {
        Scope { cols, qual: None, join: None, aggregates: false, order: &[], extra: &[] }
    }

    pub fn empty() -> Scope<'static> {
        Scope { cols: &[], qual: None, join: None, aggregates: false, order: &[], extra: &[] }
    }

    pub fn find(&self, name: &str) -> Option<&'s Column> {
        self.cols.iter().find(|c| c.name == name)
    }
}

pub(crate) fn col_ref(qual: Option<&str>, name: &str) -> String {
    match qual {
        Some(q) => format!("{q}.{}", quote_ident(name)),
        None => quote_ident(name),
    }
}

impl Ctx<'_> {
    pub fn expr(&mut self, e: &Expr, scope: &Scope, env: &Env) -> Result<TExpr> {
        match e {
            Expr::Literal(l) => self.literal(l),
            Expr::Name(n) => self.name_ref(n, scope, env),
            Expr::Paren(inner) => {
                if self.is_tabular(inner, env) {
                    // a tabular expression in scalar position: an implicit toscalar
                    return self.scalar_subquery(inner, env);
                }
                self.expr(inner, scope, env)
            }
            Expr::Unary { op, expr } => {
                let x = self.expr(expr, scope, env)?;
                let x = self.undynamic_numeric(x);
                if !(x.ty.is_numeric() || x.ty == KqlType::TimeSpan) {
                    return err(format!("unary operator not supported for type {}", x.ty));
                }
                Ok(match op {
                    UnaryOp::Neg => {
                        let ty = x.ty;
                        TExpr::derived(format!("(-{})", x.sql), ty, &[&x])
                    }
                    UnaryOp::Plus => x,
                })
            }
            Expr::Binary { op, left, right } => self.binary(*op, left, right, scope, env),
            Expr::In { kind, expr, list } => self.in_expr(*kind, expr, list, scope, env),
            Expr::Between { expr, low, high, negated } => {
                let x = self.expr(expr, scope, env)?;
                let lo = self.expr(low, scope, env)?;
                let mut hi = self.expr(high, scope, env)?;
                if x.ty == KqlType::DateTime && hi.ty == KqlType::TimeSpan {
                    hi = self.dt_add(&lo, &hi);
                }
                let (x, lo, hi) = (self.undynamic_numeric(x), self.undynamic_numeric(lo), self.undynamic_numeric(hi));
                let not = if *negated { "NOT " } else { "" };
                Ok(TExpr::derived(format!("({} {not}BETWEEN {} AND {})", x.sql, lo.sql, hi.sql), KqlType::Bool, &[&x, &lo, &hi]))
            }
            Expr::Member { expr, name } => {
                if let Expr::Name(side) = &**expr {
                    if side == "$left" || side == "$right" {
                        return self.join_side_ref(side, name, scope);
                    }
                }
                let base = self.expr(expr, scope, env)?;
                self.dyn_member(base, name)
            }
            Expr::Index { expr, index } => {
                let base = self.expr(expr, scope, env)?;
                let idx = self.expr(index, scope, env)?;
                self.dyn_index(base, idx)
            }
            Expr::Call { name, args } => self.call(name, args, scope, env),
            Expr::Star => err("'*' is not valid here"),
            Expr::Pipe { .. } | Expr::Source(_) => self.scalar_subquery(e, env),
        }
    }

    /// A tabular expression used as a scalar: the single value of its first column.
    pub fn scalar_subquery(&mut self, e: &Expr, env: &Env) -> Result<TExpr> {
        let rel = self.tabular(e, env)?;
        let Some(col) = rel.cols.first().cloned() else {
            return err("toscalar() requires a tabular expression with at least one column");
        };
        let mut sel = rel.wrap(self).sel;
        sel.items = Some(vec![crate::sql::Item { sql: quote_ident(&col.name), alias: col.name.clone() }]);
        sel.limit = Some("1".into());
        Ok(TExpr::new(format!("({})", sel.render()), col.ty))
    }

    pub fn literal(&mut self, l: &Literal) -> Result<TExpr> {
        let d = self.d;
        Ok(match l {
            Literal::Null(ty) => {
                let t = ty.as_deref().and_then(KqlType::from_name).unwrap_or(KqlType::Dynamic);
                if t == KqlType::String {
                    TExpr::konst("''", KqlType::String, Const::Str(String::new()))
                } else {
                    TExpr::konst(format!("CAST(NULL AS {})", d.sql_type(t)), t, Const::Null)
                }
            }
            Literal::Bool(b) => TExpr::konst(if *b { "true" } else { "false" }, KqlType::Bool, Const::Bool(*b)),
            Literal::Int(v) => TExpr::konst(format!("CAST({v} AS {})", d.sql_type(KqlType::Int)), KqlType::Int, Const::Long(*v)),
            Literal::Long(v) => TExpr::konst(long_sql(*v), KqlType::Long, Const::Long(*v)),
            Literal::Real(v) => TExpr::konst(d.real_literal(*v), KqlType::Real, Const::Real(*v)),
            Literal::Decimal(t) => {
                let v: f64 = t.parse().map_err(|_| crate::Error::new(format!("invalid decimal literal '{t}'")))?;
                TExpr::konst(d.real_literal(v), KqlType::Decimal, Const::Real(v))
            }
            Literal::String(s) => TExpr::konst(quote_str(s), KqlType::String, Const::Str(s.clone())),
            Literal::DateTime(t) => {
                let us = datetime::parse_datetime(t).ok_or_else(|| crate::Error::new(format!("invalid datetime literal '{t}'")))?;
                TExpr::konst(d.timestamp_literal(&datetime::format_sql(us)), KqlType::DateTime, Const::DateTime(us))
            }
            Literal::TimeSpan(ticks) => TExpr::konst(long_sql(*ticks), KqlType::TimeSpan, Const::TimeSpan(*ticks)),
            Literal::Guid(g) => {
                TExpr::konst(format!("CAST({} AS {})", quote_str(&g.to_ascii_lowercase()), d.sql_type(KqlType::Guid)), KqlType::Guid, Const::Str(g.clone()))
            }
            Literal::Dynamic(j) => {
                let v = json_value(j)?;
                TExpr::konst(d.json_literal(&v.to_string()), KqlType::Dynamic, Const::Dynamic(v))
            }
        })
    }

    fn name_ref(&mut self, n: &str, scope: &Scope, env: &Env) -> Result<TExpr> {
        // input columns first (`extend a = c, c = a` reads the original a), then columns
        // defined earlier in the same operator
        if let Some(c) = scope.find(n) {
            return Ok(TExpr::new(col_ref(scope.qual, &c.name), c.ty));
        }
        if let Some((_, sql, ty)) = scope.extra.iter().rev().find(|(name, _, _)| name == n) {
            return Ok(TExpr::new(sql.clone(), *ty));
        }
        match env.get(n) {
            Some(Binding::Scalar(t)) => return Ok(t.clone()),
            Some(Binding::Tabular(_)) => return err(format!("'{n}' is a tabular expression and cannot be used as a scalar")),
            Some(Binding::Function(_)) => return err(format!("function '{n}' must be called with ()")),
            None => {}
        }
        if let Some(j) = &scope.join {
            // join conditions: bare names refer to both sides; resolved by the join handler.
            if j.left.iter().any(|c| c.name == n) {
                return Ok(TExpr::new(col_ref(Some(j.left_alias), n), j.left.iter().find(|c| c.name == n).unwrap().ty));
            }
        }
        err(format!("unknown name '{n}'"))
    }

    fn join_side_ref(&mut self, side: &str, name: &str, scope: &Scope) -> Result<TExpr> {
        let Some(j) = &scope.join else {
            return err(format!("'{side}' is only valid in join conditions"));
        };
        let (cols, alias) = if side == "$left" { (j.left, j.left_alias) } else { (j.right, j.right_alias) };
        match cols.iter().find(|c| c.name == name) {
            Some(c) => Ok(TExpr::new(col_ref(Some(alias), name), c.ty)),
            None => err(format!("unknown column '{name}' on the {} side of the join", &side[1..])),
        }
    }

    // ------------------------------------------------------------------ operators

    fn binary(&mut self, op: BinaryOp, left: &Expr, right: &Expr, scope: &Scope, env: &Env) -> Result<TExpr> {
        if matches!(left, Expr::Star) {
            // `* has "x"`: any column
            return crate::op_search::star_binary(self, op, right, scope, env);
        }
        let l = self.expr(left, scope, env)?;
        let r = self.expr(right, scope, env)?;
        match op {
            BinaryOp::And | BinaryOp::Or => {
                let (l, r) = (self.to_bool(l), self.to_bool(r));
                let kw = if op == BinaryOp::And { "AND" } else { "OR" };
                let both = l.cmp && r.cmp;
                let t = TExpr::derived(format!("({} {kw} {})", l.sql, r.sql), KqlType::Bool, &[&l, &r]);
                Ok(if both { t.comparison() } else { t })
            }
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => self.arith(op, l, r),
            BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => self.compare(op, l, r),
            BinaryOp::EqTilde | BinaryOp::NeTilde => {
                let (l, r) = (self.to_string(l), self.to_string(r));
                let o = if op == BinaryOp::EqTilde { "=" } else { "<>" };
                Ok(TExpr::derived(format!("(lower({}) {o} lower({}))", l.sql, r.sql), KqlType::Bool, &[&l, &r]))
            }
            BinaryOp::Str(sop, negated) => {
                let t = self.string_op(sop, l, r)?;
                Ok(if negated { TExpr::derived(format!("(NOT {})", t.bool_sql()), KqlType::Bool, &[&t]).comparison() } else { t })
            }
            BinaryOp::MatchesRegex => {
                let (l, r) = (self.to_string(l), self.to_string(r));
                if let Some(re) = r.str_const() {
                    let re = crate::regex::translate(re, self.d.kind());
                    return Ok(TExpr::derived(self.d.regex_match(&l.sql, &quote_str(&re)), KqlType::Bool, &[&l]));
                }
                Ok(TExpr::derived(self.d.regex_match(&l.sql, &r.sql), KqlType::Bool, &[&l, &r]))
            }
        }
    }

    /// Dynamic operands of arithmetic/comparison become numbers (or text for strings).
    fn undynamic_numeric(&self, x: TExpr) -> TExpr {
        if x.ty == KqlType::Dynamic {
            let t = self.d.try_cast(&self.d.json_to_text(&x.sql), KqlType::Real);
            TExpr::derived(t, KqlType::Real, &[&x])
        } else {
            x
        }
    }

    pub fn arith(&mut self, op: BinaryOp, l: TExpr, r: TExpr) -> Result<TExpr> {
        use KqlType::*;
        let (l, r) = (self.undynamic_numeric(l), self.undynamic_numeric(r));
        let sym = match op {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            _ => "%",
        };
        let parts = [&l, &r];
        let ty_err = || err(format!("operator '{sym}' is not defined for {} and {}", l.ty, r.ty));
        match (l.ty, r.ty) {
            (a, b) if a.is_numeric() && b.is_numeric() => {
                let t = widest(a, b);
                if t.is_integer() {
                    let (ls, rs) = (as_bigint(&l), as_bigint(&r));
                    let idiv = if self.d.kind() == crate::Dialect::DuckDb { "//" } else { "/" };
                    let sql = match op {
                        // Kusto: integer division truncates toward zero; division by zero is null.
                        BinaryOp::Div => format!("({ls} {idiv} NULLIF({rs}, 0))"),
                        // Kusto: the result of % has the sign of the divisor... (Euclidean, verified against Kusto)
                        BinaryOp::Mod => format!("((({ls} % NULLIF({rs}, 0)) + abs({rs})) % NULLIF(abs({rs}), 0))"),
                        _ => format!("({ls} {sym} {rs})"),
                    };
                    // Kusto promotes int arithmetic to long
                    Ok(TExpr::derived(sql, Long, &parts))
                } else {
                    let (ls, rs) = (self.d.cast(&l.sql, Real), self.d.cast(&r.sql, Real));
                    let sql = match op {
                        BinaryOp::Div => format!("({} / {})", ls, rs),
                        // Kusto's % is Euclidean for reals too: the result has the divisor's... absolute sign
                        BinaryOp::Mod => format!("fmod(fmod({ls}, {rs}) + abs({rs}), abs({rs}))"),
                        _ => format!("({} {sym} {})", ls, rs),
                    };
                    Ok(TExpr::derived(sql, if t == Decimal { Decimal } else { Real }, &parts))
                }
            }
            (TimeSpan, TimeSpan) => match op {
                BinaryOp::Add | BinaryOp::Sub => Ok(TExpr::derived(format!("({} {sym} {})", l.sql, r.sql), TimeSpan, &parts)),
                BinaryOp::Div => Ok(TExpr::derived(
                    format!("(CAST({} AS DOUBLE) / NULLIF(CAST({} AS DOUBLE), 0))", l.sql, r.sql).replace(
                        "AS DOUBLE",
                        if self.d.kind() == crate::Dialect::Postgres { "AS double precision" } else { "AS DOUBLE" },
                    ),
                    Real,
                    &parts,
                )),
                BinaryOp::Mod => Ok(TExpr::derived(format!("({} % NULLIF({}, 0))", l.sql, r.sql), TimeSpan, &parts)),
                _ => ty_err(),
            },
            (TimeSpan, n) | (n, TimeSpan) if n.is_numeric() && matches!(op, BinaryOp::Mul) => {
                let (ts, num) = if l.ty == TimeSpan { (&l, &r) } else { (&r, &l) };
                Ok(TExpr::derived(format!("CAST(round({} * {}) AS BIGINT)", ts.sql, self.d.cast(&num.sql, Real)), TimeSpan, &parts))
            }
            (TimeSpan, n) if n.is_numeric() && matches!(op, BinaryOp::Div) => {
                Ok(TExpr::derived(format!("CAST(round({} / NULLIF({}, 0)) AS BIGINT)", l.sql, self.d.cast(&r.sql, Real)), TimeSpan, &parts))
            }
            (DateTime, TimeSpan) if matches!(op, BinaryOp::Add | BinaryOp::Sub) => {
                Ok(if op == BinaryOp::Add { self.dt_add(&l, &r) } else { self.dt_sub_ts(&l, &r) })
            }
            (TimeSpan, DateTime) if op == BinaryOp::Add => Ok(self.dt_add(&r, &l)),
            (DateTime, DateTime) if op == BinaryOp::Sub => {
                let sql = format!("(({} - {}) * 10)", self.d.epoch_us(&l.sql), self.d.epoch_us(&r.sql));
                Ok(TExpr::derived(sql, TimeSpan, &parts))
            }
            _ => ty_err(),
        }
    }

    /// datetime + timespan
    pub fn dt_add(&self, dt: &TExpr, ts: &TExpr) -> TExpr {
        let us = format!("({} + CAST(trunc({} / 10) AS BIGINT))", self.d.epoch_us(&dt.sql), ts.sql);
        TExpr::derived(self.d.ts_from_us(&us), KqlType::DateTime, &[dt, ts])
    }

    pub fn dt_sub_ts(&self, dt: &TExpr, ts: &TExpr) -> TExpr {
        let us = format!("({} - CAST(trunc({} / 10) AS BIGINT))", self.d.epoch_us(&dt.sql), ts.sql);
        TExpr::derived(self.d.ts_from_us(&us), KqlType::DateTime, &[dt, ts])
    }

    pub fn compare(&mut self, op: BinaryOp, l: TExpr, r: TExpr) -> Result<TExpr> {
        use KqlType::*;
        let sym = match op {
            BinaryOp::Eq => "=",
            BinaryOp::Ne => "<>",
            BinaryOp::Lt => "<",
            BinaryOp::Le => "<=",
            BinaryOp::Gt => ">",
            _ => ">=",
        };
        // Bring both sides to a comparable representation.
        let (l, r) = match (l.ty, r.ty) {
            (a, b) if a == b && a != Dynamic => (l, r),
            (a, b) if a.is_numeric() && b.is_numeric() => (l, r),
            (Dynamic, String) | (String, Dynamic) => (self.to_string(l), self.to_string(r)),
            (Dynamic, Dynamic) if matches!(op, BinaryOp::Eq | BinaryOp::Ne) => (l, r),
            (Dynamic, b) | (b, Dynamic) if b.is_numeric() => (self.undynamic_numeric(l), self.undynamic_numeric(r)),
            (Dynamic, b) | (b, Dynamic) => {
                let (l, r) = if l.ty == Dynamic { (self.convert(l, b), r) } else { (l, self.convert(r, b)) };
                (l, r)
            }
            (Bool, b) | (b, Bool) if b.is_numeric() => (self.to_long(l), self.to_long(r)),
            (DateTime, String) => {
                let r = self.convert(r, DateTime);
                (l, r)
            }
            (String, DateTime) => {
                let l = self.convert(l, DateTime);
                (l, r)
            }
            (Guid, String) | (String, Guid) => (self.to_string(l), self.to_string(r)),
            _ if l.is_null_const() || r.is_null_const() => (l, r),
            _ => return err(format!("cannot compare {} with {}", l.ty, r.ty)),
        };
        if op == BinaryOp::Ne {
            // Kusto's != is true when exactly one side is null, and NaN != NaN
            let mut sql = format!("({} IS DISTINCT FROM {})", l.sql, r.sql);
            if l.ty == Real || r.ty == Real {
                let nans: Vec<std::string::String> = [&l, &r].iter().filter(|t| t.ty == Real && t.konst.is_none()).map(|t| format!("isnan({})", t.sql)).collect();
                if !nans.is_empty() {
                    sql = format!("({sql} OR {})", nans.join(" OR "));
                }
            }
            return Ok(TExpr::derived(sql, Bool, &[&l, &r]));
        }
        let mut sql = format!("({} {sym} {})", l.sql, r.sql);
        // NaN compares false with everything in Kusto; DuckDB orders NaN above all numbers.
        let nan_guard = |t: &TExpr| t.ty == Real && !matches!(t.konst, Some(Const::Real(v)) if !v.is_nan()) && !matches!(t.konst, Some(Const::Long(_)));
        if l.ty == Real || r.ty == Real {
            let mut guards = Vec::new();
            for t in [&l, &r] {
                if nan_guard(t) {
                    guards.push(format!("NOT isnan({})", t.sql));
                }
            }
            if !guards.is_empty() {
                sql = format!("({sql} AND {})", guards.join(" AND "));
            }
        }
        Ok(TExpr::derived(sql, Bool, &[&l, &r]).comparison())
    }

    fn string_op(&mut self, op: StringOp, l: TExpr, r: TExpr) -> Result<TExpr> {
        let (l, r) = (self.to_string(l), self.to_string(r));
        let d = self.d;
        let parts = [&l, &r];
        let ci = |s: &str| format!("lower({s})");
        let sql = match op {
            StringOp::Contains => format!("({} > 0)", d.strpos(&ci(&l.sql), &ci(&r.sql))),
            StringOp::ContainsCs => format!("({} > 0)", d.strpos(&l.sql, &r.sql)),
            StringOp::StartsWith => d.starts_with(&ci(&l.sql), &ci(&r.sql)),
            StringOp::StartsWithCs => d.starts_with(&l.sql, &r.sql),
            StringOp::EndsWith => d.ends_with(&ci(&l.sql), &ci(&r.sql)),
            StringOp::EndsWithCs => d.ends_with(&l.sql, &r.sql),
            StringOp::Has | StringOp::HasCs | StringOp::HasPrefix | StringOp::HasPrefixCs | StringOp::HasSuffix | StringOp::HasSuffixCs => {
                let cs = matches!(op, StringOp::HasCs | StringOp::HasPrefixCs | StringOp::HasSuffixCs);
                let (pre, post) = match op {
                    StringOp::Has | StringOp::HasCs => (true, true),
                    StringOp::HasPrefix | StringOp::HasPrefixCs => (true, false),
                    _ => (false, true),
                };
                return Ok(TExpr::derived(self.term_match(&l.sql, &r, cs, pre, post), KqlType::Bool, &parts));
            }
            StringOp::Like | StringOp::LikeCs => {
                let kw = if op == StringOp::Like { "ILIKE" } else { "LIKE" };
                format!("({} {kw} {})", l.sql, r.sql)
            }
        };
        Ok(TExpr::derived(sql, KqlType::Bool, &parts))
    }

    /// Whole-term matching for `has` and friends: a term is a maximal run of letters/digits.
    pub fn term_match(&self, s: &str, needle: &TExpr, case_sensitive: bool, term_start: bool, term_end: bool) -> String {
        const BOUND_L: &str = "(^|[^\\p{L}\\p{N}])";
        const BOUND_R: &str = "([^\\p{L}\\p{N}]|$)";
        let flags = if case_sensitive { "" } else { "(?i)" };
        let pg = self.d.kind() == crate::Dialect::Postgres;
        let (bl, br) = if pg { ("(^|[^[:alnum:]])", "([^[:alnum:]]|$)") } else { (BOUND_L, BOUND_R) };
        if let Some(lit) = needle.str_const() {
            if lit.is_empty() {
                return "true".into();
            }
            // a boundary is only required next to an alphanumeric edge of the needle
            let first_alnum = lit.chars().next().is_some_and(char::is_alphanumeric);
            let last_alnum = lit.chars().last().is_some_and(char::is_alphanumeric);
            let mut re = String::new();
            if term_start && first_alnum {
                re.push_str(bl);
            }
            re.push_str(&crate::regex::escape(lit));
            if term_end && last_alnum {
                re.push_str(br);
            }
            if pg {
                let op = if case_sensitive { "~" } else { "~*" };
                return format!("({s} {op} {})", quote_str(&re));
            }
            return self.d.regex_match(s, &quote_str(&format!("{flags}{re}")));
        }
        let esc = self.d.regex_escape(&needle.sql);
        let pre = if term_start { quote_str(&format!("{flags}{bl}")) } else { quote_str(flags) };
        let post = if term_end { quote_str(br) } else { "''".into() };
        let pattern = format!("({pre} || {esc} || {post})");
        if pg {
            let op = if case_sensitive { "~" } else { "~*" };
            return format!("({s} {op} {pattern})");
        }
        self.d.regex_match(s, &pattern)
    }

    fn in_expr(&mut self, kind: InKind, expr: &Expr, list: &[Expr], scope: &Scope, env: &Env) -> Result<TExpr> {
        let x = self.expr(expr, scope, env)?;
        // tabular argument: x in (T | project c)
        if list.len() == 1 && self.is_tabular(&list[0], env) {
            let rel = self.tabular(&list[0], env)?;
            let Some(col) = rel.cols.first().cloned() else { return err("'in' subquery has no columns") };
            let ci = matches!(kind, InKind::InCi | InKind::NotInCi);
            let colsql = quote_ident(&col.name);
            let mut sel = rel.wrap(self).sel;
            let item = if ci { format!("lower({colsql})") } else { colsql.clone() };
            sel.items = Some(vec![crate::sql::Item { sql: item, alias: col.name.clone() }]);
            sel.filters.push(format!("({colsql} IS NOT NULL)"));
            let lhs = if ci { format!("lower({})", self.to_string(x.clone()).sql) } else { x.sql.clone() };
            let not = if matches!(kind, InKind::NotIn | InKind::NotInCi) { "NOT " } else { "" };
            return match kind {
                InKind::HasAny | InKind::HasAll => err("has_any/has_all with a tabular argument is not supported"),
                _ => Ok(TExpr::derived(format!("(COALESCE({lhs} {not}IN ({}), {}))", sel.render(), not.is_empty().then_some("false").unwrap_or("true")), KqlType::Bool, &[&x])),
            };
        }
        // expand dynamic array constants into the list
        let mut items = Vec::new();
        for e in list {
            let t = self.expr(e, scope, env)?;
            match &t.konst {
                Some(Const::Dynamic(serde_json::Value::Array(vals))) => {
                    for v in vals {
                        items.push(self.json_const_to_scalar(v));
                    }
                }
                _ if t.ty == KqlType::Dynamic => {
                    return self.in_dynamic(kind, x, t);
                }
                _ => items.push(t),
            }
        }
        match kind {
            InKind::HasAny | InKind::HasAll => {
                if items.is_empty() {
                    return Ok(TExpr::new(if kind == InKind::HasAll { "true" } else { "false" }, KqlType::Bool));
                }
                let s = self.to_string(x);
                let parts: Vec<String> = items.into_iter().map(|i| {
                    let i = self.to_string(i);
                    self.term_match(&s.sql, &i, false, true, true)
                }).collect();
                let j = if kind == InKind::HasAny { " OR " } else { " AND " };
                Ok(TExpr::derived(format!("({})", parts.join(j)), KqlType::Bool, &[&s]))
            }
            _ => {
                if items.is_empty() {
                    let v = matches!(kind, InKind::NotIn | InKind::NotInCi);
                    return Ok(TExpr::new(if v { "true" } else { "false" }, KqlType::Bool));
                }
                let ci = matches!(kind, InKind::InCi | InKind::NotInCi);
                let x = if ci || items.iter().any(|i| i.ty == KqlType::String) && x.ty == KqlType::Dynamic { self.to_string(x) } else { x };
                let lhs = if ci { format!("lower({})", x.sql) } else { x.sql.clone() };
                let mut vals = Vec::new();
                for i in items {
                    let i = if x.ty == KqlType::String { self.to_string(i) } else { i };
                    vals.push(if ci { format!("lower({})", i.sql) } else { i.sql });
                }
                let t = TExpr::derived(format!("({lhs} IN ({}))", vals.join(", ")), KqlType::Bool, &[&x]).comparison();
                if matches!(kind, InKind::NotIn | InKind::NotInCi) {
                    return Ok(TExpr::derived(format!("(NOT {})", t.bool_sql()), KqlType::Bool, &[&t]));
                }
                Ok(t)
            }
        }
    }

    /// `x in (dyn)` where dyn is a non-constant dynamic array.
    fn in_dynamic(&mut self, kind: InKind, x: TExpr, arr: TExpr) -> Result<TExpr> {
        let s = self.to_string(x);
        let je = self.d.json_each(&arr.sql, "_e");
        let ci = matches!(kind, InKind::InCi | InKind::NotInCi);
        let v = self.d.json_to_text(&je.value);
        let cond = if ci { format!("lower({v}) = lower({})", s.sql) } else { format!("{v} = {}", s.sql) };
        let exists = format!("EXISTS (SELECT 1 FROM {} WHERE {cond})", je.from_item);
        let sql = match kind {
            InKind::NotIn | InKind::NotInCi => format!("(NOT {exists})"),
            InKind::In | InKind::InCi => format!("({exists})"),
            _ => return err("has_any/has_all with a non-constant dynamic argument is not supported"),
        };
        Ok(TExpr::derived(sql, KqlType::Bool, &[&s, &arr]))
    }

    // ------------------------------------------------------------------ dynamic access

    pub fn dyn_member(&mut self, base: TExpr, name: &str) -> Result<TExpr> {
        if base.ty != KqlType::Dynamic {
            return err(format!("'.{name}': member access requires a dynamic value, found {}", base.ty));
        }
        Ok(TExpr::derived(self.d.json_get_key_lit(&base.sql, name), KqlType::Dynamic, &[&base]))
    }

    pub fn dyn_index(&mut self, base: TExpr, idx: TExpr) -> Result<TExpr> {
        if base.ty != KqlType::Dynamic {
            return err(format!("indexing requires a dynamic value, found {}", base.ty));
        }
        let sql = match (&idx.konst, idx.ty) {
            (Some(Const::Str(k)), _) => self.d.json_get_key_lit(&base.sql, k),
            (Some(Const::Long(i)), _) => self.d.json_get_index_lit(&base.sql, *i),
            (_, KqlType::String) => self.d.json_get_key(&base.sql, &idx.sql),
            (_, t) if t.is_integer() => self.d.json_get_index(&base.sql, &idx.sql),
            _ => return err(format!("invalid index type {}", idx.ty)),
        };
        Ok(TExpr::derived(sql, KqlType::Dynamic, &[&base, &idx]))
    }

    pub fn json_const_to_scalar(&mut self, v: &serde_json::Value) -> TExpr {
        match v {
            serde_json::Value::Null => TExpr::konst("NULL", KqlType::Dynamic, Const::Null),
            serde_json::Value::Bool(b) => TExpr::konst(b.to_string(), KqlType::Bool, Const::Bool(*b)),
            serde_json::Value::Number(n) => match n.as_i64() {
                Some(i) => TExpr::konst(long_sql(i), KqlType::Long, Const::Long(i)),
                None => {
                    let f = n.as_f64().unwrap_or(f64::NAN);
                    TExpr::konst(self.d.real_literal(f), KqlType::Real, Const::Real(f))
                }
            },
            serde_json::Value::String(s) => TExpr::konst(quote_str(s), KqlType::String, Const::Str(s.clone())),
            other => {
                let t = other.to_string();
                TExpr::konst(self.d.json_literal(&t), KqlType::Dynamic, Const::Dynamic(other.clone()))
            }
        }
    }

    // ------------------------------------------------------------------ conversions

    pub fn to_bool(&self, x: TExpr) -> TExpr {
        match x.ty {
            KqlType::Bool => x,
            KqlType::Dynamic => {
                let s = self.d.try_cast(&self.d.json_to_text(&x.sql), KqlType::Bool);
                TExpr::derived(s, KqlType::Bool, &[&x])
            }
            t if t.is_numeric() => TExpr::derived(format!("({} <> 0)", x.sql), KqlType::Bool, &[&x]),
            _ => {
                let s = self.d.try_cast(&x.sql, KqlType::Bool);
                TExpr::derived(s, KqlType::Bool, &[&x])
            }
        }
    }

    pub fn to_long(&self, x: TExpr) -> TExpr {
        match x.ty {
            KqlType::Long => x,
            KqlType::Int => TExpr::derived(self.d.cast(&x.sql, KqlType::Long), KqlType::Long, &[&x]),
            KqlType::Bool => TExpr::derived(format!("CAST({} AS {})", x.sql, self.d.sql_type(KqlType::Long)).replace("CAST(true", "CAST(true").to_string(), KqlType::Long, &[&x]),
            _ => self.convert(x, KqlType::Long),
        }
    }

    /// Kusto string conversion (`tostring`).
    pub fn to_string(&self, x: TExpr) -> TExpr {
        let d = self.d;
        let sql = match x.ty {
            KqlType::String => return x,
            _ if x.is_null_const() => "''".to_string(),
            KqlType::Bool => format!("COALESCE(CASE WHEN {0} THEN 'True' WHEN NOT {0} THEN 'False' END, '')", x.sql),
            KqlType::Int | KqlType::Long => format!("COALESCE(CAST({} AS {}), '')", x.sql, d.sql_type(KqlType::String)),
            KqlType::Real | KqlType::Decimal => format!("COALESCE({}, '')", self.real_to_string(&x.sql)),
            KqlType::DateTime => format!("COALESCE({}, '')", self.datetime_to_string(&x.sql)),
            KqlType::TimeSpan => format!("COALESCE({}, '')", self.timespan_to_string(&x.sql)),
            KqlType::Guid => format!("COALESCE(CAST({} AS {}), '')", x.sql, d.sql_type(KqlType::String)),
            KqlType::Dynamic => format!("COALESCE({}, '')", d.json_to_text(&x.sql)),
        };
        TExpr::derived(sql, KqlType::String, &[&x])
    }

    /// Formats a double like .NET's default `ToString()`: integral values without a fraction.
    pub fn real_to_string(&self, x: &str) -> String {
        let vs = self.d.sql_type(KqlType::String);
        let bi = self.d.sql_type(KqlType::Long);
        format!(
            "CASE WHEN isnan({x}) THEN 'NaN' WHEN {x} = CAST('inf' AS DOUBLE) THEN 'inf' WHEN {x} = CAST('-inf' AS DOUBLE) THEN '-inf' \
             WHEN {x} = trunc({x}) AND abs({x}) < 1e15 THEN CAST(CAST({x} AS {bi}) AS {vs}) || '.0' ELSE CAST({x} AS {vs}) END"
        )
        .replace("CAST('inf' AS DOUBLE)", &self.d.real_literal(f64::INFINITY))
        .replace("CAST('-inf' AS DOUBLE)", &self.d.real_literal(f64::NEG_INFINITY))
    }

    pub fn datetime_to_string(&self, x: &str) -> String {
        match self.d.kind() {
            crate::Dialect::DuckDb => format!("strftime({x}, '%Y-%m-%dT%H:%M:%S.%f0Z')"),
            crate::Dialect::Postgres => format!("to_char({x}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"0Z\"')"),
        }
    }

    /// Kusto timespan text: `[-][d.]hh:mm:ss[.fffffff]`.
    pub fn timespan_to_string(&self, ticks: &str) -> String {
        let vs = self.d.sql_type(KqlType::String);
        let a = format!("abs({ticks})");
        let lpad = |e: String, n: u32| format!("lpad(CAST({e} AS {vs}), {n}, '0')");
        let days = format!("({a} // 864000000000)");
        let hours = format!("(({a} // 36000000000) % 24)");
        let mins = format!("(({a} // 600000000) % 60)");
        let secs = format!("(({a} // 10000000) % 60)");
        let frac = format!("({a} % 10000000)");
        let s = format!(
            "(CASE WHEN {ticks} < 0 THEN '-' ELSE '' END || CASE WHEN {days} > 0 THEN CAST({days} AS {vs}) || '.' ELSE '' END || {} || ':' || {} || ':' || {} || CASE WHEN {frac} > 0 THEN '.' || {} ELSE '' END)",
            lpad(hours, 2),
            lpad(mins, 2),
            lpad(secs, 2),
            lpad(frac.clone(), 7)
        );
        if self.d.kind() == crate::Dialect::Postgres {
            s.replace(" // ", " / ")
        } else {
            s
        }
    }

    /// Converts to `t` with Kusto semantics (null on failure).
    pub fn convert(&self, x: TExpr, t: KqlType) -> TExpr {
        use KqlType::*;
        let d = self.d;
        if x.ty == t {
            return x;
        }
        if x.is_null_const() {
            return if t == String {
                TExpr::konst("''", String, Const::Str(std::string::String::new()))
            } else {
                TExpr::konst(format!("CAST(NULL AS {})", d.sql_type(t)), t, Const::Null)
            };
        }
        let ty = |t: KqlType| d.sql_type(t);
        let sql = match (x.ty, t) {
            (_, String) => return self.to_string(x),
            (_, Dynamic) => return self.to_dynamic(x),
            (Bool, b) if b.is_numeric() => format!("CAST(CASE WHEN {0} THEN 1 WHEN NOT {0} THEN 0 END AS {1})", x.sql, ty(b)),
            (a, Bool) if a.is_numeric() => format!("({} <> 0)", x.sql),
            (a, b) if a.is_numeric() && b.is_numeric() => {
                if b.is_integer() && !a.is_integer() {
                    format!("CAST(trunc({}) AS {})", x.sql, ty(b))
                } else {
                    d.cast(&x.sql, b)
                }
            }
            (Dynamic, b) => {
                // JSON booleans convert to 1/0 (numbers) or true/false; everything else via its text
                let text = TExpr::derived(d.json_to_text(&x.sql), String, &[&x]);
                let via_text = self.convert(text, b).sql;
                let is_bool = format!("{} = {}", d.json_type(&x.sql), if d.kind() == crate::Dialect::DuckDb { "'boolean'" } else { "'boolean'" });
                match b {
                    // a JSON real converts to an integer by truncation (`tolong(dynamic(1.0))` is 1)
                    b if b.is_integer() => format!(
                        "CASE WHEN {is_bool} THEN CAST(CASE WHEN {0} = 'true' THEN 1 ELSE 0 END AS {1}) WHEN {2} IN ('double', 'number') THEN {3} ELSE {via_text} END",
                        d.json_to_text(&x.sql),
                        ty(b),
                        d.json_type(&x.sql),
                        d.try_cast(&format!("trunc({})", d.try_cast(&d.json_to_text(&x.sql), Real)), b)
                    ),
                    b if b.is_numeric() => format!(
                        "CASE WHEN {is_bool} THEN CAST(CASE WHEN {} = 'true' THEN 1 ELSE 0 END AS {}) ELSE {via_text} END",
                        d.json_to_text(&x.sql),
                        ty(b)
                    ),
                    _ => via_text,
                }
            }
            (String, b) if b.is_integer() => {
                let v = format!("regexp_replace({}, '^\\s+|\\s+$', '', 'g')", x.sql);
                let ok = d.regex_match(&v, "'^[+-]?([0-9]+|0[xX][0-9a-fA-F]+)$'");
                format!("CASE WHEN {ok} THEN {} END", d.try_cast(&v, b))
            }
            (String, Real | Decimal) => {
                let v = format!("regexp_replace({}, '^\\s+|\\s+$', '', 'g')", x.sql);
                let ok = d.regex_match(&v, r"'^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$'");
                format!(
                    "CASE WHEN {ok} THEN {} WHEN {v} IN ('nan', 'NaN') THEN {} WHEN {v} IN ('inf', '+inf', 'Inf', '+Inf', 'Infinity', '+Infinity', 'infinity') THEN {} WHEN {v} IN ('-inf', '-Inf', '-Infinity', '-infinity') THEN {} END",
                    d.try_cast(&v, Real),
                    d.real_literal(f64::NAN),
                    d.real_literal(f64::INFINITY),
                    d.real_literal(f64::NEG_INFINITY)
                )
            }
            (String, DateTime) => d.try_cast(&format!("trim({})", x.sql), DateTime),
            (String, TimeSpan) => self.parse_timespan_sql(&x.sql),
            (String, Bool) => {
                let v = format!("lower({})", x.sql);
                let int = d.regex_match(&v, "'^[+-]?[0-9]+$'");
                format!("CASE WHEN {v} = 'true' THEN true WHEN {v} = 'false' THEN false WHEN {int} THEN {} <> 0 END", d.try_cast(&v, Long))
            }
            (String, b) => d.try_cast(&x.sql, b),
            (TimeSpan, b) if b.is_numeric() => d.cast(&x.sql, b),
            (DateTime, b) if b.is_numeric() => d.cast(&format!("({} * 10 + 621355968000000000)", d.epoch_us(&x.sql)), b),
            (a, DateTime) if a.is_numeric() => d.ts_from_us(&format!("CAST(({} - 621355968000000000) / 10 AS BIGINT)", d.cast(&x.sql, Long))),
            (a, TimeSpan) if a.is_numeric() => d.cast(&x.sql, Long),
            (Guid, _) | (_, Guid) => format!("CAST(NULL AS {})", ty(t)),
            _ => format!("CAST(NULL AS {})", ty(t)),
        };
        TExpr::derived(sql, t, &[&x])
    }

    /// Parses timespan text (`1.02:03:04.5`, `00:10:00`) into ticks; NULL if invalid.
    pub fn parse_timespan_sql(&self, s: &str) -> String {
        let d = self.d;
        let lit_re = r"^\s*(-?[0-9]+(?:\.[0-9]+)?)\s*(d|h|m|s|ms|microsecond|tick|day|days|hour|hours|minute|minutes|second|seconds|milliseconds?|microseconds?|ticks)\s*$";
        let num = d.try_cast(&d.regex_extract(s, &quote_str(lit_re), 1), KqlType::Real);
        let unit = d.regex_extract(s, &quote_str(lit_re), 2);
        let literal = format!(
            "CAST(round({num} * CASE {unit} WHEN 'd' THEN 864000000000 WHEN 'day' THEN 864000000000 WHEN 'days' THEN 864000000000 \
             WHEN 'h' THEN 36000000000 WHEN 'hour' THEN 36000000000 WHEN 'hours' THEN 36000000000 WHEN 'm' THEN 600000000 WHEN 'minute' THEN 600000000 WHEN 'minutes' THEN 600000000 \
             WHEN 's' THEN 10000000 WHEN 'second' THEN 10000000 WHEN 'seconds' THEN 10000000 WHEN 'ms' THEN 10000 WHEN 'millisecond' THEN 10000 WHEN 'milliseconds' THEN 10000 \
             WHEN 'microsecond' THEN 10 WHEN 'microseconds' THEN 10 ELSE 1 END) AS BIGINT)"
        );
        let clock = self.parse_clock_timespan_sql(s);
        format!("CASE WHEN {} THEN {literal} ELSE {clock} END", d.regex_match(s, &quote_str(lit_re)))
    }

    fn parse_clock_timespan_sql(&self, s: &str) -> String {
        let d = self.d;
        let re = r"^\s*(-)?(?:(\d+)\.)?(\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d{1,7}))?)?\s*$";
        let g = |n: u32| d.regex_extract(s, &quote_str(re), n);
        let num = |n: u32| format!("CAST(NULLIF({}, '') AS BIGINT)", g(n));
        let frac = format!("CAST(rpad(NULLIF({}, ''), 7, '0') AS BIGINT)", g(6));
        format!(
            "CASE WHEN {} THEN (CASE WHEN {} = '-' THEN -1 ELSE 1 END) * (COALESCE({}, 0) * 864000000000 + {} * 36000000000 + {} * 600000000 + COALESCE({}, 0) * 10000000 + COALESCE({frac}, 0)) END",
            d.regex_match(s, &quote_str(re)),
            g(1),
            num(2),
            num(3),
            num(4),
            num(5)
        )
    }

    /// Any value → dynamic.
    pub fn to_dynamic(&self, x: TExpr) -> TExpr {
        if x.ty == KqlType::Dynamic {
            return x;
        }
        let sql = match x.ty {
            KqlType::DateTime => self.d.to_json(&self.datetime_to_string(&x.sql)),
            KqlType::TimeSpan => self.d.to_json(&self.timespan_to_string(&x.sql)),
            KqlType::Guid => self.d.to_json(&format!("CAST({} AS {})", x.sql, self.d.sql_type(KqlType::String))),
            _ => self.d.to_json(&x.sql),
        };
        TExpr::derived(sql, KqlType::Dynamic, &[&x])
    }
}

/// A BIGINT literal that keeps 64-bit arithmetic.
pub(crate) fn long_sql(v: i64) -> String {
    if (i32::MIN as i64..=i32::MAX as i64).contains(&v) {
        if v < 0 {
            format!("({v})")
        } else {
            v.to_string()
        }
    } else if v == i64::MIN {
        "(-9223372036854775807 - 1)".into()
    } else if v < 0 {
        format!("({v})")
    } else {
        v.to_string()
    }
}

/// Integer operands of arithmetic are widened so 32-bit literals cannot overflow.
fn as_bigint(x: &TExpr) -> String {
    match x.konst {
        Some(Const::Long(v)) if x.ty == KqlType::Long && (i32::MIN as i64..=i32::MAX as i64).contains(&v) => format!("CAST({v} AS BIGINT)"),
        _ => x.sql.clone(),
    }
}

/// Converts a dynamic literal into a JSON value (typed scalars become strings).
pub(crate) fn json_value(j: &ast::Json) -> Result<serde_json::Value> {
    use serde_json::Value;
    Ok(match j {
        ast::Json::Null => Value::Null,
        ast::Json::Bool(b) => Value::Bool(*b),
        ast::Json::Number(n) => {
            if let Ok(i) = n.parse::<i64>() {
                Value::from(i)
            } else {
                let f: f64 = n.parse().map_err(|_| crate::Error::new(format!("invalid number '{n}'")))?;
                serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null)
            }
        }
        ast::Json::String(s) => Value::String(s.clone()),
        ast::Json::Array(items) => Value::Array(items.iter().map(json_value).collect::<Result<_>>()?),
        ast::Json::Object(members) => {
            let mut m = serde_json::Map::new();
            for (k, v) in members {
                m.insert(k.clone(), json_value(v)?);
            }
            Value::Object(m)
        }
        ast::Json::Scalar(lit) => match &**lit {
            Literal::DateTime(t) => {
                let us = datetime::parse_datetime(t).ok_or_else(|| crate::Error::new(format!("invalid datetime '{t}'")))?;
                Value::String(datetime::format_iso(us))
            }
            Literal::TimeSpan(ticks) => Value::String(crate::funcs::format_timespan_ticks(*ticks)),
            Literal::Guid(g) => Value::String(g.to_ascii_lowercase()),
            Literal::Null(_) => Value::Null,
            Literal::Long(v) | Literal::Int(v) => Value::from(*v),
            Literal::Real(f) => serde_json::Number::from_f64(*f).map(Value::Number).unwrap_or(Value::Null),
            Literal::Bool(b) => Value::Bool(*b),
            Literal::String(s) => Value::String(s.clone()),
            Literal::Decimal(d) => Value::String(d.clone()),
            Literal::Dynamic(j) => json_value(j)?,
        },
    })
}
