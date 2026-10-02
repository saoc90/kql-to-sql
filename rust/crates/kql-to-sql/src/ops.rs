//! Query operators. Each takes a [`Rel`] and returns a new one.

use kql_parser::ast::{self, Expr, NamedExpr, NullsOrder, Operator, SortDir};

use crate::binder::{Ctx, Env, OrderSpec, Rel};
use crate::expr::{Const, Scope, TExpr};
use crate::names::{result_name, DefaultNames, UniqueNames};
use crate::sql::{quote_ident, quote_str, From, Item, Query, Select};
use crate::{err, Column, KqlType, Result};

pub(crate) fn apply(ctx: &mut Ctx, rel: Rel, op: &Operator, env: &Env) -> Result<Rel> {
    match op {
        Operator::Where(e) => where_(ctx, rel, e, env),
        Operator::Extend(items) => extend(ctx, rel, items, env, false),
        Operator::Serialize(items) => extend(ctx, rel, items, env, true),
        Operator::Project(items) => project(ctx, rel, items, env),
        Operator::ProjectAway(pats) => {
            let keep: Vec<Column> = match_patterns(&rel.cols, pats.iter().map(|p| p.pattern.as_str()), false)?;
            select_columns(ctx, rel, keep)
        }
        Operator::ProjectKeep(pats) => {
            let keep = match_patterns(&rel.cols, pats.iter().map(|p| p.pattern.as_str()), true)?;
            select_columns(ctx, rel, keep)
        }
        Operator::ProjectRename(pairs) => project_rename(ctx, rel, pairs),
        Operator::ProjectReorder(pats) => project_reorder(ctx, rel, pats),
        Operator::Summarize { aggs, by, .. } => crate::aggs::summarize(ctx, rel, aggs, by, env),
        Operator::Sort(keys) => sort(ctx, rel, keys, env),
        Operator::Take(n) => take(ctx, rel, n, env),
        Operator::Top { count, key, .. } => {
            let rel = sort(ctx, rel, std::slice::from_ref(key), env)?;
            take(ctx, rel, count, env)
        }
        Operator::Count { name } => count(ctx, rel, name.as_deref().unwrap_or("Count")),
        Operator::Distinct(exprs) => distinct(ctx, rel, exprs, env),
        Operator::Join { params, right, on } => crate::join::join(ctx, rel, params, right, on, env),
        Operator::Lookup { params, right, on } => crate::join::lookup(ctx, rel, params, right, on, env),
        Operator::Union { params, tables } => crate::join::union(ctx, Some(rel), params, tables, env),
        Operator::As { name, .. } => {
            let t = ctx.add_cte(name, rel, false);
            ctx.as_names.push((name.clone(), t.clone()));
            Ok(ctx.rel_from_tableref(&t))
        }
        Operator::Sample(n) => {
            let n = ctx.expr(n, &Scope::empty(), env)?;
            let mut r = rel.passthrough(ctx);
            if r.sel.limit.is_some() {
                r = r.wrap(ctx);
            }
            r.sel.order_by = vec![ctx.d.random()];
            r.sel.limit = Some(n.sql);
            r.order.clear();
            Ok(r)
        }
        Operator::GetSchema => getschema(ctx, &rel),
        Operator::Render { .. } => Ok(rel),
        Operator::MvExpand { params, items, limit } => crate::advanced::mv_expand(ctx, rel, params, items, limit.as_ref(), env),
        Operator::Parse { params, expr, parts, filter } => crate::advanced::parse(ctx, rel, params, expr, parts, *filter, env),
        other => crate::advanced::apply_advanced(ctx, rel, other, env),
    }
}

pub(crate) fn source(ctx: &mut Ctx, op: &Operator, env: &Env) -> Result<Rel> {
    match op {
        Operator::Print(items) => print(ctx, items, env),
        Operator::DataTable { columns, values } => datatable(ctx, columns, values, env),
        Operator::Range { name, from, to, step } => range(ctx, name, from, to, step, env),
        Operator::Union { params, tables } => crate::join::union(ctx, None, params, tables, env),
        other => crate::advanced::source_advanced(ctx, other, env),
    }
}

// ---------------------------------------------------------------------- basic operators

fn where_(ctx: &mut Ctx, rel: Rel, e: &Expr, env: &Env) -> Result<Rel> {
    let mut rel = rel.passthrough(ctx);
    let order = rel.order_sql();
    let cond = {
        let mut scope = Scope::rows(&rel.cols);
        scope.order = &order;
        ctx.expr(e, &scope, env)?
    };
    if cond.agg {
        return err("aggregate functions are not allowed in 'where'");
    }
    let cond = ctx.to_bool(cond);
    if cond.window {
        // compute the window expression first, then filter on it
        let mut items = rel.identity_items();
        items.push(Item { sql: cond.sql, alias: "__kql_cond".into() });
        let mut cols = rel.cols.clone();
        cols.push(Column { name: "__kql_cond".into(), ty: KqlType::Bool });
        let keep = rel.cols.clone();
        let mut r = rel.project(ctx, items, cols).wrap(ctx);
        r.sel.filters.push("__kql_cond".into());
        let ident = keep.iter().map(|c| Item { sql: quote_ident(&c.name), alias: c.name.clone() }).collect();
        return Ok(r.project(ctx, ident, keep));
    }
    rel.sel.filters.push(cond.sql);
    Ok(rel)
}

/// `extend` (and `serialize x = ...`): adds or replaces columns.
pub(crate) fn extend(ctx: &mut Ctx, rel: Rel, items: &[NamedExpr], env: &Env, _serialize: bool) -> Result<Rel> {
    let rel = rel.passthrough(ctx);
    if rel.sel.limit.is_some() {
        let wrapped = rel.wrap(ctx);
        return extend(ctx, wrapped, items, env, _serialize);
    }
    let order = rel.order_sql();
    let mut out_items = rel.identity_items();
    let mut cols = rel.cols.clone();
    let mut defined: Vec<(String, String, KqlType)> = Vec::new();
    let mut defaults = DefaultNames::default();
    for ne in items {
        let t = {
            let mut scope = Scope::rows(&rel.cols);
            scope.order = &order;
            scope.extra = &defined;
            ctx.expr(&ne.expr, &scope, env)?
        };
        if t.agg {
            return err("aggregate functions are only allowed in 'summarize'");
        }
        let names: Vec<String> = if !ne.names.is_empty() {
            ne.names.clone()
        } else {
            vec![result_name(&ne.expr, false).unwrap_or_else(|| defaults.next(|n| cols.iter().any(|c| c.name == n)))]
        };
        if names.len() > 1 {
            return err("tuple assignment in 'extend' is not supported for this expression");
        }
        let name = names.into_iter().next().unwrap();
        let sql = output_sql(ctx, &t);
        defined.retain(|(n, _, _)| *n != name);
        defined.push((name.clone(), sql.clone(), t.ty));
        match cols.iter().position(|c| c.name == name) {
            Some(i) => {
                cols[i].ty = t.ty;
                out_items[i] = Item { sql, alias: name };
            }
            None => {
                cols.push(Column { name: name.clone(), ty: t.ty });
                out_items.push(Item { sql, alias: name });
            }
        }
    }
    Ok(rel.project(ctx, out_items, cols))
}

/// Constants in projections are cast to their Kusto type so the column type is exact.
pub(crate) fn output_sql(ctx: &Ctx, t: &TExpr) -> String {
    match &t.konst {
        Some(Const::Long(_)) | Some(Const::Real(_)) | Some(Const::Bool(_)) if !t.sql.starts_with("CAST(") => ctx.d.cast(&t.sql, t.ty),
        Some(Const::TimeSpan(_)) => ctx.d.cast(&t.sql, KqlType::TimeSpan),
        Some(Const::Str(_)) if t.ty == KqlType::String => ctx.d.cast(&t.sql, KqlType::String),
        _ => t.sql.clone(),
    }
}

fn project(ctx: &mut Ctx, rel: Rel, items: &[NamedExpr], env: &Env) -> Result<Rel> {
    let rel = rel.passthrough(ctx);
    let order = rel.order_sql();
    let mut out_items = Vec::new();
    let mut cols: Vec<Column> = Vec::new();
    let mut names = UniqueNames::default();
    let mut defaults = DefaultNames::default();
    let mut defined: Vec<(String, String, KqlType)> = Vec::new();
    for ne in items {
        let t = {
            let mut scope = Scope::rows(&rel.cols);
            scope.order = &order;
            scope.extra = &defined;
            ctx.expr(&ne.expr, &scope, env)?
        };
        if t.agg {
            return err("aggregate functions are only allowed in 'summarize'");
        }
        let name = match ne.name() {
            Some(n) => {
                if cols.iter().any(|c| c.name == n) {
                    return err(format!("duplicate column name '{n}' in project"));
                }
                names.add_existing(n);
                n.to_string()
            }
            None => {
                let base = result_name(&ne.expr, false).unwrap_or_else(|| defaults.next(|n| names.contains(n)));
                names.unique(&base)
            }
        };
        let sql = output_sql(ctx, &t);
        defined.push((name.clone(), sql.clone(), t.ty));
        cols.push(Column { name: name.clone(), ty: t.ty });
        out_items.push(Item { sql, alias: name });
    }
    Ok(rel.project(ctx, out_items, cols))
}

/// Matches `project-away`/`project-keep` patterns (with `*` wildcards).
fn match_patterns<'p>(cols: &[Column], pats: impl Iterator<Item = &'p str>, keep: bool) -> Result<Vec<Column>> {
    let pats: Vec<&str> = pats.collect();
    for p in &pats {
        if !p.contains('*') && !cols.iter().any(|c| c.name == *p) {
            return err(format!("unknown column '{p}'"));
        }
    }
    Ok(cols.iter().filter(|c| pats.iter().any(|p| wildcard(p, &c.name)) == keep).cloned().collect())
}

pub(crate) fn wildcard(pattern: &str, name: &str) -> bool {
    fn go(p: &[u8], n: &[u8]) -> bool {
        match p.first() {
            None => n.is_empty(),
            Some(b'*') => (0..=n.len()).any(|i| go(&p[1..], &n[i..])),
            Some(c) => n.first() == Some(c) && go(&p[1..], &n[1..]),
        }
    }
    go(pattern.as_bytes(), name.as_bytes())
}

pub(crate) fn select_columns(ctx: &mut Ctx, rel: Rel, keep: Vec<Column>) -> Result<Rel> {
    let items = keep.iter().map(|c| Item { sql: quote_ident(&c.name), alias: c.name.clone() }).collect();
    Ok(rel.project(ctx, items, keep))
}

fn project_rename(ctx: &mut Ctx, rel: Rel, pairs: &[(String, String)]) -> Result<Rel> {
    let mut cols = rel.cols.clone();
    let mut items = rel.identity_items();
    for (new, old) in pairs {
        let Some(i) = cols.iter().position(|c| c.name == *old) else {
            return err(format!("unknown column '{old}'"));
        };
        cols[i].name = new.clone();
        items[i].alias = new.clone();
    }
    let mut order = rel.order.clone();
    for o in &mut order {
        if let Some((new, _)) = pairs.iter().find(|(_, old)| *old == o.col) {
            o.col = new.clone();
        }
    }
    let mut r = rel;
    r.order.clear();
    let mut r = r.project(ctx, items, cols);
    r.order = order;
    Ok(r)
}

fn project_reorder(ctx: &mut Ctx, rel: Rel, pats: &[ast::NamePattern]) -> Result<Rel> {
    let mut first: Vec<Column> = Vec::new();
    for p in pats {
        let mut matched: Vec<Column> = rel.cols.iter().filter(|c| wildcard(&p.pattern, &c.name) && !first.contains(c)).cloned().collect();
        if matched.is_empty() && !p.pattern.contains('*') {
            return err(format!("unknown column '{}'", p.pattern));
        }
        match p.dir {
            Some(SortDir::Asc) => matched.sort_by_key(|c| c.name.to_lowercase()),
            Some(SortDir::Desc) => {
                matched.sort_by_key(|c| c.name.to_lowercase());
                matched.reverse();
            }
            None => {}
        }
        first.extend(matched);
    }
    let rest: Vec<Column> = rel.cols.iter().filter(|c| !first.contains(c)).cloned().collect();
    first.extend(rest);
    select_columns(ctx, rel, first)
}

pub(crate) fn sort(ctx: &mut Ctx, rel: Rel, keys: &[ast::OrderKey], env: &Env) -> Result<Rel> {
    let mut rel = if rel.sel.limit.is_some() { rel.wrap(ctx) } else { rel };
    let mut specs = Vec::new();
    let mut physical = Vec::new();
    for k in keys {
        let t = {
            let scope = Scope::rows(&rel.cols);
            ctx.expr(&k.expr, &scope, env)?
        };
        let desc = k.dir != Some(SortDir::Asc);
        let nulls_first = match k.nulls {
            Some(NullsOrder::First) => true,
            Some(NullsOrder::Last) => false,
            None => !desc,
        };
        let t = if t.ty == KqlType::Dynamic { ctx.to_string(t) } else { t };
        match rel.cols.iter().find(|c| quote_ident(&c.name) == t.sql) {
            Some(c) => specs.push(OrderSpec { col: c.name.clone(), desc, nulls_first }),
            None => {}
        }
        physical.push(format!("{} {} NULLS {}", t.sql, if desc { "DESC" } else { "ASC" }, if nulls_first { "FIRST" } else { "LAST" }));
    }
    if specs.len() == keys.len() {
        rel.order = specs;
        rel.sel.order_by.clear();
    } else {
        // expression keys: order physically in this select
        rel = rel.passthrough(ctx);
        rel.order.clear();
        rel.sel.order_by = physical;
    }
    Ok(rel)
}

fn take(ctx: &mut Ctx, rel: Rel, n: &Expr, env: &Env) -> Result<Rel> {
    let n = ctx.expr(n, &Scope::empty(), env)?;
    if !n.ty.is_numeric() {
        return err("take/limit requires a numeric count");
    }
    let mut rel = if rel.sel.limit.is_some() { rel.wrap(ctx) } else { rel };
    if rel.sel.order_by.is_empty() && !rel.order.is_empty() {
        rel.sel.order_by = rel.order_sql();
    }
    rel.sel.limit = Some(match n.long_const() {
        Some(v) => v.max(0).to_string(),
        None => n.sql,
    });
    Ok(rel)
}

fn count(ctx: &mut Ctx, rel: Rel, name: &str) -> Result<Rel> {
    let mut rel = rel.passthrough(ctx);
    rel.sel.order_by.clear();
    rel.order.clear();
    let item = Item { sql: "COUNT(*)".into(), alias: name.into() };
    Ok(rel.project(ctx, vec![item], vec![Column { name: name.into(), ty: KqlType::Long }]))
}

fn distinct(ctx: &mut Ctx, rel: Rel, exprs: &[Expr], env: &Env) -> Result<Rel> {
    let mut rel = rel.passthrough(ctx);
    rel.sel.order_by.clear();
    rel.order.clear();
    let (items, cols) = if matches!(exprs, [Expr::Star]) {
        (rel.identity_items(), rel.cols.clone())
    } else {
        let mut items = Vec::new();
        let mut cols = Vec::new();
        let mut names = UniqueNames::default();
        let mut defaults = DefaultNames::default();
        for e in exprs {
            let t = ctx.expr(e, &Scope::rows(&rel.cols), env)?;
            let base = result_name(e, false).unwrap_or_else(|| defaults.next(|n| names.contains(n)));
            let name = names.unique(&base);
            cols.push(Column { name: name.clone(), ty: t.ty });
            items.push(Item { sql: output_sql(ctx, &t), alias: name });
        }
        (items, cols)
    };
    let mut r = rel.project(ctx, items, cols);
    r.sel.distinct = true;
    Ok(r)
}

fn getschema(ctx: &mut Ctx, rel: &Rel) -> Result<Rel> {
    let cols = vec![
        Column { name: "ColumnName".into(), ty: KqlType::String },
        Column { name: "ColumnOrdinal".into(), ty: KqlType::Int },
        Column { name: "DataType".into(), ty: KqlType::String },
        Column { name: "ColumnType".into(), ty: KqlType::String },
    ];
    let rows: Vec<Vec<String>> = rel
        .cols
        .iter()
        .enumerate()
        .map(|(i, c)| vec![quote_str(&c.name), format!("CAST({i} AS {})", ctx.d.sql_type(KqlType::Int)), quote_str(c.ty.clr_name()), quote_str(c.ty.name())])
        .collect();
    Ok(values_rel(ctx, &cols, rows))
}

/// A relation from literal rows.
pub(crate) fn values_rel(ctx: &mut Ctx, cols: &[Column], rows: Vec<Vec<String>>) -> Rel {
    if rows.is_empty() {
        let items = cols.iter().map(|c| Item { sql: format!("CAST(NULL AS {})", ctx.d.sql_type(c.ty)), alias: c.name.clone() }).collect();
        let sel = Select { items: Some(items), filters: vec!["false".into()], ..Default::default() };
        return Rel::from_select(sel, cols.to_vec());
    }
    let alias = ctx.alias();
    let names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
    let body: Vec<String> = rows.iter().map(|r| format!("({})", r.join(", "))).collect();
    let from = From::Raw(format!("(VALUES {}) AS {alias}({})", body.join(", "), names.join(", ")));
    Rel::from_select(Select::star_from(from), cols.to_vec())
}

fn print(ctx: &mut Ctx, items: &[NamedExpr], env: &Env) -> Result<Rel> {
    let mut out = Vec::new();
    let mut cols: Vec<Column> = Vec::new();
    let mut names = UniqueNames::default();
    let mut defined: Vec<(String, String, KqlType)> = Vec::new();
    for (i, ne) in items.iter().enumerate() {
        let t = {
            let mut scope = Scope::empty();
            scope.extra = &defined;
            ctx.expr(&ne.expr, &scope, env)?
        };
        if t.agg {
            return err("aggregate functions are not allowed in 'print'");
        }
        let name = match ne.name() {
            Some(n) => n.to_string(),
            None => format!("print_{i}"),
        };
        let name = names.unique(&name);
        let sql = output_sql(ctx, &t);
        defined.push((name.clone(), sql.clone(), t.ty));
        cols.push(Column { name: name.clone(), ty: t.ty });
        out.push(Item { sql, alias: name });
    }
    let sel = Select { items: Some(out), ..Default::default() };
    Ok(Rel::from_select(sel, cols))
}

fn datatable(ctx: &mut Ctx, columns: &[ast::ColumnDecl], values: &[Expr], env: &Env) -> Result<Rel> {
    let cols: Vec<Column> = columns
        .iter()
        .map(|c| KqlType::from_name(&c.ty).map(|ty| Column { name: c.name.clone(), ty }).ok_or_else(|| crate::Error::new(format!("unknown type '{}'", c.ty))))
        .collect::<Result<_>>()?;
    if values.len() % cols.len() != 0 {
        return err(format!("datatable: {} values do not fill {} columns", values.len(), cols.len()));
    }
    let mut rows = Vec::new();
    for chunk in values.chunks(cols.len()) {
        let mut row = Vec::new();
        for (v, c) in chunk.iter().zip(&cols) {
            let t = ctx.expr(v, &Scope::empty(), env)?;
            let t = coerce_value(ctx, t, c.ty)?;
            row.push(ctx.d.cast(&t.sql, c.ty));
        }
        rows.push(row);
    }
    Ok(values_rel(ctx, &cols, rows))
}

/// Converts a literal to a declared column type (datatable, externaldata).
fn coerce_value(ctx: &mut Ctx, t: TExpr, ty: KqlType) -> Result<TExpr> {
    if t.ty == ty || t.is_null_const() || (t.ty.is_numeric() && ty.is_numeric()) {
        let t = if t.is_null_const() && ty == KqlType::String { TExpr::new("''", KqlType::String) } else { t };
        return Ok(if t.ty == ty || t.is_null_const() { t } else { ctx.convert(t, ty) });
    }
    if ty == KqlType::Dynamic {
        return Ok(ctx.to_dynamic(t));
    }
    err(format!("datatable: value of type {} does not match column type {ty}", t.ty))
}

fn range(ctx: &mut Ctx, name: &str, from: &Expr, to: &Expr, step: &Expr, env: &Env) -> Result<Rel> {
    let f = ctx.expr(from, &Scope::empty(), env)?;
    let t = ctx.expr(to, &Scope::empty(), env)?;
    let s = ctx.expr(step, &Scope::empty(), env)?;
    let alias = ctx.alias();
    let col = quote_ident(name);
    let d = ctx.d;
    let (from_item, item_sql, ty) = match f.ty {
        ty if ty.is_integer() && t.ty.is_integer() && s.ty.is_integer() => {
            (d.generate_series(&d.cast(&f.sql, KqlType::Long), &d.cast(&t.sql, KqlType::Long), &d.cast(&s.sql, KqlType::Long), &alias, &col), col.clone(), KqlType::Long)
        }
        KqlType::TimeSpan => (d.generate_series(&f.sql, &t.sql, &s.sql, &alias, &col), col.clone(), KqlType::TimeSpan),
        KqlType::DateTime if s.ty == KqlType::TimeSpan => {
            let (fu, tu) = (d.epoch_us(&f.sql), d.epoch_us(&t.sql));
            let step_us = format!("CAST(trunc({} / 10) AS BIGINT)", s.sql);
            let n = format!("CASE WHEN {step_us} = 0 THEN -1 ELSE CAST(floor(({tu} - {fu}) / CAST({step_us} AS DOUBLE)) AS BIGINT) END");
            let gs = d.generate_series("0", &n, "1", &alias, "i");
            (gs, d.ts_from_us(&format!("({fu} + {alias}.i * {step_us})")), KqlType::DateTime)
        }
        ty if ty.is_numeric() => {
            let (fr, tr, sr) = (d.cast(&f.sql, KqlType::Real), d.cast(&t.sql, KqlType::Real), d.cast(&s.sql, KqlType::Real));
            let n = format!("CASE WHEN {sr} = 0 THEN -1 ELSE CAST(floor(({tr} - {fr}) / {sr} + 1e-9) AS BIGINT) END");
            let gs = d.generate_series("0", &n, "1", &alias, "i");
            (gs, format!("({fr} + {alias}.i * {sr})"), KqlType::Real)
        }
        _ => return err(format!("range: unsupported types {}, {}, {}", f.ty, t.ty, s.ty)),
    };
    let sel = Select { items: Some(vec![Item { sql: item_sql, alias: name.to_string() }]), from: From::Raw(from_item), ..Default::default() };
    let mut rel = Rel::from_select(sel, vec![Column { name: name.to_string(), ty }]);
    // range produces ascending (or descending for a negative step) values in order
    let desc = matches!(s.konst, Some(Const::Long(v)) if v < 0) || matches!(s.konst, Some(Const::Real(v)) if v < 0.0);
    rel.order = vec![OrderSpec { col: name.to_string(), desc, nulls_first: false }];
    Ok(rel)
}

/// Builds `SELECT ... FROM (q) AS alias` with explicit items.
pub(crate) fn select_from_query(ctx: &mut Ctx, q: Query, items: Vec<Item>) -> Select {
    let alias = ctx.alias();
    Select { items: Some(items), from: From::Query(Box::new(q), alias), ..Default::default() }
}
