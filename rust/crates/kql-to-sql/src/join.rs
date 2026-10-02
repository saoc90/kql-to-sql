//! `join`, `lookup` and `union`.

use kql_parser::ast::{BinaryOp, Expr, OpParam};

use crate::binder::{Ctx, Env, Rel};
use crate::expr::{JoinSides, Scope};
use crate::names::UniqueNames;
use crate::sql::{quote_ident, quote_str, From, Item, Query, Select};
use crate::{err, Column, KqlType, Result};

fn param<'p>(params: &'p [OpParam], name: &str) -> Option<&'p Expr> {
    params.iter().find(|p| p.name == name).map(|p| &p.value)
}

fn param_word(params: &[OpParam], name: &str) -> Option<String> {
    match param(params, name)? {
        Expr::Name(n) => Some(n.to_ascii_lowercase()),
        Expr::Literal(kql_parser::ast::Literal::String(s)) => Some(s.to_ascii_lowercase()),
        Expr::Literal(kql_parser::ast::Literal::Bool(b)) => Some(b.to_string()),
        _ => None,
    }
}

/// Equality key pairs (left column, right column) of a join/lookup `on` clause.
fn join_keys(on: &[Expr], left: &[Column], right: &[Column]) -> Result<Vec<(String, String)>> {
    let mut keys = Vec::new();
    fn side(e: &Expr) -> Option<(&str, &str)> {
        match e {
            Expr::Member { expr, name } => match &**expr {
                Expr::Name(s) if s == "$left" || s == "$right" => Some((s.as_str(), name.as_str())),
                _ => None,
            },
            _ => None,
        }
    }
    fn walk(e: &Expr, keys: &mut Vec<(String, String)>) -> Result<()> {
        match e {
            Expr::Paren(x) => walk(x, keys),
            Expr::Name(n) => {
                keys.push((n.clone(), n.clone()));
                Ok(())
            }
            Expr::Binary { op: BinaryOp::And, left, right } => {
                walk(left, keys)?;
                walk(right, keys)
            }
            Expr::Binary { op: BinaryOp::Eq, left, right } => match (side(left), side(right)) {
                (Some(("$left", l)), Some(("$right", r))) | (Some(("$right", r)), Some(("$left", l))) => {
                    keys.push((l.to_string(), r.to_string()));
                    Ok(())
                }
                _ => err("join conditions must compare $left.<column> with $right.<column>"),
            },
            _ => err("join conditions must be column names or $left.x == $right.y equalities"),
        }
    }
    for e in on {
        walk(e, &mut keys)?;
    }
    for (l, r) in &keys {
        if !left.iter().any(|c| c.name == *l) {
            return err(format!("join: unknown column '{l}' on the left side"));
        }
        if !right.iter().any(|c| c.name == *r) {
            return err(format!("join: unknown column '{r}' on the right side"));
        }
    }
    if keys.is_empty() {
        return err("join requires an 'on' clause");
    }
    Ok(keys)
}

fn key_condition(keys: &[(String, String)], la: &str, ra: &str) -> String {
    keys.iter()
        .map(|(l, r)| format!("{la}.{} IS NOT DISTINCT FROM {ra}.{}", quote_ident(l), quote_ident(r)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Keeps one arbitrary row per key (innerunique's left side).
fn dedup(ctx: &mut Ctx, rel: Rel, keys: &[String]) -> Rel {
    let part = keys.iter().map(|k| quote_ident(k)).collect::<Vec<_>>().join(", ");
    let mut items = rel.identity_items();
    items.push(Item { sql: format!("ROW_NUMBER() OVER (PARTITION BY {part})"), alias: "__kql_rn".into() });
    let cols = rel.cols.clone();
    let mut with_rn = cols.clone();
    with_rn.push(Column { name: "__kql_rn".into(), ty: KqlType::Long });
    let mut r = rel.project(ctx, items, with_rn).wrap(ctx);
    r.sel.filters.push("(__kql_rn = 1)".into());
    let ident = cols.iter().map(|c| Item { sql: quote_ident(&c.name), alias: c.name.clone() }).collect();
    r.project(ctx, ident, cols)
}

fn sub(ctx: &mut Ctx, rel: Rel) -> String {
    let _ = ctx;
    format!("({})", rel.into_query().render())
}

fn padded(c: &Column, sql: String) -> String {
    if c.ty == KqlType::String {
        format!("COALESCE({sql}, '')")
    } else {
        sql
    }
}

pub(crate) fn join(ctx: &mut Ctx, left: Rel, params: &[OpParam], right: &Expr, on: &[Expr], env: &Env) -> Result<Rel> {
    let kind = param_word(params, "kind").unwrap_or_else(|| "innerunique".into());
    let right = ctx.tabular(right, env)?;
    let keys = join_keys(on, &left.cols, &right.cols)?;
    let (mut left, mut right) = (left, right);
    left.order.clear();
    right.order.clear();
    let (lcols, rcols) = (left.cols.clone(), right.cols.clone());
    let cond = key_condition(&keys, "L", "R");

    let semi = |ctx: &mut Ctx, outer: Rel, inner: Rel, oa: &str, ia: &str, negate: bool| -> Rel {
        let cols = outer.cols.clone();
        let items: Vec<Item> = cols.iter().map(|c| Item { sql: format!("{oa}.{}", quote_ident(&c.name)), alias: c.name.clone() }).collect();
        let not = if negate { "NOT " } else { "" };
        let (o, i) = (sub(ctx, outer), sub(ctx, inner));
        let sel = Select {
            items: Some(items),
            from: From::Raw(format!("{o} AS {oa}")),
            filters: vec![format!("({not}EXISTS (SELECT 1 FROM {i} AS {ia} WHERE {cond}))")],
            ..Default::default()
        };
        Rel::from_select(sel, cols)
    };
    match kind.as_str() {
        "leftsemi" => return Ok(semi(ctx, left, right, "L", "R", false)),
        "leftanti" | "anti" | "leftantisemi" => return Ok(semi(ctx, left, right, "L", "R", true)),
        "rightsemi" => return Ok(semi(ctx, right, left, "R", "L", false)),
        "rightanti" | "rightantisemi" => return Ok(semi(ctx, right, left, "R", "L", true)),
        _ => {}
    }
    let (sql_kind, pad_left, pad_right) = match kind.as_str() {
        "innerunique" => {
            let lk: Vec<String> = keys.iter().map(|(l, _)| l.clone()).collect();
            left = dedup(ctx, left, &lk);
            ("INNER JOIN", false, false)
        }
        "inner" => ("INNER JOIN", false, false),
        "leftouter" => ("LEFT JOIN", false, true),
        "rightouter" => ("RIGHT JOIN", true, false),
        "fullouter" => ("FULL JOIN", true, true),
        other => return err(format!("unsupported join kind '{other}'")),
    };
    let mut names = UniqueNames::default();
    let mut items = Vec::new();
    let mut cols = Vec::new();
    for c in &lcols {
        names.add_existing(&c.name);
        let sql = format!("L.{}", quote_ident(&c.name));
        items.push(Item { sql: if pad_left { padded(c, sql) } else { sql }, alias: c.name.clone() });
        cols.push(c.clone());
    }
    for c in &rcols {
        let name = names.unique(&c.name);
        let sql = format!("R.{}", quote_ident(&c.name));
        items.push(Item { sql: if pad_right { padded(c, sql) } else { sql }, alias: name.clone() });
        cols.push(Column { name, ty: c.ty });
    }
    let (l, r) = (sub(ctx, left), sub(ctx, right));
    let sel = Select { items: Some(items), from: From::Raw(format!("{l} AS L {sql_kind} {r} AS R ON {cond}")), ..Default::default() };
    Ok(Rel::from_select(sel, cols))
}

pub(crate) fn lookup(ctx: &mut Ctx, left: Rel, params: &[OpParam], right: &Expr, on: &[Expr], env: &Env) -> Result<Rel> {
    let kind = param_word(params, "kind").unwrap_or_else(|| "leftouter".into());
    let right = ctx.tabular(right, env)?;
    let keys = join_keys(on, &left.cols, &right.cols)?;
    let (lcols, rcols) = (left.cols.clone(), right.cols.clone());
    let cond = key_condition(&keys, "L", "R");
    let sql_kind = match kind.as_str() {
        "leftouter" => "LEFT JOIN",
        "inner" => "INNER JOIN",
        other => return err(format!("unsupported lookup kind '{other}'")),
    };
    let mut names = UniqueNames::default();
    let mut items = Vec::new();
    let mut cols = Vec::new();
    for c in &lcols {
        names.add_existing(&c.name);
        items.push(Item { sql: format!("L.{}", quote_ident(&c.name)), alias: c.name.clone() });
        cols.push(c.clone());
    }
    for c in &rcols {
        if keys.iter().any(|(_, r)| *r == c.name) {
            continue;
        }
        let name = names.unique(&c.name);
        let sql = format!("R.{}", quote_ident(&c.name));
        items.push(Item { sql: if sql_kind == "LEFT JOIN" { padded(c, sql) } else { sql }, alias: name.clone() });
        cols.push(Column { name, ty: c.ty });
    }
    let order = left.order.clone();
    let (l, r) = (sub(ctx, left), sub(ctx, right));
    let sel = Select { items: Some(items), from: From::Raw(format!("{l} AS L {sql_kind} {r} AS R ON {cond}")), ..Default::default() };
    let mut rel = Rel::from_select(sel, cols);
    rel.order = order;
    rel.order.clear();
    Ok(rel)
}

pub(crate) fn union(ctx: &mut Ctx, input: Option<Rel>, params: &[OpParam], tables: &[Expr], env: &Env) -> Result<Rel> {
    let kind = param_word(params, "kind").unwrap_or_else(|| "outer".into());
    let source_col = match param(params, "withsource") {
        Some(Expr::Name(n)) => Some(n.clone()),
        Some(Expr::Literal(kql_parser::ast::Literal::String(s))) => Some(s.clone()),
        Some(_) => return err("withsource requires a column name"),
        None => None,
    };
    let mut parts: Vec<(String, Rel)> = Vec::new();
    if let Some(r) = input {
        parts.push(("union_arg0".into(), r));
    }
    for t in tables {
        match t {
            Expr::Name(n) if n.contains('*') => {
                let mut names: Vec<String> = ctx.catalog.tables.iter().map(|(n, _)| n.clone()).filter(|tn| crate::ops::wildcard(n, tn)).collect();
                names.sort();
                for tn in names {
                    let r = ctx.table_ref(&tn, env)?;
                    parts.push((tn, r));
                }
            }
            _ => {
                let label = match t {
                    Expr::Name(n) => n.clone(),
                    _ => format!("union_arg{}", parts.len()),
                };
                let r = ctx.tabular(t, env)?;
                parts.push((label, r));
            }
        }
    }
    if parts.is_empty() {
        return err("union requires at least one table");
    }
    // column set: by name; a name with several types becomes name_type for each
    let mut order: Vec<String> = Vec::new();
    let mut types: Vec<(String, Vec<KqlType>)> = Vec::new();
    for (_, r) in &parts {
        for c in &r.cols {
            match types.iter_mut().find(|(n, _)| *n == c.name) {
                Some((_, ts)) => {
                    if !ts.contains(&c.ty) {
                        ts.push(c.ty)
                    }
                }
                None => {
                    order.push(c.name.clone());
                    types.push((c.name.clone(), vec![c.ty]));
                }
            }
        }
    }
    let mut out: Vec<(String, String, KqlType)> = Vec::new(); // (output name, source name, type)
    for (name, ts) in &types {
        if kind == "inner" && !parts.iter().all(|(_, r)| r.cols.iter().any(|c| c.name == *name)) {
            continue;
        }
        if ts.len() == 1 {
            out.push((name.clone(), name.clone(), ts[0]));
        } else {
            for t in ts {
                out.push((format!("{name}_{}", t.name()), name.clone(), *t));
            }
        }
    }
    let mut cols: Vec<Column> = out.iter().map(|(n, _, t)| Column { name: n.clone(), ty: *t }).collect();
    if let Some(sc) = &source_col {
        cols.insert(0, Column { name: sc.clone(), ty: KqlType::String });
    }
    let mut queries = Vec::new();
    for (label, r) in parts {
        let mut items = Vec::new();
        if let Some(sc) = &source_col {
            items.push(Item { sql: ctx.d.cast(&quote_str(&label), KqlType::String), alias: sc.clone() });
        }
        for (oname, src, ty) in &out {
            let sql = match r.cols.iter().find(|c| c.name == *src && c.ty == *ty) {
                Some(c) => quote_ident(&c.name),
                None if *ty == KqlType::String => "CAST('' AS VARCHAR)".replace("VARCHAR", ctx.d.sql_type(KqlType::String)),
                None => format!("CAST(NULL AS {})", ctx.d.sql_type(*ty)),
            };
            items.push(Item { sql, alias: oname.clone() });
        }
        let mut r = r;
        r.order.clear();
        let projected = r.project(ctx, items, cols.clone());
        queries.push(projected.into_query());
    }
    let q = if queries.len() == 1 { queries.pop().unwrap() } else { Query::SetOp { op: "UNION ALL", parts: queries } };
    let alias = ctx.alias();
    Ok(Rel::from_select(Select::star_from(From::Query(Box::new(q), alias)), cols))
}

#[allow(dead_code)]
fn unused(_: &JoinSides, _: &Scope) {}
