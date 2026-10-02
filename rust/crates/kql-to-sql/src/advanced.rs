//! `mv-expand`, `parse` and less common operators.

use kql_parser::ast::{self, Expr, MvExpandItem, OpParam, Operator, ParsePart};

use crate::binder::{Ctx, Env, Rel};
use crate::expr::{Scope, TExpr};
use crate::names::result_name;
use crate::sql::{quote_ident, quote_str, Item, Select};
use crate::{err, Column, KqlType, Result};

fn param_word(params: &[OpParam], name: &str) -> Option<String> {
    params.iter().find(|p| p.name == name).and_then(|p| match &p.value {
        Expr::Name(n) => Some(n.clone()),
        Expr::Literal(ast::Literal::String(s)) => Some(s.clone()),
        _ => None,
    })
}

/// The innermost identifier of an expression (`d.a.b` -> `b`, `tostring(x)` -> `x`).
fn inner_identifier(e: &Expr) -> Option<String> {
    match e {
        Expr::Name(n) => Some(n.clone()),
        Expr::Paren(x) => inner_identifier(x),
        Expr::Member { name, .. } => Some(name.clone()),
        Expr::Index { expr, .. } => inner_identifier(expr),
        Expr::Call { args, .. } => args.first().and_then(|a| inner_identifier(&a.expr)),
        _ => None,
    }
}

pub(crate) fn mv_expand(ctx: &mut Ctx, rel: Rel, params: &[OpParam], items: &[MvExpandItem], limit: Option<&Expr>, env: &Env) -> Result<Rel> {
    let mut rel = rel;
    rel.order.clear();
    let rel = rel.passthrough(ctx);
    let bag_as_array = param_word(params, "bagexpansion").or_else(|| param_word(params, "kind")).is_some_and(|b| b.eq_ignore_ascii_case("array"));
    let index_col = param_word(params, "with_itemindex");
    let d = ctx.d;

    // (output name, replaces an input column?, target type)
    let mut specs: Vec<(String, bool, KqlType)> = Vec::new();
    // step 1: compute each expanded value once, as a helper column
    let mut items1 = rel.identity_items();
    let mut cols1 = rel.cols.clone();
    for (i, it) in items.iter().enumerate() {
        let t = ctx.expr(&it.expr, &Scope::rows(&rel.cols), env)?;
        if t.ty != KqlType::Dynamic {
            return err(format!("mv-expand requires a dynamic value, found {}", t.ty));
        }
        let (name, replaces) = match &it.name {
            Some(n) => (n.clone(), rel.cols.iter().any(|c| c.name == *n)),
            None => match inner_identifier(&it.expr).or_else(|| result_name(&it.expr, false)) {
                Some(n) => {
                    let exists = rel.cols.iter().any(|c| c.name == n);
                    (n, exists)
                }
                None => return err("mv-expand: cannot infer a column name; use name = expr"),
            },
        };
        let ty = match &it.to_type {
            Some(tn) => KqlType::from_name(tn).ok_or_else(|| crate::Error::new(format!("unknown type '{tn}'")))?,
            None => KqlType::Dynamic,
        };
        specs.push((name, replaces, ty));
        let h = format!("__kql_mv{i}");
        items1.push(Item { sql: t.sql, alias: h.clone() });
        cols1.push(Column { name: h, ty: KqlType::Dynamic });
    }
    let in_cols = rel.cols.clone();
    let r1 = rel.project(ctx, items1, cols1).wrap(ctx);

    // step 2: expand. Values, keys (bags) and positions are unnested in the select list, which
    // keeps element order and expands several columns in parallel (shorter ones padded with null).
    let mut items2: Vec<Item> = r1.cols.iter().map(|c| Item { sql: quote_ident(&c.name), alias: c.name.clone() }).collect();
    let mut cols2 = r1.cols.clone();
    for i in 0..specs.len() {
        let h = format!("__kql_mv{i}");
        let (vals, keys) = match d.kind() {
            crate::Dialect::DuckDb => (
                format!(
                    "CASE WHEN json_type({h}) = 'ARRAY' THEN CAST({h} AS JSON[]) WHEN json_type({h}) = 'OBJECT' THEN list_transform(json_keys({h}), k -> json_extract({h}, '$.\"' || replace(k, '\"', '\\\"') || '\"')) ELSE [{h}] END"
                ),
                format!("CASE WHEN json_type({h}) = 'OBJECT' THEN json_keys({h}) END"),
            ),
            crate::Dialect::Postgres => (
                format!(
                    "CASE WHEN jsonb_typeof({h}) = 'array' THEN ARRAY(SELECT jsonb_array_elements({h})) WHEN jsonb_typeof({h}) = 'object' THEN ARRAY(SELECT v FROM jsonb_each({h}) AS e(k, v)) ELSE ARRAY[{h}] END"
                ),
                format!("CASE WHEN jsonb_typeof({h}) = 'object' THEN ARRAY(SELECT k FROM jsonb_each({h}) AS e(k, v)) END"),
            ),
        };
        let len = match d.kind() {
            crate::Dialect::DuckDb => format!("len({vals})"),
            crate::Dialect::Postgres => format!("cardinality({vals})"),
        };
        let range = match d.kind() {
            crate::Dialect::DuckDb => format!("range(CAST({len} AS BIGINT))"),
            crate::Dialect::Postgres => format!("ARRAY(SELECT generate_series(0, {len} - 1))"),
        };
        items2.push(Item { sql: format!("unnest({vals})"), alias: format!("__kql_v{i}") });
        items2.push(Item { sql: format!("unnest({keys})"), alias: format!("__kql_k{i}") });
        items2.push(Item { sql: format!("unnest({range})"), alias: format!("__kql_i{i}") });
        cols2.push(Column { name: format!("__kql_v{i}"), ty: KqlType::Dynamic });
        cols2.push(Column { name: format!("__kql_k{i}"), ty: KqlType::String });
        cols2.push(Column { name: format!("__kql_i{i}"), ty: KqlType::Long });
    }
    // r1 is a fresh `SELECT * FROM (...)` wrapper, so its FROM can take the unnest projection
    let r2 = Rel::from_select(Select { items: Some(items2), from: r1.sel.from, ..Default::default() }, cols2);
    let mut r3 = r2.wrap(ctx);

    // step 3: shape the expanded values
    let mut out_items: Vec<Item> = Vec::new();
    let mut cols: Vec<Column> = Vec::new();
    for c in &in_cols {
        out_items.push(Item { sql: quote_ident(&c.name), alias: c.name.clone() });
        cols.push(c.clone());
    }
    for (i, (name, replaces, _)) in specs.iter().enumerate() {
        let (v, k) = (format!("__kql_v{i}"), format!("__kql_k{i}"));
        let bag_item = if bag_as_array { d.json_array(&[d.to_json(&k), v.clone()]) } else { d.json_object(&[(k.clone(), v.clone())]) };
        let value = format!("CASE WHEN {k} IS NOT NULL THEN {bag_item} WHEN {} = 'null' THEN NULL ELSE {v} END", d.json_type(&v));
        // typed conversion happens in a final step over the plain column
        if *replaces {
            let j = cols.iter().position(|c| c.name == *name).unwrap();
            out_items[j].sql = value;
            cols[j].ty = KqlType::Dynamic;
        } else {
            out_items.push(Item { sql: value, alias: name.clone() });
            cols.push(Column { name: name.clone(), ty: KqlType::Dynamic });
        }
    }
    if let Some(ic) = index_col {
        out_items.push(Item { sql: "__kql_i0".into(), alias: ic.clone() });
        cols.push(Column { name: ic, ty: KqlType::Long });
    }
    if let Some(l) = limit {
        let n = ctx.expr(l, &Scope::empty(), env)?;
        r3.sel.filters.push(format!("(__kql_i0 < {})", n.sql));
    }
    let r4 = r3.project(ctx, out_items, cols.clone());
    if specs.iter().all(|(_, _, ty)| *ty == KqlType::Dynamic) {
        return Ok(r4);
    }
    let mut r5 = r4.wrap(ctx);
    let mut items5 = r5.identity_items();
    for (name, _, ty) in &specs {
        if *ty != KqlType::Dynamic {
            let j = cols.iter().position(|c| c.name == *name).unwrap();
            items5[j].sql = ctx.convert(TExpr::new(quote_ident(name), KqlType::Dynamic), *ty).sql;
            cols[j].ty = *ty;
        }
    }
    r5.order.clear();
    Ok(r5.project(ctx, items5, cols))
}

pub(crate) fn parse(ctx: &mut Ctx, rel: Rel, params: &[OpParam], expr: &Expr, parts: &[ParsePart], filter: bool, env: &Env) -> Result<Rel> {
    let kind = param_word(params, "kind").unwrap_or_else(|| "simple".into()).to_ascii_lowercase();
    let regex_mode = kind == "regex";
    let rel = rel.passthrough(ctx);
    let src = ctx.expr(expr, &Scope::rows(&rel.cols), env)?;
    let src = ctx.to_string(src);
    let mut re = String::new();
    let mut captures: Vec<(String, KqlType)> = Vec::new();
    for (i, p) in parts.iter().enumerate() {
        match p {
            ParsePart::Star => re.push_str(".*?"),
            ParsePart::Text(t) | ParsePart::Regex(t) => {
                if regex_mode {
                    re.push_str(t)
                } else {
                    re.push_str(&crate::regex::escape(t))
                }
            }
            ParsePart::Column { name, ty } => {
                let ty = match ty {
                    Some(t) => KqlType::from_name(t).ok_or_else(|| crate::Error::new(format!("unknown type '{t}'")))?,
                    None => KqlType::String,
                };
                let last = !parts[i + 1..].iter().any(|p| matches!(p, ParsePart::Column { .. } | ParsePart::Text(_) | ParsePart::Regex(_)));
                re.push_str(match ty {
                    KqlType::Int | KqlType::Long => r"(-?\d+)",
                    KqlType::Real | KqlType::Decimal => r"(-?\d+\.?\d*(?:[eE][+-]?\d+)?)",
                    KqlType::Bool => "(true|false|True|False|TRUE|FALSE)",
                    _ if regex_mode || last => "(.*)",
                    _ => "(.*?)",
                });
                captures.push((name.clone(), ty));
            }
        }
    }
    let flags = param_word(params, "flags").unwrap_or_default();
    let re = if flags.is_empty() { re } else { format!("(?{}){re}", flags.to_ascii_lowercase().replace('u', "U")) };
    let re = crate::regex::translate(&re, ctx.d.kind());
    let re_sql = quote_str(&re);
    let matched = ctx.d.regex_match(&src.sql, &re_sql);
    let mut items = rel.identity_items();
    let mut cols = rel.cols.clone();
    for (i, (name, ty)) in captures.iter().enumerate() {
        let raw = TExpr::new(ctx.d.regex_extract(&src.sql, &re_sql, i as u32 + 1), KqlType::String);
        let v = if *ty == KqlType::String {
            format!("CASE WHEN {matched} THEN {} ELSE '' END", raw.sql)
        } else {
            let c = ctx.convert(raw, *ty);
            format!("CASE WHEN {matched} THEN {} END", c.sql)
        };
        match cols.iter().position(|c| c.name == *name) {
            Some(j) => {
                items[j].sql = v;
                cols[j].ty = *ty;
            }
            None => {
                items.push(Item { sql: v, alias: name.clone() });
                cols.push(Column { name: name.clone(), ty: *ty });
            }
        }
    }
    if filter {
        // parse-where keeps only rows where the pattern matched and typed captures converted
        let mut conds = vec![matched.clone()];
        for (name, ty) in &captures {
            if *ty != KqlType::String {
                let i = cols.iter().position(|c| c.name == *name).unwrap();
                conds.push(format!("({} IS NOT NULL)", items[i].sql));
            }
        }
        items.push(Item { sql: format!("({})", conds.join(" AND ")), alias: "__kql_match".into() });
        let visible = cols.clone();
        cols.push(Column { name: "__kql_match".into(), ty: KqlType::Bool });
        let mut r = rel.project(ctx, items, cols).wrap(ctx);
        r.sel.filters.push("__kql_match".into());
        let ident = visible.iter().map(|c| Item { sql: quote_ident(&c.name), alias: c.name.clone() }).collect();
        return Ok(r.project(ctx, ident, visible));
    }
    Ok(rel.project(ctx, items, cols))
}

pub(crate) fn apply_advanced(ctx: &mut Ctx, rel: Rel, op: &Operator, env: &Env) -> Result<Rel> {
    use crate::{op_evaluate, op_search, op_series, op_subquery};
    match op {
        Operator::MakeSeries { params, aggs, on, from, to, step, by } => {
            op_series::make_series(ctx, rel, params, aggs, on, from.as_ref(), to.as_ref(), step, by, env)
        }
        Operator::Scan { order_by, partition_by, declare, steps } => op_series::scan(ctx, rel, order_by, partition_by, declare, steps, env),
        Operator::Search { params, tables, predicate } => {
            if !tables.is_empty() {
                return err("search: 'in (tables)' is only valid when search starts the query");
            }
            op_search::search(ctx, Some(rel), params, tables, predicate, env)
        }
        Operator::ParseKv { expr, columns, params } => op_search::parse_kv(ctx, rel, expr, columns, params, env),
        Operator::MvApply { params, items, limit, context_id, body } => {
            op_subquery::mv_apply(ctx, rel, params, items, limit.as_ref(), context_id.as_deref(), body, env)
        }
        Operator::Partition { params, by, body } => op_subquery::partition(ctx, rel, params, by, body, env),
        Operator::TopNested(levels) => op_subquery::top_nested(ctx, rel, levels, env),
        Operator::TopHitters { count, of, by } => op_subquery::top_hitters(ctx, rel, count, of, by.as_ref(), env),
        Operator::SampleDistinct { count, of } => op_subquery::sample_distinct(ctx, rel, count, of, env),
        Operator::Reduce { by, params } => op_subquery::reduce(ctx, rel, by, params, env),
        Operator::Fork(branches) => op_subquery::fork(ctx, rel, branches, env),
        Operator::Facet { by, with } => op_subquery::facet(ctx, rel, by, with.as_deref(), env),
        Operator::Evaluate { params, name, args } => op_evaluate::evaluate(ctx, Some(rel), params, name, args, env),
        Operator::Invoke { name, args } => {
            // `T | invoke f(args)` calls f with T as its first (tabular) argument
            let t = ctx.add_cte("invoke_input", rel, false);
            let tmp = format!("__kql_invoke_{}", ctx.ctes.len());
            let env2 = env.with(&tmp, crate::binder::Binding::Tabular(t));
            let mut all = vec![ast::Arg::positional(Expr::Name(tmp))];
            all.extend(args.iter().cloned());
            ctx.tabular(&Expr::Call { name: name.clone(), args: all }, &env2)
        }
        Operator::Consume => {
            let mut r = rel.passthrough(ctx);
            r.sel.filters.push("false".into());
            Ok(r)
        }
        other => err(format!("the '{}' operator is not supported yet", other.keyword())),
    }
}

pub(crate) fn source_advanced(ctx: &mut Ctx, op: &Operator, env: &Env) -> Result<Rel> {
    use crate::{op_evaluate, op_search};
    match op {
        Operator::Search { params, tables, predicate } => op_search::search(ctx, None, params, tables, predicate, env),
        Operator::Evaluate { params, name, args } => op_evaluate::evaluate(ctx, None, params, name, args, env),
        Operator::ExternalData { columns, uris, props } => op_evaluate::externaldata(ctx, columns, uris, props, env),
        other => err(format!("the '{}' operator is not supported yet", other.keyword())),
    }
}
