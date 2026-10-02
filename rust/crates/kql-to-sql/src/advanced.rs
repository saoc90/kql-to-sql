//! `mv-expand`, `parse` and less common operators.

use kql_parser::ast::{self, Expr, MvExpandItem, OpParam, Operator, ParsePart};

use crate::binder::{Ctx, Env, Rel};
use crate::expr::{Scope, TExpr};
use crate::names::result_name;
use crate::sql::{quote_ident, quote_str, From, Item, Select};
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
    let t_alias = ctx.alias();
    let d = ctx.d;

    // (output name, replaces existing column?, source dynamic expr over t_alias, target type)
    let mut specs = Vec::new();
    for it in items {
        let t = {
            let mut scope = Scope::rows(&rel.cols);
            scope.qual = Some(&t_alias);
            ctx.expr(&it.expr, &scope, env)?
        };
        let t = if t.ty == KqlType::Dynamic { t } else { ctx.to_dynamic(t) };
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
        specs.push((name, replaces, t, ty));
    }

    let e_alias = ctx.alias();
    let mut from = format!("({}) AS {t_alias}", rel.sel.clone().render());
    let mut values: Vec<String> = Vec::new();
    let mut limit_filter: Option<String> = None;
    let index_sql;
    if specs.len() == 1 {
        let src = &specs[0].2.sql;
        // arrays expand to elements; bags to single-key bags (or [key, value] pairs); other
        // values expand to themselves
        let arr = match d.kind() {
            crate::Dialect::DuckDb => format!(
                "CASE WHEN json_type({src}) IN ('ARRAY', 'OBJECT') THEN {src} ELSE json_array({src}) END"
            ),
            crate::Dialect::Postgres => format!(
                "CASE WHEN jsonb_typeof({src}) IN ('array', 'object') THEN {src} ELSE jsonb_build_array({src}) END"
            ),
        };
        let je = d.json_each(&arr, &e_alias);
        from.push_str(&format!(" CROSS JOIN LATERAL {}", je.from_item));
        let is_obj = match d.kind() {
            crate::Dialect::DuckDb => format!("json_type({src}) = 'OBJECT'"),
            crate::Dialect::Postgres => format!("jsonb_typeof({src}) = 'object'"),
        };
        let bag_item = if bag_as_array {
            d.json_array(&[d.to_json(&je.key), je.value.clone()])
        } else {
            d.json_object(&[(je.key.clone(), je.value.clone())])
        };
        values.push(format!("CASE WHEN {is_obj} THEN {bag_item} WHEN {} = 'null' THEN NULL ELSE {} END", d.json_type(&je.value), je.value));
        index_sql = format!("CASE WHEN {is_obj} THEN NULL ELSE {} END", je.index);
        if let Some(l) = limit {
            let n = ctx.expr(l, &Scope::empty(), env)?;
            limit_filter = Some(format!("(row_number_in_source < {})", n.sql).replace("row_number_in_source", &je.index));
        }
    } else {
        // parallel expansion, padded with nulls to the longest array
        let lens: Vec<String> = specs.iter().map(|(_, _, t, _)| format!("COALESCE({}, 0)", d.json_array_length(&t.sql))).collect();
        let g = e_alias.clone();
        from.push_str(&format!(" CROSS JOIN LATERAL {}", d.generate_series("0", &format!("greatest({}) - 1", lens.join(", ")), "1", &g, "i")));
        for (_, _, t, _) in &specs {
            values.push(d.json_get_index(&t.sql, &format!("{g}.i")));
        }
        index_sql = format!("{g}.i");
        if let Some(l) = limit {
            let n = ctx.expr(l, &Scope::empty(), env)?;
            limit_filter = Some(format!("({g}.i < {})", n.sql));
        }
    }

    let mut out_items: Vec<Item> = Vec::new();
    let mut cols: Vec<Column> = Vec::new();
    for c in &rel.cols {
        out_items.push(Item { sql: format!("{t_alias}.{}", quote_ident(&c.name)), alias: c.name.clone() });
        cols.push(c.clone());
    }
    for ((name, replaces, _, ty), v) in specs.iter().zip(values) {
        let tv = TExpr::new(v, KqlType::Dynamic);
        let v = if *ty == KqlType::Dynamic { tv } else { ctx.convert(tv, *ty) };
        if *replaces {
            let i = cols.iter().position(|c| c.name == *name).unwrap();
            out_items[i].sql = v.sql;
            cols[i].ty = *ty;
        } else {
            out_items.push(Item { sql: v.sql, alias: name.clone() });
            cols.push(Column { name: name.clone(), ty: *ty });
        }
    }
    if let Some(ic) = index_col {
        out_items.push(Item { sql: index_sql, alias: ic.clone() });
        cols.push(Column { name: ic, ty: KqlType::Long });
    }
    let sel = Select { items: Some(out_items), from: From::Raw(from), filters: limit_filter.into_iter().collect(), ..Default::default() };
    Ok(Rel::from_select(sel, cols))
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
