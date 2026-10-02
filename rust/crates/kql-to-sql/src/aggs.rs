//! `summarize` and aggregate functions.

use kql_parser::ast::{Arg, Expr, NamedExpr};

use crate::binder::{Ctx, Env, Rel};
use crate::expr::{Const, Scope, TExpr};
use crate::names::{result_name, DefaultNames, UniqueNames};
use crate::sql::{quote_ident, Item};
use crate::{err, Column, Dialect, KqlType, Result};

/// Is `name` an aggregate function?
pub(crate) fn is_aggregate(name: &str) -> bool {
    crate::catalog_data::lookup(&name.to_ascii_lowercase(), crate::catalog_data::FnKind::Aggregate).is_some()
}

pub(crate) fn summarize(ctx: &mut Ctx, rel: Rel, aggs: &[NamedExpr], by: &[NamedExpr], env: &Env) -> Result<Rel> {
    let mut rel = rel.passthrough(ctx);
    // the input order still matters to order-sensitive aggregates (make_list, ...)
    let input_order = rel.order_sql();
    rel.sel.order_by.clear();
    rel.order.clear();
    let input_cols = rel.cols.clone();
    let mut items = Vec::new();
    let mut cols: Vec<Column> = Vec::new();
    let mut names = UniqueNames::default();
    let mut defaults = DefaultNames::default();

    // group keys
    let mut key_cols = Vec::new();
    for ne in by {
        let t = ctx.expr(&ne.expr, &Scope::rows(&input_cols), env)?;
        if t.agg {
            return err("aggregates are not allowed in 'by' keys");
        }
        let t = if t.ty == KqlType::Dynamic { t } else { t };
        let base = match ne.name() {
            Some(n) => n.to_string(),
            None => result_name(&ne.expr, false).unwrap_or_else(|| defaults.next(|n| names.contains(n))),
        };
        let name = names.unique(&base);
        key_cols.push(name.clone());
        cols.push(Column { name: name.clone(), ty: t.ty });
        items.push(Item { sql: crate::ops::output_sql(ctx, &t), alias: name });
    }
    let nkeys = items.len();

    // aggregates
    for ne in aggs {
        if let Some(multi) = multi_column_aggregate(ctx, ne, &input_cols, &key_cols, env)? {
            for (base, t) in multi {
                let name = names.unique(&base);
                cols.push(Column { name: name.clone(), ty: t.ty });
                items.push(Item { sql: t.sql, alias: name });
            }
            continue;
        }
        let t = {
            let mut scope = Scope::rows(&input_cols);
            scope.aggregates = true;
            scope.order = &input_order;
            ctx.expr(&ne.expr, &scope, env)?
        };
        if !t.agg && t.konst.is_none() {
            return err("summarize: expression must contain an aggregate function");
        }
        let base = match ne.name() {
            Some(n) => n.to_string(),
            None => result_name(&ne.expr, true).unwrap_or_else(|| defaults.next(|n| names.contains(n))),
        };
        let name = names.unique(&base);
        cols.push(Column { name: name.clone(), ty: t.ty });
        items.push(Item { sql: crate::ops::output_sql(ctx, &t), alias: name });
    }
    if items.is_empty() {
        return err("summarize requires aggregates or group keys");
    }
    let group: Vec<String> = (1..=nkeys).map(|i| i.to_string()).collect();
    let mut r = rel.project(ctx, items, cols);
    r.serialized = false;
    r.sel.group_by = group;
    if nkeys == 0 {
        r.sel.group_by.clear();
    }
    Ok(r)
}

/// Aggregates that produce several columns: `arg_max`/`arg_min`, `take_any(a, b)`/`take_any(*)`,
/// `percentiles(x, p1, p2)`.
fn multi_column_aggregate(
    ctx: &mut Ctx,
    ne: &NamedExpr,
    cols: &[Column],
    keys: &[String],
    env: &Env,
) -> Result<Option<Vec<(String, TExpr)>>> {
    let Expr::Call { name, args } = &ne.expr else { return Ok(None) };
    let lname = name.to_ascii_lowercase();
    let scope = Scope::rows(cols);
    let expand = |args: &[Arg], skip: &[String]| -> Vec<Expr> {
        let mut out = Vec::new();
        for a in args {
            if matches!(a.expr, Expr::Star) {
                for c in cols {
                    if !skip.contains(&c.name) && !out.iter().any(|e: &Expr| matches!(e, Expr::Name(n) if *n == c.name))
                    {
                        out.push(Expr::Name(c.name.clone()));
                    }
                }
            } else {
                out.push(a.expr.clone());
            }
        }
        out
    };
    let rename = |out: Vec<(String, TExpr)>| -> Vec<(String, TExpr)> {
        if ne.names.is_empty() {
            return out;
        }
        out.into_iter().enumerate().map(|(i, (n, t))| (ne.names.get(i).cloned().unwrap_or(n), t)).collect()
    };
    match lname.as_str() {
        "arg_max" | "arg_min" | "argmax" | "argmin" => {
            if args.len() < 2 && lname.len() == 7 {
                return Ok(None);
            }
            let is_max = lname.contains("max");
            let by_expr = &args[0].expr;
            let by = ctx.expr(by_expr, &scope, env)?;
            let by_name =
                result_name(by_expr, false).unwrap_or_else(|| if is_max { "max".into() } else { "min".into() });
            let mut out = vec![(by_name.clone(), {
                let f = if is_max { "max" } else { "min" };
                TExpr::new(format!("{f}({})", by.sql), by.ty)
            })];
            let mut skip: Vec<String> = keys.to_vec();
            if let Expr::Name(n) = by_expr {
                skip.push(n.clone());
            }
            for e in expand(&args[1..], &skip) {
                let t = ctx.expr(&e, &scope, env)?;
                let n = result_name(&e, false).unwrap_or_else(|| "Column".into());
                out.push((n, TExpr::new(arg_extreme(ctx, &t.sql, &by.sql, is_max), t.ty)));
            }
            Ok(Some(rename(out)))
        }
        "take_any" | "any" if args.len() > 1 || matches!(args.first().map(|a| &a.expr), Some(Expr::Star)) => {
            let mut out = Vec::new();
            for e in expand(args, keys) {
                let t = ctx.expr(&e, &scope, env)?;
                let n = result_name(&e, false).unwrap_or_else(|| "Column".into());
                out.push((n, TExpr::new(any_value(ctx, &t.sql), t.ty)));
            }
            Ok(Some(rename(out)))
        }
        "percentilesw" => {
            if args.len() < 3 {
                return err("percentilesw() requires at least three arguments");
            }
            let x = ctx.expr(&args[0].expr, &scope, env)?;
            let w = ctx.expr(&args[1].expr, &scope, env)?;
            let xname = result_name(&args[0].expr, false).unwrap_or_default();
            let mut out = Vec::new();
            for a in &args[2..] {
                let p = ctx.expr(&a.expr, &Scope::empty(), env)?;
                let pv = match p.konst {
                    Some(Const::Long(v)) => v as f64,
                    Some(Const::Real(v)) => v,
                    _ => return err("percentilesw(): percentiles must be constants"),
                };
                out.push((
                    format!("percentile_{xname}_{}", format_percentile(pv)),
                    TExpr::new(weighted_percentile_sql(ctx, &x.sql, &w.sql, pv)?, x.ty),
                ));
            }
            Ok(Some(rename(out)))
        }
        "percentiles" | "percentiles_array" if lname == "percentiles" => {
            if args.len() < 2 {
                return err("percentiles() requires at least two arguments");
            }
            let x = ctx.expr(&args[0].expr, &scope, env)?;
            let xname = result_name(&args[0].expr, false).unwrap_or_default();
            let mut out = Vec::new();
            for a in &args[1..] {
                let p = ctx.expr(&a.expr, &Scope::empty(), env)?;
                let pv = match p.konst {
                    Some(Const::Long(v)) => v as f64,
                    Some(Const::Real(v)) => v,
                    _ => return err("percentiles(): percentiles must be constants"),
                };
                let label = format_percentile(pv);
                out.push((
                    format!("percentile_{xname}_{label}"),
                    TExpr::new(percentile_sql(ctx, &x.sql, pv), percentile_type(x.ty)),
                ));
            }
            Ok(Some(rename(out)))
        }
        _ => Ok(None),
    }
}

fn format_percentile(p: f64) -> String {
    if p.fract() == 0.0 {
        format!("{}", p as i64)
    } else {
        format!("{p}").replace('.', "_")
    }
}

fn percentile_type(t: KqlType) -> KqlType {
    t
}

pub(crate) fn percentile_sql(ctx: &Ctx, x: &str, p: f64) -> String {
    let frac = p / 100.0;
    match ctx.d.kind() {
        Dialect::DuckDb => format!("quantile_disc({x}, {frac})"),
        Dialect::Postgres => format!("percentile_disc({frac}) WITHIN GROUP (ORDER BY {x})"),
    }
}

pub(crate) fn arg_extreme(ctx: &Ctx, value: &str, by: &str, max: bool) -> String {
    match ctx.d.kind() {
        // when every `by` is null, Kusto still returns a row's values
        Dialect::DuckDb => format!(
            "CASE WHEN COUNT({by}) = 0 THEN first({value}) ELSE {}({value}, {by}) END",
            if max { "arg_max" } else { "arg_min" }
        ),
        Dialect::Postgres => {
            format!("(array_agg({value} ORDER BY {by} {} NULLS LAST))[1]", if max { "DESC" } else { "ASC" })
        }
    }
}

pub(crate) fn any_value(_ctx: &Ctx, x: &str) -> String {
    format!("any_value({x})")
}

/// Compiles an aggregate function call. Arguments are compiled in the (non-aggregate) row scope.
pub(crate) fn call(ctx: &mut Ctx, name: &str, args: &[Arg], scope: &Scope, env: &Env) -> Result<TExpr> {
    let lname = name.to_ascii_lowercase();
    let row_scope = Scope {
        cols: scope.cols,
        qual: scope.qual,
        join: None,
        aggregates: false,
        order: scope.order,
        extra: scope.extra,
        serialized: scope.serialized,
    };
    let mut a = Vec::new();
    for arg in args {
        if matches!(arg.expr, Expr::Star) {
            a.push(TExpr::new("*", KqlType::Dynamic));
            continue;
        }
        let t = ctx.expr(&arg.expr, &row_scope, env)?;
        if t.agg {
            return err(format!("{name}(): nested aggregates are not allowed"));
        }
        a.push(t);
    }
    let d = ctx.d;
    let n = a.len();
    let need = |min: usize, max: usize| -> Result<()> {
        if n < min || n > max {
            err(format!("{name}(): expected {min}..{max} arguments, got {n}"))
        } else {
            Ok(())
        }
    };
    let pred = |ctx: &Ctx, t: &TExpr| ctx.to_bool(t.clone()).sql;
    let num = |t: &TExpr| -> TExpr {
        if t.ty == KqlType::Dynamic {
            TExpr::new(d.try_cast(&d.json_to_text(&t.sql), KqlType::Real), KqlType::Real)
        } else {
            t.clone()
        }
    };
    let promoted = |t: KqlType| if t == KqlType::Int || t == KqlType::Bool { KqlType::Long } else { t };
    let real = d.sql_type(KqlType::Real);
    let (sql, ty) = match lname.as_str() {
        "count" => {
            need(0, 1)?;
            if n == 1 && a[0].ty == KqlType::Bool {
                (format!("COUNT(*) FILTER (WHERE {})", pred(ctx, &a[0])), KqlType::Long)
            } else {
                ("COUNT(*)".to_string(), KqlType::Long)
            }
        }
        "countif" => {
            need(1, 1)?;
            (format!("COUNT(*) FILTER (WHERE {})", pred(ctx, &a[0])), KqlType::Long)
        }
        "sum" | "sumif" => {
            need(if lname == "sum" { 1 } else { 2 }, if lname == "sum" { 1 } else { 2 })?;
            let x = num(&a[0]);
            let filter = if n == 2 { format!(" FILTER (WHERE {})", pred(ctx, &a[1])) } else { String::new() };
            let ty = promoted(x.ty);
            let zero = if ty == KqlType::TimeSpan || ty.is_integer() { "0".to_string() } else { d.real_literal(0.0) };
            let s = format!("SUM({}){filter}", x.sql);
            let s = if ty.is_integer() || ty == KqlType::TimeSpan { d.cast(&s, KqlType::Long) } else { s };
            (format!("COALESCE({s}, {zero})"), ty)
        }
        "avg" | "avgif" => {
            let x = num(&a[0]);
            let filter = if n == 2 { format!(" FILTER (WHERE {})", pred(ctx, &a[1])) } else { String::new() };
            if x.ty == KqlType::TimeSpan {
                (format!("CAST(AVG({}){filter} AS BIGINT)", x.sql), KqlType::TimeSpan)
            } else {
                (
                    format!("COALESCE(AVG(CAST({} AS {real})){filter}, {})", x.sql, d.real_literal(f64::NAN)),
                    KqlType::Real,
                )
            }
        }
        "min" | "max" | "minif" | "maxif" => {
            let f = if lname.starts_with("min") { "MIN" } else { "MAX" };
            let filter = if n == 2 { format!(" FILTER (WHERE {})", pred(ctx, &a[1])) } else { String::new() };
            let x = &a[0];
            (format!("{f}({}){filter}", x.sql), x.ty)
        }
        "dcount" | "dcountif" | "count_distinct" | "count_distinctif" => {
            let filter =
                if lname.ends_with("if") { format!(" FILTER (WHERE {})", pred(ctx, &a[1])) } else { String::new() };
            (format!("COUNT(DISTINCT {}){filter}", a[0].sql), KqlType::Long)
        }
        "make_list"
        | "makelist"
        | "make_list_if"
        | "makelist_if"
        | "make_set"
        | "makeset"
        | "make_set_if"
        | "makeset_if"
        | "make_list_with_nulls" => {
            let distinct = lname.contains("set");
            let is_if = lname.ends_with("_if");
            let with_nulls = lname == "make_list_with_nulls";
            let x = &a[0];
            let max_size = if is_if { a.get(2) } else { a.get(1) };
            let cond = if is_if { Some(pred(ctx, &a[1])) } else { None };
            (make_list_sql(ctx, x, cond.as_deref(), distinct, with_nulls, max_size, scope.order), KqlType::Dynamic)
        }
        "make_bag" | "make_bag_if" => {
            let x = &a[0];
            let v = if lname == "make_bag_if" {
                format!("CASE WHEN {} THEN {} END", pred(ctx, &a[1]), x.sql)
            } else {
                x.sql.clone()
            };
            (bag_merge_agg(ctx, &v), KqlType::Dynamic)
        }
        "take_any" | "any" | "take_anyif" | "anyif" => {
            let x = &a[0];
            let s = if lname.ends_with("if") {
                format!("any_value({}) FILTER (WHERE {})", x.sql, pred(ctx, &a[1]))
            } else {
                any_value(ctx, &x.sql)
            };
            (s, x.ty)
        }
        "arg_max" | "arg_min" | "argmax" | "argmin" => {
            // single-column form inside an expression: value of the maximized expression
            let f = if lname.contains("max") { "MAX" } else { "MIN" };
            (format!("{f}({})", a[0].sql), a[0].ty)
        }
        "percentile" => {
            need(2, 2)?;
            let p = match a[1].konst {
                Some(Const::Long(v)) => v as f64,
                Some(Const::Real(v)) => v,
                _ => return err("percentile(): the percentile must be a constant"),
            };
            (percentile_sql(ctx, &a[0].sql, p), percentile_type(a[0].ty))
        }
        "stdev" | "stdevif" | "stdevp" | "variance" | "varianceif" | "variancep" => {
            let f = match lname.as_str() {
                "stdev" | "stdevif" => "stddev_samp",
                "stdevp" => "stddev_pop",
                "variance" | "varianceif" => "var_samp",
                _ => "var_pop",
            };
            let filter =
                if lname.ends_with("if") { format!(" FILTER (WHERE {})", pred(ctx, &a[1])) } else { String::new() };
            let x = num(&a[0]);
            (format!("COALESCE({f}(CAST({} AS {real})){filter}, {})", x.sql, d.real_literal(0.0)), KqlType::Real)
        }
        "covariance" | "covariancep" | "covarianceif" | "covariancepif" => {
            let f = if lname.starts_with("covariancep") { "covar_pop" } else { "covar_samp" };
            let filter =
                if lname.ends_with("if") { format!(" FILTER (WHERE {})", pred(ctx, &a[2])) } else { String::new() };
            let (x, y) = (num(&a[0]), num(&a[1]));
            (
                format!(
                    "COALESCE({f}(CAST({} AS {real}), CAST({} AS {real})){filter}, {})",
                    x.sql,
                    y.sql,
                    d.real_literal(0.0)
                ),
                KqlType::Real,
            )
        }
        "variancepif" | "stdevpif" => {
            let f = if lname.starts_with("variance") { "var_pop" } else { "stddev_pop" };
            let x = num(&a[0]);
            (
                format!(
                    "COALESCE({f}(CAST({} AS {real})) FILTER (WHERE {}), {})",
                    x.sql,
                    pred(ctx, &a[1]),
                    d.real_literal(0.0)
                ),
                KqlType::Real,
            )
        }
        "percentilew" => {
            need(3, 3)?;
            let p = match a[2].konst {
                Some(Const::Long(v)) => v as f64,
                Some(Const::Real(v)) => v,
                _ => return err("percentilew(): the percentile must be a constant"),
            };
            (weighted_percentile_sql(ctx, &a[0].sql, &a[1].sql, p)?, a[0].ty)
        }
        "binary_all_and" | "binary_all_or" | "binary_all_xor" => {
            let f = match lname.as_str() {
                "binary_all_and" => "bit_and",
                "binary_all_or" => "bit_or",
                _ => "bit_xor",
            };
            (format!("{f}({})", d.cast(&a[0].sql, KqlType::Long)), KqlType::Long)
        }
        // hll sketches are represented exactly, as the set of distinct values; dcount_hll()
        // counts them and hll_merge() unions them
        "hll" | "hll_if" => {
            let cond = if lname == "hll_if" { Some(pred(ctx, &a[1])) } else { None };
            (make_list_sql(ctx, &a[0], cond.as_deref(), true, false, None, &[]), KqlType::Dynamic)
        }
        "hll_merge" => (make_list_sql(ctx, &a[0], None, true, false, None, &[]), KqlType::Dynamic),
        "tdigest" | "tdigest_merge" | "merge_tdigest" => {
            return err(format!("{name}() is not supported"));
        }
        _ => return err(format!("unsupported aggregate function '{name}'")),
    };
    let mut t = TExpr::new(sql, ty);
    t.agg = true;
    Ok(t)
}

fn bag_merge_agg(ctx: &Ctx, v: &str) -> String {
    match ctx.d.kind() {
        // merge all object values; the first occurrence of a key wins
        Dialect::DuckDb => format!(
            "COALESCE(list_reduce(list({v}) FILTER (WHERE json_type({v}) = 'OBJECT'), (acc, x) -> json_merge_patch(x, acc)), CAST('{{}}' AS JSON))"
        ),
        Dialect::Postgres => format!(
            "COALESCE((SELECT jsonb_object_agg(k, val) FROM (SELECT DISTINCT ON (e.key) e.key AS k, e.value AS val FROM unnest(array_agg({v}) FILTER (WHERE jsonb_typeof({v}) = 'object')) WITH ORDINALITY AS b(bag, ord), jsonb_each(b.bag) AS e ORDER BY e.key, b.ord) AS kv), '{{}}'::jsonb)"
        ),
    }
}

/// `quote_ident` re-export for callers composing aggregate SQL.
#[allow(dead_code)]
pub(crate) fn qi(n: &str) -> String {
    quote_ident(n)
}

/// `make_list`/`make_set` and variants. Dynamic arrays are flattened into the result (Kusto
/// appends array elements), nulls are skipped unless `with_nulls`, the input order is kept.
fn make_list_sql(
    ctx: &Ctx,
    x: &TExpr,
    cond: Option<&str>,
    distinct: bool,
    with_nulls: bool,
    max_size: Option<&TExpr>,
    order: &[String],
) -> String {
    let d = ctx.d;
    let v = if x.ty == KqlType::Dynamic { x.sql.clone() } else { ctx.to_dynamic(x.clone()).sql };
    let order_by = if order.is_empty() { String::new() } else { format!(" ORDER BY {}", order.join(", ")) };
    let mut filters = Vec::new();
    // dynamic nulls are kept (Kusto keeps null elements of dynamic input)
    if !with_nulls && x.ty != KqlType::Dynamic {
        filters.push(format!("{} IS NOT NULL", x.sql));
    }
    if let Some(c) = cond {
        filters.push(c.to_string());
    }
    let filter = if filters.is_empty() { String::new() } else { format!(" FILTER (WHERE {})", filters.join(" AND ")) };
    match d.kind() {
        Dialect::DuckDb => {
            let mut list = if x.ty == KqlType::Dynamic {
                let elems = format!("CASE WHEN json_type({v}) = 'ARRAY' THEN CAST({v} AS JSON[]) ELSE [{v}] END");
                format!("flatten(list({elems}{order_by}){filter})")
            } else {
                format!("list({v}{order_by}){filter}")
            };
            if distinct {
                list = format!("list_filter({list}, (e, i) -> list_position({list}, e) = i)");
            }
            if let Some(n) = max_size {
                list = format!("list_slice({list}, 1, {})", d.cast(&n.sql, KqlType::Long));
            }
            format!("COALESCE(to_json({list}), CAST('[]' AS JSON))")
        }
        Dialect::Postgres => {
            let dynamic = x.ty == KqlType::Dynamic;
            if !dynamic && !distinct && max_size.is_none() {
                return format!("COALESCE(jsonb_agg({v}{order_by}){filter}, CAST('[]' AS jsonb))");
            }
            // collect in order, then post-process the array in a scalar subquery: flatten dynamic
            // arrays, keep the first occurrence of each value, cut at the maximum size
            let collected = format!("array_agg({v}{order_by}){filter}");
            let flat = if dynamic {
                format!(
                    "SELECT _y.__kql_v, row_number() OVER (ORDER BY _m.__kql_i, _y.__kql_j) AS __kql_n FROM unnest({collected}) WITH ORDINALITY AS _m(__kql_x, __kql_i), \
                     LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(_m.__kql_x) = 'array' THEN _m.__kql_x ELSE jsonb_build_array(_m.__kql_x) END) WITH ORDINALITY AS _y(__kql_v, __kql_j)"
                )
            } else {
                format!(
                    "SELECT _m.__kql_v, _m.__kql_n FROM unnest({collected}) WITH ORDINALITY AS _m(__kql_v, __kql_n)"
                )
            };
            let rows = if distinct {
                format!("SELECT _f.__kql_v, min(_f.__kql_n) AS __kql_n FROM ({flat}) AS _f GROUP BY _f.__kql_v")
            } else {
                flat
            };
            let rows = match max_size {
                Some(n) => {
                    format!("SELECT * FROM ({rows}) AS _l ORDER BY _l.__kql_n LIMIT {}", d.cast(&n.sql, KqlType::Long))
                }
                None => rows,
            };
            format!(
                "(SELECT COALESCE(jsonb_agg(_s.__kql_v ORDER BY _s.__kql_n), CAST('[]' AS jsonb)) FROM ({rows}) AS _s)"
            )
        }
    }
}

/// Weighted nearest-rank percentile: the smallest value whose cumulative weight reaches p% of the
/// total weight.
fn weighted_percentile_sql(ctx: &Ctx, x: &str, w: &str, p: f64) -> Result<String> {
    match ctx.d.kind() {
        Dialect::DuckDb => Ok(format!(
            "list_extract(list_filter(list_transform(list_sort(list([{x}, {w}]) FILTER (WHERE {x} IS NOT NULL)), (e, i) -> [e[1], list_sum(list_transform(list_slice(list_sort(list([{x}, {w}]) FILTER (WHERE {x} IS NOT NULL)), 1, i), z -> z[2]))]), e -> e[2] >= {p} / 100.0 * sum({w}) FILTER (WHERE {x} IS NOT NULL)), 1)[1]"
        )),
        Dialect::Postgres => err("weighted percentiles are not supported for PostgreSQL yet"),
    }
}
