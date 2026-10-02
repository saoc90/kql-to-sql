//! Operators with sub-pipelines or nested grouping: `mv-apply`, `partition`, `fork`, `facet`, `top-nested`, `top-hitters`, `sample-distinct`, `reduce`.

use kql_parser::ast::{Arg, Expr, MvExpandItem, NullsOrder, OpParam, Operator, SortDir, TopNestedLevel};

use crate::binder::{Ctx, Env, OrderSpec, Rel};

const HIDDEN_INDEX: &str = "__kql_mv_index";
const HIDDEN_ROW: &str = "__kql_mv_row";
use crate::expr::{Scope, TExpr};
use crate::names::{result_name, DefaultNames, UniqueNames};
use crate::sql::{quote_ident, From, Item, Select};
use crate::{err, Column, KqlType, Result};

/// Runs a sub-pipeline over `rel`, which is correlated with the outer FROM item `outer`.
/// Operators that hoist their input into a CTE (`as`, nested `partition`, `top-nested`, ...)
/// cannot see the correlated row, so they are rejected inside the sub-pipeline.
fn run_body(ctx: &mut Ctx, rel: Rel, body: &[Operator], env: &Env, op_name: &str, outer: &str) -> Result<Rel> {
    let ncte = ctx.ctes.len();
    let mut r = rel;
    for op in body {
        r = crate::ops::apply(ctx, r, op, env)?;
    }
    let needle = format!("{outer}.");
    if ctx.ctes[ncte..].iter().any(|(_, q, _)| q.contains(&needle)) {
        return err(format!("{op_name}: operators that name or hoist their input (as, partition, top-nested, ...) are not supported inside its sub-query"));
    }
    Ok(r)
}

/// A string column read from an outer join side is never NULL.
fn non_null(c: &Column, sql: String) -> String {
    if c.ty == KqlType::String {
        format!("COALESCE({sql}, '')")
    } else {
        sql
    }
}

// ---------------------------------------------------------------------- mv-apply

/// `mv-apply items on (body)`: for every input row, the body runs over a sub-table holding the
/// row's columns with the expanded items (one row per element). The result is the row's columns
/// (those the body does not output) followed by the body's output, for each body output row.
///
/// SQL: `SELECT o.<cols>, s.<body cols> FROM (<input>) AS o CROSS JOIN LATERAL (<body over the
/// expansion of o's row>) AS s`.
pub(crate) fn mv_apply(
    ctx: &mut Ctx,
    rel: Rel,
    params: &[OpParam],
    items: &[MvExpandItem],
    limit: Option<&Expr>,
    _context_id: Option<&str>,
    body: &[Operator],
    env: &Env,
) -> Result<Rel> {
    if items.is_empty() {
        return err("mv-apply requires at least one expression to expand");
    }
    // The result keeps the input row order, and each row's sub-query rows in element order: a
    // hidden row number over the input (in its logical order) and the element index order the
    // output physically.
    let rel = rel.passthrough(ctx);
    let outer_order = rel.order_sql();
    let outer_cols = rel.cols.clone();
    let o = ctx.alias();
    let over = if outer_order.is_empty() { String::new() } else { format!("ORDER BY {}", outer_order.join(", ")) };
    let mut numbered = rel.identity_items();
    numbered.push(Item { sql: format!("ROW_NUMBER() OVER ({over})"), alias: HIDDEN_ROW.into() });
    let mut numbered_cols = outer_cols.clone();
    numbered_cols.push(Column::new(HIDDEN_ROW, KqlType::Long));
    let mut numbered = rel.project(ctx, numbered, numbered_cols);
    numbered.order.clear();
    numbered.sel.order_by.clear();
    let outer_sql = numbered.into_query().render();

    // the single outer row, as a FROM-less select over the lateral reference
    let row_items: Vec<Item> = outer_cols.iter().map(|c| Item { sql: format!("{o}.{}", quote_ident(&c.name)), alias: c.name.clone() }).collect();
    let row = Rel::from_select(Select { items: Some(row_items), ..Default::default() }, outer_cols.clone());
    // The sub-table is ordered by element position (make_list, top, row_number in the body see
    // the array order); without a user-visible item index, a hidden one carries that order.
    let user_index = params.iter().find(|p| p.name == "with_itemindex").and_then(|p| match &p.value {
        Expr::Name(n) => Some(n.clone()),
        Expr::Literal(kql_parser::ast::Literal::String(s)) => Some(s.clone()),
        _ => None,
    });
    let mut params = params.to_vec();
    let index = match &user_index {
        Some(n) => n.clone(),
        None => {
            params.push(OpParam { name: "with_itemindex".into(), value: Expr::Name(HIDDEN_INDEX.into()) });
            HIDDEN_INDEX.to_string()
        }
    };
    let mut expanded = crate::advanced::mv_expand(ctx, row, &params, items, limit, env)?;
    expanded.order = vec![OrderSpec { col: index.clone(), desc: false, nulls_first: false }];
    let sub = run_body(ctx, expanded, body, env, "mv-apply", &o)?;
    let sub_index = sub.cols.iter().any(|c| c.name == index).then(|| index.clone());
    let sub_cols: Vec<Column> = sub.cols.iter().filter(|c| user_index.is_some() || c.name != HIDDEN_INDEX).cloned().collect();
    let s = ctx.alias();
    let sub_sql = sub.into_query().render();

    let mut out_items = Vec::new();
    let mut cols = Vec::new();
    for c in &outer_cols {
        if sub_cols.iter().any(|sc| sc.name == c.name) {
            continue;
        }
        out_items.push(Item { sql: format!("{o}.{}", quote_ident(&c.name)), alias: c.name.clone() });
        cols.push(c.clone());
    }
    for c in &sub_cols {
        out_items.push(Item { sql: format!("{s}.{}", quote_ident(&c.name)), alias: c.name.clone() });
        cols.push(c.clone());
    }
    let mut order_by = vec![format!("{o}.{HIDDEN_ROW} ASC")];
    if let Some(ix) = sub_index {
        order_by.push(format!("{s}.{} ASC NULLS LAST", quote_ident(&ix)));
    }
    let from = From::Raw(format!("({outer_sql}) AS {o} CROSS JOIN LATERAL ({sub_sql}) AS {s}"));
    Ok(Rel::from_select(Select { items: Some(out_items), from, order_by, ..Default::default() }, cols))
}

// ---------------------------------------------------------------------- partition

/// `partition by Col (body)`: the body runs independently over the rows of every distinct value
/// of `Col`; the result is the union of all runs.
///
/// SQL: `SELECT s.* FROM (SELECT DISTINCT Col FROM T) AS pk CROSS JOIN LATERAL (<body over
/// T WHERE Col = pk.Col>) AS s`.
pub(crate) fn partition(ctx: &mut Ctx, rel: Rel, _params: &[OpParam], by: &Expr, body: &[Operator], env: &Env) -> Result<Rel> {
    let key = match by {
        Expr::Name(n) => n.clone(),
        Expr::Paren(x) => match &**x {
            Expr::Name(n) => n.clone(),
            _ => return err("partition: the partition key must be a column name"),
        },
        _ => return err("partition: the partition key must be a column name"),
    };
    if rel.col(&key).is_none() {
        return err(format!("partition: unknown column '{key}'"));
    }
    let mut rel = rel;
    rel.order.clear();
    let t = ctx.add_cte("partition_input", rel, false);
    let qk = quote_ident(&key);

    let ta = ctx.alias();
    let pk = ctx.alias();
    let part_sel = Select {
        from: From::Raw(format!("{} AS {ta}", t.from)),
        filters: vec![format!("({ta}.{qk} IS NOT DISTINCT FROM {pk}.{qk})")],
        ..Default::default()
    };
    let part = Rel::from_select(part_sel, t.cols.clone());
    let sub = run_body(ctx, part, body, env, "partition", &pk)?;
    let cols = sub.cols.clone();
    let s = ctx.alias();
    let sub_sql = sub.into_query().render();
    let items = cols.iter().map(|c| Item { sql: format!("{s}.{}", quote_ident(&c.name)), alias: c.name.clone() }).collect();
    let from = From::Raw(format!("(SELECT DISTINCT {qk} FROM {}) AS {pk} CROSS JOIN LATERAL ({sub_sql}) AS {s}", t.from));
    Ok(Rel::from_select(Select { items: Some(items), from, ..Default::default() }, cols))
}

// ---------------------------------------------------------------------- top-nested

struct Level {
    /// output name of the `of` column and of the aggregate column
    name: String,
    agg_name: String,
    key: TExpr,
    agg: TExpr,
    count: Option<String>,
    others: Option<TExpr>,
    desc: bool,
    nulls_first: bool,
}

/// `top-nested [N] of Expr [with others = V] by Agg [asc|desc] [nulls first|last], ...`.
///
/// Level `i` groups the rows that survived levels `0..i` by the level's key within its parent
/// group, ranks the groups by the aggregate and keeps the first N (the rest form the `others`
/// group when `with others` is given; it is not nested further).
///
/// Every row is annotated level by level with its effective key `__p{i}` (the key, or the others
/// value) and `__o{i}` (in the others group); `L{i}` aggregates each level's groups; the result
/// left-joins the levels along the path.
pub(crate) fn top_nested(ctx: &mut Ctx, rel: Rel, levels: &[TopNestedLevel], env: &Env) -> Result<Rel> {
    if levels.is_empty() {
        return err("top-nested requires at least one clause");
    }
    let mut rel = rel.passthrough(ctx);
    rel.order.clear();
    rel.sel.order_by.clear();
    let input_cols = rel.cols.clone();

    // compile the levels against the input row scope
    let mut names = UniqueNames::default();
    let mut defaults = DefaultNames::default();
    let mut lv = Vec::new();
    for l in levels {
        let key = ctx.expr(&l.of, &Scope::rows(&input_cols), env)?;
        if key.agg {
            return err("top-nested: aggregates are not allowed in 'of'");
        }
        let agg = {
            let mut scope = Scope::rows(&input_cols);
            scope.aggregates = true;
            ctx.expr(&l.by, &scope, env)?
        };
        if !agg.agg {
            return err("top-nested: 'by' must be an aggregation");
        }
        let count = match &l.count {
            Some(c) => {
                let n = ctx.expr(c, &Scope::empty(), env)?;
                if !n.ty.is_numeric() {
                    return err("top-nested: the count must be numeric");
                }
                Some(match n.long_const() {
                    Some(v) => v.max(0).to_string(),
                    None => n.sql,
                })
            }
            None => None,
        };
        let others = match &l.others {
            Some(e) => {
                let v = ctx.expr(e, &Scope::empty(), env)?;
                let v = if v.ty == key.ty || v.is_null_const() { v } else { ctx.convert(v, key.ty) };
                Some(v)
            }
            None => None,
        };
        let base = match &l.name {
            Some(n) => n.clone(),
            None => result_name(&l.of, false).unwrap_or_else(|| defaults.next(|n| names.contains(n))),
        };
        let name = names.unique(&base);
        let agg_base = match &l.by_name {
            Some(n) => n.clone(),
            None => format!("aggregated_{name}"),
        };
        let agg_name = names.unique(&agg_base);
        let desc = l.dir != Some(SortDir::Asc);
        let nulls_first = match l.nulls {
            Some(NullsOrder::First) => true,
            Some(NullsOrder::Last) => false,
            None => !desc,
        };
        lv.push(Level { name, agg_name, key, agg, count, others, desc, nulls_first });
    }

    // base rows with the level keys
    let mut items = rel.identity_items();
    let mut base_cols = input_cols.clone();
    for (i, l) in lv.iter().enumerate() {
        items.push(Item { sql: crate::ops::output_sql(ctx, &l.key), alias: format!("__k{i}") });
        base_cols.push(Column { name: format!("__k{i}"), ty: l.key.ty });
    }
    let base = rel.project(ctx, items, base_cols);
    let base = ctx.add_cte("top_nested_input", base, false);

    let p = |j: usize| format!("__p{j}");
    // the surviving rows of the current level (not in an "others" group)
    let mut rows = base.from.clone();
    let mut level_ctes: Vec<String> = Vec::new();
    for (i, l) in lv.iter().enumerate() {
        let prefix: Vec<String> = (0..i).map(p).collect();
        let k = format!("__k{i}");
        let annotated = match &l.count {
            None => format!("SELECT *, {k} AS {}, false AS __o{i} FROM {rows} AS __src", p(i)),
            Some(n) => {
                let part = if prefix.is_empty() { String::new() } else { format!("PARTITION BY {} ", prefix.join(", ")) };
                let dir = if l.desc { "DESC" } else { "ASC" };
                let nulls = if l.nulls_first { "FIRST" } else { "LAST" };
                let mut group: Vec<String> = prefix.clone();
                group.push(k.clone());
                let ranked = format!(
                    "SELECT {}, ROW_NUMBER() OVER ({part}ORDER BY {} {dir} NULLS {nulls}, {k} ASC NULLS LAST) AS __rn FROM {rows} AS __src GROUP BY {}",
                    group.join(", "),
                    l.agg.sql,
                    group.join(", ")
                );
                let on: Vec<String> = group.iter().map(|c| format!("r.{c} IS NOT DISTINCT FROM g.{c}")).collect();
                let join = format!("FROM {rows} AS r JOIN ({ranked}) AS g ON {}", on.join(" AND "));
                match &l.others {
                    Some(v) => format!(
                        "SELECT r.*, CASE WHEN g.__rn <= {n} THEN r.{k} ELSE {} END AS {}, (g.__rn > {n}) AS __o{i} {join}",
                        v.sql,
                        p(i)
                    ),
                    None => format!("SELECT r.*, r.{k} AS {}, false AS __o{i} {join} WHERE g.__rn <= {n}", p(i)),
                }
            }
        };
        let tname = add_raw_cte(ctx, &format!("top_nested_{i}"), annotated, true);
        let mut group: Vec<String> = (0..=i).map(p).collect();
        group.push(format!("__o{i}"));
        let level = format!("SELECT {}, {} AS __a{i} FROM {tname} GROUP BY {}", group.join(", "), l.agg.sql, group.join(", "));
        level_ctes.push(add_raw_cte(ctx, &format!("top_nested_level_{i}"), level, false));
        rows = format!("(SELECT * FROM {tname} WHERE NOT __o{i})");
    }

    // the result: levels joined along the path
    let mut out_items = Vec::new();
    let mut cols = Vec::new();
    let mut from = format!("{} AS L0", level_ctes[0]);
    for (i, l) in lv.iter().enumerate() {
        if i > 0 {
            let mut on = vec![format!("NOT L{}.__o{}", i - 1, i - 1)];
            on.extend((0..i).map(|j| format!("L{i}.{} IS NOT DISTINCT FROM L{}.{}", p(j), i - 1, p(j))));
            from.push_str(&format!(" LEFT JOIN {} AS L{i} ON {}", level_ctes[i], on.join(" AND ")));
        }
        let kc = Column::new(l.name.clone(), l.key.ty);
        let key_sql = format!("L{i}.{}", p(i));
        out_items.push(Item { sql: if i > 0 { non_null(&kc, key_sql) } else { key_sql }, alias: l.name.clone() });
        cols.push(kc);
        out_items.push(Item { sql: format!("L{i}.__a{i}"), alias: l.agg_name.clone() });
        cols.push(Column::new(l.agg_name.clone(), l.agg.ty));
    }
    Ok(Rel::from_select(Select { items: Some(out_items), from: From::Raw(from), ..Default::default() }, cols))
}

/// Registers a CTE from raw SQL; returns its quoted name.
fn add_raw_cte(ctx: &mut Ctx, base: &str, sql: String, materialized: bool) -> String {
    let sel = Select { items: None, from: From::Raw(format!("({sql}) AS {}", ctx.alias())), ..Default::default() };
    let t = ctx.add_cte(base, Rel::from_select(sel, Vec::new()), materialized);
    t.from
}

// ---------------------------------------------------------------------- top-hitters

/// `top-hitters N of Key [by Weight]`: the N keys with the highest count (or sum of `Weight`).
/// Kusto computes this approximately; we compute it exactly (ties broken by the larger key).
pub(crate) fn top_hitters(ctx: &mut Ctx, rel: Rel, count: &Expr, of: &Expr, by: Option<&Expr>, env: &Env) -> Result<Rel> {
    let mut rel = rel.passthrough(ctx);
    rel.order.clear();
    rel.sel.order_by.clear();
    let cols = rel.cols.clone();
    let n = ctx.expr(count, &Scope::empty(), env)?;
    if !n.ty.is_numeric() {
        return err("top-hitters: the count must be numeric");
    }
    let key = ctx.expr(of, &Scope::rows(&cols), env)?;
    let key_name = result_name(of, false).unwrap_or_else(|| "Column1".into());
    let (weight_call, metric) = match by {
        Some(b) => {
            let bn = result_name(b, false).unwrap_or_default();
            (Expr::Call { name: "sum".into(), args: vec![Arg::positional(b.clone())] }, format!("approximate_sum_{bn}"))
        }
        None => (Expr::Call { name: "count".into(), args: vec![] }, format!("approximate_count_{key_name}")),
    };
    let weight = {
        let mut scope = Scope::rows(&cols);
        scope.aggregates = true;
        ctx.expr(&weight_call, &scope, env)?
    };
    let mut names = UniqueNames::default();
    let key_name = names.unique(&key_name);
    let metric = names.unique(&metric);
    let items = vec![Item { sql: crate::ops::output_sql(ctx, &key), alias: key_name.clone() }, Item { sql: weight.sql, alias: metric.clone() }];
    let out_cols = vec![Column::new(key_name.clone(), key.ty), Column::new(metric.clone(), weight.ty)];
    let mut r = rel.project(ctx, items, out_cols);
    r.sel.group_by = vec!["1".into()];
    let mut r = r.wrap(ctx);
    r.sel.order_by = vec![format!("{} DESC NULLS LAST", quote_ident(&metric)), format!("{} DESC NULLS LAST", quote_ident(&key_name))];
    r.sel.limit = Some(match n.long_const() {
        Some(v) => v.max(0).to_string(),
        None => n.sql,
    });
    Ok(r)
}

// ---------------------------------------------------------------------- sample-distinct

/// `sample-distinct N of Expr`: up to N random distinct values.
pub(crate) fn sample_distinct(ctx: &mut Ctx, rel: Rel, count: &Expr, of: &Expr, env: &Env) -> Result<Rel> {
    let mut rel = rel.passthrough(ctx);
    rel.order.clear();
    rel.sel.order_by.clear();
    let n = ctx.expr(count, &Scope::empty(), env)?;
    if !n.ty.is_numeric() {
        return err("sample-distinct: the count must be numeric");
    }
    let t = ctx.expr(of, &Scope::rows(&rel.cols), env)?;
    let name = result_name(of, false).unwrap_or_else(|| "Column1".into());
    let mut r = rel.project(ctx, vec![Item { sql: crate::ops::output_sql(ctx, &t), alias: name.clone() }], vec![Column::new(name, t.ty)]);
    r.sel.distinct = true;
    let mut r = r.wrap(ctx);
    r.sel.order_by = vec![ctx.d.random()];
    r.sel.limit = Some(match n.long_const() {
        Some(v) => v.max(0).to_string(),
        None => n.sql,
    });
    Ok(r)
}

// ---------------------------------------------------------------------- unsupported

pub(crate) fn reduce(_ctx: &mut Ctx, _rel: Rel, _by: &Expr, _params: &[OpParam], _env: &Env) -> Result<Rel> {
    err("the 'reduce' operator is not supported")
}

pub(crate) fn fork(_ctx: &mut Ctx, _rel: Rel, _branches: &[(Option<String>, Vec<Operator>)], _env: &Env) -> Result<Rel> {
    err("the 'fork' operator returns several result sets, which a single SQL query cannot express")
}

pub(crate) fn facet(_ctx: &mut Ctx, _rel: Rel, _by: &[String], _with: Option<&[Operator]>, _env: &Env) -> Result<Rel> {
    err("the 'facet' operator returns several result sets, which a single SQL query cannot express")
}

#[cfg(test)]
mod tests {
    use crate::{translate, Catalog, Column, Dialect, KqlType};

    fn catalog() -> Catalog {
        Catalog::new().with_table(
            "T",
            vec![Column::new("cat", KqlType::String), Column::new("sub", KqlType::String), Column::new("v", KqlType::Long), Column::new("d", KqlType::Dynamic)],
        )
    }

    fn names(kql: &str) -> Vec<String> {
        let mut out = Vec::new();
        for dialect in [Dialect::DuckDb, Dialect::Postgres] {
            let t = translate(kql, &catalog(), dialect).unwrap_or_else(|e| panic!("{kql}: {e}"));
            out = t.columns.iter().map(|c| format!("{}:{}", c.name, c.ty)).collect();
        }
        out
    }

    #[test]
    fn top_nested_columns() {
        assert_eq!(
            names("T | top-nested 2 of cat with others = 'rest' by sum(v), top-nested of sub by S = max(v) asc nulls last"),
            vec!["cat:string", "aggregated_cat:long", "sub:string", "S:long"]
        );
    }

    #[test]
    fn mv_apply_columns() {
        assert_eq!(names("T | mv-apply x = d to typeof(long) on (summarize s = sum(x))"), vec!["cat:string", "sub:string", "v:long", "d:dynamic", "s:long"]);
        assert_eq!(names("T | mv-apply with_itemindex=i d on (top 1 by i)"), vec!["cat:string", "sub:string", "v:long", "d:dynamic", "i:long"]);
    }

    #[test]
    fn partition_columns() {
        assert_eq!(names("T | partition by cat (summarize c = count() by sub)"), vec!["sub:string", "c:long"]);
        assert_eq!(names("T | partition by cat (top 1 by v)"), vec!["cat:string", "sub:string", "v:long", "d:dynamic"]);
    }

    #[test]
    fn hitters_and_distinct_columns() {
        assert_eq!(names("T | top-hitters 2 of cat by v"), vec!["cat:string", "approximate_sum_v:long"]);
        assert_eq!(names("T | top-hitters 2 of cat"), vec!["cat:string", "approximate_count_cat:long"]);
        assert_eq!(names("T | sample-distinct 2 of sub"), vec!["sub:string"]);
    }

    #[test]
    fn unsupported_shapes_are_rejected() {
        assert!(translate("T | partition by cat (as X | count)", &catalog(), Dialect::DuckDb).is_err());
        assert!(translate("T | fork (count) (take 1)", &catalog(), Dialect::DuckDb).is_err());
    }
}
