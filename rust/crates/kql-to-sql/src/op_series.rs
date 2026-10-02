//! `make-series` and `scan`.
//!
//! Both operators produce a self-contained subquery (`FROM (WITH ... SELECT ...) AS _qN`) so
//! their helper CTEs stay local and never clash with the query's top-level CTEs.

use kql_parser::ast::{Arg, Expr, MakeSeriesAgg, NamedExpr, NullsOrder, OpParam, OrderKey, ScanStep, SortDir};

use crate::binder::{Ctx, Env, OrderSpec, Rel};
use crate::expr::{Const, Scope, TExpr};
use crate::names::{result_name, DefaultNames, UniqueNames};
use crate::sql::{quote_ident, From, Select};
use crate::{err, Column, Dialect, KqlType, Result};

/// Renders the input relation as a plain SELECT (its order is irrelevant to the consumer).
fn input_sql(rel: Rel) -> String {
    let mut sel = rel.sel;
    if sel.limit.is_none() {
        sel.order_by.clear();
    }
    sel.render()
}

// ====================================================================== make-series

/// The numeric domain bucket arithmetic is done in.
#[derive(Clone, Copy, PartialEq)]
enum Axis {
    /// datetime: microseconds since the epoch
    Time,
    /// timespan: ticks
    Span,
    /// integer axis
    Int,
    /// real axis
    Real,
}

impl Axis {
    fn pos_type(self) -> KqlType {
        match self {
            Axis::Real => KqlType::Real,
            _ => KqlType::Long,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn make_series(
    ctx: &mut Ctx,
    rel: Rel,
    params: &[OpParam],
    aggs: &[MakeSeriesAgg],
    on: &Expr,
    from: Option<&Expr>,
    to: Option<&Expr>,
    step: &Expr,
    by: &[NamedExpr],
    env: &Env,
) -> Result<Rel> {
    let d = ctx.d;
    for p in params {
        if !p.name.eq_ignore_ascii_case("kind") {
            return err(format!("make-series: unsupported parameter '{}'", p.name));
        }
    }
    if aggs.is_empty() {
        return err("make-series requires at least one aggregate");
    }
    let in_cols = rel.cols.clone();
    let row_scope = Scope::rows(&in_cols);

    // ---- axis
    let on_t = ctx.expr(on, &row_scope, env)?;
    if on_t.agg {
        return err("make-series: the 'on' expression cannot contain aggregates");
    }
    let step_t = ctx.expr(step, &Scope::empty(), env)?;
    let from_t = match from {
        Some(e) => Some(ctx.expr(e, &Scope::empty(), env)?),
        None => None,
    };
    let to_t = match to {
        Some(e) => Some(ctx.expr(e, &Scope::empty(), env)?),
        None => None,
    };
    let on_t = if on_t.ty == KqlType::Dynamic { ctx.convert(on_t, KqlType::Real) } else { on_t };
    let axis = match on_t.ty {
        KqlType::DateTime => Axis::Time,
        KqlType::TimeSpan => Axis::Span,
        t if t.is_numeric() => {
            let all_int = t.is_integer()
                && step_t.ty.is_integer()
                && from_t.as_ref().is_none_or(|f| f.ty.is_integer())
                && to_t.as_ref().is_none_or(|f| f.ty.is_integer());
            if all_int {
                Axis::Int
            } else {
                Axis::Real
            }
        }
        t => return err(format!("make-series: unsupported axis type {t}")),
    };
    match axis {
        Axis::Time | Axis::Span if step_t.ty != KqlType::TimeSpan => {
            return err("make-series: the step of a datetime/timespan axis must be a timespan")
        }
        Axis::Int | Axis::Real if !step_t.ty.is_numeric() => {
            return err("make-series: the step of a numeric axis must be numeric")
        }
        _ => {}
    }
    match &step_t.konst {
        Some(Const::Long(v)) | Some(Const::TimeSpan(v)) if *v <= 0 => {
            return err("make-series: the step must be positive")
        }
        Some(Const::Real(v)) if *v <= 0.0 => return err("make-series: the step must be positive"),
        _ => {}
    }
    let pos_ty = d.sql_type(axis.pos_type());
    let dbl = d.sql_type(KqlType::Real);
    // value → position in the bucket domain
    let pos = |ctx: &Ctx, x: &TExpr| -> String {
        match axis {
            Axis::Time => d.epoch_us(&ctx.convert(x.clone(), KqlType::DateTime).sql),
            Axis::Span => ctx.convert(x.clone(), KqlType::TimeSpan).sql,
            Axis::Int => d.cast(&x.sql, KqlType::Long),
            Axis::Real => d.cast(&x.sql, KqlType::Real),
        }
    };
    let step_pos = match axis {
        Axis::Time => format!("CAST(trunc({} / 10) AS {pos_ty})", step_t.sql),
        Axis::Span | Axis::Int => d.cast(&step_t.sql, KqlType::Long),
        Axis::Real => d.cast(&step_t.sql, KqlType::Real),
    };
    let unpos = |p: String| -> TExpr {
        match axis {
            Axis::Time => TExpr::new(d.ts_from_us(&p), KqlType::DateTime),
            Axis::Span => TExpr::new(p, KqlType::TimeSpan),
            Axis::Int => TExpr::new(p, KqlType::Long),
            Axis::Real => TExpr::new(p, KqlType::Real),
        }
    };

    let tag = ctx.alias();
    let src = format!("{tag}_src");
    let p0 = format!("{tag}_p0");
    let p = format!("{tag}_p");
    let agg = format!("{tag}_agg");
    let grid = format!("{tag}_grid");

    // ---- group keys
    let mut names = UniqueNames::default();
    let mut defaults = DefaultNames::default();
    let mut key_items = Vec::new(); // (sql over input, name, type)
    for ne in by {
        let t = ctx.expr(&ne.expr, &row_scope, env)?;
        if t.agg {
            return err("make-series: aggregates are not allowed in 'by' keys");
        }
        let base = match ne.name() {
            Some(n) => n.to_string(),
            None => result_name(&ne.expr, false).unwrap_or_else(|| defaults.next(|n| names.contains(n))),
        };
        let name = names.unique(&base);
        key_items.push((crate::ops::output_sql(ctx, &t), name, t.ty));
    }

    // ---- aggregates
    let mut agg_items = Vec::new(); // (sql, name, type, default json sql)
    for a in aggs {
        let t = {
            let mut scope = Scope::rows(&in_cols);
            scope.aggregates = true;
            ctx.expr(&a.expr, &scope, env)?
        };
        if !t.agg {
            return err("make-series: each series expression must be an aggregate");
        }
        let base = match &a.name {
            Some(n) => n.clone(),
            None => result_name(&a.expr, true).unwrap_or_else(|| defaults.next(|n| names.contains(n))),
        };
        let name = names.unique(&base);
        let default = match &a.default {
            Some(e) => {
                let v = ctx.expr(e, &Scope::empty(), env)?;
                if v.is_null_const() {
                    format!("CAST(NULL AS {})", d.sql_type(KqlType::Dynamic))
                } else {
                    let v =
                        if v.ty != t.ty && v.ty.is_numeric() && t.ty.is_numeric() { ctx.convert(v, t.ty) } else { v };
                    ctx.to_dynamic(v).sql
                }
            }
            // Kusto fills missing buckets with 0 (null for types without a numeric zero)
            None if t.ty.is_numeric() || t.ty == KqlType::Bool => d.json_literal("0"),
            None => format!("CAST(NULL AS {})", d.sql_type(KqlType::Dynamic)),
        };
        agg_items.push((t.sql.clone(), name, t.ty, default));
    }
    let axis_name = names.unique(&result_name(on, false).unwrap_or_else(|| defaults.next(|n| names.contains(n))));

    // ---- SQL
    let kq = |i: usize| quote_ident(&format!("__k{i}"));
    let aq = |i: usize| quote_ident(&format!("__a{i}"));
    let on_pos = pos(ctx, &on_t);
    let mut src_items = vec!["*".to_string()];
    for (i, (s, _, _)) in key_items.iter().enumerate() {
        src_items.push(format!("{s} AS {}", kq(i)));
    }
    src_items.push(format!("{on_pos} AS __pos"));
    let src_sql = format!("SELECT {} FROM ({}) AS {tag}_in", src_items.join(", "), input_sql(rel));

    let base = if axis == Axis::Time { "(-62135596800000000)".to_string() } else { "0".to_string() };
    let f_sql = match &from_t {
        Some(f) => format!("CAST({} AS {pos_ty})", pos(ctx, f)),
        // the first bin with data, aligned like bin()
        None => format!(
            "(SELECT CAST(CAST(floor((min(__pos) - {base}) / CAST({step_pos} AS {dbl})) AS {bi}) * {step_pos} + {base} AS {pos_ty}) FROM {src})",
            bi = d.sql_type(KqlType::Long)
        ),
    };
    let p0_sql = format!("SELECT {f_sql} AS __f, CAST({step_pos} AS {pos_ty}) AS __s");
    let bi = d.sql_type(KqlType::Long);
    let n_sql = match &to_t {
        Some(t) => format!("greatest(CAST(ceil((CAST({} AS {pos_ty}) - __f) / CAST(__s AS {dbl})) AS {bi}), 0)", pos(ctx, t)),
        // through the last bin with data
        None => format!("COALESCE(greatest(CAST(floor(((SELECT max(__pos) FROM {src}) - __f) / CAST(__s AS {dbl})) AS {bi}) + 1, 0), 0)"),
    };
    let p_sql = format!("SELECT __f, __s, {n_sql} AS __n FROM {p0}");

    let bucket = format!("CAST(floor((__pos - __f) / CAST(__s AS {dbl})) AS {bi})");
    let mut agg_sel = Vec::new();
    for i in 0..key_items.len() {
        agg_sel.push(kq(i));
    }
    agg_sel.push(format!("{bucket} AS __b"));
    for (i, (s, _, _, _)) in agg_items.iter().enumerate() {
        agg_sel.push(format!("{s} AS {}", aq(i)));
    }
    let group: Vec<String> = (1..=key_items.len() + 1).map(|i| i.to_string()).collect();
    let agg_sql = format!(
        "SELECT {} FROM {src} CROSS JOIN {p} WHERE __pos IS NOT NULL AND {bucket} >= 0 AND {bucket} < __n GROUP BY {}",
        agg_sel.join(", "),
        group.join(", ")
    );

    let keys_list: Vec<String> = (0..key_items.len()).map(kq).collect();
    let distinct = if keys_list.is_empty() { "1 AS __g".to_string() } else { keys_list.join(", ") };
    let series = match d.kind() {
        Dialect::DuckDb => "unnest(range(0, __n))".to_string(),
        Dialect::Postgres => "generate_series(0, __n - 1)".to_string(),
    };
    let grid_sql =
        format!("SELECT _g.*, {series} AS __b FROM (SELECT DISTINCT {distinct} FROM {src}) AS _g CROSS JOIN {p}");

    let list = |elem: &str| -> String {
        match d.kind() {
            Dialect::DuckDb => format!("to_json(list({elem} ORDER BY _gr.__b))"),
            Dialect::Postgres => format!("jsonb_agg({elem} ORDER BY _gr.__b)"),
        }
    };
    let mut out_items = Vec::new();
    let mut cols = Vec::new();
    for (i, (_, name, ty)) in key_items.iter().enumerate() {
        out_items.push(format!("_gr.{} AS {}", kq(i), quote_ident(name)));
        cols.push(Column::new(name.clone(), *ty));
    }
    for (i, (_, name, ty, default)) in agg_items.iter().enumerate() {
        let v = ctx.to_dynamic(TExpr::new(format!("_ag.{}", aq(i)), *ty)).sql;
        let elem = format!("CASE WHEN _ag.__b IS NULL THEN {default} ELSE {v} END");
        out_items.push(format!("{} AS {}", list(&elem), quote_ident(name)));
        cols.push(Column::new(name.clone(), KqlType::Dynamic));
    }
    let axis_val = unpos(format!("(_gp.__f + _gr.__b * _gp.__s)"));
    let axis_elem = ctx.to_dynamic(axis_val).sql;
    out_items.push(format!("{} AS {}", list(&axis_elem), quote_ident(&axis_name)));
    cols.push(Column::new(axis_name, KqlType::Dynamic));

    let mut join = vec!["_ag.__b = _gr.__b".to_string()];
    for k in &keys_list {
        join.push(format!("_ag.{k} IS NOT DISTINCT FROM _gr.{k}"));
    }
    let group_out: Vec<String> = (1..=key_items.len()).map(|i| i.to_string()).collect();
    let final_sql = format!(
        "SELECT {} FROM {grid} AS _gr CROSS JOIN {p} AS _gp LEFT JOIN {agg} AS _ag ON {}{} HAVING count(*) > 0",
        out_items.join(", "),
        join.join(" AND "),
        if group_out.is_empty() { String::new() } else { format!(" GROUP BY {}", group_out.join(", ")) }
    );
    let sql = format!(
        "(WITH {src} AS ({src_sql}), {p0} AS ({p0_sql}), {p} AS ({p_sql}), {agg} AS ({agg_sql}), {grid} AS ({grid_sql}) {final_sql}) AS {tag}"
    );
    Ok(Rel::from_select(Select::star_from(From::Raw(sql)), cols))
}

// ====================================================================== scan
//
// Kusto's scan is a state machine over the serialized input. It is evaluated with a recursive
// CTE that walks the rows in order (`__rn`), carrying the state of every step:
//
// * `__a{L}`          — step L has an active match,
// * `__v{L}_{j}_{f}`  — inside the state of step L, the value of field `f` of step j (j <= L);
//                       fields are the declared variables plus every input column referenced
//                       as `step.column`.
//
// For each record the steps are checked from last to first (Kusto's "Check 1" — advance from
// the previous step's state — then "Check 2" — extend the step's own match). Every match emits
// the record extended with the step's assignments. Referencing a declared variable without a
// step prefix yields its declared default.

/// Replaces `step.field` by a synthetic name `__ref_{j}_{field}` and records the references.
fn rewrite_refs(e: &Expr, steps: &[String], refs: &mut Vec<(usize, String)>) -> Expr {
    let rw = |x: &Expr, refs: &mut Vec<(usize, String)>| Box::new(rewrite_refs(x, steps, refs));
    match e {
        Expr::Member { expr, name } => {
            if let Expr::Name(base) = &**expr {
                if let Some(j) = steps.iter().position(|s| s == base) {
                    if !refs.contains(&(j, name.clone())) {
                        refs.push((j, name.clone()));
                    }
                    return Expr::Name(format!("__ref_{j}_{name}"));
                }
            }
            Expr::Member { expr: rw(expr, refs), name: name.clone() }
        }
        Expr::Binary { op, left, right } => Expr::Binary { op: *op, left: rw(left, refs), right: rw(right, refs) },
        Expr::Unary { op, expr } => Expr::Unary { op: *op, expr: rw(expr, refs) },
        Expr::In { kind, expr, list } => Expr::In {
            kind: *kind,
            expr: rw(expr, refs),
            list: list.iter().map(|x| rewrite_refs(x, steps, refs)).collect(),
        },
        Expr::Between { expr, low, high, negated } => {
            Expr::Between { expr: rw(expr, refs), low: rw(low, refs), high: rw(high, refs), negated: *negated }
        }
        Expr::Call { name, args } => Expr::Call {
            name: name.clone(),
            args: args.iter().map(|a| Arg { name: a.name.clone(), expr: rewrite_refs(&a.expr, steps, refs) }).collect(),
        },
        Expr::Index { expr, index } => Expr::Index { expr: rw(expr, refs), index: rw(index, refs) },
        Expr::Paren(x) => Expr::Paren(rw(x, refs)),
        other => other.clone(),
    }
}

struct ScanVar {
    name: String,
    ty: KqlType,
    default: String,
}

/// A field of a step's state: a declared variable or a referenced input column.
struct Field {
    ty: KqlType,
    /// declared variable index, or `None` for an input column
    var: Option<usize>,
    /// input column name (for column fields)
    col: String,
    /// SQL of the field's empty value
    init: String,
}

pub(crate) fn scan(
    ctx: &mut Ctx,
    rel: Rel,
    order_by: &[OrderKey],
    partition_by: &[Expr],
    declare: &[(String, String, Option<Expr>)],
    steps: &[ScanStep],
    env: &Env,
) -> Result<Rel> {
    let d = ctx.d;
    if d.kind() != Dialect::DuckDb {
        // PostgreSQL forbids the recursive reference inside the staged subqueries used here
        return err("the 'scan' operator is not supported for PostgreSQL yet");
    }
    if steps.is_empty() {
        return err("scan requires at least one step");
    }
    if steps.iter().any(|s| s.optional) {
        return err("scan: optional steps are not supported yet");
    }
    let in_cols = rel.cols.clone();
    let null_of =
        |t: KqlType| if t == KqlType::String { "''".to_string() } else { format!("CAST(NULL AS {})", d.sql_type(t)) };

    // ---- declared variables
    let mut vars: Vec<ScanVar> = Vec::new();
    for (name, ty, def) in declare {
        let ty = KqlType::from_name(ty).ok_or_else(|| crate::Error::new(format!("scan: unknown type '{ty}'")))?;
        if in_cols.iter().any(|c| c.name == *name) || vars.iter().any(|v| v.name == *name) {
            return err(format!("scan: declared variable '{name}' conflicts with an existing column"));
        }
        let default = match def {
            Some(e) => {
                let t = ctx.expr(e, &Scope::empty(), env)?;
                let t = if t.ty == ty { t } else { ctx.convert(t, ty) };
                d.cast(&t.sql, ty)
            }
            None => null_of(ty),
        };
        vars.push(ScanVar { name: name.clone(), ty, default });
    }

    // ---- row order and partitions
    let row_scope = Scope::rows(&in_cols);
    let mut order_sql = Vec::new();
    for k in order_by {
        let t = ctx.expr(&k.expr, &row_scope, env)?;
        let t = if t.ty == KqlType::Dynamic { ctx.to_string(t) } else { t };
        let desc = k.dir == Some(SortDir::Desc);
        let nulls_first = match k.nulls {
            Some(NullsOrder::First) => true,
            Some(NullsOrder::Last) => false,
            None => !desc,
        };
        order_sql.push(format!(
            "{} {} NULLS {}",
            t.sql,
            if desc { "DESC" } else { "ASC" },
            if nulls_first { "FIRST" } else { "LAST" }
        ));
    }
    let logical_order: Vec<OrderSpec> =
        if order_by.is_empty() && partition_by.is_empty() { rel.order.clone() } else { Vec::new() };
    if order_by.is_empty() {
        order_sql = rel.order_sql();
    }
    let mut part_sql = Vec::new();
    for p in partition_by {
        let t = ctx.expr(p, &row_scope, env)?;
        part_sql.push(t.sql);
    }

    // ---- step expressions with `step.field` references
    let step_names: Vec<String> = steps.iter().map(|s| s.name.clone()).collect();
    let mut refs: Vec<(usize, String)> = Vec::new();
    let rewritten: Vec<(Expr, Vec<(String, Expr)>)> = steps
        .iter()
        .map(|s| {
            let c = rewrite_refs(&s.condition, &step_names, &mut refs);
            let a = s.assignments.iter().map(|(n, e)| (n.clone(), rewrite_refs(e, &step_names, &mut refs))).collect();
            (c, a)
        })
        .collect();
    for (_, assigns) in &rewritten {
        for (n, _) in assigns {
            if !vars.iter().any(|v| v.name == *n) {
                return err(format!("scan: '{n}' is not a declared variable"));
            }
        }
    }
    // slot fields: declared variables, then referenced input columns
    let mut fields: Vec<Field> = vars
        .iter()
        .enumerate()
        .map(|(i, v)| Field { ty: v.ty, var: Some(i), col: String::new(), init: v.default.clone() })
        .collect();
    for (_, name) in &refs {
        if vars.iter().any(|v| v.name == *name) || fields.iter().any(|f| f.var.is_none() && f.col == *name) {
            continue;
        }
        match in_cols.iter().find(|c| c.name == *name) {
            Some(c) => fields.push(Field { ty: c.ty, var: None, col: c.name.clone(), init: null_of(c.ty) }),
            None => return err(format!("scan: unknown column or variable '{name}'")),
        }
    }
    let field_index = |name: &str| -> usize {
        fields
            .iter()
            .position(|f| match f.var {
                Some(i) => vars[i].name == name,
                None => f.col == name,
            })
            .unwrap()
    };

    let nsteps = steps.len();
    let act = |l: usize| format!("__a{l}");
    let slot = |l: usize, j: usize, f: usize| format!("__v{l}_{j}_{f}");
    let m_col = |k: usize| format!("__m{k}");
    let e_col = |k: usize, v: usize| format!("__e{k}_{v}");

    // state columns of the recursive CTE: (name, sql type, initial value)
    let mut state_cols: Vec<(String, String, String)> = Vec::new();
    for l in 1..=nsteps {
        state_cols.push((act(l), d.sql_type(KqlType::Bool).to_string(), "false".into()));
        for j in 1..=l {
            for (fi, f) in fields.iter().enumerate() {
                state_cols.push((slot(l, j, fi), d.sql_type(f.ty).to_string(), f.init.clone()));
            }
        }
    }
    // per-record match outputs
    let mut out_cols: Vec<(String, String, String)> = Vec::new();
    for k in 1..=nsteps {
        out_cols.push((m_col(k), d.sql_type(KqlType::Bool).to_string(), "false".into()));
        for (vi, v) in vars.iter().enumerate() {
            out_cols.push((e_col(k, vi), d.sql_type(v.ty).to_string(), null_of(v.ty)));
        }
    }

    // compiles a step expression against the state of step `l` (0 = no state)
    let compile = |ctx: &mut Ctx, e: &Expr, l: usize| -> Result<TExpr> {
        let mut extra: Vec<(String, String, KqlType)> = Vec::new();
        for v in &vars {
            extra.push((v.name.clone(), v.default.clone(), v.ty));
        }
        for (j, name) in &refs {
            let fi = field_index(name);
            let f = &fields[fi];
            let sql = if l > 0 && j + 1 <= l { quote_ident(&slot(l, j + 1, fi)) } else { f.init.clone() };
            extra.push((format!("__ref_{j}_{name}"), sql, f.ty));
        }
        let mut scope = Scope::rows(&in_cols);
        scope.extra = &extra;
        let t = ctx.expr(e, &scope, env)?;
        if t.agg || t.window {
            return err("scan: aggregate and window functions are not allowed in scan steps");
        }
        Ok(t)
    };

    let tag = ctx.alias();
    let base = format!("{tag}_base");
    let rec = format!("{tag}_rec");

    // ---- base: the input with row numbers (partitions are walked one after another)
    let over_order = if order_sql.is_empty() { String::new() } else { format!("ORDER BY {}", order_sql.join(", ")) };
    let rn_order: Vec<String> =
        part_sql.iter().map(|p| format!("{p} ASC NULLS FIRST")).chain(order_sql.iter().cloned()).collect();
    let rn_over = if rn_order.is_empty() { String::new() } else { format!("ORDER BY {}", rn_order.join(", ")) };
    let first = if part_sql.is_empty() {
        "false".to_string()
    } else {
        format!("(row_number() OVER (PARTITION BY {} {over_order}) = 1)", part_sql.join(", "))
    };
    let base_sql = format!(
        "SELECT *, row_number() OVER ({rn_over}) AS __rn, {first} AS __first FROM ({}) AS {tag}_in",
        input_sql(rel)
    );

    // ---- recursive term, built as nested stages (one pair per step, last step first)
    let mut cur: Vec<String> = in_cols.iter().map(|c| c.name.clone()).collect();
    cur.push("__rn".into());
    cur.extend(state_cols.iter().map(|(n, _, _)| n.clone()));
    // The next row is read from a single list of all rows, which is much cheaper per iteration
    // than re-joining the base table.
    let rows = format!("{tag}_rows");
    let row_fields: Vec<String> = in_cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("'f{i}': {}", quote_ident(&c.name)))
        .chain(std::iter::once("'first': __first".to_string()))
        .collect();
    let rows_cte = format!(
        ", {rows} AS MATERIALIZED (SELECT list({{{}}} ORDER BY __rn) AS rows, count(*) AS n FROM {base})",
        row_fields.join(", ")
    );
    let first_items: Vec<String> = in_cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("_l.rows[_r.__rn + 1].f{i} AS {}", quote_ident(&c.name)))
        .chain(std::iter::once("(_r.__rn + 1) AS __rn".to_string()))
        .chain(
            state_cols
                .iter()
                .map(|(n, _, init)| format!("CASE WHEN _l.rows[_r.__rn + 1].first THEN {init} ELSE _r.{n} END AS {n}")),
        )
        .collect();
    let next_from = format!("{rec} AS _r CROSS JOIN {rows} AS _l WHERE _r.__rn < _l.n");
    let mut stage = format!("SELECT {} FROM {next_from}", first_items.join(", "));
    let mut sn = 0;
    let mut wrap = |stage: String, cur: &[String], replace: &[(String, String)], add: &[(String, String)]| -> String {
        sn += 1;
        let mut items: Vec<String> = cur
            .iter()
            .map(|c| match replace.iter().find(|(n, _)| n == c) {
                Some((_, s)) => format!("{s} AS {}", quote_ident(c)),
                None => quote_ident(c),
            })
            .collect();
        for (n, s) in add {
            items.push(format!("{s} AS {}", quote_ident(n)));
        }
        format!("SELECT {} FROM ({stage}) AS {tag}_s{sn}", items.join(", "))
    };
    for k in (1..=nsteps).rev() {
        let (cond, assigns) = &rewritten[k - 1];
        // condition and assigned values computed with the state of step `l`
        let eval = |ctx: &mut Ctx, l: usize| -> Result<(String, Vec<String>)> {
            let c = compile(ctx, cond, l)?;
            let c = ctx.to_bool(c);
            let mut es = Vec::new();
            for v in &vars {
                let s = match assigns.iter().rev().find(|(n, _)| *n == v.name) {
                    Some((_, e)) => {
                        let t = compile(ctx, e, l)?;
                        let t = if t.ty == v.ty { t } else { ctx.convert(t, v.ty) };
                        d.cast(&t.sql, v.ty)
                    }
                    None => v.default.clone(),
                };
                es.push(s);
            }
            Ok((format!("COALESCE({}, false)", c.sql), es))
        };
        let (c2, e2) = eval(ctx, k)?;
        let (c1, e1) = if k > 1 { eval(ctx, k - 1)? } else { ("false".to_string(), Vec::new()) };
        // stage A: evaluate both checks and the assignments
        let mut add = vec![
            (
                "__c1".to_string(),
                if k > 1 { format!("({} AND {c1})", quote_ident(&act(k - 1))) } else { "false".into() },
            ),
            ("__c2".to_string(), if k == 1 { c2.clone() } else { format!("({} AND {c2})", quote_ident(&act(k))) }),
        ];
        for (vi, v) in vars.iter().enumerate() {
            add.push((format!("__x1_{vi}"), e1.get(vi).cloned().unwrap_or_else(|| v.default.clone())));
            add.push((format!("__x2_{vi}"), e2[vi].clone()));
        }
        stage = wrap(stage, &cur, &[], &add);
        let mut cur_a = cur.clone();
        cur_a.extend(add.iter().map(|(n, _)| n.clone()));
        // stage B: update the state (Check 1 wins over Check 2) and emit the match
        let mut replace = vec![(act(k), format!("(__c1 OR __c2 OR {})", quote_ident(&act(k))))];
        for (fi, f) in fields.iter().enumerate() {
            let rec_val = |x: &str| match f.var {
                Some(vi) => format!("__{x}_{vi}"),
                None => quote_ident(&f.col),
            };
            for j in 1..k {
                replace.push((
                    slot(k, j, fi),
                    format!(
                        "CASE WHEN __c1 THEN {} ELSE {} END",
                        quote_ident(&slot(k - 1, j, fi)),
                        quote_ident(&slot(k, j, fi))
                    ),
                ));
                replace.push((
                    slot(k - 1, j, fi),
                    format!("CASE WHEN __c1 THEN {} ELSE {} END", f.init, quote_ident(&slot(k - 1, j, fi))),
                ));
            }
            replace.push((
                slot(k, k, fi),
                format!(
                    "CASE WHEN __c1 THEN {} WHEN __c2 THEN {} ELSE {} END",
                    rec_val("x1"),
                    rec_val("x2"),
                    quote_ident(&slot(k, k, fi))
                ),
            ));
        }
        if k > 1 {
            replace.push((act(k - 1), format!("({} AND NOT __c1)", quote_ident(&act(k - 1)))));
        }
        let mut add_b = vec![(m_col(k), "(__c1 OR __c2)".to_string())];
        for (vi, v) in vars.iter().enumerate() {
            add_b.push((
                e_col(k, vi),
                format!("CASE WHEN __c1 THEN __x1_{vi} WHEN __c2 THEN __x2_{vi} ELSE {} END", null_of(v.ty)),
            ));
        }
        // stage B keeps only the pipeline columns (temporaries dropped)
        stage = wrap(stage, &cur, &replace, &add_b);
        cur.extend(add_b.iter().map(|(n, _)| n.clone()));
    }
    // final projection of the recursive term (same columns as the anchor)
    let bigint = d.sql_type(KqlType::Long);
    let mut rec_items = vec![format!("CAST(__rn AS {bigint}) AS __rn")];
    let mut anchor_items = vec![format!("CAST(0 AS {bigint}) AS __rn")];
    for (n, ty, init) in state_cols.iter().chain(out_cols.iter()) {
        rec_items.push(format!("CAST({} AS {ty}) AS {}", quote_ident(n), quote_ident(n)));
        anchor_items.push(format!("CAST({init} AS {ty}) AS {}", quote_ident(n)));
    }
    let rec_term = format!("SELECT {} FROM ({stage}) AS {tag}_fin", rec_items.join(", "));
    let anchor = format!("SELECT {}", anchor_items.join(", "));

    // ---- output: one row per match (for several steps, in the order the steps are checked)
    let mut parts = Vec::new();
    for k in (1..=nsteps).rev() {
        let mut items: Vec<String> = in_cols.iter().map(|c| format!("_b.{}", quote_ident(&c.name))).collect();
        for (vi, v) in vars.iter().enumerate() {
            items.push(format!("_r.{} AS {}", e_col(k, vi), quote_ident(&v.name)));
        }
        items.push("_b.__rn AS __rn".into());
        items.push(format!("{} AS __o", nsteps - k));
        parts.push(format!(
            "SELECT {} FROM {rec} AS _r JOIN {base} AS _b ON _b.__rn = _r.__rn WHERE _r.{}",
            items.join(", "),
            m_col(k)
        ));
    }
    let body = if parts.len() == 1 {
        parts.pop().unwrap()
    } else {
        parts.iter().map(|p| format!("({p})")).collect::<Vec<_>>().join(" UNION ALL ")
    };
    let sql = format!("(WITH RECURSIVE {base} AS MATERIALIZED ({base_sql}){rows_cte}, {rec} AS (({anchor}) UNION ALL ({rec_term})) {body}) AS {tag}");

    let mut cols = in_cols.clone();
    for v in &vars {
        cols.push(Column::new(v.name.clone(), v.ty));
    }
    let items = cols
        .iter()
        .map(|c| crate::sql::Item { sql: format!("{tag}.{}", quote_ident(&c.name)), alias: c.name.clone() })
        .collect();
    let mut sel = Select { items: Some(items), from: From::Raw(sql), ..Default::default() };
    if nsteps == 1 && !logical_order.is_empty() {
        let mut r = Rel::from_select(sel, cols);
        r.order = logical_order;
        Ok(r)
    } else {
        // the walk order is not expressible over the output columns: order physically
        sel.order_by = vec![format!("{tag}.__rn ASC"), format!("{tag}.__o ASC")];
        Ok(Rel::from_select(sel, cols))
    }
}

#[cfg(test)]
mod tests {
    use crate::{translate, Catalog, Dialect, KqlType};

    fn ok(kql: &str, d: Dialect) -> crate::Translation {
        translate(kql, &Catalog::new(), d).unwrap_or_else(|e| panic!("{kql}: {e}"))
    }

    #[test]
    fn make_series_columns() {
        let q = "datatable(t:datetime, v:long, g:string)[datetime(2020-01-01),1,'a'] \
                 | make-series s = sum(v), avg(v) default = 0 on t from datetime(2020-01-01) to datetime(2020-01-04) step 1d by g";
        for d in [Dialect::DuckDb, Dialect::Postgres] {
            let t = ok(q, d);
            let cols: Vec<(&str, KqlType)> = t.columns.iter().map(|c| (c.name.as_str(), c.ty)).collect();
            assert_eq!(
                cols,
                vec![
                    ("g", KqlType::String),
                    ("s", KqlType::Dynamic),
                    ("avg_v", KqlType::Dynamic),
                    ("t", KqlType::Dynamic)
                ]
            );
        }
        // inferred range
        ok("datatable(x:long, v:real)[1,2.0] | make-series max(v) on x step 2", Dialect::DuckDb);
    }

    #[test]
    fn scan_shapes() {
        let single = "datatable(t:long, v:long)[1,5, 2,3] | sort by t asc | scan declare (c:long=0) with (step s: true => c = s.c + v;)";
        let multi = "datatable(t:long, e:string)[1,'a', 2,'b'] | sort by t asc \
                     | scan declare (n:long) with (step s1: e == 'a' => n = 1; step s2: e == 'b' and t > s1.t => n = s1.n + 1;)";
        assert!(translate(single, &Catalog::new(), Dialect::Postgres).is_err());
        {
            for q in [single, multi] {
                let t = ok(q, Dialect::DuckDb);
                assert!(t.sql.contains("WITH RECURSIVE"), "{}", t.sql);
                assert_eq!(t.columns.last().unwrap().ty, KqlType::Long);
            }
        }
        assert!(translate(
            "datatable(t:long)[1] | scan with (step s: true => x = 1;)",
            &Catalog::new(),
            Dialect::DuckDb
        )
        .is_err());
    }
}
