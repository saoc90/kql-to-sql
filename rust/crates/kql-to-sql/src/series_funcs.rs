//! `series_*` functions over dynamic arrays (the output of `make-series`).
//!
//! Arrays are JSON in flight; the functions are written with DuckDB list lambdas
//! (`CAST(x AS JSON[])`, `list_transform`, ...). PostgreSQL is not supported yet.

use crate::binder::Ctx;
use crate::expr::{Const, Scope, TExpr};
use crate::{err, Dialect, KqlType, Result};

/// Series functions. `name` is lowercase; `a` are compiled arguments. Returns `None` if `name`
/// is not handled here.
pub(crate) fn call(ctx: &mut Ctx, name: &str, a: &[TExpr], _scope: &Scope) -> Option<Result<TExpr>> {
    const NAMES: &[&str] = &[
        "series_fill_forward",
        "series_fill_backward",
        "series_fill_const",
        "series_fill_linear",
        "series_add",
        "series_subtract",
        "series_multiply",
        "series_divide",
        "series_greater",
        "series_greater_equals",
        "series_less",
        "series_less_equals",
        "series_equals",
        "series_not_equals",
        "series_stats",
        "series_stats_dynamic",
        "series_fit_line",
        "series_fit_line_dynamic",
        "series_pearson_correlation",
        "series_fir",
        "series_iir",
    ];
    if !NAMES.contains(&name) {
        return None;
    }
    if ctx.d.kind() != Dialect::DuckDb {
        return Some(err(format!("{name}() is not supported for PostgreSQL yet")));
    }
    Some(duck(ctx, name, a))
}

/// Lambda variable names are made unique per call so nested lambdas never shadow each other.
struct Names(usize);

impl Names {
    fn v(&mut self, base: &str) -> String {
        self.0 += 1;
        format!("_{base}{}", self.0)
    }
}

/// `x` as `JSON[]`, NULL when it is not an array.
fn json_list(x: &str) -> String {
    format!("(CASE WHEN json_type({x}) = 'ARRAY' THEN CAST({x} AS JSON[]) END)")
}

/// A JSON element as DOUBLE (NULL for non-numbers).
fn num(e: &str) -> String {
    format!("TRY_CAST(json_extract_string({e}, '$') AS DOUBLE)")
}

/// `x` as `DOUBLE[]`.
fn num_list(x: &str, n: &mut Names) -> String {
    let e = n.v("e");
    format!("list_transform({}, {e} -> {})", json_list(x), num(&e))
}

/// A constant numeric array argument.
fn const_numbers(t: &TExpr, what: &str) -> Result<Vec<f64>> {
    match &t.konst {
        Some(Const::Dynamic(serde_json::Value::Array(items))) => items
            .iter()
            .map(|v| v.as_f64().ok_or_else(|| crate::Error::new(format!("{what} must contain only numbers"))))
            .collect(),
        _ => err(format!("{what} must be a constant dynamic array")),
    }
}

fn real_lit(v: f64) -> String {
    format!("CAST({v:?} AS DOUBLE)")
}

fn need(name: &str, a: &[TExpr], min: usize, max: usize) -> Result<()> {
    if a.len() < min || a.len() > max {
        return err(format!("{name}(): expected {min}..{max} arguments, got {}", a.len()));
    }
    Ok(())
}

fn duck(ctx: &mut Ctx, name: &str, a: &[TExpr]) -> Result<TExpr> {
    let mut n = Names(0);
    let parts: Vec<&TExpr> = a.iter().collect();
    let mk = |sql: String, ty: KqlType| TExpr::derived(sql, ty, &parts);
    match name {
        "series_fill_forward" | "series_fill_backward" | "series_fill_const" => {
            let (min, max) = if name == "series_fill_const" { (2, 3) } else { (1, 2) };
            need(name, a, min, max)?;
            let placeholder = if name == "series_fill_const" { a.get(2) } else { a.get(1) };
            let l = n.v("l");
            let i = n.v("i");
            let e = n.v("e");
            let missing = |x: &str| missing_sql(ctx, x, placeholder);
            let body = match name {
                "series_fill_forward" => format!(
                    "COALESCE(list_extract(list_filter(list_slice({l}, 1, {i}), {e} -> NOT {}), -1), {l}[{i}])",
                    missing(&e)
                ),
                "series_fill_backward" => format!(
                    "COALESCE(list_extract(list_filter(list_slice({l}, {i}, len({l})), {e} -> NOT {}), 1), {l}[{i}])",
                    missing(&e)
                ),
                _ => {
                    let c = ctx.to_dynamic(a[1].clone()).sql;
                    format!("CASE WHEN {} THEN {c} ELSE {l}[{i}] END", missing(&format!("{l}[{i}]")))
                }
            };
            Ok(mk(apply_list(&a[0].sql, &l, &i, &body), KqlType::Dynamic))
        }
        "series_fill_linear" => {
            need(name, a, 1, 4)?;
            let placeholder = a.get(1);
            let fill_edges = match a.get(2) {
                None => "true".to_string(),
                Some(t) => ctx.to_bool(t.clone()).sql,
            };
            let l = n.v("l");
            let i = n.v("i");
            let j = n.v("j");
            let missing_at = |x: &str| missing_sql(ctx, &format!("{l}[{x}]"), placeholder);
            let valid = format!("list_filter(range(1, len({l}) + 1), {j} -> NOT {})", missing_at(&j));
            let p = format!("list_extract(list_filter({valid}, {j} -> {j} <= {i}), -1)");
            let q = format!("list_extract(list_filter({valid}, {j} -> {j} >= {i}), 1)");
            let at = |k: &str| num(&format!("{l}[{k}]"));
            let body = format!(
                "CASE WHEN NOT {} THEN {} WHEN {p} IS NOT NULL AND {q} IS NOT NULL THEN {} + ({} - {}) * ({i} - {p}) / ({q} - {p}) \
                 WHEN {fill_edges} AND COALESCE({p}, {q}) IS NOT NULL THEN {} ELSE {} END",
                missing_at(&i),
                at(&i),
                at(&p),
                at(&q),
                at(&p),
                at(&format!("COALESCE({p}, {q})")),
                at(&i),
            );
            let body = if let Some(c) = a.get(3) {
                // constant_value: used when no interpolation is possible
                format!("COALESCE({body}, {})", ctx.d.cast(&c.sql, KqlType::Real))
            } else {
                body
            };
            Ok(mk(apply_list(&a[0].sql, &l, &i, &body), KqlType::Dynamic))
        }
        "series_add" | "series_subtract" | "series_multiply" | "series_divide" | "series_greater" | "series_greater_equals" | "series_less"
        | "series_less_equals" | "series_equals" | "series_not_equals" => {
            need(name, a, 2, 2)?;
            let op = match name {
                "series_add" => "+",
                "series_subtract" => "-",
                "series_multiply" => "*",
                "series_divide" => "/",
                "series_greater" => ">",
                "series_greater_equals" => ">=",
                "series_less" => "<",
                "series_less_equals" => "<=",
                "series_equals" => "=",
                _ => "<>",
            };
            let i = n.v("i");
            let x = num_list(&a[0].sql, &mut n);
            let y = num_list(&a[1].sql, &mut n);
            let (xs, xlen) = operand(ctx, &a[0], &x, &i);
            let (ys, ylen) = operand(ctx, &a[1], &y, &i);
            let len = match (xlen, ylen) {
                (Some(a), Some(b)) => format!("greatest({a}, {b})"),
                (Some(a), None) | (None, Some(a)) => a,
                (None, None) => return err(format!("{name}(): at least one argument must be a dynamic array")),
            };
            let elem = if op == "/" { format!("({xs} / NULLIF({ys}, 0))") } else { format!("({xs} {op} {ys})") };
            Ok(mk(format!("to_json(list_transform(range(1, {len} + 1), {i} -> {elem}))"), KqlType::Dynamic))
        }
        "series_stats_dynamic" | "series_stats" => {
            need(name, a, 1, 2)?;
            let x = num_list(&a[0].sql, &mut n);
            let e = n.v("e");
            let vals = format!("list_filter({x}, {e} -> {e} IS NOT NULL)");
            let min = format!("list_min({vals})");
            let max = format!("list_max({vals})");
            if name == "series_stats" {
                // used as a single value, series_stats() yields its first column (min)
                return Ok(mk(min, KqlType::Real));
            }
            let fields = [
                ("min", min.clone()),
                ("min_idx", format!("CAST(list_position({x}, {min}) - 1 AS BIGINT)")),
                ("max", max.clone()),
                ("max_idx", format!("CAST(list_position({x}, {max}) - 1 AS BIGINT)")),
                ("avg", format!("list_avg({vals})")),
                ("stdev", format!("COALESCE(list_stddev_samp({vals}), 0.0)")),
                ("variance", format!("COALESCE(list_var_samp({vals}), 0.0)")),
                ("sum", format!("CAST(list_sum({vals}) AS DOUBLE)")),
                ("len", format!("CAST(len({x}) AS BIGINT)")),
            ];
            let pairs: Vec<String> = fields.iter().map(|(k, v)| format!("'{k}', {v}")).collect();
            Ok(mk(format!("CASE WHEN json_type({0}) = 'ARRAY' THEN json_object({1}) END", a[0].sql, pairs.join(", ")), KqlType::Dynamic))
        }
        "series_fit_line" | "series_fit_line_dynamic" => {
            need(name, a, 1, 1)?;
            let y = num_list(&a[0].sql, &mut n);
            let (i, j, k) = (n.v("i"), n.v("j"), n.v("k"));
            // x = 1..n
            let cnt = format!("CAST(len({y}) AS DOUBLE)");
            let mx = format!("(({cnt} + 1) / 2.0)");
            let my = format!("list_avg({y})");
            let sxx = format!("list_sum(list_transform(range(1, len({y}) + 1), {i} -> ({i} - {mx}) * ({i} - {mx})))");
            let sxy = format!("list_sum(list_transform(range(1, len({y}) + 1), {j} -> ({j} - {mx}) * ({y}[{j}] - {my})))");
            let slope = format!("({sxy} / NULLIF({sxx}, 0))");
            let icept = format!("({my} - {slope} * {mx})");
            let sst = format!("list_sum(list_transform({y}, {k} -> ({k} - {my}) * ({k} - {my})))");
            let r = n.v("r");
            let fit = |x: &str| format!("({icept} + {slope} * {x})");
            let ssr = format!("list_sum(list_transform(range(1, len({y}) + 1), {r} -> ({y}[{r}] - {}) * ({y}[{r}] - {})))", fit(&r), fit(&r));
            let rsq = format!("CASE WHEN {sst} = 0 THEN 1.0 ELSE 1 - {ssr} / {sst} END");
            if name == "series_fit_line" {
                // used as a single value, series_fit_line() yields its first column (rsquare)
                return Ok(mk(rsq, KqlType::Real));
            }
            let f = n.v("f");
            let pairs = [
                ("rsquare", rsq),
                ("slope", slope.clone()),
                ("variance", format!("({sst} / NULLIF({cnt} - 1, 0))")),
                ("rvariance", format!("({ssr} / NULLIF({cnt} - 1, 0))")),
                ("interception", icept.clone()),
                ("line_fit", format!("to_json(list_transform(range(1, len({y}) + 1), {f} -> {}))", fit(&f))),
            ];
            let pairs: Vec<String> = pairs.iter().map(|(k, v)| format!("'{k}', {v}")).collect();
            Ok(mk(format!("CASE WHEN json_type({0}) = 'ARRAY' THEN json_object({1}) END", a[0].sql, pairs.join(", ")), KqlType::Dynamic))
        }
        "series_pearson_correlation" => {
            need(name, a, 2, 2)?;
            let x = num_list(&a[0].sql, &mut n);
            let y = num_list(&a[1].sql, &mut n);
            let (i, j, k) = (n.v("i"), n.v("j"), n.v("k"));
            let (mx, my) = (format!("list_avg({x})"), format!("list_avg({y})"));
            let len = format!("least(len({x}), len({y}))");
            let sxy = format!("list_sum(list_transform(range(1, {len} + 1), {i} -> ({x}[{i}] - {mx}) * ({y}[{i}] - {my})))");
            let sxx = format!("list_sum(list_transform(range(1, {len} + 1), {j} -> ({x}[{j}] - {mx}) * ({x}[{j}] - {mx})))");
            let syy = format!("list_sum(list_transform(range(1, {len} + 1), {k} -> ({y}[{k}] - {my}) * ({y}[{k}] - {my})))");
            Ok(mk(format!("({sxy} / NULLIF(sqrt({sxx} * {syy}), 0))"), KqlType::Real))
        }
        "series_fir" => {
            need(name, a, 2, 4)?;
            let w = const_numbers(&a[1], "series_fir(): the filter")?;
            if w.is_empty() {
                return err("series_fir(): the filter must not be empty");
            }
            let normalize = match a.get(2) {
                Some(t) => match t.konst {
                    Some(Const::Bool(b)) => b,
                    _ => return err("series_fir(): normalize must be a constant bool"),
                },
                // normalized by default unless the filter has negative coefficients
                None => !w.iter().any(|v| *v < 0.0),
            };
            let center = match a.get(3) {
                Some(t) => match t.konst {
                    Some(Const::Bool(b)) => b,
                    _ => return err("series_fir(): center must be a constant bool"),
                },
                None => false,
            };
            let sum: f64 = w.iter().sum();
            let w: Vec<f64> = if normalize && sum != 0.0 { w.iter().map(|v| v / sum).collect() } else { w };
            let shift = if center { (w.len() / 2) as i64 } else { 0 };
            let x = num_list(&a[0].sql, &mut n);
            let i = n.v("i");
            // y[i] = sum_k w[k] * x[i - k + shift] (0-based), values outside the series are 0
            let terms: Vec<String> = w
                .iter()
                .enumerate()
                .map(|(k, c)| {
                    let idx = format!("({i} - {k} + {shift})");
                    format!("{} * (CASE WHEN {idx} >= 0 AND {idx} < len({x}) THEN COALESCE({x}[{idx} + 1], 0) ELSE 0 END)", real_lit(*c))
                })
                .collect();
            Ok(mk(format!("to_json(list_transform(range(0, len({x})), {i} -> {}))", terms.join(" + ")), KqlType::Dynamic))
        }
        "series_iir" => {
            need(name, a, 3, 3)?;
            let b = const_numbers(&a[1], "series_iir(): the numerators")?;
            let den = const_numbers(&a[2], "series_iir(): the denominators")?;
            if b.is_empty() || den.is_empty() || den[0] == 0.0 {
                return err("series_iir(): invalid filter coefficients");
            }
            if den.len() > 2 {
                return err("series_iir(): denominators with more than two coefficients are not supported yet");
            }
            let a0 = den[0];
            let ratio = if den.len() == 2 { -den[1] / a0 } else { 0.0 };
            let x = num_list(&a[0].sql, &mut n);
            let (i, m) = (n.v("i"), n.v("m"));
            // f[m] = sum_k b[k]/a0 * x[m - k]
            let f = |m: &str| -> String {
                b.iter()
                    .enumerate()
                    .map(|(k, c)| {
                        let idx = format!("({m} - {k})");
                        format!("{} * (CASE WHEN {idx} >= 0 THEN COALESCE({x}[{idx} + 1], 0) ELSE 0 END)", real_lit(c / a0))
                    })
                    .collect::<Vec<_>>()
                    .join(" + ")
            };
            // y[i] = sum_{m<=i} ratio^(i-m) * f[m]
            let body = if ratio == 0.0 {
                format!("({})", f(&i))
            } else {
                format!("list_sum(list_transform(range(0, {i} + 1), {m} -> pow({}, {i} - {m}) * ({})))", real_lit(ratio), f(&m))
            };
            Ok(mk(format!("to_json(list_transform(range(0, len({x})), {i} -> {body}))"), KqlType::Dynamic))
        }
        _ => err(format!("function '{name}' is not supported yet")),
    }
}

/// `to_json(list_transform(range(1, len(L) + 1), i -> body))` with `L` bound to the input list.
fn apply_list(x: &str, l: &str, i: &str, body: &str) -> String {
    let list = json_list(x);
    // bind the list once through a single-element list_transform
    format!("to_json(list_extract(list_transform([{list}], {l} -> list_transform(range(1, len({l}) + 1), {i} -> {body})), 1))")
}

/// Is the JSON element `e` a missing value (null, or equal to the placeholder)?
fn missing_sql(ctx: &Ctx, e: &str, placeholder: Option<&TExpr>) -> String {
    match placeholder {
        Some(p) if !p.is_null_const() => {
            if p.ty.is_numeric() {
                format!("COALESCE({} = {}, false)", num(e), ctx.d.cast(&p.sql, KqlType::Real))
            } else {
                let pj = ctx.to_dynamic(p.clone()).sql;
                format!("COALESCE(CAST({e} AS VARCHAR) = CAST({pj} AS VARCHAR), false)")
            }
        }
        _ => format!("({e} IS NULL OR json_type({e}) = 'NULL')"),
    }
}

/// One operand of an element-wise series operation: (element SQL at index `i`, list length).
fn operand(_ctx: &Ctx, t: &TExpr, list: &str, i: &str) -> (String, Option<String>) {
    if t.ty == KqlType::Dynamic {
        (format!("{list}[{i}]"), Some(format!("len({list})")))
    } else {
        (format!("CAST({} AS DOUBLE)", t.sql), None)
    }
}
