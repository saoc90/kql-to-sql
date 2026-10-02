//! Port of the C# `Comparator`: decides Match vs Mismatch between the Kusto oracle sample and the
//! translator→DuckDB result, tolerating differences that are not translator bugs (row order where
//! undefined, float noise, datetime precision, JSON key order / formatting, int-vs-real widening).

use std::fmt;

use serde_json::Value as Json;
use unicode_normalization::UnicodeNormalization;

use crate::analyzer::{Analysis, Mode};
use crate::value::{
    format_datetime, format_real, format_row, format_timespan, parse_datetime, parse_timespan, Class, ColumnInfo, Row,
    Value, TICKS_PER_MICRO,
};

#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub abs_epsilon: f64,
    pub rel_epsilon: f64,
    /// Compare JSON arrays order-insensitively (make_set/make_bag results).
    pub sort_json_arrays: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options { abs_epsilon: 1e-9, rel_epsilon: 1e-9, sort_json_arrays: false }
    }
}

/// Our per-record verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Outcome {
    Match,
    MismatchRows,
    MismatchColumns,
    MismatchOrder,
    TranslateError,
    SqlExecError,
    /// Kusto rejected the KQL and so did we (translate or execute) — the desired behaviour.
    KustoRejectedOurError,
    /// Kusto rejected the KQL but we produced a result.
    KustoRejectedWeAccepted,
    SkippedNondeterministic,
    /// Valid SQL, but DuckDB's runtime is stricter than Kusto on numeric domains.
    SkippedEngineError,
    /// The harness could not interpret the oracle record (unparseable sample rows).
    OracleUnreadable,
}

impl Outcome {
    pub fn name(self) -> &'static str {
        match self {
            Outcome::Match => "Match",
            Outcome::MismatchRows => "MismatchRows",
            Outcome::MismatchColumns => "MismatchColumns",
            Outcome::MismatchOrder => "MismatchOrder",
            Outcome::TranslateError => "TranslateError",
            Outcome::SqlExecError => "SqlExecError",
            Outcome::KustoRejectedOurError => "KustoRejected-OurError",
            Outcome::KustoRejectedWeAccepted => "KustoRejected-WeAccepted",
            Outcome::SkippedNondeterministic => "SkippedNondeterministic",
            Outcome::SkippedEngineError => "SkippedEngineError",
            Outcome::OracleUnreadable => "OracleUnreadable",
        }
    }

    /// Short column header for the by-family table.
    pub fn short(self) -> &'static str {
        match self {
            Outcome::Match => "Match",
            Outcome::MismatchRows => "MmRow",
            Outcome::MismatchColumns => "MmCol",
            Outcome::MismatchOrder => "MmOrd",
            Outcome::TranslateError => "TrErr",
            Outcome::SqlExecError => "SqlErr",
            Outcome::KustoRejectedOurError => "KR-Err",
            Outcome::KustoRejectedWeAccepted => "KR-Acc",
            Outcome::SkippedNondeterministic => "SkNdet",
            Outcome::SkippedEngineError => "SkEng",
            Outcome::OracleUnreadable => "OrUnr",
        }
    }

    /// Outcomes that count as "agreeing with Kusto".
    pub fn is_good(self) -> bool {
        matches!(self, Outcome::Match | Outcome::KustoRejectedOurError)
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub outcome: Outcome,
    pub detail: Option<String>,
    pub sub_verdicts: Vec<String>,
}

impl Verdict {
    pub fn new(outcome: Outcome, detail: Option<String>) -> Verdict {
        Verdict { outcome, detail, sub_verdicts: Vec::new() }
    }
}

/// The Kusto side as recovered from the verdict record.
#[derive(Debug, Clone)]
pub struct KustoSample {
    pub columns: Vec<ColumnInfo>,
    /// Total rows Kusto returned.
    pub row_count: usize,
    /// The first (up to 8) rows.
    pub rows: Vec<Row>,
}

/// The DuckDB side (all rows).
#[derive(Debug, Clone)]
pub struct DuckResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Row>,
}

/// Compares a successful DuckDB result against the Kusto sample (error handling is done by the
/// caller; this mirrors `Comparator.Compare` from the "columns" section on).
pub fn compare(analysis: &Analysis, kusto: &KustoSample, duck: &DuckResult, base: Options) -> Verdict {
    let opts = Options { sort_json_arrays: base.sort_json_arrays || analysis.set_semantics, ..base };
    let mut subs = Vec::new();
    let verdict = |outcome, detail: Option<String>, subs: Vec<String>| Verdict { outcome, detail, sub_verdicts: subs };

    // ---- columns ----
    if kusto.columns.len() != duck.columns.len() {
        return verdict(
            Outcome::MismatchColumns,
            Some(format!(
                "column count: kusto={} duck={} (kusto: {}; duck: {})",
                kusto.columns.len(),
                duck.columns.len(),
                names(&kusto.columns),
                names(&duck.columns)
            )),
            subs,
        );
    }
    if kusto.columns.iter().zip(&duck.columns).any(|(k, d)| k.name != d.name) {
        subs.push(format!("NAME_MISMATCH[kusto={} duck={}]", names(&kusto.columns), names(&duck.columns)));
    }
    for (k, d) in kusto.columns.iter().zip(&duck.columns) {
        if k.class != Class::Unknown && d.class != Class::Unknown && k.class != d.class {
            subs.push(format!("TYPE_MISMATCH[{}:{}|{}]", k.name, k.class, d.class));
        }
    }

    // ---- rows ----
    let key_idx: Vec<usize> = analysis
        .order_keys
        .iter()
        .filter_map(|k| kusto.columns.iter().position(|c| c.name.eq_ignore_ascii_case(k)))
        .collect();
    let ordered = analysis.mode == Mode::Ordered;

    if kusto.row_count != duck.rows.len() {
        return verdict(
            Outcome::MismatchRows,
            Some(format!("row count: kusto={} duck={}", kusto.row_count, duck.rows.len())),
            subs,
        );
    }

    let partial = kusto.rows.len() < kusto.row_count;
    if partial {
        // Only a prefix of the Kusto result is known.
        if ordered {
            let prefix = &duck.rows[..kusto.rows.len()];
            if ordered_prefix_equal(&kusto.rows, &duck.rows, &key_idx, &opts) {
                return verdict(Outcome::Match, None, subs);
            }
            if contains_all(&kusto.rows, &duck.rows, &opts) {
                return verdict(Outcome::MismatchOrder, Some("sample rows present but order differs".into()), subs);
            }
            return verdict(Outcome::MismatchRows, Some(first_row_diff(&kusto.rows, prefix, &opts)), subs);
        }
        if contains_all(&kusto.rows, &duck.rows, &opts) {
            return verdict(Outcome::Match, None, subs);
        }
        return verdict(Outcome::MismatchRows, Some(missing_row(&kusto.rows, &duck.rows, &opts)), subs);
    }

    if ordered {
        if ordered_equal(&kusto.rows, &duck.rows, &key_idx, &opts) {
            return verdict(Outcome::Match, None, subs);
        }
        if multiset_equal(&kusto.rows, &duck.rows, &opts) {
            return verdict(Outcome::MismatchOrder, Some("rows match as a set but order differs".into()), subs);
        }
        return verdict(Outcome::MismatchRows, Some(first_row_diff(&kusto.rows, &duck.rows, &opts)), subs);
    }
    if multiset_equal(&kusto.rows, &duck.rows, &opts) {
        return verdict(Outcome::Match, None, subs);
    }
    verdict(Outcome::MismatchRows, Some(missing_row(&kusto.rows, &duck.rows, &opts)), subs)
}

fn names(cols: &[ColumnInfo]) -> String {
    let n: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    format!("[{}]", n.join(", "))
}

// ---- row comparison ------------------------------------------------------------------------

fn row_equal(a: &[Value], b: &[Value], opts: &Options) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| cell_equal(x, y, opts))
}

/// Greedy one-to-one matching: every row of `a` is matched to a distinct row of `b`.
fn contains_all(a: &[Row], b: &[Row], opts: &Options) -> bool {
    let mut used = vec![false; b.len()];
    a.iter().all(|ra| match (0..b.len()).find(|&j| !used[j] && row_equal(ra, &b[j], opts)) {
        Some(j) => {
            used[j] = true;
            true
        }
        None => false,
    })
}

pub fn multiset_equal(a: &[Row], b: &[Row], opts: &Options) -> bool {
    a.len() == b.len() && contains_all(a, b, opts)
}

fn keys_equal(a: &[Value], b: &[Value], key_idx: &[usize], opts: &Options) -> bool {
    key_idx.iter().all(|&k| cell_equal(&a[k], &b[k], opts))
}

/// Length of the tie block starting at `a[i]` (rows sharing equal order keys); 1 without keys.
fn tie_block_len(a: &[Row], i: usize, key_idx: &[usize], opts: &Options) -> usize {
    if key_idx.is_empty() {
        return 1;
    }
    1 + a[i + 1..].iter().take_while(|r| keys_equal(&a[i], r, key_idx, opts)).count()
}

/// Ordered equality where rows within a tie block (equal order keys) may appear in any order.
pub fn ordered_equal(a: &[Row], b: &[Row], key_idx: &[usize], opts: &Options) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        let len = tie_block_len(a, i, key_idx, opts);
        let ok =
            if len == 1 { row_equal(&a[i], &b[i], opts) } else { multiset_equal(&a[i..i + len], &b[i..i + len], opts) };
        if !ok {
            return false;
        }
        i += len;
    }
    true
}

/// Like [`ordered_equal`] for a known prefix `a` of the full Kusto result. A tie block that reaches
/// the end of the prefix may continue beyond it, so its rows only need to appear among the `b` rows
/// from the block start that share its keys.
fn ordered_prefix_equal(a: &[Row], b: &[Row], key_idx: &[usize], opts: &Options) -> bool {
    if b.len() < a.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        let len = tie_block_len(a, i, key_idx, opts);
        let last_block = i + len == a.len();
        let ok = if last_block && !key_idx.is_empty() {
            let tail: Vec<Row> = b[i..].iter().take_while(|r| keys_equal(&a[i], r, key_idx, opts)).cloned().collect();
            contains_all(&a[i..], &tail, opts)
        } else if len == 1 {
            row_equal(&a[i], &b[i], opts)
        } else {
            multiset_equal(&a[i..i + len], &b[i..i + len], opts)
        };
        if !ok {
            return false;
        }
        i += len;
    }
    true
}

fn first_row_diff(kusto: &[Row], duck: &[Row], opts: &Options) -> String {
    for (i, (k, d)) in kusto.iter().zip(duck).enumerate() {
        if !row_equal(k, d, opts) {
            return format!("first differing row[{i}]: kusto={} duck={}", format_row(k), format_row(d));
        }
    }
    format!("rows differ (kusto={}, duck={})", kusto.len(), duck.len())
}

/// Detail for a multiset mismatch: the first Kusto row with no counterpart, plus a DuckDB row.
fn missing_row(kusto: &[Row], duck: &[Row], opts: &Options) -> String {
    let mut used = vec![false; duck.len()];
    for (i, k) in kusto.iter().enumerate() {
        match (0..duck.len()).find(|&j| !used[j] && row_equal(k, &duck[j], opts)) {
            Some(j) => used[j] = true,
            None => {
                let near = duck.get(i).or(duck.first()).map(|r| format_row(r)).unwrap_or_else(|| "<none>".into());
                return format!("kusto row[{i}] {} not in duck result (duck row[{i}]={near})", format_row(k));
            }
        }
    }
    first_row_diff(kusto, duck, opts)
}

// ---- cell comparison -----------------------------------------------------------------------

/// Port of `Comparator.CellEqual`.
pub fn cell_equal(a: &Value, b: &Value, opts: &Options) -> bool {
    use Value::*;
    match (a, b) {
        (Null, Null) => return true,
        (Null, _) | (_, Null) => return false,
        _ => {}
    }

    // dynamic / JSON
    if matches!(a, Json(_)) || matches!(b, Json(_)) {
        return json_equal(&json_side(a), &json_side(b), opts.sort_json_arrays);
    }
    // Both JSON-text strings (tostring(dynamic) on both engines): tolerate formatting / key order.
    if let (Str(x), Str(y)) = (a, b) {
        if looks_structured_json(x) && looks_structured_json(y) {
            return json_equal(&json_side(a), &json_side(b), opts.sort_json_arrays);
        }
    }

    // numeric (int/long/real/decimal widening)
    if is_numeric(a) && is_numeric(b) {
        return numeric_equal(a, b, opts);
    }

    if matches!(a, DateTime(_)) || matches!(b, DateTime(_)) {
        return match (as_datetime(a), as_datetime(b)) {
            (Some(x), Some(y)) => (x.div_euclid(TICKS_PER_MICRO) - y.div_euclid(TICKS_PER_MICRO)).abs() <= 1,
            _ => false,
        };
    }

    if matches!(a, TimeSpan(_)) || matches!(b, TimeSpan(_)) {
        return match (as_timespan(a), as_timespan(b)) {
            (Some(x), Some(y)) => (x as i128 - y as i128).abs() <= 10,
            _ => false,
        };
    }

    if let (Bool(x), Bool(y)) = (a, b) {
        return x == y;
    }

    normalize_string(a) == normalize_string(b)
}

fn is_numeric(v: &Value) -> bool {
    matches!(v, Value::Int(_) | Value::Real(_))
}

fn numeric_equal(a: &Value, b: &Value, opts: &Options) -> bool {
    let to_f64 = |v: &Value| match v {
        Value::Int(i) => *i as f64,
        Value::Real(r) => *r,
        _ => unreachable!("numeric_equal on non-numeric"),
    };
    if let (Value::Int(x), Value::Int(y)) = (a, b) {
        return x == y;
    }
    let (x, y) = (to_f64(a), to_f64(b));
    if x.is_nan() || y.is_nan() {
        return x.is_nan() && y.is_nan();
    }
    if x.is_infinite() || y.is_infinite() {
        return x == y;
    }
    (x - y).abs() <= opts.abs_epsilon + opts.rel_epsilon * x.abs().max(y.abs())
}

fn as_datetime(v: &Value) -> Option<i64> {
    match v {
        Value::DateTime(t) => Some(*t),
        Value::Str(s) => parse_datetime(s),
        _ => None,
    }
}

fn as_timespan(v: &Value) -> Option<i64> {
    match v {
        Value::TimeSpan(t) => Some(*t),
        Value::Str(s) => parse_timespan(s),
        Value::Int(i) => i64::try_from(*i).ok(),
        _ => None,
    }
}

fn normalize_string(v: &Value) -> String {
    let s = match v {
        Value::Str(s) | Value::Guid(s) => s.clone(),
        Value::Bool(b) => (if *b { "true" } else { "false" }).to_string(),
        Value::DateTime(t) => format_datetime(*t),
        Value::TimeSpan(t) => format_timespan(*t),
        Value::Int(i) => i.to_string(),
        Value::Real(r) => format_real(*r),
        Value::Json(j) => j.to_string(),
        Value::Null => String::new(),
    };
    s.nfc().collect()
}

// ---- JSON / dynamic ------------------------------------------------------------------------

/// One side of a JSON comparison: valid JSON, or text that is not JSON.
enum JsonSide {
    Parsed(Json),
    Raw(String),
}

/// Mirrors C# `ToJsonText` followed by `JsonDocument.Parse`: strings are parsed as JSON text,
/// other scalars are serialized.
fn json_side(v: &Value) -> JsonSide {
    match v {
        Value::Json(j) => JsonSide::Parsed(j.clone()),
        Value::Null => JsonSide::Parsed(Json::Null),
        Value::Str(s) => match serde_json::from_str::<Json>(s) {
            Ok(j) => JsonSide::Parsed(j),
            Err(_) => JsonSide::Raw(s.trim().to_string()),
        },
        Value::Bool(b) => JsonSide::Parsed(Json::Bool(*b)),
        Value::Int(i) => JsonSide::Parsed(i64::try_from(*i).map(Json::from).unwrap_or_else(|_| Json::from(*i as f64))),
        Value::Real(r) => match serde_json::Number::from_f64(*r) {
            Some(n) => JsonSide::Parsed(Json::Number(n)),
            None => JsonSide::Raw(format_real(*r)),
        },
        Value::DateTime(t) => JsonSide::Parsed(Json::String(format_datetime(*t))),
        Value::TimeSpan(t) => JsonSide::Parsed(Json::String(format_timespan(*t))),
        Value::Guid(g) => JsonSide::Parsed(Json::String(g.clone())),
    }
}

fn json_equal(a: &JsonSide, b: &JsonSide, sort_arrays: bool) -> bool {
    match (a, b) {
        (JsonSide::Parsed(x), JsonSide::Parsed(y)) => canonical_json(x, sort_arrays) == canonical_json(y, sort_arrays),
        // A JSON string scalar ("x") equals the same text as a plain (non-JSON) string (x).
        _ => {
            let unwrap = |s: &JsonSide| match s {
                JsonSide::Parsed(Json::String(t)) => t.clone(),
                JsonSide::Parsed(j) => canonical_json(j, sort_arrays),
                JsonSide::Raw(r) => r.clone(),
            };
            unwrap(a) == unwrap(b)
        }
    }
}

fn looks_structured_json(s: &str) -> bool {
    let t = s.trim();
    t.len() >= 2 && (t.starts_with('{') || t.starts_with('['))
}

/// Canonical text: object keys sorted, numbers normalized through f64, arrays optionally sorted.
pub fn canonical_json(v: &Json, sort_arrays: bool) -> String {
    let mut out = String::new();
    write_canonical(v, sort_arrays, &mut out);
    out
}

fn write_canonical(v: &Json, sort_arrays: bool, out: &mut String) {
    match v {
        Json::Null => out.push_str("null"),
        Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Json::Number(n) => match n.as_f64() {
            Some(f) => out.push_str(&format_real(f)),
            None => out.push_str(&n.to_string()),
        },
        Json::String(s) => out.push_str(&Json::String(s.nfc().collect()).to_string()),
        Json::Array(items) => {
            let mut parts: Vec<String> = items.iter().map(|i| canonical_json(i, sort_arrays)).collect();
            if sort_arrays {
                parts.sort();
            }
            out.push('[');
            out.push_str(&parts.join(","));
            out.push(']');
        }
        Json::Object(map) => {
            let mut entries: Vec<(&String, &Json)> = map.iter().collect();
            entries.sort_by(|x, y| x.0.cmp(y.0));
            out.push('{');
            for (i, (k, val)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Json::String(k.clone()).to_string());
                out.push(':');
                write_canonical(val, sort_arrays, out);
            }
            out.push('}');
        }
    }
}

// ---- error classification ------------------------------------------------------------------

/// DuckDB *runtime* errors reflecting a stricter numeric domain than Kusto (which wraps overflow
/// or returns NaN/±inf) rather than invalid SQL. Binder/parser/catalog errors are bugs.
pub fn is_engine_domain_error(err: &str) -> bool {
    const MARKERS: &[&str] = &[
        "out of range error",
        "overflow in",
        "cannot take square root",
        "cannot take logarithm",
        "can't be cast because",
        "date out of range",
    ];
    let lower = err.to_lowercase();
    MARKERS.iter().any(|m| lower.contains(m))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use Value::*;

    fn eq(a: Value, b: Value) -> bool {
        cell_equal(&a, &b, &Options::default())
    }

    fn analysis(mode: Mode, keys: &[&str]) -> Analysis {
        Analysis {
            mode,
            nondeterministic: false,
            set_semantics: false,
            order_keys: keys.iter().map(|k| k.to_string()).collect(),
        }
    }

    fn cols(spec: &[(&str, Class)]) -> Vec<ColumnInfo> {
        spec.iter().map(|(n, c)| ColumnInfo { name: n.to_string(), class: *c }).collect()
    }

    #[test]
    fn numbers() {
        assert!(eq(Int(3), Real(3.0)));
        assert!(eq(Real(0.1 + 0.2), Real(0.3)));
        assert!(!eq(Real(1.0), Real(1.001)));
        assert!(eq(Real(f64::NAN), Real(f64::NAN)));
        assert!(!eq(Real(f64::NAN), Real(1.0)));
        assert!(eq(Real(f64::INFINITY), Real(f64::INFINITY)));
        assert!(!eq(Real(f64::INFINITY), Real(f64::NEG_INFINITY)));
        assert!(eq(Int(i64::MAX as i128), Int(i64::MAX as i128)));
        assert!(!eq(Int(1), Int(2)));
        assert!(eq(Real(1e20), Real(1.0000000000000001e20)));
    }

    #[test]
    fn nulls() {
        assert!(eq(Null, Null));
        assert!(!eq(Null, Str(String::new())));
        assert!(!eq(Int(0), Null));
    }

    #[test]
    fn datetimes_and_timespans() {
        assert!(eq(DateTime(10_000_000), DateTime(10_000_009)));
        assert!(eq(DateTime(0), DateTime(19)));
        assert!(!eq(DateTime(0), DateTime(25)));
        assert!(eq(DateTime(0), Str("1970-01-01T00:00:00Z".into())));
        assert!(eq(TimeSpan(100), TimeSpan(110)));
        assert!(!eq(TimeSpan(100), TimeSpan(111)));
        assert!(eq(TimeSpan(36_000_000_000), Str("01:00:00".into())));
        assert!(eq(TimeSpan(5), Int(5)));
    }

    #[test]
    fn json() {
        assert!(eq(Json(json!({"b": 1, "a": [1, 2]})), Json(json!({"a": [1.0, 2], "b": 1.0}))));
        assert!(!eq(Json(json!([1, 2])), Json(json!([2, 1]))));
        let set = Options { sort_json_arrays: true, ..Options::default() };
        assert!(cell_equal(&Json(json!([1, 2])), &Json(json!([2, 1])), &set));
        // JSON text from DuckDB vs parsed dynamic
        assert!(eq(Json(json!({"a": 1})), Str("{\"a\": 1}".into())));
        // JSON string scalar vs plain string
        assert!(eq(Json(json!("hello")), Str("hello".into())));
        assert!(eq(Json(json!("he said \"x\"")), Str("he said \"x\"".into())));
        assert!(eq(Json(json!(5)), Int(5)));
        assert!(!eq(Json(json!(5)), Str("6".into())));
        // Structured JSON in plain strings on both sides
        assert!(eq(Str("{\"b\":1,\"a\":2}".into()), Str("{ \"a\": 2, \"b\": 1 }".into())));
        // ...but scalar strings stay strict
        assert!(!eq(Str("1".into()), Str("1.0".into())));
    }

    #[test]
    fn strings_guids_bools() {
        assert!(eq(Str("abc".into()), Str("abc".into())));
        assert!(!eq(Str("abc".into()), Str("ABC".into())));
        assert!(eq(Str("e\u{301}".into()), Str("\u{e9}".into())), "NFC");
        assert!(eq(
            Guid("550e8400-e29b-41d4-a716-446655440000".into()),
            Str("550e8400-e29b-41d4-a716-446655440000".into())
        ));
        assert!(eq(Bool(true), Bool(true)));
        assert!(!eq(Bool(true), Bool(false)));
        assert!(eq(Bool(true), Str("true".into())));
    }

    fn kusto(columns: Vec<ColumnInfo>, row_count: usize, rows: Vec<Row>) -> KustoSample {
        KustoSample { columns, row_count, rows }
    }

    #[test]
    fn multiset_vs_ordered() {
        let c = cols(&[("k", Class::Int), ("v", Class::Int)]);
        let k = kusto(c.clone(), 2, vec![vec![Int(1), Int(10)], vec![Int(2), Int(20)]]);
        let d = DuckResult { columns: c, rows: vec![vec![Int(2), Int(20)], vec![Int(1), Int(10)]] };
        let o = Options::default();
        assert_eq!(compare(&analysis(Mode::Multiset, &[]), &k, &d, o).outcome, Outcome::Match);
        assert_eq!(compare(&analysis(Mode::Ordered, &["k"]), &k, &d, o).outcome, Outcome::MismatchOrder);
    }

    #[test]
    fn tie_blocks_relax_order() {
        let c = cols(&[("k", Class::Int), ("v", Class::String)]);
        let rows = |vs: &[(i128, &str)]| vs.iter().map(|(k, v)| vec![Int(*k), Str(v.to_string())]).collect::<Vec<_>>();
        let k = kusto(c.clone(), 3, rows(&[(1, "a"), (1, "b"), (2, "c")]));
        let d = DuckResult { columns: c, rows: rows(&[(1, "b"), (1, "a"), (2, "c")]) };
        let o = Options::default();
        assert_eq!(compare(&analysis(Mode::Ordered, &["k"]), &k, &d, o).outcome, Outcome::Match);
        // Without known keys the order is strict.
        assert_eq!(compare(&analysis(Mode::Ordered, &[]), &k, &d, o).outcome, Outcome::MismatchOrder);
    }

    #[test]
    fn columns_and_names() {
        let k = kusto(cols(&[("a", Class::Int)]), 1, vec![vec![Int(1)]]);
        let d = DuckResult { columns: cols(&[("a", Class::Int), ("b", Class::Int)]), rows: vec![vec![Int(1), Int(2)]] };
        assert_eq!(
            compare(&analysis(Mode::Multiset, &[]), &k, &d, Options::default()).outcome,
            Outcome::MismatchColumns
        );

        let d = DuckResult { columns: cols(&[("x", Class::Real)]), rows: vec![vec![Real(1.0)]] };
        let v = compare(&analysis(Mode::Multiset, &[]), &k, &d, Options::default());
        assert_eq!(v.outcome, Outcome::Match);
        assert!(v.sub_verdicts.iter().any(|s| s.starts_with("NAME_MISMATCH")));
        assert!(v.sub_verdicts.iter().any(|s| s.starts_with("TYPE_MISMATCH")));
    }

    #[test]
    fn row_count_and_values() {
        let c = cols(&[("a", Class::Int)]);
        let k = kusto(c.clone(), 2, vec![vec![Int(1)], vec![Int(2)]]);
        let d = DuckResult { columns: c.clone(), rows: vec![vec![Int(1)]] };
        assert_eq!(compare(&analysis(Mode::Multiset, &[]), &k, &d, Options::default()).outcome, Outcome::MismatchRows);
        let d = DuckResult { columns: c, rows: vec![vec![Int(1)], vec![Int(3)]] };
        assert_eq!(compare(&analysis(Mode::Multiset, &[]), &k, &d, Options::default()).outcome, Outcome::MismatchRows);
    }

    #[test]
    fn partial_samples() {
        let c = cols(&[("a", Class::Int)]);
        let all: Vec<Row> = (0..20).map(|i| vec![Int(i)]).collect();
        let k = kusto(c.clone(), 20, all[..8].to_vec());
        let mut shuffled = all.clone();
        shuffled.reverse();
        let o = Options::default();
        let d = DuckResult { columns: c.clone(), rows: shuffled };
        assert_eq!(compare(&analysis(Mode::Multiset, &[]), &k, &d, o).outcome, Outcome::Match);
        assert_eq!(compare(&analysis(Mode::Ordered, &["a"]), &k, &d, o).outcome, Outcome::MismatchOrder);
        let d = DuckResult { columns: c.clone(), rows: all.clone() };
        assert_eq!(compare(&analysis(Mode::Ordered, &["a"]), &k, &d, o).outcome, Outcome::Match);
        let d = DuckResult { columns: c.clone(), rows: all[..19].to_vec() };
        assert_eq!(compare(&analysis(Mode::Multiset, &[]), &k, &d, o).outcome, Outcome::MismatchRows);

        // A tie block straddling the sample boundary: kusto shows two of four tied rows.
        let k = kusto(c.clone(), 4, vec![vec![Int(1)], vec![Int(1)]]);
        let tied = cols(&[("a", Class::Int)]);
        let d = DuckResult { columns: tied, rows: vec![vec![Int(1)]; 4] };
        assert_eq!(compare(&analysis(Mode::Ordered, &["a"]), &k, &d, o).outcome, Outcome::Match);
    }

    #[test]
    fn engine_domain_errors() {
        assert!(is_engine_domain_error("Out of Range Error: Overflow in addition of INT64"));
        assert!(is_engine_domain_error("Invalid Input Error: cannot take logarithm of zero"));
        assert!(!is_engine_domain_error("Binder Error: column not found"));
    }
}
