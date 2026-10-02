//! PGlite engine: runs translated (Postgres-dialect) SQL in PGlite — PostgreSQL compiled to
//! WASI, hosted in-process by `pglite-oxide` (no Node.js) — and converts its lossless text cells
//! into [`Value`]s using the translator's declared Kusto column types.
//!
//! Statements are executed in one batch on one instance because starting PGlite costs ~1 s.

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use pglite_oxide::{Pglite, QueryOptions, RowMode, TypeParser};
use serde::Deserialize;
use serde_json::Value as Json;

use crate::compare::DuckResult;
use crate::value::{parse_datetime, parse_timespan, Class, ColumnInfo, Value, TICKS_PER_DAY, TICKS_PER_SECOND};

// PostgreSQL type OIDs we interpret (pg_type.oid).
const BOOL: u32 = 16;
const INT8: u32 = 20;
const INT2: u32 = 21;
const INT4: u32 = 23;
const OID: u32 = 26;
const JSON_OID: u32 = 114;
const FLOAT4: u32 = 700;
const FLOAT8: u32 = 701;
const DATE: u32 = 1082;
const TIME: u32 = 1083;
const TIMESTAMP: u32 = 1114;
const TIMESTAMPTZ: u32 = 1184;
const INTERVAL: u32 = 1186;
const NUMERIC: u32 = 1700;
const UUID: u32 = 2950;
const JSONB: u32 = 3802;

/// One result as written by the runner.
#[derive(Debug, Clone, Deserialize)]
pub struct PgRaw {
    pub id: String,
    #[serde(default)]
    pub columns: Vec<PgColumn>,
    #[serde(default)]
    pub rows: Vec<Vec<Option<String>>>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PgColumn {
    pub name: String,
    pub type_oid: u32,
}

/// Extra per-instance initialization.
#[derive(Default)]
pub struct Setup {
    /// SQL run once before the batch (CREATE TABLE ...).
    pub sql: Option<String>,
    /// `(table, csv path)`: `COPY table FROM` a (optionally gzipped) CSV with a header line.
    pub copies: Vec<(String, PathBuf)>,
}

/// Results are fetched through a cursor in chunks, so a huge result (e.g. a full StormEvents
/// projection) never has to fit into one protocol message.
const CHUNK: usize = 2000;

/// One PGlite instance with the session settings and setup applied.
struct Instance {
    db: Pglite,
    text: QueryOptions,
}

impl Instance {
    fn open(setup: &Setup, blobs: &[(String, Vec<u8>)]) -> Result<Instance, String> {
        let mut db = Pglite::temporary().map_err(|e| format!("cannot start PGlite: {e}"))?;
        db.exec("SET TIME ZONE 'UTC'; SET intervalstyle = 'postgres'; SET extra_float_digits = 1;", None)
            .map_err(|e| e.to_string())?;
        if let Some(sql) = &setup.sql {
            db.exec(sql, None).map_err(|e| format!("setup: {e}"))?;
        }
        for (table, blob) in blobs {
            let opts = QueryOptions { blob: Some(blob.clone()), ..QueryOptions::default() };
            db.exec(&format!("COPY {table} FROM '/dev/blob' WITH (FORMAT csv, HEADER true)"), Some(&opts))
                .map_err(|e| format!("COPY {table}: {e}"))?;
        }
        // Every cell stays PostgreSQL's own text output, so nothing is lossy (int8 beyond 2^53,
        // NaN/Infinity, numeric, microsecond timestamps, intervals, jsonb); NULL stays null.
        let mut text = QueryOptions { row_mode: Some(RowMode::Array), ..QueryOptions::default() };
        let as_text: TypeParser = Arc::new(|s: &str, _| Json::String(s.to_string()));
        for oid in 16..10000 {
            text.parsers.insert(oid, as_text.clone());
        }
        Ok(Instance { db, text })
    }

    /// Runs one statement inside BEGIN ... ROLLBACK so side effects do not leak into the next.
    fn run(&mut self, id: &str, sql: &str) -> PgRaw {
        let r = self.db.exec("BEGIN", None).map_err(|e| e.to_string()).and_then(|_| self.fetch(sql));
        let _ = self.db.exec("ROLLBACK", None);
        match r {
            Ok((columns, rows)) => PgRaw { id: id.into(), columns, rows, error: None },
            Err(e) => PgRaw { id: id.into(), columns: vec![], rows: vec![], error: Some(e) },
        }
    }

    fn fetch(&mut self, sql: &str) -> Result<(Vec<PgColumn>, Vec<Vec<Option<String>>>), String> {
        self.db
            .exec(&format!("DECLARE kql_oracle_cursor NO SCROLL CURSOR FOR {sql}"), None)
            .map_err(|e| e.to_string())?;
        let mut columns = None;
        let mut rows = Vec::new();
        loop {
            let res = self
                .db
                .query(&format!("FETCH FORWARD {CHUNK} FROM kql_oracle_cursor"), &[], Some(&self.text))
                .map_err(|e| e.to_string())?;
            columns.get_or_insert_with(|| {
                res.fields.iter().map(|f| PgColumn { name: f.name.clone(), type_oid: f.data_type_id as u32 }).collect()
            });
            let n = res.rows.len();
            for row in res.rows {
                let Json::Array(cells) = row else { return Err("PGlite returned a non-array row".into()) };
                rows.push(cells.into_iter().map(|c| c.as_str().map(str::to_string)).collect());
            }
            if n < CHUNK {
                break;
            }
        }
        Ok((columns.unwrap_or_default(), rows))
    }
}

/// Runs every `(id, sql)` statement in one in-process PGlite instance (PGlite's WASI build
/// through `pglite-oxide`, no JavaScript runtime) and returns the results keyed by id.
pub fn run_batch(stmts: &[(String, String)], setup: &Setup) -> Result<HashMap<String, PgRaw>, String> {
    let mut blobs = Vec::new();
    for (table, csv) in &setup.copies {
        let bytes = fs::read(csv).map_err(|e| format!("{}: {e}", csv.display()))?;
        let bytes = if csv.extension().is_some_and(|e| e == "gz") {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(&bytes[..])
                .read_to_end(&mut out)
                .map_err(|e| format!("{}: {e}", csv.display()))?;
            out
        } else {
            bytes
        };
        blobs.push((table.clone(), bytes));
    }
    let started = std::time::Instant::now();
    let mut inst = Instance::open(setup, &blobs)?;
    let mut map = HashMap::new();
    for (i, (id, sql)) in stmts.iter().enumerate() {
        let raw = inst.run(id, sql);
        // an error can (rarely) leave the instance unusable; replace it
        if raw.error.is_some() && inst.db.exec("SELECT 1", None).is_err() {
            inst = Instance::open(setup, &blobs)?;
        }
        map.insert(id.clone(), raw);
        if (i + 1) % 250 == 0 {
            eprintln!("pglite: {}/{}", i + 1, stmts.len());
        }
    }
    let _ = inst.db.close();
    eprintln!("pglite: {} statements in {:.1}s", stmts.len(), started.elapsed().as_secs_f64());
    Ok(map)
}

/// Converts a runner result, classifying columns by the translator's declared types when the
/// column count agrees (same policy as the DuckDB engine).
pub fn to_result(raw: &PgRaw, declared: &[Class]) -> Result<DuckResult, String> {
    if let Some(e) = &raw.error {
        return Err(e.clone());
    }
    let ncols = raw.columns.len();
    let classes: Vec<Class> = if declared.len() == ncols { declared.to_vec() } else { vec![Class::Unknown; ncols] };
    let mut rows = Vec::with_capacity(raw.rows.len());
    for r in &raw.rows {
        if r.len() != ncols {
            return Err(format!("runner returned a row with {} cells for {ncols} columns", r.len()));
        }
        rows.push(
            r.iter()
                .zip(&raw.columns)
                .zip(&classes)
                .map(|((cell, col), &class)| convert(cell.as_deref(), col.type_oid, class))
                .collect::<Vec<_>>(),
        );
    }
    let columns = raw
        .columns
        .iter()
        .zip(classes)
        .map(|(c, class)| ColumnInfo {
            name: c.name.clone(),
            class: if class == Class::Unknown { oid_class(c.type_oid) } else { class },
        })
        .collect();
    Ok(DuckResult { columns, rows })
}

/// Class of an undescribed column, from its PostgreSQL type.
fn oid_class(oid: u32) -> Class {
    match oid {
        BOOL => Class::Bool,
        INT2 | INT4 | INT8 | OID => Class::Int,
        FLOAT4 | FLOAT8 | NUMERIC => Class::Real,
        DATE | TIMESTAMP | TIMESTAMPTZ => Class::DateTime,
        TIME | INTERVAL => Class::TimeSpan,
        UUID => Class::Guid,
        JSON_OID | JSONB => Class::Dynamic,
        _ => Class::String,
    }
}

/// Converts one PostgreSQL text cell, guided by the declared Kusto class.
pub fn convert(cell: Option<&str>, oid: u32, class: Class) -> Value {
    let Some(text) = cell else { return Value::Null };
    match class {
        Class::Dynamic => match oid {
            JSON_OID | JSONB | 25 | 1043 | 1042 | 19 => match serde_json::from_str::<Json>(text) {
                Ok(Json::Null) => Value::Null,
                Ok(j) => Value::Json(j),
                Err(_) => Value::Str(text.to_string()),
            },
            _ => match scalar_json(scalar(text, oid)) {
                Json::Null => Value::Null,
                j => Value::Json(j),
            },
        },
        Class::Guid => match oid {
            UUID | 25 | 1043 => Value::Guid(text.to_ascii_lowercase()),
            _ => scalar(text, oid),
        },
        Class::DateTime => match scalar(text, oid) {
            Value::Str(s) => parse_datetime(&s).map(Value::DateTime).unwrap_or(Value::Str(s)),
            other => other,
        },
        Class::Bool => match scalar(text, oid) {
            Value::Str(s) if s.eq_ignore_ascii_case("true") => Value::Bool(true),
            Value::Str(s) if s.eq_ignore_ascii_case("false") => Value::Bool(false),
            other => other,
        },
        _ => scalar(text, oid),
    }
}

/// Type-directed conversion of a PostgreSQL text value; unknown types stay strings.
fn scalar(text: &str, oid: u32) -> Value {
    let s = || Value::Str(text.to_string());
    match oid {
        BOOL => match text {
            "t" | "true" => Value::Bool(true),
            "f" | "false" => Value::Bool(false),
            _ => s(),
        },
        INT2 | INT4 | INT8 | OID => text.parse::<i128>().map(Value::Int).unwrap_or_else(|_| s()),
        FLOAT4 | FLOAT8 | NUMERIC => parse_float(text).map(Value::Real).unwrap_or_else(s),
        DATE | TIMESTAMP | TIMESTAMPTZ => parse_pg_timestamp(text).map(Value::DateTime).unwrap_or_else(s),
        TIME => parse_timespan(text).map(Value::TimeSpan).unwrap_or_else(s),
        INTERVAL => parse_interval(text).map(Value::TimeSpan).unwrap_or_else(s),
        UUID => Value::Guid(text.to_ascii_lowercase()),
        JSON_OID | JSONB => match serde_json::from_str::<Json>(text) {
            Ok(Json::Null) => Value::Null,
            Ok(j) => Value::Json(j),
            Err(_) => s(),
        },
        _ => s(),
    }
}

/// PostgreSQL float/numeric text: decimal or exponent notation, `NaN`, `Infinity`, `-Infinity`.
fn parse_float(text: &str) -> Option<f64> {
    match text {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ => text.parse().ok(),
    }
}

/// `YYYY-MM-DD[ hh:mm:ss[.ffffff]][+hh[:mm]]` (the session runs in UTC). BC dates and
/// `infinity` are not representable as ticks and yield `None`.
pub fn parse_pg_timestamp(text: &str) -> Option<i64> {
    let t = text.trim();
    if t.ends_with(" BC") {
        return None;
    }
    // A timestamptz offset: "+00", "-05", "+05:30" after the time part.
    let (base, offset_secs) = match t.get(10..).and_then(|rest| rest.rfind(['+', '-']).map(|i| i + 10)) {
        Some(i) if t.len() > 10 && t[i..].len() >= 3 => {
            let off = &t[i + 1..];
            let (h, m) = off.split_once(':').unwrap_or((off, "0"));
            let secs = h.parse::<i64>().ok()? * 3600 + m.parse::<i64>().ok()? * 60;
            (&t[..i], if t.as_bytes()[i] == b'-' { -secs } else { secs })
        }
        _ => (t, 0),
    };
    parse_datetime(base).map(|ticks| ticks - offset_secs * TICKS_PER_SECOND)
}

/// Parses PostgreSQL `intervalstyle = postgres` output into ticks, e.g. `1 day 02:00:00`,
/// `-00:00:01.5`, `-3 days -00:00:01.5`, `1 year 2 mons -1 days +02:00:00`, `100000:00:00`.
/// A month counts as 30 days and a year as 12 months (the same normalization DuckDB uses).
pub fn parse_interval(text: &str) -> Option<i64> {
    let mut ticks: i128 = 0;
    let mut tokens = text.split_whitespace().peekable();
    let mut seen = false;
    while let Some(tok) = tokens.next() {
        seen = true;
        if tok.contains(':') {
            ticks += clock_ticks(tok)?;
            continue;
        }
        let n: i128 = tok.parse().ok()?;
        let unit = tokens.next()?;
        let days = match unit.trim_end_matches('s') {
            "year" => 360,
            "mon" => 30,
            "day" => 1,
            _ => return None,
        };
        ticks += n * days * TICKS_PER_DAY as i128;
    }
    if !seen {
        return None;
    }
    i64::try_from(ticks).ok()
}

/// `[+-]h+:mm[:ss[.ffffff]]` → ticks (hours are unbounded in interval output).
fn clock_ticks(tok: &str) -> Option<i128> {
    let (neg, body) = match tok.as_bytes().first()? {
        b'-' => (true, &tok[1..]),
        b'+' => (false, &tok[1..]),
        _ => (false, tok),
    };
    let parts: Vec<&str> = body.split(':').collect();
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit());
    let (h, m, sec) = match parts.as_slice() {
        [h, m] => (*h, *m, "0"),
        [h, m, s] => (*h, *m, *s),
        _ => return None,
    };
    if !digits(h) || !digits(m) {
        return None;
    }
    let (whole, frac) = sec.split_once('.').unwrap_or((sec, ""));
    if !digits(whole) || !(frac.is_empty() || digits(frac)) {
        return None;
    }
    let mut frac7: String = frac.chars().take(7).collect();
    while frac7.len() < 7 {
        frac7.push('0');
    }
    let secs = h.parse::<i128>().ok()? * 3600 + m.parse::<i128>().ok()? * 60 + whole.parse::<i128>().ok()?;
    let t = secs * TICKS_PER_SECOND as i128 + frac7.parse::<i128>().ok()?;
    Some(if neg { -t } else { t })
}

/// A scalar as a JSON value (for dynamic columns that arrive as a non-JSON type).
fn scalar_json(v: Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(b),
        Value::Int(i) => i64::try_from(i).map(Json::from).unwrap_or_else(|_| float_json(i as f64)),
        Value::Real(r) => float_json(r),
        Value::Str(s) | Value::Guid(s) => Json::String(s),
        Value::DateTime(t) => Json::String(crate::value::format_datetime(t)),
        Value::TimeSpan(t) => Json::String(crate::value::format_timespan(t)),
        Value::Json(j) => j,
    }
}

fn float_json(f: f64) -> Json {
    serde_json::Number::from_f64(f).map(Json::Number).unwrap_or(Json::Null)
}

/// PostgreSQL *runtime* errors reflecting a stricter numeric domain than Kusto (which wraps
/// overflow or returns NaN/±inf/null) rather than invalid SQL. Parse, binder and catalog errors
/// (`syntax error`, `does not exist`, `operator does not exist`, ...) stay `SqlExecError`.
/// `division by zero` is deliberately *not* listed: Kusto defines it (null/±inf) and the dialect
/// can guard it, so it is a translator gap.
pub fn is_engine_domain_error(err: &str) -> bool {
    const MARKERS: &[&str] = &[
        "out of range",
        "value overflows numeric format",
        "cannot take logarithm",
        "cannot take square root",
        "a negative number raised to a non-integer power",
        "zero raised to a negative power",
    ];
    let lower = err.to_lowercase();
    MARKERS.iter().any(|m| lower.contains(m))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::TICKS_PER_MICRO;
    use serde_json::json;

    const H: i64 = 3600 * TICKS_PER_SECOND;

    #[test]
    fn intervals() {
        assert_eq!(parse_interval("00:00:00"), Some(0));
        assert_eq!(parse_interval("1 day 02:00:00"), Some(TICKS_PER_DAY + 2 * H));
        assert_eq!(parse_interval("-00:00:01.5"), Some(-15_000_000));
        assert_eq!(parse_interval("-3 days -00:00:01.5"), Some(-3 * TICKS_PER_DAY - 15_000_000));
        assert_eq!(parse_interval("-1 days +02:00:00"), Some(-TICKS_PER_DAY + 2 * H));
        assert_eq!(parse_interval("100000:00:00"), Some(100_000 * H));
        assert_eq!(parse_interval("1 year 2 mons"), Some(420 * TICKS_PER_DAY));
        assert_eq!(parse_interval("2 days"), Some(2 * TICKS_PER_DAY));
        assert_eq!(parse_interval("00:00:00.000001"), Some(TICKS_PER_MICRO));
        assert_eq!(parse_interval("12:34"), Some(12 * H + 34 * 60 * TICKS_PER_SECOND));
        assert_eq!(parse_interval(""), None);
        assert_eq!(parse_interval("1 fortnight"), None);
        assert_eq!(parse_interval("garbage"), None);
    }

    #[test]
    fn timestamps() {
        let base = parse_datetime("2007-01-02T03:04:05.123456").unwrap();
        assert_eq!(parse_pg_timestamp("2007-01-02 03:04:05.123456"), Some(base));
        assert_eq!(parse_pg_timestamp("2007-01-02 03:04:05.123456+00"), Some(base));
        assert_eq!(parse_pg_timestamp("2007-01-02 05:04:05.123456+02"), Some(base));
        assert_eq!(parse_pg_timestamp("2007-01-02 08:34:05.123456+05:30"), Some(base));
        assert_eq!(parse_pg_timestamp("2007-01-02"), Some(parse_datetime("2007-01-02").unwrap()));
        assert_eq!(parse_pg_timestamp("0044-03-15 00:00:00 BC"), None);
        assert_eq!(parse_pg_timestamp("infinity"), None);
    }

    #[test]
    fn scalars() {
        assert_eq!(convert(Some("9007199254740993"), INT8, Class::Int), Value::Int(9_007_199_254_740_993));
        assert_eq!(convert(Some("0.1"), FLOAT8, Class::Real), Value::Real(0.1));
        assert!(matches!(convert(Some("NaN"), FLOAT8, Class::Real), Value::Real(x) if x.is_nan()));
        assert_eq!(convert(Some("-Infinity"), FLOAT8, Class::Real), Value::Real(f64::NEG_INFINITY));
        assert_eq!(convert(Some("1.50"), NUMERIC, Class::Real), Value::Real(1.5));
        assert_eq!(convert(Some("t"), BOOL, Class::Bool), Value::Bool(true));
        assert_eq!(convert(Some("f"), BOOL, Class::Unknown), Value::Bool(false));
        assert_eq!(convert(Some("x"), 25, Class::String), Value::Str("x".into()));
        assert_eq!(convert(None, INT8, Class::Int), Value::Null);
        assert_eq!(convert(Some("{1,2}"), 1007, Class::Unknown), Value::Str("{1,2}".into()));
    }

    #[test]
    fn temporal_and_guid() {
        assert_eq!(
            convert(Some("2007-01-02 03:04:05.123456"), TIMESTAMP, Class::DateTime),
            Value::DateTime(parse_datetime("2007-01-02T03:04:05.123456").unwrap())
        );
        assert_eq!(convert(Some("1 day 02:00:00"), INTERVAL, Class::TimeSpan), Value::TimeSpan(TICKS_PER_DAY + 2 * H));
        // Ticks that were never converted to INTERVAL stay integers (the comparator accepts them).
        assert_eq!(convert(Some("10000000"), INT8, Class::TimeSpan), Value::Int(10_000_000));
        assert_eq!(
            convert(Some("2007-01-02T00:00:00Z"), 25, Class::DateTime),
            Value::DateTime(parse_datetime("2007-01-02").unwrap())
        );
        assert_eq!(
            convert(Some("550E8400-E29B-41D4-A716-446655440000"), UUID, Class::Guid),
            Value::Guid("550e8400-e29b-41d4-a716-446655440000".into())
        );
    }

    #[test]
    fn dynamic() {
        assert_eq!(convert(Some(r#"{"a": [1, 2]}"#), JSONB, Class::Dynamic), Value::Json(json!({"a": [1, 2]})));
        assert_eq!(convert(Some("null"), JSONB, Class::Dynamic), Value::Null);
        assert_eq!(convert(Some(r#""s""#), JSONB, Class::Dynamic), Value::Json(json!("s")));
        assert_eq!(convert(Some("5"), INT8, Class::Dynamic), Value::Json(json!(5)));
        assert_eq!(convert(Some("not json"), 25, Class::Dynamic), Value::Str("not json".into()));
        assert_eq!(convert(Some("[1]"), JSONB, Class::Unknown), Value::Json(json!([1])));
    }

    #[test]
    fn result_conversion() {
        let raw = PgRaw {
            id: "x".into(),
            columns: vec![
                PgColumn { name: "a".into(), type_oid: INT8 },
                PgColumn { name: "b".into(), type_oid: INTERVAL },
            ],
            rows: vec![vec![Some("1".into()), Some("00:00:01".into())], vec![None, None]],
            error: None,
        };
        let r = to_result(&raw, &[Class::Int, Class::TimeSpan]).unwrap();
        assert_eq!(r.rows[0], vec![Value::Int(1), Value::TimeSpan(TICKS_PER_SECOND)]);
        assert_eq!(r.rows[1], vec![Value::Null, Value::Null]);
        // Declared types that don't match the column count are ignored; OIDs classify instead.
        let r = to_result(&raw, &[Class::Int]).unwrap();
        assert_eq!(r.columns[1].class, Class::TimeSpan);
        let err = PgRaw { error: Some("syntax error at or near \"x\"".into()), ..raw };
        assert!(to_result(&err, &[]).is_err());
    }

    #[test]
    fn domain_errors() {
        assert!(is_engine_domain_error("bigint out of range"));
        assert!(is_engine_domain_error("timestamp out of range"));
        assert!(is_engine_domain_error("cannot take logarithm of zero"));
        assert!(!is_engine_domain_error("division by zero"));
        assert!(!is_engine_domain_error("syntax error at or near \"FROM\""));
        assert!(!is_engine_domain_error("operator does not exist: jsonb + integer"));
    }
}
