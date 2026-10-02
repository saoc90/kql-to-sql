//! Executes translated SQL in a fresh in-memory DuckDB and converts the result cells into
//! [`Value`]s, interpreting them with the translator's declared Kusto column types.

use duckdb::types::Value as Dv;
use duckdb::Connection;
use serde_json::{Map, Number, Value as Json};

use crate::compare::DuckResult;
use crate::value::{parse_datetime, Class, ColumnInfo, Value, TICKS_PER_DAY, TICKS_PER_MICRO};

/// Runs `sql`; `declared` are the translator's output columns (used to classify cells when the
/// column count agrees with the actual result).
pub fn execute(sql: &str, declared: &[Class]) -> Result<DuckResult, String> {
    let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;

    let (names, ncols) = {
        let s = rows.as_ref().ok_or("statement has no result set")?;
        (s.column_names(), s.column_count())
    };
    let classes: Vec<Class> = if declared.len() == ncols { declared.to_vec() } else { vec![Class::Unknown; ncols] };

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let mut cells = Vec::with_capacity(ncols);
        for (i, &class) in classes.iter().enumerate() {
            let v: Dv = row.get(i).map_err(|e| format!("reading column {i}: {e}"))?;
            cells.push(convert(v, class));
        }
        out.push(cells);
    }

    let columns = names
        .into_iter()
        .zip(classes)
        .enumerate()
        .map(|(i, (name, class))| {
            let class = if class == Class::Unknown { infer_class(out.iter().map(|r| &r[i])) } else { class };
            ColumnInfo { name, class }
        })
        .collect();
    Ok(DuckResult { columns, rows: out })
}

/// Class of the first non-null value (for columns the translator did not describe).
fn infer_class<'a>(mut cells: impl Iterator<Item = &'a Value>) -> Class {
    match cells.find(|v| !matches!(v, Value::Null)) {
        Some(Value::Bool(_)) => Class::Bool,
        Some(Value::Int(_)) => Class::Int,
        Some(Value::Real(_)) => Class::Real,
        Some(Value::Str(_)) => Class::String,
        Some(Value::DateTime(_)) => Class::DateTime,
        Some(Value::TimeSpan(_)) => Class::TimeSpan,
        Some(Value::Guid(_)) => Class::Guid,
        Some(Value::Json(_)) => Class::Dynamic,
        Some(Value::Null) | None => Class::Unknown,
    }
}

/// Converts one DuckDB value, guided by the declared Kusto class.
pub fn convert(v: Dv, class: Class) -> Value {
    if matches!(v, Dv::Null) {
        return Value::Null;
    }
    match class {
        Class::Dynamic => match v {
            // JSON columns arrive as text.
            Dv::Text(s) => match serde_json::from_str::<Json>(&s) {
                Ok(Json::Null) => Value::Null,
                Ok(j) => Value::Json(j),
                Err(_) => Value::Str(s),
            },
            other => match to_json(other) {
                Json::Null => Value::Null,
                j => Value::Json(j),
            },
        },
        Class::Guid => match v {
            Dv::Text(s) => Value::Guid(s.to_ascii_lowercase()),
            Dv::Blob(b) if b.len() == 16 => Value::Guid(format_guid(&b)),
            other => scalar(other),
        },
        Class::DateTime => match v {
            Dv::Text(s) => parse_datetime(&s).map(Value::DateTime).unwrap_or(Value::Str(s)),
            other => scalar(other),
        },
        Class::Bool => match v {
            Dv::Text(s) if s.eq_ignore_ascii_case("true") => Value::Bool(true),
            Dv::Text(s) if s.eq_ignore_ascii_case("false") => Value::Bool(false),
            other => scalar(other),
        },
        _ => scalar(v),
    }
}

/// Type-directed conversion of a scalar DuckDB value; containers become JSON.
fn scalar(v: Dv) -> Value {
    match v {
        Dv::Null => Value::Null,
        Dv::Boolean(b) => Value::Bool(b),
        Dv::TinyInt(i) => Value::Int(i.into()),
        Dv::SmallInt(i) => Value::Int(i.into()),
        Dv::Int(i) => Value::Int(i.into()),
        Dv::BigInt(i) => Value::Int(i.into()),
        Dv::HugeInt(i) => Value::Int(i),
        Dv::UTinyInt(i) => Value::Int(i.into()),
        Dv::USmallInt(i) => Value::Int(i.into()),
        Dv::UInt(i) => Value::Int(i.into()),
        Dv::UBigInt(i) => Value::Int(i.into()),
        Dv::UHugeInt(i) => i128::try_from(i).map(Value::Int).unwrap_or(Value::Real(i as f64)),
        Dv::Float(f) => Value::Real(f.into()),
        Dv::Double(f) => Value::Real(f),
        Dv::Decimal(d) => Value::Real(d.value() as f64 / 10f64.powi(d.scale().into())),
        Dv::Timestamp(unit, t) => Value::DateTime(unit.to_micros(t).saturating_mul(TICKS_PER_MICRO)),
        Dv::Date32(days) => Value::DateTime(i64::from(days) * TICKS_PER_DAY),
        Dv::Time64(unit, t) => Value::TimeSpan(unit.to_micros(t) * TICKS_PER_MICRO),
        Dv::Interval { months, days, nanos } => Value::TimeSpan(interval_ticks(months, days, nanos)),
        Dv::Text(s) | Dv::Enum(s) => Value::Str(s),
        Dv::Blob(b) | Dv::Geometry(b) => Value::Str(String::from_utf8_lossy(&b).into_owned()),
        Dv::Union(inner) => scalar(*inner),
        container @ (Dv::List(_) | Dv::Array(_) | Dv::Struct(_) | Dv::Map(_)) => Value::Json(to_json(container)),
        other => Value::Str(format!("{other:?}")),
    }
}

/// INTERVAL → ticks, counting a month as 30 days (DuckDB's own normalization).
fn interval_ticks(months: i32, days: i32, nanos: i64) -> i64 {
    (i64::from(months) * 30 + i64::from(days)) * TICKS_PER_DAY + nanos / 100
}

/// Converts any DuckDB value (including nested containers) to JSON, the way Kusto would render
/// the equivalent dynamic value.
pub fn to_json(v: Dv) -> Json {
    let num = |f: f64| Number::from_f64(f).map(Json::Number).unwrap_or(Json::Null);
    match v {
        Dv::Null => Json::Null,
        Dv::Boolean(b) => Json::Bool(b),
        Dv::TinyInt(i) => i.into(),
        Dv::SmallInt(i) => i.into(),
        Dv::Int(i) => i.into(),
        Dv::BigInt(i) => i.into(),
        Dv::UTinyInt(i) => i.into(),
        Dv::USmallInt(i) => i.into(),
        Dv::UInt(i) => i.into(),
        Dv::UBigInt(i) => i.into(),
        Dv::HugeInt(i) => i64::try_from(i).map(Json::from).unwrap_or_else(|_| num(i as f64)),
        Dv::UHugeInt(i) => u64::try_from(i).map(Json::from).unwrap_or_else(|_| num(i as f64)),
        Dv::Float(f) => num(f.into()),
        Dv::Double(f) => num(f),
        Dv::Decimal(d) => num(d.value() as f64 / 10f64.powi(d.scale().into())),
        Dv::Text(s) | Dv::Enum(s) => Json::String(s),
        Dv::List(items) | Dv::Array(items) => Json::Array(items.into_iter().map(to_json).collect()),
        Dv::Struct(fields) => {
            let mut m = Map::new();
            for (k, v) in fields.iter() {
                m.insert(k.clone(), to_json(v.clone()));
            }
            Json::Object(m)
        }
        Dv::Map(entries) => {
            let mut m = Map::new();
            for (k, v) in entries.iter() {
                let key = match to_json(k.clone()) {
                    Json::String(s) => s,
                    other => other.to_string(),
                };
                m.insert(key, to_json(v.clone()));
            }
            Json::Object(m)
        }
        Dv::Union(inner) => to_json(*inner),
        other => match scalar(other) {
            Value::DateTime(t) => Json::String(crate::value::format_datetime(t)),
            Value::TimeSpan(t) => Json::String(crate::value::format_timespan(t)),
            Value::Str(s) => Json::String(s),
            _ => Json::Null,
        },
    }
}

fn format_guid(b: &[u8]) -> String {
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(sql: &str, classes: &[Class]) -> Vec<Value> {
        let r = execute(sql, classes).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(r.rows.len(), 1);
        r.rows.into_iter().next().unwrap()
    }

    #[test]
    fn scalars() {
        let r = run(
            "SELECT 1::BIGINT, 2.5::DOUBLE, 'x', true, NULL, 1.25::DECIMAL(10,2), 170141183460469231731687303715884105727::HUGEINT",
            &[Class::Int, Class::Real, Class::String, Class::Bool, Class::String, Class::Real, Class::Int],
        );
        assert_eq!(
            r,
            vec![
                Value::Int(1),
                Value::Real(2.5),
                Value::Str("x".into()),
                Value::Bool(true),
                Value::Null,
                Value::Real(1.25),
                Value::Int(i128::MAX)
            ]
        );
    }

    #[test]
    fn temporal() {
        let r = run(
            "SELECT TIMESTAMP '2007-01-02 03:04:05.123456', INTERVAL '1 day 2 hours', INTERVAL '-1.5 seconds', DATE '1970-01-02'",
            &[Class::DateTime, Class::TimeSpan, Class::TimeSpan, Class::DateTime],
        );
        assert_eq!(r[0], Value::DateTime(parse_datetime("2007-01-02T03:04:05.123456").unwrap()));
        assert_eq!(r[1], Value::TimeSpan(TICKS_PER_DAY + 2 * 3600 * 10_000_000));
        assert_eq!(r[2], Value::TimeSpan(-15_000_000));
        assert_eq!(r[3], Value::DateTime(TICKS_PER_DAY));
    }

    #[test]
    fn dynamic() {
        let r = run(
            r#"SELECT '{"a":[1,2]}'::JSON, [1, 2, 3], {'k': 'v'}, 'null'::JSON, '"s"'::JSON"#,
            &[Class::Dynamic; 5],
        );
        assert_eq!(r[0], Value::Json(json!({"a": [1, 2]})));
        assert_eq!(r[1], Value::Json(json!([1, 2, 3])));
        assert_eq!(r[2], Value::Json(json!({"k": "v"})));
        assert_eq!(r[3], Value::Null);
        assert_eq!(r[4], Value::Json(json!("s")));
    }

    #[test]
    fn guid() {
        let r = run("SELECT '550E8400-E29B-41D4-A716-446655440000'::UUID", &[Class::Guid]);
        assert_eq!(r[0], Value::Guid("550e8400-e29b-41d4-a716-446655440000".into()));
    }

    #[test]
    fn errors_and_column_mismatch() {
        assert!(execute("SELECT nope FROM nowhere", &[]).is_err());
        // Declared types that don't match the actual column count are ignored.
        let r = execute("SELECT 1 AS a, 2 AS b", &[Class::Int]).unwrap();
        assert_eq!(r.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(r.columns[0].class, Class::Int);
    }
}
