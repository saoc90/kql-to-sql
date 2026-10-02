//! DuckDB values → Kusto JSON values, and Kusto type names.

use duckdb::types::{TimeUnit, Value};
use kql_to_sql::KqlType;
use serde_json::{json, Value as Json};

const TICKS_PER_SEC: i128 = 10_000_000;
const TICKS_PER_DAY: i128 = 86_400 * TICKS_PER_SEC;

/// The .NET type name Kusto puts in a v1 column's `DataType`.
pub fn v1_data_type(ty: KqlType) -> &'static str {
    match ty {
        KqlType::Bool => "Boolean",
        KqlType::Int => "Int32",
        KqlType::Long => "Int64",
        KqlType::Real => "Double",
        KqlType::Decimal => "Decimal",
        KqlType::String => "String",
        KqlType::DateTime => "DateTime",
        KqlType::TimeSpan => "TimeSpan",
        KqlType::Guid => "Guid",
        KqlType::Dynamic => "Object",
    }
}

/// Kusto type of a value whose column type is unknown.
pub fn type_of_value(v: &Value) -> KqlType {
    match v {
        Value::Boolean(_) => KqlType::Bool,
        Value::TinyInt(_) | Value::SmallInt(_) | Value::Int(_) | Value::UTinyInt(_) | Value::USmallInt(_) => {
            KqlType::Int
        }
        Value::BigInt(_) | Value::UInt(_) | Value::UBigInt(_) | Value::HugeInt(_) | Value::UHugeInt(_) => KqlType::Long,
        Value::Float(_) | Value::Double(_) => KqlType::Real,
        Value::Decimal(_) => KqlType::Decimal,
        Value::Timestamp(..) | Value::Date32(_) => KqlType::DateTime,
        Value::Interval { .. } => KqlType::TimeSpan,
        Value::List(_) | Value::Array(_) | Value::Struct(_) | Value::Map(_) => KqlType::Dynamic,
        _ => KqlType::String,
    }
}

/// Approximate serialized size of a value, used for the result-size limit.
pub fn approx_size(v: &Json) -> usize {
    match v {
        Json::String(s) => s.len() + 2,
        Json::Array(a) => 2 + a.iter().map(approx_size).sum::<usize>() + a.len(),
        Json::Object(o) => 2 + o.iter().map(|(k, v)| k.len() + 3 + approx_size(v)).sum::<usize>(),
        _ => 8,
    }
}

fn float(f: f64) -> Json {
    if f.is_nan() {
        json!("NaN")
    } else if f.is_infinite() {
        json!(if f > 0.0 { "Infinity" } else { "-Infinity" })
    } else {
        json!(f)
    }
}

fn int_of(v: &Value) -> Option<i128> {
    Some(match v {
        Value::TinyInt(x) => *x as i128,
        Value::SmallInt(x) => *x as i128,
        Value::Int(x) => *x as i128,
        Value::BigInt(x) => *x as i128,
        Value::HugeInt(x) => *x,
        Value::UTinyInt(x) => *x as i128,
        Value::USmallInt(x) => *x as i128,
        Value::UInt(x) => *x as i128,
        Value::UBigInt(x) => *x as i128,
        Value::UHugeInt(x) => i128::try_from(*x).ok()?,
        Value::Boolean(b) => *b as i128,
        _ => return None,
    })
}

fn float_of(v: &Value) -> Option<f64> {
    match v {
        Value::Float(f) => Some(*f as f64),
        Value::Double(f) => Some(*f),
        Value::Decimal(d) => d.to_string().parse().ok(),
        Value::Text(s) => s.parse().ok(),
        _ => int_of(v).map(|i| i as f64),
    }
}

fn ticks_of_timestamp(unit: TimeUnit, v: i64) -> i128 {
    let v = v as i128;
    match unit {
        TimeUnit::Second => v * TICKS_PER_SEC,
        TimeUnit::Millisecond => v * 10_000,
        TimeUnit::Microsecond => v * 10,
        TimeUnit::Nanosecond => v.div_euclid(100),
    }
}

/// Days since 1970-01-01 → (year, month, day). (Howard Hinnant's civil_from_days.)
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Unix-epoch ticks (100 ns) → `yyyy-MM-ddTHH:mm:ss.fffffffZ` (.NET "o" format, UTC).
pub fn format_datetime(ticks: i128) -> String {
    let days = ticks.div_euclid(TICKS_PER_DAY);
    let rem = ticks.rem_euclid(TICKS_PER_DAY);
    let (y, m, d) = civil_from_days(days as i64);
    let secs = rem / TICKS_PER_SEC;
    let frac = rem % TICKS_PER_SEC;
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:07}Z", secs / 3600, (secs / 60) % 60, secs % 60)
}

/// Ticks → Kusto timespan text `[-][d.]hh:mm:ss[.fffffff]` (.NET "c" format).
pub fn format_timespan(ticks: i128) -> String {
    let sign = if ticks < 0 { "-" } else { "" };
    let t = ticks.unsigned_abs() as i128;
    let days = t / TICKS_PER_DAY;
    let rem = t % TICKS_PER_DAY;
    let secs = rem / TICKS_PER_SEC;
    let frac = rem % TICKS_PER_SEC;
    let mut s = String::from(sign);
    if days > 0 {
        s.push_str(&format!("{days}."));
    }
    s.push_str(&format!("{:02}:{:02}:{:02}", secs / 3600, (secs / 60) % 60, secs % 60));
    if frac > 0 {
        s.push_str(&format!(".{frac:07}"));
    }
    s
}

/// The current time in the `"o"` format.
pub fn now_iso() -> String {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    format_datetime(d.as_nanos() as i128 / 100)
}

fn uuid_text(v: &Value) -> Option<String> {
    match v {
        Value::Text(s) => Some(s.clone()),
        Value::Blob(b) if b.len() == 16 => uuid::Uuid::from_slice(b).ok().map(|u| u.to_string()),
        Value::UHugeInt(x) => Some(uuid::Uuid::from_u128(*x).to_string()),
        // DuckDB stores UUIDs as a HUGEINT with the top bit flipped
        Value::HugeInt(x) => Some(uuid::Uuid::from_u128((*x as u128) ^ (1u128 << 127)).to_string()),
        _ => None,
    }
}

/// Structural (dynamic) conversion of nested values.
fn dynamic(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Text(s) => serde_json::from_str(s).unwrap_or_else(|_| json!(s)),
        Value::List(items) | Value::Array(items) => Json::Array(items.iter().map(dynamic).collect()),
        Value::Struct(m) => Json::Object(m.iter().map(|(k, v)| (k.clone(), dynamic(v))).collect()),
        Value::Map(m) => Json::Object(
            m.iter()
                .map(|(k, v)| {
                    let key = match generic(k) {
                        Json::String(s) => s,
                        other => other.to_string(),
                    };
                    (key, dynamic(v))
                })
                .collect(),
        ),
        Value::Union(b) => dynamic(b),
        other => generic(other),
    }
}

/// Conversion driven by the value itself (unknown column type).
fn generic(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Boolean(b) => json!(b),
        Value::Float(f) => float(*f as f64),
        Value::Double(f) => float(*f),
        Value::Decimal(d) => json!(d.to_string()),
        Value::Timestamp(u, x) => json!(format_datetime(ticks_of_timestamp(*u, *x))),
        Value::Date32(d) => json!(format_datetime(*d as i128 * TICKS_PER_DAY)),
        Value::Time64(u, x) => json!(format_timespan(ticks_of_timestamp(*u, *x))),
        Value::Interval { months, days, nanos } => json!(format_timespan(interval_ticks(*months, *days, *nanos))),
        Value::Text(s) | Value::Enum(s) => json!(s),
        Value::Blob(b) | Value::Geometry(b) => json!(b.iter().map(|x| format!("{x:02x}")).collect::<String>()),
        Value::List(_) | Value::Array(_) | Value::Struct(_) | Value::Map(_) | Value::Union(_) => dynamic(v),
        other => match int_of(other) {
            Some(i) => i64::try_from(i).map(|i| json!(i)).unwrap_or_else(|_| json!(i.to_string())),
            None => Json::Null,
        },
    }
}

fn interval_ticks(months: i32, days: i32, nanos: i64) -> i128 {
    (months as i128 * 30 + days as i128) * TICKS_PER_DAY + nanos as i128 / 100
}

/// Converts one cell of a column with Kusto type `ty` to its Kusto JSON representation.
pub fn to_json(v: &Value, ty: KqlType) -> Json {
    if matches!(v, Value::Null) {
        // Kusto strings are never null
        return if ty == KqlType::String { json!("") } else { Json::Null };
    }
    match ty {
        KqlType::Bool => match v {
            Value::Boolean(b) => json!(b),
            other => int_of(other).map(|i| json!(i != 0)).unwrap_or_else(|| generic(other)),
        },
        KqlType::Int | KqlType::Long => match int_of(v) {
            Some(i) => i64::try_from(i).map(|i| json!(i)).unwrap_or_else(|_| json!(i.to_string())),
            None => generic(v),
        },
        KqlType::Real => float_of(v).map(float).unwrap_or_else(|| generic(v)),
        KqlType::Decimal => match v {
            Value::Decimal(d) => json!(d.to_string()),
            Value::Text(s) => json!(s),
            other => float_of(other).map(|f| json!(f.to_string())).unwrap_or(Json::Null),
        },
        KqlType::String => match v {
            Value::Text(s) | Value::Enum(s) => json!(s),
            other => match generic(other) {
                Json::String(s) => json!(s),
                Json::Null => json!(""),
                j => json!(j.to_string()),
            },
        },
        KqlType::DateTime => generic(v),
        KqlType::TimeSpan => match v {
            Value::Interval { .. } | Value::Time64(..) => generic(v),
            // ticks
            other => match int_of(other) {
                Some(t) => json!(format_timespan(t)),
                None => generic(other),
            },
        },
        KqlType::Guid => uuid_text(v).map(Json::String).unwrap_or_else(|| generic(v)),
        KqlType::Dynamic => dynamic(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetimes() {
        assert_eq!(format_datetime(0), "1970-01-01T00:00:00.0000000Z");
        // 2007-09-29T08:11:00Z
        let t = ticks_of_timestamp(TimeUnit::Second, 1_191_053_460);
        assert_eq!(format_datetime(t), "2007-09-29T08:11:00.0000000Z");
        assert_eq!(format_datetime(ticks_of_timestamp(TimeUnit::Microsecond, -1)), "1969-12-31T23:59:59.9999990Z");
    }

    #[test]
    fn timespans() {
        assert_eq!(format_timespan(0), "00:00:00");
        assert_eq!(format_timespan(TICKS_PER_SEC * 3661 + 5_000_000), "01:01:01.5000000");
        assert_eq!(format_timespan(-TICKS_PER_DAY - TICKS_PER_SEC), "-1.00:00:01");
        assert_eq!(
            to_json(&Value::Interval { months: 0, days: 2, nanos: 3_600_000_000_000 }, KqlType::TimeSpan),
            json!("2.01:00:00")
        );
    }

    #[test]
    fn scalars() {
        assert_eq!(to_json(&Value::Double(f64::NAN), KqlType::Real), json!("NaN"));
        assert_eq!(to_json(&Value::Null, KqlType::String), json!(""));
        assert_eq!(to_json(&Value::Null, KqlType::Long), Json::Null);
        assert_eq!(to_json(&Value::Text("{\"a\":[1]}".into()), KqlType::Dynamic), json!({"a": [1]}));
        assert_eq!(to_json(&Value::Int(1), KqlType::Bool), json!(true));
    }
}
