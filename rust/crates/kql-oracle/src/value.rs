//! The engine-neutral cell value both sides (Kusto samples, DuckDB results) are converted into,
//! plus the coarse column type class shared with the C# fuzzer (`TypeClass`).

use std::fmt;

use kql_to_sql::KqlType;

/// .NET ticks (100 ns) per second / microsecond / day.
pub const TICKS_PER_SECOND: i64 = 10_000_000;
pub const TICKS_PER_MICRO: i64 = 10;
pub const TICKS_PER_DAY: i64 = 86_400 * TICKS_PER_SECOND;

/// Coarse type bucket both engines map onto (mirrors the C# `TypeClass`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    Bool,
    Int,
    Real,
    String,
    DateTime,
    TimeSpan,
    Guid,
    Dynamic,
    Unknown,
}

impl Class {
    /// Parses the `Class` suffix of a verdict column descriptor (`"name:Class"`).
    pub fn parse(s: &str) -> Class {
        match s {
            "Bool" => Class::Bool,
            "Int" => Class::Int,
            "Real" => Class::Real,
            "String" => Class::String,
            "DateTime" => Class::DateTime,
            "TimeSpan" => Class::TimeSpan,
            "Guid" => Class::Guid,
            "Dynamic" => Class::Dynamic,
            _ => Class::Unknown,
        }
    }

    pub fn from_kql(ty: KqlType) -> Class {
        match ty {
            KqlType::Bool => Class::Bool,
            KqlType::Int | KqlType::Long => Class::Int,
            KqlType::Real | KqlType::Decimal => Class::Real,
            KqlType::String => Class::String,
            KqlType::DateTime => Class::DateTime,
            KqlType::TimeSpan => Class::TimeSpan,
            KqlType::Guid => Class::Guid,
            KqlType::Dynamic => Class::Dynamic,
        }
    }
}

impl fmt::Display for Class {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// A named, classified result column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub class: Class,
}

impl ColumnInfo {
    /// Parses a verdict column descriptor `"name:Class"` (the name itself may contain ':').
    pub fn parse(desc: &str) -> ColumnInfo {
        match desc.rsplit_once(':') {
            Some((name, class)) => ColumnInfo { name: name.to_string(), class: Class::parse(class) },
            None => ColumnInfo { name: desc.to_string(), class: Class::Unknown },
        }
    }
}

/// One cell.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i128),
    Real(f64),
    Str(String),
    /// .NET ticks (100 ns) since the Unix epoch, UTC.
    DateTime(i64),
    /// .NET ticks (100 ns).
    TimeSpan(i64),
    /// Lower-case "D" format.
    Guid(String),
    Json(serde_json::Value),
}

pub type Row = Vec<Value>;

impl fmt::Display for Value {
    /// Renders like the C# `Comparator.FormatCell` (strings quoted, datetimes ISO "o").
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("null"),
            Value::Bool(b) => f.write_str(if *b { "True" } else { "False" }),
            Value::Int(i) => write!(f, "{i}"),
            Value::Real(r) => f.write_str(&format_real(*r)),
            Value::Str(s) => write!(f, "'{s}'"),
            Value::DateTime(t) => f.write_str(&format_datetime(*t)),
            Value::TimeSpan(t) => f.write_str(&format_timespan(*t)),
            Value::Guid(g) => write!(f, "'{g}'"),
            Value::Json(j) => write!(f, "{j}"),
        }
    }
}

pub fn format_row(row: &[Value]) -> String {
    let cells: Vec<String> = row.iter().map(Value::to_string).collect();
    format!("({})", cells.join(", "))
}

pub fn format_real(r: f64) -> String {
    if r.is_nan() {
        "NaN".into()
    } else if r == f64::INFINITY {
        "Infinity".into()
    } else if r == f64::NEG_INFINITY {
        "-Infinity".into()
    } else {
        format!("{r}")
    }
}

// ---- calendar helpers ------------------------------------------------------------------------

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Inverse of [`days_from_civil`].
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Formats ticks-since-epoch like .NET `DateTime.ToString("o")` for a UTC value.
pub fn format_datetime(ticks: i64) -> String {
    let days = ticks.div_euclid(TICKS_PER_DAY);
    let rem = ticks.rem_euclid(TICKS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    let secs = rem / TICKS_PER_SECOND;
    let frac = rem % TICKS_PER_SECOND;
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:07}Z", secs / 3600, (secs / 60) % 60, secs % 60)
}

/// Parses an ISO-8601-ish datetime (`YYYY-MM-DD[(T| )hh:mm[:ss[.fffffff]]][Z|±hh:mm]`) to ticks.
pub fn parse_datetime(s: &str) -> Option<i64> {
    let s = s.trim();
    let b = s.as_bytes();
    let num = |r: std::ops::Range<usize>| -> Option<i64> {
        let t = s.get(r)?;
        if t.is_empty() || !t.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        t.parse().ok()
    };
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    let mut ticks = days_from_civil(y, mo as u32, d as u32) * TICKS_PER_DAY;
    let mut i = 10;
    if i < b.len() && (b[i] == b'T' || b[i] == b' ') {
        i += 1;
        let h = num(i..i + 2)?;
        if b.get(i + 2) != Some(&b':') {
            return None;
        }
        let mi = num(i + 3..i + 5)?;
        i += 5;
        let mut sec = 0;
        if b.get(i) == Some(&b':') {
            sec = num(i + 1..i + 3)?;
            i += 3;
        }
        ticks += (h * 3600 + mi * 60 + sec) * TICKS_PER_SECOND;
        if b.get(i) == Some(&b'.') {
            let start = i + 1;
            let mut end = start;
            while end < b.len() && b[end].is_ascii_digit() {
                end += 1;
            }
            ticks += frac_to_ticks(&s[start..end])?;
            i = end;
        }
    }
    match &s[i..] {
        "" | "Z" => Some(ticks),
        tz if (tz.starts_with('+') || tz.starts_with('-')) && tz.len() == 6 && &tz[3..4] == ":" => {
            let sign = if tz.starts_with('-') { -1 } else { 1 };
            let off = num(i + 1..i + 3)? * 3600 + num(i + 4..i + 6)? * 60;
            Some(ticks - sign * off * TICKS_PER_SECOND)
        }
        _ => None,
    }
}

/// Converts fractional-second digits ("5", "0000001", "123456789") to ticks (truncating).
fn frac_to_ticks(digits: &str) -> Option<i64> {
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut padded: String = digits.chars().take(7).collect();
    while padded.len() < 7 {
        padded.push('0');
    }
    padded.parse().ok()
}

/// Formats ticks like .NET `TimeSpan.ToString()` ("c" format): `[-][d.]hh:mm:ss[.fffffff]`.
pub fn format_timespan(ticks: i64) -> String {
    let neg = ticks < 0;
    let t = ticks.unsigned_abs();
    let tps = TICKS_PER_SECOND as u64;
    let days = t / TICKS_PER_DAY as u64;
    let secs = (t / tps) % 86_400;
    let frac = t % tps;
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if days > 0 {
        out.push_str(&format!("{days}."));
    }
    out.push_str(&format!("{:02}:{:02}:{:02}", secs / 3600, (secs / 60) % 60, secs % 60));
    if frac > 0 {
        out.push_str(&format!(".{frac:07}"));
    }
    out
}

/// Parses a .NET-style timespan (`[-][d.]hh:mm[:ss[.fffffff]]` or `[-]d`) to ticks.
pub fn parse_timespan(s: &str) -> Option<i64> {
    let s = s.trim();
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    if body.is_empty() {
        return None;
    }
    let int = |t: &str| -> Option<i64> {
        if t.is_empty() || !t.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        t.parse().ok()
    };
    let ticks = if !body.contains(':') {
        int(body)? * TICKS_PER_DAY
    } else {
        let (days, clock) = match body.split_once(':') {
            Some((head, _)) if head.contains('.') => {
                let (d, _) = body.split_once('.')?;
                (int(d)?, &body[d.len() + 1..])
            }
            _ => (0, body),
        };
        let parts: Vec<&str> = clock.split(':').collect();
        let (h, m, sec_part) = match parts.as_slice() {
            [h, m] => (int(h)?, int(m)?, "0"),
            [h, m, s] => (int(h)?, int(m)?, *s),
            _ => return None,
        };
        let (sec, frac) = match sec_part.split_once('.') {
            Some((sec, frac)) => (int(sec)?, frac_to_ticks(frac)?),
            None => (int(sec_part)?, 0),
        };
        if h > 23 || m > 59 || sec > 59 {
            return None;
        }
        days * TICKS_PER_DAY + (h * 3600 + m * 60 + sec) * TICKS_PER_SECOND + frac
    };
    Some(if neg { -ticks } else { ticks })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_roundtrip() {
        for s in [
            "2007-01-02T00:00:00.0000000Z",
            "1969-12-31T23:00:00.0000000Z",
            "9999-12-31T23:59:59.9999999Z",
            "0001-01-01T00:00:00.0000000Z",
            "2020-02-29T13:30:00.1234567Z",
        ] {
            let t = parse_datetime(s).unwrap();
            assert_eq!(format_datetime(t), s);
        }
        assert_eq!(parse_datetime("1970-01-01"), Some(0));
        assert_eq!(parse_datetime("1970-01-01 00:00:01"), Some(TICKS_PER_SECOND));
        assert_eq!(parse_datetime("1970-01-01T01:00:00+01:00"), Some(0));
        assert_eq!(parse_datetime("not a date"), None);
    }

    #[test]
    fn timespan_roundtrip() {
        for s in
            ["4.00:00:00", "-00:00:01.5000000", "00:00:00.0000001", "6.23:59:59.9999999", "00:00:00", "-1.02:03:04"]
        {
            let t = parse_timespan(s).unwrap();
            assert_eq!(format_timespan(t), s, "{s}");
        }
        assert_eq!(parse_timespan("00:00:01.5"), Some(15_000_000));
        assert_eq!(parse_timespan("2"), Some(2 * TICKS_PER_DAY));
        assert_eq!(parse_timespan("1:2"), Some(3_720 * TICKS_PER_SECOND));
        assert_eq!(parse_timespan("abc"), None);
    }
}
