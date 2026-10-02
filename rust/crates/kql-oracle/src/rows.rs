//! Parser for Kusto `SampleRows` as written by the C# `Comparator.FormatRow`:
//! `"(" + cells.join(", ") + ")"` where null → `null`, string → `'text'` (NOT escaped),
//! dynamic → raw JSON (possibly pretty-printed), datetime → ISO "o", timespan → .NET
//! `TimeSpan.ToString()`, numbers → invariant culture, bool → `True`/`False`.
//!
//! Because strings are not escaped the format is ambiguous; the parser is guided by the
//! column classes and backtracks: for each cell it enumerates candidate parses (shortest
//! first) and keeps the first one for which the rest of the row parses.

use crate::value::{parse_datetime, parse_timespan, Class, Row, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Parses one formatted sample row into typed values, one per class.
pub fn parse_row(text: &str, classes: &[Class]) -> Result<Row, ParseError> {
    let inner = text
        .strip_prefix('(')
        .and_then(|t| t.strip_suffix(')'))
        .ok_or_else(|| ParseError(format!("row is not parenthesised: {text}")))?;
    let mut out = Vec::with_capacity(classes.len());
    if parse_cells(inner, classes, &mut out) {
        Ok(out)
    } else {
        Err(ParseError(format!("cannot parse row as [{}]: {text}", class_list(classes))))
    }
}

fn class_list(classes: &[Class]) -> String {
    classes.iter().map(Class::to_string).collect::<Vec<_>>().join(", ")
}

/// Parses `rest` as exactly `classes.len()` cells separated by ", ", pushing onto `out`.
fn parse_cells(rest: &str, classes: &[Class], out: &mut Row) -> bool {
    let Some((&class, more)) = classes.split_first() else {
        return rest.is_empty();
    };
    for (value, len) in candidates(rest, class) {
        let after = &rest[len..];
        let next = if more.is_empty() {
            Some(after)
        } else {
            after.strip_prefix(", ")
        };
        let Some(next) = next else { continue };
        if more.is_empty() && !next.is_empty() {
            continue;
        }
        out.push(value);
        if parse_cells(next, more, out) {
            return true;
        }
        out.pop();
    }
    false
}

/// Every plausible parse of a cell of `class` at the start of `s`, as (value, byte length).
/// Ordered by preference; the caller takes the first that lets the rest of the row parse.
fn candidates(s: &str, class: Class) -> Vec<(Value, usize)> {
    let mut out = Vec::new();
    if s.starts_with("null") {
        out.push((Value::Null, 4));
    }
    match class {
        Class::String => quoted(s, &mut out, Value::Str),
        Class::Guid => {
            quoted(s, &mut out, |g| Value::Guid(g.to_ascii_lowercase()));
        }
        Class::Dynamic => {
            if let Some(c) = json_value(s) {
                out.push(c);
            }
            quoted(s, &mut out, Value::Str);
        }
        Class::Bool => {
            for (lit, b) in [("True", true), ("False", false)] {
                if s.starts_with(lit) {
                    out.push((Value::Bool(b), lit.len()));
                }
            }
            quoted(s, &mut out, Value::Str);
        }
        Class::Int | Class::Real => {
            if let Some(c) = number(s) {
                out.push(c);
            }
            quoted(s, &mut out, Value::Str);
        }
        Class::DateTime => {
            let tok = token(s);
            if let Some(t) = parse_datetime(tok) {
                out.push((Value::DateTime(t), tok.len()));
            }
            quoted(s, &mut out, Value::Str);
        }
        Class::TimeSpan => {
            let tok = token(s);
            if let Some(t) = parse_timespan(tok) {
                out.push((Value::TimeSpan(t), tok.len()));
            }
            quoted(s, &mut out, Value::Str);
        }
        Class::Unknown => {
            quoted(s, &mut out, Value::Str);
            for (lit, b) in [("True", true), ("False", false)] {
                if s.starts_with(lit) {
                    out.push((Value::Bool(b), lit.len()));
                }
            }
            if let Some(c) = number(s) {
                out.push(c);
            }
            if let Some(c) = json_value(s) {
                out.push(c);
            }
        }
    }
    out
}

/// A bare token: everything up to the next ',' or ')' (neither occurs in the scalar formats).
fn token(s: &str) -> &str {
    let end = s.find([',', ')']).unwrap_or(s.len());
    &s[..end]
}

/// `'...'` with every possible closing quote, shortest first.
fn quoted(s: &str, out: &mut Vec<(Value, usize)>, make: impl Fn(String) -> Value) {
    let Some(body) = s.strip_prefix('\'') else { return };
    for (i, _) in body.match_indices('\'') {
        out.push((make(body[..i].to_string()), i + 2));
    }
}

/// An invariant-culture .NET number: integers, decimals, exponents, NaN, ±Infinity, ±∞.
fn number(s: &str) -> Option<(Value, usize)> {
    let tok = token(s);
    let v = match tok {
        "NaN" => Value::Real(f64::NAN),
        "Infinity" | "∞" => Value::Real(f64::INFINITY),
        "-Infinity" | "-∞" => Value::Real(f64::NEG_INFINITY),
        _ => {
            if let Ok(i) = tok.parse::<i128>() {
                Value::Int(i)
            } else {
                let looks_numeric = tok
                    .bytes()
                    .all(|c| c.is_ascii_digit() || matches!(c, b'-' | b'+' | b'.' | b'E' | b'e'));
                if !looks_numeric {
                    return None;
                }
                Value::Real(tok.parse::<f64>().ok()?)
            }
        }
    };
    Some((v, tok.len()))
}

/// One JSON value (any formatting) at the start of `s`.
fn json_value(s: &str) -> Option<(Value, usize)> {
    let mut stream = serde_json::Deserializer::from_str(s).into_iter::<serde_json::Value>();
    let value = stream.next()?.ok()?;
    let len = stream.byte_offset();
    let value = match value {
        serde_json::Value::Null => Value::Null,
        other => Value::Json(other),
    };
    Some((value, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use Class::*;

    fn p(text: &str, classes: &[Class]) -> Row {
        parse_row(text, classes).unwrap_or_else(|e| panic!("{e}"))
    }

    #[test]
    fn simple_numbers() {
        assert_eq!(
            p("(1, 30, 15, 2.5)", &[Int, Int, Real, Real]),
            vec![Value::Int(1), Value::Int(30), Value::Int(15), Value::Real(2.5)]
        );
        assert_eq!(p("(1E+20)", &[Real]), vec![Value::Real(1e20)]);
        assert_eq!(p("(-5.5)", &[Real]), vec![Value::Real(-5.5)]);
    }

    #[test]
    fn special_reals() {
        let r = p("(NaN, Infinity, -Infinity, ∞, -∞)", &[Real; 5]);
        assert!(matches!(r[0], Value::Real(x) if x.is_nan()));
        assert_eq!(r[1], Value::Real(f64::INFINITY));
        assert_eq!(r[2], Value::Real(f64::NEG_INFINITY));
        assert_eq!(r[3], Value::Real(f64::INFINITY));
        assert_eq!(r[4], Value::Real(f64::NEG_INFINITY));
    }

    #[test]
    fn nulls_in_every_class() {
        let classes = [Bool, Int, Real, String, DateTime, TimeSpan, Guid, Dynamic];
        let r = p("(null, null, null, null, null, null, null, null)", &classes);
        assert!(r.iter().all(|v| *v == Value::Null));
    }

    #[test]
    fn string_null_vs_quoted_null() {
        assert_eq!(p("('null', null)", &[String, String]), vec![Value::Str("null".into()), Value::Null]);
    }

    #[test]
    fn strings_with_commas_and_quotes() {
        assert_eq!(
            p("('a, b', 'it's', 3)", &[String, String, Int]),
            vec![Value::Str("a, b".into()), Value::Str("it's".into()), Value::Int(3)]
        );
        // A string containing the separator pattern itself ("', '") - backtracking must
        // give the int column its value.
        assert_eq!(
            p("('x', 'y', 7)", &[String, Int]),
            vec![Value::Str("x', 'y".into()), Value::Int(7)]
        );
        assert_eq!(p("('')", &[String]), vec![Value::Str("".into())]);
        assert_eq!(p("('a)')", &[String]), vec![Value::Str("a)".into())]);
        assert_eq!(p("('multi\nline')", &[String]), vec![Value::Str("multi\nline".into())]);
    }

    #[test]
    fn pretty_printed_json() {
        let r = p("(1, [\n  10,\n  20\n], {\n  \"a\": \"x, y\",\n  \"b\": [1, 2]\n})", &[Int, Dynamic, Dynamic]);
        assert_eq!(r[1], Value::Json(json!([10, 20])));
        assert_eq!(r[2], Value::Json(json!({"a": "x, y", "b": [1, 2]})));
    }

    #[test]
    fn json_scalars_and_null() {
        let r = p("(\"str\", 5, null, true)", &[Dynamic; 4]);
        assert_eq!(r, vec![Value::Json(json!("str")), Value::Json(json!(5)), Value::Null, Value::Json(json!(true))]);
    }

    #[test]
    fn datetimes_timespans_bools_guids() {
        let r = p(
            "(2007-01-02T00:00:00.0000000Z, 4.00:00:00, -00:00:01.5000000, 00:00:00.0000001, True, '550E8400-e29b-41d4-a716-446655440000')",
            &[DateTime, TimeSpan, TimeSpan, TimeSpan, Bool, Guid],
        );
        assert_eq!(r[0], Value::DateTime(crate::value::parse_datetime("2007-01-02").unwrap()));
        assert_eq!(r[1], Value::TimeSpan(4 * crate::value::TICKS_PER_DAY));
        assert_eq!(r[2], Value::TimeSpan(-15_000_000));
        assert_eq!(r[3], Value::TimeSpan(1));
        assert_eq!(r[4], Value::Bool(true));
        assert_eq!(r[5], Value::Guid("550e8400-e29b-41d4-a716-446655440000".into()));
    }

    #[test]
    fn unparseable_typed_cell_falls_back_to_string() {
        // The C# oracle stores an unparseable datetime as a plain string → rendered quoted.
        assert_eq!(p("('garbage')", &[DateTime]), vec![Value::Str("garbage".into())]);
    }

    #[test]
    fn empty_row_and_errors() {
        assert_eq!(p("()", &[]), Vec::<Value>::new());
        assert!(parse_row("(1, 2)", &[Int]).is_err());
        assert!(parse_row("(abc)", &[Int]).is_err());
        assert!(parse_row("1", &[Int]).is_err());
    }
}
