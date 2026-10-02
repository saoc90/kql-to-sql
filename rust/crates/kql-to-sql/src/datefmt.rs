//! `format_datetime` / `format_timespan` format strings (.NET-style specifiers).

use crate::binder::Ctx;
use crate::sql::quote_str;
use crate::{err, Dialect, Result};

/// Splits a .NET format string into runs of the same specifier character and literals.
fn tokens(fmt: &str) -> Vec<(char, usize)> {
    let mut out: Vec<(char, usize)> = Vec::new();
    for c in fmt.chars() {
        match out.last_mut() {
            Some((p, n)) if *p == c && c.is_ascii_alphabetic() => *n += 1,
            _ => out.push((c, 1)),
        }
    }
    out
}

pub(crate) fn format_datetime(d: Dialect, ts: &str, fmt: &str) -> Result<String> {
    let mut parts: Vec<String> = Vec::new();
    let mut lit = String::new();
    let flush = |lit: &mut String, parts: &mut Vec<String>| {
        if !lit.is_empty() {
            parts.push(quote_str(lit));
            lit.clear();
        }
    };
    for (c, n) in tokens(fmt) {
        let spec = match (d, c, n) {
            (_, 'y', 4) => Some(fmt_part(d, ts, "%Y", "YYYY")),
            (_, 'y', 2) => Some(fmt_part(d, ts, "%y", "YY")),
            (_, 'M', 2) => Some(fmt_part(d, ts, "%m", "MM")),
            (_, 'M', 1) => Some(num_part(d, ts, "MONTH")),
            (_, 'd', 2) => Some(fmt_part(d, ts, "%d", "DD")),
            (_, 'd', 1) => Some(num_part(d, ts, "DAY")),
            (_, 'H', 2) => Some(fmt_part(d, ts, "%H", "HH24")),
            (_, 'H', 1) => Some(num_part(d, ts, "HOUR")),
            (_, 'h', 2) => Some(fmt_part(d, ts, "%I", "HH12")),
            (_, 'm', 2) => Some(fmt_part(d, ts, "%M", "MI")),
            (_, 'm', 1) => Some(num_part(d, ts, "MINUTE")),
            (_, 's', 2) => Some(fmt_part(d, ts, "%S", "SS")),
            (_, 's', 1) => Some(format!("CAST(CAST(floor(EXTRACT(SECOND FROM {ts})) AS BIGINT) AS VARCHAR)")),
            (_, 't', 2) => Some(fmt_part(d, ts, "%p", "AM")),
            (_, 'f' | 'F', k) if k <= 7 => {
                let us = fmt_part(d, ts, "%f", "US");
                let digits = if k <= 6 { format!("substr({us}, 1, {k})") } else { format!("({us} || '0')") };
                Some(digits)
            }
            (_, c, _) if c.is_ascii_alphabetic() => return err(format!("format_datetime(): unsupported format specifier '{}'", c.to_string().repeat(n))),
            _ => None,
        };
        match spec {
            Some(s) => {
                flush(&mut lit, &mut parts);
                parts.push(s);
            }
            None => lit.push_str(&c.to_string().repeat(n)),
        }
    }
    flush(&mut lit, &mut parts);
    if parts.is_empty() {
        return Ok("''".into());
    }
    Ok(format!("({})", parts.join(" || ")))
}

fn fmt_part(d: Dialect, ts: &str, strf: &str, pg: &str) -> String {
    match d {
        Dialect::DuckDb => format!("strftime({ts}, '{strf}')"),
        Dialect::Postgres => format!("to_char({ts}, '{pg}')"),
    }
}

fn num_part(d: Dialect, ts: &str, part: &str) -> String {
    let vs = if d == Dialect::Postgres { "text" } else { "VARCHAR" };
    format!("CAST(CAST(EXTRACT({part} FROM {ts}) AS BIGINT) AS {vs})")
}

pub(crate) fn format_timespan(ctx: &Ctx, ticks: &str, fmt: &str) -> Result<String> {
    let vs = ctx.d.sql_type(crate::KqlType::String);
    let a = format!("abs({ticks})");
    let div = if ctx.d.kind() == Dialect::Postgres { "/" } else { "//" };
    let pad = |e: String, n: usize| format!("lpad(CAST({e} AS {vs}), {n}, '0')");
    let mut parts = Vec::new();
    let mut lit = String::new();
    for (c, n) in tokens(fmt) {
        let spec = match c {
            'd' => Some(pad(format!("({a} {div} 864000000000)"), n)),
            'h' | 'H' => Some(pad(format!("(({a} {div} 36000000000) % 24)"), n)),
            'm' => Some(pad(format!("(({a} {div} 600000000) % 60)"), n)),
            's' => Some(pad(format!("(({a} {div} 10000000) % 60)"), n)),
            'f' | 'F' if n <= 7 => Some(format!("substr({}, 1, {n})", pad(format!("({a} % 10000000)"), 7))),
            c if c.is_ascii_alphabetic() => return err(format!("format_timespan(): unsupported format specifier '{c}'")),
            _ => None,
        };
        match spec {
            Some(s) => {
                if !lit.is_empty() {
                    parts.push(quote_str(&lit));
                    lit.clear();
                }
                parts.push(s);
            }
            None => lit.push_str(&c.to_string().repeat(n)),
        }
    }
    if !lit.is_empty() {
        parts.push(quote_str(&lit));
    }
    Ok(format!("(CASE WHEN {ticks} < 0 THEN '-' ELSE '' END || {})", parts.join(" || ")))
}
