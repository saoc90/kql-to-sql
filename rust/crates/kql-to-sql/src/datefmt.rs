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

/// The separators Kusto allows between format specifiers.
const SEPARATORS: &str = " /-:,._[]";

/// Kusto's specifier lengths per letter, longest first. Longer runs are split greedily
/// (Kusto formats `dddd` as `dd` twice, `MMM` as `MM` then `M`).
fn spec_lengths(c: char) -> &'static [usize] {
    match c {
        'y' => &[4, 2, 1],
        'M' | 'd' | 'h' | 'H' | 'm' | 's' => &[2, 1],
        't' => &[2],
        'f' | 'F' => &[7, 6, 5, 4, 3, 2, 1],
        _ => &[],
    }
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
    let vs = if d == Dialect::Postgres { "text" } else { "VARCHAR" };
    for (c, run) in tokens(fmt) {
        if !c.is_ascii_alphabetic() {
            if !SEPARATORS.contains(c) {
                return err(format!("format_datetime(): unsupported separator '{c}'"));
            }
            lit.push_str(&c.to_string().repeat(run));
            continue;
        }
        let mut left = run;
        while left > 0 {
            let Some(&n) = spec_lengths(c).iter().find(|&&k| k <= left) else {
                return err(format!("format_datetime(): unsupported format specifier '{}'", c.to_string().repeat(left)));
            };
            left -= n;
            let s = match (c, n) {
                ('y', 4) => fmt_part(d, ts, "%Y", "YYYY"),
                ('y', 2) => fmt_part(d, ts, "%y", "YY"),
                ('y', _) => format!("CAST(CAST(EXTRACT(YEAR FROM {ts}) AS BIGINT) % 100 AS {vs})"),
                ('M', 2) => fmt_part(d, ts, "%m", "MM"),
                ('M', _) => num_part(d, ts, "MONTH"),
                ('d', 2) => fmt_part(d, ts, "%d", "DD"),
                ('d', _) => num_part(d, ts, "DAY"),
                ('H', 2) => fmt_part(d, ts, "%H", "HH24"),
                ('H', _) => num_part(d, ts, "HOUR"),
                ('h', 2) => fmt_part(d, ts, "%I", "HH12"),
                ('h', _) => format!("CAST((CAST(EXTRACT(HOUR FROM {ts}) AS BIGINT) + 11) % 12 + 1 AS {vs})"),
                ('m', 2) => fmt_part(d, ts, "%M", "MI"),
                ('m', _) => num_part(d, ts, "MINUTE"),
                ('s', 2) => fmt_part(d, ts, "%S", "SS"),
                ('s', _) => format!("CAST(CAST(floor(EXTRACT(SECOND FROM {ts})) AS BIGINT) AS {vs})"),
                ('t', _) => fmt_part(d, ts, "%p", "AM"),
                (_, k) => {
                    // f: fixed digits; F: trailing zeros trimmed (nothing when all zero)
                    let us = fmt_part(d, ts, "%f", "US");
                    let digits = if k <= 6 { format!("substr({us}, 1, {k})") } else { format!("({us} || '0')") };
                    if c == 'F' {
                        format!("rtrim({digits}, '0')")
                    } else {
                        digits
                    }
                }
            };
            flush(&mut lit, &mut parts);
            parts.push(s);
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
    // zero-pad to at least n digits (lpad alone would truncate longer values)
    let pad = |e: String, n: usize| {
        let t = format!("CAST({e} AS {vs})");
        if n <= 1 {
            t
        } else {
            format!("CASE WHEN length({t}) >= {n} THEN {t} ELSE lpad({t}, {n}, '0') END")
        }
    };
    let mut parts = Vec::new();
    let mut lit = String::new();
    for (c, n) in tokens(fmt) {
        let spec = match c {
            'd' => Some(pad(format!("({a} {div} 864000000000)"), n)),
            'h' | 'H' => Some(pad(format!("(({a} {div} 36000000000) % 24)"), n)),
            'm' => Some(pad(format!("(({a} {div} 600000000) % 60)"), n)),
            's' => Some(pad(format!("(({a} {div} 10000000) % 60)"), n)),
            'f' if n <= 7 => Some(format!("substr({}, 1, {n})", pad(format!("({a} % 10000000)"), 7))),
            'F' if n <= 7 => Some(format!("rtrim(substr({}, 1, {n}), '0')", pad(format!("({a} % 10000000)"), 7))),
            c if c.is_ascii_alphabetic() => return err(format!("format_timespan(): unsupported format specifier '{c}'")),
            c if !SEPARATORS.contains(c) => return err(format!("format_timespan(): unsupported separator '{c}'")),
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
