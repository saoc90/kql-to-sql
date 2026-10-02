//! Regular expression helpers. Kusto uses RE2 syntax, as does DuckDB; PostgreSQL uses POSIX ARE,
//! which accepts the common subset.

use crate::Dialect;

/// Escapes regex metacharacters in a literal string.
pub(crate) fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if "\\.^$|?*+()[]{}".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Adapts a Kusto (RE2) pattern for the target engine.
pub(crate) fn translate(re: &str, dialect: Dialect) -> String {
    match dialect {
        Dialect::DuckDb => re.to_string(),
        // ARE has no (?i) inline flag in the middle; leading (?i) is supported as an embedded option.
        Dialect::Postgres => re.replace("(?i)", "(?i)"),
    }
}

/// PostgreSQL ARE: the whole match takes the greediness of the *first* quantifier, so a pattern
/// like `^(.*?) (.*)` matches as little as possible and the trailing greedy group comes back
/// empty. A top-level alternation is always greedy (longest overall match) while each group keeps
/// its own preference, which is what RE2 yields for `parse` patterns; the extra branch `(?!)`
/// never matches. Leading embedded options stay in front.
pub(crate) fn pg_prefer_longest(re: &str) -> String {
    let (opts, body) = match re.strip_prefix("(?") {
        Some(rest) if rest.find(')').is_some_and(|i| i > 0 && rest[..i].bytes().all(|b| b.is_ascii_alphabetic())) => {
            let i = rest.find(')').unwrap() + 3;
            (&re[..i], &re[i..])
        }
        _ => ("", re),
    };
    format!("{opts}(?:{body})|(?!)")
}

/// Adapts a replacement string: Kusto uses `$1`/`\1` for groups; RE2/DuckDB uses `\1`.
pub(crate) fn translate_replacement(rep: &str, _dialect: Dialect) -> String {
    let mut out = String::new();
    let mut chars = rep.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek().is_some_and(|n| n.is_ascii_digit()) {
            out.push('\\');
            continue;
        }
        out.push(c);
    }
    out
}

/// Number of capturing groups in a pattern.
pub(crate) fn capture_groups(re: &str) -> usize {
    let b = re.as_bytes();
    let mut n = 0;
    let mut i = 0;
    let mut in_class = false;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 1,
            b'[' => in_class = true,
            b']' => in_class = false,
            b'(' if !in_class => {
                if b.get(i + 1) != Some(&b'?') || (b.get(i + 2) == Some(&b'P') || b.get(i + 2) == Some(&b'<')) && b.get(i + 3) != Some(&b'=') && b.get(i + 3) != Some(&b'!') {
                    n += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups() {
        assert_eq!(capture_groups(r"x(\d+)"), 1);
        assert_eq!(capture_groups(r"(a)(?:b)(c)"), 2);
        assert_eq!(capture_groups(r"[(]a"), 0);
        assert_eq!(escape("a.b*"), r"a\.b\*");
    }
}
