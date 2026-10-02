//! Port of the C# `QueryAnalyzer`: decides a query's comparison mode and whether its result is
//! nondeterministic / approximate or relies on unordered set aggregates.
//!
//! The C# version finds the *last* `QueryOperator` in the parsed syntax tree (pre-order) and
//! treats `sort`/`order`/`top`/`serialize` as ordering. Without the Kusto parser we take the
//! textually-last pipe stage (outside string literals and comments), which picks the same
//! operator: pre-order traversal visits a nested pipeline after its enclosing operator.
//!
//! Extension over C#: the verdict records carry no `OrderKeys`, so we derive them from the final
//! sort/top clause (plain column names only) to enable tie-block relaxation.

/// Functions/operators whose results are time-dependent, random, or approximate.
const NONDETERMINISTIC_MARKERS: &[&str] = &[
    "rand",
    "now",
    "ago",
    "new_guid",
    "newguid",
    "guid(",
    "datetime(now",
    "dcount",
    "dcountif",
    "hll",
    "hll_if",
    "hll_merge",
    "tdigest",
    "percentile",
    "sample",
    "take_any",
    "any(",
    "anyif",
    "current_",
    "rand(",
    "make_string(rand",
    "top-hitters",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Multiset,
    Ordered,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Analysis {
    pub mode: Mode,
    pub nondeterministic: bool,
    /// make_set/make_bag: compare JSON arrays order-insensitively.
    pub set_semantics: bool,
    /// Columns of the final sort/top clause (for tie-block relaxation); empty when unknown.
    pub order_keys: Vec<String>,
}

pub fn analyze(kql: &str) -> Analysis {
    let lower = kql.to_lowercase();
    let nondeterministic = NONDETERMINISTIC_MARKERS.iter().any(|m| lower.contains(m));
    let set_semantics = ["make_set", "make_bag", "makeset"].iter().any(|m| lower.contains(m));

    let code = strip_literals(kql);
    let last_stage = code.rfind('|').map(|i| code[i + 1..].trim_start()).unwrap_or("");
    let (keyword, rest) = split_keyword(last_stage);
    let (mode, order_keys) = match keyword.to_ascii_lowercase().as_str() {
        "sort" | "order" => (Mode::Ordered, by_keys(rest)),
        "top" => (Mode::Ordered, by_keys(rest)),
        "serialize" => (Mode::Ordered, Vec::new()),
        _ => (Mode::Multiset, Vec::new()),
    };
    Analysis { mode, nondeterministic, set_semantics, order_keys }
}

/// Splits the leading operator keyword (letters, digits, '-', '_') off a pipe stage.
fn split_keyword(stage: &str) -> (&str, &str) {
    let end = stage.find(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '_')).unwrap_or(stage.len());
    (&stage[..end], &stage[end..])
}

/// Extracts plain column names from `... by a desc, b asc nulls last, ...`. Stops at the first
/// sort key that is not a bare identifier (an expression), since later keys only order within
/// ties of it.
fn by_keys(rest: &str) -> Vec<String> {
    let Some(pos) = find_word(rest, "by") else { return Vec::new() };
    let mut keys = Vec::new();
    for item in split_top_level(&rest[pos + 2..]) {
        let mut words = item.split_whitespace();
        let Some(first) = words.next() else { break };
        let ident = first.trim_matches(|c| c == '[' || c == ']' || c == '\'' || c == '"');
        let is_ident = !ident.is_empty()
            && ident.chars().all(|c| c.is_alphanumeric() || c == '_')
            && !ident.starts_with(|c: char| c.is_ascii_digit());
        let modifiers_ok =
            words.all(|w| matches!(w.to_ascii_lowercase().as_str(), "asc" | "desc" | "nulls" | "first" | "last"));
        if !is_ident || !modifiers_ok {
            break;
        }
        keys.push(ident.to_string());
    }
    keys
}

fn find_word(s: &str, word: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    s.match_indices(word).map(|(i, _)| i).find(|&i| {
        let before = i == 0 || !is_word_byte(bytes[i - 1]);
        let after = i + word.len() >= bytes.len() || !is_word_byte(bytes[i + word.len()]);
        before && after
    })
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Splits on commas that are not nested in (), [] or {}; stops at an unbalanced closer
/// (the end of an enclosing subquery) or ';'.
fn split_top_level(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let (mut depth, mut start) = (0i32, 0);
    for (i, c) in s.char_indices() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth == 0 => {
                parts.push(&s[start..i]);
                return parts;
            }
            ')' | ']' | '}' => depth -= 1,
            ';' if depth == 0 => {
                parts.push(&s[start..i]);
                return parts;
            }
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Replaces the contents of string literals and comments with spaces (preserving byte offsets of
/// the surrounding code) so '|' inside them is not mistaken for a pipe.
fn strip_literals(kql: &str) -> String {
    let b = kql.as_bytes();
    let mut out = b.to_vec();
    let blank = |out: &mut Vec<u8>, from: usize, to: usize| {
        for c in &mut out[from..to.min(b.len())] {
            if *c != b'\n' {
                *c = b' ';
            }
        }
    };
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                let end = kql[i..].find('\n').map_or(b.len(), |n| i + n);
                blank(&mut out, i, end);
                i = end;
            }
            b'`' if kql[i..].starts_with("```") => {
                let end = kql[i + 3..].find("```").map_or(b.len(), |n| i + 3 + n + 3);
                blank(&mut out, i, end);
                i = end;
            }
            q @ (b'\'' | b'"') => {
                let verbatim = i > 0 && matches!(b[i - 1], b'@');
                let mut j = i + 1;
                while j < b.len() {
                    if b[j] == b'\\' && !verbatim {
                        j += 2;
                        continue;
                    }
                    if b[j] == q {
                        if verbatim && b.get(j + 1) == Some(&q) {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j += 1;
                }
                let end = (j + 1).min(b.len());
                blank(&mut out, i, end);
                i = end;
            }
            _ => i += 1,
        }
    }
    // Only ASCII bytes inside literals were replaced by ASCII spaces; non-ASCII bytes outside
    // literals are untouched, and multi-byte sequences inside are fully overwritten.
    String::from_utf8(out).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_modes() {
        assert_eq!(analyze("T | sort by x desc").mode, Mode::Ordered);
        assert_eq!(analyze("T | order by x").mode, Mode::Ordered);
        assert_eq!(analyze("T | top 3 by x").mode, Mode::Ordered);
        assert_eq!(analyze("T | serialize r = row_number()").mode, Mode::Ordered);
        assert_eq!(analyze("T | top-nested 3 of x by count()").mode, Mode::Multiset);
        assert_eq!(analyze("T | sort by x | take 3").mode, Mode::Multiset);
        assert_eq!(analyze("T | where s == '| sort by x'").mode, Mode::Multiset);
        assert_eq!(analyze("T | sort by x // | count").mode, Mode::Ordered);
        // Pre-order "last operator" semantics: a nested sort is the last descendant.
        assert_eq!(analyze("T | join (U | sort by x) on k").mode, Mode::Ordered);
    }

    #[test]
    fn order_keys() {
        assert_eq!(analyze("T | sort by a desc, b asc nulls last").order_keys, vec!["a", "b"]);
        assert_eq!(analyze("T | top 2 by a").order_keys, vec!["a"]);
        assert_eq!(analyze("T | sort by strlen(s), a").order_keys, Vec::<String>::new());
        assert_eq!(analyze("T | sort by a, strlen(s)").order_keys, vec!["a"]);
        assert_eq!(analyze("T | join (U | sort by x) on k").order_keys, vec!["x"]);
    }

    #[test]
    fn flags() {
        assert!(analyze("T | extend r = rand()").nondeterministic);
        assert!(analyze("print ago(1d)").nondeterministic);
        assert!(!analyze("print 1").nondeterministic);
        assert!(analyze("T | summarize make_set(x)").set_semantics);
        assert!(!analyze("T | summarize make_list(x)").set_semantics);
    }
}
