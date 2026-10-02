//! Tokenizer. Lexical rules follow Kusto.Language's `TokenParser`.
//!
//! Keywords are not distinguished from identifiers here: KQL keywords are contextual (most may be
//! used as column names), so the parser decides by looking at identifier text. Compound keywords
//! such as `mv-expand`, `!contains` or `in~` are recognized by the parser from adjacent tokens.

use crate::ast::Span;
use crate::ParseError;

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Ident(String),
    /// Integer literal text (decimal or `0x` hex).
    Long(i64),
    Real(f64),
    /// Timespan literal in ticks.
    TimeSpan(i64),
    Str(String),
    /// A typed literal with raw contents: `datetime(2020-01-01)`, `long(null)`, `guid(...)`.
    Goo { ty: String, text: String },
    Punct(&'static str),
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub tok: Tok,
    pub span: Span,
}

const PUNCTS: &[&str] = &[
    "!~", "!=", "<>", "<=", ">=", "==", "=~", "=>", "..", "<|", "(", ")", "[", "]", "{", "}", "|",
    ".", ",", ";", ":", "=", "<", ">", "+", "-", "*", "/", "%", "!", "?", "@", "~", "#", "$",
];

/// Type names whose `name(...)` form is a literal with raw contents.
const GOO_TYPES: &[&str] = &[
    "bool", "boolean", "int", "long", "real", "double", "decimal", "datetime", "date", "timespan",
    "time", "guid", "uuid", "uniqueid",
];

pub fn tokenize(text: &str) -> Result<Vec<Token>, ParseError> {
    let mut lx = Lexer { src: text, pos: 0, tokens: Vec::new() };
    lx.run()?;
    Ok(lx.tokens)
}

struct Lexer<'a> {
    src: &'a str,
    pos: usize,
    tokens: Vec<Token>,
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_' || c == '$'
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

impl<'a> Lexer<'a> {
    fn peek(&self, offset: usize) -> Option<char> {
        self.src[self.pos..].chars().nth(offset)
    }

    fn rest(&self) -> &'a str {
        &self.src[self.pos..]
    }

    fn err(&self, msg: impl Into<String>) -> ParseError {
        ParseError { message: msg.into(), span: Span { start: self.pos, end: self.pos } }
    }

    fn push(&mut self, tok: Tok, start: usize) {
        self.tokens.push(Token { tok, span: Span { start, end: self.pos } });
    }

    fn run(&mut self) -> Result<(), ParseError> {
        loop {
            self.skip_trivia();
            let start = self.pos;
            let Some(c) = self.peek(0) else {
                self.push(Tok::Eof, start);
                return Ok(());
            };

            if let Some(s) = self.try_string()? {
                self.push(Tok::Str(s), start);
                continue;
            }

            if c.is_ascii_digit() {
                let tok = self.number()?;
                self.push(tok, start);
                continue;
            }

            if is_ident_start(c) {
                let ident = self.ident();
                if self.peek(0) == Some('(') && GOO_TYPES.contains(&ident.to_ascii_lowercase().as_str()) {
                    if let Some(text) = self.goo() {
                        self.push(Tok::Goo { ty: ident.to_ascii_lowercase(), text }, start);
                        continue;
                    }
                }
                self.push(Tok::Ident(ident), start);
                continue;
            }

            let rest = self.rest();
            if let Some(p) = PUNCTS.iter().find(|p| rest.starts_with(**p)) {
                self.pos += p.len();
                self.push(Tok::Punct(p), start);
                continue;
            }
            return Err(self.err(format!("unexpected character '{c}'")));
        }
    }

    fn skip_trivia(&mut self) {
        loop {
            let rest = self.rest();
            let trimmed = rest.trim_start();
            self.pos += rest.len() - trimmed.len();
            if self.rest().starts_with("//") {
                match self.rest().find('\n') {
                    Some(i) => self.pos += i + 1,
                    None => self.pos = self.src.len(),
                }
                continue;
            }
            break;
        }
    }

    fn ident(&mut self) -> String {
        let start = self.pos;
        let mut first = true;
        while let Some(c) = self.peek(0) {
            if (first && is_ident_start(c)) || (!first && is_ident_char(c)) {
                self.pos += c.len_utf8();
                first = false;
            } else {
                break;
            }
        }
        self.src[start..self.pos].to_string()
    }

    /// Scans `( ... )` raw contents after a typed-literal keyword. Returns `None` if unbalanced.
    fn goo(&mut self) -> Option<String> {
        let rest = self.rest();
        let mut depth = 0;
        for (i, c) in rest.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let inner = rest[1..i].trim().to_string();
                        self.pos += i + 1;
                        return Some(inner);
                    }
                }
                '\n' => return None,
                _ => {}
            }
        }
        None
    }

    fn digits(&self, from: usize) -> usize {
        self.src[from..].chars().take_while(|c| c.is_ascii_digit()).count()
    }

    fn number(&mut self) -> Result<Tok, ParseError> {
        let start = self.pos;
        let bytes = self.src.as_bytes();
        let at = |i: usize| bytes.get(i).copied().map(char::from);
        let ident_follows = |i: usize| self.src[i..].chars().next().is_some_and(is_ident_char);

        // hex
        if at(start) == Some('0') && matches!(at(start + 1), Some('x' | 'X')) {
            let hex: String = self.src[start + 2..].chars().take_while(|c| c.is_ascii_hexdigit()).collect();
            if !hex.is_empty() && !ident_follows(start + 2 + hex.len()) {
                self.pos = start + 2 + hex.len();
                let v = u64::from_str_radix(&hex, 16).map_err(|_| self.err("hex literal out of range"))?;
                return Ok(Tok::Long(v as i64));
            }
        }

        let int_len = self.digits(start);
        let mut end = start + int_len;
        let mut is_real = false;
        // fraction: `1.5` but not `1..5`
        if at(end) == Some('.') && (at(end + 1) != Some('.') || at(end + 2) == Some('.')) {
            let frac = self.digits(end + 1);
            end += 1 + frac;
            is_real = true;
        }
        // timespan suffix (also allowed after a fraction: 1.5h)
        if let Some(suffix) = timespan_suffix(&self.src[end..]) {
            if !ident_follows(end + suffix.len()) {
                let number: f64 = self.src[start..end].parse().map_err(|_| self.err("bad timespan"))?;
                let ticks = (number * suffix_ticks(suffix) as f64).round() as i64;
                self.pos = end + suffix.len();
                return Ok(Tok::TimeSpan(ticks));
            }
        }
        // exponent
        if matches!(at(end), Some('e' | 'E')) {
            let mut e = end + 1;
            if matches!(at(e), Some('+' | '-')) {
                e += 1;
            }
            let d = self.digits(e);
            if d > 0 {
                end = e + d;
                is_real = true;
            }
        }
        if ident_follows(end) {
            return Err(self.err(format!("invalid numeric literal '{}'", &self.src[start..end + 1])));
        }
        self.pos = end;
        let text = &self.src[start..end];
        if is_real {
            Ok(Tok::Real(text.parse().map_err(|_| self.err("bad real literal"))?))
        } else {
            match text.parse::<i64>() {
                Ok(v) => Ok(Tok::Long(v)),
                // Kusto turns out-of-range integer literals into reals.
                Err(_) => Ok(Tok::Real(text.parse().map_err(|_| self.err("bad literal"))?)),
            }
        }
    }

    /// Scans a string literal at the current position (`'..'`, `".."`, `@'..'`, `h".."`,
    /// ```` ```..``` ````, `~~~..~~~`). Returns the decoded value.
    fn try_string(&mut self) -> Result<Option<String>, ParseError> {
        let rest = self.rest();
        let mut p = 0;
        let b = rest.as_bytes();
        if matches!(b.first(), Some(b'h' | b'H')) && matches!(b.get(1), Some(b'\'' | b'"' | b'@')) {
            p = 1;
        }
        let verbatim = b.get(p) == Some(&b'@');
        if verbatim {
            p += 1;
        }
        let q = match b.get(p) {
            Some(b'\'') => '\'',
            Some(b'"') => '"',
            Some(b'`') if rest[p..].starts_with("```") && p == 0 => {
                return self.multiline("```").map(Some);
            }
            Some(b'~') if rest[p..].starts_with("~~~") && p == 0 => {
                return self.multiline("~~~").map(Some);
            }
            _ => return Ok(None),
        };
        let start = self.pos;
        let mut out = String::new();
        let body = &rest[p + 1..];
        let mut chars = body.char_indices().peekable();
        while let Some((i, c)) = chars.next() {
            if c == q {
                if verbatim && matches!(chars.peek(), Some((_, n)) if *n == q) {
                    chars.next();
                    out.push(q);
                    continue;
                }
                self.pos = start + p + 1 + i + 1;
                return Ok(Some(out));
            }
            if c == '\n' || c == '\r' {
                break;
            }
            if c == '\\' && !verbatim {
                let Some((_, e)) = chars.next() else { break };
                match e {
                    '\\' => out.push('\\'),
                    '\'' => out.push('\''),
                    '"' => out.push('"'),
                    'a' => out.push('\x07'),
                    'b' => out.push('\x08'),
                    'f' => out.push('\x0c'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    'v' => out.push('\x0b'),
                    'u' | 'U' | 'x' => {
                        let n = match e {
                            'u' => 4,
                            'U' => 8,
                            _ => 2,
                        };
                        let mut hex = String::new();
                        for _ in 0..n {
                            match chars.next() {
                                Some((_, h)) if h.is_ascii_hexdigit() => hex.push(h),
                                _ => return Err(self.err("invalid escape sequence")),
                            }
                        }
                        let v = u32::from_str_radix(&hex, 16).unwrap();
                        out.push(char::from_u32(v).ok_or_else(|| self.err("invalid escape"))?);
                    }
                    d if d.is_digit(8) => {
                        let mut oct = String::from(d);
                        while oct.len() < 3 {
                            match chars.peek() {
                                Some((_, o)) if o.is_digit(8) => {
                                    oct.push(*o);
                                    chars.next();
                                }
                                _ => break,
                            }
                        }
                        let v = u32::from_str_radix(&oct, 8).unwrap();
                        out.push(char::from_u32(v).unwrap_or('\u{fffd}'));
                    }
                    _ => return Err(self.err(format!("invalid escape sequence '\\{e}'"))),
                }
                continue;
            }
            out.push(c);
        }
        Err(ParseError { message: "unterminated string literal".into(), span: Span { start, end: self.src.len() } })
    }

    fn multiline(&mut self, quote: &str) -> Result<String, ParseError> {
        let start = self.pos;
        let body = &self.rest()[quote.len()..];
        match body.find(quote) {
            Some(i) => {
                let s = body[..i].to_string();
                self.pos = start + quote.len() + i + quote.len();
                Ok(s)
            }
            None => Err(ParseError {
                message: "unterminated multi-line string literal".into(),
                span: Span { start, end: self.src.len() },
            }),
        }
    }
}

const TIMESPAN_SUFFIXES: &[(&str, i64)] = &[
    ("milliseconds", 10_000),
    ("millisecond", 10_000),
    ("microseconds", 10),
    ("microsecond", 10),
    ("nanoseconds", 0),
    ("nanosecond", 0),
    ("millisec", 10_000),
    ("microsec", 10),
    ("nanosec", 0),
    ("minutes", 600_000_000),
    ("seconds", 10_000_000),
    ("minute", 600_000_000),
    ("second", 10_000_000),
    ("millis", 10_000),
    ("micros", 10),
    ("nanos", 0),
    ("milli", 10_000),
    ("micro", 10),
    ("nano", 0),
    ("hours", 36_000_000_000),
    ("ticks", 1),
    ("hour", 36_000_000_000),
    ("tick", 1),
    ("days", 864_000_000_000),
    ("min", 600_000_000),
    ("sec", 10_000_000),
    ("day", 864_000_000_000),
    ("hrs", 36_000_000_000),
    ("hr", 36_000_000_000),
    ("ms", 10_000),
    ("m", 600_000_000),
    ("s", 10_000_000),
    ("d", 864_000_000_000),
    ("h", 36_000_000_000),
];

fn timespan_suffix(s: &str) -> Option<&'static str> {
    TIMESPAN_SUFFIXES.iter().map(|(n, _)| *n).find(|n| s.starts_with(n))
}

fn suffix_ticks(suffix: &str) -> f64 {
    let t = TIMESPAN_SUFFIXES.iter().find(|(n, _)| *n == suffix).unwrap().1;
    if t == 0 {
        0.01 // nanoseconds: 1ns = 0.01 ticks
    } else {
        t as f64
    }
}

/// Parses a timespan given as text: `1d`, `1.5h`, `00:10:00`, `1.02:03:04.5`, a number of ticks.
pub fn parse_timespan_text(text: &str) -> Option<i64> {
    let t = text.trim();
    if t.is_empty() {
        return None;
    }
    let (neg, body) = match t.strip_prefix('-') {
        Some(r) => (true, r.trim()),
        None => (false, t.strip_prefix('+').unwrap_or(t).trim()),
    };
    let ticks = if body.contains(':') {
        // [d.]hh:mm[:ss[.fffffff]]
        let (days, clock) = match body.find('.') {
            Some(dot) if dot < body.find(':')? => (body[..dot].parse::<i64>().ok()?, &body[dot + 1..]),
            _ => (0, body),
        };
        let parts: Vec<&str> = clock.split(':').collect();
        if parts.len() < 2 || parts.len() > 3 {
            return None;
        }
        let h: i64 = parts[0].parse().ok()?;
        let m: i64 = parts[1].parse().ok()?;
        let s: f64 = if parts.len() == 3 { parts[2].parse().ok()? } else { 0.0 };
        days * 864_000_000_000 + h * 36_000_000_000 + m * 600_000_000 + (s * 10_000_000.0).round() as i64
    } else {
        let num_len = body.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(body.len());
        let number: f64 = body[..num_len].parse().ok()?;
        let suffix = body[num_len..].trim();
        if suffix.is_empty() {
            // a bare number in timespan(...) means days
            (number * 864_000_000_000.0).round() as i64
        } else {
            let s = timespan_suffix(suffix).filter(|s| s.len() == suffix.len())?;
            (number * suffix_ticks(s)).round() as i64
        }
    };
    Some(if neg { -ticks } else { ticks })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(s: &str) -> Vec<Tok> {
        tokenize(s).unwrap().into_iter().map(|t| t.tok).collect()
    }

    #[test]
    fn literals() {
        assert_eq!(toks("1"), vec![Tok::Long(1), Tok::Eof]);
        assert_eq!(toks("1.5"), vec![Tok::Real(1.5), Tok::Eof]);
        assert_eq!(toks("1e3"), vec![Tok::Real(1000.0), Tok::Eof]);
        assert_eq!(toks("0x1F"), vec![Tok::Long(31), Tok::Eof]);
        assert_eq!(toks("1d"), vec![Tok::TimeSpan(864_000_000_000), Tok::Eof]);
        assert_eq!(toks("1.5h"), vec![Tok::TimeSpan(54_000_000_000), Tok::Eof]);
        assert_eq!(toks("10ms"), vec![Tok::TimeSpan(100_000), Tok::Eof]);
        assert_eq!(toks("1tick"), vec![Tok::TimeSpan(1), Tok::Eof]);
        assert_eq!(toks("1..5"), vec![Tok::Long(1), Tok::Punct(".."), Tok::Long(5), Tok::Eof]);
    }

    #[test]
    fn strings() {
        assert_eq!(toks(r#""a\"b""#), vec![Tok::Str("a\"b".into()), Tok::Eof]);
        assert_eq!(toks(r"'it''s'"), vec![Tok::Str("it".into()), Tok::Str("s".into()), Tok::Eof]);
        assert_eq!(toks(r#"@"C:\t""#), vec![Tok::Str("C:\\t".into()), Tok::Eof]);
        assert_eq!(toks(r#"@'a''b'"#), vec![Tok::Str("a'b".into()), Tok::Eof]);
        assert_eq!(toks(r#"h"secret""#), vec![Tok::Str("secret".into()), Tok::Eof]);
        assert_eq!(toks("```a\nb```"), vec![Tok::Str("a\nb".into()), Tok::Eof]);
        assert_eq!(toks(r#""\u0041""#), vec![Tok::Str("A".into()), Tok::Eof]);
    }

    #[test]
    fn goo_and_idents() {
        assert_eq!(
            toks("datetime(2020-01-01 10:00) todatetime(x)"),
            vec![
                Tok::Goo { ty: "datetime".into(), text: "2020-01-01 10:00".into() },
                Tok::Ident("todatetime".into()),
                Tok::Punct("("),
                Tok::Ident("x".into()),
                Tok::Punct(")"),
                Tok::Eof
            ]
        );
        assert_eq!(toks("a // c\n| b"), vec![Tok::Ident("a".into()), Tok::Punct("|"), Tok::Ident("b".into()), Tok::Eof]);
        assert_eq!(toks("$left.k"), vec![Tok::Ident("$left".into()), Tok::Punct("."), Tok::Ident("k".into()), Tok::Eof]);
    }

    #[test]
    fn timespan_text() {
        assert_eq!(parse_timespan_text("1.02:03:04"), Some(864_000_000_000 + 2 * 36_000_000_000 + 3 * 600_000_000 + 4 * 10_000_000));
        assert_eq!(parse_timespan_text("00:00:00.5"), Some(5_000_000));
        assert_eq!(parse_timespan_text("2d"), Some(2 * 864_000_000_000));
        assert_eq!(parse_timespan_text("-1h"), Some(-36_000_000_000));
        assert_eq!(parse_timespan_text("1"), Some(864_000_000_000));
    }
}
