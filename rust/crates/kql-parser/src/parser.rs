//! Recursive-descent parser. Precedence and operator syntax follow Kusto.Language's `QueryParser`:
//!
//! ```text
//! pipe  :  or ( '|' operator )*
//! or    :  and ( 'or' and )*
//! and   :  eq ( 'and' eq )*
//! eq    :  rel [ (==|!=|<>|in|!in|in~|!in~|has_any|has_all|between|!between) ... ]
//! rel   :  add [ (<|<=|>|>=) add ]
//! add   :  mul ( (+|-) mul )*
//! mul   :  str ( (*|/|%) str )*
//! str   :  unary [ (=~|!~|has|contains|startswith|matches regex|...) unary ]
//! unary :  (-|+)? postfix
//! ```

use crate::ast::*;
use crate::lexer::{parse_timespan_text, tokenize, Tok, Token};
use crate::ParseError;

pub fn parse_query(text: &str) -> Result<Query, ParseError> {
    let tokens = tokenize(text)?;
    let mut p = Parser { src: text, tokens, pos: 0, no_in: false };
    let statements = p.statements(&[])?;
    if !p.at_eof() {
        return Err(p.unexpected("end of query"));
    }
    Ok(Query { statements })
}

pub fn parse_expression(text: &str) -> Result<Expr, ParseError> {
    let tokens = tokenize(text)?;
    let mut p = Parser { src: text, tokens, pos: 0, no_in: false };
    let e = p.expr()?;
    if !p.at_eof() {
        return Err(p.unexpected("end of expression"));
    }
    Ok(e)
}

struct Parser<'a> {
    src: &'a str,
    tokens: Vec<Token>,
    pos: usize,
    /// Set while parsing `make-series ... on <expr> in range(...)` so `in` is not an operator.
    no_in: bool,
}

const STRING_OPS: &[(&str, StringOp)] = &[
    ("has", StringOp::Has),
    ("has_cs", StringOp::HasCs),
    ("hasprefix", StringOp::HasPrefix),
    ("hasprefix_cs", StringOp::HasPrefixCs),
    ("hassuffix", StringOp::HasSuffix),
    ("hassuffix_cs", StringOp::HasSuffixCs),
    ("contains", StringOp::Contains),
    ("contains_cs", StringOp::ContainsCs),
    ("containscs", StringOp::ContainsCs),
    ("startswith", StringOp::StartsWith),
    ("startswith_cs", StringOp::StartsWithCs),
    ("endswith", StringOp::EndsWith),
    ("endswith_cs", StringOp::EndsWithCs),
    ("like", StringOp::Like),
    ("likecs", StringOp::LikeCs),
];

const SOURCE_KEYWORDS: &[&str] = &["print", "datatable", "range", "union", "search", "externaldata", "find", "evaluate"];

impl<'a> Parser<'a> {
    // ------------------------------------------------------------------ token helpers

    fn tok(&self, n: usize) -> &Tok {
        let i = (self.pos + n).min(self.tokens.len() - 1);
        &self.tokens[i].tok
    }

    fn token(&self, n: usize) -> &Token {
        let i = (self.pos + n).min(self.tokens.len() - 1);
        &self.tokens[i]
    }

    fn at_eof(&self) -> bool {
        matches!(self.tok(0), Tok::Eof)
    }

    fn bump(&mut self) -> Token {
        let t = self.token(0).clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn is_punct(&self, n: usize, p: &str) -> bool {
        matches!(self.tok(n), Tok::Punct(x) if *x == p)
    }

    fn ident_at(&self, n: usize) -> Option<&str> {
        match self.tok(n) {
            Tok::Ident(s) => Some(s.as_str()),
            _ => None,
        }
    }

    fn is_kw(&self, n: usize, kw: &str) -> bool {
        self.ident_at(n).is_some_and(|s| s.eq_ignore_ascii_case(kw))
    }

    /// True if token `n + 1` starts exactly where token `n` ends.
    fn adjacent(&self, n: usize) -> bool {
        self.token(n).span.end == self.token(n + 1).span.start
    }

    fn eat_punct(&mut self, p: &str) -> bool {
        if self.is_punct(0, p) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.is_kw(0, kw) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_punct(&mut self, p: &str) -> Result<(), ParseError> {
        if self.eat_punct(p) {
            Ok(())
        } else {
            Err(self.unexpected(&format!("'{p}'")))
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), ParseError> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(self.unexpected(&format!("'{kw}'")))
        }
    }

    fn unexpected(&self, expected: &str) -> ParseError {
        let t = self.token(0);
        let found = match &t.tok {
            Tok::Eof => "end of query".to_string(),
            _ => format!("'{}'", &self.src[t.span.start..t.span.end]),
        };
        ParseError { message: format!("expected {expected}, found {found}"), span: t.span }
    }

    /// Reads a hyphenated keyword such as `mv-expand` or `project-away` made of adjacent tokens.
    /// Returns the lowercase keyword and the number of tokens it spans.
    fn compound_kw(&self, n: usize) -> Option<(String, usize)> {
        let first = self.ident_at(n)?.to_ascii_lowercase();
        let mut kw = first;
        let mut len = 1;
        while self.is_punct(n + len, "-") && self.adjacent(n + len - 1) && self.adjacent(n + len) {
            match self.ident_at(n + len + 1) {
                Some(next) => {
                    kw.push('-');
                    kw.push_str(&next.to_ascii_lowercase());
                    len += 2;
                }
                None => break,
            }
        }
        Some((kw, len))
    }

    fn name(&mut self) -> Result<String, ParseError> {
        match self.tok(0).clone() {
            Tok::Ident(s) => {
                self.bump();
                Ok(s)
            }
            Tok::Punct("[") => {
                // ['name'] / ["name"]
                if let Tok::Str(s) = self.tok(1).clone() {
                    if self.is_punct(2, "]") {
                        self.bump();
                        self.bump();
                        self.bump();
                        return Ok(s);
                    }
                }
                Err(self.unexpected("a name"))
            }
            // Typed-literal keywords lexed as goo are also valid names when not followed by '('.
            _ => Err(self.unexpected("a name")),
        }
    }

    fn is_name_start(&self, n: usize) -> bool {
        matches!(self.tok(n), Tok::Ident(_))
            || (self.is_punct(n, "[") && matches!(self.tok(n + 1), Tok::Str(_)) && self.is_punct(n + 2, "]"))
    }

    fn type_name(&mut self) -> Result<String, ParseError> {
        let t = self.name()?;
        Ok(t.to_ascii_lowercase())
    }

    // ------------------------------------------------------------------ statements

    fn statements(&mut self, terminators: &[&str]) -> Result<Vec<Statement>, ParseError> {
        let mut out = Vec::new();
        loop {
            while self.eat_punct(";") {}
            if self.at_eof() || terminators.iter().any(|t| self.is_punct(0, t)) {
                break;
            }
            out.push(self.statement()?);
            if !self.eat_punct(";") {
                break;
            }
        }
        Ok(out)
    }

    fn statement(&mut self) -> Result<Statement, ParseError> {
        if self.is_kw(0, "let") && self.is_name_start(1) {
            self.bump();
            let name = self.name()?;
            self.expect_punct("=")?;
            let value = if self.looks_like_function() {
                LetValue::Function(self.function()?)
            } else {
                LetValue::Expr(self.expr()?)
            };
            return Ok(Statement::Let { name, value });
        }
        if self.is_kw(0, "set") && self.is_name_start(1) && !self.is_punct(2, "|") {
            self.bump();
            let mut name = self.name()?;
            while self.is_punct(0, ".") || self.is_punct(0, "-") {
                let p = if self.is_punct(0, ".") { "." } else { "-" };
                self.bump();
                name.push_str(p);
                name.push_str(&self.name()?);
            }
            let value = if self.eat_punct("=") { Some(self.or()?) } else { None };
            return Ok(Statement::Set { name, value });
        }
        if self.is_kw(0, "declare") || self.is_kw(0, "alias") || self.is_kw(0, "pattern") || self.is_kw(0, "restrict") {
            return Err(ParseError {
                message: format!("'{}' statements are not supported", self.ident_at(0).unwrap()),
                span: self.token(0).span,
            });
        }
        Ok(Statement::Expr(self.expr()?))
    }

    /// After `let x =`: is this `(params) { body }` or `view (params) { body }`?
    fn looks_like_function(&self) -> bool {
        let start = if self.is_kw(0, "view") && self.is_punct(1, "(") { 1 } else { 0 };
        if !self.is_punct(start, "(") {
            return false;
        }
        let mut depth = 0;
        let mut i = start;
        loop {
            match self.tok(i) {
                Tok::Eof => return false,
                Tok::Punct("(") => depth += 1,
                Tok::Punct(")") => {
                    depth -= 1;
                    if depth == 0 {
                        return self.is_punct(i + 1, "{");
                    }
                }
                _ => {}
            }
            i += 1;
        }
    }

    fn function(&mut self) -> Result<Function, ParseError> {
        let is_view = self.eat_kw("view");
        self.expect_punct("(")?;
        let mut params = Vec::new();
        if !self.is_punct(0, ")") {
            loop {
                let name = self.name()?;
                self.expect_punct(":")?;
                let ty = if self.is_punct(0, "(") {
                    self.bump();
                    if self.eat_punct("*") {
                        self.expect_punct(")")?;
                        ParamType::Tabular { columns: Vec::new(), open: true }
                    } else {
                        let mut columns = Vec::new();
                        let mut open = false;
                        loop {
                            if self.eat_punct("*") {
                                open = true;
                            } else {
                                columns.push(self.column_decl()?);
                            }
                            if !self.eat_punct(",") {
                                break;
                            }
                        }
                        self.expect_punct(")")?;
                        ParamType::Tabular { columns, open }
                    }
                } else {
                    ParamType::Scalar(self.type_name_or_goo()?)
                };
                let default = if self.eat_punct("=") { Some(self.or()?) } else { None };
                params.push(Param { name, ty, default });
                if !self.eat_punct(",") {
                    break;
                }
            }
        }
        self.expect_punct(")")?;
        self.expect_punct("{")?;
        let body = self.statements(&["}"])?;
        self.expect_punct("}")?;
        Ok(Function { params, body, is_view })
    }

    /// A type name; `datetime` etc. may have been lexed as goo if followed by '(' (never here).
    fn type_name_or_goo(&mut self) -> Result<String, ParseError> {
        self.type_name()
    }

    fn column_decl(&mut self) -> Result<ColumnDecl, ParseError> {
        let name = self.name()?;
        self.expect_punct(":")?;
        let ty = self.type_name()?;
        Ok(ColumnDecl { name, ty })
    }

    // ------------------------------------------------------------------ expressions

    /// Full expression including pipes.
    pub(crate) fn expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.or()?;
        while self.is_punct(0, "|") {
            self.bump();
            let op = self.operator()?;
            left = Expr::Pipe { input: Box::new(left), op: Box::new(op) };
        }
        Ok(left)
    }

    fn or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.and()?;
        while self.is_kw(0, "or") {
            self.bump();
            let right = self.and()?;
            left = Expr::Binary { op: BinaryOp::Or, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.equality()?;
        while self.is_kw(0, "and") {
            self.bump();
            let right = self.equality()?;
            left = Expr::Binary { op: BinaryOp::And, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn equality(&mut self) -> Result<Expr, ParseError> {
        let left = self.relational()?;
        let bin = |op, l: Expr, r: Expr| Expr::Binary { op, left: Box::new(l), right: Box::new(r) };
        if self.eat_punct("==") {
            let r = self.relational()?;
            return Ok(bin(BinaryOp::Eq, left, r));
        }
        if self.eat_punct("!=") || self.eat_punct("<>") {
            let r = self.relational()?;
            return Ok(bin(BinaryOp::Ne, left, r));
        }
        // in / in~ / !in / !in~ / has_any / has_all / between / !between
        let negated = self.is_punct(0, "!") && self.adjacent(0) && self.ident_at(1).is_some();
        let k = if negated { 1 } else { 0 };
        if let Some(word) = self.ident_at(k).map(|s| s.to_ascii_lowercase()) {
            let tilde = self.is_punct(k + 1, "~") && self.adjacent(k);
            match word.as_str() {
                "in" if !self.no_in => {
                    let kind = match (negated, tilde) {
                        (false, false) => InKind::In,
                        (true, false) => InKind::NotIn,
                        (false, true) => InKind::InCi,
                        (true, true) => InKind::NotInCi,
                    };
                    self.pos += k + 1 + usize::from(tilde);
                    let list = self.in_list()?;
                    return Ok(Expr::In { kind, expr: Box::new(left), list });
                }
                "has_any" | "has_all" if !negated => {
                    self.bump();
                    let kind = if word == "has_any" { InKind::HasAny } else { InKind::HasAll };
                    let list = self.in_list()?;
                    return Ok(Expr::In { kind, expr: Box::new(left), list });
                }
                "between" => {
                    self.pos += k + 1;
                    self.expect_punct("(")?;
                    let low = self.or()?;
                    self.expect_punct("..")?;
                    let high = self.or()?;
                    self.expect_punct(")")?;
                    return Ok(Expr::Between { expr: Box::new(left), low: Box::new(low), high: Box::new(high), negated });
                }
                _ => {}
            }
        }
        Ok(left)
    }

    fn in_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        self.expect_punct("(")?;
        let mut list = Vec::new();
        if !self.is_punct(0, ")") {
            loop {
                list.push(self.expr()?);
                if !self.eat_punct(",") {
                    break;
                }
            }
        }
        self.expect_punct(")")?;
        Ok(list)
    }

    fn relational(&mut self) -> Result<Expr, ParseError> {
        let left = self.additive()?;
        let op = match self.tok(0) {
            Tok::Punct("<") => BinaryOp::Lt,
            Tok::Punct("<=") => BinaryOp::Le,
            Tok::Punct(">") => BinaryOp::Gt,
            Tok::Punct(">=") => BinaryOp::Ge,
            _ => return Ok(left),
        };
        self.bump();
        let right = self.additive()?;
        Ok(Expr::Binary { op, left: Box::new(left), right: Box::new(right) })
    }

    fn additive(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.multiplicative()?;
        loop {
            let op = match self.tok(0) {
                Tok::Punct("+") => BinaryOp::Add,
                Tok::Punct("-") => BinaryOp::Sub,
                _ => return Ok(left),
            };
            self.bump();
            let right = self.multiplicative()?;
            left = Expr::Binary { op, left: Box::new(left), right: Box::new(right) };
        }
    }

    fn multiplicative(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.string_op()?;
        loop {
            let op = match self.tok(0) {
                Tok::Punct("*") => BinaryOp::Mul,
                Tok::Punct("/") => BinaryOp::Div,
                Tok::Punct("%") => BinaryOp::Mod,
                _ => return Ok(left),
            };
            self.bump();
            let right = self.string_op()?;
            left = Expr::Binary { op, left: Box::new(left), right: Box::new(right) };
        }
    }

    /// Recognizes a string operator at the current position. Returns (op, token count).
    fn peek_string_op(&self) -> Option<(BinaryOp, usize)> {
        match self.tok(0) {
            Tok::Punct("=~") => return Some((BinaryOp::EqTilde, 1)),
            Tok::Punct("!~") => return Some((BinaryOp::NeTilde, 1)),
            Tok::Punct(":") => return Some((BinaryOp::Str(StringOp::Has, false), 1)),
            _ => {}
        }
        let negated = self.is_punct(0, "!") && self.adjacent(0);
        let k = usize::from(negated);
        let word = self.ident_at(k)?.to_ascii_lowercase();
        if word == "matches" && !negated && self.is_kw(1, "regex") {
            return Some((BinaryOp::MatchesRegex, 2));
        }
        let (word, neg) = match word.strip_prefix("not") {
            Some(rest) if !negated && matches!(rest, "contains" | "containscs" | "like" | "likecs") => (rest.to_string(), true),
            _ => (word, negated),
        };
        let op = STRING_OPS.iter().find(|(w, _)| *w == word).map(|(_, op)| *op)?;
        Some((BinaryOp::Str(op, neg), k + 1))
    }

    fn string_op(&mut self) -> Result<Expr, ParseError> {
        let left = if self.is_punct(0, "*") && self.peek_string_op_at(1) {
            self.bump();
            Expr::Star
        } else {
            self.unary()?
        };
        if let Some((op, n)) = self.peek_string_op() {
            self.pos += n;
            let right = self.unary()?;
            return Ok(Expr::Binary { op, left: Box::new(left), right: Box::new(right) });
        }
        Ok(left)
    }

    /// True if token `n` is a string-operator keyword (used for `* has "x"`).
    fn peek_string_op_at(&self, n: usize) -> bool {
        self.ident_at(n).is_some_and(|w| {
            let w = w.to_ascii_lowercase();
            STRING_OPS.iter().any(|(s, _)| *s == w)
        })
    }

    fn unary(&mut self) -> Result<Expr, ParseError> {
        let op = match self.tok(0) {
            Tok::Punct("-") => UnaryOp::Neg,
            Tok::Punct("+") => UnaryOp::Plus,
            _ => return self.postfix(),
        };
        self.bump();
        let expr = self.postfix()?;
        // Fold negative numeric literals so `-5` is a literal (matters for int64 min and datatable).
        if op == UnaryOp::Neg {
            match &expr {
                Expr::Literal(Literal::Long(v)) => return Ok(Expr::Literal(Literal::Long(-v))),
                Expr::Literal(Literal::Real(v)) => return Ok(Expr::Literal(Literal::Real(-v))),
                Expr::Literal(Literal::TimeSpan(v)) => return Ok(Expr::Literal(Literal::TimeSpan(-v))),
                _ => {}
            }
        }
        Ok(Expr::Unary { op, expr: Box::new(expr) })
    }

    fn postfix(&mut self) -> Result<Expr, ParseError> {
        let mut e = self.primary()?;
        loop {
            if self.is_punct(0, ".") && !self.is_punct(0, "..") {
                // member access: a.b, a.['b c'], a.b()
                self.bump();
                if self.is_punct(0, "[") {
                    let name = self.name()?;
                    e = Expr::Member { expr: Box::new(e), name };
                    continue;
                }
                let name = match self.tok(0).clone() {
                    Tok::Ident(s) => {
                        self.bump();
                        s
                    }
                    Tok::Goo { .. } => return Err(self.unexpected("a member name")),
                    _ => return Err(self.unexpected("a member name")),
                };
                e = Expr::Member { expr: Box::new(e), name };
            } else if self.is_punct(0, "[") {
                self.bump();
                let index = self.or()?;
                self.expect_punct("]")?;
                e = Expr::Index { expr: Box::new(e), index: Box::new(index) };
            } else {
                return Ok(e);
            }
        }
    }

    fn primary(&mut self) -> Result<Expr, ParseError> {
        let t = self.token(0).clone();
        match t.tok {
            Tok::Long(v) => {
                self.bump();
                Ok(Expr::Literal(Literal::Long(v)))
            }
            Tok::Real(v) => {
                self.bump();
                Ok(Expr::Literal(Literal::Real(v)))
            }
            Tok::TimeSpan(v) => {
                self.bump();
                Ok(Expr::Literal(Literal::TimeSpan(v)))
            }
            Tok::Str(s) => {
                self.bump();
                let mut s = s;
                // adjacent string literals concatenate: "a" "b"
                while let Tok::Str(next) = self.tok(0).clone() {
                    self.bump();
                    s.push_str(&next);
                }
                Ok(Expr::Literal(Literal::String(s)))
            }
            Tok::Goo { ty, text } => {
                self.bump();
                goo_literal(&ty, &text).map(Expr::Literal).map_err(|m| ParseError { message: m, span: t.span })
            }
            Tok::Punct("(") => {
                self.bump();
                let e = self.expr()?;
                self.expect_punct(")")?;
                Ok(Expr::Paren(Box::new(e)))
            }
            Tok::Punct("[") => Ok(Expr::Name(self.name()?)),
            Tok::Punct("*") => {
                self.bump();
                Ok(Expr::Star)
            }
            Tok::Ident(ref id) => {
                let lower = id.to_ascii_lowercase();
                match lower.as_str() {
                    "true" if !self.is_punct(1, "(") => {
                        self.bump();
                        return Ok(Expr::Literal(Literal::Bool(true)));
                    }
                    "false" if !self.is_punct(1, "(") => {
                        self.bump();
                        return Ok(Expr::Literal(Literal::Bool(false)));
                    }
                    "null" if !self.is_punct(1, "(") => {
                        self.bump();
                        return Ok(Expr::Literal(Literal::Null(None)));
                    }
                    "dynamic" if self.is_punct(1, "(") => {
                        self.bump();
                        self.bump();
                        let j = self.json()?;
                        self.expect_punct(")")?;
                        return Ok(Expr::Literal(match j {
                            Json::Null => Literal::Null(Some("dynamic".into())),
                            j => Literal::Dynamic(j),
                        }));
                    }
                    _ => {}
                }
                if SOURCE_KEYWORDS.contains(&lower.as_str()) && self.is_source_start(&lower) {
                    let op = self.source_operator()?;
                    return Ok(Expr::Source(Box::new(op)));
                }
                if self.is_punct(1, "(") {
                    let name = id.clone();
                    self.bump();
                    let args = self.args()?;
                    return Ok(Expr::Call { name, args });
                }
                self.bump();
                Ok(Expr::Name(id.clone()))
            }
            _ => Err(self.unexpected("an expression")),
        }
    }

    fn is_source_start(&self, kw: &str) -> bool {
        match kw {
            "datatable" | "externaldata" => self.is_punct(1, "(") || self.is_kw(1, "with"),
            "range" => self.ident_at(1).is_some() && self.is_kw(2, "from"),
            "print" => true,
            "union" | "search" | "find" => !self.is_punct(1, "(") || kw != "find",
            "evaluate" => self.ident_at(1).is_some(),
            _ => false,
        }
    }

    /// `( [name =] expr, ... )` after a function name.
    fn args(&mut self) -> Result<Vec<Arg>, ParseError> {
        self.expect_punct("(")?;
        let mut args = Vec::new();
        if !self.is_punct(0, ")") {
            loop {
                let name = if matches!(self.tok(0), Tok::Ident(_)) && self.is_punct(1, "=") {
                    let n = self.name()?;
                    self.bump();
                    Some(n)
                } else {
                    None
                };
                let expr = self.expr()?;
                args.push(Arg { name, expr });
                if !self.eat_punct(",") {
                    break;
                }
            }
        }
        self.expect_punct(")")?;
        Ok(args)
    }

    fn json(&mut self) -> Result<Json, ParseError> {
        let t = self.token(0).clone();
        match t.tok {
            Tok::Punct("{") => {
                self.bump();
                let mut members = Vec::new();
                if !self.is_punct(0, "}") {
                    loop {
                        let key = match self.tok(0).clone() {
                            Tok::Str(s) => s,
                            Tok::Ident(s) => s,
                            _ => return Err(self.unexpected("a property name")),
                        };
                        self.bump();
                        self.expect_punct(":")?;
                        let v = self.json()?;
                        members.push((key, v));
                        if !self.eat_punct(",") {
                            break;
                        }
                    }
                }
                self.expect_punct("}")?;
                Ok(Json::Object(members))
            }
            Tok::Punct("[") => {
                self.bump();
                let mut items = Vec::new();
                if !self.is_punct(0, "]") {
                    loop {
                        items.push(self.json()?);
                        if !self.eat_punct(",") {
                            break;
                        }
                    }
                }
                self.expect_punct("]")?;
                Ok(Json::Array(items))
            }
            Tok::Str(s) => {
                self.bump();
                Ok(Json::String(s))
            }
            Tok::Long(_) | Tok::Real(_) => {
                self.bump();
                Ok(Json::Number(self.src[t.span.start..t.span.end].to_string()))
            }
            Tok::Punct("-") | Tok::Punct("+") => {
                self.bump();
                let n = self.token(0).clone();
                match n.tok {
                    Tok::Long(_) | Tok::Real(_) => {
                        self.bump();
                        let sign = if self.src[t.span.start..t.span.end] == *"-" { "-" } else { "" };
                        Ok(Json::Number(format!("{sign}{}", &self.src[n.span.start..n.span.end])))
                    }
                    Tok::TimeSpan(v) => {
                        self.bump();
                        Ok(Json::Scalar(Box::new(Literal::TimeSpan(-v))))
                    }
                    _ => Err(self.unexpected("a number")),
                }
            }
            Tok::TimeSpan(v) => {
                self.bump();
                Ok(Json::Scalar(Box::new(Literal::TimeSpan(v))))
            }
            Tok::Goo { ty, text } => {
                self.bump();
                let lit = goo_literal(&ty, &text).map_err(|m| ParseError { message: m, span: t.span })?;
                Ok(Json::Scalar(Box::new(lit)))
            }
            Tok::Ident(ref s) if s == "dynamic" && self.is_punct(1, "(") => {
                self.bump();
                self.bump();
                let inner = self.json()?;
                self.expect_punct(")")?;
                Ok(inner)
            }
            Tok::Ident(ref s) => {
                let v = match s.as_str() {
                    "true" | "True" | "TRUE" => Json::Bool(true),
                    "false" | "False" | "FALSE" => Json::Bool(false),
                    "null" => Json::Null,
                    _ => return Err(self.unexpected("a JSON value")),
                };
                self.bump();
                Ok(v)
            }
            _ => Err(self.unexpected("a JSON value")),
        }
    }

    // ------------------------------------------------------------------ operators

    fn named_expr(&mut self) -> Result<NamedExpr, ParseError> {
        // (a, b) = expr
        if self.is_punct(0, "(") {
            let mut i = 1;
            let mut ok = false;
            loop {
                if !(matches!(self.tok(i), Tok::Ident(_))) {
                    break;
                }
                i += 1;
                if self.is_punct(i, ",") {
                    i += 1;
                    continue;
                }
                if self.is_punct(i, ")") && self.is_punct(i + 1, "=") {
                    ok = true;
                }
                break;
            }
            if ok {
                self.bump();
                let mut names = Vec::new();
                loop {
                    names.push(self.name()?);
                    if !self.eat_punct(",") {
                        break;
                    }
                }
                self.expect_punct(")")?;
                self.expect_punct("=")?;
                let expr = self.or()?;
                return Ok(NamedExpr { names, expr });
            }
        }
        if self.is_name_start(0) {
            let width = if self.is_punct(0, "[") { 3 } else { 1 };
            if self.is_punct(width, "=") {
                let name = self.name()?;
                self.bump();
                let expr = self.or()?;
                return Ok(NamedExpr { names: vec![name], expr });
            }
        }
        Ok(NamedExpr { names: Vec::new(), expr: self.or()? })
    }

    fn named_list(&mut self) -> Result<Vec<NamedExpr>, ParseError> {
        let mut out = vec![self.named_expr()?];
        while self.eat_punct(",") {
            out.push(self.named_expr()?);
        }
        Ok(out)
    }

    /// Parses `name=value` operator parameters whose names satisfy `allowed`.
    fn op_params(&mut self, allowed: &[&str]) -> Result<Vec<OpParam>, ParseError> {
        let mut out = Vec::new();
        loop {
            // hint.strategy=x is lexed as ident . ident = ...
            let mut name = match self.ident_at(0) {
                Some(s) => s.to_ascii_lowercase(),
                None => break,
            };
            let mut n = 1;
            while self.is_punct(n, ".") && self.ident_at(n + 1).is_some() {
                name.push('.');
                name.push_str(&self.ident_at(n + 1).unwrap().to_ascii_lowercase());
                n += 2;
            }
            if !self.is_punct(n, "=") {
                break;
            }
            let ok = allowed.contains(&name.as_str()) || name.starts_with("hint.");
            if !ok {
                break;
            }
            self.pos += n + 1;
            let value = match self.tok(0).clone() {
                Tok::Ident(s) => {
                    self.bump();
                    match s.as_str() {
                        "true" => Expr::Literal(Literal::Bool(true)),
                        "false" => Expr::Literal(Literal::Bool(false)),
                        _ => Expr::Name(s),
                    }
                }
                _ => self.unary()?,
            };
            out.push(OpParam { name, value });
        }
        Ok(out)
    }

    fn order_key(&mut self) -> Result<OrderKey, ParseError> {
        let expr = self.or()?;
        let (dir, nulls) = self.sort_suffix()?;
        Ok(OrderKey { expr, dir, nulls })
    }

    fn sort_suffix(&mut self) -> Result<(Option<SortDir>, Option<NullsOrder>), ParseError> {
        let dir = if self.eat_kw("asc") {
            Some(SortDir::Asc)
        } else if self.eat_kw("desc") {
            Some(SortDir::Desc)
        } else {
            None
        };
        let nulls = if self.is_kw(0, "nulls") {
            self.bump();
            if self.eat_kw("first") {
                Some(NullsOrder::First)
            } else {
                self.expect_kw("last")?;
                Some(NullsOrder::Last)
            }
        } else {
            None
        };
        Ok((dir, nulls))
    }

    /// A name pattern possibly containing `*`, assembled from adjacent tokens (`Inj*`, `*Id`).
    fn name_pattern(&mut self) -> Result<String, ParseError> {
        let mut s = String::new();
        let mut first = true;
        loop {
            if !first && !self.adjacent_prev() {
                break;
            }
            match self.tok(0).clone() {
                Tok::Ident(id) => {
                    s.push_str(&id);
                    self.bump();
                }
                Tok::Punct("*") => {
                    s.push('*');
                    self.bump();
                }
                Tok::Punct("[") if first => {
                    s.push_str(&self.name()?);
                    break;
                }
                _ => break,
            }
            first = false;
        }
        if s.is_empty() {
            return Err(self.unexpected("a column name"));
        }
        Ok(s)
    }

    fn adjacent_prev(&self) -> bool {
        self.pos > 0 && self.tokens[self.pos - 1].span.end == self.tokens[self.pos].span.start
    }

    fn name_patterns(&mut self, with_dir: bool) -> Result<Vec<NamePattern>, ParseError> {
        let mut out = Vec::new();
        loop {
            let pattern = self.name_pattern()?;
            let dir = if with_dir {
                if self.eat_kw("asc") || self.eat_kw("granny-asc") {
                    Some(SortDir::Asc)
                } else if self.eat_kw("desc") || self.eat_kw("granny-desc") {
                    Some(SortDir::Desc)
                } else {
                    None
                }
            } else {
                None
            };
            out.push(NamePattern { pattern, dir });
            if !self.eat_punct(",") {
                break;
            }
        }
        Ok(out)
    }

    fn sub_pipeline(&mut self) -> Result<Vec<Operator>, ParseError> {
        self.expect_punct("(")?;
        let mut ops = vec![self.operator()?];
        while self.eat_punct("|") {
            ops.push(self.operator()?);
        }
        self.expect_punct(")")?;
        Ok(ops)
    }

    fn mv_items(&mut self) -> Result<Vec<MvExpandItem>, ParseError> {
        let mut items = Vec::new();
        loop {
            let ne = self.named_expr()?;
            let to_type = if self.is_kw(0, "to") && self.is_kw(1, "typeof") {
                self.bump();
                self.bump();
                self.expect_punct("(")?;
                let t = self.type_name()?;
                self.expect_punct(")")?;
                Some(t)
            } else {
                None
            };
            items.push(MvExpandItem { name: ne.names.into_iter().next(), expr: ne.expr, to_type });
            if !self.eat_punct(",") {
                break;
            }
        }
        Ok(items)
    }

    fn at_operator_end(&self) -> bool {
        self.at_eof() || self.is_punct(0, "|") || self.is_punct(0, ")") || self.is_punct(0, ";") || self.is_punct(0, "}")
    }

    fn source_operator(&mut self) -> Result<Operator, ParseError> {
        self.operator()
    }

    fn operator(&mut self) -> Result<Operator, ParseError> {
        let Some((kw, len)) = self.compound_kw(0) else {
            return Err(self.unexpected("a query operator"));
        };
        let start_span = self.token(0).span;
        self.pos += len;
        let op = match kw.as_str() {
            "where" | "filter" => Operator::Where(self.or()?),
            "extend" => Operator::Extend(self.named_list()?),
            "project" => Operator::Project(self.named_list()?),
            "project-away" => Operator::ProjectAway(self.name_patterns(false)?),
            "project-keep" => Operator::ProjectKeep(self.name_patterns(false)?),
            "project-reorder" => Operator::ProjectReorder(self.name_patterns(true)?),
            "project-rename" => {
                let mut out = Vec::new();
                loop {
                    let new = self.name()?;
                    self.expect_punct("=")?;
                    let old = self.name()?;
                    out.push((new, old));
                    if !self.eat_punct(",") {
                        break;
                    }
                }
                Operator::ProjectRename(out)
            }
            "summarize" => {
                let params = self.op_params(&[])?;
                let aggs = if self.is_kw(0, "by") || self.at_operator_end() { Vec::new() } else { self.named_list()? };
                let by = if self.eat_kw("by") { self.named_list()? } else { Vec::new() };
                Operator::Summarize { params, aggs, by }
            }
            "sort" | "order" => {
                let _ = self.op_params(&[])?;
                self.expect_kw("by")?;
                let mut keys = vec![self.order_key()?];
                while self.eat_punct(",") {
                    keys.push(self.order_key()?);
                }
                Operator::Sort(keys)
            }
            "take" | "limit" => {
                let _ = self.op_params(&[])?;
                Operator::Take(self.or()?)
            }
            "top" => {
                let count = self.or()?;
                self.expect_kw("by")?;
                let key = self.order_key()?;
                Operator::Top { count, key, params: Vec::new() }
            }
            "top-nested" => self.top_nested()?,
            "top-hitters" => {
                let count = self.or()?;
                self.expect_kw("of")?;
                let of = self.or()?;
                let by = if self.eat_kw("by") { Some(self.or()?) } else { None };
                Operator::TopHitters { count, of, by }
            }
            "count" => {
                let name = if self.eat_kw("as") { Some(self.name()?) } else { None };
                Operator::Count { name }
            }
            "distinct" => {
                let _ = self.op_params(&[])?;
                let mut list = vec![self.or()?];
                while self.eat_punct(",") {
                    list.push(self.or()?);
                }
                Operator::Distinct(list)
            }
            "join" => {
                let params = self.op_params(&["kind"])?;
                let right = self.or()?;
                let on = if self.eat_kw("on") {
                    let mut on = vec![self.or()?];
                    while self.eat_punct(",") {
                        on.push(self.or()?);
                    }
                    on
                } else if self.is_kw(0, "where") {
                    return Err(ParseError { message: "join ... where is not supported".into(), span: self.token(0).span });
                } else {
                    Vec::new()
                };
                Operator::Join { params, right, on }
            }
            "lookup" => {
                let params = self.op_params(&["kind"])?;
                let right = self.or()?;
                self.expect_kw("on")?;
                let mut on = vec![self.or()?];
                while self.eat_punct(",") {
                    on.push(self.or()?);
                }
                Operator::Lookup { params, right, on }
            }
            "union" => {
                let params = self.op_params(&["kind", "withsource", "isfuzzy"])?;
                let mut tables = vec![self.union_table()?];
                while self.eat_punct(",") {
                    tables.push(self.union_table()?);
                }
                Operator::Union { params, tables }
            }
            "mv-expand" | "mvexpand" => {
                let params = self.op_params(&["bagexpansion", "with_itemindex", "kind"])?;
                let items = self.mv_items()?;
                let limit = if self.eat_kw("limit") { Some(self.or()?) } else { None };
                Operator::MvExpand { params, items, limit }
            }
            "mv-apply" | "mvapply" => {
                let params = self.op_params(&["bagexpansion", "with_itemindex", "kind"])?;
                let items = self.mv_items()?;
                let limit = if self.eat_kw("limit") { Some(self.or()?) } else { None };
                let context_id = if self.is_kw(0, "id") && self.is_punct(1, "=") {
                    self.pos += 2;
                    Some(self.name()?)
                } else {
                    None
                };
                self.expect_kw("on")?;
                let body = self.sub_pipeline()?;
                Operator::MvApply { params, items, limit, context_id, body }
            }
            "parse" | "parse-where" => {
                let params = self.op_params(&["kind", "flags"])?;
                let expr = self.or()?;
                self.expect_kw("with")?;
                let mut parts = Vec::new();
                while !self.at_operator_end() {
                    match self.tok(0).clone() {
                        Tok::Str(s) => {
                            self.bump();
                            parts.push(ParsePart::Text(s));
                        }
                        Tok::Punct("*") => {
                            self.bump();
                            parts.push(ParsePart::Star);
                        }
                        Tok::Ident(_) | Tok::Punct("[") => {
                            let name = self.name()?;
                            let ty = if self.eat_punct(":") { Some(self.type_name()?) } else { None };
                            parts.push(ParsePart::Column { name, ty });
                        }
                        _ => return Err(self.unexpected("a parse pattern element")),
                    }
                    let _ = self.eat_punct(",");
                }
                Operator::Parse { params, expr, parts, filter: kw == "parse-where" }
            }
            "parse-kv" => {
                let expr = self.or()?;
                self.expect_kw("as")?;
                self.expect_punct("(")?;
                let mut columns = vec![self.column_decl()?];
                while self.eat_punct(",") {
                    columns.push(self.column_decl()?);
                }
                self.expect_punct(")")?;
                let mut params = Vec::new();
                if self.eat_kw("with") {
                    self.expect_punct("(")?;
                    loop {
                        let name = self.name()?.to_ascii_lowercase();
                        self.expect_punct("=")?;
                        let value = self.or()?;
                        params.push(OpParam { name, value });
                        if !self.eat_punct(",") {
                            break;
                        }
                    }
                    self.expect_punct(")")?;
                }
                Operator::ParseKv { expr, columns, params }
            }
            "as" => {
                let params = self.op_params(&[])?;
                Operator::As { params, name: self.name()? }
            }
            "serialize" => {
                let list = if self.at_operator_end() { Vec::new() } else { self.named_list()? };
                Operator::Serialize(list)
            }
            "sample" => Operator::Sample(self.or()?),
            "sample-distinct" => {
                let count = self.or()?;
                self.expect_kw("of")?;
                Operator::SampleDistinct { count, of: self.or()? }
            }
            "search" => {
                let params = self.op_params(&["kind"])?;
                let mut tables = Vec::new();
                if self.is_kw(0, "in") && self.is_punct(1, "(") {
                    self.bump();
                    self.bump();
                    loop {
                        tables.push(self.union_table()?);
                        if !self.eat_punct(",") {
                            break;
                        }
                    }
                    self.expect_punct(")")?;
                }
                Operator::Search { params, tables, predicate: self.or()? }
            }
            "make-series" => self.make_series()?,
            "scan" => self.scan()?,
            "evaluate" => {
                let params = self.op_params(&[])?;
                let name = self.name()?;
                let args = self.args()?;
                Operator::Evaluate { params, name, args }
            }
            "invoke" => {
                let name = self.name()?;
                let args = self.args()?;
                Operator::Invoke { name, args }
            }
            "render" => {
                let chart = self.name()?;
                let mut props = Vec::new();
                if self.eat_kw("with") {
                    self.expect_punct("(")?;
                    loop {
                        let name = self.name()?;
                        self.expect_punct("=")?;
                        let value = self.or()?;
                        props.push((name, value));
                        if !self.eat_punct(",") {
                            break;
                        }
                    }
                    self.expect_punct(")")?;
                }
                Operator::Render { chart, props }
            }
            "getschema" => Operator::GetSchema,
            "consume" => {
                let _ = self.op_params(&["decodeblocks"])?;
                Operator::Consume
            }
            "fork" => {
                let mut branches = Vec::new();
                while self.is_punct(0, "(") || (self.ident_at(0).is_some() && self.is_punct(1, "=")) {
                    let name = if self.is_punct(0, "(") {
                        None
                    } else {
                        let n = self.name()?;
                        self.bump();
                        Some(n)
                    };
                    branches.push((name, self.sub_pipeline()?));
                }
                Operator::Fork(branches)
            }
            "facet" => {
                self.expect_kw("by")?;
                let mut by = vec![self.name()?];
                while self.eat_punct(",") {
                    by.push(self.name()?);
                }
                let with = if self.eat_kw("with") { Some(self.sub_pipeline()?) } else { None };
                Operator::Facet { by, with }
            }
            "partition" => {
                let params = self.op_params(&[])?;
                self.expect_kw("by")?;
                let by = Expr::Name(self.name()?);
                if self.is_punct(0, "{") {
                    return Err(ParseError { message: "partition with a { subquery } is not supported".into(), span: self.token(0).span });
                }
                let body = self.sub_pipeline()?;
                Operator::Partition { params, by, body }
            }
            "reduce" => {
                self.expect_kw("by")?;
                let by = self.or()?;
                let mut params = Vec::new();
                if self.eat_kw("with") {
                    params = self.op_params(&["threshold", "characters"])?;
                }
                Operator::Reduce { by, params }
            }
            "print" => {
                let list = if self.at_operator_end() { Vec::new() } else { self.named_list()? };
                Operator::Print(list)
            }
            "datatable" => {
                let _ = self.op_params(&[])?;
                self.expect_punct("(")?;
                let mut columns = vec![self.column_decl()?];
                while self.eat_punct(",") {
                    columns.push(self.column_decl()?);
                }
                self.expect_punct(")")?;
                self.expect_punct("[")?;
                let mut values = Vec::new();
                if !self.is_punct(0, "]") {
                    loop {
                        values.push(self.or()?);
                        if !self.eat_punct(",") || self.is_punct(0, "]") {
                            break;
                        }
                    }
                }
                self.expect_punct("]")?;
                Operator::DataTable { columns, values }
            }
            "range" => {
                let name = self.name()?;
                self.expect_kw("from")?;
                let from = self.or()?;
                self.expect_kw("to")?;
                let to = self.or()?;
                self.expect_kw("step")?;
                let step = self.or()?;
                Operator::Range { name, from, to, step }
            }
            "externaldata" => {
                self.expect_punct("(")?;
                let mut columns = vec![self.column_decl()?];
                while self.eat_punct(",") {
                    columns.push(self.column_decl()?);
                }
                self.expect_punct(")")?;
                self.expect_punct("[")?;
                let mut uris = vec![self.or()?];
                while self.eat_punct(",") {
                    uris.push(self.or()?);
                }
                self.expect_punct("]")?;
                let mut props = Vec::new();
                if self.eat_kw("with") {
                    self.expect_punct("(")?;
                    loop {
                        let name = self.name()?;
                        self.expect_punct("=")?;
                        props.push((name, self.or()?));
                        if !self.eat_punct(",") {
                            break;
                        }
                    }
                    self.expect_punct(")")?;
                }
                Operator::ExternalData { columns, uris, props }
            }
            "find" => {
                return Err(ParseError { message: "the 'find' operator is not supported".into(), span: start_span });
            }
            other => {
                return Err(ParseError { message: format!("unknown query operator '{other}'"), span: start_span });
            }
        };
        Ok(op)
    }

    fn union_table(&mut self) -> Result<Expr, ParseError> {
        // wildcard table names: Storm*, *Events
        if (matches!(self.tok(0), Tok::Ident(_)) && self.is_punct(1, "*") && self.adjacent(0))
            || (self.is_punct(0, "*") && matches!(self.tok(1), Tok::Ident(_)) && self.adjacent(0))
        {
            return Ok(Expr::Name(self.name_pattern()?));
        }
        self.or()
    }

    fn top_nested(&mut self) -> Result<Operator, ParseError> {
        let mut levels = Vec::new();
        loop {
            let count = if self.is_kw(0, "of") { None } else { Some(self.additive()?) };
            self.expect_kw("of")?;
            let ne = self.named_expr()?;
            let others = if self.is_kw(0, "with") && self.is_kw(1, "others") {
                self.bump();
                self.bump();
                self.expect_punct("=")?;
                Some(self.or()?)
            } else {
                None
            };
            self.expect_kw("by")?;
            let by = self.named_expr()?;
            let (dir, nulls) = self.sort_suffix()?;
            levels.push(TopNestedLevel {
                count,
                name: ne.names.into_iter().next(),
                of: ne.expr,
                others,
                by_name: by.names.into_iter().next(),
                by: by.expr,
                dir,
                nulls,
            });
            if !self.eat_punct(",") {
                break;
            }
            if !(self.is_kw(0, "top-nested") || (self.is_kw(0, "top") && self.is_punct(1, "-"))) {
                return Err(self.unexpected("'top-nested'"));
            }
            let (_, len) = self.compound_kw(0).unwrap();
            self.pos += len;
        }
        Ok(Operator::TopNested(levels))
    }

    fn make_series(&mut self) -> Result<Operator, ParseError> {
        let params = self.op_params(&["kind"])?;
        let mut aggs = Vec::new();
        loop {
            let ne = self.named_expr()?;
            let default = if self.is_kw(0, "default") && self.is_punct(1, "=") {
                self.pos += 2;
                Some(self.or()?)
            } else {
                None
            };
            aggs.push(MakeSeriesAgg { name: ne.names.into_iter().next(), expr: ne.expr, default });
            if !self.eat_punct(",") {
                break;
            }
        }
        self.expect_kw("on")?;
        self.no_in = true;
        let on = self.or();
        self.no_in = false;
        let on = on?;
        let (mut from, mut to, step);
        if self.is_kw(0, "in") && self.is_kw(1, "range") {
            self.bump();
            self.bump();
            let args = self.args()?;
            if args.len() != 3 {
                return Err(self.unexpected("range(start, stop, step)"));
            }
            let mut it = args.into_iter().map(|a| a.expr);
            from = it.next();
            to = it.next();
            step = it.next().unwrap();
        } else {
            from = None;
            to = None;
            if self.eat_kw("from") {
                from = Some(self.or()?);
            }
            if self.eat_kw("to") {
                to = Some(self.or()?);
            }
            self.expect_kw("step")?;
            step = self.or()?;
        }
        let by = if self.eat_kw("by") { self.named_list()? } else { Vec::new() };
        Ok(Operator::MakeSeries { params, aggs, on, from, to, step, by })
    }

    fn scan(&mut self) -> Result<Operator, ParseError> {
        let _ = self.op_params(&["with_match_id"])?;
        let mut order_by = Vec::new();
        let mut partition_by = Vec::new();
        let mut declare = Vec::new();
        loop {
            if self.is_kw(0, "order") && self.is_kw(1, "by") {
                self.pos += 2;
                order_by.push(self.order_key()?);
                while self.eat_punct(",") {
                    order_by.push(self.order_key()?);
                }
            } else if self.is_kw(0, "partition") && self.is_kw(1, "by") {
                self.pos += 2;
                partition_by.push(self.or()?);
                while self.eat_punct(",") {
                    partition_by.push(self.or()?);
                }
            } else if self.eat_kw("declare") {
                self.expect_punct("(")?;
                loop {
                    let name = self.name()?;
                    self.expect_punct(":")?;
                    let ty = self.type_name()?;
                    let default = if self.eat_punct("=") { Some(self.or()?) } else { None };
                    declare.push((name, ty, default));
                    if !self.eat_punct(",") {
                        break;
                    }
                }
                self.expect_punct(")")?;
            } else {
                break;
            }
        }
        self.expect_kw("with")?;
        self.expect_punct("(")?;
        let mut steps = Vec::new();
        while self.eat_kw("step") {
            let name = self.name()?;
            let optional = self.eat_kw("optional");
            self.expect_punct(":")?;
            let condition = self.or()?;
            let mut assignments = Vec::new();
            if self.eat_punct("=>") {
                loop {
                    let n = self.name()?;
                    self.expect_punct("=")?;
                    assignments.push((n, self.or()?));
                    if !self.eat_punct(",") {
                        break;
                    }
                }
            }
            self.expect_punct(";")?;
            steps.push(ScanStep { name, optional, condition, assignments });
        }
        self.expect_punct(")")?;
        Ok(Operator::Scan { order_by, partition_by, declare, steps })
    }
}

/// Decodes a typed literal such as `datetime(2020-01-01)` or `long(null)`.
fn goo_literal(ty: &str, text: &str) -> Result<Literal, String> {
    let t = text.trim();
    let canonical = match ty {
        "boolean" => "bool",
        "double" => "real",
        "date" => "datetime",
        "time" => "timespan",
        "uuid" | "uniqueid" => "guid",
        other => other,
    };
    if t.eq_ignore_ascii_case("null") {
        return Ok(Literal::Null(Some(canonical.to_string())));
    }
    let bad = || format!("invalid {canonical} literal '{t}'");
    Ok(match canonical {
        "bool" => match t.to_ascii_lowercase().as_str() {
            "true" | "1" => Literal::Bool(true),
            "false" | "0" => Literal::Bool(false),
            _ => return Err(bad()),
        },
        "int" => Literal::Int(parse_int(t).ok_or_else(bad)?),
        "long" => Literal::Long(parse_int(t).ok_or_else(bad)?),
        "real" => Literal::Real(match t.to_ascii_lowercase().as_str() {
            "nan" => f64::NAN,
            "inf" | "+inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            _ => t.parse().map_err(|_| bad())?,
        }),
        "decimal" => Literal::Decimal(t.to_string()),
        "datetime" => Literal::DateTime(t.to_string()),
        "timespan" => Literal::TimeSpan(parse_timespan_text(t).ok_or_else(bad)?),
        "guid" => Literal::Guid(t.to_string()),
        _ => return Err(bad()),
    })
}

fn parse_int(t: &str) -> Option<i64> {
    let (neg, body) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let v = if let Some(hex) = body.strip_prefix("0x").or_else(|| body.strip_prefix("0X")) {
        u64::from_str_radix(hex, 16).ok()? as i64
    } else {
        // allow "-9223372036854775808"
        return if neg { format!("-{body}").parse().ok() } else { body.parse().ok() };
    };
    Some(if neg { -v } else { v })
}
