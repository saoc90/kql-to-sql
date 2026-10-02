//! Kusto management commands (`.create table`, `.set-or-append`, `.show tables`, ...) translated
//! to SQL statements.
//!
//! Commands are tokenized with the KQL lexer and parsed explicitly; every identifier is quoted and
//! every string literal escaped. Query parts (`<| query`, function bodies) go through the regular
//! query translator. Commands that would touch the file system or external storage (`.export`,
//! external tables, ingestion from URIs) are deliberately not supported.

use kql_parser::lexer::{tokenize, Tok, Token};

use crate::sql::{quote_ident, quote_str};
use crate::{dialect, err, Catalog, Column, Dialect, Error, KqlType, Result};

/// The SQL for one management command.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandTranslation {
    /// Statements to run in order (in one transaction if possible).
    pub statements: Vec<String>,
    /// For `.show` commands: the result columns of the last statement.
    pub columns: Vec<Column>,
}

/// True if `text` is a management command (starts with `.`).
pub fn is_command(text: &str) -> bool {
    let t = text.trim_start();
    let t = skip_comments(t);
    t.starts_with('.')
}

fn skip_comments(mut t: &str) -> &str {
    loop {
        t = t.trim_start();
        match t.strip_prefix("//") {
            Some(rest) => t = rest.find('\n').map(|i| &rest[i + 1..]).unwrap_or(""),
            None => return t,
        }
    }
}

/// Translates a management command.
pub fn translate_command(text: &str, catalog: &Catalog, dialect: Dialect) -> Result<CommandTranslation> {
    let tokens = tokenize(text).map_err(Error::from)?;
    let mut c = Cmd { src: text, toks: tokens, pos: 0, catalog, dialect };
    c.command()
}

struct Cmd<'a> {
    src: &'a str,
    toks: Vec<Token>,
    pos: usize,
    catalog: &'a Catalog,
    dialect: Dialect,
}

fn ddl(statements: Vec<String>) -> CommandTranslation {
    CommandTranslation { statements, columns: Vec::new() }
}

impl<'a> Cmd<'a> {
    fn tok(&self, n: usize) -> &Tok {
        &self.toks[(self.pos + n).min(self.toks.len() - 1)].tok
    }

    fn bump(&mut self) -> Token {
        let t = self.toks[self.pos.min(self.toks.len() - 1)].clone();
        if self.pos < self.toks.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn adjacent(&self, n: usize) -> bool {
        let i = (self.pos + n).min(self.toks.len() - 1);
        let j = (i + 1).min(self.toks.len() - 1);
        self.toks[i].span.end == self.toks[j].span.start
    }

    fn at_end(&self) -> bool {
        matches!(self.tok(0), Tok::Eof)
    }

    fn is_punct(&self, n: usize, p: &str) -> bool {
        matches!(self.tok(n), Tok::Punct(x) if *x == p)
    }

    fn is_word(&self, n: usize, w: &str) -> bool {
        matches!(self.tok(n), Tok::Ident(s) if s.eq_ignore_ascii_case(w))
    }

    fn eat_word(&mut self, w: &str) -> bool {
        if self.is_word(0, w) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn eat_punct(&mut self, p: &str) -> bool {
        if self.is_punct(0, p) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_punct(&mut self, p: &str) -> Result<()> {
        if self.eat_punct(p) {
            Ok(())
        } else {
            err(format!("expected '{p}' {}", self.here()))
        }
    }

    fn expect_word(&mut self, w: &str) -> Result<()> {
        if self.eat_word(w) {
            Ok(())
        } else {
            err(format!("expected '{w}' {}", self.here()))
        }
    }

    fn here(&self) -> String {
        let t = &self.toks[self.pos.min(self.toks.len() - 1)];
        match t.tok {
            Tok::Eof => "at end of command".into(),
            _ => format!("at '{}'", &self.src[t.span.start..t.span.end]),
        }
    }

    /// A (possibly hyphenated) keyword made of adjacent tokens: `create-merge`, `set-or-append`.
    fn keyword(&mut self) -> Result<String> {
        let mut kw = match self.bump().tok {
            Tok::Ident(s) => s.to_ascii_lowercase(),
            _ => return err(format!("expected a command keyword {}", self.here())),
        };
        while self.is_punct(0, "-") && self.pos > 0 && self.toks[self.pos - 1].span.end == self.toks[self.pos].span.start && self.adjacent(0) {
            self.bump();
            match self.bump().tok {
                Tok::Ident(s) => {
                    kw.push('-');
                    kw.push_str(&s.to_ascii_lowercase());
                }
                _ => return err("malformed command keyword"),
            }
        }
        Ok(kw)
    }

    /// An entity name: identifier, `['name']`, or a dotted `db.table` (database part ignored).
    fn name(&mut self) -> Result<String> {
        let mut n = self.simple_name()?;
        while self.is_punct(0, ".") && matches!(self.tok(1), Tok::Ident(_) | Tok::Punct("[")) {
            self.bump();
            n = self.simple_name()?;
        }
        Ok(n)
    }

    fn simple_name(&mut self) -> Result<String> {
        match self.tok(0).clone() {
            Tok::Ident(s) => {
                self.bump();
                Ok(s)
            }
            Tok::Punct("[") => {
                self.bump();
                let s = match self.bump().tok {
                    Tok::Str(s) => s,
                    _ => return err("expected a quoted name inside [...]"),
                };
                self.expect_punct("]")?;
                Ok(s)
            }
            _ => err(format!("expected a name {}", self.here())),
        }
    }

    fn string(&mut self) -> Result<String> {
        match self.bump().tok {
            Tok::Str(s) => Ok(s),
            _ => err(format!("expected a string literal {}", self.here())),
        }
    }

    /// `(name:type, ...)`
    fn schema(&mut self) -> Result<Vec<Column>> {
        self.expect_punct("(")?;
        let mut cols = Vec::new();
        if !self.is_punct(0, ")") {
            loop {
                let n = self.name()?;
                self.expect_punct(":")?;
                let t = self.simple_name()?;
                let ty = KqlType::from_name(&t).ok_or_else(|| Error::new(format!("unknown type '{t}'")))?;
                cols.push(Column::new(n, ty));
                if !self.eat_punct(",") {
                    break;
                }
            }
        }
        self.expect_punct(")")?;
        Ok(cols)
    }

    /// `with (k = v, ...)` properties (values are kept as text).
    fn with_props(&mut self) -> Result<Vec<(String, String)>> {
        let mut props = Vec::new();
        if !self.eat_word("with") {
            return Ok(props);
        }
        self.expect_punct("(")?;
        loop {
            let k = self.name()?.to_ascii_lowercase();
            self.expect_punct("=")?;
            let v = match self.bump().tok {
                Tok::Str(s) => s,
                Tok::Ident(s) => s,
                Tok::Long(v) => v.to_string(),
                _ => return err(format!("unsupported property value for '{k}'")),
            };
            props.push((k, v));
            if !self.eat_punct(",") {
                break;
            }
        }
        self.expect_punct(")")?;
        Ok(props)
    }

    /// The text after `<|`.
    fn piped_text(&mut self) -> Result<&'a str> {
        if !self.is_punct(0, "<|") {
            return err(format!("expected '<|' {}", self.here()));
        }
        let end = self.toks[self.pos].span.end;
        self.pos = self.toks.len() - 1;
        Ok(&self.src[end..])
    }

    /// The text of a `{ ... }` block (balanced braces).
    fn braced_text(&mut self) -> Result<&'a str> {
        if !self.is_punct(0, "{") {
            return err(format!("expected '{{' {}", self.here()));
        }
        let start = self.toks[self.pos].span.end;
        let mut depth = 0;
        loop {
            match self.tok(0) {
                Tok::Eof => return err("unbalanced braces"),
                Tok::Punct("{") => depth += 1,
                Tok::Punct("}") => {
                    depth -= 1;
                    if depth == 0 {
                        let end = self.toks[self.pos].span.start;
                        self.bump();
                        return Ok(&self.src[start..end]);
                    }
                }
                _ => {}
            }
            self.bump();
        }
    }

    fn done(&self) -> Result<()> {
        if self.at_end() {
            Ok(())
        } else {
            err(format!("unexpected input {}", self.here()))
        }
    }

    fn sql_type(&self, t: KqlType) -> &'static str {
        let d = dialect::get(self.dialect);
        match t {
            // stored tables keep timespans as INTERVAL (queries convert to ticks on read)
            KqlType::TimeSpan => match self.dialect {
                Dialect::DuckDb => "INTERVAL",
                Dialect::Postgres => "interval",
            },
            KqlType::Decimal => match self.dialect {
                Dialect::DuckDb => "DECIMAL(38, 18)",
                Dialect::Postgres => "numeric",
            },
            t => d.sql_type(t),
        }
    }

    fn column_defs(&self, cols: &[Column]) -> String {
        cols.iter().map(|c| format!("{} {}", quote_ident(&c.name), self.sql_type(c.ty))).collect::<Vec<_>>().join(", ")
    }

    fn query_sql(&self, kql: &str) -> Result<String> {
        Ok(crate::translate(kql, self.catalog, self.dialect)?.sql)
    }

    fn table_cols(&self, table: &str) -> Result<Vec<Column>> {
        match self.catalog.table(table) {
            Some((_, cols)) => Ok(cols.clone()),
            None => err(format!("unknown table '{table}'")),
        }
    }

    fn command(&mut self) -> Result<CommandTranslation> {
        self.expect_punct(".")?;
        let verb = self.keyword()?;
        match verb.as_str() {
            "create" | "create-merge" | "create-or-alter" => self.create(&verb),
            "alter" | "alter-merge" => self.alter(&verb),
            "drop" => self.drop(),
            "rename" => self.rename(),
            "set" | "append" | "set-or-append" | "set-or-replace" => self.set_append(&verb),
            "ingest" => self.ingest(),
            "show" => self.show(),
            "clear" => {
                self.expect_word("table")?;
                let t = self.name()?;
                self.expect_word("data")?;
                self.done()?;
                Ok(ddl(vec![format!("DELETE FROM {}", quote_ident(&t))]))
            }
            "delete" => {
                // .delete table T records <| T | where ...
                self.expect_word("table")?;
                let t = self.name()?;
                self.expect_word("records")?;
                let q = self.piped_text()?;
                let sql = self.query_sql(q)?;
                let cols = self.table_cols(&t)?;
                let names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
                Ok(ddl(vec![format!(
                    "DELETE FROM {t} WHERE ({cols}) IN (SELECT {cols} FROM ({sql}) AS _d)",
                    t = quote_ident(&t),
                    cols = names.join(", ")
                )]))
            }
            "export" => err(".export is not supported (it would write to the server's file system)"),
            other => err(format!("unsupported command '.{other}'")),
        }
    }

    fn create(&mut self, verb: &str) -> Result<CommandTranslation> {
        let what = self.keyword()?;
        match what.as_str() {
            "table" => {
                let t = self.name()?;
                if self.is_word(0, "based-on") || (self.is_word(0, "based") && self.is_punct(1, "-")) {
                    let _ = self.keyword()?;
                    let src = self.name()?;
                    let _ = self.with_props()?;
                    self.done()?;
                    let cols = self.table_cols(&src)?;
                    return Ok(ddl(vec![format!("CREATE TABLE IF NOT EXISTS {} ({})", quote_ident(&t), self.column_defs(&cols))]));
                }
                let cols = self.schema()?;
                let props = self.with_props()?;
                self.done()?;
                let qt = quote_ident(&t);
                let mut stmts = match verb {
                    "create" => vec![format!("CREATE TABLE IF NOT EXISTS {qt} ({})", self.column_defs(&cols))],
                    _ => {
                        // create-merge: create if missing, then add any missing columns
                        let mut s = vec![format!("CREATE TABLE IF NOT EXISTS {qt} ({})", self.column_defs(&cols))];
                        for c in &cols {
                            s.push(format!("ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {} {}", quote_ident(&c.name), self.sql_type(c.ty)));
                        }
                        s
                    }
                };
                if let Some((_, doc)) = props.iter().find(|(k, _)| k == "docstring") {
                    stmts.push(format!("COMMENT ON TABLE {qt} IS {}", quote_str(doc)));
                }
                Ok(ddl(stmts))
            }
            "tables" => {
                // .create tables A (..), B (..)
                let mut stmts = Vec::new();
                loop {
                    let t = self.name()?;
                    let cols = self.schema()?;
                    stmts.push(format!("CREATE TABLE IF NOT EXISTS {} ({})", quote_ident(&t), self.column_defs(&cols)));
                    if !self.eat_punct(",") {
                        break;
                    }
                }
                let _ = self.with_props()?;
                self.done()?;
                Ok(ddl(stmts))
            }
            "function" => {
                let ifnotexists = self.eat_word("ifnotexists");
                let _ = self.with_props()?;
                let name = self.name()?;
                self.expect_punct("(")?;
                if !self.eat_punct(")") {
                    return err("functions with parameters cannot be stored as SQL views; only parameterless functions are supported");
                }
                let body = self.braced_text()?;
                self.done()?;
                let sql = self.query_sql(body)?;
                let create = match (verb, ifnotexists) {
                    (_, true) => "CREATE VIEW IF NOT EXISTS",
                    ("create", false) => "CREATE VIEW",
                    _ => "CREATE OR REPLACE VIEW",
                };
                let create = if self.dialect == Dialect::Postgres && ifnotexists { "CREATE OR REPLACE VIEW" } else { create };
                Ok(ddl(vec![format!("{create} {} AS {sql}", quote_ident(&name))]))
            }
            "materialized-view" => {
                let _ = self.with_props()?;
                let name = self.name()?;
                self.expect_word("on")?;
                let _ = self.keyword()?; // table / materialized-view
                let _source = self.name()?;
                let body = self.braced_text()?;
                self.done()?;
                let sql = self.query_sql(body)?;
                // materialized views are kept up to date by Kusto; a plain view gives the same results
                let create = if verb == "create" { "CREATE VIEW" } else { "CREATE OR REPLACE VIEW" };
                Ok(ddl(vec![format!("{create} {} AS {sql}", quote_ident(&name))]))
            }
            "external" => err("external tables are not supported (they would read the server's file system)"),
            other => err(format!("unsupported command '.{verb} {other}'")),
        }
    }

    fn alter(&mut self, verb: &str) -> Result<CommandTranslation> {
        let what = self.keyword()?;
        match what.as_str() {
            "table" => {
                let t = self.name()?;
                let qt = quote_ident(&t);
                if self.eat_word("docstring") {
                    let doc = self.string()?;
                    self.done()?;
                    return Ok(ddl(vec![format!("COMMENT ON TABLE {qt} IS {}", quote_str(&doc))]));
                }
                if self.is_word(0, "column") && self.is_punct(1, "-") {
                    let kw = self.keyword()?;
                    if kw == "column-docstrings" {
                        self.expect_punct("(")?;
                        let mut stmts = Vec::new();
                        loop {
                            let c = self.name()?;
                            self.expect_punct(":")?;
                            let doc = self.string()?;
                            stmts.push(format!("COMMENT ON COLUMN {qt}.{} IS {}", quote_ident(&c), quote_str(&doc)));
                            if !self.eat_punct(",") {
                                break;
                            }
                        }
                        self.expect_punct(")")?;
                        self.done()?;
                        return Ok(ddl(stmts));
                    }
                    return err(format!("unsupported command '.alter table {kw}'"));
                }
                let cols = self.schema()?;
                let _ = self.with_props()?;
                self.done()?;
                let existing = self.table_cols(&t)?;
                let mut stmts = Vec::new();
                for c in &cols {
                    match existing.iter().find(|e| e.name == c.name) {
                        None => stmts.push(format!("ALTER TABLE {qt} ADD COLUMN {} {}", quote_ident(&c.name), self.sql_type(c.ty))),
                        Some(e) if e.ty != c.ty && verb == "alter" => {
                            stmts.push(format!("ALTER TABLE {qt} ALTER COLUMN {} TYPE {}", quote_ident(&c.name), self.sql_type(c.ty)))
                        }
                        Some(e) if e.ty != c.ty => return err(format!(".alter-merge cannot change the type of column '{}'", e.name)),
                        Some(_) => {}
                    }
                }
                if verb == "alter" {
                    // .alter table sets the exact column list; data in kept columns is preserved
                    for e in &existing {
                        if !cols.iter().any(|c| c.name == e.name) {
                            stmts.push(format!("ALTER TABLE {qt} DROP COLUMN {}", quote_ident(&e.name)));
                        }
                    }
                }
                Ok(ddl(stmts))
            }
            "column" => {
                // .alter column T.c type = long
                let t = self.simple_name()?;
                self.expect_punct(".")?;
                let c = self.simple_name()?;
                self.expect_word("type")?;
                self.expect_punct("=")?;
                let ty_name = self.simple_name()?;
                let ty = KqlType::from_name(&ty_name).ok_or_else(|| Error::new(format!("unknown type '{ty_name}'")))?;
                self.done()?;
                Ok(ddl(vec![format!("ALTER TABLE {} ALTER COLUMN {} TYPE {}", quote_ident(&t), quote_ident(&c), self.sql_type(ty))]))
            }
            "function" | "materialized-view" => {
                // same as create-or-alter
                self.pos -= 1;
                self.create("create-or-alter")
            }
            other => err(format!("unsupported command '.{verb} {other}'")),
        }
    }

    fn drop(&mut self) -> Result<CommandTranslation> {
        let what = self.keyword()?;
        match what.as_str() {
            "table" => {
                let t = self.name()?;
                if self.eat_word("columns") {
                    self.expect_punct("(")?;
                    let mut stmts = Vec::new();
                    loop {
                        let c = self.name()?;
                        stmts.push(format!("ALTER TABLE {} DROP COLUMN {}", quote_ident(&t), quote_ident(&c)));
                        if !self.eat_punct(",") {
                            break;
                        }
                    }
                    self.expect_punct(")")?;
                    self.done()?;
                    return Ok(ddl(stmts));
                }
                let ifexists = self.eat_word("ifexists");
                self.done()?;
                Ok(ddl(vec![format!("DROP TABLE {}{}", if ifexists { "IF EXISTS " } else { "" }, quote_ident(&t))]))
            }
            "tables" => {
                self.expect_punct("(")?;
                let mut names = Vec::new();
                loop {
                    names.push(self.name()?);
                    if !self.eat_punct(",") {
                        break;
                    }
                }
                self.expect_punct(")")?;
                let ifexists = self.eat_word("ifexists");
                self.done()?;
                let ie = if ifexists { "IF EXISTS " } else { "" };
                Ok(ddl(names.iter().map(|n| format!("DROP TABLE {ie}{}", quote_ident(n))).collect()))
            }
            "column" => {
                let t = self.simple_name()?;
                self.expect_punct(".")?;
                let c = self.simple_name()?;
                let ifexists = self.eat_word("ifexists");
                self.done()?;
                Ok(ddl(vec![format!("ALTER TABLE {} DROP COLUMN {}{}", quote_ident(&t), if ifexists { "IF EXISTS " } else { "" }, quote_ident(&c))]))
            }
            "function" | "materialized-view" | "view" => {
                let n = self.name()?;
                let ifexists = self.eat_word("ifexists");
                self.done()?;
                Ok(ddl(vec![format!("DROP VIEW {}{}", if ifexists { "IF EXISTS " } else { "" }, quote_ident(&n))]))
            }
            other => err(format!("unsupported command '.drop {other}'")),
        }
    }

    fn rename(&mut self) -> Result<CommandTranslation> {
        let what = self.keyword()?;
        match what.as_str() {
            "table" => {
                let a = self.name()?;
                self.expect_word("to")?;
                let b = self.name()?;
                self.done()?;
                Ok(ddl(vec![format!("ALTER TABLE {} RENAME TO {}", quote_ident(&a), quote_ident(&b))]))
            }
            "tables" => {
                let mut stmts = Vec::new();
                loop {
                    let new = self.name()?;
                    self.expect_punct("=")?;
                    let old = self.name()?;
                    stmts.push(format!("ALTER TABLE {} RENAME TO {}", quote_ident(&old), quote_ident(&new)));
                    if !self.eat_punct(",") {
                        break;
                    }
                }
                self.done()?;
                Ok(ddl(stmts))
            }
            "column" => {
                let t = self.simple_name()?;
                self.expect_punct(".")?;
                let c = self.simple_name()?;
                self.expect_word("to")?;
                let n = self.simple_name()?;
                self.done()?;
                Ok(ddl(vec![format!("ALTER TABLE {} RENAME COLUMN {} TO {}", quote_ident(&t), quote_ident(&c), quote_ident(&n))]))
            }
            other => err(format!("unsupported command '.rename {other}'")),
        }
    }

    fn set_append(&mut self, verb: &str) -> Result<CommandTranslation> {
        let _async = self.eat_word("async");
        let t = self.name()?;
        let _ = self.with_props()?;
        let q = self.piped_text()?;
        let sql = self.query_sql(q)?;
        let qt = quote_ident(&t);
        let exists = self.catalog.table(&t).is_some();
        // `INSERT ... SELECT *` by position: the query's columns must match the table's order
        let insert = |cols: Option<&Vec<Column>>| match cols {
            Some(cols) => {
                let names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
                format!("INSERT INTO {qt} ({n}) SELECT {n} FROM ({sql}) AS _src", n = names.join(", "))
            }
            None => format!("INSERT INTO {qt} SELECT * FROM ({sql}) AS _src"),
        };
        let table_cols = self.catalog.table(&t).map(|(_, c)| c.clone());
        let stmts = match verb {
            "set" => {
                if exists {
                    return err(format!(".set: table '{t}' already exists (use .append or .set-or-append)"));
                }
                vec![format!("CREATE TABLE {qt} AS {sql}")]
            }
            "append" => {
                if !exists {
                    return err(format!(".append: unknown table '{t}'"));
                }
                vec![insert(table_cols.as_ref())]
            }
            "set-or-append" => {
                if exists {
                    vec![insert(table_cols.as_ref())]
                } else {
                    vec![format!("CREATE TABLE {qt} AS {sql}")]
                }
            }
            _ => vec![format!("DROP TABLE IF EXISTS {qt}"), format!("CREATE TABLE {qt} AS {sql}")],
        };
        Ok(ddl(stmts))
    }

    /// `.ingest inline into table T [with (format=csv)] <| rows`
    fn ingest(&mut self) -> Result<CommandTranslation> {
        let _async = self.eat_word("async");
        if !self.eat_word("inline") {
            return err("only '.ingest inline' is supported (ingestion from URIs would read external storage)");
        }
        self.expect_word("into")?;
        self.expect_word("table")?;
        let t = self.name()?;
        let props = self.with_props()?;
        let data = self.piped_text()?;
        let format = props.iter().find(|(k, _)| k == "format").map(|(_, v)| v.to_ascii_lowercase()).unwrap_or_else(|| "csv".into());
        let sep = match format.as_str() {
            "csv" => ',',
            "tsv" => '\t',
            "psv" => '|',
            "scsv" => ';',
            other => return err(format!(".ingest inline: unsupported format '{other}'")),
        };
        let cols = self.table_cols(&t)?;
        let mut rows = Vec::new();
        for record in parse_csv(data.trim_start_matches(['\r', '\n']), sep)? {
            if record.len() == 1 && record[0].is_empty() {
                continue;
            }
            let mut vals = Vec::new();
            for (i, c) in cols.iter().enumerate() {
                let field = record.get(i).map(String::as_str).unwrap_or("");
                vals.push(self.literal_for(field, c.ty));
            }
            rows.push(format!("({})", vals.join(", ")));
        }
        if rows.is_empty() {
            return Ok(ddl(Vec::new()));
        }
        let names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
        Ok(ddl(vec![format!("INSERT INTO {} ({}) VALUES {}", quote_ident(&t), names.join(", "), rows.join(", "))]))
    }

    /// A CSV field as a typed SQL literal; empty fields are NULL (strings: '').
    fn literal_for(&self, field: &str, ty: KqlType) -> String {
        let d = dialect::get(self.dialect);
        if field.is_empty() {
            return if ty == KqlType::String { "''".into() } else { "NULL".into() };
        }
        match ty {
            KqlType::String => quote_str(field),
            KqlType::DateTime => match crate::datetime::parse_datetime(field) {
                Some(us) => d.timestamp_literal(&crate::datetime::format_sql(us)),
                None => "NULL".into(),
            },
            KqlType::TimeSpan => match kql_parser::parse_timespan_text(field) {
                Some(ticks) => format!("CAST({} AS {})", quote_str(&format!("{} microseconds", ticks / 10)), self.sql_type(KqlType::TimeSpan)),
                None => "NULL".into(),
            },
            _ => format!("CAST({} AS {})", quote_str(field), self.sql_type(ty)),
        }
    }

    fn show(&mut self) -> Result<CommandTranslation> {
        let what = self.keyword()?;
        let s = |n: &str| Column::new(n, KqlType::String);
        let schema_filter = match self.dialect {
            Dialect::DuckDb => "table_schema = current_schema()",
            Dialect::Postgres => "table_schema = current_schema()",
        };
        match what.as_str() {
            "tables" => {
                self.done()?;
                Ok(CommandTranslation {
                    statements: vec![format!(
                        "SELECT table_name AS TableName, current_database() AS DatabaseName, '' AS Folder, '' AS DocString FROM information_schema.tables WHERE {schema_filter} AND table_type = 'BASE TABLE' ORDER BY table_name"
                    )],
                    columns: vec![s("TableName"), s("DatabaseName"), s("Folder"), s("DocString")],
                })
            }
            "table" => {
                let t = self.name()?;
                let _ = self.eat_word("schema") || self.eat_word("cslschema");
                let _ = self.eat_word("as") && (self.eat_word("json") || self.eat_word("csl"));
                self.done()?;
                let cols = self.table_cols(&t)?;
                // the schema is known from the catalog: answer with literal rows
                let rows: Vec<String> = cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("SELECT {} AS ColumnName, {i} AS ColumnOrdinal, {} AS ColumnType", quote_str(&c.name), quote_str(c.ty.name())))
                    .collect();
                Ok(CommandTranslation {
                    statements: vec![rows.join(" UNION ALL ")],
                    columns: vec![s("ColumnName"), Column::new("ColumnOrdinal", KqlType::Long), s("ColumnType")],
                })
            }
            "functions" => {
                self.done()?;
                Ok(CommandTranslation {
                    statements: vec![format!("SELECT table_name AS Name, '()' AS Parameters, view_definition AS Body FROM information_schema.views WHERE {schema_filter} ORDER BY table_name")],
                    columns: vec![s("Name"), s("Parameters"), s("Body")],
                })
            }
            "databases" | "database" => {
                self.done()?;
                Ok(CommandTranslation {
                    statements: vec!["SELECT current_database() AS DatabaseName".into()],
                    columns: vec![s("DatabaseName")],
                })
            }
            "version" => {
                self.done()?;
                Ok(CommandTranslation { statements: vec!["SELECT version() AS BuildVersion".into()], columns: vec![s("BuildVersion")] })
            }
            other => err(format!("unsupported command '.show {other}'")),
        }
    }
}

/// RFC 4180-style CSV parsing (quoted fields, doubled quotes, embedded separators/newlines).
fn parse_csv(text: &str, sep: char) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut chars = text.chars().peekable();
    let mut quoted = false;
    let mut any = false;
    while let Some(c) = chars.next() {
        any = true;
        if quoted {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    quoted = false;
                }
            } else {
                field.push(c);
            }
            continue;
        }
        match c {
            '"' if field.is_empty() => quoted = true,
            c if c == sep => row.push(std::mem::take(&mut field)),
            '\r' => {}
            '\n' => {
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
                any = false;
            }
            c => field.push(c),
        }
    }
    if quoted {
        return err(".ingest inline: unterminated quoted field");
    }
    if any {
        row.push(field);
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cat() -> Catalog {
        Catalog::new().with_table("T", vec![Column::new("a", KqlType::Long), Column::new("s", KqlType::String)])
    }

    fn sql(cmd: &str) -> Vec<String> {
        translate_command(cmd, &cat(), Dialect::DuckDb).unwrap().statements
    }

    #[test]
    fn create_and_alter() {
        assert_eq!(sql(".create table X (a:long, ['my col']:string)"), vec!["CREATE TABLE IF NOT EXISTS X (a BIGINT, \"my col\" VARCHAR)"]);
        // .alter keeps data: add/drop/retype individual columns only
        assert_eq!(sql(".alter table T (a:real, b:int)"), vec![
            "ALTER TABLE T ALTER COLUMN a TYPE DOUBLE",
            "ALTER TABLE T ADD COLUMN b INTEGER",
            "ALTER TABLE T DROP COLUMN s",
        ]);
        assert_eq!(sql(".alter-merge table T (b:int)"), vec!["ALTER TABLE T ADD COLUMN b INTEGER"]);
        assert_eq!(sql(".create-merge table N (a:long)"), vec!["CREATE TABLE IF NOT EXISTS N (a BIGINT)", "ALTER TABLE N ADD COLUMN IF NOT EXISTS a BIGINT"]);
    }

    #[test]
    fn quoting_blocks_injection() {
        assert_eq!(sql(".alter table T docstring \"x'; DROP TABLE T; --\""), vec!["COMMENT ON TABLE T IS 'x''; DROP TABLE T; --'"]);
        assert!(translate_command(".drop tables (x; DROP TABLE y)", &cat(), Dialect::DuckDb).is_err());
        assert_eq!(sql(".drop table ['a\"b'] ifexists"), vec!["DROP TABLE IF EXISTS \"a\"\"b\""]);
    }

    #[test]
    fn data_commands() {
        assert_eq!(sql(".set-or-append T <| print a = 1, s = 'x'"), vec!["INSERT INTO T (a, s) SELECT a, s FROM (SELECT CAST(1 AS BIGINT) AS a, CAST('x' AS VARCHAR) AS s) AS _src"]);
        assert_eq!(sql(".set-or-append New <| print a = 1"), vec!["CREATE TABLE New AS SELECT CAST(1 AS BIGINT) AS a"]);
        assert_eq!(
            sql(".ingest inline into table T <|\n1,\"a, \"\"b\"\"\"\n,"),
            vec!["INSERT INTO T (a, s) VALUES (CAST('1' AS BIGINT), 'a, \"b\"'), (NULL, '')"]
        );
        assert!(translate_command(".export to csv ('/etc/passwd') <| T", &cat(), Dialect::DuckDb).is_err());
    }
}
