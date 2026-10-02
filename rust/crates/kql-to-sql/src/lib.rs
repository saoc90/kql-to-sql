//! KQL → SQL translator.
//!
//! Pipeline: parse (kql-parser) → bind against a [`Catalog`] (every column gets a Kusto type)
//! → build a structured SQL query → render for a [`Dialect`].

use std::fmt;

mod advanced;
mod aggs;
mod binder;
#[allow(dead_code)]
mod catalog_data;
mod datefmt;
pub mod datetime;
mod dialect;
mod expr;
mod funcs;
mod funcs_extra;
mod join;
mod names;
mod op_evaluate;
mod op_search;
mod op_series;
mod op_subquery;
mod ops;
mod regex;
pub mod sql;
mod types;
mod window;

pub use types::{common_type, widest};

/// Kusto scalar types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KqlType {
    Bool,
    Int,
    Long,
    Real,
    Decimal,
    String,
    DateTime,
    TimeSpan,
    Guid,
    Dynamic,
}

impl KqlType {
    pub fn name(self) -> &'static str {
        match self {
            KqlType::Bool => "bool",
            KqlType::Int => "int",
            KqlType::Long => "long",
            KqlType::Real => "real",
            KqlType::Decimal => "decimal",
            KqlType::String => "string",
            KqlType::DateTime => "datetime",
            KqlType::TimeSpan => "timespan",
            KqlType::Guid => "guid",
            KqlType::Dynamic => "dynamic",
        }
    }
}

impl fmt::Display for KqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A named, typed output column.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub ty: KqlType,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: KqlType) -> Column {
        Column { name: name.into(), ty }
    }
}

/// Table schemas the query can reference.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    pub tables: Vec<(String, Vec<Column>)>,
}

impl Catalog {
    pub fn new() -> Catalog {
        Catalog::default()
    }

    /// Adds (or replaces) a table schema.
    pub fn with_table(mut self, name: impl Into<String>, columns: Vec<Column>) -> Catalog {
        let name = name.into();
        self.tables.retain(|(n, _)| *n != name);
        self.tables.push((name, columns));
        self
    }

    /// Looks a table up by name: exact match first, then case-insensitively.
    pub fn table(&self, name: &str) -> Option<(&str, &Vec<Column>)> {
        self.tables
            .iter()
            .find(|(n, _)| n == name)
            .or_else(|| self.tables.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)))
            .map(|(n, c)| (n.as_str(), c))
    }
}

/// Target SQL dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    DuckDb,
    Postgres,
}

/// The result of translating one KQL query.
#[derive(Debug, Clone)]
pub struct Translation {
    pub sql: String,
    /// Output schema, with Kusto types. Timespan columns are emitted as SQL INTERVAL,
    /// dynamic columns as JSON.
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    pub message: String,
}

impl Error {
    pub fn new(message: impl Into<String>) -> Error {
        Error { message: message.into() }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn err<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(message))
}

impl From<kql_parser::ParseError> for Error {
    fn from(e: kql_parser::ParseError) -> Error {
        Error::new(format!("syntax error: {e}"))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

/// Translates a KQL query into SQL for `dialect`.
///
/// Every table the query references must be described in `catalog`, because translation is
/// type-directed (integer vs. real division, datetime arithmetic, dynamic access, ...).
pub fn translate(kql: &str, catalog: &Catalog, dialect: Dialect) -> Result<Translation> {
    let query = kql_parser::parse_query(kql)?;
    let d = dialect::get(dialect);
    let mut ctx = binder::Ctx::new(d, catalog);
    let mut env = binder::Env::default();
    let Some(result) = ctx.statements(&query.statements, &mut env)? else {
        return err("the query has no result expression");
    };
    let rel = if ctx.is_tabular(result, &env) {
        ctx.tabular(result, &env)?
    } else {
        // a bare scalar expression: `print`-like single value
        let t = ctx.expr(result, &expr::Scope::empty(), &env)?;
        let sel = sql::Select { items: Some(vec![sql::Item { sql: ops::output_sql(&ctx, &t), alias: "print_0".into() }]), ..Default::default() };
        binder::Rel::from_select(sel, vec![Column::new("print_0", t.ty)])
    };
    Ok(finish(&mut ctx, rel))
}

/// Applies the final output representation (timespan ticks → INTERVAL) and the logical order.
fn finish(ctx: &mut binder::Ctx, rel: binder::Rel) -> Translation {
    let columns = rel.cols.clone();
    let rel = if columns.iter().any(|c| c.ty == KqlType::TimeSpan) {
        let order = rel.order.clone();
        let physical = !rel.sel.order_by.is_empty() && rel.order.is_empty();
        let mut r = if physical { rel } else { rel.wrap(ctx) };
        if physical {
            r = r.wrap_keep_order(ctx);
        }
        let items = columns
            .iter()
            .map(|c| {
                let q = sql::quote_ident(&c.name);
                let s = if c.ty == KqlType::TimeSpan { ctx.d.interval_from_ticks(&q) } else { q };
                sql::Item { sql: s, alias: c.name.clone() }
            })
            .collect();
        r.sel.items = Some(items);
        // order by the underlying tick values
        r.sel.order_by = order.iter().map(|o| o.sql()).collect();
        r.order.clear();
        r
    } else {
        rel
    };
    let body = rel.into_query().render();
    let sql = if ctx.ctes.is_empty() {
        body
    } else {
        let ctes: Vec<String> = ctx
            .ctes
            .iter()
            .map(|(n, q, m)| format!("{n} AS {}({q})", if *m { "MATERIALIZED " } else { "" }))
            .collect();
        format!("WITH {} {body}", ctes.join(", "))
    };
    Translation { sql, columns }
}
