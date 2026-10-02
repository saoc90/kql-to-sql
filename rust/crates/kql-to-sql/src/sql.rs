//! A small structured model of the SQL we emit, plus rendering.
//!
//! Operators build on a [`Select`] and decide whether their clause fits into it or whether the
//! current select must be wrapped as a subquery. Expressions are kept as SQL text, but every
//! expression string is *atomic* (a literal, identifier, function call or parenthesized group), so
//! composing them never changes precedence.

/// One `SELECT` block.
#[derive(Debug, Clone, Default)]
pub struct Select {
    pub distinct: bool,
    /// `None` renders as `*`.
    pub items: Option<Vec<Item>>,
    pub from: From,
    pub filters: Vec<String>,
    pub group_by: Vec<String>,
    pub having: Vec<String>,
    pub order_by: Vec<String>,
    pub limit: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub sql: String,
    pub alias: String,
}

#[derive(Debug, Clone, Default)]
pub enum From {
    #[default]
    None,
    /// A table or CTE name (already quoted).
    Table(String),
    /// A subquery; rendered as `(...) AS alias`.
    Query(Box<Query>, String),
    /// Any other FROM item, rendered verbatim (table functions, joins).
    Raw(String),
}

#[derive(Debug, Clone)]
pub enum Query {
    Select(Box<Select>),
    /// `UNION ALL` etc. of several queries (each rendered in parentheses).
    SetOp { op: &'static str, parts: Vec<Query> },
}

impl Select {
    pub fn star_from(from: From) -> Select {
        Select { from, ..Default::default() }
    }

    /// True when this select outputs exactly the columns of its FROM (`SELECT *`, no grouping).
    pub fn is_passthrough(&self) -> bool {
        self.items.is_none() && !self.distinct && self.group_by.is_empty() && self.limit.is_none()
    }

    pub fn render(&self) -> String {
        let mut s = String::from("SELECT ");
        if self.distinct {
            s.push_str("DISTINCT ");
        }
        match &self.items {
            None => s.push('*'),
            Some(items) if items.is_empty() => s.push('*'),
            Some(items) => {
                let parts: Vec<String> = items
                    .iter()
                    .map(|i| if i.sql == quote_ident(&i.alias) { i.sql.clone() } else { format!("{} AS {}", i.sql, quote_ident(&i.alias)) })
                    .collect();
                s.push_str(&parts.join(", "));
            }
        }
        match &self.from {
            From::None => {}
            From::Table(t) => {
                s.push_str(" FROM ");
                s.push_str(t);
            }
            From::Query(q, alias) => {
                s.push_str(" FROM (");
                s.push_str(&q.render());
                s.push_str(") AS ");
                s.push_str(alias);
            }
            From::Raw(r) => {
                s.push_str(" FROM ");
                s.push_str(r);
            }
        }
        if !self.filters.is_empty() {
            s.push_str(" WHERE ");
            s.push_str(&self.filters.join(" AND "));
        }
        if !self.group_by.is_empty() {
            s.push_str(" GROUP BY ");
            s.push_str(&self.group_by.join(", "));
        }
        if !self.having.is_empty() {
            s.push_str(" HAVING ");
            s.push_str(&self.having.join(" AND "));
        }
        if !self.order_by.is_empty() {
            s.push_str(" ORDER BY ");
            s.push_str(&self.order_by.join(", "));
        }
        if let Some(l) = &self.limit {
            s.push_str(" LIMIT ");
            s.push_str(l);
        }
        s
    }
}

impl Query {
    pub fn render(&self) -> String {
        match self {
            Query::Select(s) => s.render(),
            Query::SetOp { op, parts } => {
                parts.iter().map(|p| format!("({})", p.render())).collect::<Vec<_>>().join(&format!(" {op} "))
            }
        }
    }
}

/// SQL keywords that must be quoted when used as identifiers (union of DuckDB and PostgreSQL
/// reserved words).
const RESERVED: &[&str] = &[
    "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "at", "authorization", "between",
    "binary", "both", "case", "cast", "check", "collate", "collation", "column", "concurrently", "constraint", "create",
    "cross", "current_catalog", "current_date", "current_role", "current_schema", "current_time",
    "current_timestamp", "current_user", "default", "deferrable", "desc", "describe", "distinct", "do", "else", "end",
    "except", "false", "fetch", "for", "foreign", "freeze", "from", "full", "glob", "grant", "group", "having", "ilike",
    "in", "initially", "inner", "intersect", "into", "is", "isnull", "join", "lateral", "leading", "left", "like",
    "limit", "localtime", "localtimestamp", "map", "natural", "not", "notnull", "null", "offset", "on", "only", "or",
    "order", "outer", "overlaps", "pivot", "pivot_longer", "pivot_wider", "placing", "primary", "qualify",
    "references", "returning", "right", "select", "semi", "anti", "session_user", "show", "similar", "some", "struct",
    "summarize", "symmetric", "table", "tablesample", "then", "to", "trailing", "true", "try_cast", "union", "unique",
    "unpivot", "user", "using", "variadic", "verbose", "when", "where", "window", "with", "year", "month", "day",
    "hour", "minute", "second", "interval", "over", "filter", "within", "row", "rows", "range", "value", "values",
    "time", "timestamp", "date", "position", "count", "sample", "lambda", "asof", "positional", "by", "key",
];

thread_local! {
    /// Set while translating for PostgreSQL, which folds unquoted identifiers to lower case:
    /// names with upper-case letters must then be quoted to keep their case.
    static PRESERVE_CASE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Sets identifier quoting for `dialect` until the guard is dropped.
pub(crate) fn quoting_for(dialect: crate::Dialect) -> impl Drop {
    struct Guard(bool);
    impl Drop for Guard {
        fn drop(&mut self) {
            PRESERVE_CASE.with(|c| c.set(self.0));
        }
    }
    let prev = PRESERVE_CASE.with(|c| c.replace(dialect == crate::Dialect::Postgres));
    Guard(prev)
}

/// Quotes an identifier the way the translator does for `dialect`.
pub fn quote_ident_for(name: &str, dialect: crate::Dialect) -> String {
    let _g = quoting_for(dialect);
    quote_ident(name)
}

/// Quotes an identifier unless it is a plain name (for PostgreSQL also: lower case).
pub fn quote_ident(name: &str) -> String {
    let preserve_case = PRESERVE_CASE.with(|c| c.get());
    let simple = !name.is_empty()
        && !(preserve_case && name.chars().any(|c| c.is_ascii_uppercase()))
        && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !RESERVED.contains(&name.to_ascii_lowercase().as_str());
    if simple {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// A SQL string literal.
pub fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting() {
        assert_eq!(quote_ident("State"), "State");
        assert_eq!(quote_ident("order"), "\"order\"");
        assert_eq!(quote_ident("my col"), "\"my col\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_str("it's"), "'it''s'");
    }

    #[test]
    fn render_select() {
        let s = Select {
            items: Some(vec![Item { sql: "a".into(), alias: "a".into() }, Item { sql: "(a + 1)".into(), alias: "b".into() }]),
            from: From::Table("T".into()),
            filters: vec!["(a > 1)".into(), "(a < 5)".into()],
            order_by: vec!["a ASC NULLS FIRST".into()],
            limit: Some("10".into()),
            ..Default::default()
        };
        assert_eq!(s.render(), "SELECT a, (a + 1) AS b FROM T WHERE (a > 1) AND (a < 5) ORDER BY a ASC NULLS FIRST LIMIT 10");
    }
}
