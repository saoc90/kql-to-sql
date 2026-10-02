//! Parser for the Kusto Query Language (KQL).
//!
//! Lexical rules, operator precedence and operator syntax are ported from Microsoft's
//! [Kusto.Language](https://github.com/microsoft/Kusto-Query-Language) (Apache-2.0), the parser
//! behind Azure Data Explorer's tooling. This crate produces a compact AST for translation rather
//! than a full-fidelity syntax tree.

pub mod ast;
mod lexer;
mod parser;

use std::fmt;

pub use lexer::parse_timespan_text;
pub use parser::{parse_expression, parse_query};

#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
    pub span: ast::Span,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (at offset {})", self.message, self.span.start)
    }
}

impl std::error::Error for ParseError {}
