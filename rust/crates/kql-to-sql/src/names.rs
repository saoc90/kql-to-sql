//! Kusto's column naming rules (ported from Kusto.Language `Binder_Projection` and
//! `ProjectionBuilder`).

use std::collections::HashMap;

use kql_parser::ast::{Expr, Literal};

use crate::catalog_data::{self, FnKind, ResultName};

/// The name an unnamed expression gets in a projection, or `None` if it gets a default name
/// (`Column1`, ...). `aggregate` selects the aggregate function table (summarize).
pub(crate) fn result_name(e: &Expr, aggregate: bool) -> Option<String> {
    match e {
        Expr::Paren(x) => result_name(x, aggregate),
        Expr::Name(n) => Some(n.clone()),
        Expr::Member { expr, name } => match result_name(expr, aggregate) {
            Some(l) if !l.is_empty() && !l.starts_with('$') => Some(format!("{l}_{name}")),
            _ => Some(name.clone()),
        },
        Expr::Index { expr, index } => {
            let right = match &**index {
                Expr::Literal(Literal::String(s)) => s.clone(),
                Expr::Literal(Literal::Long(v)) | Expr::Literal(Literal::Int(v)) => v.to_string(),
                _ => return None,
            };
            match result_name(expr, aggregate) {
                Some(l) if !l.is_empty() => Some(format!("{l}_{right}")),
                _ => Some(right),
            }
        }
        Expr::Call { name, args } => function_result_name(name, args.iter().map(|a| &a.expr).collect(), aggregate),
        _ => None,
    }
}

fn function_result_name(name: &str, args: Vec<&Expr>, aggregate: bool) -> Option<String> {
    let lower = name.to_ascii_lowercase();
    let info = if aggregate {
        catalog_data::lookup(&lower, FnKind::Aggregate).or_else(|| catalog_data::lookup(&lower, FnKind::Scalar))
    } else {
        catalog_data::lookup(&lower, FnKind::Scalar)
    }?;
    let mut kind = info.result_name;
    let mut prefix = info.prefix.map(str::to_string);
    match kind {
        ResultName::NameAndFirstArgument => {
            prefix = Some(info.name.to_string());
            kind = ResultName::PrefixAndFirstArgument;
        }
        ResultName::NameAndOnlyArgument => {
            prefix = Some(info.name.to_string());
            kind = ResultName::PrefixAndOnlyArgument;
        }
        _ => {}
    }
    let arg_name = |i: usize| args.get(i).map(|a| result_name(a, aggregate).unwrap_or_default());
    match kind {
        ResultName::PrefixAndFirstArgument => match (arg_name(0), prefix) {
            (Some(n), Some(p)) => Some(format!("{p}_{n}")),
            (Some(n), None) => Some(n),
            (None, Some(p)) => Some(format!("{p}_")),
            (None, None) => None,
        },
        ResultName::PrefixAndOnlyArgument if args.len() == 1 => match prefix {
            Some(p) => Some(format!("{p}_{}", arg_name(0).unwrap())),
            None => arg_name(0),
        },
        ResultName::FirstArgument => arg_name(0).filter(|n| !n.is_empty()),
        ResultName::OnlyArgument if args.len() == 1 => arg_name(0).filter(|n| !n.is_empty()),
        ResultName::PrefixOnly => prefix,
        ResultName::FirstArgumentValueIfColumn => match args.first() {
            Some(Expr::Literal(Literal::String(s))) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
    .filter(|n| !n.is_empty())
}

/// Assigns unique names the way Kusto's `UniqueNameTable` does: `x`, `x1`, `x2`, ...
#[derive(Default)]
pub(crate) struct UniqueNames {
    used: HashMap<String, usize>,
}

impl UniqueNames {
    pub fn add_existing(&mut self, name: &str) {
        self.used.entry(name.to_string()).or_insert(0);
    }

    pub fn contains(&self, name: &str) -> bool {
        self.used.contains_key(name)
    }

    pub fn unique(&mut self, name: &str) -> String {
        match self.used.get(name).copied() {
            None => {
                self.used.insert(name.to_string(), 0);
                name.to_string()
            }
            Some(last) => {
                let mut n = last + 1;
                loop {
                    let candidate = format!("{name}{n}");
                    if !self.used.contains_key(&candidate) {
                        self.used.insert(name.to_string(), n);
                        self.used.insert(candidate.clone(), 0);
                        return candidate;
                    }
                    n += 1;
                }
            }
        }
    }
}

/// Generates `Column1`, `Column2`, ... skipping names already in use.
#[derive(Default)]
pub(crate) struct DefaultNames {
    next: usize,
}

impl DefaultNames {
    pub fn next(&mut self, taken: impl Fn(&str) -> bool) -> String {
        loop {
            self.next += 1;
            let n = format!("Column{}", self.next);
            if !taken(&n) {
                return n;
            }
        }
    }
}
