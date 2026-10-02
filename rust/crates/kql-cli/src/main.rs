//! `kql2sql`: translate a KQL query to SQL.
//!
//! ```text
//! kql2sql [--dialect duckdb|postgres] [--table 'Name(col:type, ...)']... [--schema schema.json] [QUERY]
//! ```
//!
//! The query is read from the argument or, if absent, from stdin. Table schemas use KQL's
//! `(col:type, ...)` syntax; `--schema` takes JSON `{"Table": {"col": "type", ...}, ...}`.

use std::io::Read;
use std::process::ExitCode;

use kql_to_sql::{Catalog, Column, Dialect, KqlType};

const USAGE: &str = "usage: kql2sql [--dialect duckdb|postgres] [--table 'Name(col:type, ...)']... [--schema schema.json] [--columns] [QUERY]";

fn parse_table(spec: &str) -> Result<(String, Vec<Column>), String> {
    let (name, rest) = spec.split_once('(').ok_or_else(|| format!("bad --table '{spec}': expected Name(col:type, ...)"))?;
    let body = rest.trim_end().strip_suffix(')').ok_or_else(|| format!("bad --table '{spec}': missing ')'"))?;
    let mut cols = Vec::new();
    for part in body.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (c, t) = part.split_once(':').ok_or_else(|| format!("bad column '{part}': expected name:type"))?;
        let ty = KqlType::from_name(t.trim()).ok_or_else(|| format!("unknown type '{}'", t.trim()))?;
        cols.push(Column::new(c.trim(), ty));
    }
    Ok((name.trim().to_string(), cols))
}

fn parse_schema_json(text: &str) -> Result<Vec<(String, Vec<Column>)>, String> {
    let v: serde_json::Value = serde_json::from_str(text).map_err(|e| format!("schema: {e}"))?;
    let obj = v.as_object().ok_or("schema: expected an object of tables")?;
    let mut out = Vec::new();
    for (table, cols) in obj {
        let cols = cols.as_object().ok_or_else(|| format!("schema: table '{table}' must map column names to types"))?;
        let mut cs = Vec::new();
        for (c, t) in cols {
            let t = t.as_str().ok_or_else(|| format!("schema: type of '{table}.{c}' must be a string"))?;
            cs.push(Column::new(c.clone(), KqlType::from_name(t).ok_or_else(|| format!("unknown type '{t}'"))?));
        }
        out.push((table.clone(), cs));
    }
    Ok(out)
}

fn run() -> Result<(), String> {
    let mut dialect = Dialect::DuckDb;
    let mut catalog = Catalog::new();
    let mut query: Option<String> = None;
    let mut show_columns = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            "--dialect" => {
                dialect = match args.next().as_deref() {
                    Some("duckdb") => Dialect::DuckDb,
                    Some("postgres" | "pg" | "pglite") => Dialect::Postgres,
                    other => return Err(format!("unknown dialect {other:?}")),
                }
            }
            "--table" => {
                let (n, c) = parse_table(&args.next().ok_or("--table needs a value")?)?;
                catalog = catalog.with_table(n, c);
            }
            "--schema" => {
                let path = args.next().ok_or("--schema needs a file")?;
                let text = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
                for (n, c) in parse_schema_json(&text)? {
                    catalog = catalog.with_table(n, c);
                }
            }
            "--columns" => show_columns = true,
            _ if query.is_none() => query = Some(a),
            _ => return Err(USAGE.to_string()),
        }
    }
    let query = match query {
        Some(q) => q,
        None => {
            let mut s = String::new();
            std::io::stdin().read_to_string(&mut s).map_err(|e| e.to_string())?;
            s
        }
    };
    let t = kql_to_sql::translate(&query, &catalog, dialect).map_err(|e| e.message)?;
    println!("{}", t.sql);
    if show_columns {
        for c in &t.columns {
            eprintln!("{}: {}", c.name, c.ty);
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
