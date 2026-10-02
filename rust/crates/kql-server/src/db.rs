//! The DuckDB database: opening + hardening, the schema catalog, and bounded query execution.

use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use duckdb::types::Value;
use duckdb::{AccessMode, Config, Connection};
use kql_to_sql::{Catalog, Column, KqlType};
use serde_json::Value as Json;

use crate::error::ApiError;
use crate::values;

/// How the database is opened.
#[derive(Debug, Clone, Default)]
pub struct DbOptions {
    /// DuckDB file; `None` = in-memory.
    pub path: Option<std::path::PathBuf>,
    pub read_only: bool,
    /// `StormEvents.csv(.gz)` to load into table `StormEvents` at startup.
    pub storm_events_csv: Option<std::path::PathBuf>,
    pub memory_limit: Option<String>,
    pub threads: Option<u32>,
}

pub struct Database {
    /// Connection used only to create per-request connections and to read the schema.
    base: Mutex<Connection>,
    catalog: RwLock<Option<Arc<Catalog>>>,
}

/// One executed result set (bounded).
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Json>>,
    /// Set when the row or size limit cut the result short.
    pub truncated: Option<Truncation>,
}

#[derive(Debug, Clone)]
pub enum Truncation {
    Rows(usize),
    Bytes(usize),
}

impl Truncation {
    /// Kusto's message for a result set over the limits.
    pub fn message(&self) -> String {
        match self {
            Truncation::Rows(n) => format!(
                "Query result set has exceeded the internal record count limit {n} (E_QUERY_RESULT_SET_TOO_LARGE; see https://aka.ms/kustoquerylimits)"
            ),
            Truncation::Bytes(n) => format!(
                "Query result set has exceeded the internal data size limit {n} (E_QUERY_RESULT_SET_TOO_LARGE; see https://aka.ms/kustoquerylimits)"
            ),
        }
    }
}

/// Limits applied to one execution.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub timeout: Duration,
}

fn engine(e: duckdb::Error) -> ApiError {
    ApiError::engine(e.to_string())
}

/// The StormEvents schema as Azure Data Explorer's help cluster defines it.
pub fn storm_columns() -> Vec<Column> {
    use KqlType::*;
    [
        ("StartTime", DateTime),
        ("EndTime", DateTime),
        ("EpisodeId", Int),
        ("EventId", Int),
        ("State", String),
        ("EventType", String),
        ("InjuriesDirect", Int),
        ("InjuriesIndirect", Int),
        ("DeathsDirect", Int),
        ("DeathsIndirect", Int),
        ("DamageProperty", Int),
        ("DamageCrops", Int),
        ("Source", String),
        ("BeginLocation", String),
        ("EndLocation", String),
        ("BeginLat", Real),
        ("BeginLon", Real),
        ("EndLat", Real),
        ("EndLon", Real),
        ("EpisodeNarrative", String),
        ("EventNarrative", String),
        ("StormSummary", Dynamic),
    ]
    .iter()
    .map(|(n, t)| Column::new(*n, *t))
    .collect()
}

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Creates table StormEvents (Kusto schema) from the CSV shipped with the web demo.
pub fn load_storm(conn: &Connection, csv: &Path) -> Result<(), String> {
    let types: Vec<String> = storm_columns()
        .iter()
        .map(|c| {
            let t = match c.ty {
                KqlType::DateTime => "TIMESTAMP",
                KqlType::Int => "INTEGER",
                KqlType::Real => "DOUBLE",
                KqlType::Dynamic => "JSON",
                _ => "VARCHAR",
            };
            format!("{}: '{t}'", sql_str(&c.name))
        })
        .collect();
    let path = csv.to_str().ok_or("the StormEvents path is not valid UTF-8")?;
    let sql = format!(
        "CREATE OR REPLACE TABLE StormEvents AS SELECT * FROM read_csv({}, header = true, columns = {{{}}})",
        sql_str(path),
        types.join(", ")
    );
    conn.execute_batch(&sql).map_err(|e| e.to_string())
}

impl Database {
    pub fn open(opts: &DbOptions) -> Result<Database, String> {
        if opts.read_only && opts.storm_events_csv.is_some() {
            return Err("--load-storm-events cannot be combined with --read-only".into());
        }
        if opts.read_only && opts.path.is_none() {
            return Err("--read-only needs --db <file>".into());
        }
        let mut config = Config::default().enable_autoload_extension(false).map_err(|e| e.to_string())?;
        if opts.read_only {
            config = config.access_mode(AccessMode::ReadOnly).map_err(|e| e.to_string())?;
        }
        let conn = match &opts.path {
            Some(p) => Connection::open_with_flags(p, config),
            None => Connection::open_in_memory_with_flags(config),
        }
        .map_err(|e| e.to_string())?;
        if let Some(csv) = &opts.storm_events_csv {
            load_storm(&conn, csv)?;
        }
        let mut setup = Vec::new();
        if let Some(m) = &opts.memory_limit {
            setup.push(format!("SET memory_limit = {}", sql_str(m)));
        }
        if let Some(t) = opts.threads {
            setup.push(format!("SET threads = {t}"));
        }
        // Hardening: no file system / network access from SQL (read_csv, COPY, ATTACH, httpfs,
        // INSTALL, ...), no extension loading, and no way for a query to undo that.
        setup.push("SET autoinstall_known_extensions = false".into());
        setup.push("SET autoload_known_extensions = false".into());
        setup.push("SET enable_external_access = false".into());
        setup.push("SET lock_configuration = true".into());
        for s in setup {
            conn.execute_batch(&s).map_err(|e| format!("{s}: {e}"))?;
        }
        Ok(Database { base: Mutex::new(conn), catalog: RwLock::new(None) })
    }

    /// A new connection to the same database, for one request.
    pub fn connect(&self) -> Result<Connection, ApiError> {
        self.base.lock().unwrap_or_else(|p| p.into_inner()).try_clone().map_err(engine)
    }

    /// The current schema (cached; [`Database::invalidate`] after commands).
    pub fn catalog(&self) -> Result<Arc<Catalog>, ApiError> {
        if let Some(c) = self.catalog.read().unwrap_or_else(|p| p.into_inner()).as_ref() {
            return Ok(c.clone());
        }
        let conn = self.connect()?;
        let cat = Arc::new(build_catalog(&conn)?);
        *self.catalog.write().unwrap_or_else(|p| p.into_inner()) = Some(cat.clone());
        Ok(cat)
    }

    pub fn invalidate(&self) {
        *self.catalog.write().unwrap_or_else(|p| p.into_inner()) = None;
    }

    /// Base tables (not views) with their columns, in order.
    pub fn base_tables(&self) -> Result<Vec<(String, Vec<Column>)>, ApiError> {
        let conn = self.connect()?;
        let names = string_rows(
            &conn,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' ORDER BY table_name",
        )?;
        let cat = self.catalog()?;
        Ok(names
            .into_iter()
            .filter_map(|n| cat.table(&n).map(|(name, cols)| (name.to_string(), cols.clone())))
            .collect())
    }
}

fn string_rows(conn: &Connection, sql: &str) -> Result<Vec<String>, ApiError> {
    let mut stmt = conn.prepare(sql).map_err(engine)?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0)).map_err(engine)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(engine)
}

/// Reads every table's and view's schema from information_schema.
pub fn build_catalog(conn: &Connection) -> Result<Catalog, ApiError> {
    let mut stmt = conn
        .prepare("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = current_schema() ORDER BY table_name, ordinal_position")
        .map_err(engine)?;
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)))
        .map_err(engine)?;
    let mut tables: Vec<(String, Vec<Column>)> = Vec::new();
    for row in rows {
        let (t, c, ty) = row.map_err(engine)?;
        let ty = KqlType::from_sql_type(&ty).unwrap_or(KqlType::String);
        match tables.iter_mut().find(|(n, _)| *n == t) {
            Some((_, cols)) => cols.push(Column::new(c, ty)),
            None => tables.push((t, vec![Column::new(c, ty)])),
        }
    }
    Ok(tables.into_iter().fold(Catalog::new(), |cat, (t, cols)| cat.with_table(t, cols)))
}

/// Runs one SELECT with the given limits on `conn` (blocking). The timeout is enforced by the
/// caller through the connection's interrupt handle; `deadline` additionally stops row fetching.
pub fn query(
    conn: &Connection,
    sql: &str,
    columns: &[Column],
    limits: Limits,
    deadline: Instant,
) -> Result<ResultSet, ApiError> {
    let mut stmt = conn.prepare(sql).map_err(engine)?;
    let mut rows = stmt.query([]).map_err(engine)?;
    let n = rows.as_ref().map(|s| s.column_count()).unwrap_or(columns.len());
    // the translator's schema is authoritative; fall back to value types if it disagrees
    let typed = n == columns.len();
    let mut cols: Vec<Column> = if typed {
        columns.to_vec()
    } else {
        let names = rows.as_ref().map(|s| s.column_names()).unwrap_or_default();
        (0..n)
            .map(|i| Column::new(names.get(i).cloned().unwrap_or_else(|| format!("Column{}", i + 1)), KqlType::String))
            .collect()
    };
    let mut first = true;
    let mut out = Vec::new();
    let mut bytes = 0usize;
    let mut truncated = None;
    while let Some(row) = rows.next().map_err(engine)? {
        if Instant::now() > deadline {
            return Err(ApiError::timeout());
        }
        if out.len() >= limits.max_rows {
            truncated = Some(Truncation::Rows(limits.max_rows));
            break;
        }
        let mut vals = Vec::with_capacity(n);
        for (i, col) in cols.iter_mut().enumerate() {
            let v: Value = row.get(i).map_err(engine)?;
            if !typed && first {
                col.ty = values::type_of_value(&v);
            }
            let j = values::to_json(&v, col.ty);
            bytes += values::approx_size(&j) + 1;
            vals.push(j);
        }
        first = false;
        if bytes > limits.max_bytes {
            truncated = Some(Truncation::Bytes(limits.max_bytes));
            break;
        }
        out.push(vals);
    }
    Ok(ResultSet { columns: cols, rows: out, truncated })
}
