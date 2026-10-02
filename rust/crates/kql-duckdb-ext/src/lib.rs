//! DuckDB loadable extension `kql`.
//!
//! Functions:
//! * `kql_to_sql(kql)`                 — KQL → DuckDB SQL (scalar)
//! * `kql_to_sql_dialect(kql, dialect)` — KQL → SQL for `duckdb` | `postgres` | `pglite` (scalar)
//! * `kql_explain(kql [, dialect := ...])` — table: kql_input, sql_output, dialect, columns
//! * `kql(kql)`                         — table: translates the query and returns its rows
//!
//! Translation is type-directed, so every call builds a [`Catalog`] from the tables and views of
//! the database (via `duckdb_columns()`), read on an internal connection. See README.md for the
//! consequences (visibility of uncommitted tables, connection lifetime).

use std::error::Error;
use std::ffi::{c_char, CStr};
use std::sync::{Arc, Mutex, MutexGuard};

use duckdb::ffi;
use kql_to_sql::{Catalog, Column, Dialect, KqlType};

mod exec;
mod functions;

/// C API version requested from the host. With the `C_STRUCT_UNSTABLE` ABI (see build.sh) the
/// host ignores it and hands over its full function table; the extension must then be loaded by
/// exactly the DuckDB version it was built against.
const MIN_CAPI_VERSION: &str = "v1.2.0";

/// Number of internal connections reserved for streaming `kql()` scans. A scan that finds none
/// free falls back to a materialized execution on the shared connection.
const STREAM_POOL_SIZE: usize = 4;

/// A raw connection owned by the extension.
pub(crate) struct RawCon(pub(crate) ffi::duckdb_connection);

// SAFETY: a DuckDB connection may be used from any thread, one thread at a time (guarded by the
// Mutexes that hold every RawCon).
unsafe impl Send for RawCon {}

impl Drop for RawCon {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_disconnect(&mut self.0) };
    }
}

/// State shared by every registered function.
pub(crate) struct Shared {
    /// Connection used to read the catalog (`duckdb_columns()`).
    catalog: Mutex<duckdb::Connection>,
    /// Connection used to prepare `kql()` queries in bind and for materialized execution.
    pub(crate) exec: Mutex<RawCon>,
    /// Idle connections for streaming `kql()` scans.
    pub(crate) pool: Mutex<Vec<RawCon>>,
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

const CATALOG_SQL: &str = "SELECT table_name, column_name, data_type FROM duckdb_columns() \
     WHERE database_name = current_database() AND schema_name = current_schema() AND NOT internal \
     ORDER BY table_name, column_index";

impl Shared {
    /// Builds the translator catalog from the current tables and views.
    pub(crate) fn catalog(&self) -> Result<Catalog, String> {
        let con = lock(&self.catalog);
        let mut stmt = con.prepare_cached(CATALOG_SQL).map_err(|e| format!("kql: reading the catalog failed: {e}"))?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)))
            .map_err(|e| format!("kql: reading the catalog failed: {e}"))?;
        let mut tables: Vec<(String, Vec<Column>)> = Vec::new();
        for row in rows {
            let (table, column, ty) = row.map_err(|e| format!("kql: reading the catalog failed: {e}"))?;
            // Types without a Kusto counterpart (BLOB, TIME, BIT, ...) are treated as strings.
            let kty = KqlType::from_sql_type(&ty).unwrap_or(KqlType::String);
            match tables.last_mut() {
                Some((t, cols)) if *t == table => cols.push(Column::new(column, kty)),
                _ => tables.push((table, vec![Column::new(column, kty)])),
            }
        }
        Ok(tables.into_iter().fold(Catalog::new(), |c, (t, cols)| c.with_table(t, cols)))
    }
}

/// Parses a dialect name.
pub(crate) fn parse_dialect(name: &str) -> Result<Dialect, String> {
    match name.trim().to_ascii_lowercase().as_str() {
        "duckdb" => Ok(Dialect::DuckDb),
        "postgres" | "postgresql" | "pg" | "pglite" => Ok(Dialect::Postgres),
        other => Err(format!("kql: unknown dialect '{other}' (expected 'duckdb', 'postgres' or 'pglite')")),
    }
}

pub(crate) fn dialect_name(d: Dialect) -> &'static str {
    match d {
        Dialect::DuckDb => "duckdb",
        Dialect::Postgres => "postgres",
    }
}

/// `"name:type, ..."`.
pub(crate) fn describe_columns(cols: &[Column]) -> String {
    cols.iter().map(|c| format!("{}:{}", c.name, c.ty)).collect::<Vec<_>>().join(", ")
}

/// Takes ownership of a DuckDB-allocated C string.
pub(crate) unsafe fn take_duck_string(p: *mut c_char) -> Option<String> {
    if p.is_null() {
        return None;
    }
    let s = unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
    unsafe { ffi::duckdb_free(p.cast()) };
    Some(s)
}

unsafe fn connect(db: ffi::duckdb_database) -> Result<RawCon, Box<dyn Error>> {
    let mut con: ffi::duckdb_connection = std::ptr::null_mut();
    if unsafe { ffi::duckdb_connect(db, &mut con) } != ffi::DuckDBSuccess {
        return Err("kql: could not open an internal connection".into());
    }
    Ok(RawCon(con))
}

unsafe fn init(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn Error>> {
    unsafe {
        if !ffi::duckdb_rs_extension_api_init(info, access, MIN_CAPI_VERSION)? {
            return Ok(false);
        }
        let get_database = (*access).get_database.ok_or("kql: get_database is null")?;
        let db_ptr = get_database(info);
        if db_ptr.is_null() {
            return Ok(false);
        }
        // The duckdb_database handle is only valid during this call: every connection the
        // extension will ever need is opened now.
        let db: ffi::duckdb_database = *db_ptr;
        let registration = duckdb::Connection::open_from_raw(db.cast())?;
        let shared = Arc::new(Shared {
            catalog: Mutex::new(duckdb::Connection::open_from_raw(db.cast())?),
            exec: Mutex::new(connect(db)?),
            pool: Mutex::new((0..STREAM_POOL_SIZE).map(|_| connect(db)).collect::<Result<_, _>>()?),
        });
        functions::register(&registration, &shared)?;
        exec::register(&shared)?;
        Ok(true)
    }
}

/// Extension entry point (`<name>_init_c_api`), looked up by DuckDB on `LOAD kql`.
///
/// # Safety
/// Called by DuckDB with valid `info`/`access` pointers.
#[no_mangle]
pub unsafe extern "C" fn kql_init_c_api(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> bool {
    let result = std::panic::catch_unwind(|| unsafe { init(info, access) });
    let message = match result {
        Ok(Ok(ok)) => return ok,
        Ok(Err(e)) => e.to_string(),
        Err(_) => "kql: extension initialization panicked".to_string(),
    };
    unsafe {
        if let Some(set_error) = (*access).set_error {
            let msg = std::ffi::CString::new(message.replace('\0', " ")).unwrap_or_default();
            set_error(info, msg.as_ptr());
        }
    }
    false
}
