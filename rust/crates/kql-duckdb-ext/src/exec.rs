//! `kql(query)`: a table function that translates the query and returns its rows.
//!
//! * **bind** translates (DuckDB dialect), prepares the SQL on the shared exec connection and
//!   declares the output columns. Each column gets the DuckDB type of its Kusto type
//!   (bool→BOOLEAN, int→INTEGER, long→BIGINT, real/decimal→DOUBLE, string→VARCHAR,
//!   datetime→TIMESTAMP, timespan→INTERVAL, guid→UUID); a column whose SQL type differs is cast
//!   in a wrapping SELECT. Dynamic columns keep the type the SQL produces (normally JSON).
//! * **init** executes the SQL — streaming on a pooled connection, or materialized on the shared
//!   connection when the pool is exhausted.
//! * **func** fetches one result chunk per call and hands its vectors over by reference
//!   (`duckdb_vector_reference_vector`, no copy).
//!
//! Written against the raw C API: duckdb-rs' `VTab` cannot declare an arbitrary
//! `duckdb_logical_type` (e.g. JSON) nor expose a connection's raw handle.

use std::ffi::{c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

use duckdb::ffi;
use kql_to_sql::{translate, Dialect, KqlType};

use crate::{lock, take_duck_string, RawCon, Shared};

type Res<T> = Result<T, String>;

pub(crate) fn register(shared: &Arc<Shared>) -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        let mut tf = ffi::duckdb_create_table_function();
        let name = CString::new("kql").unwrap();
        ffi::duckdb_table_function_set_name(tf, name.as_ptr());
        let mut varchar = ffi::duckdb_create_logical_type(ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
        ffi::duckdb_table_function_add_parameter(tf, varchar);
        ffi::duckdb_destroy_logical_type(&mut varchar);
        let extra = Box::into_raw(Box::new(shared.clone()));
        ffi::duckdb_table_function_set_extra_info(tf, extra.cast(), Some(drop_box::<Arc<Shared>>));
        ffi::duckdb_table_function_set_bind(tf, Some(kql_bind));
        ffi::duckdb_table_function_set_init(tf, Some(kql_init));
        ffi::duckdb_table_function_set_function(tf, Some(kql_func));
        let state = {
            let con = lock(&shared.exec);
            ffi::duckdb_register_table_function(con.0, tf)
        };
        ffi::duckdb_destroy_table_function(&mut tf);
        if state != ffi::DuckDBSuccess {
            return Err("kql: registering the kql() table function failed".into());
        }
    }
    Ok(())
}

unsafe extern "C" fn drop_box<T>(p: *mut c_void) {
    if !p.is_null() {
        drop(unsafe { Box::from_raw(p.cast::<T>()) });
    }
}

fn c_message(msg: &str) -> CString {
    CString::new(msg.replace('\0', " ")).unwrap_or_default()
}

/// Runs a callback body, turning errors and panics into a message.
fn guarded(f: impl FnOnce() -> Res<()>) -> Option<String> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e),
        Err(_) => Some("kql: internal error (panic)".to_string()),
    }
}

// ---------------------------------------------------------------------------------------------
// owned C API handles

struct Prepared(ffi::duckdb_prepared_statement);

impl Prepared {
    /// Prepares `sql` on `con`.
    unsafe fn new(con: ffi::duckdb_connection, sql: &str) -> Res<Prepared> {
        let c = CString::new(sql).map_err(|_| "kql: the generated SQL contains a NUL byte".to_string())?;
        let mut p = Prepared(ptr::null_mut());
        if unsafe { ffi::duckdb_prepare(con, c.as_ptr(), &mut p.0) } != ffi::DuckDBSuccess {
            let err = unsafe { ffi::duckdb_prepare_error(p.0) };
            let msg = if err.is_null() { "unknown error".into() } else { unsafe { CStr::from_ptr(err) }.to_string_lossy().into_owned() };
            return Err(format!("kql: the generated SQL failed to prepare: {msg}\nSQL: {sql}"));
        }
        Ok(p)
    }

    fn column_count(&self) -> usize {
        unsafe { ffi::duckdb_prepared_statement_column_count(self.0) as usize }
    }

    fn column_name(&self, i: usize) -> String {
        unsafe { take_duck_string(ffi::duckdb_prepared_statement_column_name(self.0, i as u64) as *mut _) }.unwrap_or_default()
    }

    fn column_type(&self, i: usize) -> LogicalType {
        LogicalType(unsafe { ffi::duckdb_prepared_statement_column_logical_type(self.0, i as u64) })
    }
}

impl Drop for Prepared {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_prepare(&mut self.0) };
    }
}

struct LogicalType(ffi::duckdb_logical_type);

impl LogicalType {
    fn id(&self) -> ffi::duckdb_type {
        unsafe { ffi::duckdb_get_type_id(self.0) }
    }
}

impl Drop for LogicalType {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_logical_type(&mut self.0) };
    }
}

/// A query result (streaming or materialized), consumed with `duckdb_fetch_chunk`.
struct QueryResult(Box<ffi::duckdb_result>);

impl QueryResult {
    fn error(&self) -> Option<String> {
        let p = unsafe { ffi::duckdb_result_error(&*self.0 as *const _ as *mut _) };
        if p.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
        }
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_result(&mut *self.0) };
    }
}

fn empty_result() -> Box<ffi::duckdb_result> {
    // SAFETY: duckdb_result is a plain C struct; all-zero is its "empty" state.
    Box::new(unsafe { std::mem::zeroed() })
}

// ---------------------------------------------------------------------------------------------
// bind

struct BindData {
    sql: String,
}

/// DuckDB type for a Kusto output column (None: keep the SQL type).
fn target_type(ty: KqlType) -> Option<(ffi::duckdb_type, &'static str)> {
    Some(match ty {
        KqlType::Bool => (ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN, "BOOLEAN"),
        KqlType::Int => (ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, "INTEGER"),
        KqlType::Long => (ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, "BIGINT"),
        KqlType::Real | KqlType::Decimal => (ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, "DOUBLE"),
        KqlType::String => (ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR, "VARCHAR"),
        KqlType::DateTime => (ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP, "TIMESTAMP"),
        KqlType::TimeSpan => (ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL, "INTERVAL"),
        KqlType::Guid => (ffi::DUCKDB_TYPE_DUCKDB_TYPE_UUID, "UUID"),
        KqlType::Dynamic => return None,
    })
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

unsafe fn varchar_parameter(info: ffi::duckdb_bind_info, index: u64) -> Res<String> {
    unsafe {
        let mut v = ffi::duckdb_bind_get_parameter(info, index);
        let s = if v.is_null() || ffi::duckdb_is_null_value(v) { None } else { take_duck_string(ffi::duckdb_get_varchar(v)) };
        if !v.is_null() {
            ffi::duckdb_destroy_value(&mut v);
        }
        s.ok_or_else(|| "kql: the query must not be NULL".to_string())
    }
}

unsafe fn bind_impl(info: ffi::duckdb_bind_info) -> Res<BindData> {
    unsafe {
        let shared = &*(ffi::duckdb_bind_get_extra_info(info) as *const Arc<Shared>);
        let kql = varchar_parameter(info, 0)?;
        let t = translate(&kql, &shared.catalog()?, Dialect::DuckDb).map_err(|e| e.message)?;
        let con = lock(&shared.exec);
        let prepared = Prepared::new(con.0, &t.sql)?;
        let n = prepared.column_count();
        let names: Vec<String> =
            (0..n).map(|i| t.columns.get(i).filter(|_| t.columns.len() == n).map(|c| c.name.clone()).unwrap_or_else(|| prepared.column_name(i))).collect();
        let mut cast = false;
        let mut items = Vec::with_capacity(n);
        for i in 0..n {
            let actual = prepared.column_type(i).id();
            let q = format!("__kql.{}", quote_ident(&prepared.column_name(i)));
            let target = if t.columns.len() == n { target_type(t.columns[i].ty) } else { None };
            match target {
                Some((id, sql_ty)) if id != actual => {
                    cast = true;
                    items.push(format!("CAST({q} AS {sql_ty}) AS {}", quote_ident(&names[i])));
                }
                _ => items.push(format!("{q} AS {}", quote_ident(&names[i]))),
            }
        }
        let (sql, prepared) = if cast {
            // the projection over the ordered subquery keeps its row order
            let sql = format!("SELECT {} FROM ({}) AS __kql", items.join(", "), t.sql);
            let p = Prepared::new(con.0, &sql)?;
            (sql, p)
        } else {
            (t.sql, prepared)
        };
        for (i, name) in names.iter().enumerate() {
            let ty = prepared.column_type(i);
            let cname = c_message(name);
            ffi::duckdb_bind_add_result_column(info, cname.as_ptr(), ty.0);
        }
        Ok(BindData { sql })
    }
}

unsafe extern "C" fn kql_bind(info: ffi::duckdb_bind_info) {
    let mut data = None;
    let err = guarded(|| {
        data = Some(unsafe { bind_impl(info) }?);
        Ok(())
    });
    unsafe {
        match (err, data) {
            (None, Some(d)) => ffi::duckdb_bind_set_bind_data(info, Box::into_raw(Box::new(d)).cast(), Some(drop_box::<BindData>)),
            (e, _) => ffi::duckdb_bind_set_error(info, c_message(&e.unwrap_or_default()).as_ptr()),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// init / func

struct InitData {
    shared: Arc<Shared>,
    /// Pooled connection running a streaming result; returned to the pool on drop.
    con: Option<RawCon>,
    result: Option<QueryResult>,
    prepared: Option<Prepared>,
}

impl Drop for InitData {
    fn drop(&mut self) {
        self.result.take();
        self.prepared.take();
        if let Some(con) = self.con.take() {
            lock(&self.shared.pool).push(con);
        }
    }
}

unsafe fn init_impl(info: ffi::duckdb_init_info) -> Res<InitData> {
    unsafe {
        let shared = (*(ffi::duckdb_init_get_extra_info(info) as *const Arc<Shared>)).clone();
        let bind = &*(ffi::duckdb_init_get_bind_data(info) as *const BindData);
        let pooled = lock(&shared.pool).pop();
        let mut data = InitData { shared: shared.clone(), con: None, result: None, prepared: None };
        let mut result = QueryResult(empty_result());
        let ok = match pooled {
            Some(con) => {
                // streaming: chunks are produced as kql() is scanned
                let prepared = Prepared::new(con.0, &bind.sql);
                data.con = Some(con);
                let prepared = prepared?;
                let ok = ffi::duckdb_execute_prepared_streaming(prepared.0, &mut *result.0) == ffi::DuckDBSuccess;
                data.prepared = Some(prepared);
                ok
            }
            None => {
                // no free streaming connection: materialize on the shared connection
                let con = lock(&shared.exec);
                let sql = CString::new(bind.sql.as_str()).map_err(|_| "kql: the generated SQL contains a NUL byte".to_string())?;
                ffi::duckdb_query(con.0, sql.as_ptr(), &mut *result.0) == ffi::DuckDBSuccess
            }
        };
        if !ok {
            return Err(format!("kql: executing the generated SQL failed: {}", result.error().unwrap_or_default()));
        }
        data.result = Some(result);
        Ok(data)
    }
}

unsafe extern "C" fn kql_init(info: ffi::duckdb_init_info) {
    let mut data = None;
    let err = guarded(|| {
        data = Some(unsafe { init_impl(info) }?);
        Ok(())
    });
    unsafe {
        match (err, data) {
            (None, Some(d)) => {
                ffi::duckdb_init_set_max_threads(info, 1);
                ffi::duckdb_init_set_init_data(info, Box::into_raw(Box::new(d)).cast(), Some(drop_box::<InitData>));
            }
            (e, _) => ffi::duckdb_init_set_error(info, c_message(&e.unwrap_or_default()).as_ptr()),
        }
    }
}

unsafe fn func_impl(info: ffi::duckdb_function_info, output: ffi::duckdb_data_chunk) -> Res<()> {
    unsafe {
        let data = &mut *(ffi::duckdb_function_get_init_data(info) as *mut InitData);
        let Some(result) = data.result.as_mut() else {
            ffi::duckdb_data_chunk_set_size(output, 0);
            return Ok(());
        };
        loop {
            let mut chunk = ffi::duckdb_fetch_chunk(*result.0);
            if chunk.is_null() {
                if let Some(e) = result.error() {
                    return Err(format!("kql: {e}"));
                }
                // exhausted: release the result (and the pooled connection) right away
                drop(data.result.take());
                drop(data.prepared.take());
                if let Some(con) = data.con.take() {
                    lock(&data.shared.pool).push(con);
                }
                ffi::duckdb_data_chunk_set_size(output, 0);
                return Ok(());
            }
            let size = ffi::duckdb_data_chunk_get_size(chunk);
            if size > 0 {
                let columns = ffi::duckdb_data_chunk_get_column_count(output);
                for c in 0..columns {
                    ffi::duckdb_vector_reference_vector(ffi::duckdb_data_chunk_get_vector(output, c), ffi::duckdb_data_chunk_get_vector(chunk, c));
                }
                ffi::duckdb_data_chunk_set_size(output, size);
            }
            ffi::duckdb_destroy_data_chunk(&mut chunk);
            if size > 0 {
                return Ok(());
            }
        }
    }
}

unsafe extern "C" fn kql_func(info: ffi::duckdb_function_info, output: ffi::duckdb_data_chunk) {
    if let Some(e) = guarded(|| unsafe { func_impl(info, output) }) {
        unsafe { ffi::duckdb_function_set_error(info, c_message(&e).as_ptr()) };
    }
}
