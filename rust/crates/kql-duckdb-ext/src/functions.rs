//! `kql_to_sql`, `kql_to_sql_dialect` (scalar) and `kql_explain` (table), on duckdb-rs' traits.

use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::ffi::duckdb_string_t;
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::Connection;
use kql_to_sql::{translate, Catalog, Dialect};

use crate::{describe_columns, dialect_name, parse_dialect, Shared};

type BoxError = Box<dyn Error>;

pub(crate) fn register(con: &Connection, shared: &Arc<Shared>) -> Result<(), BoxError> {
    con.register_scalar_function_with_state::<KqlToSql>("kql_to_sql", shared)?;
    con.register_scalar_function_with_state::<KqlToSqlDialect>("kql_to_sql_dialect", shared)?;
    con.register_table_function_with_extra_info::<KqlExplain, Arc<Shared>>("kql_explain", shared)?;
    Ok(())
}

fn varchar() -> LogicalTypeHandle {
    LogicalTypeHandle::from(LogicalTypeId::Varchar)
}

/// Reads row `i` of a flat VARCHAR vector (None for NULL).
fn read_str(strings: &[duckdb_string_t], vector: &duckdb::core::FlatVector, i: usize) -> Option<String> {
    if vector.row_is_null(i as u64) {
        return None;
    }
    let mut s = strings[i];
    Some(DuckString::new(&mut s).as_str().into_owned())
}

/// Shared body of the two scalar functions: `dialect_col` is the input column holding the
/// dialect name, if any.
fn translate_column(
    shared: &Shared,
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
    dialect_col: Option<usize>,
) -> Result<(), BoxError> {
    let n = input.len();
    let kql_vec = input.flat_vector(0);
    let kql = unsafe { kql_vec.as_slice_with_len::<duckdb_string_t>(n) };
    let dialect_vec = dialect_col.map(|c| input.flat_vector(c));
    let dialects = dialect_vec.as_ref().map(|v| unsafe { v.as_slice_with_len::<duckdb_string_t>(n) });
    let mut out = output.flat_vector();
    // the catalog is read once per chunk, and only when a row needs it
    let mut catalog: Option<Catalog> = None;
    for i in 0..n {
        let Some(query) = read_str(kql, &kql_vec, i) else {
            out.set_null(i);
            continue;
        };
        let dialect = match (&dialect_vec, dialects) {
            (Some(v), Some(d)) => match read_str(d, v, i) {
                Some(name) => parse_dialect(&name)?,
                None => {
                    out.set_null(i);
                    continue;
                }
            },
            _ => Dialect::DuckDb,
        };
        if catalog.is_none() {
            catalog = Some(shared.catalog()?);
        }
        let t = translate(&query, catalog.as_ref().unwrap(), dialect).map_err(|e| e.message)?;
        out.insert(i, t.sql.as_str());
    }
    Ok(())
}

struct KqlToSql;

impl VScalar for KqlToSql {
    type State = Arc<Shared>;

    fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), BoxError> {
        translate_column(state, input, output, None)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(vec![varchar()], varchar())]
    }

    // the result depends on the catalog, which can change between executions
    fn volatile() -> bool {
        true
    }
}

struct KqlToSqlDialect;

impl VScalar for KqlToSqlDialect {
    type State = Arc<Shared>;

    fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), BoxError> {
        translate_column(state, input, output, Some(1))
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(vec![varchar(), varchar()], varchar())]
    }

    fn volatile() -> bool {
        true
    }
}

struct ExplainBind {
    row: [String; 4],
}

struct ExplainInit {
    done: AtomicBool,
}

struct KqlExplain;

impl VTab for KqlExplain {
    type BindData = ExplainBind;
    type InitData = ExplainInit;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, BoxError> {
        for name in ["kql_input", "sql_output", "dialect", "columns"] {
            bind.add_result_column(name, varchar());
        }
        let shared = unsafe { &*bind.get_extra_info::<Arc<Shared>>() };
        let param = bind.get_parameter(0);
        if param.is_null() {
            return Err("kql_explain: the query must not be NULL".into());
        }
        let kql = param.to_string();
        let dialect = match bind.get_named_parameter("dialect") {
            Some(v) if !v.is_null() => parse_dialect(&v.to_string())?,
            _ => Dialect::DuckDb,
        };
        let t = translate(&kql, &shared.catalog()?, dialect).map_err(|e| e.message)?;
        bind.set_cardinality(1, true);
        Ok(ExplainBind { row: [kql, t.sql, dialect_name(dialect).to_string(), describe_columns(&t.columns)] })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, BoxError> {
        Ok(ExplainInit { done: AtomicBool::new(false) })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), BoxError> {
        if func.get_init_data().done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        for (i, value) in func.get_bind_data().row.iter().enumerate() {
            output.flat_vector(i).insert(0, value.as_str());
        }
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![varchar()])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![("dialect".to_string(), varchar())])
    }
}
