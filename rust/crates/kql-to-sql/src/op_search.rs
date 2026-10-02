//! `search` and `parse-kv`.

use kql_parser::ast::{ColumnDecl, Expr, OpParam};

use crate::binder::{Ctx, Env, Rel};
use crate::{err, Result};

/// `search` as an operator (`input` is Some) or as a source over tables.
pub(crate) fn search(_ctx: &mut Ctx, _input: Option<Rel>, _params: &[OpParam], _tables: &[Expr], _predicate: &Expr, _env: &Env) -> Result<Rel> {
    err("the 'search' operator is not supported yet")
}

pub(crate) fn parse_kv(_ctx: &mut Ctx, _rel: Rel, _expr: &Expr, _columns: &[ColumnDecl], _params: &[OpParam], _env: &Env) -> Result<Rel> {
    err("the 'parse-kv' operator is not supported yet")
}

