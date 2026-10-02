//! `make-series` and `scan`.

use kql_parser::ast::{Expr, MakeSeriesAgg, NamedExpr, OpParam, OrderKey, ScanStep};

use crate::binder::{Ctx, Env, Rel};
use crate::{err, Result};

#[allow(clippy::too_many_arguments)]
pub(crate) fn make_series(
    _ctx: &mut Ctx,
    _rel: Rel,
    _params: &[OpParam],
    _aggs: &[MakeSeriesAgg],
    _on: &Expr,
    _from: Option<&Expr>,
    _to: Option<&Expr>,
    _step: &Expr,
    _by: &[NamedExpr],
    _env: &Env,
) -> Result<Rel> {
    err("the 'make-series' operator is not supported yet")
}

pub(crate) fn scan(
    _ctx: &mut Ctx,
    _rel: Rel,
    _order_by: &[OrderKey],
    _partition_by: &[Expr],
    _declare: &[(String, String, Option<Expr>)],
    _steps: &[ScanStep],
    _env: &Env,
) -> Result<Rel> {
    err("the 'scan' operator is not supported yet")
}

