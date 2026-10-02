//! Operators with sub-pipelines or nested grouping: `mv-apply`, `partition`, `fork`, `facet`, `top-nested`, `top-hitters`, `sample-distinct`, `reduce`.

use kql_parser::ast::{Expr, MvExpandItem, OpParam, Operator, TopNestedLevel};

use crate::binder::{Ctx, Env, Rel};
use crate::{err, Result};

pub(crate) fn mv_apply(
    _ctx: &mut Ctx,
    _rel: Rel,
    _params: &[OpParam],
    _items: &[MvExpandItem],
    _limit: Option<&Expr>,
    _context_id: Option<&str>,
    _body: &[Operator],
    _env: &Env,
) -> Result<Rel> {
    err("the 'mv-apply' operator is not supported yet")
}

pub(crate) fn partition(_ctx: &mut Ctx, _rel: Rel, _params: &[OpParam], _by: &Expr, _body: &[Operator], _env: &Env) -> Result<Rel> {
    err("the 'partition' operator is not supported yet")
}

pub(crate) fn top_nested(_ctx: &mut Ctx, _rel: Rel, _levels: &[TopNestedLevel], _env: &Env) -> Result<Rel> {
    err("the 'top-nested' operator is not supported yet")
}

pub(crate) fn top_hitters(_ctx: &mut Ctx, _rel: Rel, _count: &Expr, _of: &Expr, _by: Option<&Expr>, _env: &Env) -> Result<Rel> {
    err("the 'top-hitters' operator is not supported yet")
}

pub(crate) fn sample_distinct(_ctx: &mut Ctx, _rel: Rel, _count: &Expr, _of: &Expr, _env: &Env) -> Result<Rel> {
    err("the 'sample-distinct' operator is not supported yet")
}

pub(crate) fn reduce(_ctx: &mut Ctx, _rel: Rel, _by: &Expr, _params: &[OpParam], _env: &Env) -> Result<Rel> {
    err("the 'reduce' operator is not supported yet")
}

pub(crate) fn fork(_ctx: &mut Ctx, _rel: Rel, _branches: &[(Option<String>, Vec<Operator>)], _env: &Env) -> Result<Rel> {
    err("the 'fork' operator is not supported yet")
}

pub(crate) fn facet(_ctx: &mut Ctx, _rel: Rel, _by: &[String], _with: Option<&[Operator]>, _env: &Env) -> Result<Rel> {
    err("the 'facet' operator is not supported yet")
}

