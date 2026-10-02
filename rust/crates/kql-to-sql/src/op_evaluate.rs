//! `evaluate` plugins (`bag_unpack`, `pivot`, `narrow`, ...), `invoke` and `externaldata`.

use kql_parser::ast::{Arg, ColumnDecl, Expr, OpParam};

use crate::binder::{Ctx, Env, Rel};
use crate::{err, Result};

pub(crate) fn evaluate(_ctx: &mut Ctx, _rel: Option<Rel>, _params: &[OpParam], name: &str, _args: &[Arg], _env: &Env) -> Result<Rel> {
    err(format!("the '{name}' plugin is not supported yet"))
}

pub(crate) fn externaldata(_ctx: &mut Ctx, _columns: &[ColumnDecl], _uris: &[Expr], _props: &[(String, Expr)], _env: &Env) -> Result<Rel> {
    err("the 'externaldata' operator is not supported yet")
}

