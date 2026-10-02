//! Additional scalar functions (dynamic/bag helpers, string utilities, ...).

use crate::binder::Ctx;
use crate::expr::{Scope, TExpr};
use crate::Result;

/// Scalar functions not handled by the core library. `name` is lowercase; `a` are compiled
/// arguments. Returns `None` if `name` is not handled here.
pub(crate) fn call(_ctx: &mut Ctx, _name: &str, _a: &[TExpr], _scope: &Scope) -> Option<Result<TExpr>> {
    None
}
