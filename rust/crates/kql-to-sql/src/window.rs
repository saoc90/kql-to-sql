//! Window functions over the serialized row order: `row_number`, `prev`, `next`, `row_cumsum`,
//! `row_rank_dense`, `row_rank_min`, `row_window_session`.

use crate::binder::Ctx;
use crate::expr::{Scope, TExpr};
use crate::{err, Result};

/// Window functions not handled by the core library. `name` is lowercase; `a` are compiled
/// arguments. Returns `None` if `name` is not a window function handled here.
pub(crate) fn call(_ctx: &mut Ctx, name: &str, _a: &[TExpr], _scope: &Scope) -> Option<Result<TExpr>> {
    match name {
        "row_rank_dense" | "row_rank_min" | "row_window_session" => Some(err(format!("function '{name}' is not supported yet"))),
        _ => None,
    }
}
