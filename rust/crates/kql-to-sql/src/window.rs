//! Window functions over the serialized row order: `row_number`, `prev`, `next`, `row_cumsum`,
//! `row_rank_dense`, `row_rank_min`, `row_window_session`.
//!
//! SQL does not allow nested window functions, but Kusto's window functions nest freely
//! (`row_cumsum(x, g != prev(g))`, `prev(row_number())`). Such inner window expressions are
//! *hoisted*: computed as hidden columns in wrapping subqueries below the operator's projection.
//! The operators that compile row expressions (`extend`/`serialize`, `project`, `where`) bracket
//! the compilation with [`begin`] / [`finish`]; [`hoist`] registers an expression and returns the
//! column that holds its value. A hoisted expression may reference earlier hoisted columns; its
//! level (subquery depth) is one more than the deepest column it references.

use crate::binder::{Ctx, Rel};
use crate::expr::{Const, Scope, TExpr};
use crate::sql::Item;
use crate::{err, Dialect, KqlType, Result};

/// A hidden column computed below an operator's projection.
#[derive(Debug, Clone)]
pub(crate) struct Hoist {
    pub alias: String,
    pub sql: String,
    pub level: usize,
}

/// Starts collecting hoisted window expressions; returns the enclosing collector to restore.
pub(crate) fn begin(ctx: &mut Ctx) -> Option<Vec<Hoist>> {
    ctx.window_hoists.replace(Vec::new())
}

/// Ends collection and adds the hoisted columns to `rel` (one wrapping subquery per level).
/// `rel`'s visible columns and logical order are unchanged.
pub(crate) fn finish(ctx: &mut Ctx, saved: Option<Vec<Hoist>>, rel: Rel) -> Rel {
    let hoists = std::mem::replace(&mut ctx.window_hoists, saved).unwrap_or_default();
    if hoists.is_empty() {
        return rel;
    }
    let max = hoists.iter().map(|h| h.level).max().unwrap_or(0);
    let mut r = rel.passthrough(ctx);
    let mut hidden: Vec<Item> = Vec::new();
    for level in 1..=max {
        let mut items = r.identity_items();
        items.extend(hidden.iter().cloned());
        for h in hoists.iter().filter(|h| h.level == level) {
            items.push(Item { sql: h.sql.clone(), alias: h.alias.clone() });
            hidden.push(Item { sql: h.alias.clone(), alias: h.alias.clone() });
        }
        r.sel.items = Some(items);
        r = r.wrap(ctx);
    }
    r
}

/// Registers `sql` as a hidden column and returns the column reference.
pub(crate) fn hoist(ctx: &mut Ctx, sql: String) -> Result<String> {
    let Some(hs) = ctx.window_hoists.as_ref() else {
        return err("nested window functions are only supported in 'extend', 'serialize', 'project' and 'where'");
    };
    if let Some(h) = hs.iter().find(|h| h.sql == sql) {
        return Ok(h.alias.clone());
    }
    let alias = format!("__kql{}_", ctx.alias());
    let hs = ctx.window_hoists.as_mut().expect("checked above");
    let level = hs.iter().filter(|h| sql.contains(&h.alias)).map(|h| h.level).max().unwrap_or(0) + 1;
    hs.push(Hoist { alias: alias.clone(), sql, level });
    Ok(alias)
}

/// The SQL of `t`, hoisted into a column if it contains a window function (so it can be the
/// argument of another window function).
fn unnest(ctx: &mut Ctx, t: &TExpr) -> Result<String> {
    if t.window {
        hoist(ctx, t.sql.clone())
    } else {
        Ok(t.sql.clone())
    }
}

fn order_by(scope: &Scope) -> String {
    if scope.order.is_empty() {
        String::new()
    } else {
        format!("ORDER BY {}", scope.order.join(", "))
    }
}

/// `OVER (...)` contents with an optional partition.
fn over(scope: &Scope, partition: Option<&str>) -> String {
    let o = order_by(scope);
    match partition {
        Some(p) if o.is_empty() => format!("PARTITION BY {p}"),
        Some(p) => format!("PARTITION BY {p} {o}"),
        None => o,
    }
}

/// `OVER (...)` contents of a running (cumulative) frame.
fn running(scope: &Scope, partition: Option<&str>) -> String {
    let o = over(scope, partition);
    if o.is_empty() {
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".into()
    } else {
        format!("{o} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
    }
}

/// A hidden running group id that increments at each row where `restart` is true, or `None`
/// when the restart predicate is the constant `false`.
fn restart_group(ctx: &mut Ctx, restart: &TExpr, scope: &Scope) -> Result<Option<String>> {
    if matches!(restart.konst, Some(Const::Bool(false))) {
        return Ok(None);
    }
    let r = ctx.to_bool(restart.clone());
    let rs = unnest(ctx, &r)?;
    let g = hoist(ctx, format!("SUM(CASE WHEN {rs} THEN 1 ELSE 0 END) OVER ({})", running(scope, None)))?;
    Ok(Some(g))
}

fn win(sql: String, ty: KqlType, parts: &[&TExpr]) -> TExpr {
    let mut t = TExpr::derived(sql, ty, parts);
    t.window = true;
    t
}

fn arity(name: &str, n: usize, min: usize, max: usize) -> Result<()> {
    if n < min || n > max {
        return err(format!(
            "{name}(): expected {} arguments, got {n}",
            if min == max { min.to_string() } else { format!("{min}..{max}") }
        ));
    }
    Ok(())
}

/// Window functions. `name` is lowercase; `a` are compiled arguments. Returns `None` if `name`
/// is not a window function.
pub(crate) fn call(ctx: &mut Ctx, name: &str, a: &[TExpr], scope: &Scope) -> Option<Result<TExpr>> {
    let r = match name {
        "row_number" => row_number(ctx, a, scope),
        "prev" | "next" => prev_next(ctx, name, a, scope),
        "row_cumsum" => row_cumsum(ctx, a, scope),
        "row_rank_dense" | "row_rank_min" => row_rank(ctx, name, a, scope),
        "row_window_session" => row_window_session(ctx, a, scope),
        _ => return None,
    };
    Some(r)
}

fn row_number(ctx: &mut Ctx, a: &[TExpr], scope: &Scope) -> Result<TExpr> {
    arity("row_number", a.len(), 0, 2)?;
    let parts: Vec<&TExpr> = a.iter().collect();
    let start = match a.first() {
        Some(s) => {
            let s = unnest(ctx, s)?;
            ctx.d.cast(&s, KqlType::Long)
        }
        None => "1".into(),
    };
    let group = match a.get(1) {
        Some(r) => restart_group(ctx, r, scope)?,
        None => None,
    };
    let rn = format!("ROW_NUMBER() OVER ({})", over(scope, group.as_deref()));
    let sql = if start == "1" { rn } else { format!("({rn} + {start} - 1)") };
    Ok(win(sql, KqlType::Long, &parts))
}

fn prev_next(ctx: &mut Ctx, name: &str, a: &[TExpr], scope: &Scope) -> Result<TExpr> {
    arity(name, a.len(), 1, 3)?;
    let parts: Vec<&TExpr> = a.iter().collect();
    // Kusto requires a constant offset and default value
    if a.get(1).is_some_and(|o| o.konst.is_none()) {
        return err(format!("{name}(): the offset must be a constant"));
    }
    if a.get(2).is_some_and(|d| d.konst.is_none()) {
        return err(format!("{name}(): the default value must be a constant"));
    }
    let f = if name == "prev" { "LAG" } else { "LEAD" };
    let x = &a[0];
    let xs = unnest(ctx, x)?;
    let off = match a.get(1) {
        Some(o) => ctx.d.cast(&o.sql, KqlType::Long),
        None => "1".into(),
    };
    let def = match a.get(2) {
        Some(dv) => ctx.convert(dv.clone(), x.ty).sql,
        None if x.ty == KqlType::String => "''".into(),
        None => "NULL".into(),
    };
    let int = if ctx.d.kind() == Dialect::Postgres { "integer" } else { "INTEGER" };
    Ok(win(format!("{f}({xs}, CAST({off} AS {int}), {def}) OVER ({})", order_by(scope)), x.ty, &parts))
}

fn row_cumsum(ctx: &mut Ctx, a: &[TExpr], scope: &Scope) -> Result<TExpr> {
    arity("row_cumsum", a.len(), 1, 2)?;
    let parts: Vec<&TExpr> = a.iter().collect();
    let x = &a[0];
    let x = if x.ty == KqlType::Dynamic {
        TExpr::derived(ctx.d.try_cast(&ctx.d.json_to_text(&x.sql), KqlType::Real), KqlType::Real, &[x])
    } else {
        x.clone()
    };
    if !(x.ty.is_numeric() || x.ty == KqlType::TimeSpan) {
        return err(format!("row_cumsum(): expected a numeric argument, got {}", x.ty));
    }
    let xs = unnest(ctx, &x)?;
    let group = match a.get(1) {
        Some(r) => restart_group(ctx, r, scope)?,
        None => None,
    };
    let ty = match x.ty {
        KqlType::Int | KqlType::Bool => KqlType::Long,
        t => t,
    };
    let sum = format!("SUM({xs}) OVER ({})", running(scope, group.as_deref()));
    Ok(win(ctx.d.cast(&sum, ty), ty, &parts))
}

/// `row_rank_dense(x [, restart])`: 1 + the number of value changes since the group start;
/// `row_rank_min(x [, restart])`: the row number (within the group) of the first row of the
/// current run of equal values.
fn row_rank(ctx: &mut Ctx, name: &str, a: &[TExpr], scope: &Scope) -> Result<TExpr> {
    arity(name, a.len(), 1, 2)?;
    let parts: Vec<&TExpr> = a.iter().collect();
    let xs = unnest(ctx, &a[0])?;
    let restart = match a.get(1) {
        Some(r) if !matches!(r.konst, Some(Const::Bool(false))) => {
            let r = ctx.to_bool(r.clone());
            Some(unnest(ctx, &r)?)
        }
        _ => None,
    };
    let o = order_by(scope);
    // a new run starts at the first row, at a restart and where the value changes
    let mut starts =
        vec![format!("ROW_NUMBER() OVER ({o}) = 1"), format!("{xs} IS DISTINCT FROM LAG({xs}) OVER ({o})")];
    if let Some(r) = &restart {
        starts.push(format!("COALESCE({r}, false)"));
    }
    let run_start = hoist(ctx, format!("CASE WHEN {} THEN 1 ELSE 0 END", starts.join(" OR ")))?;
    let run = hoist(ctx, format!("SUM({run_start}) OVER ({})", running(scope, None)))?;
    let group = match &restart {
        Some(r) => Some(hoist(
            ctx,
            format!("SUM(CASE WHEN COALESCE({r}, false) THEN 1 ELSE 0 END) OVER ({})", running(scope, None)),
        )?),
        None => None,
    };
    let sql = if name == "row_rank_dense" {
        match &group {
            Some(g) => format!("({run} - MIN({run}) OVER (PARTITION BY {g}) + 1)"),
            None => run,
        }
    } else {
        let rn = hoist(ctx, format!("ROW_NUMBER() OVER ({o})"))?;
        match &group {
            Some(g) => format!("(MIN({rn}) OVER (PARTITION BY {run}) - MIN({rn}) OVER (PARTITION BY {g}) + 1)"),
            None => format!("MIN({rn}) OVER (PARTITION BY {run})"),
        }
    };
    Ok(win(ctx.d.cast(&sql, KqlType::Long), KqlType::Long, &parts))
}

/// `row_window_session(x, max_from_first, max_between_neighbors [, restart])`: the value of the
/// first row of the session the row belongs to. A session restarts when the value is more than
/// `max_from_first` after the session start, more than `max_between_neighbors` after the
/// previous row, or `restart` is true. The session start depends on earlier session starts,
/// so it is computed by folding the ordered prefix of values (DuckDB `list_reduce`).
fn row_window_session(ctx: &mut Ctx, a: &[TExpr], scope: &Scope) -> Result<TExpr> {
    arity("row_window_session", a.len(), 3, 4)?;
    if ctx.d.kind() != Dialect::DuckDb {
        return err("row_window_session() is only supported for DuckDB");
    }
    let parts: Vec<&TExpr> = a.iter().collect();
    let x = &a[0];
    // values and distances on a common numeric scale: datetime → ticks
    let (v, d1, d2) = match x.ty {
        KqlType::DateTime => {
            if a[1].ty != KqlType::TimeSpan || a[2].ty != KqlType::TimeSpan {
                return err("row_window_session(): the distances must be timespans for a datetime value");
            }
            (
                format!("(CAST({} AS DOUBLE) * 10)", ctx.d.epoch_us(&x.sql)),
                ctx.d.cast(&a[1].sql, KqlType::Real),
                ctx.d.cast(&a[2].sql, KqlType::Real),
            )
        }
        t if t.is_numeric() || t == KqlType::TimeSpan => (
            ctx.d.cast(&x.sql, KqlType::Real),
            ctx.d.cast(&a[1].sql, KqlType::Real),
            ctx.d.cast(&a[2].sql, KqlType::Real),
        ),
        t => return err(format!("row_window_session(): unsupported value type {t}")),
    };
    let vt = TExpr::derived(v, KqlType::Real, &[x]);
    let vs = unnest(ctx, &vt)?;
    let rs = match a.get(3) {
        Some(r) => {
            let r = ctx.to_bool(r.clone());
            format!("COALESCE({}, false)", unnest(ctx, &r)?)
        }
        None => "false".into(),
    };
    let d1 = unnest(ctx, &TExpr::derived(d1, KqlType::Real, &[&a[1]]))?;
    let d2 = unnest(ctx, &TExpr::derived(d2, KqlType::Real, &[&a[2]]))?;
    let prefix = format!("list({{'v': {vs}, 's': {vs}, 'r': {rs}}}) OVER ({})", running(scope, None));
    let fold = format!(
        "list_reduce({prefix}, (acc, e) -> CASE WHEN e.v IS NULL THEN {{'v': acc.v, 's': acc.s, 'r': false}} \
         WHEN acc.s IS NULL OR e.r OR e.v - acc.s > {d1} OR e.v - acc.v > {d2} THEN {{'v': e.v, 's': e.v, 'r': false}} \
         ELSE {{'v': e.v, 's': acc.s, 'r': false}} END).s"
    );
    let start = format!("CASE WHEN {vs} IS NULL THEN NULL ELSE {fold} END");
    let sql = match x.ty {
        KqlType::DateTime => ctx.d.ts_from_us(&format!("CAST(round(({start}) / 10) AS BIGINT)")),
        KqlType::Real | KqlType::Decimal => start,
        t => ctx.d.cast(&format!("round({start})"), t),
    };
    Ok(win(sql, x.ty, &parts))
}
