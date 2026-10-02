//! `evaluate` plugins (`bag_unpack`, `pivot`, `narrow`) and `externaldata`.
//!
//! Translation is type-directed, so the output schema must be known at translation time.
//! `bag_unpack` and `pivot` produce columns from the *data* (bag keys, distinct pivot values);
//! they are supported when those values are compile-time constants, traced back through the
//! pipeline to a `datatable`, `print` or constant `extend`/`project` (see [`column_values`]).
//! The parser does not accept an explicit output schema (`evaluate bag_unpack(d) : (a:long)`).

use std::collections::BTreeMap;

use kql_parser::ast::{Arg, BinaryOp, ColumnDecl, Expr, NamedExpr, OpParam, Operator};

use crate::binder::{Ctx, Env, Rel};
use crate::expr::{col_ref, Const, Scope, TExpr};
use crate::sql::{quote_ident, quote_str, From, Item, Select};
use crate::{err, Column, Dialect, KqlType, Result};

/// `T | evaluate plugin(...)` where the input expression `input` is available, so plugins whose
/// output schema depends on constant data can inspect it.
pub(crate) fn evaluate_with_input(ctx: &mut Ctx, rel: Rel, input: &Expr, params: &[OpParam], name: &str, args: &[Arg], env: &Env) -> Result<Rel> {
    match name.to_ascii_lowercase().as_str() {
        "bag_unpack" => bag_unpack(ctx, rel, Some(input), args, env),
        "pivot" => pivot(ctx, rel, Some(input), args, env),
        _ => evaluate(ctx, Some(rel), params, name, args, env),
    }
}

pub(crate) fn evaluate(ctx: &mut Ctx, rel: Option<Rel>, _params: &[OpParam], name: &str, args: &[Arg], env: &Env) -> Result<Rel> {
    let lname = name.to_ascii_lowercase();
    let Some(rel) = rel else {
        return err(format!("the '{name}' plugin is not supported as a query source"));
    };
    match lname.as_str() {
        "bag_unpack" => bag_unpack(ctx, rel, None, args, env),
        "pivot" => pivot(ctx, rel, None, args, env),
        "narrow" => narrow(ctx, rel, args),
        _ => err(format!("the '{name}' plugin is not supported")),
    }
}

// ---------------------------------------------------------------------- constant tracing

/// The expressions that produce column `col` in the rows of `input`, when they can be found
/// syntactically (a `datatable`/`print` source or an `extend`/`project` assignment, through
/// operators that keep the column). Operators that drop rows (`where`, `take`) are looked
/// through, so the result may describe a superset of the rows.
fn column_values(input: &Expr, col: &str) -> Option<Vec<Expr>> {
    match input {
        Expr::Paren(x) => column_values(x, col),
        Expr::Source(op) => match &**op {
            Operator::DataTable { columns, values } => {
                let i = columns.iter().position(|c| c.name == col)?;
                if columns.is_empty() {
                    return None;
                }
                Some(values.chunks(columns.len()).filter_map(|r| r.get(i).cloned()).collect())
            }
            Operator::Print(items) => items.iter().enumerate().find(|(i, ne)| ne.name().map(str::to_string).unwrap_or_else(|| format!("print_{i}")) == col).map(|(_, ne)| vec![ne.expr.clone()]),
            _ => None,
        },
        Expr::Pipe { input, op } => match &**op {
            Operator::Extend(items) | Operator::Serialize(items) => match items.iter().rev().find(|ne| ne.names.iter().any(|n| n == col)) {
                Some(ne) => Some(vec![ne.expr.clone()]),
                None => column_values(input, col),
            },
            Operator::Project(items) => {
                let ne = items.iter().find(|ne| ne.name() == Some(col) || (ne.names.is_empty() && matches!(&ne.expr, Expr::Name(n) if n == col)))?;
                match &ne.expr {
                    Expr::Name(src) => column_values(input, src),
                    e => Some(vec![e.clone()]),
                }
            }
            Operator::Sort(_) | Operator::Where(_) | Operator::Take(_) | Operator::ProjectAway(_) | Operator::ProjectKeep(_) | Operator::ProjectReorder(_) | Operator::Render { .. } => {
                column_values(input, col)
            }
            _ => None,
        },
        _ => None,
    }
}

/// Compiles traced value expressions to constants.
fn constant_values(ctx: &mut Ctx, exprs: &[Expr], env: &Env) -> Option<Vec<TExpr>> {
    let mut out = Vec::new();
    for e in exprs {
        let t = ctx.expr(e, &Scope::empty(), env).ok()?;
        t.konst.as_ref()?;
        out.push(t);
    }
    Some(out)
}

fn named_arg<'a>(args: &'a [Arg], name: &str) -> Option<&'a Expr> {
    args.iter().find(|a| a.name.as_deref().is_some_and(|n| n.eq_ignore_ascii_case(name))).map(|a| &a.expr)
}

// ---------------------------------------------------------------------- bag_unpack

#[derive(Default, Clone, Copy)]
struct Kinds {
    long: bool,
    real: bool,
    string: bool,
    bool_: bool,
    other: bool,
}

impl Kinds {
    fn add(&mut self, v: &serde_json::Value) {
        use serde_json::Value;
        match v {
            Value::Null => {}
            Value::Bool(_) => self.bool_ = true,
            Value::Number(n) if n.is_i64() || n.is_u64() => self.long = true,
            Value::Number(_) => self.real = true,
            Value::String(_) => self.string = true,
            _ => self.other = true,
        }
    }

    /// The column type Kusto infers for the values of one key.
    fn ty(self) -> KqlType {
        let Kinds { long, real, string, bool_, other } = self;
        match (long, real, string, bool_, other) {
            (true, false, false, false, false) => KqlType::Long,
            (_, true, false, false, false) => KqlType::Real,
            (false, false, true, false, false) => KqlType::String,
            (false, false, false, true, false) => KqlType::Bool,
            _ => KqlType::Dynamic,
        }
    }
}

fn bag_unpack(ctx: &mut Ctx, rel: Rel, input: Option<&Expr>, args: &[Arg], env: &Env) -> Result<Rel> {
    let positional: Vec<&Arg> = args.iter().filter(|a| a.name.is_none()).collect();
    let Some(Expr::Name(col)) = positional.first().map(|a| &a.expr) else {
        return err("bag_unpack(): the first argument must be a dynamic column");
    };
    let Some(src) = rel.cols.iter().find(|c| c.name == *col).cloned() else {
        return err(format!("bag_unpack(): unknown column '{col}'"));
    };
    if src.ty != KqlType::Dynamic {
        return err(format!("bag_unpack(): column '{col}' is not dynamic"));
    }
    let str_arg = |ctx: &mut Ctx, e: Option<&Expr>, what: &str| -> Result<Option<String>> {
        match e {
            None => Ok(None),
            Some(e) => match ctx.expr(e, &Scope::empty(), env)?.konst {
                Some(Const::Str(s)) => Ok(Some(s)),
                _ => err(format!("bag_unpack(): {what} must be a constant string")),
            },
        }
    };
    let prefix = str_arg(ctx, named_arg(args, "OutputColumnPrefix").or(positional.get(1).map(|a| &a.expr)), "the column prefix")?.unwrap_or_default();
    let conflict = str_arg(ctx, named_arg(args, "columnsConflict").or(positional.get(2).map(|a| &a.expr)), "columnsConflict")?.unwrap_or_else(|| "error".into());
    let ignored: Vec<String> = match named_arg(args, "ignoredProperties").or(positional.get(3).map(|a| &a.expr)) {
        None => Vec::new(),
        Some(e) => match ctx.expr(e, &Scope::empty(), env)?.konst {
            Some(Const::Dynamic(serde_json::Value::Array(items))) => items.iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
            _ => return err("bag_unpack(): ignoredProperties must be a constant dynamic array"),
        },
    };
    if !matches!(conflict.as_str(), "error" | "replace_source" | "keep_source") {
        return err(format!("bag_unpack(): unknown columnsConflict value '{conflict}'"));
    }

    // the bag keys, from the constant data
    let values = input
        .and_then(|i| column_values(i, col))
        .and_then(|exprs| constant_values(ctx, &exprs, env))
        .ok_or_else(|| crate::Error::new("bag_unpack(): the output columns depend on the data; only bags whose keys are known at translation time (dynamic constants in datatable/print/extend) are supported"))?;
    let mut keys: BTreeMap<String, Kinds> = BTreeMap::new();
    for v in &values {
        match &v.konst {
            Some(Const::Dynamic(serde_json::Value::Object(m))) => {
                for (k, x) in m {
                    if !ignored.contains(k) {
                        keys.entry(k.clone()).or_default().add(x);
                    }
                }
            }
            Some(Const::Null) | Some(Const::Dynamic(_)) => {}
            _ => return err("bag_unpack(): the column values must be dynamic"),
        }
    }

    let rel = rel.passthrough(ctx);
    let bag = col_ref(None, col);
    let unpacked = |ctx: &Ctx, key: &str, kinds: Kinds| -> (Item, Column) {
        let ty = kinds.ty();
        let v = TExpr::new(ctx.d.json_get_key_lit(&bag, key), KqlType::Dynamic);
        let v = if ty == KqlType::Dynamic { v } else { ctx.convert(v, ty) };
        let name = format!("{prefix}{key}");
        (Item { sql: v.sql, alias: name.clone() }, Column::new(name, ty))
    };
    let mut items = Vec::new();
    let mut cols = Vec::new();
    let mut used: Vec<String> = Vec::new();
    for c in &rel.cols {
        let key = c.name.strip_prefix(prefix.as_str()).filter(|k| keys.contains_key(*k)).map(str::to_string);
        if c.name == *col {
            // a key named like the source column takes its place
            if let Some(k) = key {
                let (i, cl) = unpacked(ctx, &k, keys[&k]);
                items.push(i);
                cols.push(cl);
                used.push(k);
            }
            continue;
        }
        match (key, conflict.as_str()) {
            (Some(k), "error") => return err(format!("bag_unpack(): the unpacked column '{prefix}{k}' already exists")),
            (Some(k), "replace_source") => {
                let (i, cl) = unpacked(ctx, &k, keys[&k]);
                items.push(i);
                cols.push(cl);
                used.push(k);
            }
            (Some(k), _) => {
                used.push(k);
                items.push(Item { sql: quote_ident(&c.name), alias: c.name.clone() });
                cols.push(c.clone());
            }
            (None, _) => {
                items.push(Item { sql: quote_ident(&c.name), alias: c.name.clone() });
                cols.push(c.clone());
            }
        }
    }
    for (k, kinds) in &keys {
        if !used.contains(k) {
            let (i, cl) = unpacked(ctx, k, *kinds);
            items.push(i);
            cols.push(cl);
        }
    }
    Ok(rel.project(ctx, items, cols))
}

// ---------------------------------------------------------------------- pivot

fn pivot(ctx: &mut Ctx, rel: Rel, input: Option<&Expr>, args: &[Arg], env: &Env) -> Result<Rel> {
    let Some(Expr::Name(pc)) = args.first().map(|a| &a.expr) else {
        return err("pivot(): the first argument must be the pivot column");
    };
    if rel.col(pc).is_none() {
        return err(format!("pivot(): unknown column '{pc}'"));
    }
    // the aggregation (default count()) and its "if" variant
    let (agg_name, agg_args, rest) = match args.get(1).map(|a| &a.expr) {
        Some(Expr::Call { name, args: aa }) if crate::aggs::is_aggregate(name) => (name.to_ascii_lowercase(), aa.clone(), &args[2..]),
        _ => ("count".to_string(), Vec::new(), &args[1.min(args.len())..]),
    };
    let if_name = match agg_name.as_str() {
        "count" => "countif",
        "sum" => "sumif",
        "avg" => "avgif",
        "min" => "minif",
        "max" => "maxif",
        "dcount" => "dcountif",
        "stdev" => "stdevif",
        "variance" => "varianceif",
        "make_list" => "make_list_if",
        "make_set" => "make_set_if",
        "take_any" | "any" => "take_anyif",
        other => return err(format!("pivot(): the aggregation '{other}' is not supported")),
    };
    // group-by columns: explicit, or every column not used by the pivot or the aggregation
    let mut used: Vec<String> = vec![pc.clone()];
    for a in &agg_args {
        collect_names(&a.expr, &mut used);
    }
    let by: Vec<String> = if rest.is_empty() {
        rel.cols.iter().filter(|c| !used.contains(&c.name)).map(|c| c.name.clone()).collect()
    } else {
        rest.iter()
            .map(|a| match &a.expr {
                Expr::Name(n) => Ok(n.clone()),
                _ => err("pivot(): group-by arguments must be column names"),
            })
            .collect::<Result<_>>()?
    };
    // the pivot values, from the constant data
    let exprs = input
        .and_then(|i| column_values(i, pc))
        .ok_or_else(|| crate::Error::new("pivot(): the output columns depend on the data; only pivot columns with values known at translation time (datatable/print constants) are supported"))?;
    let consts = constant_values(ctx, &exprs, env).ok_or_else(|| crate::Error::new("pivot(): the pivot column values must be constants"))?;
    let mut values: BTreeMap<String, Expr> = BTreeMap::new();
    for (e, t) in exprs.iter().zip(&consts) {
        if t.is_null_const() {
            continue;
        }
        let name = match &t.konst {
            Some(Const::Str(s)) => s.clone(),
            Some(Const::Long(v)) => v.to_string(),
            Some(Const::Bool(b)) => if *b { "True".into() } else { "False".into() },
            Some(Const::Real(v)) => v.to_string(),
            _ => return err("pivot(): unsupported pivot value type"),
        };
        values.entry(name).or_insert_with(|| e.clone());
    }
    let aggs: Vec<NamedExpr> = values
        .into_iter()
        .map(|(name, v)| {
            let pred = Expr::Binary { op: BinaryOp::Eq, left: Box::new(Expr::Name(pc.clone())), right: Box::new(v) };
            let mut a = agg_args.clone();
            a.push(Arg::positional(pred));
            NamedExpr { names: vec![name], expr: Expr::Call { name: if_name.to_string(), args: a } }
        })
        .collect();
    let by: Vec<NamedExpr> = by.into_iter().map(|n| NamedExpr { names: Vec::new(), expr: Expr::Name(n) }).collect();
    crate::aggs::summarize(ctx, rel, &aggs, &by, env)
}

fn collect_names(e: &Expr, out: &mut Vec<String>) {
    match e {
        Expr::Name(n) => out.push(n.clone()),
        Expr::Paren(x) | Expr::Unary { expr: x, .. } | Expr::Member { expr: x, .. } => collect_names(x, out),
        Expr::Binary { left, right, .. } => {
            collect_names(left, out);
            collect_names(right, out);
        }
        Expr::Index { expr, index } => {
            collect_names(expr, out);
            collect_names(index, out);
        }
        Expr::Call { args, .. } => args.iter().for_each(|a| collect_names(&a.expr, out)),
        Expr::Between { expr, low, high, .. } => {
            collect_names(expr, out);
            collect_names(low, out);
            collect_names(high, out);
        }
        Expr::In { expr, list, .. } => {
            collect_names(expr, out);
            list.iter().for_each(|x| collect_names(x, out));
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------- narrow

/// `narrow()`: one `(Row, Column, Value)` row per input cell; `Row` is the 0-based row number.
fn narrow(ctx: &mut Ctx, rel: Rel, args: &[Arg]) -> Result<Rel> {
    if !args.is_empty() {
        return err("narrow() takes no arguments");
    }
    if rel.cols.is_empty() {
        return err("narrow(): the input has no columns");
    }
    let order = rel.order_sql();
    let mut r = rel.passthrough(ctx);
    let over = if order.is_empty() { String::new() } else { format!("ORDER BY {}", order.join(", ")) };
    let mut items = r.identity_items();
    items.push(Item { sql: format!("(ROW_NUMBER() OVER ({over}) - 1)"), alias: "__kql_row".into() });
    r.sel.items = Some(items);
    let r = r.wrap(ctx);
    let names: Vec<String> = r.cols.iter().map(|c| quote_str(&c.name)).collect();
    let vals: Vec<String> = r.cols.iter().map(|c| ctx.to_string(TExpr::new(quote_ident(&c.name), c.ty)).sql).collect();
    let (ln, lv) = match ctx.d.kind() {
        Dialect::DuckDb => (format!("unnest([{}])", names.join(", ")), format!("unnest([{}])", vals.join(", "))),
        Dialect::Postgres => (format!("unnest(ARRAY[{}])", names.join(", ")), format!("unnest(ARRAY[{}])", vals.join(", "))),
    };
    let items = vec![
        Item { sql: "__kql_row".into(), alias: "Row".into() },
        Item { sql: ln, alias: "Column".into() },
        Item { sql: lv, alias: "Value".into() },
    ];
    let cols = vec![Column::new("Row", KqlType::Long), Column::new("Column", KqlType::String), Column::new("Value", KqlType::String)];
    let mut out = r;
    out.order.clear();
    out.sel.items = Some(items);
    out.cols = cols;
    // the unnested select is wrapped so later operators see plain columns
    Ok(out.wrap(ctx))
}

// ---------------------------------------------------------------------- externaldata

pub(crate) fn externaldata(ctx: &mut Ctx, columns: &[ColumnDecl], uris: &[Expr], props: &[(String, Expr)], env: &Env) -> Result<Rel> {
    if ctx.d.kind() != Dialect::DuckDb {
        return err("externaldata is only supported for DuckDB");
    }
    let cols: Vec<Column> = columns
        .iter()
        .map(|c| KqlType::from_name(&c.ty).map(|ty| Column::new(c.name.clone(), ty)).ok_or_else(|| crate::Error::new(format!("unknown type '{}'", c.ty))))
        .collect::<Result<_>>()?;
    if cols.is_empty() {
        return err("externaldata: at least one column is required");
    }
    let mut paths = Vec::new();
    for u in uris {
        match ctx.expr(u, &Scope::empty(), env)?.konst {
            // Kusto connection strings may carry options after ';' (credentials, ...)
            Some(Const::Str(s)) => paths.push(quote_str(s.split(';').next().unwrap_or_default())),
            _ => return err("externaldata: the storage URIs must be constant strings"),
        }
    }
    if paths.is_empty() {
        return err("externaldata: no storage URI given");
    }
    let mut format = "csv".to_string();
    let mut skip_header = false;
    for (k, v) in props {
        let t = ctx.expr(v, &Scope::empty(), env)?;
        match k.to_ascii_lowercase().as_str() {
            "format" => match (&t.konst, v) {
                (Some(Const::Str(s)), _) => format = s.to_ascii_lowercase(),
                (_, Expr::Name(n)) => format = n.to_ascii_lowercase(),
                _ => return err("externaldata: format must be a constant"),
            },
            "ignorefirstrecord" => skip_header = matches!(t.konst, Some(Const::Bool(true))),
            _ => {}
        }
    }
    let list = format!("[{}]", paths.join(", "));
    let all_varchar = format!("{{{}}}", cols.iter().map(|c| format!("{}: 'VARCHAR'", quote_str(&c.name))).collect::<Vec<_>>().join(", "));
    let from = match format.as_str() {
        "csv" | "tsv" | "tsve" | "psv" | "scsv" | "sohsv" | "txt" | "raw" => {
            let delim = match format.as_str() {
                "tsv" | "tsve" => "\\t",
                "psv" => "|",
                "scsv" => ";",
                "sohsv" => "\\x01",
                _ => ",",
            };
            if matches!(format.as_str(), "txt" | "raw") && cols.len() != 1 {
                return err("externaldata: the txt format requires exactly one column");
            }
            format!("read_csv({list}, columns = {all_varchar}, header = {skip_header}, delim = '{delim}', auto_detect = false, quote = '\"', escape = '\"')")
        }
        "json" | "multijson" => {
            let f = if format == "json" { "newline_delimited" } else { "auto" };
            format!("read_json({list}, columns = {all_varchar}, format = '{f}')")
        }
        "parquet" => format!("read_parquet({list})"),
        other => return err(format!("externaldata: unsupported format '{other}'")),
    };
    let alias = ctx.alias();
    let items = cols
        .iter()
        .map(|c| {
            let raw = format!("{alias}.{}", quote_ident(&c.name));
            let sql = if format == "parquet" {
                if c.ty == KqlType::String {
                    format!("COALESCE(CAST({raw} AS VARCHAR), '')")
                } else if c.ty == KqlType::TimeSpan {
                    ctx.d.ticks_from_interval(&raw)
                } else {
                    ctx.d.try_cast(&raw, c.ty)
                }
            } else {
                let t = TExpr::new(raw.clone(), KqlType::String);
                match c.ty {
                    KqlType::String => format!("COALESCE({raw}, '')"),
                    KqlType::Dynamic => format!("CASE WHEN {raw} IS NULL OR {raw} = '' THEN NULL ELSE COALESCE(TRY_CAST({raw} AS JSON), to_json({raw})) END"),
                    ty => ctx.convert(t, ty).sql,
                }
            };
            Item { sql, alias: c.name.clone() }
        })
        .collect();
    let sel = Select { items: Some(items), from: From::Raw(format!("{from} AS {alias}")), ..Default::default() };
    Ok(Rel::from_select(sel, cols))
}
