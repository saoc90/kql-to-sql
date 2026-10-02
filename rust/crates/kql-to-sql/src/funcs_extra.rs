//! Additional scalar functions: dynamic array/bag helpers, string utilities, bit operations,
//! IPv4 and URL helpers.
//!
//! Dynamic values are JSON (DuckDB `JSON`); arrays are processed as `JSON[]` lists and turned
//! back into JSON with `to_json`, bags as lists of `{k, v}` entries (`map_from_entries`).
//! Most functions here are implemented for DuckDB only; PostgreSQL gets a clear error.

use crate::binder::Ctx;
use crate::expr::{Const, Scope, TExpr};
use crate::sql::quote_str;
use crate::{err, Dialect, KqlType, Result};

/// Scalar functions not handled by the core library. `name` is lowercase; `a` are compiled
/// arguments. Returns `None` if `name` is not handled here.
pub(crate) fn call(ctx: &mut Ctx, name: &str, a: &[TExpr], _scope: &Scope) -> Option<Result<TExpr>> {
    let r = match name {
        // portable
        "strcmp" => strcmp(ctx, a),
        "binary_and" | "binary_or" | "binary_xor" | "binary_not" | "binary_shift_left" | "binary_shift_right" | "bitset_count_ones" => bits(ctx, name, a),
        "translate" => translate(ctx, a),
        "isascii" | "isutf8" => isascii(ctx, name, a),
        "ingestion_time" => need(name, a, 0, 0).map(|_| TExpr::new(format!("CAST(NULL AS {})", ctx.d.sql_type(KqlType::DateTime)), KqlType::DateTime)),
        "new_guid" => new_guid(ctx, a),
        "hash_sha1" => hash_sha1(ctx, a),
        // DuckDB
        _ => {
            if !is_duck_function(name) {
                return None;
            }
            if ctx.d.kind() != Dialect::DuckDb {
                if let Some(r) = pg(ctx, name, a) {
                    return Some(r);
                }
                return Some(err(format!("{name}() is not supported for PostgreSQL yet")));
            }
            duck(ctx, name, a)
        }
    };
    Some(r)
}

fn is_duck_function(name: &str) -> bool {
    matches!(
        name,
        "bag_merge"
            | "bag_remove_keys"
            | "bag_set_key"
            | "bag_zip"
            | "dynamic_to_json"
            | "array_rotate_left"
            | "array_rotate_right"
            | "array_shift_left"
            | "array_shift_right"
            | "array_split"
            | "array_iff"
            | "array_iif"
            | "array_strcat"
            | "jaccard_index"
            | "treepath"
            | "range"
            | "repeat"
            | "make_string"
            | "unicode_codepoints_from_string"
            | "unicode_codepoints_to_string"
            | "tohex"
            | "regex_quote"
            | "indexof_regex"
            | "has_any_index"
            | "url_encode"
            | "url_encode_component"
            | "url_decode"
            | "parse_url"
            | "parse_urlquery"
            | "parse_path"
            | "format_bytes"
            | "extract_json"
            | "extractjson"
            | "parse_csv"
            | "parse_ipv4"
            | "parse_ipv4_mask"
            | "ipv4_compare"
            | "ipv4_is_match"
            | "ipv4_is_in_range"
            | "ipv4_is_in_any_range"
            | "ipv4_is_private"
            | "ipv4_netmask_suffix"
            | "format_ipv4"
            | "format_ipv4_mask"
            | "has_ipv4"
            | "has_ipv4_prefix"
            | "has_any_ipv4"
            | "has_any_ipv4_prefix"
            | "base64_encode_fromguid"
            | "base64_decode_toguid"
            | "base64_encode_fromarray"
            | "base64_decode_toarray"
            | "assert"
    )
}

fn need(name: &str, a: &[TExpr], min: usize, max: usize) -> Result<()> {
    let n = a.len();
    if n < min || n > max {
        return err(format!("{name}(): expected {} arguments, got {n}", if min == max { min.to_string() } else { format!("{min}..{max}") }));
    }
    Ok(())
}

fn mk(sql: String, ty: KqlType, a: &[TExpr]) -> TExpr {
    let parts: Vec<&TExpr> = a.iter().collect();
    TExpr::derived(sql, ty, &parts)
}

/// An integer argument as BIGINT SQL (dynamic numbers are converted).
fn long_arg(ctx: &Ctx, t: &TExpr) -> String {
    match t.ty {
        KqlType::Long => t.sql.clone(),
        KqlType::Dynamic | KqlType::String | KqlType::Real | KqlType::Decimal => ctx.convert(t.clone(), KqlType::Long).sql,
        _ => ctx.d.cast(&t.sql, KqlType::Long),
    }
}

fn str_arg(ctx: &Ctx, t: &TExpr) -> String {
    ctx.to_string(t.clone()).sql
}

// ---------------------------------------------------------------------- portable functions

fn strcmp(ctx: &mut Ctx, a: &[TExpr]) -> Result<TExpr> {
    need("strcmp", a, 2, 2)?;
    let (x, y) = (str_arg(ctx, &a[0]), str_arg(ctx, &a[1]));
    let c = if ctx.d.kind() == Dialect::Postgres { " COLLATE \"C\"" } else { "" };
    let int = ctx.d.sql_type(KqlType::Int);
    Ok(mk(format!("CAST(CASE WHEN {x}{c} < {y}{c} THEN -1 WHEN {x} = {y} THEN 0 ELSE 1 END AS {int})"), KqlType::Int, a))
}

/// Wraps a 128-bit integer expression to a signed 64-bit value (two's complement).
fn wrap64(v: &str) -> String {
    let m = "CAST(18446744073709551616 AS HUGEINT)";
    format!("CAST(CASE WHEN ((({v}) % {m}) + {m}) % {m} >= CAST(9223372036854775808 AS HUGEINT) THEN ((({v}) % {m}) + {m}) % {m} - {m} ELSE ((({v}) % {m}) + {m}) % {m} END AS BIGINT)")
}

fn bits(ctx: &mut Ctx, name: &str, a: &[TExpr]) -> Result<TExpr> {
    let pg = ctx.d.kind() == Dialect::Postgres;
    let unary = matches!(name, "binary_not" | "bitset_count_ones");
    need(name, a, if unary { 1 } else { 2 }, if unary { 1 } else { 2 })?;
    let x = long_arg(ctx, &a[0]);
    let y = a.get(1).map(|t| long_arg(ctx, t));
    let y = y.as_deref().unwrap_or("0");
    let sql = match name {
        "binary_and" => format!("({x} & {y})"),
        "binary_or" => format!("({x} | {y})"),
        "binary_xor" if pg => format!("({x} # {y})"),
        "binary_xor" => format!("xor({x}, {y})"),
        "binary_not" => format!("(~{x})"),
        "bitset_count_ones" if pg => format!("CAST(length(replace(CAST(CAST({x} AS bit(64)) AS text), '0', '')) AS integer)"),
        "bitset_count_ones" => format!("CAST(bit_count({x}) AS INTEGER)"),
        // .NET semantics: the shift count is taken modulo 64; left shifts wrap around
        "binary_shift_left" if pg => format!("({x} << CAST(({y}) & 63 AS integer))"),
        "binary_shift_left" => {
            let v = format!("CAST({x} AS HUGEINT) * (CAST(1 AS HUGEINT) << CAST(({y}) & 63 AS INTEGER))");
            wrap64(&v)
        }
        _ if pg => format!("({x} >> CAST(({y}) & 63 AS integer))"),
        _ => format!("({x} >> CAST(({y}) & 63 AS INTEGER))"),
    };
    let ty = if name == "bitset_count_ones" { KqlType::Int } else { KqlType::Long };
    // null inputs give null (the CASE in wrap64 already propagates them)
    Ok(mk(sql, ty, a))
}

fn translate(ctx: &mut Ctx, a: &[TExpr]) -> Result<TExpr> {
    need("translate", a, 3, 3)?;
    let (search, repl, text) = (str_arg(ctx, &a[0]), str_arg(ctx, &a[1]), str_arg(ctx, &a[2]));
    // a shorter replacement list repeats its last character; an empty one deletes
    let repl = format!("(CASE WHEN length({repl}) = 0 THEN '' ELSE {repl} || repeat(right({repl}, 1), CAST(greatest(length({search}) - length({repl}), 0) AS INTEGER)) END)");
    Ok(mk(format!("translate({text}, {search}, {repl})"), KqlType::String, a))
}

fn isascii(ctx: &mut Ctx, name: &str, a: &[TExpr]) -> Result<TExpr> {
    need(name, a, 1, 1)?;
    let s = str_arg(ctx, &a[0]);
    if name == "isutf8" {
        return Ok(mk("true".into(), KqlType::Bool, a));
    }
    Ok(mk(ctx.d.regex_match(&s, "'^[\\x00-\\x7F]*$'"), KqlType::Bool, a))
}

fn new_guid(ctx: &mut Ctx, a: &[TExpr]) -> Result<TExpr> {
    need("new_guid", a, 0, 0)?;
    let _ = ctx;
    Ok(TExpr::new("gen_random_uuid()", KqlType::Guid))
}

fn hash_sha1(ctx: &mut Ctx, a: &[TExpr]) -> Result<TExpr> {
    need("hash_sha1", a, 1, 1)?;
    let s = str_arg(ctx, &a[0]);
    let sql = match ctx.d.kind() {
        Dialect::DuckDb => format!("sha1({s})"),
        Dialect::Postgres => format!("encode(sha1(convert_to({s}, 'UTF8')), 'hex')"),
    };
    Ok(mk(sql, KqlType::String, a))
}

// ---------------------------------------------------------------------- DuckDB functions

/// `CAST(x AS JSON[])` when `x` is an array, NULL otherwise.
fn jlist(x: &str) -> String {
    format!("(CASE WHEN json_type({x}) = 'ARRAY' THEN CAST({x} AS JSON[]) END)")
}

/// Binds `value` to a lambda variable so it is evaluated (and written) once:
/// `list_transform([value], v -> body)[1]`.
fn bind(ctx: &mut Ctx, value: &str, body: impl FnOnce(&mut Ctx, &str) -> String) -> String {
    let v = ctx.alias();
    let b = body(ctx, &v);
    format!("(list_transform([{value}], {v} -> {b})[1])")
}

const ENTRY_TYPE: &str = "STRUCT(k VARCHAR, v JSON)[]";

/// The `{k, v}` entries of a bag (empty for non-bags).
fn entries(ctx: &mut Ctx, x: &str) -> String {
    let k = ctx.alias();
    let v = ctx.d.json_get_key(x, &k);
    format!("(CASE WHEN json_type({x}) = 'OBJECT' THEN list_transform(json_keys({x}), {k} -> {{'k': {k}, 'v': {v}}}) ELSE CAST([] AS {ENTRY_TYPE}) END)")
}

/// A bag from `{k, v}` entries, keeping the first entry of each key.
fn bag_from_entries(ctx: &mut Ctx, list: &str) -> String {
    bind(ctx, list, |ctx, l| {
        let (e, i, x) = (ctx.alias(), ctx.alias(), ctx.alias());
        format!("to_json(map_from_entries(list_filter({l}, ({e}, {i}) -> list_position(list_transform({l}, {x} -> {x}.k), {e}.k) = {i})))")
    })
}

fn dyn_arg(ctx: &Ctx, t: &TExpr) -> String {
    ctx.to_dynamic(t.clone()).sql
}

fn duck(ctx: &mut Ctx, name: &str, a: &[TExpr]) -> Result<TExpr> {
    use KqlType::*;
    let d = ctx.d;
    match name {
        // ------------------------------------------------------------------ bags
        "bag_merge" => {
            need(name, a, 2, 64)?;
            let mut lists = Vec::new();
            for t in a {
                let x = dyn_arg(ctx, t);
                lists.push(entries(ctx, &x));
            }
            let all = format!("flatten([{}])", lists.join(", "));
            Ok(mk(bag_from_entries(ctx, &all), Dynamic, a))
        }
        "bag_remove_keys" => {
            need(name, a, 2, 2)?;
            let bag = dyn_arg(ctx, &a[0]);
            let keys = dyn_arg(ctx, &a[1]);
            let e = ctx.alias();
            let es = entries(ctx, &bag);
            let k = ctx.alias();
            let key_list = format!("list_transform({}, {k} -> json_extract_string({k}, '$'))", jlist(&keys));
            let kept = format!("list_filter({es}, {e} -> NOT COALESCE(list_contains({key_list}, {e}.k), false))");
            Ok(mk(format!("CASE WHEN json_type({bag}) = 'OBJECT' THEN {} END", bag_from_entries(ctx, &kept)), Dynamic, a))
        }
        "bag_set_key" => {
            need(name, a, 3, 3)?;
            let bag = dyn_arg(ctx, &a[0]);
            let key = str_arg(ctx, &a[1]);
            let val = dyn_arg(ctx, &a[2]);
            let es = entries(ctx, &bag);
            let e = ctx.alias();
            let rest = format!("list_filter({es}, {e} -> {e}.k <> {key})");
            let list = format!("list_concat({rest}, [{{'k': {key}, 'v': {val}}}])");
            // the new key keeps the position of the key it replaces
            let pos = ctx.alias();
            let replaced = format!("list_transform({es}, {pos} -> CASE WHEN {pos}.k = {key} THEN {{'k': {key}, 'v': {val}}} ELSE {pos} END)");
            let has = format!("list_contains(json_keys({bag}), {key})");
            Ok(mk(
                format!("CASE WHEN json_type({bag}) = 'OBJECT' THEN to_json(map_from_entries(CASE WHEN {has} THEN {replaced} ELSE {list} END)) END"),
                Dynamic,
                a,
            ))
        }
        "bag_zip" => {
            need(name, a, 2, 2)?;
            let (ks, vs) = (dyn_arg(ctx, &a[0]), dyn_arg(ctx, &a[1]));
            let (kl, vl) = (jlist(&ks), jlist(&vs));
            let i = ctx.alias();
            let list = format!(
                "list_filter(list_transform(range(len({kl})), {i} -> {{'k': CASE WHEN json_type({kl}[{i} + 1]) = 'VARCHAR' THEN json_extract_string({kl}[{i} + 1], '$') END, 'v': {vl}[{i} + 1]}}), {i} -> {i}.k IS NOT NULL)"
            );
            Ok(mk(format!("CASE WHEN json_type({ks}) = 'ARRAY' AND json_type({vs}) = 'ARRAY' THEN {} END", bag_from_entries(ctx, &list)), Dynamic, a))
        }
        "dynamic_to_json" => {
            need(name, a, 1, 1)?;
            let x = &a[0];
            if let Some(Const::Dynamic(v)) = &x.konst {
                // serde_json maps are ordered by key, as Kusto's dynamic_to_json output is
                let s = v.to_string();
                return Ok(TExpr::konst(quote_str(&s), String, Const::Str(s)));
            }
            if x.is_null_const() {
                return Ok(TExpr::konst("'null'", String, Const::Str("null".into())));
            }
            if x.ty != Dynamic {
                let dj = dyn_arg(ctx, x);
                return Ok(mk(format!("COALESCE(CAST(json({dj}) AS VARCHAR), 'null')"), String, a));
            }
            let canon = canonical_json(ctx, &x.sql, 4);
            Ok(mk(format!("COALESCE(CAST(json({canon}) AS VARCHAR), 'null')"), String, a))
        }
        // ------------------------------------------------------------------ arrays
        "array_rotate_left" | "array_rotate_right" => {
            need(name, a, 2, 2)?;
            let arr = dyn_arg(ctx, &a[0]);
            let n = long_arg(ctx, &a[1]);
            let n = if name == "array_rotate_right" { format!("(-{n})") } else { n };
            let body = bind(ctx, &jlist(&arr), |ctx, l| {
                let i = ctx.alias();
                format!("to_json(list_transform(range(len({l})), {i} -> {l}[(({i} + {n}) % len({l}) + len({l})) % len({l}) + 1]))")
            });
            Ok(mk(format!("CASE WHEN json_type({arr}) = 'ARRAY' THEN {body} END"), Dynamic, a))
        }
        "array_shift_left" | "array_shift_right" => {
            need(name, a, 2, 3)?;
            let arr = dyn_arg(ctx, &a[0]);
            let n = long_arg(ctx, &a[1]);
            let n = if name == "array_shift_right" { format!("(-{n})") } else { n };
            let fill = match a.get(2) {
                Some(f) => dyn_arg(ctx, f),
                None => "CAST(NULL AS JSON)".into(),
            };
            let body = bind(ctx, &jlist(&arr), |ctx, l| {
                let i = ctx.alias();
                format!("to_json(list_transform(range(len({l})), {i} -> CASE WHEN {i} + {n} >= 0 AND {i} + {n} < len({l}) THEN {l}[{i} + {n} + 1] ELSE {fill} END))")
            });
            Ok(mk(format!("CASE WHEN json_type({arr}) = 'ARRAY' THEN {body} END"), Dynamic, a))
        }
        "array_split" => {
            need(name, a, 2, 2)?;
            let arr = dyn_arg(ctx, &a[0]);
            let idx = if a[1].ty == Dynamic {
                let i = ctx.alias();
                format!("list_transform({}, {i} -> TRY_CAST(json_extract_string({i}, '$') AS BIGINT))", jlist(&a[1].sql))
            } else {
                format!("[{}]", long_arg(ctx, &a[1]))
            };
            // negative indices count from the end; indices are clamped to the array
            let body = bind(ctx, &jlist(&arr), |ctx, l| {
                let j = ctx.alias();
                let norm = format!("list_transform({idx}, {j} -> least(greatest(CASE WHEN {j} < 0 THEN {j} + len({l}) ELSE {j} END, 0), len({l})))");
                let bounds = format!("list_concat([0], {norm}, [len({l})])");
                bind(ctx, &bounds, |ctx, b| {
                    let k = ctx.alias();
                    format!("to_json(list_transform(range(len({b}) - 1), {k} -> list_slice({l}, {b}[{k} + 1] + 1, {b}[{k} + 2])))")
                })
            });
            Ok(mk(format!("CASE WHEN json_type({arr}) = 'ARRAY' THEN {body} END"), Dynamic, a))
        }
        "array_iff" | "array_iif" => {
            need(name, a, 3, 3)?;
            let cond = dyn_arg(ctx, &a[0]);
            let i = ctx.alias();
            // array arguments are indexed, scalars broadcast
            let mut branch = Vec::new();
            for t in &a[1..3] {
                branch.push(if t.ty == Dynamic {
                    let l = ctx.alias();
                    (Some((l.clone(), format!("CASE WHEN json_type({0}) = 'ARRAY' THEN CAST({0} AS JSON[]) ELSE [{0}] END", t.sql))), format!("CASE WHEN len({l}) = 1 AND json_type({0}) <> 'ARRAY' THEN {l}[1] ELSE {l}[{i} + 1] END", t.sql))
                } else {
                    (None, dyn_arg(ctx, t))
                });
            }
            let body = bind(ctx, &jlist(&cond), |ctx, cl| {
                let c = format!("{cl}[{i} + 1]");
                let truth = format!("CASE WHEN json_type({c}) = 'BOOLEAN' THEN json_extract_string({c}, '$') = 'true' WHEN json_type({c}) IN ('BIGINT', 'UBIGINT', 'DOUBLE') THEN TRY_CAST(json_extract_string({c}, '$') AS DOUBLE) <> 0 END");
                let _ = ctx;
                format!("to_json(list_transform(range(len({cl})), {i} -> CASE WHEN {truth} THEN {} WHEN NOT {truth} THEN {} END))", branch[0].1, branch[1].1)
            });
            // bind the array branches (outermost first)
            let mut sql = body;
            for (bound, _) in branch.iter().rev() {
                if let Some((l, v)) = bound {
                    sql = format!("(list_transform([{v}], {l} -> {sql})[1])");
                }
            }
            Ok(mk(format!("CASE WHEN json_type({cond}) = 'ARRAY' THEN {sql} END"), Dynamic, a))
        }
        "array_strcat" => {
            need(name, a, 2, 2)?;
            let arr = dyn_arg(ctx, &a[0]);
            let delim = str_arg(ctx, &a[1]);
            let e = ctx.alias();
            Ok(mk(format!("COALESCE(array_to_string(list_transform({}, {e} -> COALESCE(json_extract_string({e}, '$'), '')), {delim}), '')", jlist(&arr)), String, a))
        }
        "jaccard_index" => {
            need(name, a, 2, 2)?;
            let (x, y) = (jlist(&dyn_arg(ctx, &a[0])), jlist(&dyn_arg(ctx, &a[1])));
            let (e1, e2) = (ctx.alias(), ctx.alias());
            let xs = format!("list_distinct(list_transform({x}, {e1} -> CAST({e1} AS VARCHAR)))");
            let ys = format!("list_distinct(list_transform({y}, {e2} -> CAST({e2} AS VARCHAR)))");
            let e3 = ctx.alias();
            let inter = format!("len(list_filter({xs}, {e3} -> list_contains({ys}, {e3})))");
            let union = format!("len(list_distinct(list_concat({xs}, {ys})))");
            Ok(mk(format!("CASE WHEN {union} = 0 THEN {} ELSE CAST({inter} AS DOUBLE) / {union} END", d.real_literal(f64::NAN)), Real, a))
        }
        "treepath" => {
            need(name, a, 1, 1)?;
            if let Some(Const::Dynamic(v)) = &a[0].konst {
                let mut paths = Vec::new();
                tree_paths(v, "", &mut paths);
                let json = serde_json::Value::Array(paths.into_iter().map(serde_json::Value::String).collect()).to_string();
                return Ok(TExpr::konst(d.json_literal(&json), Dynamic, Const::Dynamic(serde_json::from_str(&json).unwrap_or_default())));
            }
            let x = dyn_arg(ctx, &a[0]);
            let t = ctx.alias();
            // keys containing '.' or '[' cannot be told apart in DuckDB's json_tree paths
            let p = format!("regexp_replace(regexp_replace(substr({t}.fullkey, 2), '\\[[0-9]+\\]', '[0]', 'g'), '\\.([^.\\[]+)', '[''\\1'']', 'g')");
            Ok(mk(
                format!("(SELECT COALESCE(to_json(list(p ORDER BY first_id)), CAST('[]' AS JSON)) FROM (SELECT {p} AS p, min({t}.id) AS first_id FROM json_tree({x}) AS {t} WHERE {t}.id > 0 GROUP BY 1))"),
                Dynamic,
                a,
            ))
        }
        "range" => range(ctx, a),
        "repeat" => {
            need(name, a, 2, 2)?;
            if a[0].ty == Dynamic {
                return err("repeat(): the value must be a scalar");
            }
            let v = dyn_arg(ctx, &a[0]);
            let n = long_arg(ctx, &a[1]);
            let i = ctx.alias();
            Ok(mk(format!("CASE WHEN {n} IS NOT NULL THEN to_json(list_transform(range(greatest({n}, 0)), {i} -> {v})) END"), Dynamic, a))
        }
        // ------------------------------------------------------------------ strings
        "make_string" => {
            need(name, a, 1, 64)?;
            let mut parts = Vec::new();
            for t in a {
                if t.ty == Dynamic {
                    let c = ctx.alias();
                    parts.push(format!("COALESCE(array_to_string(list_transform({}, {c} -> chr(CAST(json_extract_string({c}, '$') AS INTEGER))), ''), '')", jlist(&t.sql)));
                } else {
                    parts.push(format!("chr(CAST({} AS INTEGER))", long_arg(ctx, t)));
                }
            }
            Ok(mk(format!("({})", parts.join(" || ")), String, a))
        }
        "unicode_codepoints_from_string" => {
            need(name, a, 1, 1)?;
            let s = str_arg(ctx, &a[0]);
            let c = ctx.alias();
            Ok(mk(format!("CASE WHEN {s} = '' THEN CAST('[]' AS JSON) ELSE to_json(list_transform(string_split({s}, ''), {c} -> CAST(unicode({c}) AS BIGINT))) END"), Dynamic, a))
        }
        "unicode_codepoints_to_string" => {
            need(name, a, 1, 1)?;
            let x = dyn_arg(ctx, &a[0]);
            let c = ctx.alias();
            Ok(mk(format!("COALESCE(array_to_string(list_transform({}, {c} -> chr(CAST(json_extract_string({c}, '$') AS INTEGER))), ''), '')", jlist(&x)), String, a))
        }
        "tohex" => {
            need(name, a, 1, 2)?;
            let x = long_arg(ctx, &a[0]);
            let h = format!("printf('%x', {x})");
            let sql = match a.get(1) {
                Some(m) => {
                    let m = long_arg(ctx, m);
                    format!("CASE WHEN length({h}) >= {m} THEN {h} ELSE lpad({h}, CAST({m} AS INTEGER), '0') END")
                }
                None => h,
            };
            Ok(mk(format!("COALESCE({sql}, '')"), String, a))
        }
        "regex_quote" => {
            need(name, a, 1, 1)?;
            let s = str_arg(ctx, &a[0]);
            // .NET Regex.Escape
            let esc = format!("regexp_replace({s}, '([\\\\*+?|{{\\[()^$.#])', '\\\\\\1', 'g')");
            let esc = format!("replace(replace(replace(replace({esc}, ' ', '\\ '), chr(9), '\\t'), chr(10), '\\n'), chr(13), '\\r')");
            Ok(mk(esc, String, a))
        }
        "indexof_regex" => {
            need(name, a, 2, 2)?;
            let s = str_arg(ctx, &a[0]);
            let re = match a[1].str_const() {
                Some(r) => quote_str(&format!("(?s)^(.*?)(?:{})", crate::regex::translate(r, d.kind()))),
                None => format!("('(?s)^(.*?)(?:' || {} || ')')", str_arg(ctx, &a[1])),
            };
            Ok(mk(format!("CASE WHEN regexp_matches({s}, {re}) THEN CAST(length(regexp_extract({s}, {re}, 1)) AS BIGINT) ELSE -1 END"), Long, a))
        }
        "has_any_index" => {
            need(name, a, 2, 2)?;
            let s = str_arg(ctx, &a[0]);
            let Some(Const::Dynamic(serde_json::Value::Array(items))) = &a[1].konst else {
                return err("has_any_index(): the values must be a constant dynamic array");
            };
            if items.is_empty() {
                return Ok(mk("CAST(-1 AS BIGINT)".into(), Long, a));
            }
            let mut sql = std::string::String::from("CASE");
            for (i, v) in items.iter().enumerate() {
                let t = ctx.json_const_to_scalar(v);
                let t = ctx.to_string(t);
                sql.push_str(&format!(" WHEN {} THEN {i}", ctx.term_match(&s, &t, false, true, true)));
            }
            sql.push_str(" ELSE -1 END");
            Ok(mk(format!("CAST({sql} AS BIGINT)"), Long, a))
        }
        "url_encode_component" => {
            need(name, a, 1, 1)?;
            Ok(mk(format!("url_encode({})", str_arg(ctx, &a[0])), String, a))
        }
        "url_encode" => {
            need(name, a, 1, 1)?;
            // HttpUtility.UrlEncode: lowercase hex, '+' for spaces, -_.!*() unescaped
            let s = str_arg(ctx, &a[0]);
            let c = ctx.alias();
            Ok(mk(
                format!(
                    "COALESCE(array_to_string(list_transform(string_split({s}, ''), {c} -> CASE WHEN regexp_matches({c}, '^[A-Za-z0-9_.!*()-]$') THEN {c} WHEN {c} = ' ' THEN '+' WHEN {c} = '~' THEN '%7e' ELSE lower(url_encode({c})) END), ''), '')"
                ),
                String,
                a,
            ))
        }
        "url_decode" => {
            need(name, a, 1, 1)?;
            Ok(mk(format!("COALESCE(TRY(url_decode(replace({}, '+', ' '))), '')", str_arg(ctx, &a[0])), String, a))
        }
        "parse_url" => {
            need(name, a, 1, 1)?;
            let s = str_arg(ctx, &a[0]);
            let re = quote_str(r"^([A-Za-z][A-Za-z0-9+.\-]*)://(?:([^:@/?#]*)(?::([^@/?#]*))?@)?(\[[^\]]*\]|[^:/?#]*)(?::([0-9]*))?([^?#]*)(?:\?([^#]*))?(?:#(.*))?$");
            let g = |n: u32| format!("CASE WHEN regexp_matches({s}, {re}) THEN regexp_extract({s}, {re}, {n}) ELSE '' END");
            let q = query_bag(ctx, &g(7));
            Ok(mk(
                format!(
                    "json_object('Scheme', {}, 'Host', {}, 'Port', {}, 'Path', {}, 'Username', {}, 'Password', {}, 'Query Parameters', {q}, 'Fragment', {})",
                    g(1),
                    g(4),
                    g(5),
                    g(6),
                    g(2),
                    g(3),
                    g(8)
                ),
                Dynamic,
                a,
            ))
        }
        "parse_urlquery" => {
            need(name, a, 1, 1)?;
            let s = str_arg(ctx, &a[0]);
            let q = format!("regexp_replace({s}, '^\\?', '')");
            let bag = query_bag(ctx, &q);
            Ok(mk(format!("json_object('Query Parameters', {bag})"), Dynamic, a))
        }
        "parse_path" => {
            need(name, a, 1, 1)?;
            let s = str_arg(ctx, &a[0]);
            let re = quote_str(r"^(?:([A-Za-z][A-Za-z0-9+.\-]*)://)?(.*)$");
            let scheme = format!("regexp_extract({s}, {re}, 1)");
            let rest = format!("regexp_extract({s}, {re}, 2)");
            let file = format!("regexp_extract({rest}, '([^/\\\\]*)$', 1)");
            let dir = format!("regexp_extract({rest}, '^(.*)[/\\\\][^/\\\\]*$', 1)");
            let dirname = format!("regexp_extract({dir}, '([^/\\\\]*)$', 1)");
            let ext = format!("regexp_extract({file}, '\\.([^.]*)$', 1)");
            let root = format!("CASE WHEN {scheme} <> '' THEN regexp_extract({rest}, '^([^/\\\\]*)', 1) ELSE regexp_extract({rest}, '^([A-Za-z]:)', 1) END");
            Ok(mk(
                format!(
                    "json_object('Scheme', {scheme}, 'RootPath', {root}, 'DirectoryPath', {dir}, 'DirectoryName', {dirname}, 'Filename', {file}, 'Extension', {ext}, 'AlternateDataStreamName', '')"
                ),
                Dynamic,
                a,
            ))
        }
        "format_bytes" => format_bytes(ctx, a),
        "extract_json" | "extractjson" => {
            need(name, a, 2, 3)?;
            let path = str_arg(ctx, &a[0]);
            let text = if a[1].ty == Dynamic { format!("CAST({} AS VARCHAR)", a[1].sql) } else { str_arg(ctx, &a[1]) };
            let v = format!("CASE WHEN json_valid({text}) THEN json_extract_string({text}, {path}) END");
            let t = TExpr::derived(v, String, &[&a[0], &a[1]]);
            match a.get(2) {
                Some(tl) => {
                    let ty = match &tl.konst {
                        Some(Const::Str(s)) => KqlType::from_name(s).ok_or_else(|| crate::Error::new(format!("unknown type '{s}'")))?,
                        _ => return err("extract_json(): the third argument must be typeof(<type>)"),
                    };
                    if ty == String {
                        Ok(mk(format!("COALESCE({}, '')", t.sql), String, a))
                    } else if ty == Dynamic {
                        let j = format!("CASE WHEN json_valid({text}) THEN json_extract({text}, {path}) END");
                        Ok(mk(j, Dynamic, a))
                    } else {
                        Ok(ctx.convert(t, ty))
                    }
                }
                None => Ok(mk(format!("COALESCE({}, '')", t.sql), String, a)),
            }
        }
        "parse_csv" => {
            need(name, a, 1, 1)?;
            let s = str_arg(ctx, &a[0]);
            let re = quote_str("(?:^|,)(\"(?:[^\"]|\"\")*\"|[^,]*)");
            let f = ctx.alias();
            let unq = format!("CASE WHEN {f} LIKE '\"%\"' AND length({f}) >= 2 THEN replace(substr({f}, 2, length({f}) - 2), '\"\"', '\"') ELSE {f} END");
            Ok(mk(format!("to_json(list_transform(regexp_extract_all({s}, {re}, 1), {f} -> {unq}))"), Dynamic, a))
        }
        // ------------------------------------------------------------------ IPv4
        "parse_ipv4" => {
            need(name, a, 1, 1)?;
            let s = str_arg(ctx, &a[0]);
            let ip = ipv4(&s);
            Ok(mk(bind(ctx, &ip, |_, x| masked(&format!("{x}.v"), &format!("{x}.p"))), Long, a))
        }
        "parse_ipv4_mask" => {
            need(name, a, 2, 2)?;
            let s = str_arg(ctx, &a[0]);
            let m = long_arg(ctx, &a[1]);
            let ip = ipv4(&s);
            Ok(mk(bind(ctx, &ip, |_, x| format!("CASE WHEN {m} BETWEEN 0 AND 32 THEN {} END", masked(&format!("{x}.v"), &format!("least({x}.p, {m})")))), Long, a))
        }
        "ipv4_compare" | "ipv4_is_match" => {
            need(name, a, 2, 3)?;
            let (xs, ys) = (ipv4(&str_arg(ctx, &a[0])), ipv4(&str_arg(ctx, &a[1])));
            let m = a.get(2).map(|t| long_arg(ctx, t));
            let is_match = name == "ipv4_is_match";
            // both sides are compared on the shortest of their prefixes and the explicit one
            let sql = bind(ctx, &xs, |ctx, x| {
                bind(ctx, &ys, |_, y| {
                    let mut p = format!("least({x}.p, {y}.p)");
                    if let Some(m) = &m {
                        p = format!("least({p}, {m})");
                    }
                    let (ax, ay) = (masked(&format!("{x}.v"), &p), masked(&format!("{y}.v"), &p));
                    if is_match {
                        format!("({ax} = {ay})")
                    } else {
                        format!("CAST(CASE WHEN {ax} < {ay} THEN -1 WHEN {ax} = {ay} THEN 0 WHEN {ax} > {ay} THEN 1 END AS INTEGER)")
                    }
                })
            });
            Ok(mk(sql, if is_match { Bool } else { Int }, a))
        }
        "ipv4_is_in_range" => {
            need(name, a, 2, 2)?;
            let (ip, range) = (str_arg(ctx, &a[0]), str_arg(ctx, &a[1]));
            Ok(mk(ipv4_in_range(ctx, &ip, &range), Bool, a))
        }
        "ipv4_is_in_any_range" => {
            need(name, a, 2, 64)?;
            let ip = str_arg(ctx, &a[0]);
            let mut ranges = Vec::new();
            for t in &a[1..] {
                match &t.konst {
                    Some(Const::Dynamic(serde_json::Value::Array(items))) => ranges.extend(items.iter().filter_map(|v| v.as_str().map(quote_str))),
                    _ if t.ty == Dynamic => return err("ipv4_is_in_any_range(): the ranges must be strings or a constant array"),
                    _ => ranges.push(str_arg(ctx, t)),
                }
            }
            if ranges.is_empty() {
                return Ok(mk("false".into(), Bool, a));
            }
            let conds: Vec<std::string::String> = ranges.iter().map(|r| ipv4_in_range(ctx, &ip, r)).collect();
            let valid = ipv4(&ip);
            Ok(mk(format!("CASE WHEN {valid} IS NULL THEN NULL ELSE COALESCE({}, false) END", conds.join(" OR ")), Bool, a))
        }
        "ipv4_is_private" => {
            need(name, a, 1, 1)?;
            let ip = ipv4(&str_arg(ctx, &a[0]));
            let sql = bind(ctx, &ip, |_, x| {
                let inside = |net: i64, bits: i64| format!("({x}.p >= {bits} AND ({x}.v // {}) = {})", 1i64 << (32 - bits), net >> (32 - bits));
                format!("CASE WHEN {x} IS NULL THEN NULL ELSE ({} OR {} OR {}) END", inside(0x0A00_0000, 8), inside(0xAC10_0000, 12), inside(0xC0A8_0000, 16))
            });
            Ok(mk(sql, Bool, a))
        }
        "ipv4_netmask_suffix" => {
            need(name, a, 1, 1)?;
            let ip = ipv4(&str_arg(ctx, &a[0]));
            Ok(mk(bind(ctx, &ip, |_, x| format!("CAST({x}.p AS INTEGER)")), Int, a))
        }
        "format_ipv4" | "format_ipv4_mask" => {
            need(name, a, 1, 2)?;
            let m = a.get(1).map(|t| long_arg(ctx, t)).unwrap_or_else(|| "32".into());
            let v = if a[0].ty.is_numeric() {
                let x = long_arg(ctx, &a[0]);
                format!("CASE WHEN {x} BETWEEN 0 AND 4294967295 AND {m} BETWEEN 0 AND 32 THEN {} END", masked(&x, &m))
            } else {
                let ip = ipv4(&str_arg(ctx, &a[0]));
                bind(ctx, &ip, |_, x| format!("CASE WHEN {m} BETWEEN 0 AND 32 THEN {} END", masked(&format!("{x}.v"), &format!("least({x}.p, {m})"))))
            };
            let with_mask = name == "format_ipv4_mask";
            let sql = bind(ctx, &v, |_, v| {
                let dotted = format!("printf('%d.%d.%d.%d', {v} // 16777216, ({v} // 65536) % 256, ({v} // 256) % 256, {v} % 256)");
                if with_mask {
                    format!("CASE WHEN {v} IS NULL THEN '' ELSE {dotted} || '/' || CAST({m} AS VARCHAR) END")
                } else {
                    format!("CASE WHEN {v} IS NULL THEN '' ELSE {dotted} END")
                }
            });
            Ok(mk(sql, String, a))
        }
        "has_ipv4" | "has_ipv4_prefix" | "has_any_ipv4" | "has_any_ipv4_prefix" => {
            need(name, a, 2, 64)?;
            let s = str_arg(ctx, &a[0]);
            let prefix = name.ends_with("_prefix");
            let mut needles: Vec<std::string::String> = Vec::new();
            for t in &a[1..] {
                match &t.konst {
                    Some(Const::Str(v)) => needles.push(v.clone()),
                    Some(Const::Dynamic(serde_json::Value::Array(items))) => needles.extend(items.iter().filter_map(|v| v.as_str().map(str::to_string))),
                    _ => return err(format!("{name}(): the IP addresses must be constants")),
                }
            }
            if needles.is_empty() {
                return Ok(mk("false".into(), Bool, a));
            }
            let conds: Vec<std::string::String> = needles
                .iter()
                .map(|n| {
                    // an address is delimited by non-alphanumeric, non-dot characters
                    let body = crate::regex::escape(n);
                    let tail = if prefix && n.ends_with('.') { "[0-9]".to_string() } else if prefix { "([.][0-9]|[^0-9A-Za-z.]|$)".to_string() } else { "([^0-9A-Za-z.]|[.][^0-9]|[.]$|$)".to_string() };
                    format!("regexp_matches({s}, {})", quote_str(&format!("(^|[^0-9A-Za-z.]){body}{tail}")))
                })
                .collect();
            Ok(mk(format!("({})", conds.join(" OR ")), Bool, a))
        }
        // ------------------------------------------------------------------ base64
        "base64_encode_fromguid" => {
            need(name, a, 1, 1)?;
            let h = format!("replace(CAST({} AS VARCHAR), '-', '')", a[0].sql);
            let b = |i: usize| format!("substr({h}, {}, 2)", i * 2 + 1);
            // .NET Guid byte order: the first three groups are little-endian
            let order = [3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15];
            let hex: Vec<std::string::String> = order.iter().map(|&i| b(i)).collect();
            Ok(mk(format!("COALESCE(to_base64(unhex({})), '')", hex.join(" || ")), String, a))
        }
        "base64_decode_toguid" => {
            need(name, a, 1, 1)?;
            let s = str_arg(ctx, &a[0]);
            let h = format!("lower(hex(TRY(from_base64({s}))))");
            let b = |i: usize| format!("substr({h}, {}, 2)", i * 2 + 1);
            let g = format!(
                "{} || {} || {} || {} || '-' || {} || {} || '-' || {} || {} || '-' || {} || {} || '-' || {} || {} || {} || {} || {} || {}",
                b(3),
                b(2),
                b(1),
                b(0),
                b(5),
                b(4),
                b(7),
                b(6),
                b(8),
                b(9),
                b(10),
                b(11),
                b(12),
                b(13),
                b(14),
                b(15)
            );
            Ok(mk(format!("CASE WHEN length({h}) = 32 THEN TRY_CAST({g} AS UUID) END"), Guid, a))
        }
        "base64_encode_fromarray" => {
            need(name, a, 1, 1)?;
            let x = dyn_arg(ctx, &a[0]);
            let e = ctx.alias();
            Ok(mk(
                format!("COALESCE(to_base64(unhex(array_to_string(list_transform({}, {e} -> lpad(printf('%x', CAST(json_extract_string({e}, '$') AS INTEGER) & 255), 2, '0')), ''))), '')", jlist(&x)),
                String,
                a,
            ))
        }
        "base64_decode_toarray" => {
            need(name, a, 1, 1)?;
            let s = str_arg(ctx, &a[0]);
            let h = format!("hex(TRY(from_base64({s})))");
            let i = ctx.alias();
            Ok(mk(format!("CASE WHEN {h} IS NOT NULL THEN to_json(list_transform(range(length({h}) // 2), {i} -> CAST(('0x' || substr({h}, {i} * 2 + 1, 2)) AS BIGINT))) END"), Dynamic, a))
        }
        "assert" => {
            need(name, a, 2, 2)?;
            let c = ctx.to_bool(a[0].clone());
            let msg = str_arg(ctx, &a[1]);
            Ok(mk(format!("CASE WHEN {} THEN true ELSE error('assert failed: ' || {msg}) END", c.sql), Bool, a))
        }
        _ => err(format!("function '{name}' is not supported yet")),
    }
}

/// A recursive key-sorting rewrite of a JSON value (`depth` levels), for `dynamic_to_json`.
// ---------------------------------------------------------------------- PostgreSQL versions

/// `x` when it is a jsonb array, `[]` otherwise.
fn pg_arr(x: &str) -> String {
    format!("(CASE WHEN jsonb_typeof({x}) = 'array' THEN {x} ELSE '[]'::jsonb END)")
}

/// `x` when it is a jsonb object, `{}` otherwise.
fn pg_obj(x: &str) -> String {
    format!("(CASE WHEN jsonb_typeof({x}) = 'object' THEN {x} ELSE '{{}}'::jsonb END)")
}

/// `jsonb_array_elements` of `x` (non-arrays: no rows) as `al(__kql_v, __kql_i)`, `__kql_i` 1-based.
fn pg_elems(x: &str, al: &str) -> String {
    format!("jsonb_array_elements({}) WITH ORDINALITY AS {al}(__kql_v, __kql_i)", pg_arr(x))
}

/// Compact JSON text with object keys sorted (Kusto's `dynamic_to_json`), `depth` levels deep.
fn pg_json_text(x: &str, depth: usize) -> String {
    if depth == 0 {
        return format!("CAST({x} AS text)");
    }
    let (k, e) = (format!("_k{depth}"), format!("_a{depth}"));
    let v = pg_json_text(&format!("{k}.value"), depth - 1);
    let el = pg_json_text(&format!("{e}.__kql_v"), depth - 1);
    format!(
        "(CASE jsonb_typeof({x}) WHEN 'object' THEN '{{' || COALESCE((SELECT string_agg(CAST(to_jsonb({k}.key) AS text) || ':' || {v}, ',' ORDER BY {k}.key COLLATE \"C\") FROM jsonb_each({x}) AS {k}), '') || '}}' \
         WHEN 'array' THEN '[' || COALESCE((SELECT string_agg({el}, ',' ORDER BY {e}.__kql_i) FROM {}), '') || ']' ELSE CAST({x} AS text) END)",
        pg_elems(x, &e)
    )
}

/// PostgreSQL (jsonb) implementations of the dynamic helpers that DuckDB builds from lists.
fn pg(ctx: &mut Ctx, name: &str, a: &[TExpr]) -> Option<Result<TExpr>> {
    use KqlType::*;
    let r = (|| -> Result<TExpr> {
        let d = ctx.d;
        let arr_len = |x: &str| format!("jsonb_array_length({})", pg_arr(x));
        let agg = |v: &str, ord: &str| format!("COALESCE(jsonb_agg({v} ORDER BY {ord}), '[]'::jsonb)");
        Ok(match name {
            "bag_merge" => {
                need(name, a, 2, 64)?;
                // jsonb || lets the right operand win: concatenate in reverse so the first bag wins
                let parts: Vec<std::string::String> = a.iter().rev().map(|t| pg_obj(&dyn_arg(ctx, t))).collect();
                mk(format!("({})", parts.join(" || ")), Dynamic, a)
            }
            "bag_remove_keys" => {
                need(name, a, 2, 2)?;
                let (bag, keys) = (dyn_arg(ctx, &a[0]), dyn_arg(ctx, &a[1]));
                mk(format!("CASE WHEN jsonb_typeof({bag}) = 'object' THEN {bag} - ARRAY(SELECT jsonb_array_elements_text({})) END", pg_arr(&keys)), Dynamic, a)
            }
            "bag_set_key" => {
                need(name, a, 3, 3)?;
                let (bag, key, val) = (dyn_arg(ctx, &a[0]), str_arg(ctx, &a[1]), dyn_arg(ctx, &a[2]));
                mk(format!("CASE WHEN jsonb_typeof({bag}) = 'object' THEN {bag} || jsonb_build_object({key}, {val}) END"), Dynamic, a)
            }
            "bag_zip" => {
                need(name, a, 2, 2)?;
                let (ks, vs) = (dyn_arg(ctx, &a[0]), dyn_arg(ctx, &a[1]));
                mk(
                    format!(
                        "CASE WHEN jsonb_typeof({ks}) = 'array' AND jsonb_typeof({vs}) = 'array' THEN (SELECT COALESCE(jsonb_object_agg(_z.__kql_v #>> '{{}}', {vs} -> CAST(_z.__kql_i - 1 AS integer)), '{{}}'::jsonb) FROM {} WHERE jsonb_typeof(_z.__kql_v) = 'string') END",
                        pg_elems(&ks, "_z")
                    ),
                    Dynamic,
                    a,
                )
            }
            "dynamic_to_json" => {
                need(name, a, 1, 1)?;
                let x = &a[0];
                if let Some(Const::Dynamic(v)) = &x.konst {
                    let s = v.to_string();
                    return Ok(TExpr::konst(quote_str(&s), String, Const::Str(s)));
                }
                if x.is_null_const() {
                    return Ok(TExpr::konst("'null'", String, Const::Str("null".into())));
                }
                let dj = dyn_arg(ctx, x);
                mk(format!("COALESCE({}, 'null')", pg_json_text(&dj, 4)), String, a)
            }
            "array_rotate_left" | "array_rotate_right" | "array_shift_left" | "array_shift_right" => {
                let shift = name.starts_with("array_shift");
                need(name, a, 2, if shift { 3 } else { 2 })?;
                let arr = dyn_arg(ctx, &a[0]);
                let n = long_arg(ctx, &a[1]);
                let n = if name.ends_with("right") { format!("(-{n})") } else { n };
                let len = arr_len(&arr);
                let elem = if shift {
                    let fill = a.get(2).map(|f| dyn_arg(ctx, f)).unwrap_or_else(|| "CAST(NULL AS jsonb)".into());
                    format!("CASE WHEN _g.__kql_i + {n} >= 0 AND _g.__kql_i + {n} < {len} THEN {arr} -> CAST(_g.__kql_i + {n} AS integer) ELSE {fill} END")
                } else {
                    format!("{arr} -> CAST((((_g.__kql_i + {n}) % {len}) + {len}) % {len} AS integer)")
                };
                mk(
                    format!("CASE WHEN jsonb_typeof({arr}) = 'array' THEN (SELECT {} FROM generate_series(0, {len} - 1) AS _g(__kql_i)) END", agg(&elem, "_g.__kql_i")),
                    Dynamic,
                    a,
                )
            }
            "array_split" => {
                need(name, a, 2, 2)?;
                let arr = dyn_arg(ctx, &a[0]);
                let idx = if a[1].ty == Dynamic {
                    format!("ARRAY(SELECT {} FROM jsonb_array_elements_text({}) AS _t(x))", d.try_cast("_t.x", Long), pg_arr(&a[1].sql))
                } else {
                    format!("ARRAY[{}]", long_arg(ctx, &a[1]))
                };
                let len = arr_len(&arr);
                let norm = format!("least(greatest(CASE WHEN _j.x < 0 THEN _j.x + {len} ELSE _j.x END, 0), {len})");
                let bounds = format!(
                    "SELECT CAST(0 AS bigint) AS k, CAST(0 AS bigint) AS b UNION ALL SELECT _j.o, {norm} FROM unnest({idx}) WITH ORDINALITY AS _j(x, o) UNION ALL SELECT 9223372036854775807, {len}"
                );
                let piece = format!("(SELECT {} FROM {} WHERE _e.__kql_i - 1 >= _b.lo AND _e.__kql_i - 1 < _b.hi)", agg("_e.__kql_v", "_e.__kql_i"), pg_elems(&arr, "_e"));
                mk(
                    format!(
                        "CASE WHEN jsonb_typeof({arr}) = 'array' THEN (SELECT {} FROM (SELECT _c.k, _c.b AS lo, lead(_c.b) OVER (ORDER BY _c.k) AS hi FROM ({bounds}) AS _c) AS _b WHERE _b.hi IS NOT NULL) END",
                        agg(&piece, "_b.k")
                    ),
                    Dynamic,
                    a,
                )
            }
            "array_iff" | "array_iif" => {
                need(name, a, 3, 3)?;
                let cond = dyn_arg(ctx, &a[0]);
                let branch: Vec<std::string::String> = a[1..3]
                    .iter()
                    .map(|t| {
                        if t.ty == Dynamic {
                            // arrays are indexed, scalars broadcast
                            format!("CASE WHEN jsonb_typeof({0}) = 'array' THEN {0} -> CAST(_c.__kql_i - 1 AS integer) ELSE {0} END", t.sql)
                        } else {
                            dyn_arg(ctx, t)
                        }
                    })
                    .collect();
                let truth = "CASE WHEN jsonb_typeof(_c.__kql_v) = 'boolean' THEN _c.__kql_v = 'true'::jsonb WHEN jsonb_typeof(_c.__kql_v) = 'number' THEN CAST(_c.__kql_v #>> '{}' AS double precision) <> 0 END";
                let elem = format!("CASE WHEN {truth} THEN {} WHEN NOT {truth} THEN {} END", branch[0], branch[1]);
                mk(format!("CASE WHEN jsonb_typeof({cond}) = 'array' THEN (SELECT {} FROM {}) END", agg(&elem, "_c.__kql_i"), pg_elems(&cond, "_c")), Dynamic, a)
            }
            "array_strcat" => {
                need(name, a, 2, 2)?;
                let (arr, delim) = (dyn_arg(ctx, &a[0]), str_arg(ctx, &a[1]));
                mk(format!("COALESCE((SELECT string_agg(COALESCE(_e.__kql_v #>> '{{}}', ''), {delim} ORDER BY _e.__kql_i) FROM {}), '')", pg_elems(&arr, "_e")), String, a)
            }
            "jaccard_index" => {
                need(name, a, 2, 2)?;
                let (x, y) = (dyn_arg(ctx, &a[0]), dyn_arg(ctx, &a[1]));
                let nan = d.real_literal(f64::NAN);
                mk(
                    format!(
                        "(SELECT CASE WHEN count(*) = 0 THEN {nan} ELSE CAST(count(*) FILTER (WHERE _u.inx AND _u.iny) AS double precision) / count(*) END \
                         FROM (SELECT _s.v, bool_or(_s.src = 1) AS inx, bool_or(_s.src = 2) AS iny FROM (SELECT _e.__kql_v AS v, 1 AS src FROM {} UNION ALL SELECT _f.__kql_v, 2 FROM {}) AS _s GROUP BY _s.v) AS _u)",
                        pg_elems(&x, "_e"),
                        pg_elems(&y, "_f")
                    ),
                    Real,
                    a,
                )
            }
            "repeat" => {
                need(name, a, 2, 2)?;
                if a[0].ty == Dynamic {
                    return err("repeat(): the value must be a scalar");
                }
                let (v, n) = (dyn_arg(ctx, &a[0]), long_arg(ctx, &a[1]));
                mk(format!("CASE WHEN {n} IS NOT NULL THEN (SELECT COALESCE(jsonb_agg({v}), '[]'::jsonb) FROM generate_series(1, {n}) AS _g(__kql_i)) END"), Dynamic, a)
            }
            "range" => {
                need(name, a, 2, 3)?;
                let (start, stop) = (&a[0], &a[1]);
                let i = "_g.__kql_i";
                // (number of elements - 1, element), capped like Kusto (1,048,576 elements)
                let (zero, n, elem) = match start.ty {
                    DateTime => {
                        if a.get(2).is_some_and(|t| t.ty != TimeSpan) {
                            return err("range(): a datetime range requires a timespan step");
                        }
                        let step = a.get(2).map(|t| t.sql.clone()).unwrap_or_else(|| "864000000000".into());
                        let (f, t) = (d.epoch_us(&start.sql), d.epoch_us(&ctx.convert(stop.clone(), DateTime).sql));
                        let st = format!("CAST(trunc(({step}) / 10) AS bigint)");
                        let n = format!("least(CAST(floor(({t} - {f}) / {}) AS bigint), 1048575)", d.cast(&format!("NULLIF({st}, 0)"), Real));
                        (format!("{st} = 0"), n, d.to_json(&ctx.datetime_to_string(&d.ts_from_us(&format!("({f} + {i} * {st})")))))
                    }
                    TimeSpan => {
                        let step = a.get(2).map(|t| t.sql.clone()).unwrap_or_else(|| "1".into());
                        let n = format!("least(CAST(floor(({} - {}) / {}) AS bigint), 1048575)", stop.sql, start.sql, d.cast(&format!("NULLIF({step}, 0)"), Real));
                        (format!("{step} = 0"), n, d.to_json(&ctx.timespan_to_string(&format!("({} + {i} * {step})", start.sql))))
                    }
                    t if t.is_numeric() || t == Dynamic => {
                        let step_ty = a.get(2).map(|s| s.ty).unwrap_or(Long);
                        if start.ty.is_integer() && (stop.ty.is_integer() || stop.ty == Dynamic) && step_ty.is_integer() {
                            let (f, t) = (long_arg(ctx, start), long_arg(ctx, stop));
                            let s = a.get(2).map(|x| long_arg(ctx, x)).unwrap_or_else(|| "1".into());
                            (format!("{s} = 0"), format!("least(({t} - {f}) / NULLIF({s}, 0), 1048575)"), format!("to_jsonb({f} + {i} * {s})"))
                        } else {
                            let real = |ctx: &Ctx, x: &TExpr| ctx.convert(x.clone(), Real).sql;
                            let (f, t) = (real(ctx, start), real(ctx, stop));
                            let s = a.get(2).map(|x| real(ctx, x)).unwrap_or_else(|| d.real_literal(1.0));
                            (format!("{s} = 0"), format!("least(CAST(floor(({t} - {f}) / NULLIF({s}, 0)) AS bigint), 1048575)"), format!("to_jsonb({f} + {i} * {s})"))
                        }
                    }
                    _ => return err("range(): unsupported argument types"),
                };
                mk(
                    format!("CASE WHEN {zero} OR {n} < 0 THEN '[]'::jsonb ELSE (SELECT {} FROM generate_series(0, {n}) AS _g(__kql_i)) END", agg(&elem, i)),
                    Dynamic,
                    a,
                )
            }
            _ => return err("__unsupported__"),
        })
    })();
    match r {
        Err(e) if e.to_string().contains("__unsupported__") => None,
        r => Some(r),
    }
}

fn canonical_json(ctx: &mut Ctx, x: &str, depth: usize) -> String {
    if depth == 0 {
        return x.to_string();
    }
    let k = ctx.alias();
    let e = ctx.alias();
    let v = ctx.d.json_get_key(x, &k);
    let inner_v = canonical_json(ctx, &v, depth - 1);
    let inner_e = canonical_json(ctx, &e, depth - 1);
    format!(
        "(CASE WHEN json_type({x}) = 'OBJECT' THEN to_json(map_from_entries(list_transform(list_sort(json_keys({x})), {k} -> {{'k': {k}, 'v': CAST({inner_v} AS JSON)}}))) \
         WHEN json_type({x}) = 'ARRAY' THEN to_json(list_transform(CAST({x} AS JSON[]), {e} -> CAST({inner_e} AS JSON))) ELSE {x} END)"
    )
}

/// Kusto's `treepath` of a constant: every path (`['key']`, `[0]` for any array element), in
/// first-appearance order.
fn tree_paths(v: &serde_json::Value, prefix: &str, out: &mut Vec<String>) {
    match v {
        serde_json::Value::Object(m) => {
            for (k, child) in m {
                let p = format!("{prefix}['{k}']");
                if !out.contains(&p) {
                    out.push(p.clone());
                }
                tree_paths(child, &p, out);
            }
        }
        serde_json::Value::Array(items) => {
            let p = format!("{prefix}[0]");
            if !items.is_empty() && !out.contains(&p) {
                out.push(p.clone());
            }
            for child in items {
                tree_paths(child, &p, out);
            }
        }
        _ => {}
    }
}

/// `{"k": "v", ...}` from a URL query string (values URL-decoded; later duplicates ignored).
fn query_bag(ctx: &mut Ctx, q: &str) -> String {
    let p = ctx.alias();
    let list = format!(
        "list_transform(list_filter(string_split({q}, '&'), {p} -> {p} <> ''), {p} -> {{'k': COALESCE(TRY(url_decode(replace(split_part({p}, '=', 1), '+', ' '))), ''), 'v': to_json(COALESCE(TRY(url_decode(replace(substr({p}, length(split_part({p}, '=', 1)) + 2), '+', ' '))), ''))}})"
    );
    format!("CASE WHEN {q} = '' THEN CAST('{{}}' AS JSON) ELSE {} END", bag_from_entries(ctx, &list))
}

/// `a.b.c.d[/n]` parsed into `{v: address, p: prefix length}`, or NULL if invalid.
fn ipv4(s: &str) -> String {
    let re = quote_str(r"^\s*([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\s*(?:/\s*([0-9]{1,2})\s*)?$");
    format!(
        "(CASE WHEN regexp_matches({s}, {re}) THEN (list_transform([regexp_extract({s}, {re}, ['a', 'b', 'c', 'd', 'p'])], m -> \
         CASE WHEN CAST(m.a AS BIGINT) <= 255 AND CAST(m.b AS BIGINT) <= 255 AND CAST(m.c AS BIGINT) <= 255 AND CAST(m.d AS BIGINT) <= 255 \
         AND COALESCE(TRY_CAST(NULLIF(m.p, '') AS BIGINT), 32) <= 32 THEN \
         {{'v': CAST(m.a AS BIGINT) * 16777216 + CAST(m.b AS BIGINT) * 65536 + CAST(m.c AS BIGINT) * 256 + CAST(m.d AS BIGINT), 'p': COALESCE(TRY_CAST(NULLIF(m.p, '') AS BIGINT), 32)}} END)[1]) END)"
    )
}

/// An address masked to its first `p` bits.
fn masked(v: &str, p: &str) -> String {
    let unit = format!("(CAST(1 AS BIGINT) << CAST(32 - ({p}) AS INTEGER))");
    format!("((({v}) // {unit}) * {unit})")
}

/// `ip` lies inside the CIDR `range` (`a.b.c.d/n`).
fn ipv4_in_range(ctx: &mut Ctx, ip: &str, range: &str) -> String {
    let (xs, rs) = (ipv4(ip), ipv4(range));
    bind(ctx, &xs, |ctx, x| {
        bind(ctx, &rs, |_, r| {
            format!("CASE WHEN {x} IS NULL OR {r} IS NULL THEN NULL ELSE ({} = {} AND {x}.p >= {r}.p) END", masked(&format!("{x}.v"), &format!("{r}.p")), masked(&format!("{r}.v"), &format!("{r}.p")))
        })
    })
}

fn format_bytes(ctx: &mut Ctx, a: &[TExpr]) -> Result<TExpr> {
    need("format_bytes", a, 1, 4)?;
    let v = ctx.d.cast(&a[0].sql, KqlType::Real);
    let prec = a.get(1).map(|t| long_arg(ctx, t)).unwrap_or_else(|| "0".into());
    let base: f64 = match a.get(3).and_then(|t| t.long_const()) {
        Some(10) => 1000.0,
        Some(2) | None => 1024.0,
        Some(b) => return err(format!("format_bytes(): unsupported base {b}")),
    };
    let units = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB"];
    // the unit index: explicit, or the largest unit not exceeding the value
    let idx = match a.get(2).and_then(|t| t.str_const()) {
        Some(u) => {
            let Some(i) = units.iter().position(|x| x.eq_ignore_ascii_case(u)) else {
                return err(format!("format_bytes(): unknown unit '{u}'"));
            };
            i.to_string()
        }
        None if a.get(2).is_some() && a[2].str_const().is_none() => return err("format_bytes(): the unit must be a constant"),
        None => format!("CAST(least(greatest(floor(log(abs({v})) / log({base})), 0), 6) AS INTEGER)"),
    };
    let idx = format!("(CASE WHEN {v} = 0 THEN 0 ELSE {idx} END)");
    let scaled = format!("round({v} / power({base}, {idx}), CAST({prec} AS INTEGER))");
    let num = ctx.real_to_string(&scaled);
    let num = format!("regexp_replace({num}, '\\.0$', '')");
    let unit = format!("[{}][{idx} + 1]", units.iter().map(|u| quote_str(u)).collect::<Vec<_>>().join(", "));
    Ok(mk(format!("COALESCE({num} || ' ' || {unit}, '')"), KqlType::String, a))
}

/// Scalar `range(start, stop [, step])` → dynamic array.
fn range(ctx: &mut Ctx, a: &[TExpr]) -> Result<TExpr> {
    use KqlType::*;
    need("range", a, 2, 3)?;
    let d = ctx.d;
    let (start, stop) = (&a[0], &a[1]);
    let i = ctx.alias();
    // number of elements - 1, capped like Kusto (1,048,576 elements)
    let count = |from: &str, to: &str, step: &str| format!("least(CAST(floor(({to} - {from}) / {step}) AS BIGINT), 1048575)");
    match start.ty {
        DateTime => {
            let step = a.get(2).map(|t| t.sql.clone()).unwrap_or_else(|| "864000000000".into());
            if a.get(2).is_some_and(|t| t.ty != TimeSpan) {
                return err("range(): a datetime range requires a timespan step");
            }
            let (f, t) = (d.epoch_us(&start.sql), d.epoch_us(&ctx.convert(stop.clone(), DateTime).sql));
            let st = format!("CAST(trunc(({step}) / 10) AS DOUBLE)");
            let n = count(&f, &t, &st);
            let elem = ctx.datetime_to_string(&d.ts_from_us(&format!("CAST({f} + {i} * {st} AS BIGINT)")));
            Ok(mk(format!("CASE WHEN {st} = 0 OR {n} < 0 THEN CAST('[]' AS JSON) ELSE to_json(list_transform(range({n} + 1), {i} -> {elem})) END"), Dynamic, a))
        }
        TimeSpan => {
            let step = a.get(2).map(|t| t.sql.clone()).unwrap_or_else(|| "1".into());
            let n = count(&start.sql, &stop.sql, &format!("CAST({step} AS DOUBLE)"));
            let elem = ctx.timespan_to_string(&format!("({} + {i} * {step})", start.sql));
            Ok(mk(format!("CASE WHEN {step} = 0 OR {n} < 0 THEN CAST('[]' AS JSON) ELSE to_json(list_transform(range({n} + 1), {i} -> {elem})) END"), Dynamic, a))
        }
        t if t.is_numeric() || t == Dynamic => {
            let step_ty = a.get(2).map(|s| s.ty).unwrap_or(Long);
            let integer = start.ty.is_integer() && (stop.ty.is_integer() || stop.ty == Dynamic) && step_ty.is_integer();
            if integer {
                let (f, t) = (long_arg(ctx, start), long_arg(ctx, stop));
                let s = a.get(2).map(|x| long_arg(ctx, x)).unwrap_or_else(|| "1".into());
                let n = format!("least(({t} - {f}) // {s}, 1048575)");
                Ok(mk(format!("CASE WHEN {s} = 0 OR {n} < 0 THEN CAST('[]' AS JSON) ELSE to_json(list_transform(range({n} + 1), {i} -> {f} + {i} * {s})) END"), Dynamic, a))
            } else {
                let real = |ctx: &Ctx, x: &TExpr| ctx.convert(x.clone(), Real).sql;
                let (f, t) = (real(ctx, start), real(ctx, stop));
                let s = a.get(2).map(|x| real(ctx, x)).unwrap_or_else(|| "1.0".into());
                let n = count(&f, &t, &s);
                Ok(mk(format!("CASE WHEN {s} = 0 OR {n} < 0 THEN CAST('[]' AS JSON) ELSE to_json(list_transform(range({n} + 1), {i} -> {f} + {i} * {s})) END"), Dynamic, a))
            }
        }
        t => err(format!("range(): unsupported argument type {t}")),
    }
}
