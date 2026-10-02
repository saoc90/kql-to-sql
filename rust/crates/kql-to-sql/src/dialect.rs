//! SQL dialect primitives.
//!
//! The translator never writes engine-specific SQL directly; it asks the dialect for each
//! primitive. Representation choices shared by all dialects:
//!
//! * `datetime` is a TIMESTAMP (UTC, microsecond precision);
//! * `timespan` is a BIGINT number of ticks (100ns) while a query runs, converted to INTERVAL only
//!   in the final output, so timespan arithmetic is plain integer arithmetic;
//! * `dynamic` is JSON (DuckDB) / JSONB (PostgreSQL) everywhere, never a native list;
//! * `string` is never NULL (Kusto has no null strings), so sources of NULL strings are coalesced.

use crate::sql::quote_str;
use crate::{Dialect, KqlType};

pub(crate) trait SqlDialect: Sync {
    fn kind(&self) -> Dialect;

    fn sql_type(&self, t: KqlType) -> &'static str;

    fn cast(&self, sql: &str, t: KqlType) -> String {
        format!("CAST({sql} AS {})", self.sql_type(t))
    }

    /// A cast that yields NULL instead of failing.
    fn try_cast(&self, sql: &str, t: KqlType) -> String;

    fn real_literal(&self, v: f64) -> String;

    // ----- datetime / timespan
    /// TIMESTAMP → BIGINT microseconds since the epoch.
    fn epoch_us(&self, ts: &str) -> String;
    /// BIGINT microseconds since the epoch → TIMESTAMP.
    fn ts_from_us(&self, us: &str) -> String;
    fn timestamp_literal(&self, iso: &str) -> String {
        format!("TIMESTAMP {}", quote_str(iso))
    }
    fn now(&self) -> String;
    /// Ticks (BIGINT) → INTERVAL, for final output.
    fn interval_from_ticks(&self, ticks: &str) -> String;
    /// INTERVAL → ticks (for tables that store INTERVAL columns).
    fn ticks_from_interval(&self, iv: &str) -> String;
    fn date_trunc(&self, unit: &str, ts: &str) -> String {
        format!("date_trunc('{unit}', {ts})")
    }
    /// `EXTRACT(part FROM ts)` as a BIGINT/DOUBLE expression.
    fn extract(&self, part: &str, ts: &str) -> String {
        format!("EXTRACT({part} FROM {ts})")
    }
    /// Formats a TIMESTAMP with a strftime/to_char style pattern built by the caller.
    fn format_timestamp_iso(&self, ts: &str) -> String;

    // ----- strings
    fn regex_match(&self, s: &str, re: &str) -> String;
    fn regex_extract(&self, s: &str, re: &str, group: u32) -> String;
    fn regex_escape(&self, s: &str) -> String;
    fn starts_with(&self, s: &str, prefix: &str) -> String {
        format!("starts_with({s}, {prefix})")
    }
    fn ends_with(&self, s: &str, suffix: &str) -> String;
    fn strpos(&self, s: &str, needle: &str) -> String {
        format!("strpos({s}, {needle})")
    }
    fn string_agg(&self, s: &str, sep: &str) -> String {
        format!("string_agg({s}, {sep})")
    }

    // ----- dynamic / JSON
    /// A JSON value from its text.
    fn json_literal(&self, json_text: &str) -> String;
    /// Navigate into JSON by object key; result is JSON.
    fn json_get_key(&self, json: &str, key_sql: &str) -> String;
    /// Navigate into a JSON array by (possibly negative) index; result is JSON.
    fn json_get_index(&self, json: &str, index_sql: &str) -> String;
    /// `json_get_key` with a constant key.
    fn json_get_key_lit(&self, json: &str, key: &str) -> String {
        self.json_get_key(json, &quote_str(key))
    }
    /// `json_get_index` with a constant index.
    fn json_get_index_lit(&self, json: &str, index: i64) -> String {
        self.json_get_index(json, &index.to_string())
    }
    /// JSON scalar → text without quotes; objects/arrays → JSON text. NULL stays NULL.
    fn json_to_text(&self, json: &str) -> String;
    /// Any SQL value → JSON.
    fn to_json(&self, sql: &str) -> String;
    /// JSON type name: 'object', 'array', 'string', 'number'/'integer'/'double', 'boolean', 'null'.
    fn json_type(&self, json: &str) -> String;
    fn json_array_length(&self, json: &str) -> String;
    /// Aggregate values into a JSON array (NULLs skipped).
    fn json_agg(&self, value: &str, distinct: bool) -> String;
    /// Builds a JSON array from SQL values.
    fn json_array(&self, items: &[String]) -> String;
    /// Builds a JSON object from (key SQL, value SQL) pairs.
    fn json_object(&self, pairs: &[(String, String)]) -> String;
    /// Table function expanding a JSON array/object into rows with columns `(idx, key, value)`.
    /// Returns a FROM item (e.g. `json_each(x) AS alias`) and the column expressions.
    fn json_each(&self, json: &str, alias: &str) -> JsonEach;

    // ----- numeric
    fn is_nan(&self, x: &str) -> String {
        format!("isnan({x})")
    }
    fn is_inf(&self, x: &str) -> String {
        format!("isinf({x})")
    }
    fn is_finite(&self, x: &str) -> String {
        format!("isfinite({x})")
    }
    /// Floored modulo on doubles (sign of the divisor, like DuckDB's fmod and Kusto); x % 0 is NaN.
    fn fmod(&self, a: &str, b: &str) -> String {
        format!("fmod({a}, {b})")
    }
    fn log2(&self, x: &str) -> String {
        format!("log2({x})")
    }
    /// IEEE `exp`: overflow is +inf, underflow 0 (never an error).
    fn exp(&self, x: &str) -> String {
        format!("exp({x})")
    }
    /// IEEE `pow`: a negative base with a fractional exponent is NaN (never an error).
    fn power(&self, x: &str, y: &str) -> String {
        format!("power({x}, {y})")
    }
    /// IEEE double division: x/0 is ±inf, 0/0 is NaN (never an error).
    fn real_div(&self, a: &str, b: &str) -> String {
        format!("({a} / {b})")
    }
    /// `substr(s, start[, len])` with BIGINT positions (1-based).
    fn substr(&self, s: &str, start: &str, len: Option<&str>) -> String {
        match len {
            Some(l) => format!("substr({s}, {start}, {l})"),
            None => format!("substr({s}, {start})"),
        }
    }
    /// Null-matching equality used for join keys (NULL matches NULL).
    fn null_safe_eq(&self, l: &str, r: &str, _ty: KqlType) -> String {
        format!("{l} IS NOT DISTINCT FROM {r}")
    }

    // ----- misc
    fn generate_series(&self, from: &str, to: &str, step: &str, alias: &str, col: &str) -> String;
    fn random(&self) -> String {
        "random()".to_string()
    }
    fn supports_ignore_nulls(&self) -> bool {
        true
    }
}

/// Column expressions produced by [`SqlDialect::json_each`].
pub(crate) struct JsonEach {
    pub from_item: String,
    /// Zero-based position (arrays) as BIGINT.
    pub index: String,
    /// Key text (objects) or NULL.
    pub key: String,
    /// Element as JSON.
    pub value: String,
}

pub(crate) struct DuckDb;
pub(crate) struct Postgres;

pub(crate) fn get(d: Dialect) -> &'static dyn SqlDialect {
    match d {
        Dialect::DuckDb => &DuckDb,
        Dialect::Postgres => &Postgres,
    }
}

/// PostgreSQL (jsonb) implementations of Kusto's dynamic-array functions; the DuckDB versions use
/// native lists. `args` are SQL expressions already converted as the function expects (arrays as
/// jsonb, `array_index_of`'s value as jsonb, `strcat_array`'s delimiter as text, slice bounds as
/// bigint). Returns `None` for functions not handled here.
pub(crate) fn pg_array_fn(name: &str, args: &[String]) -> Option<String> {
    let arr = |x: &str| format!("CASE WHEN jsonb_typeof({x}) = 'array' THEN {x} ELSE '[]'::jsonb END");
    let elems = |x: &str, al: &str| format!("jsonb_array_elements({}) WITH ORDINALITY AS {al}(__kql_v, __kql_i)", arr(x));
    let is_arr = |x: &str| format!("jsonb_typeof({x}) = 'array'");
    let agg = |v: &str, ord: &str| format!("COALESCE(jsonb_agg({v} ORDER BY {ord}), '[]'::jsonb)");
    let a0 = args.first()?;
    Some(match name {
        "array_slice" => {
            let len = format!("jsonb_array_length({})", arr(a0));
            let norm = |i: &str| format!("(CASE WHEN {i} < 0 THEN {i} + {len} ELSE {i} END)");
            format!(
                "CASE WHEN {} THEN (SELECT {} FROM {} WHERE _e.__kql_i - 1 >= {} AND _e.__kql_i - 1 <= {}) END",
                is_arr(a0),
                agg("_e.__kql_v", "_e.__kql_i"),
                elems(a0, "_e"),
                norm(&args[1]),
                norm(&args[2])
            )
        }
        "array_reverse" => format!("CASE WHEN {} THEN (SELECT {} FROM {}) END", is_arr(a0), agg("_e.__kql_v", "_e.__kql_i DESC"), elems(a0, "_e")),
        "array_index_of" => format!(
            "CASE WHEN {a0} IS NULL THEN NULL ELSE COALESCE((SELECT min(_e.__kql_i) FROM {} WHERE _e.__kql_v = {}), 0) - 1 END",
            elems(a0, "_e"),
            args[1]
        ),
        "set_has_element" => format!("EXISTS (SELECT 1 FROM {} WHERE _e.__kql_v = {})", elems(a0, "_e"), args[1]),
        "array_sum" => format!(
            "CASE WHEN {} THEN (SELECT sum(CAST(_e.__kql_v #>> '{{}}' AS double precision)) FILTER (WHERE jsonb_typeof(_e.__kql_v) = 'number') FROM {}) END",
            is_arr(a0),
            elems(a0, "_e")
        ),
        "array_sort_asc" | "array_sort_desc" => {
            let dir = if name.ends_with("asc") { "ASC" } else { "DESC" };
            // Kusto puts nulls last in both directions
            format!("CASE WHEN {} THEN (SELECT {} FROM {}) END", is_arr(a0), agg("_e.__kql_v", &format!("jsonb_typeof(_e.__kql_v) = 'null', _e.__kql_v {dir}")), elems(a0, "_e"))
        }
        "zip" => {
            let lens: Vec<String> = args.iter().map(|x| format!("jsonb_array_length({})", arr(x))).collect();
            let items: Vec<String> = args.iter().map(|x| format!("({x} -> CAST(_g.__kql_i AS integer))")).collect();
            format!(
                "(SELECT {} FROM generate_series(0, greatest({}) - 1) AS _g(__kql_i))",
                agg(&format!("jsonb_build_array({})", items.join(", ")), "_g.__kql_i"),
                lens.join(", ")
            )
        }
        "strcat_array" => format!(
            "COALESCE((SELECT string_agg(COALESCE(_e.__kql_v #>> '{{}}', ''), {} ORDER BY _e.__kql_i) FROM {}), '')",
            args[1],
            elems(a0, "_e")
        ),
        "set_union" | "set_intersect" | "set_difference" => {
            let src = if name == "set_union" { args.iter().map(|x| format!("({})", arr(x))).collect::<Vec<_>>().join(" || ") } else { a0.clone() };
            let conds: Vec<String> = match name {
                "set_intersect" => args[1..]
                    .iter()
                    .enumerate()
                    .map(|(k, x)| format!("EXISTS (SELECT 1 FROM {} WHERE _f{k}.__kql_v = _e.__kql_v)", elems(x, &format!("_f{k}"))))
                    .collect(),
                "set_difference" => {
                    let rest = args[1..].iter().map(|x| format!("({})", arr(x))).collect::<Vec<_>>().join(" || ");
                    vec![format!("NOT EXISTS (SELECT 1 FROM {} WHERE _f.__kql_v = _e.__kql_v)", elems(&format!("({rest})"), "_f"))]
                }
                _ => vec![],
            };
            let wh = if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) };
            format!("(SELECT {} FROM (SELECT _e.__kql_v, min(_e.__kql_i) AS __kql_i FROM {}{wh} GROUP BY _e.__kql_v) AS _u)", agg("_u.__kql_v", "_u.__kql_i"), elems(&format!("({src})"), "_e"))
        }
        _ => return None,
    })
}

/// `'...'` — a single SQL string literal (an untyped `unknown` in PostgreSQL).
fn is_string_literal(sql: &str) -> bool {
    let b = sql.as_bytes();
    if b.len() < 2 || b[0] != b'\'' || b[b.len() - 1] != b'\'' {
        return false;
    }
    // every inner quote must be doubled
    !sql[1..sql.len() - 1].replace("''", "").contains('\'')
}

fn real_text(v: f64) -> Option<String> {
    if v.is_nan() {
        None
    } else if v.is_infinite() {
        None
    } else {
        let s = format!("{v:?}");
        Some(s)
    }
}

impl SqlDialect for DuckDb {
    fn kind(&self) -> Dialect {
        Dialect::DuckDb
    }

    fn sql_type(&self, t: KqlType) -> &'static str {
        match t {
            KqlType::Bool => "BOOLEAN",
            KqlType::Int => "INTEGER",
            KqlType::Long => "BIGINT",
            KqlType::Real => "DOUBLE",
            KqlType::Decimal => "DECIMAL(38, 18)",
            KqlType::String => "VARCHAR",
            KqlType::DateTime => "TIMESTAMP",
            KqlType::TimeSpan => "BIGINT",
            KqlType::Guid => "UUID",
            KqlType::Dynamic => "JSON",
        }
    }

    fn try_cast(&self, sql: &str, t: KqlType) -> String {
        format!("TRY_CAST({sql} AS {})", self.sql_type(t))
    }

    fn real_literal(&self, v: f64) -> String {
        match real_text(v) {
            Some(s) => format!("CAST({s} AS DOUBLE)"),
            None if v.is_nan() => "CAST('nan' AS DOUBLE)".into(),
            None if v > 0.0 => "CAST('inf' AS DOUBLE)".into(),
            None => "CAST('-inf' AS DOUBLE)".into(),
        }
    }

    fn epoch_us(&self, ts: &str) -> String {
        format!("epoch_us({ts})")
    }

    fn ts_from_us(&self, us: &str) -> String {
        format!("make_timestamp({us})")
    }

    fn now(&self) -> String {
        "CAST(now() AS TIMESTAMP)".into()
    }

    fn interval_from_ticks(&self, ticks: &str) -> String {
        format!("to_microseconds(CAST(trunc({ticks} / 10) AS BIGINT))")
    }

    fn ticks_from_interval(&self, iv: &str) -> String {
        format!("(CAST(epoch({iv}) * 1000000 AS BIGINT) * 10)")
    }

    fn format_timestamp_iso(&self, ts: &str) -> String {
        format!("strftime({ts}, '%Y-%m-%dT%H:%M:%S.%fZ')")
    }

    fn regex_match(&self, s: &str, re: &str) -> String {
        format!("regexp_matches({s}, {re})")
    }

    fn regex_extract(&self, s: &str, re: &str, group: u32) -> String {
        format!("regexp_extract({s}, {re}, {group})")
    }

    fn regex_escape(&self, s: &str) -> String {
        format!("regexp_escape({s})")
    }

    fn ends_with(&self, s: &str, suffix: &str) -> String {
        format!("ends_with({s}, {suffix})")
    }

    fn json_literal(&self, json_text: &str) -> String {
        format!("CAST({} AS JSON)", quote_str(json_text))
    }

    fn json_get_key(&self, json: &str, key_sql: &str) -> String {
        format!("json_extract({json}, '$.\"' || replace({key_sql}, '\"', '\\\"') || '\"')")
    }

    fn json_get_key_lit(&self, json: &str, key: &str) -> String {
        let path = format!("$.\"{}\"", key.replace('\\', "\\\\").replace('"', "\\\""));
        format!("json_extract({json}, {})", quote_str(&path))
    }

    fn json_get_index_lit(&self, json: &str, index: i64) -> String {
        let path = if index < 0 { format!("$[#{index}]") } else { format!("$[{index}]") };
        format!("json_extract({json}, '{path}')")
    }

    fn json_get_index(&self, json: &str, index_sql: &str) -> String {
        format!(
            "json_extract({json}, CASE WHEN {index_sql} < 0 THEN '$[#' || CAST({index_sql} AS VARCHAR) || ']' ELSE '$[' || CAST({index_sql} AS VARCHAR) || ']' END)"
        )
    }

    fn json_to_text(&self, json: &str) -> String {
        format!("json_extract_string({json}, '$')")
    }

    fn to_json(&self, sql: &str) -> String {
        format!("to_json({sql})")
    }

    fn json_type(&self, json: &str) -> String {
        // DuckDB: OBJECT, ARRAY, VARCHAR, BIGINT, UBIGINT, DOUBLE, BOOLEAN, NULL
        format!("lower(json_type({json}))")
    }

    fn json_array_length(&self, json: &str) -> String {
        format!("CASE WHEN json_type({json}) = 'ARRAY' THEN CAST(json_array_length({json}) AS BIGINT) END")
    }

    fn json_agg(&self, value: &str, distinct: bool) -> String {
        let d = if distinct { "DISTINCT " } else { "" };
        format!("COALESCE(to_json(list({d}{value}) FILTER (WHERE {value} IS NOT NULL)), CAST('[]' AS JSON))")
    }

    fn json_array(&self, items: &[String]) -> String {
        if items.is_empty() {
            return "CAST('[]' AS JSON)".into();
        }
        format!("json_array({})", items.join(", "))
    }

    fn json_object(&self, pairs: &[(String, String)]) -> String {
        let parts: Vec<String> = pairs.iter().map(|(k, v)| format!("{k}, {v}")).collect();
        format!("json_object({})", parts.join(", "))
    }

    fn json_each(&self, json: &str, alias: &str) -> JsonEach {
        // DuckDB's json_each returns array elements in reverse order, so expand with unnest,
        // which keeps element order.
        JsonEach {
            from_item: format!(
                "(SELECT unnest(range(CAST(json_array_length({json}) AS BIGINT))) AS idx, CAST(NULL AS VARCHAR) AS key, unnest(CAST({json} AS JSON[])) AS value WHERE json_type({json}) = 'ARRAY' \
                 UNION ALL SELECT NULL, unnest(json_keys({json})), unnest(list_transform(json_keys({json}), k -> json_extract({json}, '$.\"' || replace(k, '\"', '\\\"') || '\"'))) WHERE json_type({json}) = 'OBJECT') AS {alias}"
            ),
            index: format!("{alias}.idx"),
            key: format!("{alias}.key"),
            value: format!("{alias}.value"),
        }
    }

    fn generate_series(&self, from: &str, to: &str, step: &str, alias: &str, col: &str) -> String {
        format!("generate_series({from}, {to}, {step}) AS {alias}({col})")
    }
}

impl SqlDialect for Postgres {
    fn kind(&self) -> Dialect {
        Dialect::Postgres
    }

    fn sql_type(&self, t: KqlType) -> &'static str {
        match t {
            KqlType::Bool => "boolean",
            KqlType::Int => "integer",
            KqlType::Long => "bigint",
            KqlType::Real | KqlType::Decimal => "double precision",
            KqlType::String => "text",
            KqlType::DateTime => "timestamp",
            KqlType::TimeSpan => "bigint",
            KqlType::Guid => "uuid",
            KqlType::Dynamic => "jsonb",
        }
    }

    fn try_cast(&self, sql: &str, t: KqlType) -> String {
        // PostgreSQL 16+: pg_input_is_valid
        let ty = self.sql_type(t);
        // concat() is STABLE: it keeps the planner from constant-folding (and failing on) the cast of
        // an invalid constant that the CASE would skip
        format!("(CASE WHEN pg_input_is_valid(CAST({sql} AS text), '{ty}') THEN CAST(concat({sql}) AS {ty}) END)")
    }

    fn real_literal(&self, v: f64) -> String {
        match real_text(v) {
            Some(s) => format!("CAST({s} AS double precision)"),
            None if v.is_nan() => "CAST('NaN' AS double precision)".into(),
            None if v > 0.0 => "CAST('Infinity' AS double precision)".into(),
            None => "CAST('-Infinity' AS double precision)".into(),
        }
    }

    fn epoch_us(&self, ts: &str) -> String {
        format!("CAST(round(EXTRACT(EPOCH FROM {ts}) * 1000000) AS bigint)")
    }

    fn ts_from_us(&self, us: &str) -> String {
        // bigint * interval goes through float8 and loses microseconds far from 1970; the text
        // form of an interval is parsed exactly
        format!("(TIMESTAMP '1970-01-01' + CAST(CAST(CAST({us} AS bigint) AS text) || ' microseconds' AS interval))")
    }

    fn now(&self) -> String {
        "CAST(now() AT TIME ZONE 'UTC' AS timestamp)".into()
    }

    fn interval_from_ticks(&self, ticks: &str) -> String {
        format!("(({ticks}) / 10 * INTERVAL '1 microsecond')")
    }

    fn ticks_from_interval(&self, iv: &str) -> String {
        format!("(CAST(round(EXTRACT(EPOCH FROM {iv}) * 1000000) AS bigint) * 10)")
    }

    fn format_timestamp_iso(&self, ts: &str) -> String {
        format!("to_char({ts}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
    }

    fn regex_match(&self, s: &str, re: &str) -> String {
        format!("({s} ~ {re})")
    }

    fn regex_extract(&self, s: &str, re: &str, group: u32) -> String {
        if group == 0 {
            format!("COALESCE(substring({s} from {re}), '')")
        } else {
            format!("COALESCE((regexp_match({s}, {re}))[{group}], '')")
        }
    }

    fn regex_escape(&self, s: &str) -> String {
        format!("regexp_replace({s}, '([.^$*+?()\\[\\]{{}}|\\\\])', '\\\\\\1', 'g')")
    }

    fn ends_with(&self, s: &str, suffix: &str) -> String {
        format!("(right({s}, length({suffix})) = {suffix})")
    }

    fn json_literal(&self, json_text: &str) -> String {
        format!("CAST({} AS jsonb)", quote_str(json_text))
    }

    fn json_get_key(&self, json: &str, key_sql: &str) -> String {
        format!("({json} -> {key_sql})")
    }

    fn json_get_index(&self, json: &str, index_sql: &str) -> String {
        format!("({json} -> CAST({index_sql} AS integer))")
    }

    fn json_to_text(&self, json: &str) -> String {
        format!("({json} #>> '{{}}')")
    }

    fn to_json(&self, sql: &str) -> String {
        if is_string_literal(sql) {
            // an untyped literal cannot feed a polymorphic function
            return format!("to_jsonb(CAST({sql} AS text))");
        }
        format!("to_jsonb({sql})")
    }

    fn json_type(&self, json: &str) -> String {
        format!("jsonb_typeof({json})")
    }

    fn json_array_length(&self, json: &str) -> String {
        format!("CASE WHEN jsonb_typeof({json}) = 'array' THEN CAST(jsonb_array_length({json}) AS bigint) END")
    }

    fn json_agg(&self, value: &str, distinct: bool) -> String {
        let d = if distinct { "DISTINCT " } else { "" };
        format!("COALESCE(jsonb_agg({d}{value}) FILTER (WHERE {value} IS NOT NULL), CAST('[]' AS jsonb))")
    }

    fn json_array(&self, items: &[String]) -> String {
        format!("jsonb_build_array({})", items.join(", "))
    }

    fn json_object(&self, pairs: &[(String, String)]) -> String {
        let parts: Vec<String> = pairs.iter().map(|(k, v)| format!("{k}, {v}")).collect();
        format!("jsonb_build_object({})", parts.join(", "))
    }

    fn json_each(&self, json: &str, alias: &str) -> JsonEach {
        JsonEach {
            from_item: format!(
                "(SELECT e.ord - 1 AS idx, NULL::text AS key, e.value FROM jsonb_array_elements(CASE WHEN jsonb_typeof({json}) = 'array' THEN {json} ELSE '[]'::jsonb END) WITH ORDINALITY AS e(value, ord) \
                 UNION ALL SELECT NULL, o.key, o.value FROM jsonb_each(CASE WHEN jsonb_typeof({json}) = 'object' THEN {json} ELSE '{{}}'::jsonb END) AS o(key, value)) AS {alias}"
            ),
            index: format!("{alias}.idx"),
            key: format!("{alias}.key"),
            value: format!("{alias}.value"),
        }
    }

    fn generate_series(&self, from: &str, to: &str, step: &str, alias: &str, col: &str) -> String {
        format!("generate_series({from}, {to}, {step}) AS {alias}({col})")
    }

    fn is_nan(&self, x: &str) -> String {
        format!("({x} = CAST('NaN' AS double precision))")
    }

    fn is_inf(&self, x: &str) -> String {
        format!("(abs({x}) = CAST('Infinity' AS double precision))")
    }

    fn is_finite(&self, x: &str) -> String {
        format!("(abs({x}) < CAST('Infinity' AS double precision))")
    }

    fn fmod(&self, a: &str, b: &str) -> String {
        let nan = self.real_literal(f64::NAN);
        let inf = self.real_literal(f64::INFINITY);
        format!(
            "(CASE WHEN {b} = 0 THEN {nan} WHEN abs({b}) = {inf} AND abs({a}) < {inf} THEN {a} ELSE {a} - floor({a} / {b}) * {b} END)"
        )
    }

    fn log2(&self, x: &str) -> String {
        format!("(ln({x}) / ln(CAST(2 AS double precision)))")
    }

    fn exp(&self, x: &str) -> String {
        // PostgreSQL raises on overflow and underflow
        let (nan, inf) = (self.real_literal(f64::NAN), self.real_literal(f64::INFINITY));
        format!("(CASE WHEN {x} = {nan} THEN {nan} WHEN {x} > 709.782712893384 THEN {inf} WHEN {x} < -745.1332191019411 THEN CAST(0 AS double precision) ELSE exp({x}) END)")
    }

    fn power(&self, x: &str, y: &str) -> String {
        let nan = self.real_literal(f64::NAN);
        format!("(CASE WHEN {x} < 0 AND {y} <> trunc({y}) THEN {nan} ELSE power({x}, {y}) END)")
    }

    fn real_div(&self, a: &str, b: &str) -> String {
        let (nan, inf, ninf) = (self.real_literal(f64::NAN), self.real_literal(f64::INFINITY), self.real_literal(f64::NEG_INFINITY));
        // NaN sorts above every number in PostgreSQL, so test it before the sign
        format!(
            "(CASE WHEN {b} = 0 AND {a} IS NOT NULL THEN CASE WHEN {a} = 0 OR {a} = {nan} THEN {nan} WHEN {a} > 0 THEN {inf} ELSE {ninf} END ELSE {a} / {b} END)"
        )
    }

    fn substr(&self, s: &str, start: &str, len: Option<&str>) -> String {
        // PostgreSQL's substr takes integer positions; clamp so huge values cannot overflow
        let i = |x: &str| format!("CAST(least(greatest({x}, -2147483648), 2147483647) AS integer)");
        match len {
            Some(l) => format!("substr({s}, {}, {})", i(start), i(l)),
            None => format!("substr({s}, {})", i(start)),
        }
    }

    fn null_safe_eq(&self, l: &str, r: &str, ty: KqlType) -> String {
        // IS NOT DISTINCT FROM is neither hash- nor merge-joinable (FULL JOIN rejects it): compare
        // a non-null surrogate plus the null flags instead.
        let sentinel = match ty {
            KqlType::String => return format!("{l} = {r}"),
            KqlType::Bool => "false",
            KqlType::DateTime => "TIMESTAMP '1970-01-01'",
            KqlType::Guid => "CAST('00000000-0000-0000-0000-000000000000' AS uuid)",
            KqlType::Dynamic => "CAST('null' AS jsonb)",
            _ => "0",
        };
        format!("(COALESCE({l}, {sentinel}) = COALESCE({r}, {sentinel}) AND ({l} IS NULL) = ({r} IS NULL))")
    }
}
