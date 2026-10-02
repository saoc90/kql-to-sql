//! Function call dispatch and the scalar function library.

use kql_parser::ast::{Arg, BinaryOp, Expr, Literal};

use crate::binder::{Binding, Ctx, Env, FnResult};
use crate::expr::{Const, Scope, TExpr};
use crate::sql::quote_str;
use crate::types::{common_type, widest};
use crate::{err, Dialect, KqlType, Result};

impl Ctx<'_> {
    pub fn call(&mut self, name: &str, args: &[Arg], scope: &Scope, env: &Env) -> Result<TExpr> {
        if let Some(Binding::Function(f)) = env.get(name).cloned() {
            return match self.call_user_function(&f, args, scope, env)? {
                FnResult::Scalar(t) => Ok(t),
                FnResult::Tabular(_) => err(format!("function '{name}' returns a table and cannot be used as a scalar")),
            };
        }
        let lname = name.to_ascii_lowercase();
        if crate::aggs::is_aggregate(&lname) && !crate::catalog_data::lookup(&lname, crate::catalog_data::FnKind::Scalar).is_some_and(|_| !scope.aggregates) {
            if scope.aggregates {
                return crate::aggs::call(self, name, args, scope, env);
            }
            return err(format!("aggregate function '{name}' is only allowed in 'summarize'"));
        }
        // functions that need their raw arguments
        match lname.as_str() {
            "typeof" => {
                let [Arg { expr: Expr::Name(t), .. }] = args else { return err("typeof() requires a type name") };
                let ty = KqlType::from_name(t).ok_or_else(|| crate::Error::new(format!("unknown type '{t}'")))?;
                return Ok(TExpr::konst(quote_str(ty.name()), KqlType::String, Const::Str(ty.name().to_string())));
            }
            "toscalar" => {
                let [a] = args else { return err("toscalar() takes one argument") };
                return if self.is_tabular(&a.expr, env) { self.scalar_subquery(&a.expr, env) } else { self.expr(&a.expr, scope, env) };
            }
            "column_ifexists" => {
                let (Some(Expr::Literal(Literal::String(col))), Some(def)) = (args.first().map(|a| &a.expr), args.get(1)) else {
                    return err("column_ifexists(name, default) requires a constant column name");
                };
                return match scope.find(col) {
                    Some(c) => Ok(TExpr::new(crate::expr::col_ref(scope.qual, &c.name), c.ty)),
                    None => self.expr(&def.expr, scope, env),
                };
            }
            "pack_all" | "bag_pack_columns" if lname == "pack_all" => {
                let pairs: Vec<(String, String)> = scope
                    .cols
                    .iter()
                    .map(|c| {
                        let t = TExpr::new(crate::expr::col_ref(scope.qual, &c.name), c.ty);
                        (quote_str(&c.name), self.to_dynamic(t).sql)
                    })
                    .collect();
                return Ok(TExpr::new(self.d.json_object(&pairs), KqlType::Dynamic));
            }
            _ => {}
        }
        if matches!(lname.as_str(), "row_number" | "prev" | "next" | "row_cumsum" | "row_rank_dense" | "row_rank_min" | "row_window_session") && !scope.serialized {
            return err(format!("{name}(): the input is not serialized; use 'serialize' or 'sort' before calling window functions"));
        }
        let mut a = Vec::with_capacity(args.len());
        for arg in args {
            a.push(self.expr(&arg.expr, scope, env)?);
        }
        let t = self.scalar(&lname, a, scope)?;
        Ok(t)
    }

    fn scalar(&mut self, name: &str, a: Vec<TExpr>, scope: &Scope) -> Result<TExpr> {
        use KqlType::*;
        let d = self.d;
        let n = a.len();
        let need = |min: usize, max: usize| -> Result<()> {
            if n < min || n > max {
                err(format!("{name}(): expected {} arguments, got {n}", if min == max { min.to_string() } else { format!("{min}..{max}") }))
            } else {
                Ok(())
            }
        };
        let parts: Vec<&TExpr> = a.iter().collect();
        let mk = |sql: std::string::String, ty: KqlType| TExpr::derived(sql, ty, &parts);
        let real = |t: &TExpr| -> std::string::String {
            if t.ty == Dynamic {
                d.try_cast(&d.json_to_text(&t.sql), Real)
            } else {
                d.cast(&t.sql, Real)
            }
        };
        let s = |this: &Self, t: &TExpr| this.to_string(t.clone()).sql;
        match name {
            // ---------------------------------------------------------- conversion
            "tostring" => {
                need(1, 1)?;
                Ok(self.to_string(a[0].clone()))
            }
            "toint" | "tolong" | "toreal" | "todouble" | "tobool" | "toboolean" | "todatetime" | "totimespan" | "totime" | "toguid" | "todecimal" => {
                need(1, 1)?;
                let ty = match name {
                    "toint" => Int,
                    "tolong" => Long,
                    "toreal" | "todouble" => Real,
                    "tobool" | "toboolean" => Bool,
                    "todatetime" => DateTime,
                    "totimespan" | "totime" => TimeSpan,
                    "toguid" => Guid,
                    _ => Decimal,
                };
                let x = a.into_iter().next().unwrap();
                Ok(self.convert(x, ty))
            }
            "todynamic" | "parse_json" => {
                need(1, 1)?;
                let x = &a[0];
                Ok(match x.ty {
                    Dynamic => x.clone(),
                    String => {
                        let parse = match d.kind() {
                            Dialect::DuckDb => format!("CASE WHEN json_valid({0}) THEN CAST({0} AS JSON) ELSE to_json({0}) END", x.sql),
                            Dialect::Postgres => format!("COALESCE({}, {})", d.try_cast(&x.sql, Dynamic), d.to_json(&x.sql)),
                        };
                        mk(format!("CASE WHEN {} = '' THEN NULL ELSE {parse} END", x.sql), Dynamic)
                    }
                    _ => self.to_dynamic(x.clone()),
                })
            }
            "gettype" => {
                need(1, 1)?;
                let x = &a[0];
                if x.ty == Dynamic {
                    let jt = d.json_type(&x.sql);
                    let sql = format!(
                        "CASE WHEN {x} IS NULL THEN 'null' ELSE CASE {jt} WHEN 'object' THEN 'dictionary' WHEN 'array' THEN 'array' WHEN 'varchar' THEN 'string' WHEN 'string' THEN 'string' \
                         WHEN 'bigint' THEN 'long' WHEN 'ubigint' THEN 'long' WHEN 'number' THEN 'double' WHEN 'double' THEN 'double' WHEN 'boolean' THEN 'bool' WHEN 'null' THEN 'null' ELSE {jt} END END",
                        x = x.sql
                    );
                    return Ok(mk(sql, String));
                }
                // static types report their name even for null values (gettype(toint("x")) == "int")
                Ok(mk(quote_str(x.ty.gettype_name()), String))
            }
            // ---------------------------------------------------------- conditional
            "iff" | "iif" => {
                need(3, 3)?;
                let c = self.to_bool(a[0].clone());
                self.check_same_types(name, &a[1..])?;
                let (t, f, ty) = self.unify2(a[1].clone(), a[2].clone())?;
                Ok(TExpr::derived(format!("CASE WHEN {} THEN {} ELSE {} END", c.sql, t.sql, f.sql), ty, &[&c, &t, &f]))
            }
            "case" => {
                if n < 3 || n % 2 == 0 {
                    return err("case(): expected predicate/value pairs and an else value");
                }
                let mut vals: Vec<TExpr> = a.iter().skip(1).step_by(2).cloned().collect();
                vals.push(a[n - 1].clone());
                self.check_same_types(name, &vals)?;
                let (vals, ty) = self.unify(vals)?;
                let mut sql = std::string::String::from("CASE");
                for i in 0..(n - 1) / 2 {
                    let c = self.to_bool(a[2 * i].clone());
                    sql.push_str(&format!(" WHEN {} THEN {}", c.sql, vals[i].sql));
                }
                sql.push_str(&format!(" ELSE {} END", vals.last().unwrap().sql));
                Ok(mk(sql, ty))
            }
            "coalesce" => {
                need(1, 64)?;
                self.check_same_types(name, &a)?;
                let (vals, ty) = self.unify(a.clone())?;
                if ty == String {
                    let items: Vec<std::string::String> = vals.iter().map(|v| format!("NULLIF({}, '')", v.sql)).collect();
                    return Ok(mk(format!("COALESCE({}, '')", items.join(", ")), String));
                }
                Ok(mk(format!("COALESCE({})", vals.iter().map(|v| v.sql.clone()).collect::<Vec<_>>().join(", ")), ty))
            }
            "isnull" | "isnotnull" => {
                need(1, 1)?;
                let not = if name == "isnotnull" { " NOT" } else { "" };
                if a[0].ty == String {
                    return Ok(mk(if not.is_empty() { "false".into() } else { "true".into() }, Bool));
                }
                if a[0].ty == Dynamic {
                    let isn = format!("({0} IS NULL OR {1} = 'null')", a[0].sql, d.json_type(&a[0].sql));
                    return Ok(mk(if not.is_empty() { format!("(NOT {isn})") } else { isn }, Bool));
                }
                Ok(mk(format!("({} IS{not} NULL)", a[0].sql), Bool))
            }
            "isempty" | "isnotempty" => {
                need(1, 1)?;
                let x = &a[0];
                let empty = match x.ty {
                    String => format!("({} = '')", x.sql),
                    Dynamic => format!("({0} IS NULL OR {1} = 'null' OR {2} = '')", x.sql, d.json_type(&x.sql), d.json_to_text(&x.sql)),
                    _ => format!("({} IS NULL)", x.sql),
                };
                Ok(mk(if name == "isempty" { empty } else { format!("(NOT {empty})") }, Bool))
            }
            "not" => {
                need(1, 1)?;
                let b = self.to_bool(a[0].clone());
                // not() of a comparison with a null operand is true in Kusto
                Ok(TExpr::derived(format!("(NOT {})", b.bool_sql()), Bool, &[&b]))
            }
            // ---------------------------------------------------------- strings
            "strlen" => {
                need(1, 1)?;
                Ok(mk(d.cast(&format!("length({})", s(self, &a[0])), Long), Long))
            }
            "string_size" => {
                need(1, 1)?;
                let x = s(self, &a[0]);
                let sql = match d.kind() {
                    Dialect::DuckDb => format!("CAST(octet_length(encode({x})) AS BIGINT)"),
                    Dialect::Postgres => format!("CAST(octet_length({x}) AS bigint)"),
                };
                Ok(mk(sql, Long))
            }
            "strcat" => {
                need(1, 64)?;
                let parts: Vec<std::string::String> = a.iter().map(|t| s(self, t)).collect();
                Ok(mk(format!("({})", parts.join(" || ")), String))
            }
            "strcat_delim" => {
                need(2, 65)?;
                let delim = s(self, &a[0]);
                let parts: Vec<std::string::String> = a[1..].iter().map(|t| s(self, t)).collect();
                Ok(mk(format!("concat_ws({delim}, {})", parts.join(", ")), String))
            }
            "tolower" | "toupper" => {
                need(1, 1)?;
                let f = if name == "tolower" { "lower" } else { "upper" };
                Ok(mk(format!("{f}({})", s(self, &a[0])), String))
            }
            "substring" => {
                need(2, 3)?;
                let x = s(self, &a[0]);
                let st = d.cast(&a[1].sql, Long);
                // a negative start counts from the end; before the beginning yields ''
                let start = format!("(CASE WHEN {st} < 0 THEN length({x}) + {st} ELSE {st} END)");
                let sql = if n == 3 {
                    format!("CASE WHEN {start} < 0 THEN '' ELSE COALESCE(substr({x}, {start} + 1, greatest({}, 0)), '') END", d.cast(&a[2].sql, Long))
                } else {
                    format!("CASE WHEN {start} < 0 THEN '' ELSE COALESCE(substr({x}, {start} + 1), '') END")
                };
                Ok(mk(sql, String))
            }
            "reverse" => {
                need(1, 1)?;
                Ok(mk(format!("reverse({})", s(self, &a[0])), String))
            }
            "indexof" => {
                need(2, 5)?;
                let x = s(self, &a[0]);
                let look = s(self, &a[1]);
                if n == 2 {
                    return Ok(mk(format!("CASE WHEN {look} = '' THEN NULL ELSE CAST({} AS BIGINT) - 1 END", d.strpos(&x, &look)), Long));
                }
                let start = d.cast(&a[2].sql, Long);
                let sub = format!("substr({x}, {start} + 1)");
                Ok(mk(format!("CASE WHEN {p} = 0 THEN -1 ELSE CAST({p} AS BIGINT) - 1 + {start} END", p = d.strpos(&sub, &look)), Long))
            }
            "replace_string" | "replace" if name == "replace_string" => {
                need(3, 3)?;
                Ok(mk(format!("replace({}, {}, {})", s(self, &a[0]), s(self, &a[1]), s(self, &a[2])), String))
            }
            "replace_regex" | "replace" => {
                need(3, 3)?;
                let (text, re, rep) = if name == "replace" { (&a[2], &a[0], &a[1]) } else { (&a[0], &a[1], &a[2]) };
                let re_sql = match re.str_const() {
                    Some(r) => quote_str(&crate::regex::translate(r, d.kind())),
                    None => s(self, re),
                };
                let rep_sql = match rep.str_const() {
                    Some(r) => quote_str(&crate::regex::translate_replacement(r, d.kind())),
                    None => s(self, rep),
                };
                Ok(mk(format!("regexp_replace({}, {re_sql}, {rep_sql}, 'g')", s(self, text)), String))
            }
            "trim" | "trim_start" | "trim_end" => {
                need(2, 2)?;
                let re = match a[0].str_const() {
                    Some(r) => crate::regex::translate(r, d.kind()),
                    None => return err(format!("{name}(): the regular expression must be a constant")),
                };
                let x = s(self, &a[1]);
                let lead = quote_str(&format!("^(?:{re})"));
                let trail = quote_str(&format!("(?:{re})$"));
                let sql = match name {
                    "trim_start" => format!("regexp_replace({x}, {lead}, '')"),
                    "trim_end" => format!("regexp_replace({x}, {trail}, '')"),
                    _ => format!("regexp_replace(regexp_replace({x}, {lead}, ''), {trail}, '')"),
                };
                Ok(mk(sql, String))
            }
            "strrep" => {
                need(2, 3)?;
                let x = s(self, &a[0]);
                let times = d.cast(&a[1].sql, Long);
                if n == 3 {
                    let delim = s(self, &a[2]);
                    let sql = format!("CASE WHEN {times} <= 0 THEN '' ELSE repeat({x} || {delim}, CAST({times} - 1 AS INTEGER)) || {x} END");
                    return Ok(mk(sql, String));
                }
                Ok(mk(format!("repeat({x}, CAST(greatest({times}, 0) AS INTEGER))"), String))
            }
            "split" => {
                need(2, 3)?;
                let x = s(self, &a[0]);
                let delim = s(self, &a[1]);
                let arr = match d.kind() {
                    Dialect::DuckDb => format!("to_json(string_split({x}, {delim}))"),
                    Dialect::Postgres => format!("to_jsonb(string_to_array({x}, {delim}))"),
                };
                if n == 3 {
                    let el = d.json_get_index(&arr, &d.cast(&a[2].sql, Long));
                    return Ok(mk(format!("CASE WHEN {el} IS NULL THEN NULL ELSE {} END", d.json_array(&[el.clone()])), Dynamic));
                }
                Ok(mk(arr, Dynamic))
            }
            "extract" => {
                need(3, 4)?;
                let re = match a[0].str_const() {
                    Some(r) => quote_str(&crate::regex::translate(r, d.kind())),
                    None => s(self, &a[0]),
                };
                let group = a[1].long_const().ok_or_else(|| crate::Error::new("extract(): the capture group must be a constant"))?;
                let x = s(self, &a[2]);
                let m = d.regex_match(&x, &re);
                let v = format!("CASE WHEN {m} THEN {} END", d.regex_extract(&x, &re, group as u32));
                let target = if n == 4 { type_literal(&a[3])? } else { String };
                let t = TExpr::derived(v, String, &parts);
                if target == String {
                    return Ok(TExpr::derived(format!("COALESCE({}, '')", t.sql), String, &parts));
                }
                Ok(self.convert(t, target))
            }
            "extract_all" => {
                need(2, 3)?;
                let (re, text) = if n == 2 { (&a[0], &a[1]) } else { (&a[0], &a[2]) };
                let r = re.str_const().ok_or_else(|| crate::Error::new("extract_all(): the regular expression must be a constant"))?;
                let groups = crate::regex::capture_groups(r);
                if groups == 0 {
                    return err("extract_all(): the regular expression must have at least one capture group");
                }
                let re_sql = quote_str(&crate::regex::translate(r, d.kind()));
                let x = s(self, text);
                let sql = match d.kind() {
                    Dialect::DuckDb if groups <= 1 => {
                        format!("CASE WHEN regexp_matches({x}, {re_sql}) THEN to_json(regexp_extract_all({x}, {re_sql}, {})) END", if groups == 1 { 1 } else { 0 })
                    }
                    Dialect::DuckDb => {
                        let idx: Vec<std::string::String> = (1..=groups).map(|g| g.to_string()).collect();
                        format!(
                            "CASE WHEN regexp_matches({x}, {re_sql}) THEN to_json(list_transform(regexp_extract_all({x}, {re_sql}, 0), m -> [{}])) END",
                            idx.iter().map(|g| format!("regexp_extract(m, {re_sql}, {g})")).collect::<Vec<_>>().join(", ")
                        )
                    }
                    Dialect::Postgres => format!("(SELECT jsonb_agg(m[1]) FROM regexp_matches({x}, {re_sql}, 'g') AS m)"),
                };
                Ok(mk(sql, Dynamic))
            }
            "countof" => {
                need(2, 3)?;
                let x = s(self, &a[0]);
                let sub = s(self, &a[1]);
                let regex = n == 3 && a[2].str_const() == Some("regex");
                let sql = if regex {
                    match d.kind() {
                        Dialect::DuckDb => format!("CAST(len(regexp_extract_all({x}, {sub})) AS BIGINT)"),
                        Dialect::Postgres => format!("(SELECT count(*) FROM regexp_matches({x}, {sub}, 'g'))"),
                    }
                } else {
                    // overlapping occurrences
                    match d.kind() {
                        Dialect::DuckDb => format!(
                            "CASE WHEN length({sub}) = 0 THEN 0 ELSE CAST(len(list_filter(range(length({x})), i -> substr({x}, i + 1, length({sub})) = {sub})) AS BIGINT) END"
                        ),
                        Dialect::Postgres => format!(
                            "CASE WHEN length({sub}) = 0 THEN 0 ELSE (SELECT count(*) FROM generate_series(1, length({x})) AS g(i) WHERE substr({x}, g.i, length({sub})) = {sub}) END"
                        ),
                    }
                };
                Ok(mk(sql, Long))
            }
            "hash_md5" => {
                need(1, 1)?;
                Ok(mk(format!("md5({})", s(self, &a[0])), String))
            }
            "hash_sha256" => {
                need(1, 1)?;
                let x = s(self, &a[0]);
                Ok(mk(
                    match d.kind() {
                        Dialect::DuckDb => format!("sha256({x})"),
                        Dialect::Postgres => format!("encode(sha256(convert_to({x}, 'UTF8')), 'hex')"),
                    },
                    String,
                ))
            }
            "base64_encode_tostring" | "base64_encodestring" => {
                need(1, 1)?;
                let x = s(self, &a[0]);
                Ok(mk(
                    match d.kind() {
                        Dialect::DuckDb => format!("to_base64(encode({x}))"),
                        Dialect::Postgres => format!("encode(convert_to({x}, 'UTF8'), 'base64')"),
                    },
                    String,
                ))
            }
            "base64_decode_tostring" | "base64_decodestring" => {
                need(1, 1)?;
                let x = s(self, &a[0]);
                Ok(mk(
                    match d.kind() {
                        Dialect::DuckDb => format!("decode(from_base64({x}))"),
                        Dialect::Postgres => format!("convert_from(decode({x}, 'base64'), 'UTF8')"),
                    },
                    String,
                ))
            }
            // ---------------------------------------------------------- math
            "abs" => {
                need(1, 1)?;
                let x = self.numeric_arg(&a[0]);
                Ok(TExpr::derived(format!("abs({})", x.sql), x.ty, &[&x]))
            }
            "ceiling" => {
                need(1, 1)?;
                let x = self.numeric_arg(&a[0]);
                let f = "ceil";
                if x.ty.is_integer() {
                    return Ok(x);
                }
                Ok(TExpr::derived(format!("{f}({})", x.sql), x.ty, &[&x]))
            }
            "sqrt" | "exp" | "log" | "log2" | "log10" | "sin" | "cos" | "tan" | "asin" | "acos" | "atan" | "cot" | "degrees" | "radians" | "exp2" | "exp10" | "gamma" | "loggamma" => {
                need(1, 1)?;
                let x = real(&a[0]);
                let sql = match name {
                    "log" => format!("CASE WHEN {x} > 0 THEN ln({x}) WHEN {x} = 0 THEN {} END", d.real_literal(f64::NEG_INFINITY)),
                    "log2" => format!("CASE WHEN {x} > 0 THEN log2({x}) WHEN {x} = 0 THEN {} END", d.real_literal(f64::NEG_INFINITY)),
                    "log10" => format!("CASE WHEN {x} > 0 THEN log10({x}) WHEN {x} = 0 THEN {} END", d.real_literal(f64::NEG_INFINITY)),
                    "sqrt" => format!("CASE WHEN {x} >= 0 THEN sqrt({x}) WHEN {x} < 0 THEN {} END", d.real_literal(f64::NAN)),
                    "exp2" => format!("power(2, {x})"),
                    "exp10" => format!("power(10, {x})"),
                    "loggamma" => format!("lgamma({x})"),
                    f => format!("{f}({x})"),
                };
                Ok(mk(sql, Real))
            }
            "pow" | "power" | "atan2" => {
                need(2, 2)?;
                let f = if name == "atan2" { "atan2" } else { "power" };
                Ok(mk(format!("{f}({}, {})", real(&a[0]), real(&a[1])), Real))
            }
            "pi" => {
                need(0, 0)?;
                Ok(mk("pi()".into(), Real))
            }
            "sign" => {
                need(1, 1)?;
                let x = self.numeric_arg(&a[0]);
                Ok(TExpr::derived(d.cast(&format!("sign({})", x.sql), x.ty), x.ty, &[&x]))
            }
            "isnan" | "isinf" | "isfinite" => {
                need(1, 1)?;
                let x = real(&a[0]);
                let sql = match name {
                    "isnan" => format!("isnan({x})"),
                    "isinf" => format!("isinf({x})"),
                    _ => format!("isfinite({x})"),
                };
                Ok(mk(sql, Bool))
            }
            "round" => {
                need(1, 2)?;
                let x = self.numeric_arg(&a[0]);
                let digits = if n == 2 { d.cast(&a[1].sql, Int) } else { "0".into() };
                let sql = match d.kind() {
                    Dialect::DuckDb => format!("round({}, {digits})", x.sql),
                    Dialect::Postgres => format!("CAST(round(CAST({} AS numeric), {digits}) AS {})", x.sql, d.sql_type(x.ty)),
                };
                Ok(TExpr::derived(sql, x.ty, &[&x]))
            }
            "rand" => {
                need(0, 1)?;
                if n == 1 {
                    return Ok(mk(format!("CAST(floor({} * {}) AS BIGINT)", d.random(), d.cast(&a[0].sql, Real)), Long));
                }
                Ok(mk(d.random(), Real))
            }
            "min_of" | "max_of" => {
                need(1, 64)?;
                let (vals, ty) = self.unify(a.clone())?;
                let f = if name == "min_of" { "least" } else { "greatest" };
                Ok(mk(format!("{f}({})", vals.iter().map(|v| v.sql.clone()).collect::<Vec<_>>().join(", ")), ty))
            }
            "bin" | "floor" => {
                // floor(x, size) is an alias of bin; the one-argument math floor does not exist
                need(2, 2)?;
                self.bin(&a[0], &a[1], None)
            }
            "bin_at" => {
                need(3, 3)?;
                self.bin(&a[0], &a[1], Some(&a[2]))
            }
            // ---------------------------------------------------------- datetime
            "now" => {
                need(0, 1)?;
                let now = TExpr::new(d.now(), DateTime);
                Ok(if n == 1 { self.dt_add(&now, &a[0]) } else { now })
            }
            "ago" => {
                need(1, 1)?;
                if a[0].ty != TimeSpan {
                    return err("ago() requires a timespan");
                }
                let now = TExpr::new(d.now(), DateTime);
                Ok(self.dt_sub_ts(&now, &a[0]))
            }
            "startofday" | "startofweek" | "startofmonth" | "startofyear" | "endofday" | "endofweek" | "endofmonth" | "endofyear" => {
                need(1, 2)?;
                let x = self.convert(a[0].clone(), DateTime);
                let offset = if n == 2 { d.cast(&a[1].sql, Long) } else { "0".into() };
                let unit = &name[name.len() - if name.ends_with("day") { 3 } else if name.ends_with("week") { 4 } else if name.ends_with("month") { 5 } else { 4 }..];
                let start = self.start_of(unit, &x.sql, &offset);
                let sql = if name.starts_with("end") {
                    let next = self.start_of(unit, &x.sql, &format!("({offset} + 1)"));
                    d.ts_from_us(&format!("({} - 1)", d.epoch_us(&next)))
                } else {
                    start
                };
                Ok(TExpr::derived(sql, DateTime, &[&x]))
            }
            "dayofweek" => {
                need(1, 1)?;
                Ok(mk(format!("(CAST({} AS BIGINT) * 864000000000)", d.extract("DOW", &a[0].sql)), TimeSpan))
            }
            "dayofmonth" | "dayofyear" | "monthofyear" | "getmonth" | "getyear" | "hourofday" | "minuteofhour" | "secondofminute" | "week_of_year" | "weekofyear" => {
                need(1, 1)?;
                let part = match name {
                    "dayofmonth" => "DAY",
                    "dayofyear" => "DOY",
                    "monthofyear" | "getmonth" => "MONTH",
                    "getyear" => "YEAR",
                    "hourofday" => "HOUR",
                    "minuteofhour" => "MINUTE",
                    "secondofminute" => "SECOND",
                    _ => "WEEK",
                };
                let ty = if matches!(name, "getyear" | "getmonth" | "dayofmonth" | "dayofyear" | "monthofyear" | "hourofday" | "week_of_year" | "weekofyear") { Int } else { Int };
                Ok(mk(d.cast(&format!("floor({})", d.extract(part, &a[0].sql)), ty), ty))
            }
            "datetime_part" | "datepart" => {
                need(2, 2)?;
                let part = a[0].str_const().ok_or_else(|| crate::Error::new("datetime_part(): the part must be a constant"))?.to_ascii_lowercase();
                let x = &a[1].sql;
                let sql = match part.as_str() {
                    "year" => d.extract("YEAR", x),
                    "quarter" => d.extract("QUARTER", x),
                    "month" => d.extract("MONTH", x),
                    "week_of_year" | "weekofyear" | "week" => d.extract("WEEK", x),
                    "day" => d.extract("DAY", x),
                    "dayofyear" => d.extract("DOY", x),
                    "hour" => d.extract("HOUR", x),
                    "minute" => d.extract("MINUTE", x),
                    "second" => format!("floor({})", d.extract("SECOND", x)),
                    "millisecond" => format!("(floor({}) % 1000)", d.extract("MILLISECONDS", x)),
                    "microsecond" => format!("(floor({}) % 1000000)", d.extract("MICROSECONDS", x)),
                    "nanosecond" => format!("((floor({}) % 1000000) * 1000)", d.extract("MICROSECONDS", x)),
                    other => return err(format!("datetime_part(): unknown part '{other}'")),
                };
                Ok(mk(d.cast(&sql, Int), Int))
            }
            "datetime_add" => {
                need(3, 3)?;
                let part = a[0].str_const().ok_or_else(|| crate::Error::new("datetime_add(): the period must be a constant"))?.to_ascii_lowercase();
                let amount = d.cast(&a[1].sql, Long);
                let x = &a[2];
                let ticks = |mult: i64| format!("({amount} * {mult})");
                let sql = match part.as_str() {
                    "year" | "quarter" | "month" => {
                        let months = match part.as_str() {
                            "year" => format!("({amount} * 12)"),
                            "quarter" => format!("({amount} * 3)"),
                            _ => amount.clone(),
                        };
                        return Ok(mk(self.add_months(&x.sql, &months), DateTime));
                    }
                    "week" => ticks(7 * 864_000_000_000),
                    "day" => ticks(864_000_000_000),
                    "hour" => ticks(36_000_000_000),
                    "minute" => ticks(600_000_000),
                    "second" => ticks(10_000_000),
                    "millisecond" => ticks(10_000),
                    "microsecond" => ticks(10),
                    "nanosecond" => format!("({amount} / 100)"),
                    other => return err(format!("datetime_add(): unknown period '{other}'")),
                };
                Ok(self.dt_add(x, &TExpr::new(sql, TimeSpan)))
            }
            "datetime_diff" => {
                need(3, 3)?;
                let part = a[0].str_const().ok_or_else(|| crate::Error::new("datetime_diff(): the period must be a constant"))?.to_ascii_lowercase();
                let (x, y) = (&a[1].sql, &a[2].sql);
                let sql = match part.as_str() {
                    "year" | "quarter" | "month" | "day" | "hour" | "minute" | "second" | "millisecond" | "microsecond" => match d.kind() {
                        Dialect::DuckDb => format!("date_diff('{part}', {y}, {x})"),
                        Dialect::Postgres => self.pg_date_diff(&part, y, x),
                    },
                    "week" => {
                        // weeks start on Sunday
                        let sx = self.start_of("week", x, "0");
                        let sy = self.start_of("week", y, "0");
                        format!("CAST(floor(({} - {}) / 604800000000.0) AS BIGINT)", d.epoch_us(&sx), d.epoch_us(&sy))
                    }
                    "nanosecond" => format!("(({} - {}) * 1000)", d.epoch_us(x), d.epoch_us(y)),
                    other => return err(format!("datetime_diff(): unknown period '{other}'")),
                };
                Ok(mk(d.cast(&sql, Long), Long))
            }
            "make_datetime" => {
                need(1, 7)?;
                let g = |i: usize, def: &str| a.get(i).map(|t| t.sql.clone()).unwrap_or_else(|| def.to_string());
                let sec = if n >= 6 { d.cast(&a[5].sql, Real) } else { "0".into() };
                let sql = match d.kind() {
                    Dialect::DuckDb => format!("try(make_timestamp(CAST({} AS BIGINT), CAST({} AS BIGINT), CAST({} AS BIGINT), CAST({} AS BIGINT), CAST({} AS BIGINT), {sec}))", g(0, "1"), g(1, "1"), g(2, "1"), g(3, "0"), g(4, "0")),
                    Dialect::Postgres => format!("make_timestamp(CAST({} AS int), CAST({} AS int), CAST({} AS int), CAST({} AS int), CAST({} AS int), {sec})", g(0, "1"), g(1, "1"), g(2, "1"), g(3, "0"), g(4, "0")),
                };
                Ok(mk(sql, DateTime))
            }
            "make_timespan" => {
                need(2, 5)?;
                let v: Vec<std::string::String> = a.iter().map(|t| d.cast(&t.sql, Real)).collect();
                // (h, m), (h, m, s), (d, h, m, s): every part non-negative, h < 24, m < 60, s < 60
                let (h, m, sec) = match n {
                    2 => (0, 1, None),
                    3 => (0, 1, Some(2)),
                    _ => (1, 2, Some(3)),
                };
                let mut checks = vec![format!("{} >= 0 AND {} < 24", v[h], v[h]), format!("{} >= 0 AND {} < 60", v[m], v[m])];
                if let Some(si) = sec {
                    checks.push(format!("{} >= 0 AND {} < 60", v[si], v[si]));
                }
                if n >= 4 {
                    checks.push(format!("{} >= 0", v[0]));
                }
                let valid = checks.join(" AND ");
                let sql = match n {
                    2 => format!("CAST(round(({} * 3600 + {} * 60) * 10000000) AS BIGINT)", v[0], v[1]),
                    3 => format!("CAST(round(({} * 3600 + {} * 60 + {}) * 10000000) AS BIGINT)", v[0], v[1], v[2]),
                    4 => format!("CAST(round(({} * 86400 + {} * 3600 + {} * 60 + {}) * 10000000) AS BIGINT)", v[0], v[1], v[2], v[3]),
                    _ => format!("CAST(round(({} * 86400 + {} * 3600 + {} * 60 + {}) * 10000000) AS BIGINT)", v[0], v[1], v[2], v[3]),
                };
                Ok(mk(format!("CASE WHEN {valid} THEN {sql} END"), TimeSpan))
            }
            "unixtime_seconds_todatetime" | "unixtime_milliseconds_todatetime" | "unixtime_microseconds_todatetime" | "unixtime_nanoseconds_todatetime" => {
                need(1, 1)?;
                let mult = match name {
                    "unixtime_seconds_todatetime" => "* 1000000",
                    "unixtime_milliseconds_todatetime" => "* 1000",
                    "unixtime_microseconds_todatetime" => "* 1",
                    _ => "/ 1000",
                };
                let us = format!("CAST(round({} {mult}) AS BIGINT)", real(&a[0]));
                Ok(mk(d.ts_from_us(&us), DateTime))
            }
            "format_datetime" => {
                need(2, 2)?;
                let f = a[1].str_const().ok_or_else(|| crate::Error::new("format_datetime(): the format must be a constant"))?;
                Ok(mk(format!("COALESCE({}, '')", crate::datefmt::format_datetime(d.kind(), &a[0].sql, f)?), String))
            }
            "format_timespan" => {
                need(2, 2)?;
                let f = a[1].str_const().ok_or_else(|| crate::Error::new("format_timespan(): the format must be a constant"))?;
                Ok(mk(format!("COALESCE({}, '')", crate::datefmt::format_timespan(self, &a[0].sql, f)?), String))
            }
            // ---------------------------------------------------------- dynamic
            "array_length" => {
                need(1, 1)?;
                Ok(mk(d.json_array_length(&a[0].sql), Long))
            }
            "dcount_hll" => {
                // hll sketches are exact sets (see aggs.rs)
                need(1, 1)?;
                Ok(mk(format!("COALESCE({}, 0)", d.json_array_length(&a[0].sql)), Long))
            }
            "pack_array" => {
                let items: Vec<std::string::String> = a.iter().map(|t| self.to_dynamic(t.clone()).sql).collect();
                Ok(mk(d.json_array(&items), Dynamic))
            }
            "pack" | "bag_pack" | "pack_dictionary" => {
                if n % 2 != 0 {
                    return err(format!("{name}(): expected key/value pairs"));
                }
                let pairs: Vec<(std::string::String, std::string::String)> =
                    a.chunks(2).map(|kv| (self.to_string(kv[0].clone()).sql, self.to_dynamic(kv[1].clone()).sql)).collect();
                if pairs.is_empty() {
                    return Ok(mk(d.json_literal("{}"), Dynamic));
                }
                Ok(mk(d.json_object(&pairs), Dynamic))
            }
            "bag_keys" => {
                need(1, 1)?;
                let x = &a[0].sql;
                let sql = match d.kind() {
                    Dialect::DuckDb => format!("CASE WHEN json_type({x}) = 'OBJECT' THEN to_json(json_keys({x})) END"),
                    Dialect::Postgres => format!("CASE WHEN jsonb_typeof({x}) = 'object' THEN (SELECT COALESCE(jsonb_agg(k), '[]'::jsonb) FROM jsonb_object_keys({x}) AS k) END"),
                };
                Ok(mk(sql, Dynamic))
            }
            "bag_has_key" => {
                need(2, 2)?;
                let k = s(self, &a[1]);
                let sql = match d.kind() {
                    Dialect::DuckDb => format!("COALESCE(json_type({}) = 'OBJECT' AND list_contains(json_keys({}), {k}), false)", a[0].sql, a[0].sql),
                    Dialect::Postgres => format!("COALESCE(jsonb_typeof({0}) = 'object' AND {0} ? {k}, false)", a[0].sql),
                };
                Ok(mk(sql, Bool))
            }
            "array_concat" => {
                need(1, 64)?;
                let sql = match d.kind() {
                    Dialect::DuckDb => {
                        // only arrays concatenate; any other argument makes the result null
                        let lists: Vec<std::string::String> = a.iter().map(|t| format!("CAST({} AS JSON[])", t.sql)).collect();
                        let concat = lists.iter().skip(1).fold(lists[0].clone(), |acc, l| format!("list_concat({acc}, {l})"));
                        let checks: Vec<std::string::String> = a.iter().map(|t| format!("json_type({}) = 'ARRAY'", t.sql)).collect();
                        format!("CASE WHEN {} THEN to_json({concat}) END", checks.join(" AND "))
                    }
                    Dialect::Postgres => a.iter().map(|t| t.sql.clone()).collect::<Vec<_>>().join(" || "),
                };
                Ok(mk(sql, Dynamic))
            }
            "array_slice" => {
                need(3, 3)?;
                let (arr, s0, e0) = (&a[0].sql, d.cast(&a[1].sql, Long), d.cast(&a[2].sql, Long));
                let len = format!("json_array_length({arr})");
                let norm = |i: &str| format!("(CASE WHEN {i} < 0 THEN {i} + {len} ELSE {i} END)");
                let sql = match d.kind() {
                    Dialect::DuckDb => format!("to_json(list_slice(CAST({arr} AS JSON[]), {} + 1, {} + 1))", norm(&s0), norm(&e0)),
                    Dialect::Postgres => return err("array_slice() is not supported for PostgreSQL yet"),
                };
                Ok(mk(sql, Dynamic))
            }
            "array_reverse" => {
                need(1, 1)?;
                Ok(mk(format!("to_json(list_reverse(CAST({} AS JSON[])))", a[0].sql), Dynamic))
            }
            "array_index_of" | "set_has_element" => {
                need(2, 2)?;
                let v = self.to_dynamic(a[1].clone());
                let pos = format!("list_position(list_transform(CAST({} AS JSON[]), x -> CAST(x AS VARCHAR)), CAST({} AS VARCHAR))", a[0].sql, v.sql);
                if name == "set_has_element" {
                    return Ok(mk(format!("COALESCE({pos} > 0, false)"), Bool));
                }
                Ok(mk(format!("CASE WHEN {} IS NULL THEN NULL ELSE COALESCE(CAST({pos} AS BIGINT), 0) - 1 END", a[0].sql), Long))
            }
            "array_sum" => {
                need(1, 1)?;
                Ok(mk(format!("list_sum(CAST({} AS DOUBLE[]))", a[0].sql), Real))
            }
            "array_sort_asc" | "array_sort_desc" => {
                need(1, 2)?;
                let dir = if name.ends_with("asc") { "ASC" } else { "DESC" };
                let x = &a[0].sql;
                Ok(mk(format!("CASE WHEN json_type({x}) = 'ARRAY' THEN to_json(list_sort(CAST({x} AS JSON[]), '{dir}')) END"), Dynamic))
            }
            "zip" => {
                let lists: Vec<std::string::String> = a.iter().map(|t| format!("CAST({} AS JSON[])", t.sql)).collect();
                let lens: Vec<std::string::String> = lists.iter().map(|l| format!("len({l})")).collect();
                let elems: Vec<std::string::String> = lists.iter().map(|l| format!("{l}[i + 1]")).collect();
                Ok(mk(format!("to_json(list_transform(range(greatest({})), i -> [{}]))", lens.join(", "), elems.join(", ")), Dynamic))
            }
            "strcat_array" => {
                need(2, 2)?;
                let x = &a[0].sql;
                let delim = s(self, &a[1]);
                Ok(mk(format!("COALESCE(array_to_string(list_transform(CAST({x} AS JSON[]), e -> COALESCE(json_extract_string(e, '$'), '')), {delim}), '')"), String))
            }
            "set_union" | "set_intersect" | "set_difference" => {
                need(2, 64)?;
                let lists: Vec<std::string::String> = a.iter().map(|t| format!("CAST({} AS JSON[])", t.sql)).collect();
                let sql = match name {
                    "set_union" => {
                        let all = format!("flatten([{}])", lists.join(", "));
                        format!("to_json(list_filter({all}, (e, i) -> list_position({all}, e) = i))")
                    }
                    "set_intersect" => {
                        let rest: Vec<std::string::String> = lists[1..].iter().map(|l| format!("list_contains({l}, e)")).collect();
                        format!("to_json(list_filter({0}, (e, i) -> list_position({0}, e) = i AND {1}))", lists[0], rest.join(" AND "))
                    }
                    _ => format!(
                        "to_json(list_filter({0}, (e, i) -> list_position({0}, e) = i AND NOT list_contains(flatten([{1}]), e)))",
                        lists[0],
                        lists[1..].join(", ")
                    ),
                };
                Ok(mk(sql, Dynamic))
            }
            _ => {
                if let Some(r) = crate::window::call(self, name, &a, scope) {
                    return r;
                }
                if let Some(r) = crate::funcs_extra::call(self, name, &a, scope) {
                    return r;
                }
                if let Some(r) = crate::series_funcs::call(self, name, &a, scope) {
                    return r;
                }
                if crate::catalog_data::lookup(name, crate::catalog_data::FnKind::Scalar).is_some() {
                    err(format!("function '{name}' is not supported yet"))
                } else {
                    err(format!("unknown function '{name}'"))
                }
            }
        }
    }

    /// `iff`/`case`/`coalesce` branches must have the same type (int and long mix; typed nulls
    /// match anything).
    fn check_same_types(&self, name: &str, vals: &[TExpr]) -> Result<()> {
        let norm = |t: KqlType| if t == KqlType::Int { KqlType::Long } else { t };
        let mut first: Option<KqlType> = None;
        for v in vals.iter().filter(|v| !v.is_null_const()) {
            match first {
                None => first = Some(norm(v.ty)),
                Some(t) if t == norm(v.ty) => {}
                Some(t) => return err(format!("{name}(): all values must have the same type (found {t} and {})", v.ty)),
            }
        }
        Ok(())
    }

    fn numeric_arg(&self, x: &TExpr) -> TExpr {
        if x.ty == KqlType::Dynamic {
            TExpr::derived(self.d.try_cast(&self.d.json_to_text(&x.sql), KqlType::Real), KqlType::Real, &[x])
        } else {
            x.clone()
        }
    }

    /// Brings two branches (iff) to a common type.
    fn unify2(&self, a: TExpr, b: TExpr) -> Result<(TExpr, TExpr, KqlType)> {
        let (v, ty) = self.unify(vec![a, b])?;
        let mut it = v.into_iter();
        Ok((it.next().unwrap(), it.next().unwrap(), ty))
    }

    pub(crate) fn unify(&self, vals: Vec<TExpr>) -> Result<(Vec<TExpr>, KqlType)> {
        let non_null: Vec<KqlType> = vals.iter().filter(|v| !v.is_null_const()).map(|v| v.ty).collect();
        let ty = if non_null.is_empty() {
            vals[0].ty
        } else if let Some(t) = common_type(&non_null) {
            t
        } else if non_null.iter().any(|t| *t == KqlType::Dynamic) {
            KqlType::Dynamic
        } else {
            return err(format!("values have incompatible types: {}", non_null.iter().map(|t| t.name()).collect::<Vec<_>>().join(", ")));
        };
        let out = vals.into_iter().map(|v| if v.ty == ty { v } else { self.convert(v, ty) }).collect();
        Ok((out, ty))
    }

    fn bin(&self, x: &TExpr, size: &TExpr, at: Option<&TExpr>) -> Result<TExpr> {
        use KqlType::*;
        let d = self.d;
        let parts = [x, size];
        match (x.ty, size.ty) {
            (DateTime, TimeSpan) => {
                let step = match size.konst {
                    Some(Const::TimeSpan(t)) if t >= 10 => (t / 10).to_string(),
                    _ => format!("CAST(trunc({} / 10) AS BIGINT)", size.sql),
                };
                let idiv = if d.kind() == Dialect::Postgres { "/" } else { "//" };
                let us = match at {
                    // Kusto bins datetimes from 0001-01-01 (tick 0); offsets from it are never
                    // negative, so integer division is floor division
                    None => format!("(({} + 62135596800000000) {idiv} {step} * {step} - 62135596800000000)", d.epoch_us(&x.sql)),
                    Some(a) => {
                        let base = d.epoch_us(&self.convert(a.clone(), DateTime).sql);
                        let off = format!("({} - {base})", d.epoch_us(&x.sql));
                        format!("(({off} - ((({off} % {step}) + {step}) % {step})) {idiv} {step} * {step} + {base})")
                    }
                };
                Ok(TExpr::derived(d.ts_from_us(&us), DateTime, &parts))
            }
            (TimeSpan, TimeSpan) => {
                let base = at.map(|a| a.sql.clone()).unwrap_or_else(|| "0".into());
                Ok(TExpr::derived(format!("(CAST(floor(({} - {base}) / CAST({} AS DOUBLE)) AS BIGINT) * {} + {base})", x.sql, size.sql, size.sql), TimeSpan, &parts))
            }
            (a, b) if a.is_numeric() && b.is_numeric() => {
                let ty = match widest(a, b) {
                    Int => Long,
                    t => t,
                };
                let base = at.map(|a| self.d.cast(&a.sql, Real)).unwrap_or_else(|| "0".into());
                let sz = d.cast(&size.sql, Real);
                let sql = format!("(floor(({} - {base}) / {sz}) * {sz} + {base})", d.cast(&x.sql, Real));
                let sql = if ty.is_integer() { format!("CAST({sql} AS BIGINT)") } else { sql };
                Ok(TExpr::derived(sql, ty, &parts))
            }
            (Dynamic, _) => self.bin(&self.numeric_arg(x), size, at),
            _ => err(format!("bin(): unsupported argument types {} and {}", x.ty, size.ty)),
        }
    }

    /// Start of the day/week/month/year containing `x`, shifted by `offset` units.
    fn start_of(&self, unit: &str, x: &str, offset: &str) -> String {
        let d = self.d;
        match unit {
            "day" => d.ts_from_us(&format!("({} + {offset} * 86400000000)", d.epoch_us(&d.date_trunc("day", x)))),
            "week" => {
                // Kusto weeks start on Sunday
                let sunday = d.ts_from_us(&format!("({} - 86400000000)", d.epoch_us(&d.date_trunc("week", &d.ts_from_us(&format!("({} + 86400000000)", d.epoch_us(x)))))));
                d.ts_from_us(&format!("({} + {offset} * 604800000000)", d.epoch_us(&sunday)))
            }
            "month" => self.add_months(&d.date_trunc("month", x), offset),
            _ => self.add_months(&d.date_trunc("year", x), &format!("({offset} * 12)")),
        }
    }

    fn add_months(&self, ts: &str, months: &str) -> String {
        match self.d.kind() {
            Dialect::DuckDb => format!("CAST(({ts} + to_months(CAST({months} AS INTEGER))) AS TIMESTAMP)"),
            Dialect::Postgres => format!("({ts} + make_interval(months => CAST({months} AS int)))"),
        }
    }

    fn pg_date_diff(&self, part: &str, from: &str, to: &str) -> String {
        let d = self.d;
        match part {
            "year" => format!("(EXTRACT(YEAR FROM {to}) - EXTRACT(YEAR FROM {from}))"),
            "quarter" => format!("((EXTRACT(YEAR FROM {to}) - EXTRACT(YEAR FROM {from})) * 4 + EXTRACT(QUARTER FROM {to}) - EXTRACT(QUARTER FROM {from}))"),
            "month" => format!("((EXTRACT(YEAR FROM {to}) - EXTRACT(YEAR FROM {from})) * 12 + EXTRACT(MONTH FROM {to}) - EXTRACT(MONTH FROM {from}))"),
            unit => {
                let us = match unit {
                    "day" => 86_400_000_000i64,
                    "hour" => 3_600_000_000,
                    "minute" => 60_000_000,
                    "second" => 1_000_000,
                    "millisecond" => 1_000,
                    _ => 1,
                };
                format!(
                    "(floor({} / {us}.0) - floor({} / {us}.0))",
                    d.epoch_us(to),
                    d.epoch_us(from)
                )
            }
        }
    }
}

/// The type named by a `typeof(t)` argument.
fn type_literal(t: &TExpr) -> Result<KqlType> {
    match &t.konst {
        Some(Const::Str(s)) => KqlType::from_name(s).ok_or_else(|| crate::Error::new(format!("unknown type '{s}'"))),
        _ => err("expected typeof(<type>)"),
    }
}

/// Kusto's timespan text for a constant number of ticks.
pub(crate) fn format_timespan_ticks(ticks: i64) -> String {
    let neg = ticks < 0;
    let t = ticks.unsigned_abs();
    let days = t / 864_000_000_000;
    let h = t / 36_000_000_000 % 24;
    let m = t / 600_000_000 % 60;
    let s = t / 10_000_000 % 60;
    let f = t % 10_000_000;
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if days > 0 {
        out.push_str(&format!("{days}."));
    }
    out.push_str(&format!("{h:02}:{m:02}:{s:02}"));
    if f > 0 {
        out.push_str(&format!(".{f:07}"));
    }
    out
}

#[allow(dead_code)]
fn is_eq(op: BinaryOp) -> bool {
    matches!(op, BinaryOp::Eq)
}
