//! `search` and `parse-kv`.
//!
//! `search` semantics (Kusto):
//! * a bare string term `"x"` means "any column has the term `x`" (`where * has "x"`);
//!   wildcards: `"x*"` → hasprefix, `"*x"` → hassuffix, `"*x*"` → contains, `"a*b"` → a regex
//!   `\ba.*b\b`; `"*"` matches everything;
//! * `Col:"x"` restricts a term to one column (the parser produces `Col has "x"`);
//! * `kind=case_sensitive` makes terms case-sensitive (the default is case-insensitive);
//! * `not(...)`, `and`, `or` and parentheses combine sub-predicates; anything else is an ordinary
//!   scalar predicate;
//! * the output starts with a `$table` column naming the source table (`search_arg0` for an
//!   anonymous piped input).
//!
//! `parse-kv` extracts the listed keys from `key<kv_delimiter>value<pair_delimiter>...` text (or
//! with a `regex` whose first two groups are key and value). The first occurrence of a key wins,
//! keys and values are trimmed, missing keys are empty strings (or nulls for typed keys).

use kql_parser::ast::{BinaryOp, ColumnDecl, Expr, Literal, OpParam, StringOp};

use crate::binder::{Binding, Ctx, Env, Rel};
use crate::expr::{col_ref, Const, Scope, TExpr};
use crate::sql::{quote_ident, quote_str, From, Item};
use crate::{err, Column, Dialect, KqlType, Result};

fn param<'p>(params: &'p [OpParam], name: &str) -> Option<&'p Expr> {
    params.iter().find(|p| p.name.eq_ignore_ascii_case(name)).map(|p| &p.value)
}

fn param_text(params: &[OpParam], name: &str) -> Option<String> {
    match param(params, name)? {
        Expr::Name(n) => Some(n.clone()),
        Expr::Literal(Literal::String(s)) => Some(s.clone()),
        Expr::Literal(Literal::Bool(b)) => Some(b.to_string()),
        _ => None,
    }
}

// ====================================================================== search

/// `search` as an operator (`input` is Some) or as a source over tables.
pub(crate) fn search(ctx: &mut Ctx, input: Option<Rel>, params: &[OpParam], tables: &[Expr], predicate: &Expr, env: &Env) -> Result<Rel> {
    let cs = match param_text(params, "kind") {
        None => false,
        Some(k) if k.eq_ignore_ascii_case("case_sensitive") => true,
        Some(k) if k.eq_ignore_ascii_case("case_insensitive") || k.eq_ignore_ascii_case("default") => false,
        Some(k) => return err(format!("search: unknown kind '{k}'")),
    };
    if let Some(rel) = input {
        let label = piped_label(ctx, &rel);
        return search_rel(ctx, rel, Some(&label), cs, predicate, env);
    }

    // a source: the listed tables, or every table of the database
    let mut parts: Vec<(String, Rel)> = Vec::new();
    if tables.is_empty() {
        let names: Vec<String> = ctx.catalog.tables.iter().map(|(n, _)| n.clone()).collect();
        for n in names {
            let r = ctx.table_ref(&n, env)?;
            parts.push((n, r));
        }
    } else {
        for t in tables {
            match t {
                Expr::Name(n) if n.contains('*') => {
                    let mut names: Vec<String> =
                        ctx.catalog.tables.iter().map(|(tn, _)| tn.clone()).filter(|tn| crate::ops::wildcard(n, tn)).collect();
                    names.sort();
                    for tn in names {
                        let r = ctx.table_ref(&tn, env)?;
                        parts.push((tn, r));
                    }
                }
                _ => {
                    let label = match t {
                        Expr::Name(n) if env.get(n).is_none() => match ctx.catalog.table(n) {
                            Some((tn, _)) => tn.to_string(),
                            None => n.clone(),
                        },
                        Expr::Name(n) => n.clone(),
                        _ => format!("search_arg{}", parts.len()),
                    };
                    let r = ctx.tabular(t, env)?;
                    parts.push((label, r));
                }
            }
        }
    }
    if parts.is_empty() {
        return err("search: there are no tables to search");
    }
    if parts.len() == 1 {
        let (label, rel) = parts.pop().unwrap();
        return search_rel(ctx, rel, Some(&label), cs, predicate, env);
    }
    // union the tables (outer, by column name) labeled with `$table`, then search the union:
    // a predicate over a column some table lacks sees nulls there
    let mut env2 = env.clone();
    let mut names = Vec::new();
    for (i, (label, rel)) in parts.into_iter().enumerate() {
        let r = with_table_column(ctx, rel, &label);
        let t = ctx.add_cte(&format!("search_part{i}"), r, false);
        let tmp = format!("__kql_search_{i}_{}", ctx.ctes.len());
        env2.set(&tmp, Binding::Tabular(t));
        names.push(Expr::Name(tmp));
    }
    let all = crate::join::union(ctx, None, &[], &names, &env2)?;
    search_rel(ctx, all, None, cs, predicate, env)
}

/// Prepends the `$table` column.
fn with_table_column(ctx: &mut Ctx, rel: Rel, label: &str) -> Rel {
    let mut items = vec![Item { sql: ctx.d.cast(&quote_str(label), KqlType::String), alias: "$table".into() }];
    let mut cols = vec![Column::new("$table", KqlType::String)];
    for c in &rel.cols {
        if c.name == "$table" {
            continue;
        }
        items.push(Item { sql: quote_ident(&c.name), alias: c.name.clone() });
        cols.push(c.clone());
    }
    rel.project(ctx, items, cols)
}

/// The `$table` value of a piped search: the table name for a bare stored table.
fn piped_label(ctx: &Ctx, rel: &Rel) -> String {
    let s = &rel.sel;
    if let From::Table(q) = &s.from {
        let plain = s.filters.is_empty() && s.group_by.is_empty() && s.limit.is_none() && !s.distinct && s.order_by.is_empty();
        if plain {
            if let Some((name, _)) = ctx.catalog.tables.iter().find(|(n, _)| quote_ident(n) == *q) {
                return name.clone();
            }
        }
    }
    "search_arg0".into()
}

fn search_rel(ctx: &mut Ctx, rel: Rel, label: Option<&str>, cs: bool, predicate: &Expr, env: &Env) -> Result<Rel> {
    let mut rel = rel.passthrough(ctx);
    let cond = {
        let scope = Scope::rows(&rel.cols);
        search_cond(ctx, predicate, &scope, cs, env)?
    };
    if cond.agg {
        return err("aggregate functions are not allowed in 'search'");
    }
    if cond.window {
        return err("window functions are not supported in 'search'");
    }
    let cond = ctx.to_bool(cond);
    if cond.sql != "true" {
        rel.sel.filters.push(cond.sql);
    }
    Ok(match label {
        Some(l) => with_table_column(ctx, rel, l),
        None => rel,
    })
}

fn bool_expr(sql: impl Into<String>, parts: &[&TExpr]) -> TExpr {
    TExpr::derived(sql, KqlType::Bool, parts)
}

/// Compiles a search predicate.
fn search_cond(ctx: &mut Ctx, e: &Expr, scope: &Scope, cs: bool, env: &Env) -> Result<TExpr> {
    match e {
        Expr::Star => Ok(TExpr::konst("true", KqlType::Bool, Const::Bool(true))),
        Expr::Paren(x) if !ctx.is_tabular(x, env) => search_cond(ctx, x, scope, cs, env),
        Expr::Literal(Literal::String(term)) => Ok(bool_expr(any_column_term(ctx, scope, term, cs), &[])),
        // non-string literals are not search terms; they never match
        Expr::Literal(Literal::Bool(b)) => Ok(TExpr::konst(if *b { "true" } else { "false" }, KqlType::Bool, Const::Bool(*b))),
        Expr::Literal(_) => Ok(TExpr::konst("false", KqlType::Bool, Const::Bool(false))),
        Expr::Binary { op: op @ (BinaryOp::And | BinaryOp::Or), left, right } => {
            let l = search_cond(ctx, left, scope, cs, env)?;
            let r = search_cond(ctx, right, scope, cs, env)?;
            let (l, r) = (ctx.to_bool(l), ctx.to_bool(r));
            let kw = if *op == BinaryOp::And { "AND" } else { "OR" };
            Ok(bool_expr(format!("({} {kw} {})", l.sql, r.sql), &[&l, &r]))
        }
        Expr::Call { name, args } if name.eq_ignore_ascii_case("not") && args.len() == 1 && env.get(name).is_none() => {
            let x = search_cond(ctx, &args[0].expr, scope, cs, env)?;
            let x = ctx.to_bool(x);
            Ok(bool_expr(format!("(NOT {})", x.sql), &[&x]))
        }
        // `Col:"term"` (and `Col has "term"`): a term with search wildcards against one column
        Expr::Binary { op: BinaryOp::Str(StringOp::Has, negated), left, right } if matches!(&**right, Expr::Literal(Literal::String(_))) => {
            let Expr::Literal(Literal::String(term)) = &**right else { unreachable!() };
            let sql = if matches!(&**left, Expr::Star) {
                any_column_term(ctx, scope, term, cs)
            } else {
                let l = ctx.expr(left, scope, env)?;
                let l = ctx.to_string(l);
                term_sql(ctx, &l.sql, term, cs)
            };
            Ok(bool_expr(if *negated { format!("(NOT {sql})") } else { sql }, &[]))
        }
        _ => ctx.expr(e, scope, env),
    }
}

/// `term` (with search wildcards) in any column of the scope.
fn any_column_term(ctx: &mut Ctx, scope: &Scope, term: &str, cs: bool) -> String {
    let mut parts = Vec::new();
    for c in scope.cols.iter().filter(|c| c.name != "$table") {
        let x = TExpr::new(col_ref(scope.qual, &c.name), c.ty);
        let x = ctx.to_string(x);
        parts.push(term_sql(ctx, &x.sql, term, cs));
    }
    if parts.is_empty() {
        return "false".into();
    }
    if parts.iter().any(|p| p == "true") {
        return "true".into();
    }
    if parts.len() == 1 {
        return parts.pop().unwrap();
    }
    format!("({})", parts.join(" OR "))
}

/// A search term against one string expression.
fn term_sql(ctx: &Ctx, s: &str, term: &str, cs: bool) -> String {
    let lit = |t: &str| TExpr::konst(quote_str(t), KqlType::String, Const::Str(t.to_string()));
    if term.is_empty() || term.chars().all(|c| c == '*') {
        return "true".into();
    }
    if !term.contains('*') {
        return ctx.term_match(s, &lit(term), cs, true, true);
    }
    let lead = term.starts_with('*');
    let trail = term.ends_with('*');
    let core = term.trim_matches('*');
    if !core.contains('*') {
        return match (lead, trail) {
            (true, true) => {
                if cs {
                    format!("({} > 0)", ctx.d.strpos(s, &quote_str(core)))
                } else {
                    format!("({} > 0)", ctx.d.strpos(&format!("lower({s})"), &format!("lower({})", quote_str(core))))
                }
            }
            (false, true) => ctx.term_match(s, &lit(core), cs, true, false),
            (true, false) => ctx.term_match(s, &lit(core), cs, false, true),
            (false, false) => unreachable!(),
        };
    }
    // an inner wildcard: \bprefix.*suffix\b
    let pg = ctx.d.kind() == Dialect::Postgres;
    let (bl, br) = if pg { ("(^|[^[:alnum:]])", "([^[:alnum:]]|$)") } else { ("(^|[^\\p{L}\\p{N}])", "([^\\p{L}\\p{N}]|$)") };
    let body: Vec<String> = core.split('*').map(crate::regex::escape).collect();
    let mut re = String::new();
    if !lead && core.chars().next().is_some_and(char::is_alphanumeric) {
        re.push_str(bl);
    }
    re.push_str(&body.join(".*"));
    if !trail && core.chars().last().is_some_and(char::is_alphanumeric) {
        re.push_str(br);
    }
    if pg {
        let op = if cs { "~" } else { "~*" };
        return format!("({s} {op} {})", quote_str(&re));
    }
    let flags = if cs { "" } else { "(?i)" };
    ctx.d.regex_match(s, &quote_str(&format!("{flags}{re}")))
}

/// `* <op> rhs` in an ordinary expression (`where * has "x"`): the operator applied to every
/// column, combined with OR (negated operators: none of the columns matches).
pub(crate) fn star_binary(ctx: &mut Ctx, op: BinaryOp, right: &Expr, scope: &Scope, env: &Env) -> Result<TExpr> {
    let (positive, negated) = match op {
        BinaryOp::Str(s, neg) => (BinaryOp::Str(s, false), neg),
        BinaryOp::EqTilde | BinaryOp::MatchesRegex => (op, false),
        BinaryOp::NeTilde => (BinaryOp::EqTilde, true),
        _ => return err("'*' is only valid with string operators (has, contains, ...)"),
    };
    let mut parts: Vec<TExpr> = Vec::new();
    for c in scope.cols {
        let e = Expr::Binary { op: positive, left: Box::new(Expr::Name(c.name.clone())), right: Box::new(right.clone()) };
        parts.push(ctx.expr(&e, scope, env)?);
    }
    let any = if parts.is_empty() {
        "false".to_string()
    } else if parts.len() == 1 {
        parts[0].sql.clone()
    } else {
        format!("({})", parts.iter().map(|p| p.sql.as_str()).collect::<Vec<_>>().join(" OR "))
    };
    let refs: Vec<&TExpr> = parts.iter().collect();
    Ok(bool_expr(if negated { format!("(NOT {any})") } else { any }, &refs))
}

// ====================================================================== parse-kv

/// Escapes characters that are special inside a regex character class.
fn class_escape(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        if matches!(c, '\\' | ']' | '^' | '-' | '[') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

pub(crate) fn parse_kv(ctx: &mut Ctx, rel: Rel, expr: &Expr, columns: &[ColumnDecl], params: &[OpParam], env: &Env) -> Result<Rel> {
    for p in params {
        if !matches!(p.name.as_str(), "pair_delimiter" | "kv_delimiter" | "quote" | "escape" | "greedy" | "regex") {
            return err(format!("parse-kv: unknown option '{}'", p.name));
        }
    }
    if param(params, "escape").is_some() {
        return err("parse-kv: the 'escape' option is not supported");
    }
    let rel = rel.passthrough(ctx);
    let src = ctx.expr(expr, &Scope::rows(&rel.cols), env)?;
    let src = ctx.to_string(src);
    let d = ctx.d;

    let regex = param_text(params, "regex");
    let greedy = param_text(params, "greedy").is_some_and(|g| g.eq_ignore_ascii_case("true"));
    let pair = param_text(params, "pair_delimiter").unwrap_or_else(|| " ".into());
    let kv = param_text(params, "kv_delimiter").unwrap_or_else(|| "=".into());
    let quotes = param_text(params, "quote").unwrap_or_default();
    if regex.is_none() && (pair.is_empty() || kv.is_empty()) {
        return err("parse-kv: delimiters must not be empty");
    }

    let mut items = rel.identity_items();
    let mut cols = rel.cols.clone();
    for c in columns {
        let ty = KqlType::from_name(&c.ty).ok_or_else(|| crate::Error::new(format!("unknown type '{}'", c.ty)))?;
        let raw = if let Some(re) = &regex {
            if d.kind() != Dialect::DuckDb {
                return err("parse-kv with a regex is only supported on DuckDB");
            }
            let re = quote_str(&crate::regex::translate(re, d.kind()));
            format!(
                "COALESCE(trim(regexp_extract_all({s}, {re}, 2)[list_position(list_transform(regexp_extract_all({s}, {re}, 1), x -> trim(x)), {k})]), '')",
                s = src.sql,
                k = quote_str(&c.name)
            )
        } else {
            let key = format!("(?:^|{})\\s*{}\\s*{}\\s*", crate::regex::escape(&pair), crate::regex::escape(&c.name), crate::regex::escape(&kv));
            let unquoted = if greedy {
                // the value runs up to the next `<pair>key<kv>`
                let tail = format!("{key}(.*)$");
                let stop = format!("{}[^{}]*{}.*$", crate::regex::escape(&pair), class_escape(&format!("{pair}{kv}")), crate::regex::escape(&kv));
                format!(
                    "trim(regexp_replace({}, {}, ''))",
                    d.regex_extract(&src.sql, &quote_str(&tail), 1),
                    quote_str(&stop)
                )
            } else {
                let value = if pair.chars().count() == 1 {
                    format!("([^{}]*)", class_escape(&pair))
                } else {
                    format!("(.*?)(?:{}|$)", crate::regex::escape(&pair))
                };
                format!("trim({})", d.regex_extract(&src.sql, &quote_str(&format!("{key}{value}")), 1))
            };
            // quoted values: the first quote character that encloses the value
            let mut v = unquoted;
            for q in quotes.chars().rev() {
                let qe = crate::regex::escape(&q.to_string());
                let re = quote_str(&format!("{key}{qe}([^{}]*){qe}", class_escape(&q.to_string())));
                v = format!("CASE WHEN {} THEN {} ELSE {v} END", d.regex_match(&src.sql, &re), d.regex_extract(&src.sql, &re, 1));
            }
            v
        };
        let raw = TExpr::new(if raw.starts_with('(') || raw.starts_with("trim(") || raw.starts_with("COALESCE(") { raw } else { format!("({raw})") }, KqlType::String);
        let v = if ty == KqlType::String { raw.sql } else { ctx.convert(raw, ty).sql };
        match cols.iter().position(|x| x.name == c.name) {
            Some(j) => {
                items[j].sql = v;
                cols[j].ty = ty;
            }
            None => {
                items.push(Item { sql: v, alias: c.name.clone() });
                cols.push(Column::new(c.name.clone(), ty));
            }
        }
    }
    Ok(rel.project(ctx, items, cols))
}
