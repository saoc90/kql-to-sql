//! `kql-oracle smoke`: translates the StormEvents queries collected from the C# test suite and
//! executes them against the real StormEvents data. Checks that translation succeeds and that the
//! SQL is valid; it has no Kusto results to compare values with. `--engine pglite` translates
//! with the Postgres dialect and loads the data into PGlite instead.

use std::collections::BTreeMap;
use std::path::Path;

use duckdb::Connection;
use kql_to_sql::{Catalog, Column, KqlType};

use crate::{pg, Engine};

/// The StormEvents schema as Azure Data Explorer's help cluster defines it.
pub fn storm_catalog() -> Catalog {
    use KqlType::*;
    let cols = [
        ("StartTime", DateTime),
        ("EndTime", DateTime),
        ("EpisodeId", Int),
        ("EventId", Int),
        ("State", String),
        ("EventType", String),
        ("InjuriesDirect", Int),
        ("InjuriesIndirect", Int),
        ("DeathsDirect", Int),
        ("DeathsIndirect", Int),
        ("DamageProperty", Int),
        ("DamageCrops", Int),
        ("Source", String),
        ("BeginLocation", String),
        ("EndLocation", String),
        ("BeginLat", Real),
        ("BeginLon", Real),
        ("EndLat", Real),
        ("EndLon", Real),
        ("EpisodeNarrative", String),
        ("EventNarrative", String),
        ("StormSummary", Dynamic),
    ];
    Catalog::new().with_table("StormEvents", cols.iter().map(|(n, t)| Column::new(*n, *t)).collect())
}

pub fn load_storm(conn: &Connection, csv: &Path) -> Result<(), String> {
    let cat = storm_catalog();
    let (_, cols) = cat.table("StormEvents").unwrap();
    let types: Vec<String> = cols
        .iter()
        .map(|c| {
            let t = match c.ty {
                KqlType::DateTime => "TIMESTAMP",
                KqlType::Int => "INTEGER",
                KqlType::Real => "DOUBLE",
                KqlType::Dynamic => "JSON",
                _ => "VARCHAR",
            };
            format!("'{}': '{t}'", c.name)
        })
        .collect();
    let sql = format!(
        "CREATE TABLE StormEvents AS SELECT * FROM read_csv('{}', header = true, columns = {{{}}})",
        csv.display(),
        types.join(", ")
    );
    conn.execute_batch(&sql).map_err(|e| e.to_string())
}

/// `CREATE TABLE` for StormEvents in PostgreSQL, with identifiers quoted exactly the way the
/// translator refers to them (unquoted names fold to lower case on both sides).
pub fn pg_storm_ddl() -> String {
    let quote_ident = |n: &str| kql_to_sql::sql::quote_ident_for(n, kql_to_sql::Dialect::Postgres);
    let cat = storm_catalog();
    let (name, cols) = cat.table("StormEvents").unwrap();
    let defs: Vec<String> = cols
        .iter()
        .map(|c| {
            let t = match c.ty {
                KqlType::DateTime => "timestamp",
                KqlType::Int => "integer",
                KqlType::Long => "bigint",
                KqlType::Real => "double precision",
                KqlType::Dynamic => "jsonb",
                _ => "text",
            };
            format!("{} {t}", quote_ident(&c.name))
        })
        .collect();
    format!("CREATE TABLE {} ({});", quote_ident(name), defs.join(", "))
}

const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const TIMED_OUT: &str = "__kql_oracle_timeout__";

pub fn cmd_smoke(args: &[String]) -> Result<(), String> {
    let mut queries = "crates/kql-oracle/data/storm_queries.jsonl".to_string();
    let mut csv = "../src/WebDemo/wwwroot/StormEvents.csv.gz".to_string();
    let mut verbose = false;
    let mut engine = Engine::default();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--queries" => queries = it.next().cloned().ok_or("--queries needs a value")?,
            "--csv" => csv = it.next().cloned().ok_or("--csv needs a value")?,
            "--verbose" => verbose = true,
            "--engine" => engine = Engine::parse(it.next().ok_or("--engine needs a value")?)?,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let catalog = storm_catalog();
    let text = std::fs::read_to_string(&queries).map_err(|e| format!("{queries}: {e}"))?;
    let mut items = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).map_err(|e| e.to_string())?;
        let kql = v["kql"].as_str().unwrap_or_default().to_string();
        let tr = kql_to_sql::translate(&kql, &catalog, engine.dialect()).map(|t| t.sql).map_err(|e| e.message);
        items.push((v["file"].clone(), kql, tr));
    }

    // Execution results, one per item (None = not translated).
    let results: Vec<Option<Result<(), String>>> = match engine {
        Engine::DuckDb => {
            let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
            conn.execute_batch("SET memory_limit = '3GB'; SET threads = 2;").map_err(|e| e.to_string())?;
            load_storm(&conn, Path::new(&csv))?;
            items
                .iter()
                .map(|(_, _, tr)| {
                    tr.as_ref().ok().map(|sql| {
                        // interrupt queries that run longer than TIMEOUT
                        let handle = conn.interrupt_handle();
                        let (tx, rx) = std::sync::mpsc::channel::<()>();
                        let watchdog = std::thread::spawn(move || {
                            if rx.recv_timeout(TIMEOUT).is_err() {
                                handle.interrupt();
                                return true;
                            }
                            false
                        });
                        let r = conn.prepare(sql).and_then(|mut s| {
                            let mut rows = s.query([])?;
                            while rows.next()?.is_some() {}
                            Ok(())
                        });
                        let _ = tx.send(());
                        if watchdog.join().unwrap_or(false) {
                            return Err(TIMED_OUT.to_string());
                        }
                        r.map_err(|e| e.to_string())
                    })
                })
                .collect()
        }
        Engine::PgLite => {
            let stmts: Vec<(String, String)> = items
                .iter()
                .enumerate()
                .filter_map(|(i, (_, _, tr))| tr.as_ref().ok().map(|sql| (i.to_string(), sql.clone())))
                .collect();
            let setup = pg::Setup {
                sql: Some(pg_storm_ddl()),
                copies: vec![(
                    kql_to_sql::sql::quote_ident_for("StormEvents", kql_to_sql::Dialect::Postgres),
                    csv.clone().into(),
                )],
            };
            let mut raw = pg::run_batch(&stmts, &setup)?;
            (0..items.len())
                .map(|i| {
                    items[i].2.as_ref().ok().map(|_| match raw.remove(&i.to_string()) {
                        Some(r) => r.error.map_or(Ok(()), Err),
                        None => Err("statement missing from the PGlite batch output".into()),
                    })
                })
                .collect()
        }
    };

    let (mut ok, mut tr_err, mut ex_err, mut slow) = (0, 0, 0, 0);
    let mut tr_groups: BTreeMap<String, usize> = BTreeMap::new();
    let mut ex_groups: BTreeMap<String, usize> = BTreeMap::new();
    for ((file, kql, tr), res) in items.iter().zip(results) {
        match (tr, res) {
            (Err(e), _) => {
                tr_err += 1;
                *tr_groups.entry(e.chars().take(70).collect()).or_default() += 1;
                if verbose {
                    println!("TRANSLATE {file}\n  {kql}\n  {e}");
                }
            }
            (Ok(sql), res) => match res.unwrap_or(Ok(())) {
                Ok(()) => ok += 1,
                Err(m) if m == TIMED_OUT => {
                    slow += 1;
                    println!("SLOW (> {}s) {file}\n  {kql}", TIMEOUT.as_secs());
                }
                Err(m) => {
                    ex_err += 1;
                    *ex_groups.entry(m.lines().next().unwrap_or("").chars().take(90).collect()).or_default() += 1;
                    if verbose {
                        println!("EXECUTE {file}\n  {kql}\n  {sql}\n  {}", m.lines().next().unwrap_or(""));
                    }
                }
            },
        }
    }
    println!(
        "smoke ({engine:?}): {} queries — ok {ok}, translate errors {tr_err}, invalid SQL {ex_err}, timed out {slow}",
        ok + tr_err + ex_err + slow
    );
    if !tr_groups.is_empty() {
        println!("\ntranslate errors:");
        let mut g: Vec<_> = tr_groups.into_iter().collect();
        g.sort_by(|a, b| b.1.cmp(&a.1));
        for (m, n) in g {
            println!("  {n:4}  {m}");
        }
    }
    if !ex_groups.is_empty() {
        println!("\ninvalid SQL:");
        let mut g: Vec<_> = ex_groups.into_iter().collect();
        g.sort_by(|a, b| b.1.cmp(&a.1));
        for (m, n) in g {
            println!("  {n:4}  {m}");
        }
    }
    Ok(())
}
