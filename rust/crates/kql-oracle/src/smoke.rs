//! `kql-oracle smoke`: translates the StormEvents queries collected from the C# test suite and
//! executes them against the real StormEvents data. Checks that translation succeeds and that the
//! SQL is valid; it has no Kusto results to compare values with.

use std::collections::BTreeMap;
use std::path::Path;

use duckdb::Connection;
use kql_to_sql::{Catalog, Column, Dialect, KqlType};

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

pub fn cmd_smoke(args: &[String]) -> Result<(), String> {
    let mut queries = "crates/kql-oracle/data/storm_queries.jsonl".to_string();
    let mut csv = "../src/WebDemo/wwwroot/StormEvents.csv.gz".to_string();
    let mut verbose = false;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--queries" => queries = it.next().cloned().ok_or("--queries needs a value")?,
            "--csv" => csv = it.next().cloned().ok_or("--csv needs a value")?,
            "--verbose" => verbose = true,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
    conn.execute_batch("SET memory_limit = '3GB'; SET threads = 2;").map_err(|e| e.to_string())?;
    load_storm(&conn, Path::new(&csv))?;
    let (mut slow, timeout) = (0, std::time::Duration::from_secs(20));
    let catalog = storm_catalog();
    let text = std::fs::read_to_string(&queries).map_err(|e| format!("{queries}: {e}"))?;
    let (mut ok, mut tr_err, mut ex_err) = (0, 0, 0);
    let mut tr_groups: BTreeMap<String, usize> = BTreeMap::new();
    let mut ex_groups: BTreeMap<String, usize> = BTreeMap::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).map_err(|e| e.to_string())?;
        let kql = v["kql"].as_str().unwrap_or_default();
        match kql_to_sql::translate(kql, &catalog, Dialect::DuckDb) {
            Err(e) => {
                tr_err += 1;
                *tr_groups.entry(e.message.chars().take(70).collect()).or_default() += 1;
                if verbose {
                    println!("TRANSLATE {}\n  {kql}\n  {}", v["file"], e.message);
                }
            }
            Ok(t) => {
                // interrupt queries that run longer than `timeout`
                let handle = conn.interrupt_handle();
                let (tx, rx) = std::sync::mpsc::channel::<()>();
                let watchdog = std::thread::spawn(move || {
                    if rx.recv_timeout(timeout).is_err() {
                        handle.interrupt();
                        return true;
                    }
                    false
                });
                let r = conn.prepare(&t.sql).and_then(|mut s| {
                    let mut rows = s.query([])?;
                    while rows.next()?.is_some() {}
                    Ok(())
                });
                let _ = tx.send(());
                if watchdog.join().unwrap_or(false) {
                    slow += 1;
                    println!("SLOW (> {}s) {}\n  {kql}", timeout.as_secs(), v["file"]);
                    continue;
                }
                match r {
                    Ok(()) => ok += 1,
                    Err(e) => {
                        ex_err += 1;
                        let m = e.to_string();
                        *ex_groups.entry(m.lines().next().unwrap_or("").chars().take(90).collect()).or_default() += 1;
                        if verbose {
                            println!("EXECUTE {}\n  {kql}\n  {}\n  {}", v["file"], t.sql, m.lines().next().unwrap_or(""));
                        }
                    }
                }
            }
        }
    }
    println!("smoke: {} queries — ok {ok}, translate errors {tr_err}, invalid SQL {ex_err}, timed out {slow}", ok + tr_err + ex_err + slow);
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
