//! Parses every query in the fuzzing verdict corpus and reports failures on queries Kusto accepted.
use std::{env, fs};

fn main() {
    let dir = env::args().nth(1).unwrap_or_else(|| "../fuzzing/verdicts3".into());
    let (mut ok, mut failed) = (0, 0);
    let mut paths: Vec<_> = fs::read_dir(&dir).unwrap().map(|e| e.unwrap().path()).collect();
    paths.sort();
    for path in paths {
        for line in fs::read_to_string(&path).unwrap().lines() {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            let kql = v["Kql"].as_str().unwrap();
            let kusto_ok = v["Outcome"] != "KustoError";
            match kql_parser::parse_query(kql) {
                Ok(_) => ok += 1,
                Err(e) if kusto_ok => {
                    failed += 1;
                    println!("{}: {e}\n    {kql}", v["Id"]);
                }
                Err(_) => {}
            }
        }
    }
    println!("parsed {ok}, failed {failed}");
}
