//! LOADs the built `kql.duckdb_extension` into the bundled DuckDB and exercises its functions.
//!
//! The extension is built by `crates/kql-duckdb-ext/build.sh` (which runs this test with
//! `--test`). The file is taken from `KQL_EXTENSION_PATH`, else `<target>/release/
//! kql.duckdb_extension`. When `KQL_EXTENSION_PATH` is unset and the default file does not exist
//! the tests are skipped (with a message), so a plain `cargo test` of the workspace stays green.

use std::path::PathBuf;

use duckdb::{Config, Connection};

fn extension_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("KQL_EXTENSION_PATH") {
        let p = PathBuf::from(p);
        assert!(p.is_file(), "KQL_EXTENSION_PATH={} does not exist", p.display());
        return Some(p);
    }
    let target = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
    let p = target.join("release/kql.duckdb_extension");
    if p.is_file() {
        Some(p)
    } else {
        eprintln!("SKIPPED: {} not found; build it with crates/kql-duckdb-ext/build.sh", p.display());
        None
    }
}

/// A fresh in-memory database with the extension loaded and a small StormEvents table.
fn setup() -> Option<Connection> {
    let path = extension_path()?;
    let config = Config::default().allow_unsigned_extensions().unwrap();
    let con = Connection::open_in_memory_with_flags(config).unwrap();
    con.execute_batch(
        "CREATE TABLE StormEvents(State VARCHAR, EventType VARCHAR, InjuriesDirect INTEGER, \
             DamageProperty BIGINT, StartTime TIMESTAMP, Lat DOUBLE, Id UUID);
         INSERT INTO StormEvents VALUES
           ('TEXAS',   'Tornado', 3, 1000, TIMESTAMP '2007-01-01 10:00:00', 30.5, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
           ('TEXAS',   'Flood',   0,  500, TIMESTAMP '2007-01-02 11:00:00', 31.5, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12'),
           ('KANSAS',  'Tornado', 7,    0, TIMESTAMP '2007-02-01 12:00:00', 38.0, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13'),
           ('OHIO',    'Hail',    1,   20, TIMESTAMP '2007-03-01 13:00:00', 40.0, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14'),
           ('KANSAS',  'Hail',    2,   10, TIMESTAMP '2007-03-05 14:00:00', 38.5, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15');",
    )
    .unwrap();
    let load = format!("LOAD '{}'", path.display().to_string().replace('\'', "''"));
    con.execute_batch(&load).unwrap_or_else(|e| panic!("{load} failed: {e}"));
    Some(con)
}

fn string(con: &Connection, sql: &str) -> String {
    con.query_row(sql, [], |r| r.get::<_, String>(0)).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn error(con: &Connection, sql: &str) -> String {
    match con.query_row(sql, [], |r| r.get::<_, Option<String>>(0)) {
        Ok(v) => panic!("{sql}: expected an error, got {v:?}"),
        Err(e) => e.to_string(),
    }
}

/// `DESCRIBE`: (column_name, column_type) pairs.
fn describe(con: &Connection, query: &str) -> Vec<(String, String)> {
    let mut stmt = con.prepare(&format!("DESCRIBE {query}")).unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|r| r.unwrap()).collect()
}

fn strings(con: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = con.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().map(|r| r.unwrap()).collect()
}

#[test]
fn kql_to_sql_translates_against_the_live_catalog() {
    let Some(con) = setup() else { return };
    let sql = string(&con, "SELECT kql_to_sql('StormEvents | where State == ''TEXAS'' | count')");
    assert!(sql.to_uppercase().contains("SELECT"), "{sql}");
    // the generated SQL runs on the same database
    let n: i64 = con.query_row(&sql, [], |r| r.get(0)).unwrap();
    assert_eq!(n, 2);

    // tables created after LOAD are visible (the catalog is re-read on every call)
    con.execute_batch("CREATE TABLE Later(a INTEGER, b VARCHAR); INSERT INTO Later VALUES (1, 'x'), (2, 'y');")
        .unwrap();
    let sql = string(&con, "SELECT kql_to_sql('Later | where a > 1 | project b')");
    assert_eq!(string(&con, &sql), "y");

    // views are part of the catalog too
    con.execute_batch("CREATE VIEW TexasView AS SELECT * FROM StormEvents WHERE State = 'TEXAS';").unwrap();
    let sql = string(&con, "SELECT kql_to_sql('TexasView | count')");
    let n: i64 = con.query_row(&sql, [], |r| r.get(0)).unwrap();
    assert_eq!(n, 2);

    // NULL in, NULL out
    let v: Option<String> = con.query_row("SELECT kql_to_sql(NULL)", [], |r| r.get(0)).unwrap();
    assert_eq!(v, None);

    // one translation per row
    let rows = strings(&con, "SELECT kql_to_sql(q) FROM (VALUES ('StormEvents | take 1'), ('Later | count')) t(q)");
    assert_eq!(rows.len(), 2);

    // translator errors become DuckDB errors
    let e = error(&con, "SELECT kql_to_sql('NoSuchTable | take 1')");
    assert!(e.contains("NoSuchTable"), "{e}");
    let e = error(&con, "SELECT kql_to_sql('StormEvents | where (')");
    assert!(e.to_lowercase().contains("syntax"), "{e}");
}

#[test]
fn kql_to_sql_dialect_selects_the_dialect() {
    let Some(con) = setup() else { return };
    let duck = string(&con, "SELECT kql_to_sql_dialect('StormEvents | summarize count() by State', 'duckdb')");
    assert_eq!(duck, string(&con, "SELECT kql_to_sql('StormEvents | summarize count() by State')"));
    let pg = string(&con, "SELECT kql_to_sql_dialect('StormEvents | summarize count() by State', 'postgres')");
    let pglite = string(&con, "SELECT kql_to_sql_dialect('StormEvents | summarize count() by State', 'PGlite')");
    assert_eq!(pg, pglite);
    assert!(!duck.contains("GROUP BY ALL") || !pg.contains("GROUP BY ALL"), "duckdb: {duck}\npostgres: {pg}");
    let e = error(&con, "SELECT kql_to_sql_dialect('StormEvents | take 1', 'oracle')");
    assert!(e.contains("unknown dialect"), "{e}");
}

#[test]
fn kql_explain_returns_one_row() {
    let Some(con) = setup() else { return };
    let (input, sql, dialect, columns): (String, String, String, String) = con
        .query_row("SELECT * FROM kql_explain('StormEvents | project State, InjuriesDirect | take 2')", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })
        .unwrap();
    assert_eq!(input, "StormEvents | project State, InjuriesDirect | take 2");
    assert_eq!(dialect, "duckdb");
    assert_eq!(columns, "State:string, InjuriesDirect:int");
    assert_eq!(strings(&con, &sql).len(), 2);
    let names: Vec<String> =
        describe(&con, "SELECT * FROM kql_explain('StormEvents | take 1')").into_iter().map(|c| c.0).collect();
    assert_eq!(names, ["kql_input", "sql_output", "dialect", "columns"]);
    let d = string(&con, "SELECT dialect FROM kql_explain('StormEvents | take 1', dialect := 'pglite')");
    assert_eq!(d, "postgres");
    let e = error(&con, "SELECT * FROM kql_explain('Nope | take 1')");
    assert!(e.contains("Nope"), "{e}");
}

#[test]
fn kql_executes_with_kusto_column_types() {
    let Some(con) = setup() else { return };
    let rows: Vec<(String, i64)> = {
        let mut stmt =
            con.prepare("SELECT * FROM kql('StormEvents | summarize count() by State | order by State asc')").unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|r| r.unwrap()).collect()
    };
    assert_eq!(rows, [("KANSAS".to_string(), 2), ("OHIO".to_string(), 1), ("TEXAS".to_string(), 2)]);

    let cols = describe(
        &con,
        "SELECT * FROM kql('StormEvents | extend Span = 1h, Big = tolong(InjuriesDirect), Dec = todecimal(Lat), Flag = InjuriesDirect > 1, Bag = bag_pack(\"s\", State) \
         | project State, InjuriesDirect, Big, Lat, Dec, StartTime, Span, Id, Flag, Bag')",
    );
    let types: Vec<(&str, &str)> = cols.iter().map(|(n, t)| (n.as_str(), t.as_str())).collect();
    assert_eq!(
        types,
        [
            ("State", "VARCHAR"),
            ("InjuriesDirect", "INTEGER"),
            ("Big", "BIGINT"),
            ("Lat", "DOUBLE"),
            ("Dec", "DOUBLE"),
            ("StartTime", "TIMESTAMP"),
            ("Span", "INTERVAL"),
            ("Id", "UUID"),
            ("Flag", "BOOLEAN"),
            ("Bag", "JSON"),
        ]
    );
    let span = string(&con, "SELECT CAST(Span AS VARCHAR) FROM kql('StormEvents | take 1 | project Span = 90m')");
    assert_eq!(span, "01:30:00");
    let bag = string(&con, "SELECT CAST(Bag AS VARCHAR) FROM kql('StormEvents | where Lat == 40.0 | project Bag = bag_pack(\"s\", State)')");
    assert_eq!(bag, r#"{"s":"OHIO"}"#);

    // the logical order of the query is kept
    let inj: Vec<String> = strings(&con, "SELECT CAST(InjuriesDirect AS VARCHAR) FROM kql('StormEvents | order by InjuriesDirect desc | project InjuriesDirect')");
    assert_eq!(inj, ["7", "3", "2", "1", "0"]);

    // composes with SQL
    let n: i64 = con
        .query_row("SELECT sum(count_) FROM kql('StormEvents | summarize count() by EventType')", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 5);

    let e = error(&con, "SELECT * FROM kql('Missing | take 1')");
    assert!(e.contains("Missing"), "{e}");
    let e = error(&con, "SELECT * FROM kql(NULL)");
    assert!(e.contains("NULL"), "{e}");
}

#[test]
fn kql_streams_large_results_and_survives_pool_exhaustion() {
    let Some(con) = setup() else { return };
    con.execute_batch("CREATE TABLE Big AS SELECT range AS x FROM range(300000);").unwrap();
    let n: i64 = con.query_row("SELECT count(*) FROM kql('Big | where x % 2 == 0')", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 150000);
    let n: i64 = con.query_row("SELECT count(*) FROM (SELECT * FROM kql('Big') LIMIT 5)", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 5);
    // more concurrent scans than streaming connections (4): the rest are materialized
    let union = (0..7)
        .map(|i| format!("SELECT count(*) AS c FROM kql('Big | where x % 7 == {i}')"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let total: i64 = con.query_row(&format!("SELECT sum(c) FROM ({union})"), [], |r| r.get(0)).unwrap();
    assert_eq!(total, 300000);
    let joined: i64 = con
        .query_row(
            "SELECT count(*) FROM kql('Big | where x < 1000') a JOIN kql('Big | where x < 2000') b USING (x) \
             JOIN kql('Big | where x < 3000') c USING (x) JOIN kql('Big | where x < 4000') d USING (x) \
             JOIN kql('Big | where x < 5000') e USING (x) JOIN kql('Big | where x < 6000') f USING (x)",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(joined, 1000);
    // repeated scans return their connections to the pool
    for _ in 0..20 {
        let n: i64 = con.query_row("SELECT count(*) FROM kql('Big | take 10')", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 10);
    }
    // the materialized result is usable from CREATE TABLE AS
    con.execute_batch("CREATE TABLE Copy AS SELECT * FROM kql('StormEvents | project State, InjuriesDirect')").unwrap();
    let n: i64 = con.query_row("SELECT count(*) FROM Copy", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 5);
}
