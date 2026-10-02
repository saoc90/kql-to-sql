//! Management commands executed end to end on DuckDB.

use duckdb::Connection;
use kql_to_sql::{translate, translate_command, Catalog, Column, Dialect, KqlType};

/// Reads the current schema of every table (as the API server does before each command).
fn catalog(conn: &Connection) -> Catalog {
    let mut stmt = conn
        .prepare("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' ORDER BY table_name, ordinal_position")
        .unwrap();
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))).unwrap();
    let mut tables: Vec<(String, Vec<Column>)> = Vec::new();
    for row in rows {
        let (t, c, ty) = row.unwrap();
        let ty = KqlType::from_sql_type(&ty).unwrap_or(KqlType::String);
        match tables.iter_mut().find(|(n, _)| *n == t) {
            Some((_, cols)) => cols.push(Column::new(c, ty)),
            None => tables.push((t, vec![Column::new(c, ty)])),
        }
    }
    tables.into_iter().fold(Catalog::new(), |cat, (t, cols)| cat.with_table(t, cols))
}

fn run(conn: &Connection, cmd: &str) {
    let t = translate_command(cmd, &catalog(conn), Dialect::DuckDb).unwrap_or_else(|e| panic!("{cmd}: {e}"));
    for s in &t.statements {
        conn.execute_batch(s).unwrap_or_else(|e| panic!("{cmd}\n{s}\n{e}"));
    }
}

fn query(conn: &Connection, kql: &str) -> Vec<String> {
    let t = translate(kql, &catalog(conn), Dialect::DuckDb).unwrap();
    let mut stmt = conn.prepare(&t.sql).unwrap();
    let n = t.columns.len();
    let rows = stmt
        .query_map([], |r| Ok((0..n).map(|i| format!("{:?}", r.get::<_, duckdb::types::Value>(i).unwrap())).collect::<Vec<_>>().join("|")))
        .unwrap();
    rows.map(Result::unwrap).collect()
}

#[test]
fn table_lifecycle() {
    let conn = Connection::open_in_memory().unwrap();
    run(&conn, ".create table Events (Id:long, Name:string)");
    run(&conn, ".ingest inline into table Events <|\n1,alpha\n2,\"b, \"\"quoted\"\"\"\n3,");
    assert_eq!(query(&conn, "Events | count"), vec!["BigInt(3)"]);
    run(&conn, ".set-or-append Events <| print Id = 4, Name = 'delta'");
    run(&conn, ".set-or-append Copy <| Events | where Id > 2");
    assert_eq!(query(&conn, "Copy | summarize n = count()"), vec!["BigInt(2)"]);
    // .alter keeps the data of retained columns
    run(&conn, ".alter table Events (Id:long, Name:string, Score:real)");
    assert_eq!(query(&conn, "Events | where Id == 2 | project Name"), vec!["Text(\"b, \\\"quoted\\\"\")"]);
    run(&conn, ".alter-merge table Events (Extra:int)");
    run(&conn, ".alter-merge table Events (Extra:int)"); // idempotent
    run(&conn, ".rename column Events.Score to Rating");
    run(&conn, ".drop table Events columns (Extra)");
    assert_eq!(query(&conn, "Events | getschema | project ColumnName"), vec!["Text(\"Id\")", "Text(\"Name\")", "Text(\"Rating\")"]);
    run(&conn, ".create-or-alter function Big() { Events | where Id >= 3 }");
    assert_eq!(query(&conn, "Big | count"), vec!["BigInt(2)"]);
    run(&conn, ".drop function Big");
    run(&conn, ".drop tables (Events, Copy) ifexists");
    assert!(catalog(&conn).tables.is_empty());
}
