# kql-to-sql (Rust)

Translates [Kusto Query Language (KQL)](https://learn.microsoft.com/en-us/kusto/query/) into SQL for
**DuckDB** and **PostgreSQL** (including PGlite). This is a rewrite of the original C# translator; see
[DESIGN.md](DESIGN.md) for the architecture.

## Why it is different

* **Type-directed.** Every column of every table is bound to a Kusto type before any SQL is written,
  so integer vs. real division, datetime/timespan arithmetic, `dynamic` access and Kusto's naming
  rules (`count_`, `sum_x`, `Column1`, `x1`) follow the types, not guesses about SQL text.
* **Structured SQL model.** Operators decide whether their clause fits into the current `SELECT` or
  needs a subquery; no string searching for `ORDER BY`/`LIMIT`, so precedence and clause order are
  always correct (`where a | where b or c` → `WHERE (a) AND ((b) OR (c))`).
* **Checked against a real Kusto engine.** `kql-oracle` replays ~1,660 queries whose results were
  recorded on Kusto (Kustainer) and compares them with what the generated SQL returns, on DuckDB and
  on PostgreSQL (PGlite). It also rejects what Kusto rejects (reserved words, type errors, ...).
* **No shared state.** Each `translate` call is self-contained; safe from any number of threads.

| Oracle (exact matches with Kusto, of 1,662) | C# translator | Rust, DuckDB | Rust, PostgreSQL |
|---|---|---|---|
| Match | 1,073 | 1,435 | 1,404 |
| Invalid SQL | 50 | 0 | 1 |

## Crates

| crate | |
|---|---|
| [`kql-parser`](crates/kql-parser) | Lexer and parser (grammar and precedence ported from Microsoft's Kusto.Language, Apache-2.0). |
| [`kql-to-sql`](crates/kql-to-sql) | The translator: queries and management commands. |
| [`kql-cli`](crates/kql-cli) | `kql2sql` command line. |
| [`kql-wasm`](crates/kql-wasm) | WebAssembly bindings used by the web demo (`src/WebDemo`). |
| [`kql-duckdb-ext`](crates/kql-duckdb-ext) | DuckDB loadable extension: `kql_to_sql(...)`, `kql_explain(...)`, `kql(...)` table function. |
| [`kql-oracle`](crates/kql-oracle) | Differential test harness (DuckDB and PGlite), StormEvents smoke test. |

## Library

```rust
use kql_to_sql::{translate, Catalog, Column, Dialect, KqlType};

let catalog = Catalog::new().with_table("StormEvents", vec![
    Column::new("State", KqlType::String),
    Column::new("StartTime", KqlType::DateTime),
    Column::new("InjuriesDirect", KqlType::Int),
]);
let t = translate(
    "StormEvents | where State == 'TEXAS' | summarize n = count(), inj = sum(InjuriesDirect) by bin(StartTime, 7d)",
    &catalog,
    Dialect::DuckDb,
)?;
println!("{}", t.sql);          // SQL to execute
println!("{:?}", t.columns);    // output columns with Kusto types
println!("{:?}", t.render);     // `| render` chart metadata, if any
```

Every referenced table must be in the `Catalog` (`KqlType::from_sql_type` maps DuckDB/PostgreSQL
type names, e.g. from `information_schema.columns`). Value representation: `datetime` → TIMESTAMP,
`timespan` → INTERVAL in results (ticks while computing), `dynamic` → JSON/JSONB, strings never NULL.

Management commands (`.create table`, `.alter table`, `.set-or-append`, `.ingest inline`,
`.show tables`, ...) go through `translate_command`, which returns statements to run in order.
`.alter table` changes only the columns that differ (data is kept). Commands that would touch the
file system or external storage (`.export`, external tables, ingestion from URIs) are rejected.

## Command line

```sh
cargo run -p kql-cli -- --table 'T(a:long, s:string, t:datetime)' "T | where s has 'x' | take 10"
cargo run -p kql-cli -- --dialect postgres --schema schema.json < query.kql
```

## Testing

```sh
cargo test --workspace --release
cargo run -p kql-oracle --release -- run                    # Kusto oracle on DuckDB
cargo run -p kql-oracle --release -- run --engine pglite    # Kusto oracle on PostgreSQL (in-process PGlite, no Node)
cargo run -p kql-oracle --release -- one --kql "<KQL>"       # SQL + result for one query
cargo run -p kql-oracle --release -- smoke                  # C# suite's StormEvents queries on real data
```

CI (`.github/workflows/rust.yml`) gates on the oracle: a change may not reduce the number of matches
or produce invalid SQL.

## Web demo

`scripts/build-wasm.sh` builds `kql-wasm` into `src/WebDemo/wwwroot/kql-wasm/`; the demo passes the
schemas of its DuckDB-WASM / PGlite tables to `translate` on every call.
