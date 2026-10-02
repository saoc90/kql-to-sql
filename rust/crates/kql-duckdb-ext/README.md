# kql-duckdb-ext — the `kql` DuckDB extension

A DuckDB loadable extension, written in Rust on [duckdb-rs](https://github.com/duckdb/duckdb-rs)'
C-API extension support, that brings the `kql-to-sql` translator into DuckDB. It replaces the
.NET Native-AOT extension in `src/KqlToSql.DuckDbExtension` and keeps its function names.

## Functions

| Function | Returns | Description |
|---|---|---|
| `kql_to_sql(kql VARCHAR)` | `VARCHAR` | Translates KQL to DuckDB SQL. |
| `kql_to_sql_dialect(kql VARCHAR, dialect VARCHAR)` | `VARCHAR` | Translates for `'duckdb'`, `'postgres'` or `'pglite'` (`pglite` = `postgres`). |
| `kql_explain(kql VARCHAR [, dialect := VARCHAR])` | `TABLE(kql_input, sql_output, dialect, columns)` | One row: the query, its SQL, the dialect and the output schema as `"name:type, ..."` (Kusto types). |
| `kql(kql VARCHAR)` | `TABLE(...)` | **Translates and runs** the query and returns its rows. |

Translation is type-directed, so each call reads the schema of the tables and views of the
current database and schema (`duckdb_columns()`); tables created after `LOAD` are visible.
Translator errors (syntax errors, unknown tables/columns, unsupported operators) are raised as
DuckDB errors with the translator's message. `NULL` input gives `NULL` (scalar functions) or an
error (table functions).

### `kql()` column types

Output columns get the DuckDB type of their Kusto type; a column whose generated SQL produces a
different type is cast:

| Kusto | DuckDB |
|---|---|
| `bool` | `BOOLEAN` |
| `int` | `INTEGER` |
| `long` | `BIGINT` |
| `real`, `decimal` | `DOUBLE` |
| `string` | `VARCHAR` |
| `datetime` | `TIMESTAMP` |
| `timespan` | `INTERVAL` |
| `guid` | `UUID` |
| `dynamic` | type produced by the SQL — `JSON` (requires DuckDB's `json` extension, as the translator's dynamic functions do) |

The row order of the query (`order by`, `top`, ...) is preserved.

## Usage

```sql
-- duckdb -unsigned   (or SET allow_unsigned_extensions = true at startup)
LOAD '/path/to/kql.duckdb_extension';

SELECT kql_to_sql('StormEvents | where State == ''TEXAS'' | count');
SELECT kql_to_sql_dialect('StormEvents | summarize count() by State', 'postgres');
SELECT * FROM kql_explain('StormEvents | take 5');

SELECT * FROM kql('StormEvents | summarize count() by State | top 5 by count_');
CREATE TABLE texas AS SELECT * FROM kql('StormEvents | where State == ''TEXAS''');
SELECT s.State, k.Injuries
FROM kql('StormEvents | summarize Injuries = sum(InjuriesDirect) by State') k
JOIN states s USING (State);
```

From Rust (duckdb-rs, `bundled`): open with
`Config::default().allow_unsigned_extensions()?` and `execute_batch("LOAD '<path>'")`.

## Building

```bash
rust/crates/kql-duckdb-ext/build.sh          # build + footer
rust/crates/kql-duckdb-ext/build.sh --test   # ... and run the load test
```

The script

1. runs `cargo build --release` for this crate (a `cdylib`, `libkql.so` / `libkql.dylib`),
2. appends DuckDB's 512-byte extension metadata footer (eight 32-byte fields — magic `4`,
   platform, DuckDB version, extension version, ABI type — plus a 256-byte empty signature; the
   layout of `ParseExtensionMetaData` in `duckdb/src/main/extension/extension_load.cpp`),
3. verifies the result and writes `$CARGO_TARGET_DIR/release/kql.duckdb_extension`
   (default target dir: `rust/target`).

Every step fails the script with a non-zero exit status (missing cargo output, unparsable
versions, size/magic verification).

Overridable through the environment: `CARGO_TARGET_DIR`, `DUCKDB_VERSION` (default: derived from
the duckdb-rs version in `Cargo.lock`, `1.10506.0` → `v1.5.6`), `DUCKDB_PLATFORM` (default:
detected, e.g. `linux_amd64`, `osx_arm64`), `KQL_EXTENSION_VERSION`.

### Why a separate workspace

This crate is excluded from the `rust/` workspace and has its own `Cargo.lock`. duckdb-rs'
`loadable-extension` feature switches `libduckdb-sys` from a linked DuckDB to function pointers
supplied by the host at `LOAD`; Cargo unifies features across a workspace, so it cannot coexist
with the `bundled` DuckDB that `kql-oracle` (and the load test) link.

### Testing

`rust/crates/kql-duckdb-ext-test` (a member of the main workspace) holds the integration test
`tests/load.rs`: it opens the bundled DuckDB with `allow_unsigned_extensions`, creates tables,
`LOAD`s the built file and checks all four functions — translation against the live catalog
(tables and views created after `LOAD`), dialects, errors, `NULL`s, `kql_explain`'s row, `kql()`'s
rows, column types (including `INTERVAL`, `UUID`, `JSON`), ordering, large/streamed results,
`LIMIT`, joins of several `kql()` scans and `CREATE TABLE AS`. It reads the extension from
`KQL_EXTENSION_PATH` (set by `build.sh --test`) or `rust/target/release/kql.duckdb_extension`, and
skips with a message when neither exists, so `cargo test` of the workspace stays green.

## Limitations

* **Pinned DuckDB version.** duckdb-rs' bindings include DuckDB's *unstable* C API (used here:
  `duckdb_vector_reference_vector`, prepared-statement column types, ...), so the footer uses ABI
  `C_STRUCT_UNSTABLE` and DuckDB loads the file only if its version equals the footer's
  (`v1.5.6` for duckdb-rs `1.10506.x`). Rebuild for another DuckDB version by bumping duckdb-rs.
* **Unsigned.** Requires `allow_unsigned_extensions` (`duckdb -unsigned`).
* **Internal connections.** The C extension API offers no way to run a query from inside a
  function call, so on `LOAD` the extension opens internal connections (one for the catalog, one
  shared execution connection, four for streaming `kql()` scans) and keeps them. Consequences:
  * they see only **committed** data: tables created or rows written in the caller's still-open
    transaction are invisible to all four functions;
  * they use the database's default catalog/schema (`USE` on the caller's connection is not
    followed), and temporary tables (connection-local) are not visible;
  * they keep the database instance alive: a database that loaded `kql` is not closed (no final
    checkpoint — data is safe in the WAL — and the file stays locked) until the process exits.
* **`kql()` execution.** Up to four `kql()` scans stream concurrently (chunk by chunk, zero-copy);
  further concurrent scans are *materialized* on the shared connection (whole result in memory,
  serialized with other bind-time work). No projection or filter pushdown into the KQL query: put
  filters into the KQL. Scans are single-threaded on the outer query's side (the inner query itself
  runs in parallel).
* **Catalog types.** DuckDB types without a Kusto counterpart (`BLOB`, `TIME`, `BIT`, ...) are
  treated as `string`. Only the current schema is visible; tables of attached databases or other
  schemas cannot be referenced.
* `| render` instructions are ignored (the `Translation.render` metadata is not surfaced).
* No parser extension: KQL cannot be typed as a statement; use `kql('...')` / `kql_to_sql`.
