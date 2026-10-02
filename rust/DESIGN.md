# kql-to-sql (Rust) — design notes

## Crates

| crate | role |
|---|---|
| `kql-parser` | Lexer + recursive-descent parser → `ast` (no binding). Grammar/precedence follow Kusto.Language's `QueryParser`. |
| `kql-to-sql` | Binder + SQL generation. `translate(kql, &Catalog, Dialect) -> Translation { sql, columns }`. |
| `kql-oracle` | Differential harness: replays `fuzzing/verdicts3/*.jsonl` (queries with recorded **real Kusto** results) through the translator and DuckDB. |
| `kql-cli` | `kql2sql` command line. |

## Translator pipeline

1. **Parse** (`kql_parser::parse_query`).
2. **Bind** statements (`binder.rs`): `let` → `Env` bindings (scalar `TExpr`, tabular CTE, user function).
   Every table must be described in the `Catalog`; there is no "unknown schema" mode.
3. **Operators** (`ops.rs`, `join.rs`, `aggs.rs`, `advanced.rs`, `op_*.rs`) transform a `Rel`:
   * `Rel { sel: sql::Select, cols: Vec<Column>, order: Vec<OrderSpec> }` — a structured SELECT
     plus the typed output schema and the *logical* row order.
   * An operator either fits into the current `Select` or wraps it (`rel.passthrough(ctx)` /
     `rel.wrap(ctx)`); never inspect SQL text.
   * `rel.project(ctx, items, cols)` replaces the projection (wrapping when needed) and keeps the
     logical order when its columns survive.
   * Logical order is emitted only where it matters: `LIMIT` (take/top), window functions
     (`Scope::order`), order-sensitive aggregates (`make_list`), and the final result.
4. **Expressions** (`expr.rs`, `funcs.rs`, `aggs.rs`, `window.rs`, `funcs_extra.rs`) compile to
   `TExpr { sql, ty, konst, agg, window }`. **`sql` is always atomic** (literal, identifier, call
   or parenthesized), so composing never changes precedence. Types drive every decision
   (integer vs real division, datetime arithmetic, dynamic access).
5. `finish` (lib.rs) adds CTEs, converts timespan ticks to INTERVAL and applies the final order.

## Value representation (all dialects)

| Kusto | in flight | output |
|---|---|---|
| `datetime` | TIMESTAMP (UTC, µs) | TIMESTAMP |
| `timespan` | BIGINT **ticks** (100ns) | INTERVAL |
| `dynamic` | JSON (DuckDB) / JSONB (PG) — never native lists | JSON |
| `string` | VARCHAR, **never NULL** (coalesce sources of NULL to `''`) | VARCHAR |
| `int`/`long`/`real`/`decimal`/`bool`/`guid` | INTEGER/BIGINT/DOUBLE/DOUBLE/BOOLEAN/UUID | same |

Engine-specific SQL goes through `dialect::SqlDialect` (`ctx.d`). DuckDB-only SQL may be written
inline only behind `match ctx.d.kind()`.

Kusto naming (`sum_x`, `count_`, `Column1`, `x1` suffixes) comes from `names.rs` and the
generated function catalog `catalog_data.rs` (`tools/gen_catalog.py`, from Kusto.Language).

## Testing against Kusto

```
cargo build -p kql-oracle --release
./target/release/kql-oracle run [--filter <family-or-id>] [--verbose] [--out v.jsonl]
./target/release/kql-oracle one --kql "<KQL>"      # SQL + DuckDB result
./target/release/kql-oracle one --id <source/Id>   # replay one corpus record with its Kusto result
./target/release/kql-oracle sql "<SQL>"            # probe DuckDB directly
./target/release/kql-oracle smoke                  # StormEvents queries from the C# tests, real data
```

Every command takes `--engine pglite` to score the **Postgres dialect** instead: the SQL runs in
PGlite (PostgreSQL 17 compiled to WASI), hosted in-process by the `pglite-oxide` crate: no
Node.js or other JavaScript runtime is needed. One instance per batch, each statement in
`BEGIN … ROLLBACK` via a chunked cursor; cells come back as PG text and are converted with the
declared column types.

`run` prints outcome counts and the comparison with the C# implementation's recorded verdicts.
A change must not reduce `Match`, and must not add `SqlExecError` (invalid SQL).
