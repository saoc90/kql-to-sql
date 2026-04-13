# KQL Management Commands — Translation Checklist

Status of translating KQL management (dot) commands to DuckDB and PGlite/PostgreSQL SQL equivalents.

**Legend:** `[x]` implemented, `[ ]` planned, `[-]` no SQL equivalent

> [!NOTE]
> KQL management commands (prefixed with `.`) are distinct from KQL query operators. This checklist covers
> the DDL/DML control commands. For query operator support see [KqlOperatorsChecklist.md](./KqlOperatorsChecklist.md).

## Concept mapping

KQL and SQL have different terminology for similar concepts:

| KQL concept | DuckDB equivalent | PostgreSQL equivalent | Notes |
|---|---|---|---|
| Table | Table | Table | Direct mapping |
| Stored function (`view=true`) | View (`CREATE VIEW`) | View (`CREATE VIEW`) | KQL stored functions with `view=true` behave like SQL views |
| Stored function (`view=false`) | Macro (`CREATE MACRO`) | Function (`CREATE FUNCTION`) | Parameterized reusable queries |
| Materialized view | — | Materialized view (`CREATE MATERIALIZED VIEW`) | DuckDB has no native materialized views |
| Database | Database / Schema | Database / Schema | Direct mapping |
| External table | External table (`read_parquet`, `read_csv`) | Foreign table (`CREATE FOREIGN TABLE`) | Different mechanisms, same idea |
| Extent (data shard) | Row group (Parquet) / block | Page / block | ADX extents ≈ immutable data chunks; similar to Parquet row groups in DuckDB or Postgres pages/blocks |
| Sharding policy | — | — | Controls extent (chunk) sizing; conceptually similar to Parquet row group size settings |
| Partitioning policy | Hive partitioning (`PARTITION BY`) | Table partitioning (`PARTITION BY RANGE/LIST/HASH`) | Both ADX and SQL partition data for query pruning, different mechanisms |
| Retention policy | — | `pg_cron` + `DELETE` / TTL extensions | ADX auto-deletes old data; Postgres requires manual or extension-based TTL |
| Caching policy | — | — | ADX hot/cold tiering; no direct equivalent in single-node SQL engines |
| Merge policy | — | `VACUUM` / `autovacuum` | ADX merges extents for compaction; Postgres reclaims space via vacuum |
| Row order policy | `ORDER BY` in `COPY ... (ORDER BY)` | `CLUSTER ON index` | ADX sorts data within extents; similar to clustered indexes or sorted writes |
| Update policy | — | Trigger (`CREATE TRIGGER`) | ADX runs a query on ingest to populate derived tables; Postgres triggers are the closest match |
| Encoding policy | — | Column compression / `TOAST` | ADX per-column encoding profiles; Postgres uses TOAST and optional compression |
| Row-level security | — | `CREATE POLICY` + `ENABLE ROW LEVEL SECURITY` | Different mechanisms but same goal |
| Ingestion mapping | — | — | ADX-specific format mapping for ingestion |
| Ingestion batching | — | — | ADX queued ingestion tuning; no SQL equivalent |
| Streaming ingestion | — | — | ADX low-latency ingest mode |
| Auto delete policy | — | `pg_cron` + `DROP TABLE` | ADX auto-drops tables at expiry; Postgres requires scheduled jobs |
| `docstring` / `folder` | `COMMENT ON` | `COMMENT ON` | Metadata annotations |
| `.ingest inline` | `INSERT INTO ... VALUES` | `INSERT INTO ... VALUES` | Direct mapping |
| `.ingest into` (from file) | `COPY ... FROM` | `COPY ... FROM` / `\copy` | File-based bulk load |
| `.set <table> <\| <query>` | `CREATE TABLE ... AS (SELECT ...)` | `CREATE TABLE ... AS (SELECT ...)` | CTAS pattern |
| `.append <table> <\| <query>` | `INSERT INTO ... SELECT ...` | `INSERT INTO ... SELECT ...` | Query-to-table append |
| `.export` | `COPY ... TO` | `COPY ... TO` | Export to file |

## Table management

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [x] | `.create table T (Col:type, ...)` | `CREATE TABLE T (Col TYPE, ...)` | `CREATE TABLE T (Col TYPE, ...)` |
| [ ] | `.create tables T1(...), T2(...)` | Multiple `CREATE TABLE` statements | Multiple `CREATE TABLE` statements |
| [ ] | `.create-merge table T (Col:type, ...)` | `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN` | `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN` |
| [ ] | `.drop table T` | `DROP TABLE T` | `DROP TABLE T` |
| [ ] | `.drop tables (T1, T2)` | Multiple `DROP TABLE` statements | Multiple `DROP TABLE` statements |
| [ ] | `.rename table Old to New` | `ALTER TABLE Old RENAME TO New` | `ALTER TABLE Old RENAME TO New` |
| [ ] | `.rename tables New=Old, ...` | Multiple `ALTER TABLE ... RENAME TO` | Multiple `ALTER TABLE ... RENAME TO` |
| [ ] | `.alter table T (Col:type, ...)` | `DROP TABLE` + `CREATE TABLE` (schema replace) | `DROP TABLE` + `CREATE TABLE` (schema replace) |
| [ ] | `.alter-merge table T (Col:type, ...)` | `ALTER TABLE T ADD COLUMN ...` | `ALTER TABLE T ADD COLUMN ...` |
| [ ] | `.clear table T data` | `DELETE FROM T` or `TRUNCATE TABLE T` | `TRUNCATE TABLE T` |
| [ ] | `.show tables` | `SHOW TABLES` / `SELECT * FROM information_schema.tables` | `SELECT * FROM information_schema.tables` |
| [ ] | `.show table T details` | `DESCRIBE T` / `PRAGMA table_info('T')` | `SELECT * FROM information_schema.columns WHERE table_name = 'T'` |
| [ ] | `.show table T schema as json` | `DESCRIBE T` (format as JSON in app layer) | `SELECT * FROM information_schema.columns` (format as JSON) |

## Column management

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [ ] | `.alter column T.Col type=newtype` | `ALTER TABLE T ALTER COLUMN Col TYPE newtype` | `ALTER TABLE T ALTER COLUMN Col TYPE newtype` |
| [ ] | `.drop column T.Col` | `ALTER TABLE T DROP COLUMN Col` | `ALTER TABLE T DROP COLUMN Col` |
| [ ] | `.drop table T columns (C1, C2)` | Multiple `ALTER TABLE T DROP COLUMN` | `ALTER TABLE T DROP COLUMN C1, DROP COLUMN C2` |
| [ ] | `.rename column T.Old to New` | `ALTER TABLE T RENAME COLUMN Old TO New` | `ALTER TABLE T RENAME COLUMN Old TO New` |

## Database management

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [ ] | `.create database Db` | `CREATE SCHEMA Db` | `CREATE DATABASE Db` / `CREATE SCHEMA Db` |
| [ ] | `.drop database Db` | `DROP SCHEMA Db` | `DROP DATABASE Db` / `DROP SCHEMA Db` |
| [ ] | `.show databases` | `SHOW DATABASES` / `SELECT * FROM information_schema.schemata` | `SELECT * FROM information_schema.schemata` |
| [ ] | `.show database schema` | `DESCRIBE` / `information_schema` queries | `information_schema` queries |

## Stored functions and views

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [x] | `.view Name <\| Query` | `CREATE VIEW Name AS ...` | `CREATE VIEW Name AS ...` |
| [ ] | `.create function Name() { Query }` (with `view=true`) | `CREATE VIEW Name AS ...` | `CREATE VIEW Name AS ...` |
| [ ] | `.create function Name() { Query }` (with `view=false`) | `CREATE MACRO Name() AS TABLE ...` | `CREATE FUNCTION Name() RETURNS TABLE ... LANGUAGE SQL` |
| [ ] | `.create-or-alter function Name() { Query }` | `CREATE OR REPLACE VIEW Name AS ...` | `CREATE OR REPLACE VIEW Name AS ...` |
| [ ] | `.alter function Name() { Query }` | `CREATE OR REPLACE VIEW/MACRO` | `CREATE OR REPLACE FUNCTION/VIEW` |
| [ ] | `.drop function Name` | `DROP VIEW Name` / `DROP MACRO Name` | `DROP VIEW Name` / `DROP FUNCTION Name` |
| [ ] | `.show functions` | `SELECT * FROM duckdb_views()` / `duckdb_macros()` | `SELECT * FROM information_schema.routines` / `pg_views` |
| [ ] | `.show function Name` | `SELECT * FROM duckdb_views() WHERE ...` | `SELECT * FROM information_schema.routines WHERE ...` |

## Materialized views

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [ ] | `.create materialized-view V on table T { Q }` | — (not supported) | `CREATE MATERIALIZED VIEW V AS ...` |
| [ ] | `.alter materialized-view V on table T { Q }` | — | `DROP + CREATE MATERIALIZED VIEW` |
| [ ] | `.drop materialized-view V` | — | `DROP MATERIALIZED VIEW V` |
| [ ] | `.show materialized-views` | — | `SELECT * FROM pg_matviews` |
| [-] | `.enable materialized-view V` | — | — (Postgres refreshes manually) |
| [-] | `.disable materialized-view V` | — | — |
| [-] | `.rename materialized-view Old to New` | — | `ALTER MATERIALIZED VIEW Old RENAME TO New` |

> [!NOTE]
> DuckDB does not have native materialized views. Materialized view commands can only target the PGlite dialect.

## Data ingestion

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [x] | `.ingest inline into table T <\| data` | `INSERT INTO T VALUES (...)` | `INSERT INTO T VALUES (...)` |
| [x] | `.ingest into table T 'path'` | `COPY T FROM 'path' (HEADER, AUTO_DETECT TRUE)` | `COPY T FROM 'path' WITH (FORMAT csv, HEADER true)` |
| [ ] | `.set T <\| Query` | `CREATE TABLE T AS (SELECT ...)` | `CREATE TABLE T AS (SELECT ...)` |
| [ ] | `.append T <\| Query` | `INSERT INTO T SELECT ...` | `INSERT INTO T SELECT ...` |
| [ ] | `.set-or-append T <\| Query` | `CREATE TABLE IF NOT EXISTS` + `INSERT INTO ... SELECT` | `CREATE TABLE IF NOT EXISTS` + `INSERT INTO ... SELECT` |
| [ ] | `.set-or-replace T <\| Query` | `DROP TABLE IF EXISTS` + `CREATE TABLE AS` | `DROP TABLE IF EXISTS` + `CREATE TABLE AS` |

## Data export

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [ ] | `.export to csv ('path') <\| Query` | `COPY (SELECT ...) TO 'path' (FORMAT csv)` | `COPY (SELECT ...) TO 'path' WITH (FORMAT csv)` |
| [ ] | `.export to parquet ('path') <\| Query` | `COPY (SELECT ...) TO 'path' (FORMAT parquet)` | — (requires extension) |
| [ ] | `.export to json ('path') <\| Query` | `COPY (SELECT ...) TO 'path' (FORMAT json)` | `COPY (SELECT ...) TO 'path' WITH (FORMAT csv)` (no native JSON) |

## External tables

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [ ] | `.create external table T (...) kind=storage dataformat=parquet ('uri')` | `CREATE VIEW T AS SELECT * FROM read_parquet('uri')` | `CREATE FOREIGN TABLE T (...) SERVER ... OPTIONS (filename 'uri')` |
| [ ] | `.create external table T (...) kind=storage dataformat=csv ('uri')` | `CREATE VIEW T AS SELECT * FROM read_csv('uri')` | `CREATE FOREIGN TABLE T (...)` |
| [ ] | `.drop external table T` | `DROP VIEW T` | `DROP FOREIGN TABLE T` |
| [ ] | `.show external tables` | `SELECT * FROM duckdb_views()` (filter convention) | `SELECT * FROM information_schema.foreign_tables` |

> [!NOTE]
> DuckDB handles external data via its `read_parquet()`, `read_csv()`, `read_json()` table functions.
> The most natural mapping is a view wrapping these functions. PostgreSQL uses the foreign data wrapper (FDW) mechanism.

## Monitoring and diagnostics

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [ ] | `.show queries` | — | `SELECT * FROM pg_stat_activity` |
| [ ] | `.show running queries` | — | `SELECT * FROM pg_stat_activity WHERE state = 'active'` |
| [-] | `.cancel query "id"` | — | `SELECT pg_cancel_backend(pid)` |
| [-] | `.show commands` | — | — (no equivalent) |
| [-] | `.show journal` | — | — (no equivalent) |
| [-] | `.show operations` | — | — (no equivalent) |
| [ ] | `.show version` | `SELECT version()` | `SELECT version()` |

## Metadata annotations

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [ ] | `.alter table T docstring "desc"` | `COMMENT ON TABLE T IS 'desc'` | `COMMENT ON TABLE T IS 'desc'` |
| [ ] | `.alter table T folder "path"` | — (no equivalent) | — (no equivalent) |
| [ ] | `.alter function F docstring "desc"` | `COMMENT ON VIEW F IS 'desc'` | `COMMENT ON VIEW F IS 'desc'` |
| [ ] | `.alter column T.C docstring "desc"` | `COMMENT ON COLUMN T.C IS 'desc'` | `COMMENT ON COLUMN T.C IS 'desc'` |

## Policies — partitioning

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [ ] | `.alter table T policy partitioning '{...}'` | Hive partitioning via `COPY ... (PARTITION_BY ...)` | `ALTER TABLE T PARTITION BY RANGE/LIST/HASH (col)` (at create time) |
| [ ] | `.show table T policy partitioning` | — (inspect Parquet file layout) | `SELECT * FROM pg_partitioned_table WHERE ...` |
| [ ] | `.delete table T policy partitioning` | — | — (requires recreating table) |

> [!NOTE]
> ADX partitioning rearranges data within extents for query pruning. DuckDB uses Hive-style partitioning
> on Parquet files. PostgreSQL uses declarative partitioning at table creation. The concepts align
> (skip irrelevant data during scans) but the mechanisms differ significantly.

## Policies — retention and auto-delete

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [-] | `.alter table T policy retention '{...}'` | — | — (use `pg_cron` + `DELETE WHERE ts < now() - interval`) |
| [-] | `.show table T policy retention` | — | — |
| [-] | `.delete table T policy retention` | — | — |
| [-] | `.alter table T policy auto_delete '{...}'` | — | — (use `pg_cron` + `DROP TABLE`) |
| [-] | `.show table T policy auto_delete` | — | — |

> [!NOTE]
> ADX automatically garbage-collects data older than the retention period. SQL engines don't have built-in
> TTL — this requires application-level scheduling (e.g. `pg_cron`, cron jobs).

## Policies — row order and sharding

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [-] | `.alter table T policy roworder (Col asc)` | `COPY ... (ORDER BY col)` (sorted Parquet writes) | `CLUSTER T USING index_name` |
| [-] | `.show table T policy roworder` | — | — |
| [-] | `.alter table T policy sharding '{...}'` | — (Parquet row group size settings) | — (page/block size is system-level) |
| [-] | `.show table T policy sharding` | — | — |

> [!NOTE]
> ADX extents are immutable columnar data chunks (typically 1M rows). The **sharding policy** controls
> their size. This is conceptually similar to Parquet **row groups** in DuckDB or **pages/blocks** in
> PostgreSQL — all are the unit of I/O and compression. The **row order policy** controls sort order
> within these chunks, similar to sorted Parquet writes or PostgreSQL `CLUSTER`.

## Policies — merge and encoding

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [-] | `.alter table T policy merge '{...}'` | — | `VACUUM` / `autovacuum` settings (conceptually similar) |
| [-] | `.show table T policy merge` | — | — |
| [-] | `.alter column T.C policy encoding type=...` | — | — (Postgres uses TOAST + compression internally) |
| [-] | `.show table T policy encoding` | — | — |

> [!NOTE]
> ADX **merge policy** controls how small extents are compacted into larger ones (background compaction).
> PostgreSQL's `VACUUM`/`autovacuum` serves a similar purpose — reclaiming dead tuple space and
> reorganizing data. DuckDB's columnar storage handles this transparently.
>
> ADX **encoding policy** controls per-column compression profiles (`Identifier`, `BigObject`, `Vector16`).
> This is similar to PostgreSQL TOAST strategies or DuckDB's automatic columnar compression, but ADX
> exposes it as an explicit user-controlled setting.

## Policies — update (triggers)

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [-] | `.alter table T policy update '[{...}]'` | — | `CREATE TRIGGER` + trigger function |
| [-] | `.show table T policy update` | — | `SELECT * FROM information_schema.triggers` |
| [-] | `.delete table T policy update` | — | `DROP TRIGGER` |

> [!NOTE]
> ADX **update policies** run a transformation query whenever new data arrives in a source table,
> writing results to a target table. This is the ADX equivalent of PostgreSQL `AFTER INSERT` triggers.
> DuckDB does not support triggers.

## Policies — row-level security

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [-] | `.alter table T policy row_level_security enable "fn"` | — | `ALTER TABLE T ENABLE ROW LEVEL SECURITY` + `CREATE POLICY` |
| [-] | `.show table T policy row_level_security` | — | `SELECT * FROM pg_policies WHERE tablename = 'T'` |
| [-] | `.delete table T policy row_level_security` | — | `DROP POLICY ... ON T` + `ALTER TABLE T DISABLE ROW LEVEL SECURITY` |

> [!NOTE]
> Both ADX and PostgreSQL support row-level security but with different models. ADX uses a KQL function
> that replaces all access to the table. PostgreSQL uses policies with `USING` clauses attached to roles.
> DuckDB does not support RLS.

## Policies — caching, streaming ingestion, ingestion batching

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [-] | `.alter table T policy caching hot = 30d` | — | — |
| [-] | `.alter table T policy streamingingestion '{...}'` | — | — |
| [-] | `.alter table T policy ingestionbatching '{...}'` | — | — |

> [!NOTE]
> These are ADX-specific engine tuning policies with no SQL equivalent:
> - **Caching** controls hot (SSD) vs cold (remote storage) tiering
> - **Streaming ingestion** enables sub-second ingest latency
> - **Ingestion batching** tunes how queued ingestion groups small chunks

## Extents (data shards)

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [-] | `.show table T extents` | — | `SELECT * FROM pg_stat_user_tables WHERE relname = 'T'` (block-level stats) |
| [-] | `.show database Db extents` | — | `SELECT * FROM pg_stat_user_tables` |
| [-] | `.drop extents from T where ...` | — | — |
| [-] | `.move extents from T1 to T2` | — | — |
| [-] | `.replace extents in table T <\| ...` | — | — |

> [!NOTE]
> ADX **extents** are immutable columnar data shards — the fundamental unit of storage and query.
> Each extent is a self-contained chunk of compressed, sorted columnar data (similar to a Parquet file).
>
> | ADX concept | DuckDB analogy | PostgreSQL analogy |
> |---|---|---|
> | Extent | Parquet row group | Heap page / block (8KB) |
> | Extent creation | Writing a new Parquet file/row group | Inserting rows into new pages |
> | Extent merge | — (transparent) | `VACUUM FULL` / `CLUSTER` |
> | Extent tagging | — | — |
> | Extent drop | Deleting a Parquet file | `VACUUM` (reclaims dead pages) |
>
> SQL engines don't expose individual storage shards as first-class objects the way ADX does.
> Extent-level operations (drop, move, replace) have no direct SQL equivalent.

## Ingestion mappings

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [-] | `.create table T ingestion csv mapping 'Name' '[...]'` | — (handled in `read_csv` options) | — (handled in `COPY` options) |
| [-] | `.show table T ingestion csv mappings` | — | — |
| [-] | `.drop table T ingestion csv mapping 'Name'` | — | — |

> [!NOTE]
> ADX ingestion mappings define how source file columns/fields map to table columns during ingestion,
> supporting CSV, JSON, Parquet, Avro, and ORC formats. In SQL engines this is handled inline:
> DuckDB uses `read_csv(columns=...)` / `read_json(columns=...)` parameters, PostgreSQL uses
> `COPY ... WITH (FORMAT ..., HEADER ...)` options and column lists.

## Access control

| Status | KQL command | DuckDB SQL | PGlite/PostgreSQL SQL |
|---|---|---|---|
| [-] | `.add database Db admins ('user')` | — | `GRANT ALL ON DATABASE Db TO user` |
| [-] | `.drop database Db admins ('user')` | — | `REVOKE ALL ON DATABASE Db FROM user` |
| [-] | `.show database Db principals` | — | `SELECT * FROM information_schema.role_table_grants` |

> [!NOTE]
> ADX uses a role-based principal model (`admins`, `viewers`, `ingestors`, etc.) tied to AAD identities.
> PostgreSQL uses `GRANT`/`REVOKE` with roles. DuckDB has no built-in access control (single-user engine).

## Explicitly not translatable

These commands have no meaningful SQL equivalent in any dialect:

| KQL command | Reason |
|---|---|
| `.undo drop table` | ADX soft-delete recovery with versioning — SQL `DROP` is permanent |
| `.show capacity` | ADX cluster resource capacity metrics |
| `.show journal` | ADX metadata change audit log |
| `.show operations` | ADX async operation tracking |
| `.create entity_group` | ADX cross-database table grouping |
