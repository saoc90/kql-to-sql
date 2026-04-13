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
| Extent (data shard) | — | — | ADX storage internals, no SQL equivalent |
| Ingestion mapping | — | — | ADX-specific format mapping |
| Policy | — | — | ADX engine configuration, no direct SQL equivalent |
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

## Explicitly not translatable

These commands are deeply tied to ADX cluster infrastructure and have no SQL equivalent:

| KQL command | Reason |
|---|---|
| `.show extents` / `.drop extents` / `.move extents` | ADX storage shard management — no concept in SQL engines |
| `.alter table policy *` (retention, caching, merge, partitioning, etc.) | ADX engine policies — SQL engines configure this differently |
| `.alter table policy row_level_security` | ADX-specific RLS — Postgres has `CREATE POLICY`, but the mechanism is fundamentally different |
| `.alter table policy update` | ADX ingest-time triggers — no direct SQL mapping |
| `.create ingestion mapping` | ADX format-mapping for ingestion — SQL engines handle format in `COPY`/`read_*` options |
| `.add database principals` / `.drop database principals` | ADX access control — SQL uses `GRANT`/`REVOKE` |
| `.create entity_group` | ADX cross-database grouping — no equivalent |
| `.undo drop table` | ADX soft-delete recovery — no equivalent in DuckDB/PGlite |
| `.show capacity` | ADX cluster capacity metrics |
