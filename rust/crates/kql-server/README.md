# kql-server

A Kusto-compatible HTTP API over DuckDB. KQL is translated with `kql-to-sql` and executed by an
embedded DuckDB; responses use the Kusto REST v1/v2 shapes so Kusto clients (Kusto.Explorer,
Azure Data Studio, the azure-kusto SDKs) can talk to it. It replaces the C# `src/KustoApi` server
and keeps its protocol, without that server's security problems.

## Usage

```
cargo run -p kql-server --release -- \
    --db storm.duckdb --load-storm-events ../src/WebDemo/wwwroot/StormEvents.csv.gz

curl -s localhost:8080/v2/rest/query -H 'content-type: application/json' \
     -d '{"db":"StormEvents","csl":"StormEvents | summarize count() by State | top 3 by count_"}'
```

| flag | default | meaning |
|---|---|---|
| `--bind <addr>` | `127.0.0.1:8080` | listen address (loopback only by default) |
| `--db <file>` | in-memory | DuckDB database file |
| `--read-only` | off | open `--db` read-only |
| `--load-storm-events <csv.gz>` | – | create table `StormEvents` with the Kusto (ADX help cluster) schema at startup |
| `--token <t>` / `KQL_SERVER_TOKEN` | – | require `Authorization: Bearer <t>` |
| `--cors-origin <origin>` (repeatable) | none | allow browser requests from these origins |
| `--allow-commands` | off | allow management commands that change the database |
| `--max-rows <n>` | 500000 | row limit per result |
| `--max-result-bytes <n>` | 64 MiB | approximate result-size limit |
| `--max-body-bytes <n>` | 16 MiB | request body limit |
| `--timeout-secs <n>` | 60 | per-request execution timeout (interrupts DuckDB) |
| `--max-concurrent <n>` | 8 | queries executing at once (others wait) |
| `--memory-limit`, `--threads` | DuckDB defaults | DuckDB resources |
| `--database-name <name>` | `NetDefaultDB` | name reported when the request has no `db` |

With the .NET SDK and a token: `new KustoConnectionStringBuilder("http://localhost:8080").WithAadUserTokenAuthentication("<token>")`.

## Endpoints

Request bodies are `{"csl": "...", "db": "...", "properties": {...}}` (field names
case-insensitive; `properties` may also be a JSON string; `db` only affects the reported database
name — there is one database).

| endpoint | auth | response |
|---|---|---|
| `POST /v1/rest/query` | yes | `{"Tables":[{"TableName":"PrimaryResult","Columns":[{ColumnName,DataType,ColumnType}],"Rows":[...]}]}` |
| `POST /v2/rest/query` | yes | frames: `DataSetHeader`, `@ExtendedProperties` (`QueryProperties`, the `Visualization` row from `\| render`), `PrimaryResult`, `QueryCompletionInformation`, `DataSetCompletion`. With `properties.Options.results_progressive_enabled = true`: `DataSetHeader`, `TableHeader`, `TableFragment`, `TableCompletion`, `DataSetCompletion` (plus `@ExtendedProperties` when the query renders) |
| `POST /v1/rest/mgmt` | yes | v1 tables (see below) |
| `GET /v1/rest/metadata/{db}` | yes | v2 frames listing `TableName, ColumnName, DataType` |
| `GET /v1/rest/ping` | no | `{"ApplicationHealthState":"Healthy"}` |
| `GET /v1/rest/auth/metadata` | no | the public-cloud metadata document the SDKs fetch before connecting |

Management commands answered by the server itself (always available):

* `.show version`, `.show databases`, `.show tables` (`TableName, DatabaseName, Folder, DocString`),
  `.show cluster monitoring`
* `.show table T schema` — rows `ColumnName, DataType (.NET), ColumnType (Kusto)`
* `.show table T schema as json` / `.show table T cslschema` — Kusto's `TableName, Schema, DatabaseName, Folder, DocString`
* `.show databases schema as json`, `.show database [X] schema as json`, `.show schema as json` — one
  `DatabaseSchema` cell with `{"Databases":{...}}` JSON text
* `.show databases as json` — the schema object itself, unwrapped (C# server compatibility)

Other `.show` commands go through `kql_to_sql::translate_command`. With `--allow-commands`, every
command `translate_command` supports (`.create`/`.alter`/`.drop`/`.rename` table/column/function,
`.ingest inline`, `.set`/`.append`/`.set-or-append`/`.set-or-replace`) runs in one transaction; the
schema cache is invalidated afterwards.

### Values

Column types come from the translator (`bool, int, long, real, decimal, string, datetime, timespan,
guid, dynamic`); v1 `DataType` uses .NET names (`Boolean, Int32, Int64, Double, Decimal, String,
DateTime, TimeSpan, Guid, Object`). Datetimes are `yyyy-MM-ddTHH:mm:ss.fffffffZ`, timespans
`[-][d.]hh:mm:ss[.fffffff]`, dynamic values are inline JSON, decimals strings, NaN/±Infinity the
strings `"NaN"`/`"Infinity"`/`"-Infinity"`, null strings `""`.

### Errors

All endpoints answer errors with Kusto's envelope:
`{"error":{"code","message","@type","@message","@permanent"}}`.

| status | code | when |
|---|---|---|
| 400 | `General_BadRequest` | bad body, KQL syntax/semantic error, unknown table, a command sent to a query endpoint, unknown `.show` |
| 401 | `Unauthorized` | token missing or wrong |
| 403 | `Forbidden` | non-`.show` command without `--allow-commands` |
| 404 | `General_NotFound` | unknown endpoint |
| 500 | `General_InternalServerError` | DuckDB failed executing the SQL |
| 504 | `Request_ExecutionTimeout` | `--timeout-secs` exceeded (the query is interrupted) |

### Result limits

When a result exceeds `--max-rows` (or `--max-result-bytes`), the server returns the first
`max-rows` rows **as a partial query failure, the way Kusto does**: HTTP 200, and

* v2: `DataSetCompletion.HasErrors = true` with `OneApiErrors[0].error` =
  `LimitsExceeded` / `"Query result set has exceeded the internal record count limit N (E_QUERY_RESULT_SET_TOO_LARGE; ...)"`,
  plus an `Error` row in `QueryCompletionInformation` (progressive mode: on `TableCompletion` too);
* v1: an extra `QueryStatus` table with the error row.

Rows are read with the limit applied (no unbounded buffering). Clients may lower the limits with
`properties.Options.truncationmaxrecords` / `truncationmaxsize`, never raise them.

## Security model

* **Bind loopback by default.** A warning is printed when binding elsewhere without a token.
* **Authentication:** optional static bearer token (`--token` / `KQL_SERVER_TOKEN`), compared in
  constant time, required on every endpoint except `ping` and `auth/metadata`. There is no AAD;
  use TLS termination (reverse proxy) when the token crosses a network.
* **CORS off by default**; `--cors-origin` allowlists exact origins (no wildcard). Preflights pass
  without the token; the actual request still needs it.
* **Queries can't run commands**: the query endpoints reject anything `is_command` recognises.
  Mutating commands need `--allow-commands`.
* **No SQL injection**: KQL and commands are translated by the token-based translator (identifiers
  quoted, literals escaped); built-in `.show` handlers only look names up in the schema catalog and
  never splice request text into SQL. Each request runs one prepared statement (DuckDB refuses
  multiple statements in `prepare`).
* **Engine lockdown**: after optional data loading the server sets
  `enable_external_access = false` (no `read_csv`/`COPY`/`ATTACH`/`INSTALL`/http), disables
  extension auto-install/auto-load, and sets `lock_configuration = true`, so no query can re-enable
  any of it. `--read-only` opens the file read-only.
* **Isolation / resources**: per-request DuckDB connection (`try_clone`); no shared mutable
  translator state; row/size/body limits; execution timeout via DuckDB's interrupt handle; a
  concurrency limit.
* **No downloads at runtime**: DuckDB is compiled in (`duckdb` crate, `bundled`); the StormEvents
  data is only loaded from a local file given on the command line, with Kusto's schema.

## Differences from Kusto

* One database; `db` in requests is ignored (only echoed by `.show databases`/`.show tables`).
* v1 query responses contain only `PrimaryResult` (plus `QueryStatus` on truncation), not Kusto's
  `QueryProperties`/`QueryStatus`/TOC tables.
* `QueryCompletionInformation` rows are synthetic; `IsQuerySorted` in the visualization is always false.
* Bearer token instead of AAD; `/v1/rest/auth/metadata` returns the public-cloud document so the
  SDKs accept the endpoint, but the server never validates AAD tokens.
* Functions created by `.create function` are DuckDB views; `.show ... schema as json` lists them
  under neither `Tables` nor `Functions`.
* Only what `kql-to-sql` translates is supported; no `.export`, external tables or ingestion from
  URIs (deliberately).
* Timeouts are HTTP 504 with `Request_ExecutionTimeout`.
