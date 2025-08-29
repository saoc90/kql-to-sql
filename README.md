# KQL to SQL Translator 

Simple, pragmatic KQL-to-SQL translator built on top of the official Kusto language parser. The core library converts Kusto Query Language (KQL) pipelines into executable SQL so you can run familiar KQL against SQL engines.

- Live demo: https://saoc90.github.io/kql-to-sql/

## Why this exists

- Bring the KQL experience to SQL-based backends (on‑prem, embedded, or cloud) without ADX.
- Enable small-scale analytics at the edge where a light SQL engine is preferred.
- Keep KQL as the authoring language while choosing the persistence backend that fits your constraints.

## What’s in the repo

- Core library: KqlToSql (C#/.NET) – translates KQL to SQL.
- Demo: Blazor WebAssembly site with an in-browser analytics engine powered by DuckDB (and optional PGLite). No server required.
- PoC API: Minimal Kusto API compatibility surface for basic flows with Kusto.Explorer and Azure Data Explorer (web).

## Code examples

- Translate KQL to SQL and execute on DuckDB (C#)

```csharp
using KqlToSql;
using DuckDB.NET.Data;

var kql = @"StormEvents
| where EventType == 'Tornado' and State in~ ('TX','OK','KS')
| summarize cnt = count() by State
| sort by cnt desc
| take 5";

var converter = new KqlToSqlConverter();
var sql = converter.Convert(kql);

using var conn = new DuckDBConnection("DataSource=StormEvents.duckdb");
conn.Open();
using var cmd = conn.CreateCommand();
cmd.CommandText = sql;
using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    var state = reader.GetString(0);
    var cnt = Convert.ToInt32(reader.GetValue(1));
    Console.WriteLine($"{state}: {cnt}");
}
```

- Run the in-browser demo (DuckDB, optional PGLite)

```bash
cd src/DuckDbDemo
dotnet run
```

Open the printed local URL and use the KQL editor to run queries client-side.

## Supported KQL features

See the up-to-date feature matrix in KqlOperatorsChecklist.md:
- Operators (tabular)
- Scalar functions
- Aggregate functions
- Control commands

[View KqlOperatorsChecklist.md](./KqlOperatorsChecklist.md)

## Demo – all in the browser

- DuckDB demo (`src/DuckDbDemo`): WebAssembly app that runs queries entirely client‑side against DuckDB using the generated SQL.
- Also supports PGLite as an alternative in‑browser SQL engine (experimental). Pick the engine that fits your footprint.
- Bundled StormEvents sample data for quick exploration.
- Live demo: https://saoc90.github.io/kql-to-sql/

## Kusto API – Proof of Concept

- Minimal Kusto API surface to exercise basic scenarios with Kusto.Explorer and Azure Data Explorer (web).
- Intended for basic query flows and evaluation only; not a full server implementation.

## Notes and caveats

- SQL output is validated against DuckDB in tests; other SQL engines may have minor dialect differences.
- Some KQL operators map to more complex SQL; performance characteristics depend on your target engine.
- PGLite and the Kusto API are experimental/PoC and may change.

## Getting started (library)

- Add a reference to the `KqlToSql` project/package.
- Use `KqlToSqlConverter` to translate KQL pipelines into SQL text; execute the SQL against your engine of choice.

## Contributing

- See `KqlOperatorsChecklist.md` to track support status. PRs welcome for missing operators/functions.
- When adding features:
  - Implement translator(s) in `KqlToSql`
  - Add tests in `tests/KqlToSql.Tests`
  - Update `KqlOperatorsChecklist.md` and this README as needed
