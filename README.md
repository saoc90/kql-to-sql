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

## Query showcases

Complex KQL pipelines that translate and execute against DuckDB. Each of these is a passing integration test.

### Risk score dashboard

`let` statements with `datatable`, `join`, `case()`, multi-aggregate `summarize`, `countif`, and computed ranking:

```kql
let severity_lookup = datatable(EventType:string, SeverityWeight:long) [
    'Tornado', 5,
    'Hurricane', 5,
    'Flash Flood', 3,
    'Hail', 2,
    'Thunderstorm Wind', 1
];
let enriched = Events
    | join kind=inner (severity_lookup) on EventType
    | extend TotalDamage = DamageProperty + DamageCrops
    | extend DamageCategory = case(
        TotalDamage >= 100000, 'Catastrophic',
        TotalDamage >= 10000,  'Severe',
        'Moderate');
enriched
| summarize
    EventCount = count(),
    TotalInjuries = sum(Injuries),
    TotalDeaths = sum(Deaths),
    AvgSeverity = avg(toreal(SeverityWeight)),
    MaxDamage = max(TotalDamage),
    CatastrophicCount = countif(DamageCategory == 'Catastrophic')
  by State
| extend AvgSeverity = round(AvgSeverity, 2)
| extend CatastrophicPct = round(CatastrophicCount * 100.0 / EventCount, 1)
| extend RiskScore = round(AvgSeverity * EventCount + TotalInjuries * 2 + TotalDeaths * 10, 1)
| project State, EventCount, TotalInjuries, TotalDeaths, AvgSeverity, CatastrophicPct, RiskScore
| top 5 by RiskScore
```

### Year-over-year comparison

Self-join for YoY analysis with `iif()` trend labels and percentage change:

```kql
Events
| where Year == 2021
| summarize Events2021 = count(), Damage2021 = sum(DamageProperty + DamageCrops), AvgInjuries = avg(toreal(Injuries)) by State
| join kind=inner (
    Events
    | where Year == 2020
    | summarize Events2020 = count(), Damage2020 = sum(DamageProperty + DamageCrops) by State
) on State
| extend AvgInjuries = round(AvgInjuries, 1)
| extend DamageChangePct = round((Damage2021 - Damage2020) * 100.0 / Damage2020, 1)
| extend Trend = iif(Events2021 > Events2020, 'Increasing', iif(Events2021 < Events2020, 'Decreasing', 'Stable'))
| project State, Events2020, Events2021, Trend, Damage2020, Damage2021, DamageChangePct, AvgInjuries
| sort by DamageChangePct desc
```

### Source reliability analysis

Multi-step `extend` with `case()`, `countif`, `dcount`, `strcat`, and `tostring`:

```kql
Events
| extend TotalDamage = DamageProperty + DamageCrops
| extend DamageBucket = case(
    TotalDamage >= 100000, 'High',
    TotalDamage >= 10000,  'Medium',
    'Low')
| summarize
    Reports = count(),
    AvgMagnitude = avg(Magnitude),
    HighDamageReports = countif(DamageBucket == 'High'),
    TotalInjuries = sum(Injuries),
    StatesAffected = dcount(State)
  by Source
| extend AvgMagnitude = round(AvgMagnitude, 2)
| extend HighDamagePct = round(HighDamageReports * 100.0 / Reports, 1)
| extend SourceLabel = strcat(Source, ' (', tostring(Reports), ' reports)')
| project SourceLabel, Reports, AvgMagnitude, HighDamagePct, TotalInjuries, StatesAffected
| sort by Reports desc
```

### Regional impact with lookup

`lookup` enrichment, `between`, computed per-capita metrics, and multi-level `iif`:

```kql
Events
| lookup StateInfo on State
| where Injuries between (1 .. 10)
| extend ImpactPerCapita = round(Injuries * 1000000.0 / Population, 2)
| extend Quarter = case(Month <= 3, 'Q1', Month <= 6, 'Q2', Month <= 9, 'Q3', 'Q4')
| summarize
    IncidentCount = count(),
    TotalInjuries = sum(Injuries),
    AvgImpactPerCapita = avg(ImpactPerCapita)
  by Region, Quarter
| extend AvgImpactPerCapita = round(AvgImpactPerCapita, 4)
| extend Severity = iif(TotalInjuries >= 10, 'Critical', iif(TotalInjuries >= 5, 'Warning', 'Normal'))
| project Region, Quarter, IncidentCount, TotalInjuries, AvgImpactPerCapita, Severity
| sort by Region asc, Quarter asc
```

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
