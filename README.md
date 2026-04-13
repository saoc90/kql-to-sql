# KQL to SQL Translator

Translates [Kusto Query Language (KQL)](https://learn.microsoft.com/en-us/kusto/query/) into executable SQL. Built on the official Kusto language parser.

Supports two SQL dialects: **DuckDB** and **PGlite** (Postgres).

- Live demo: https://saoc90.github.io/kql-to-sql/

## What's in the repo

| Project | Description |
|---------|-------------|
| `src/KqlToSql` | Core library — parses KQL and emits SQL for the selected dialect |
| `src/KqlToSql.DuckDbExtension` | Native DuckDB extension exposing `kql_to_sql()` as a built-in function |
| `src/DuckDbDemo` | Blazor WASM demo — runs KQL queries client-side against DuckDB in the browser |
| `tests/KqlToSql.Tests` | Unit tests against DuckDB with the StormEvents dataset |
| `tests/KqlToSql.IntegrationTests` | Integration tests executing converted SQL against DuckDB WASM and PGlite |

## Usage

### As a library

```csharp
using KqlToSql;
using KqlToSql.Dialects;

var converter = new KqlToSqlConverter(new DuckDbDialect());
var sql = converter.Convert("StormEvents | where State == 'TEXAS' | summarize count() by EventType");
// SELECT EventType, COUNT(*) AS count FROM StormEvents WHERE State = 'TEXAS' GROUP BY ALL
```

### As a DuckDB extension

```sql
LOAD kql;

SELECT kql_to_sql('StormEvents | where State == ''TEXAS'' | count');
-- Returns: SELECT COUNT(*) AS Count FROM StormEvents WHERE State = 'TEXAS'

SELECT * FROM kql_explain('StormEvents | take 5');
-- kql_input              | sql_output                        | dialect
-- StormEvents | take 5   | SELECT * FROM StormEvents LIMIT 5 | duckdb
```

See [`src/KqlToSql.DuckDbExtension/README.md`](src/KqlToSql.DuckDbExtension/README.md) for build and install instructions.

### In-browser demo

```bash
cd src/DuckDbDemo
dotnet run
```

Open the printed URL — the KQL editor runs queries entirely client-side against DuckDB WASM.

## Supported KQL

The full feature matrix is in [`KqlOperatorsChecklist.md`](./KqlOperatorsChecklist.md). Summary:

- **70+ tabular operators** — `where`, `summarize`, `join`, `union`, `extend`, `project`, `sort`, `top`, `distinct`, `mv-expand`, `mv-apply`, `parse`, `parse-kv`, `search`, `make-series`, `evaluate` (pivot, narrow, bag_unpack), `serialize`, `top-nested`, `let`/`materialize`, and more
- **200+ scalar functions** — string, math, datetime, array/dynamic, hash, IP, regex, type conversion
- **40+ aggregate functions** — `count`, `sum`, `avg`, `percentile`, `dcount`, `make_list`, `make_set`, `arg_max`, `countif`, `sumif`, `stdev`, `variance`, `hll`, and more
- **Control commands** — `create table`, view declarations

## Query showcases

Complex KQL that translates and executes correctly. Each is a passing integration test.

### Risk score dashboard

`let` with `datatable`, `join`, `case()`, multi-aggregate `summarize`, `countif`, computed ranking:

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

Self-join, `iif()` trend labels, percentage change:

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

`case()`, `countif`, `dcount`, `strcat`, `tostring`:

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

`lookup` enrichment, `between`, per-capita metrics, nested `iif`:

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

## Contributing

See `KqlOperatorsChecklist.md` to track support status. PRs welcome for missing operators and functions.
