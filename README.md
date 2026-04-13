# KQL to SQL Translator

Translates [Kusto Query Language (KQL)](https://learn.microsoft.com/en-us/kusto/query/) into executable SQL. Built on the official Kusto language parser.

Supports two SQL dialects: **DuckDB** and **PGlite** (Postgres).

- Live demo: https://saoc90.github.io/kql-to-sql/

## Usage

### As a library

```csharp
using KqlToSql;
using KqlToSql.Dialects;

var converter = new KqlToSqlConverter(new DuckDbDialect());
var sql = converter.Convert("StormEvents | where State == 'TEXAS' | summarize count() by EventType");
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

## Translation examples

> `join`, `iif()`, `round()`, `project`, `sort`

```kql
Events
| where Year == 2021
| summarize Events2021 = count(), Damage2021 = sum(DamageProperty + DamageCrops),
    AvgInjuries = avg(toreal(Injuries)) by State
| join kind=inner (
    Events
    | where Year == 2020
    | summarize Events2020 = count(), Damage2020 = sum(DamageProperty + DamageCrops) by State
) on State
| extend AvgInjuries = round(AvgInjuries, 1)
| extend DamageChangePct = round((Damage2021 - Damage2020) * 100.0 / Damage2020, 1)
| extend Trend = iif(Events2021 > Events2020, 'Increasing',
    iif(Events2021 < Events2020, 'Decreasing', 'Stable'))
| project State, Events2020, Events2021, Trend, Damage2020, Damage2021, DamageChangePct
| sort by DamageChangePct desc
```

<details>
<summary>Generated SQL</summary>

```sql
SELECT State, Events2020, Events2021, Trend, Damage2020, Damage2021, DamageChangePct
FROM (SELECT *,
    CASE WHEN Events2021 > Events2020 THEN 'Increasing'
         ELSE CASE WHEN Events2021 < Events2020 THEN 'Decreasing' ELSE 'Stable' END
    END AS Trend
  FROM (SELECT *, ROUND((Damage2021 - Damage2020) * 100.0 / Damage2020, 1) AS DamageChangePct
    FROM (SELECT *, ROUND(AvgInjuries, 1) AS AvgInjuries
      FROM (SELECT L.*, R.* EXCLUDE (State)
        FROM (SELECT State, COUNT(*) AS Events2021,
              SUM(DamageProperty + DamageCrops) AS Damage2021,
              AVG(TRY_CAST(Injuries AS DOUBLE)) AS AvgInjuries
            FROM Events WHERE Year = 2021 GROUP BY ALL) AS L
        INNER JOIN
          (SELECT State, COUNT(*) AS Events2020,
              SUM(DamageProperty + DamageCrops) AS Damage2020
            FROM Events WHERE Year = 2020 GROUP BY ALL) AS R
        ON L.State = R.State))))
ORDER BY DamageChangePct DESC
```

</details>

---

> `let`, `datatable`, `join`, `case()`, `countif`, `top`

```kql
let severity_lookup = datatable(EventType:string, SeverityWeight:long) [
    'Tornado', 5, 'Hurricane', 5, 'Flash Flood', 3, 'Hail', 2, 'Thunderstorm Wind', 1
];
let enriched = Events
    | join kind=inner (severity_lookup) on EventType
    | extend TotalDamage = DamageProperty + DamageCrops
    | extend DamageCategory = case(
        TotalDamage >= 100000, 'Catastrophic',
        TotalDamage >= 10000,  'Severe',
        'Moderate');
enriched
| summarize EventCount = count(), TotalInjuries = sum(Injuries),
    AvgSeverity = avg(toreal(SeverityWeight)),
    CatastrophicCount = countif(DamageCategory == 'Catastrophic')
  by State
| extend CatastrophicPct = round(CatastrophicCount * 100.0 / EventCount, 1)
| top 5 by TotalInjuries
```

<details>
<summary>Generated SQL</summary>

```sql
WITH severity_lookup AS NOT MATERIALIZED (
  SELECT * FROM (VALUES
    ('Tornado', 5), ('Hurricane', 5), ('Flash Flood', 3),
    ('Hail', 2), ('Thunderstorm Wind', 1)
  ) AS t(EventType, SeverityWeight)
),
enriched AS NOT MATERIALIZED (
  SELECT *, CASE WHEN TotalDamage >= 100000 THEN 'Catastrophic'
                 WHEN TotalDamage >= 10000 THEN 'Severe'
                 ELSE 'Moderate' END AS DamageCategory
  FROM (SELECT *, DamageProperty + DamageCrops AS TotalDamage
    FROM (SELECT L.*, R.* EXCLUDE (EventType)
      FROM Events AS L
      INNER JOIN severity_lookup AS R ON L.EventType = R.EventType))
)
SELECT *, ROUND(CatastrophicCount * 100.0 / EventCount, 1) AS CatastrophicPct
FROM (SELECT State, COUNT(*) AS EventCount, SUM(Injuries) AS TotalInjuries,
    AVG(TRY_CAST(SeverityWeight AS DOUBLE)) AS AvgSeverity,
    COUNT(*) FILTER (WHERE DamageCategory = 'Catastrophic') AS CatastrophicCount
  FROM enriched GROUP BY ALL)
ORDER BY TotalInjuries DESC LIMIT 5
```

</details>

---

> `lookup`, `between`, `case()`, nested `iif()`, `summarize`

```kql
Events
| lookup StateInfo on State
| where Injuries between (1 .. 10)
| extend ImpactPerCapita = round(Injuries * 1000000.0 / Population, 2)
| extend Quarter = case(Month <= 3, 'Q1', Month <= 6, 'Q2', Month <= 9, 'Q3', 'Q4')
| summarize IncidentCount = count(), TotalInjuries = sum(Injuries),
    AvgImpactPerCapita = avg(ImpactPerCapita)
  by Region, Quarter
| extend Severity = iif(TotalInjuries >= 10, 'Critical',
    iif(TotalInjuries >= 5, 'Warning', 'Normal'))
| sort by Region asc, Quarter asc
```

<details>
<summary>Generated SQL</summary>

```sql
SELECT *,
  CASE WHEN TotalInjuries >= 10 THEN 'Critical'
       ELSE CASE WHEN TotalInjuries >= 5 THEN 'Warning' ELSE 'Normal' END
  END AS Severity
FROM (SELECT Region, Quarter,
    COUNT(*) AS IncidentCount, SUM(Injuries) AS TotalInjuries,
    AVG(ImpactPerCapita) AS AvgImpactPerCapita
  FROM (SELECT *,
      CASE WHEN Month <= 3 THEN 'Q1' WHEN Month <= 6 THEN 'Q2'
           WHEN Month <= 9 THEN 'Q3' ELSE 'Q4' END AS Quarter
    FROM (SELECT *, ROUND(Injuries * 1000000.0 / Population, 2) AS ImpactPerCapita
      FROM (SELECT L.*, R.* EXCLUDE (State)
        FROM Events AS L
        LEFT OUTER JOIN StateInfo AS R ON L.State = R.State)
      WHERE Injuries BETWEEN 1 AND 10))
  GROUP BY ALL)
ORDER BY Region ASC, Quarter ASC
```

</details>

## Contributing

See `KqlOperatorsChecklist.md` to track support status. PRs welcome for missing operators and functions.
