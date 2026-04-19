# Real-World Validation: KQL-to-SQL on Industrial IoT Data

Validated against production telemetry from A. Rieper AG pelleting plant and BMIN Milling, comparing translated SQL on DuckDB with native KQL on Azure Data Explorer (Kusto).

## Dataset

| Property | Value |
|---|---|
| Source | `prep-0017440030-Data`, `prep-0011990080-Data` (Kusto) |
| Rows | 13M (Rieper), 620k (BMIN) |
| Devices | 6 pelleting devices, 253 milling devices |
| Signals | 8,078 / 14,266 distinct |
| Time range | 24h |

## Supported KQL features validated

All of these translate and produce correct results against real data:

| Category | Features |
|---|---|
| **Tabular operators** | `where`, `project`, `project-away`, `extend`, `sort`, `take`, `top`, `distinct`, `count`, `serialize` |
| **Aggregation** | `summarize`, `count()`, `sum()`, `avg()`, `min()`, `max()`, `dcount()`, `take_any()`, `countif()`, `sumif()`, `arg_max()`, `arg_min()` |
| **Scalar functions in summarize** | `datetime_diff()`, `todouble()`, `toreal()`, type casts inside aggregate expressions |
| **Window functions** | `prev()`, `next()`, `row_cumsum()`, `row_number()` via `serialize` |
| **String operators** | `contains`, `has`, `has_any`, `startswith`, `endswith`, `matches regex`, `in`, `!in` |
| **Type conversions** | `toint()`, `tolong()`, `toreal()`, `todouble()`, `todecimal()`, `tostring()`, `tobool()`, `todatetime()` |
| **Conditional logic** | `case()`, `iif()`, nested `iif()`, `isempty()`, `isnotempty()`, `isnull()`, `coalesce()` |
| **Bitwise** | `binary_and()`, `binary_or()` |
| **Date/time** | `bin()`, `datetime_diff()`, `datetime_part()`, `startofday()`, `ago()`, `now()` |
| **Math** | `abs()`, `floor()`, `ceiling()`, `round()`, `log()`, `sqrt()`, `pow()` |
| **String functions** | `strlen()`, `tolower()`, `toupper()`, `strcat()`, `replace_string()`, `substring()`, `split()`, `extract()` |
| **Let bindings** | Scalar literals (`datetime`, `string`, `int`, `real`), computed scalars (`ago()`, `toreal()`, type casts), tabular CTEs, `materialize()` |
| **User-defined functions** | `let fn = view() { ... }` as CTEs, parameterized `let fn = (x: type) { ... }` with inline expansion, `fn()` calls resolved to CTEs |
| **Join/union** | `join kind=inner`, `join kind=leftouter`, `lookup`, `union`, self-joins with `bin()` bucketing |
| **Dynamic/JSON** | `dynamic([...])` arrays in `in` expressions, `tostring(MessageMetadata.field)` JSON access, `evaluate pivot()` |
| **Advanced operators** | `evaluate pivot()`, `invoke`, `toscalar()` in let bindings |
| **Query structure** | Multi-CTE queries, all-let queries (no trailing expression), nested subqueries, `PipeExpression` in expression context |

## Production formula test results

8 real production formulas from the Bühler Insights platform, ranging from 16 to 136 KQL pipe operators:

| Formula | Pipes | SQL chars | Status |
|---|---|---|---|
| Rieper - Recipe over time Gantt - Mill A | 16 | 1,520 | PASS |
| TVM Top 10 (MAX) Imbalances barchart | 18 | 1,716 | PASS |
| Rieper - PRE2 - StatusGant | 26 | 2,253 | PASS |
| Rieper - PRE1 - CyclicStatusGant | 26 | 2,253 | PASS |
| DEV - Status Gant and SME - PRE2 - Rieper | 44 | — | PASS |
| Rieper - Pelleting Essential | 52 | — | PASS |
| EMS Mill Idling energy - Mill A | 52 | — | PASS |
| Rieper - Dosing Accuracy | 136 | 17,227 | PASS |

## Result accuracy

Compared DuckDB output against Kusto on identical data (13M rows, 24h):

| Query | Rows | Match |
|---|---|---|
| Status Gantt (44 operators: `let`, `view`, `binary_and`, `prev`, `next`, `case`, `row_cumsum`, `union`) | 30 | **30/30 exact** |
| Cross-device comparison (`materialize`, `union`, `avg`, `todouble`) | 4 | **4/4 exact** (15 sig figs) |
| Signal aggregations (`count`, `avg`, `min`, `max`, `dcount`) | varies | **exact** |
| Pivot (`evaluate pivot`, `take_any`) | 5 | data matches |

## Performance

13M rows, DuckDB local (Apple Silicon) vs Kusto cloud (Azure West Europe):

| Query | DuckDB | Kusto | Speedup |
|---|---|---|---|
| Full table count | <1ms | 105ms | 1,457x |
| Filter + count | 8ms | 1,100ms | 136x |
| Summarize count by device | 7ms | 9,763ms | 1,380x |
| Top 10 signals by count | 15ms | 14,255ms | 981x |
| Avg/min/max on filtered signal | 6ms | 1,123ms | 190x |
| Distinct count per device | 12ms | 27,355ms | 2,222x |
| Pivot (3 signals) | 12ms | 319ms | 26x |
| Status Gantt (window functions + CTE) | 14ms | 170ms | 12x |
| Materialize + union (cross-device) | 7ms | 122ms | 17x |

DuckDB is 12x–2,222x faster on this dataset size. Kusto is optimized for petabyte-scale distributed queries — this comparison validates functional correctness, not architectural suitability.
