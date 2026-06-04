# Differential Fuzzing — Fixes & Remaining Findings

A differential (oracle) fuzzing campaign compared the KQL→SQL translator (`KqlToSqlConverter` →
DuckDB) against a **real Kusto engine** (the Kustainer emulator over HTTP). 10 sub-agent "families"
× 3 rounds plus a deterministic combinatorial tier produced ~1,660 self-contained queries
(`datatable`/`print`/`range`, so both engines get identical input). See `tests/KqlToSql.Fuzzer`
(harness + console driver) and `tests/KqlToSql.DifferentialTests` (xUnit, gated on Kustainer).

**Headline (all fixes — direct + 5 sub-agent clusters):** 720 → **467** bug candidates;
exact-MATCH 792 → **1038**; invalid-SQL errors (`SqlExecError`) 235 → **116**; column-shape
mismatches 19 → **3**. Full machine-readable detail: `reports/` (original campaign) and
`reports3/` (final post-fix). Each fixed root cause has a guardrail in
`tests/KqlToSql.DifferentialTests/Regression/GeneratedRegressionTests.cs` (38 cases). The existing
unit suite stays green (577) and the differential suite is 71 green.

## Fixed (root cause, verified vs Kustainer, DuckDB + PGlite parity)

| # | Bug | Root cause & fix |
|---|-----|------------------|
| 1 | `has`/`has_cs`/`!has`/`hasprefix`/`hassuffix` matched **substrings** | They reused `ConvertLike("%","%")` (= `contains`). KQL term operators match whole **terms**; now `regexp_matches(..,'(?i)\bX\b'|'\bX'|'X\b')`. `ExpressionSqlBuilder.ConvertHasTerm`. |
| 2 | Integer division `l/i` used **float** `/` | The truncating-division path existed but gated on `_integerColumns`, which was never populated for `datatable` columns. `ConvertDataTable` now `MarkIntegerColumn`s int/long columns. |
| 3 | Modulo `-5 % 3` gave `-2` | KQL modulo is **Euclidean** (always in `[0,|b|)`). New `ConvertModulo` emits `((a%b)+abs(b))%abs(b)` for integer operands. |
| 4 | `isnotempty` ignored `''` | It was aliased to `isnotnull`. Split: `isnotempty` ⇒ `IS NOT NULL AND CAST(..)<>''` (both dialects). |
| 5 | `startofweek`/`endofweek` off by a day | DuckDB/PG `DATE_TRUNC('week')` starts Monday; Kusto starts **Sunday**. Shift ±1 day (both dialects). |
| 6 | `string_size('café')` → invalid SQL | `OCTET_LENGTH(VARCHAR)` is unsupported and `CAST AS BLOB` rejects non-ASCII; use `OCTET_LENGTH(ENCODE(..))`. |
| 7 | `make_datetime`/`make_timespan` partial arities → "function does not exist" | Added 2–5-arg `make_datetime` and 2-arg `make_timespan` (both dialects). |
| 8 | `sort` placed NULLs wrong | KQL: asc→nulls first, desc→nulls last (opposite of SQL defaults). `ApplySort` now emits `NULLS FIRST/LAST`. |
| 9 | `dynamic({...})` literals **mangled** | `ConvertDynamic` used recursive `GetDescendants` (a nested array hijacked the classification) and a non-recursive value serializer (nested objects → `null`). Rewrote to inspect the **direct** child and recursively serialize to JSON; scalar arrays stay native `LIST_VALUE`. Also fixes `dynamic(1)`/`dynamic("x")` scalars. |
| 10 | `mv-expand` emitted `AS t(d) AS t` (parser error) | `ExtractFromAsRelation` returned an already-aliased relation; now wrap the source as `(leftSql) AS t`. |
| 11 | `getschema` returned DuckDB's `DESCRIBE` shape | Now emits Kusto's 4 columns (`ColumnName, ColumnOrdinal, DataType, ColumnType`) with a DuckDB→KQL/CLR type map. |
| 12 | `datatable` `long` materialized as INT32 → overflow & wrong `getschema` | Pin `long` values to `BIGINT` and `real`/`double` to `DOUBLE` in the `VALUES` rows. |
| 13 | `real(nan)`/`real(+inf)`/`real(-inf)` → bareword `NaN` / `0.0` | Emit `CAST('nan'|'inf'|'-inf' AS DOUBLE)`. |
| 14 | `1tick` timespan literal → "tick not recognized" | DuckDB has no tick unit; emit `(N/10.0)*INTERVAL '1 microsecond'`. |
| 15 | scalar `strcat_array(arr, delim)` → "does not exist" | It was only mapped as an aggregate; added the scalar `ARRAY_TO_STRING` mapping (both dialects). |

## Also fixed — 5 sub-agent clusters (parallel git-worktree agents, integrated via 3-way merge)

- **joins / lookup / union** (`JoinHandlers.cs`): outer-join `''` string padding, key-name collision
  rename (`vN`), `null == null` key equality, `union` source/padding.
- **parse / parse-kv / search** (`ParseHandlers.cs`): regex escaping, greedy trailing token,
  parse-kv delimiters, search term-to-column binding.
- **scan** (`ScanHandler.cs`): carried-state (`s.<col>`) step support; bare references correctly stay
  per-row (verified against Kusto).
- **make-series / bag_unpack / top-hitters** (`AdvancedHandlers.cs`): numeric vs datetime axis step,
  default fill; key inference for bag_unpack; top-hitters group-by.
- **dynamic property access / format strings / scalar casts** (`ExpressionSqlBuilder.cs` + dialects):
  `d.a.b`/`d[k]`/`d[i]` now return navigable JSON so chained access and array funcs work;
  `format_timespan`/`format_datetime` honor the format; `gettype` → Kusto type names.

## Remaining (genuinely hard / by-spec — 467 candidates, documented)

- **dynamic type fidelity**: many remaining `MismatchRows` carry a benign `TYPE_MISMATCH` sub-verdict
  (Kusto `dynamic` vs DuckDB JSON-as-string) — values match, only the declared column type differs.
- **Int/long overflow semantics**: Kusto wraps on overflow; DuckDB raises (`int(2147483647)+1`).
- **`toint`/`tolong` of decimal strings**: `toint("3.14")` → Kusto null vs DuckDB 3.
- **NaN equality**: `real(nan)==real(nan)` is false in KQL, true in DuckDB.
- **Approximate/nondeterministic by spec** (excluded from "bug" counts but present in the corpus):
  `dcount`/`hll`/`percentile`, `arg_max`/`arg_min` ties, `sample`, `rand`/`now`.
- **Deep parse/search edge cases and `format_*` specifier coverage**: partial; the common cases are
  fixed, exotic format strings / multi-table search remain.

To reproduce or re-measure: start Kustainer (`podman run -e ACCEPT_EULA=Y -m 4G -d -p 8080:8080
mcr.microsoft.com/azuredataexplorer/kustainer-linux:latest`), then
`dotnet run --project tests/KqlToSql.Fuzzer -- generate` / `run` / `report`, or re-run the agent
campaign (`fuzzing/fuzz-campaign.workflow.js`). `fuzzing/cluster.py <sqlerr|mismatch> <verdicts-dir>`
clusters findings by signature.
