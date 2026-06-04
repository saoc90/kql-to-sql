# Differential Fuzzing — Fixes & Remaining Findings

A differential (oracle) fuzzing campaign compared the KQL→SQL translator (`KqlToSqlConverter` →
DuckDB) against a **real Kusto engine** (the Kustainer emulator over HTTP). 10 sub-agent "families"
× 3 rounds plus a deterministic combinatorial tier produced ~1,660 self-contained queries
(`datatable`/`print`/`range`, so both engines get identical input). See `tests/KqlToSql.Fuzzer`
(harness + console driver) and `tests/KqlToSql.DifferentialTests` (xUnit, gated on Kustainer).

**Headline:** 720 → **652** bug candidates after fixes; exact-MATCH 792 → **853**; invalid-SQL
errors (`SqlExecError`) 235 → **182**. Full machine-readable detail: `reports/` (original campaign)
and `reports2/` (post-fix). Each fixed root cause has a guardrail in
`tests/KqlToSql.DifferentialTests/Regression/GeneratedRegressionTests.cs`.

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

## Remaining / deferred (documented, with root-cause pointers)

These were left for follow-up — larger refactors, architectural choices, or behaviors that warrant a
deliberate decision. Counts are approximate cluster sizes from the campaign.

- **Join/lookup/union NULL-padding & key-name collision** (~40): Kustainer fills unmatched outer-join
  cells of *string* columns with `''` (not NULL), renames a colliding right key to `c1`/`v1`, and
  treats `null == null` as equal in join keys. Touches all of `JoinHandlers`. Confirm the target
  semantics, then coalesce/rename consistently. Source: `src/KqlToSql/Operators/JoinHandlers.cs`.
- **`search` operator** (~15): column-scoped terms, `kind=case_sensitive`, the synthetic `$table`
  column, and binding terms to row columns are not implemented faithfully (terms compile to
  tautologies). Source: `src/KqlToSql/Operators/ParseHandlers.cs`.
- **`parse` / `parse-kv`** (~30): regex over-escaping (`\d` → `\\d`), the trailing unterminated token
  compiling as non-greedy, and `parse-kv` delimiter options. Source: `ParseHandlers.cs`.
- **`make-series`** (~25): numeric `from..to..step` ranges emit `range(INT,INT,INTERVAL)`; default
  fill / `series_fill_forward` gaps. Source: `src/KqlToSql/Operators/AdvancedHandlers.cs`.
- **`scan`** (~10): multi-assignment single-step bodies reference step-local columns that aren't in
  scope (`column "s"/"cum" not found`). Source: `src/KqlToSql/Operators/ScanHandler.cs`.
- **`bag_unpack`** (~5): emits `from_json(.., '{}')` with an empty schema instead of inferring keys.
- **Nested dynamic property access returning JSON** (~40): `d.a.b`/`d["k"]`/`d[i]` return a VARCHAR
  rather than a navigable dynamic, so chained access and array funcs on the result fail
  (`UNNEST requires a single list`, `+(VARCHAR, INT)`). This is the dual LIST-vs-JSON representation
  of `dynamic`; unifying it is an architectural change.
- **`format_timespan` / `format_datetime` format strings** (~15): format specifiers are partially or
  not honored (`format_timespan` ignores the format and casts).
- **Int/long overflow semantics** (~6): Kusto wraps on overflow; DuckDB raises. `int(2147483647)+1`.
- **`gettype`, string→number casts, `datetime(null)` arithmetic, NaN equality** (assorted): subtle
  scalar-semantics differences.

To reproduce or re-measure: start Kustainer (`podman run -e ACCEPT_EULA=Y -m 4G -d -p 8080:8080
mcr.microsoft.com/azuredataexplorer/kustainer-linux:latest`), then
`dotnet run --project tests/KqlToSql.Fuzzer -- generate` / `run` / `report`, or re-run the agent
campaign (`fuzzing/fuzz-campaign.workflow.js`). `fuzzing/cluster.py <sqlerr|mismatch> <verdicts-dir>`
clusters findings by signature.
