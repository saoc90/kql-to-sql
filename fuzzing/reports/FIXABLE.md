# What's left fixable

> ## STATUS — updated 2026-06-06 (after the "fix all fixable" pass)
>
> Full corpus = **1662 queries**. Current verdicts:
>
> | Outcome | Count |
> |---|---|
> | **Match** | **1319** |
> | MismatchRows | 146 |
> | MismatchColumns | 1 |
> | MismatchOrder | 0 |
> | **SqlExecError** | **0** |
> | **TranslateError** | **0** |
> | KustoError (malformed KQL — discard) | 127 |
> | SkippedNondeterministic | 40 |
> | SkippedEngineError | 29 |
>
> Match climbed **1123 → 1319** over the campaign (1300 → 1319 in this final pass). SqlExecError and
> TranslateError have been held at **0** throughout. The headline analysis below (355 diffs) is **historical** —
> most of those clusters are now fixed.
>
> ### Fixes landed this pass (all verified Match on the corpus; guardrails in `BugFixTests.cs`)
> 1. **mv-expand single-column** replaces the source column in place (`* REPLACE`) — preserves column position.
> 2. **extend redefining an existing column** replaces in place via `* REPLACE`, and detection now covers bare
>    projected identifiers, `RENAME`/`REPLACE` targets, **and columns arriving through a top-level `*`** (via the
>    semantic model) — kills duplicate-column output (`label`/`b_1`) and stale values in `extend` chains.
> 3. **top-nested** aggregate columns use collision-proof internal aliases (`_tnagg{i}`) — fixes the DuckDB
>    case-folding bug where `of sub by Sub=sum(v)` bound the ranking/output to the `sub` group key.
> 4. **top-nested / top-hitters** are no longer treated as order-sensitive (Kusto doesn't guarantee their row
>    order) → eliminated all flaky MismatchOrder; **top-hitters** marked nondeterministic (approximate + tie-arbitrary).
> 5. **Comparator**: two JSON-text strings (e.g. `dynamic_to_json` on both engines) compare canonically —
>    tolerates object key-order/whitespace (philosophy), still catches value/structure/array-order diffs.
> 6. **todatetime(numeric)** interprets the number as .NET ticks (inverse of `tolong(datetime)`).
> 7. **series_stats_dynamic** now includes the `len` field.
> 8. **make_timespan** returns null for out-of-range components (hours 0–23, minutes/seconds 0–<60).
> 9. **bool(null)** datatable cells emit SQL `NULL` (not `FALSE`); `bool(1)` now `TRUE`.
> 10. **array_index_of** returns `-1` when the value is absent (not NULL).
> 11. **dynamic array negative index** `d[-1]` returns the last element (native-LIST path passed negatives through).
> 12. **split(s, delim, index)** returns a one-element array of the indexed part (or `[]`), honoring the index arg.
>
> ### Remaining 147 candidates — honest breakdown (mostly NOT translator-fixable)
> | Category | ~Count | Verdict |
> |---|---|---|
> | datetime **100ns tick precision** (`.fffffff`, `/1tick`→Infinity, ns diff) | ~30 | **TOLERABLE** — DuckDB TIMESTAMP is µs; needs integer-tick datetime rep (architectural) |
> | **DuckDB.NET nullable-list driver bug** (`<unreadable:IndexOutOfRange>`) | 8 | **NOT FIXABLE** — driver returns garbage for a list with NULL elements |
> | **NaN IEEE comparison** semantics (`nan==nan`, `nan>1`, `min_of(nan,..)`) | ~7 | **TOLERABLE** (philosophy: NaN tolerated) — DuckDB orders NaN high / `nan==nan` true |
> | **numeric overflow / saturation / strict string-parse / null-comparison** | ~17 | mostly TOLERABLE (int wrap, `toint("1.5e3")`, `null==0`) |
> | **dynamic JSON scalar typing / navigation / element order** (`d.a.b.c[2]`→'30') | ~15 | hard — JSON scalars are stringly-typed in DuckDB |
> | **make_list/make_bag** dynamic-element stringify + element order | ~15 | hard / order TOLERABLE |
> | **format_datetime** `f`/`F`/`ddd`/`MMMM` specifiers | ~8 | fixable-but-fiddly; several also 7-digit-tick-limited |
> | **make-series** multi-day bucketing / empty-range / mv-expand order | ~7 | complex |
> | **series_\*** (`series_stats`, `series_fit_line`, `series_iir`) return shape | ~5 | complex (multi-column vs scalar) |
> | **decimal vs real** typing (`gettype(todecimal(1))`='real') | ~2 | TOLERABLE — DuckDB types bare reals as DECIMAL; lose-lose |
> | **null-aware negated filters** (`where not(x==1)`, `x !in (..)` include nulls) | ~3 | fixable but HIGH-RISK (rewrites all `!=`/`!in`) — deferred |
> | union/leftanti **null-vs-empty-string** columns; misc | residual | TOLERABLE (Kusto strings are "" not null; soft NULL_VS_EMPTY) |
>
> The remaining genuinely-fixable-but-deferred work (format_datetime specifiers, null-aware negated filters,
> series_* shape, make-series bucketing, dynamic JSON scalar typing) is fiddly and/or carries real regression
> risk against the hard 0-SqlExecError / 0-TranslateError invariant; each is 1–8 corpus cases.

---

<details><summary>Historical analysis (pre-fix, 355 differences) — kept for reference</summary>

## 1. Headline

Of the **355 remaining value-level differences**:

| Bucket | Count | Share |
|---|---|---|
| **FIXABLE** (genuine translator defects) | **~290** | ~82% |
| **COMPARATOR_GAP** (values equal; type/format noise) | **13** | ~4% |
| **TOLERABLE** (engine-specific, won't fix) | **29** | ~8% |

The FIXABLE bucket is heavily concentrated: a handful of root causes (verbatim/escape string handling, `tostring`/dynamic-JSON typing, scan/window restart semantics, extend-redefine duplicate columns, `to*` numeric coercion, order-by direction) account for the large majority. Most are **small or medium** effort, and several clusters share a *single* underlying fix.

> Note: the per-cluster JSON `efforts` maps are inconsistent in arity (often fewer entries than `count`); the effort labels below reflect the dominant/maximum effort reported per cluster.

---

## 2. Fixable clusters — ranked

Because the data fragments one defect across many small clusters, the strongest signal is in the **merged themes**. Ranked by combined count:

### Theme A — Verbatim `@"..."` prefix + KQL escape decoding in string literals (~40)
The single highest-leverage area. All trace to **`ConvertStringLiteral`** in `ExpressionSqlBuilder.cs`, which trims one quote and uses raw source text instead of the decoded `LiteralValue`, so `@"\s"` leaks as `'@"\s'` and `\t`/`\n`/`\\` stay literal.

| Category | Count | Effort |
|---|---|---|
| verbatim-string-prefix-not-stripped | 10 | medium |
| verbatim-string-literal-leaked | 8 | large |
| verbatim-string-at-prefix-leaks | 4 | small |
| string-literal-escapes-not-decoded | 4 | small |
| string-literal-escape-not-decoded | 2 | medium |
| verbatim-string-literal-not-unescaped / string-escape-not-decoded / kql-string-escape-not-decoded | 1+1+1 | medium |
| datatable-escape-not-decoded / datatable-string-escape-not-unescaped | 1+1 | small |

**Fix:** in `ConvertStringLiteral` (and the datatable cell path in `TabularHandlers.cs`), strip a leading `@`, drop surrounding quotes, and emit `lit.LiteralValue` (the decoded string) so `\t`/`\n`/`\r`/`\\` become real control chars. **File:** `src/KqlToSql/Expressions/ExpressionSqlBuilder.cs` (+ `Operators/TabularHandlers.cs`). One change clears ~40 diffs.

### Theme B — `tostring` / dynamic-as-JSON typing (~40)
`tostring` of a JSON/dynamic value emits `TRY_CAST(... AS TEXT)` keeping quotes (`'"red"'`), `tostring(null)` yields SQL NULL instead of `''`, and dynamic columns surface as String not Dynamic; `gettype` then defaults JSON scalars to `'dictionary'`.

| Category | Count | Effort |
|---|---|---|
| tostring-of-json-keeps-quotes | 7 | medium |
| tostring-of-null-yields-null-not-empty | 4 | medium |
| parse_json-scalar-and-gettype | 4 | medium |
| dynamic-col-returns-varchar (FIXABLE side) | 3 | large |
| dynamic-scalar-gettype-and-null / gettype-decimal-returns-real / gettype-int-vs-long | 2+2+2 | medium/small |
| tostring-dynamic-null-not-empty + tostring-typednull/dynamicnull variants | 2+1+1+1+1 | medium |
| gettype-null-dynamic-returns-dictionary | 1 | medium |
| tostring-bool-casing / strcat-bool-lowercase-true / tostring-formatting-mismatch | 3+1+2 | small/large |

**Fix:** (a) use `json_extract_string` / `->>'$'` for `tostring` of dynamic so string scalars are unquoted; (b) `COALESCE(..., '')` so `tostring(null)`/`tostring(dynamic(null))` is `''`; (c) rewrite the `gettype` JSON branch to switch on `json_type` (NUMBER→long/real, STRING→string, BOOLEAN→bool, NULL→null, OBJECT→dictionary, ARRAY→array) and map DECIMAL→`'decimal'`, INTEGER→`'int'` vs BIGINT→`'long'`; (d) `tostring(bool)`→`CASE` `'True'/'False'`. Carrying dynamic columns as JSON (the `large` item) also fixes several COMPARATOR_GAP rows. **Files:** `ExpressionSqlBuilder.cs`, `DuckDbDialect.cs`.

### Theme C — Scan / window restart + offset/default semantics (~30)
`row_cumsum`, `row_number`, `row_rank_dense/min`, `prev/next` ignore their reset-predicate / offset / default args; all in **`DuckDbDialect.cs`**.

| Category | Count | Effort |
|---|---|---|
| prev-next-ignore-offset-and-default | 4 | medium |
| scan-restart-predicate-dropped | 3 | large |
| row-cumsum-restart-ignored / row_cumsum-ignores-reset-flag | 3+2 | large/medium |
| row-rank-restart-ignored | 2 | large |
| row_number-restart-predicate-ignored | 1 | large |
| prev-next offset/default singletons (×3) + row_number-ignores-start-index | 4 | small |

**Fix (shared):** build a reset-group id via `SUM(reset::int) OVER (ORDER BY ...)` and `PARTITION BY` it for `SUM`/`ROW_NUMBER`/`DENSE_RANK`/`RANK`; emit `LAG/LEAD(arg, offset, default)`; honor `row_number(start)` as `+ (start-1)`. One reset-group helper clears the restart family.

### Theme D — `extend`/CTE/join redefine → duplicate column (~13)
Redefining an existing column emits `SELECT *, expr AS x` producing two `x`; downstream binds the stale one. In `TabularHandlers.cs` (and `JoinHandlers.cs` for join RHS).

| Category | Count | Effort |
|---|---|---|
| extend-redefine-duplicate-column | 4 | medium |
| cte-extend-chain-redefine-uses-stale-column | 2 | medium |
| join-rhs-extend-projection-not-applied | 2 | medium |
| extend-reassign-duplicate-column-picks-old-value / project-reorder-leaks-shadowed / sequential-extend-rename-clobber | 1+1+1 | medium |

**Fix:** apply the existing `SELECT * EXCLUDE(x), expr AS x` rewrite whenever an extend/rename redefines an already-projected column — extend it to CTE bodies, join inputs, and the `SELECT 1` base case (which the `HasTopLevelAlias` check currently misses).

### Theme E — `has`/term matching: Unicode boundary, underscore, empty needle, wildcard (~22)
`\b` (RE2 ASCII-only) breaks accented/Cyrillic terms and underscores; empty term and `search "x*"` wildcard mishandled.

| Category | Count | Effort |
|---|---|---|
| has-term-unicode-word-boundary | 6 | medium |
| has-term-empty-needle | 5 | small |
| search-wildcard-not-expanded | 3 | medium |
| has-operator-word-boundary | 2 | medium |
| has-term-tokenization-underscore / has-wordboundary-nonascii / has-word-boundary-unicode | 1+1+1 | medium |
| search-term-wildcard / search-wildcard-escaped / search-numeric-term-always-true | 1+1+1 | medium |

**Fix:** in `ConvertHasTerm`, replace `\b` with Unicode-aware non-word delimiters `(?:^|[^\p{L}\p{N}])…(?:$|[^\p{L}\p{N}])`, treat `_` as a separator, special-case empty term → `TRUE`; in the search-term builder, expand `*`→`.*` instead of escaping, and lower numeric search terms to per-column term predicates.

### Theme F — Numeric coercion: integer division, `to*` parsing, ticks, modulo, decimal (~35)

| Category | Count | Effort |
|---|---|---|
| integer-division-returns-real / integer-division-via-double-loses-precision | 4+1 | medium |
| safecast-int-via-double | 4 | medium |
| toint-tolong-of-fractional-string-truncates / accepts-non-integer / must-reject-noninteger | 2+2+1 | large/medium |
| toint-tolong hex-prefix / int64-max-via-double / tolong-todouble of datetime/timespan/ticks | 1+1+1+1+1+1 | medium |
| modulo-semantics / modulo-euclidean-real / modulo-sign / modulo-negative-float | 1+1+1+1 | small/medium |
| tobool-string (×3) / todouble-string grammar/inf (×3) / todecimal default-scale (×2) | several | small/medium |
| totimespan dotted-day / bare-number (×4) / todatetime-numeric-ticks / bin-int-returns-real / decimal-literal-not-double / nan-equality (×3) | several | small/medium |

**Fix:** emit true integer division (`a//b` / BIGINT cast) for integer operands; for `toint`/`tolong` parse the integer grammar directly (`TRY_CAST AS BIGINT`, hex `0x`, reject fractional/exponent/underscore) instead of DOUBLE+TRUNC; compute datetime/timespan↔ticks (×1e7); apply the Euclidean modulo wrap to reals; emit real literals/`todecimal` as DOUBLE / high-scale DECIMAL. **Files:** `ExpressionSqlBuilder.cs`, `DuckDbDialect.cs`.

### Theme G — Order-by / row_number sort direction inverted (~12)
The Ordering token never compares equal to `'ASC'`, so `asc` emits `DESC` (and `nulls first`→`LAST`).

| Category | Count | Effort |
|---|---|---|
| order-asc-emitted-as-desc | 4 | medium |
| order-by-nulls-direction-inverted | 3 | small |
| row_number-sort-direction-inverted / row_number-restart-predicate-ignored | 1+1 | small/large |

**Fix:** correct direction detection in `ApplySort` (`TabularHandlers.cs`) so `asc`→`ASC NULLS FIRST`. Small fix, broad effect.

### Theme H — `make_bag`/`make_list`/aggregate semantics (~25)
`make_bag` uses `histogram` (count map) instead of a JSON object merge; `make_list`/`make_set` stringify dynamics, don't flatten, drop the count arg, serialize BigInteger structs; `stdev/variance`/empty groups return NULL not 0/`{}`; `arg_max/min` null-key and alias collisions. All in `AggregationHandlers.cs`.

| Category | Count | Effort |
|---|---|---|
| make_bag-uses-histogram (+ -not-merge / -wrong-aggregate / empty-{} variants) | 4+1+1+1+1 | large |
| make_list stringify/flatten/count/bigint variants (×6) | ~7 | medium/large |
| stdev-single-row-null / stdev-variance-single-row / empty-aggregate-default-null | 2+1+2 | small/medium |
| arg_max/min null-key + name-collision (×3) / arg_min-nan-ordering | 4 | medium |

**Fix:** `make_bag`→JSON object merge (`json_group_object`/merge); aggregate dynamics as JSON values and flatten one level; honor count arg via `LIST(...)[1:N]`; `COALESCE` sample stdev/variance to 0 and empty bag to `{}`; NaN-tolerant arg selection + alias dedup.

### Theme I — mv-expand / mv-apply / make-series / partition shape (~28)

| Category | Count | Effort |
|---|---|---|
| mv-expand-column-order (+ -and-autoname / make-series-column-order) | 4+2+1 | medium |
| mv-apply-subquery-not-grouped-per-source-row / -cross-join-not-collapsed | 3+2 | large |
| partition-by-not-per-group / partition-operator-drops-by-grouping | 1+1 | large |
| series-func-returns-json-string-not-dynamic / series-stats-dynamic-missing-len / series_iir (×2) | 2+1+2 | large |
| bag_unpack / top-nested / make-series empty-range/default-null/bucket-misalign / mv-expand-array-kind/element-typed | several | medium/large |

**Fix:** preserve original column ordinal positions when re-projecting mv-expanded columns (explicit projection list, not EXCLUDE-and-append); for mv-apply ending in `summarize`, group the unnested rows by the source row; implement `partition by` as a per-partition window/GROUP BY rather than once over the whole input; expand series_* into typed columns and implement the IIR denominator recurrence.

### Theme J — substring / split / trim / countof / indexof / contains string ops (~30)

| Category | Count | Effort |
|---|---|---|
| substring negative-start/length variants (×8) | ~8 | medium/small |
| trim charset/regex/verbatim variants (×6) | ~6 | large/medium |
| split index/negative/out-of-range/Dynamic (×3) | ~3 | medium |
| countof regex-mode / empty-needle (×3) | ~3 | medium |
| contains/startswith special-char LIKE not escaped (×2) | 2 | medium |
| indexof start-arg / empty-needle / strcat_array null / strcat bool | several | small/medium |

**Fix:** negative `substring` start = `LENGTH(s)+start` clamped to 0, negative length→`''`; trim variants as anchored `REGEXP_REPLACE`; honor split index/negative addressing and tag results Dynamic; countof regex via `regexp_extract_all` returning BIGINT; escape `%`/`_` in LIKE.

### Theme K — datetime/timespan formatting & boundaries (~25)
`startof*/endof*` ignore the offset arg; `1tick` truncates to 0µs (Infinity); `format_datetime` token mapping (`dddd`/`MMMM`/`f`/`F`); `tostring`/`totimespan` day-dotted formats; `bin` multi-day/week origin; `datetime_diff` week/nanosecond; `make_timespan`/`dayofweek` boundary nulls. Split across `DuckDbDialect.cs` and `ExpressionSqlBuilder.cs`.

| Category | Count | Effort |
|---|---|---|
| startof-endof-ignores-offset / startof-ignores-offset | 3+2 | medium |
| tick-interval-truncated-to-zero / tick-divisor-rounds-to-zero | 3+2 | medium |
| format_datetime token/fractional variants (×6) | ~6 | medium/large |
| timespan-tostring-no-day / tostring-datetime/timespan format (×5) | ~5 | medium/large |
| bin week/multiday/nonpositive/datetime-origin (×4) / datetime_diff week·ns (×2) / make_timespan·dayofweek (×3) / totimespan parse (×4) | several | medium |

**Fix:** add the offset INTERVAL shift to startof/endof; compute tick quantities in 100ns (×1e7) not sub-µs INTERVAL; map `dddd→%A, ddd→%a, MMMM→%B, MMM→%b` and `f/F` to exact fractional digits; render timespan/datetime via explicit Kusto formatting; align bin origin to Kusto's reference and parse day-dotted timespans.

### Theme L — dynamic/JSON array & bag operations (~22)
`array_length` returns 0 not null for non-arrays; JSON index off-by-one (`+1` added as if LIST); negative index/slice; `bag_merge` right-wins / N-arg / null-arg / deep-merge wrong; `set_union` drops args; `set_has_element` coerces types; `treepath`→`JSON_KEYS`; `pack_array` BigInteger; JSON null not treated as SQL NULL. Mostly `ExpressionSqlBuilder.cs` (+ `DuckDbDialect.cs`).

| Category | Count | Effort |
|---|---|---|
| array_length-on-nonarray-not-null / array-fn-nonarray-returns-empty / bag-fn-nonobject | 2+2+1 | small/medium |
| bag_merge variants: precedence/null/deep/N-arg (×6) | ~6 | small/medium |
| json/dynamic index & slice negative/off-by-one (×5) | ~5 | medium |
| set_union / set_has_element / treepath / pack_array / dynamic_to_json-sort-keys / json-null-as-null | several | small/large |

**Fix:** guard array/bag fns with `json_type` returning NULL for non-array/non-object; use 0-based `json_extract('$[i]')` without `+1` and keep JSON-typed; left-biased N-arg `bag_merge` coalescing null args to `{}`; treat JSON null as SQL NULL.

### Other notable singletons
union withsource/schema column order (`JoinHandlers.cs`, 2+1); null-row survives negated predicate / null-comparison three-valued (2+1); parse-kv quote/multichar delimiter (`ParseHandlers.cs`, 3); bin/bool/arg_max one-offs; bag_unpack (large).

### Highest-leverage few
1. **Theme A** — one `ConvertStringLiteral` rewrite clears ~40 diffs.
2. **Theme B** — `json_extract_string` + `COALESCE('')` + `gettype`/`json_type` rewrite clears ~40 (and spills into comparator gaps).
3. **Theme C** — one reset-group/`LAG`-`LEAD`-arg helper clears the ~30-row scan/window family.
4. **Theme G** — a one-line `ApplySort` direction fix clears ~12.
5. **Theme D** — extending the existing EXCLUDE rewrite to CTE/join/base-case clears ~13.

---

## 3. Comparator gaps (13)

These are rows where **values are equal** and only the declared type/format or the harness reader differs:

| Category | Count | Suspect |
|---|---|---|
| dynamic-col-returns-varchar | 3 | `Comparator.cs` |
| dynamic-scalar-vs-varchar-same-value | 2 | `Comparator.cs` |
| tostring-of-json-keeps-quotes | 2 | `ExpressionSqlBuilder.cs` |
| list-with-null-unreadable-harness | 2 | `Comparator.cs` |
| split-index-returns-string-not-dynamic | 1 | `Comparator.cs` |
| dynamic-array-element-returns-varchar | 1 | `Comparator.cs` |
| dynamic-datetime-formatting | 1 | `Comparator.cs` |
| dynamic-decimal-quoted-vs-number | 1 | `Comparator.cs` |

**Single change that clears most:** make **`tests/KqlToSql.Fuzzer/Comparator.cs`** Dynamic-vs-String tolerant — when one side is a Dynamic JSON scalar and the other a plain VARCHAR holding the same text, strip surrounding quotes and compare unwrapped; parse-and-compare numerics with epsilon; normalize datetime strings. Critically, also make the **DuckDB list/array reader tolerate NULL elements** (it currently throws `IndexOutOfRangeException` → `<unreadable>`), which alone clears the 2 `list-with-null-unreadable-harness` rows and the related Unknown/LIST cases. This one comparator hardening clears ~9–11 of the 13 without touching the translator.

---

## 4. Tolerable (won't fix) — 29

Engine-specific differences intentionally accepted:

datetime-subus-precision (4), top-hitters-tie-order (3), make_list-order (3), decimal-precision-and-gettype (2), datetime-domain-underflow-null-vs-clamp (2), and one each: order-by-ties-unstable, datatable-backslash-escape-harness, sum-overflow-wrap, tick-subus-precision, nan-equality-engine-difference, long-to-double-overflow-wrap, real-to-int-overflow-clamp, top-nested-tie-order, nan-min-handling, bigint-avg-overflow, datetime-domain-extreme-date-null, bag-keys-order, datetime-subus-precision-and-domain-wrap, int-overflow-wrap-vs-null.

Mostly sub-microsecond precision, tie/ordering instability, and overflow-wrap-vs-null — not worth chasing.

---

## 5. Recommended next batch (best impact/effort)

1. **`ConvertStringLiteral`: strip `@"` and decode escapes via `LiteralValue`** (Theme A) — ~40 diffs, medium. Single function, the biggest single win. Apply the same to datatable cells in `TabularHandlers.cs`.
2. **`ApplySort` direction fix** (Theme G) — ~12 diffs, small. Likely a one-line token-comparison bug; pure upside.
3. **`tostring` of dynamic via `json_extract_string` + `COALESCE(..., '')`, and bool→`'True'/'False'`** (Theme B, the small/medium slices) — ~20 diffs, medium. Defer the full "carry dynamic columns as JSON" (large) to a later pass.
4. **Extend the existing EXCLUDE-redefine rewrite to CTE bodies, join inputs, and the SELECT-1 base case** (Theme D) — ~13 diffs, medium. Reuses logic already present for the source-column path.
5. **Scan/window reset-group helper + `LAG/LEAD(arg, offset, default)`** (Theme C) — ~30 diffs, large but one shared mechanism. Highest absolute count once the cheap wins are banked.

Parallel low-risk add-on: **harden `Comparator.cs`** (Dynamic-vs-String unwrap + NULL-tolerant list reader) to retire ~9–11 comparator-gap rows independently of translator work.

Relevant files: `src/KqlToSql/Expressions/ExpressionSqlBuilder.cs`, `src/KqlToSql/Dialects/DuckDbDialect.cs`, `src/KqlToSql/Operators/TabularHandlers.cs`, `src/KqlToSql/Operators/AggregationHandlers.cs`, `src/KqlToSql/Operators/AdvancedHandlers.cs`, `src/KqlToSql/Operators/JoinHandlers.cs`, `src/KqlToSql/Operators/ParseHandlers.cs`, `tests/KqlToSql.Fuzzer/Comparator.cs`.
</details>
