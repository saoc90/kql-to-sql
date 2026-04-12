# Agent Instructions

- Track operator coverage using `KqlOperatorsChecklist.md` when adding features.
- For each supported operator, derive unit tests from the official Kusto documentation examples and run them against the StormEvents dataset.
- The project uses the C# Kusto AST parser which is already included; use it for parsing KQL before translation.
- Validate generated SQL by executing it with the installed DuckDB engine against the StormEvents database.
- A helper pre-initializes `StormEvents.duckdb`; tests should reuse this database instead of reloading CSV data each time.

---

## Recurring sub-agent audits

The following sub-agent audits should be run regularly — especially after adding new operators, functions, or changing SQL generation logic. They catch performance regressions and correctness issues that unit tests alone miss.

### 1. Query plan performance audit

**When to run:** After any change to `OperatorSqlTranslator.cs`, `ExpressionSqlBuilder.cs`, or dialect files.

**What it does:** Converts a set of representative KQL queries to SQL, runs `EXPLAIN ANALYZE` in DuckDB, and inspects the optimized query plans for problems.

**How to invoke:**

```
Spawn a sub-agent with this prompt:

"You are a DuckDB SQL performance expert. The project is at <repo_root>.
Create a temp .NET project that:
1. Loads DuckDB from tests/KqlToSql.DuckDbExtension.Tests/bin/Debug/net10.0/libduckdb.dylib
2. Loads StormEvents from src/WebDemo/wwwroot/StormEvents.csv.gz
3. Converts these KQL queries and runs EXPLAIN ANALYZE on each:
   - StormEvents | summarize arg_max(InjuriesDirect, *) by State | top 5 by InjuriesDirect desc
   - StormEvents | where State == 'TEXAS' | sample 10
   - StormEvents | sort by InjuriesDirect desc | take 100 | serialize rn = row_number()
   - StormEvents | where EventType matches regex '^Tornado|^Flood' | summarize cnt = count() by State
   - let a = StormEvents | summarize cnt = count() by State; let b = StormEvents | summarize inj = sum(InjuriesDirect) by State; a | join kind=inner b on State | top 5 by cnt desc
   - StormEvents | extend TotalInj = InjuriesDirect + InjuriesIndirect | summarize TotalInj = sum(TotalInj) by State | top 5 by TotalInj desc
   - StormEvents | top-nested 3 of State by count(), top-nested 2 of EventType by count()
For each query output the SQL, plan, and whether any issues are visible
(double projections, unnecessary subqueries, filters not pushed into scans, etc.)."
```

**What to look for:**
- Double `PROJECTION` nodes (unnecessary subquery wrapping)
- `FILTER` nodes that should be pushed into `TABLE_SCAN` (e.g., regex not rewritten to LIKE)
- `WINDOW` functions processing more rows than necessary (e.g., ROW_NUMBER on full table before LIMIT)
- `HASH_JOIN` with a full table scan on one side when it should be a small subquery
- `SELECT *` materializing all columns when only a few are needed

### 2. Remaining bottleneck finder

**When to run:** After a performance audit reveals all known issues are fixed, to find new ones.

**What it does:** Reads the source code and identifies structural SQL generation patterns that produce suboptimal query plans.

**How to invoke:**

```
Spawn a sub-agent with this prompt:

"You are a DuckDB SQL performance expert. Read these source files:
- src/KqlToSql/Operators/OperatorSqlTranslator.cs
- src/KqlToSql/Expressions/ExpressionSqlBuilder.cs
- src/KqlToSql/Dialects/DuckDbDialect.cs

Find remaining optimization opportunities. Focus on:
1. Unnecessary subquery nesting (SELECT * FROM (subquery) patterns)
2. Aggregate inlining (extend + summarize producing intermediate subquery)
3. DuckDB-specific features not used (USING SAMPLE, GROUP BY ALL, FILTER WHERE, etc.)
4. Window function placement issues
Report ONLY clear, impactful issues with specific method names and code changes.
Keep it under 400 words."
```

**Known optimizations already applied:**
- `Qualify()` avoids double-wrapping SELECT statements
- `ApplyFilter` checks for trailing ORDER BY/LIMIT before appending AND
- Simple `^prefix` regex patterns rewritten to LIKE for scan pushdown
- `ApplySample` uses `USING SAMPLE n ROWS` on DuckDB
- `ApplySerialize` wraps in subquery when LIMIT is present (so window runs on limited rows)
- Join/lookup uses `UnwrapFromSql()` to avoid `(SELECT * FROM cte)` wrappers
- `GROUP BY ALL` used for DuckDB summarize
- `FILTER (WHERE ...)` used for all 17 conditional aggregate functions

### 3. Dialect parity checker

**When to run:** After adding functions to DuckDbDialect.

**What it does:** Compares function lists between DuckDb and PGlite dialects to find gaps.

**How to invoke:**

```
Spawn a sub-agent with this prompt:

"Compare the function lists in:
- src/KqlToSql/Dialects/DuckDbDialect.cs (TryTranslateFunction + TryTranslateAggregate)
- src/KqlToSql/Dialects/PGliteDialect.cs (same methods)
List every function present in DuckDb but missing in PGlite, and vice versa.
For each missing function, suggest the PostgreSQL equivalent if one exists."
```

### 4. KQL documentation coverage checker

**When to run:** Periodically (monthly) or before releases.

**What it does:** Fetches the official KQL operator/function documentation and compares against `KqlOperatorsChecklist.md` to find newly added KQL features not yet supported.

**How to invoke:**

```
Spawn a sub-agent with this prompt:

"Fetch https://learn.microsoft.com/en-us/kusto/query/ and enumerate all
tabular operators, scalar functions, and aggregate functions listed in the
official docs. Compare against KqlOperatorsChecklist.md in the repo.
Report any operators or functions in the docs that are not in the checklist
(either as implemented or explicitly unsupported). Focus on commonly-used
functions only, skip obscure plugins."
```
