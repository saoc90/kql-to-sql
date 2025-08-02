# Agent Instructions

- Track operator coverage using `KqlOperatorsChecklist.md` when adding features.
- For each supported operator, derive unit tests from the official Kusto documentation examples and run them against the StormEvents dataset.
- The project uses the C# Kusto AST parser which is already included; use it for parsing KQL before translation.
- Validate generated SQL by executing it with the installed DuckDB engine against the StormEvents database.
- A helper pre-initializes `StormEvents.duckdb`; tests should reuse this database instead of reloading CSV data each time.
