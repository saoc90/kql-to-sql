# kql-to-sql

Simple KQL to SQL converter built on top of the official Kusto language parser.
The implementation currently supports translating pipelines that include
`where`, `project`, `summarize`, `sort`, `extend`, `take`, `count`, `distinct`,
`project-away`, `project-rename`, `top`, `join`, the `in`/`!in`, `has`/`!has`,
`contains`/`!contains`, `startswith`/`!startswith`, `endswith`/`!endswith`, and
`between`/`!between` scalar operators, dynamic object accessors like
`Metadata['key']` or `Metadata.key`, and the `bin()`, `datetime()`, and
type-casting functions such as `tostring()`, `toint()`, `tolong()`, `todouble()`,
`tobool()`, and `todatetime()`. Control commands are supported for
`.ingest inline` and `.ingest` from CSV files, which map to DuckDB `INSERT`
and `COPY` statements respectively. Generated SQL is
validated against DuckDB in the unit tests using StormEvents data downloaded
from NOAA.

## Run tests

```
dotnet test
```
