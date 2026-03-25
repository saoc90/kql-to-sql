# KqlToSql.DuckDbExtension

A native DuckDB extension that adds KQL (Kusto Query Language) to SQL conversion as built-in DuckDB functions, built using [DuckDB.ExtensionKit](https://github.com/Giorgi/DuckDB.ExtensionKit).

## Functions

| Function | Parameters | Returns | Description |
|----------|-----------|---------|-------------|
| `kql_to_sql(kql)` | `kql: VARCHAR` | `VARCHAR` | Converts a KQL query to DuckDB SQL |
| `kql_to_sql_dialect(kql, dialect)` | `kql: VARCHAR, dialect: VARCHAR` | `VARCHAR` | Converts a KQL query to SQL using a specified dialect (`duckdb`, `pglite`) |
| `kql_explain(kql)` | `kql: VARCHAR` | `TABLE(kql_input, sql_output, dialect)` | Returns a table with the KQL input alongside its SQL translation |

## Usage Examples

```sql
-- Load the extension (after installing)
LOAD kql;

-- Convert a simple KQL query to SQL
SELECT kql_to_sql('StormEvents | where State == ''TEXAS'' | count');
-- Returns: SELECT COUNT(*) AS Count FROM StormEvents WHERE State = 'TEXAS'

-- Convert with pipe operators
SELECT kql_to_sql('StormEvents | where DamageProperty > 0 | project State, DamageProperty | sort by DamageProperty desc | take 10');

-- Use kql_explain to see both KQL and SQL side by side
SELECT * FROM kql_explain('StormEvents | take 5');
-- Returns:  kql_input                   | sql_output                          | dialect
--           StormEvents | take 5         | SELECT * FROM StormEvents LIMIT 5   | duckdb

-- Use a specific dialect
SELECT kql_to_sql_dialect('StormEvents | take 5', 'pglite');
```

### Recommended Workflow

Since DuckDB's C Extension API does not yet expose parser extension hooks, fully transparent
KQL-as-input (typing raw KQL instead of SQL) is not currently possible. The recommended
workflow is:

```python
# Application-level pattern (Python example):
sql = connection.execute("SELECT kql_to_sql(?)", [kql_query]).fetchone()[0]
result = connection.execute(sql).fetchall()
```

```csharp
// C# example:
using var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT kql_to_sql($kql)";
cmd.Parameters.Add(new DuckDBParameter("kql", "StormEvents | where State == 'TEXAS' | count"));
var sql = (string)cmd.ExecuteScalar()!;

cmd.CommandText = sql;
var result = cmd.ExecuteScalar();
```

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download) or later
- Python 3 (for extension metadata packaging)
- [DuckDB.ExtensionKit](https://github.com/Giorgi/DuckDB.ExtensionKit) (included as a git submodule)

## Setup

1. **Clone with submodules** (from the repo root):

   ```bash
   git clone --recurse-submodules <repo-url>
   # Or if already cloned:
   git submodule update --init --recursive
   ```

   This clones [DuckDB.ExtensionKit](https://github.com/Giorgi/DuckDB.ExtensionKit) into `lib/DuckDB.ExtensionKit/`.

2. **Build the extension**:

   ```bash
   # Linux/macOS
   ./build-extension.sh

   # Windows
   .\build-extension.ps1
   ```

   Or manually:

   ```bash
   dotnet publish -c Release -r linux-x64    # Linux
   dotnet publish -c Release -r osx-arm64    # macOS Apple Silicon
   dotnet publish -c Release -r win-x64      # Windows
   ```

3. **Install and load in DuckDB**:

   ```bash
   duckdb -unsigned
   ```

   ```sql
   INSTALL 'path/to/kql.duckdb_extension';
   LOAD kql;
   ```

## How It Works

This extension uses [DuckDB.ExtensionKit](https://github.com/Giorgi/DuckDB.ExtensionKit) to:

1. **Define the extension** using the `[DuckDBExtension]` attribute pattern
2. **Register scalar and table functions** (`kql_to_sql`, `kql_to_sql_dialect`, `kql_explain`) via the type-safe ExtensionKit API
3. **Compile to native code** using .NET Native AOT, producing a standalone `.duckdb_extension` binary
4. **Convert KQL at runtime** using the `KqlToSql` library and its Kusto Language parser

The extension is built with Native AOT compilation, which produces a native binary with no .NET runtime dependency. DuckDB loads it just like any other native extension.

## Project Structure

```
src/KqlToSql.DuckDbExtension/
├── KqlToSql.DuckDbExtension.csproj  # Project file with Native AOT config
├── KqlExtension.cs                   # Extension entry point and function definitions
├── build-extension.sh                # Linux/macOS build script
├── build-extension.ps1               # Windows build script
└── README.md                         # This file
```

## Supported Platforms

| Platform | Runtime Identifier |
|----------|-------------------|
| Linux x64 | `linux-x64` |
| Linux ARM64 | `linux-arm64` |
| macOS x64 | `osx-x64` |
| macOS ARM64 | `osx-arm64` |
| Windows x64 | `win-x64` |
| Windows ARM64 | `win-arm64` |
