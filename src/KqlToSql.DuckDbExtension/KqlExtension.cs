using DuckDB.ExtensionKit;
using DuckDB.ExtensionKit.Native;
using DuckDB.ExtensionKit.ScalarFunctions;
using DuckDB.ExtensionKit.TableFunctions;
using KqlToSql;
using KqlToSql.Dialects;

namespace KqlToSql.DuckDbExtension;

/// <summary>
/// DuckDB extension that exposes KQL (Kusto Query Language) to SQL conversion
/// as native DuckDB functions, built using DuckDB.ExtensionKit.
///
/// Provides:
/// - <c>kql_to_sql(kql)</c>: scalar function that converts a KQL query string to DuckDB SQL.
/// - <c>kql_to_sql_dialect(kql, dialect)</c>: scalar function with explicit dialect selection.
/// - <c>kql_explain(kql)</c>: table function that returns the KQL input alongside its SQL translation.
///
/// Usage pattern — convert then execute the result in application code or via DuckDB SQL:
/// <code>
/// -- Convert KQL to SQL (returns a string)
/// SELECT kql_to_sql('StormEvents | where State == ''TEXAS'' | count');
///
/// -- See both KQL and SQL side by side
/// SELECT * FROM kql_explain('StormEvents | take 5');
/// </code>
///
/// NOTE: DuckDB's C Extension API does not yet expose parser extension hooks, so
/// fully transparent KQL-as-input (typing raw KQL instead of SQL) is not possible.
/// The recommended workflow is to call kql_to_sql() to obtain SQL and then execute
/// that SQL string from your application, or use DuckDB macros for convenience:
/// <code>
/// -- Application-level pattern (pseudocode):
/// sql = connection.execute("SELECT kql_to_sql(?)", kql).fetchone()[0]
/// result = connection.execute(sql)
/// </code>
/// </summary>
[DuckDBExtension]
public static partial class KqlExtension
{
    private static void RegisterFunctions(DuckDBConnection connection)
    {
        connection.RegisterScalarFunction<string, string>("kql_to_sql", ConvertKqlToSql);

        connection.RegisterScalarFunction<string, string, string>("kql_to_sql_dialect", ConvertKqlToSqlWithDialect);

        connection.RegisterTableFunction("kql_explain", (string kql) => ExplainKql(kql),
            row => new { kql_input = row.Kql, sql_output = row.Sql, dialect = row.Dialect });
    }

    /// <summary>
    /// Converts a KQL query string to its DuckDB SQL equivalent.
    /// </summary>
    private static string ConvertKqlToSql(string kql)
    {
        var converter = new KqlToSqlConverter(new DuckDbDialect());
        return converter.Convert(kql);
    }

    /// <summary>
    /// Converts a KQL query string to SQL using the specified dialect.
    /// </summary>
    private static string ConvertKqlToSqlWithDialect(string kql, string dialect)
    {
        ISqlDialect sqlDialect = dialect.ToLowerInvariant() switch
        {
            "duckdb" => new DuckDbDialect(),
            "pglite" => new PGliteDialect(),
            _ => throw new ArgumentException($"Unknown dialect: {dialect}. Supported dialects: duckdb, pglite")
        };

        var converter = new KqlToSqlConverter(sqlDialect);
        return converter.Convert(kql);
    }

    private record KqlExplainRow(string Kql, string Sql, string Dialect);

    /// <summary>
    /// Returns a single-row table showing the KQL input alongside the generated SQL.
    /// </summary>
    private static KqlExplainRow[] ExplainKql(string kql)
    {
        var converter = new KqlToSqlConverter(new DuckDbDialect());
        var sql = converter.Convert(kql);
        return [new KqlExplainRow(kql, sql, "duckdb")];
    }
}
