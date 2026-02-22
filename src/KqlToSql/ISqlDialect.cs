namespace KqlToSql;

/// <summary>
/// Defines the strategy for translating KQL constructs to engine-specific SQL.
/// Implement this interface to support additional SQL engines beyond DuckDB.
/// </summary>
public interface ISqlDialect
{
    /// <summary>The name of the SQL dialect (e.g. "DuckDB", "PostgreSQL").</summary>
    string Name { get; }

    /// <summary>
    /// Translates a KQL scalar function call to the engine-specific SQL equivalent.
    /// Returns null if the function is not recognized by this dialect.
    /// </summary>
    string? TryTranslateFunction(string name, string[] args);

    /// <summary>
    /// Translates a KQL aggregate function call to the engine-specific SQL equivalent.
    /// Returns null if the aggregate is not recognized by this dialect.
    /// </summary>
    string? TryTranslateAggregate(string name, string[] args);

    /// <summary>Maps a Kusto data type name to the engine-specific SQL type.</summary>
    string MapType(string kustoType);

    /// <summary>Returns the keyword used for case-insensitive LIKE (e.g. ILIKE for DuckDB).</summary>
    string CaseInsensitiveLike { get; }

    /// <summary>Generates a JSON field access expression.</summary>
    string JsonAccess(string baseSql, string jsonPath);

    /// <summary>Generates a SELECT clause that excludes specific columns (e.g. "* EXCLUDE (col)").</summary>
    string SelectExclude(string[] columns);

    /// <summary>Generates a SELECT clause that renames specific columns (e.g. "* RENAME (old AS new)").</summary>
    string SelectRename(string[] mappings);

    /// <summary>Generates a window-filter clause (e.g. DuckDB QUALIFY).</summary>
    string Qualify(string condition);

    /// <summary>Generates a series generation query.</summary>
    string GenerateSeries(string alias, string start, string end, string step);

    /// <summary>Generates an array expansion clause (e.g. CROSS JOIN UNNEST).</summary>
    string Unnest(string sourceAlias, string column, string unnestAlias);
}
