using System.Collections.Generic;

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

    /// <summary>Index a JSON value by a (possibly dynamic) string key expression, returning the
    /// navigable JSON child. The key expression is already rendered SQL (e.g. a quoted literal or a
    /// column reference).</summary>
    string JsonIndexByKey(string baseSql, string keyExpr) =>
        $"json_extract({baseSql}, '$.' || {keyExpr})";

    /// <summary>Formats a timespan/INTERVAL value as Kusto's tostring() text: [-][d.]hh:mm:ss[.fffffff]
    /// (day part only when non-zero; 7-digit fraction only when non-zero). Used by tostring(timespan).</summary>
    string TimespanToString(string intervalSql)
    {
        var totalUs = $"(EXTRACT(EPOCH FROM ({intervalSql})) * 1000000)";
        var absUs = $"ABS({totalUs})";
        string comp(string expr) => $"CAST(FLOOR({expr}) AS BIGINT)";
        var days = comp($"{absUs} / 86400000000");
        var hh = comp($"MOD({absUs}, 86400000000) / 3600000000");
        var mm = comp($"MOD({absUs}, 3600000000) / 60000000");
        var ss = comp($"MOD({absUs}, 60000000) / 1000000");
        var fracUs = comp($"MOD({absUs}, 1000000)");
        return
            $"((CASE WHEN {totalUs} < 0 THEN '-' ELSE '' END) || " +
            $"(CASE WHEN {days} > 0 THEN CAST({days} AS VARCHAR) || '.' ELSE '' END) || " +
            $"LPAD(CAST({hh} AS VARCHAR), 2, '0') || ':' || LPAD(CAST({mm} AS VARCHAR), 2, '0') || ':' || " +
            $"LPAD(CAST({ss} AS VARCHAR), 2, '0') || " +
            $"(CASE WHEN {fracUs} > 0 THEN '.' || LPAD(CAST({fracUs} * 10 AS VARCHAR), 7, '0') ELSE '' END))";
    }

    /// <summary>Index a JSON array by a 0-based (possibly dynamic) integer position expression,
    /// returning the navigable JSON element. A negative index counts from the end (Kusto d[-1] = last),
    /// rendered with DuckDB's '$[#-N]' length-relative path syntax.</summary>
    string JsonIndexByPosition(string baseSql, string indexExpr) =>
        $"json_extract({baseSql}, '$[' || (CASE WHEN ({indexExpr}) < 0 THEN '#' ELSE '' END) || ({indexExpr}) || ']')";

    /// <summary>Generates a SELECT clause that excludes specific columns (e.g. "* EXCLUDE (col)").</summary>
    string SelectExclude(string[] columns);

    /// <summary>Replaces a column in place, preserving its position (e.g. "alias.* REPLACE (expr AS col)").
    /// Returns null when the dialect can't replace-in-place (caller falls back to EXCLUDE + append).</summary>
    string? SelectReplace(string starAlias, string col, string expr) => null;

    /// <summary>Replaces one or more columns in place over a bare star, preserving each column's position
    /// (e.g. "* REPLACE (e1 AS c1, e2 AS c2)"). Used by extend when it redefines existing columns so they
    /// keep their original position (Kusto semantics) instead of being dropped and re-appended at the end.
    /// Returns null when the dialect can't replace-in-place (caller falls back to EXCLUDE + append).</summary>
    string? SelectStarReplace(IReadOnlyList<(string Col, string Expr)> replacements) => null;

    /// <summary>Generates a SELECT clause that renames specific columns (e.g. "* RENAME (old AS new)").</summary>
    string SelectRename(string[] mappings);

    /// <summary>Wraps a query with a window-filter condition (e.g. DuckDB QUALIFY, PGlite subquery).</summary>
    string Qualify(string innerSql, string condition);

    /// <summary>Generates a series generation query.</summary>
    string GenerateSeries(string alias, string start, string end, string step);

    /// <summary>Generates an array expansion clause (e.g. CROSS JOIN UNNEST).</summary>
    string Unnest(string sourceAlias, string column, string unnestAlias);

    /// <summary>Whether this dialect supports GROUP BY ALL (auto group by non-aggregate columns).</summary>
    bool SupportsGroupByAll => false;

    /// <summary>Generates a null-safe cast expression. DuckDB uses TRY_CAST, PGlite falls back to CAST.</summary>
    string SafeCast(string expr, string sqlType) => $"TRY_CAST({expr} AS {sqlType})";

    /// <summary>Parses a value to a timestamp the way KQL's lenient <c>todatetime()</c> does. By default
    /// this is just a null-safe TIMESTAMP cast; dialects whose native cast only accepts ISO-8601 (e.g.
    /// DuckDB) override this to add format fallbacks so locale/US/abbreviated date strings parse instead
    /// of silently becoming NULL.</summary>
    string ParseDateTime(string expr) => SafeCast(expr, "TIMESTAMP");

    /// <summary>Milliseconds-since-epoch of a timestamp expression. Used by datetime bin()/bin_at().
    /// Defaults to DuckDB's EPOCH_MS; Postgres-family dialects override (no epoch_ms function).</summary>
    string EpochMillis(string tsExpr) => $"EPOCH_MS(CAST({tsExpr} AS TIMESTAMP))";

    /// <summary>A timestamp from a milliseconds-since-epoch expression (inverse of <see cref="EpochMillis"/>).</summary>
    string TimestampFromMillis(string msExpr) => $"EPOCH_MS(CAST({msExpr} AS BIGINT))";

    /// <summary>Milliseconds in an interval/timespan expression.</summary>
    string IntervalMillis(string intervalExpr) => $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + ({intervalExpr}) AS TIMESTAMP))";

    /// <summary>True when the engine's `/` operator already performs KQL-style integer (truncating)
    /// division for integer operands (PostgreSQL does; DuckDB does real division and needs a rewrite).</summary>
    bool NativeIntegerDivision => false;

    /// <summary>Generates a random sample clause. Returns null to use ORDER BY RANDOM() LIMIT n fallback.</summary>
    string? SampleClause(string fromSql, string count) => null;
}
