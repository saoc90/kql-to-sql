using System;

namespace KqlToSql.Operators;

/// <summary>
/// Shared SQL string manipulation utilities used across operator handlers.
/// Centralizes the "SELECT * FROM" pattern matching and FROM-clause extraction
/// that was previously duplicated in every operator method.
/// </summary>
internal static class SqlHelper
{
    private const string SelectStarFrom = "SELECT * FROM ";

    /// <summary>
    /// Checks if the SQL is a simple "SELECT * FROM tableName" with no clauses.
    /// </summary>
    internal static bool IsSimpleSelectStar(string sql)
        => sql.StartsWith(SelectStarFrom, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Extracts the FROM source from "SELECT * FROM source", or wraps as subquery.
    /// </summary>
    internal static string ExtractFromSource(string sql)
    {
        if (IsSimpleSelectStar(sql))
            return sql.Substring(SelectStarFrom.Length);
        return $"({sql})";
    }

    /// <summary>
    /// Unwraps "SELECT * FROM tableName" to just "tableName" for FROM/JOIN clauses.
    /// Only unwraps simple identifiers (no spaces). Falls back to parenthesized subquery.
    /// </summary>
    internal static string UnwrapFromSql(string sql)
    {
        if (IsSimpleSelectStar(sql))
        {
            var rest = sql.Substring(SelectStarFrom.Length);
            if (!rest.Contains(' '))
                return rest;
        }
        return $"({sql})";
    }

    /// <summary>
    /// Returns true if the SQL has a WHERE clause that can be safely appended to with AND.
    /// False if WHERE is followed by ORDER BY, GROUP BY, HAVING, or LIMIT.
    /// </summary>
    internal static bool CanAppendWhereCondition(string sql)
    {
        var whereIdx = sql.LastIndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
        if (whereIdx < 0) return false;

        var afterWhere = sql[whereIdx..];
        return !afterWhere.Contains(" ORDER BY ", StringComparison.OrdinalIgnoreCase) &&
               !afterWhere.Contains(" GROUP BY ", StringComparison.OrdinalIgnoreCase) &&
               !afterWhere.Contains(" HAVING ", StringComparison.OrdinalIgnoreCase) &&
               !afterWhere.Contains(" LIMIT ", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Replaces "SELECT *" with "SELECT columns" in a simple SELECT * FROM query.
    /// Falls back to wrapping as subquery if the SQL is complex.
    /// </summary>
    internal static string ReplaceSelectStar(string sql, string columns)
    {
        if (IsSimpleSelectStar(sql))
        {
            var rest = sql.Substring(SelectStarFrom.Length);
            return $"SELECT {columns} FROM {rest}";
        }
        return $"SELECT {columns} FROM ({sql})";
    }

    /// <summary>
    /// Appends extra columns to a SELECT * query: "SELECT *, extras FROM source".
    /// Falls back to wrapping as subquery if the SQL is complex.
    /// </summary>
    internal static string AppendToSelectStar(string sql, string extras)
    {
        if (IsSimpleSelectStar(sql))
        {
            var rest = sql.Substring(SelectStarFrom.Length);
            return $"SELECT *, {extras} FROM {rest}";
        }
        return $"SELECT *, {extras} FROM ({sql})";
    }

    /// <summary>
    /// Returns true if the SQL contains a LIMIT clause.
    /// </summary>
    internal static bool HasLimit(string sql)
        => sql.Contains(" LIMIT ", StringComparison.OrdinalIgnoreCase);
}
