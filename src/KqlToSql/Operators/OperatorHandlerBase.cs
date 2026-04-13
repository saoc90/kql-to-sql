using System;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

/// <summary>
/// Base class for all operator handlers. Provides shared access to the dialect,
/// expression builder, and converter, plus SQL string manipulation methods.
/// </summary>
internal abstract class OperatorHandlerBase
{
    protected readonly KqlToSqlConverter Converter;
    protected readonly ExpressionSqlBuilder Expr;
    protected ISqlDialect Dialect => Converter.Dialect;

    protected OperatorHandlerBase(KqlToSqlConverter converter, ExpressionSqlBuilder expr)
    {
        Converter = converter;
        Expr = expr;
    }

    private const string SelectStarPrefix = "SELECT * FROM ";

    protected static bool IsSimpleSelectStar(string sql)
        => sql.StartsWith(SelectStarPrefix, StringComparison.OrdinalIgnoreCase);

    /// <summary>Extracts the FROM source, or wraps in parens as subquery.</summary>
    protected static string ExtractFrom(string sql)
    {
        if (IsSimpleSelectStar(sql))
            return sql.Substring(SelectStarPrefix.Length);
        return $"({sql})";
    }

    /// <summary>Unwraps simple table references for JOIN clauses. Complex queries get parenthesized.</summary>
    protected static string UnwrapFrom(string sql)
    {
        if (IsSimpleSelectStar(sql))
        {
            var rest = sql.Substring(SelectStarPrefix.Length);
            if (!rest.Contains(' '))
                return rest;
        }
        return $"({sql})";
    }

    /// <summary>Replaces SELECT * with specific columns.</summary>
    protected static string ReplaceSelectStar(string sql, string columns)
    {
        if (IsSimpleSelectStar(sql))
            return $"SELECT {columns} FROM {sql.Substring(SelectStarPrefix.Length)}";
        return $"SELECT {columns} FROM ({sql})";
    }

    /// <summary>Appends extra columns to SELECT *.</summary>
    protected static string AppendToSelectStar(string sql, string extras)
    {
        if (IsSimpleSelectStar(sql))
            return $"SELECT *, {extras} FROM {sql.Substring(SelectStarPrefix.Length)}";
        return $"SELECT *, {extras} FROM ({sql})";
    }

    protected static bool HasLimit(string sql)
        => sql.Contains(" LIMIT ", StringComparison.OrdinalIgnoreCase);

    protected static bool CanAppendWhere(string sql)
    {
        var idx = sql.LastIndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
        if (idx < 0) return false;
        var after = sql[idx..];
        return !after.Contains(" ORDER BY ", StringComparison.OrdinalIgnoreCase) &&
               !after.Contains(" GROUP BY ", StringComparison.OrdinalIgnoreCase) &&
               !after.Contains(" HAVING ", StringComparison.OrdinalIgnoreCase) &&
               !after.Contains(" LIMIT ", StringComparison.OrdinalIgnoreCase);
    }
}
