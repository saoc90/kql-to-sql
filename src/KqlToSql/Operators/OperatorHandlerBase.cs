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

    /// <summary>Returns true if the string is a single parenthesized group: '(' at pos 0 matches ')' at the final position
    /// (i.e. safe to strip the outer parens without losing content).</summary>
    protected static bool IsFullyParenthesized(string sql)
    {
        if (sql.Length < 2 || sql[0] != '(' || sql[^1] != ')') return false;
        int depth = 0;
        bool inStr = false;
        char quote = ' ';
        for (int i = 0; i < sql.Length; i++)
        {
            var c = sql[i];
            if (inStr)
            {
                if (c == quote) inStr = false;
                continue;
            }
            if (c == '\'' || c == '"') { inStr = true; quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')')
            {
                depth--;
                if (depth == 0 && i != sql.Length - 1) return false;
            }
        }
        return depth == 0;
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
        if (after.Contains(" ORDER BY ", StringComparison.OrdinalIgnoreCase) ||
            after.Contains(" GROUP BY ", StringComparison.OrdinalIgnoreCase) ||
            after.Contains(" HAVING ", StringComparison.OrdinalIgnoreCase) ||
            after.Contains(" LIMIT ", StringComparison.OrdinalIgnoreCase))
            return false;

        // Ensure the WHERE we found is at the top level of the final SELECT — not nested
        // inside a subquery / CTE. If parens are unbalanced after the WHERE (more closers
        // than openers), the WHERE belongs to an inner query and appending AND ... would
        // attach to the wrong scope.
        int depth = 0;
        foreach (var c in after)
        {
            if (c == '\'' || c == '"') continue; // ignore content inside simple string quoting (approximation)
            if (c == '(') depth++;
            else if (c == ')') { depth--; if (depth < 0) return false; }
        }
        return true;
    }
}
