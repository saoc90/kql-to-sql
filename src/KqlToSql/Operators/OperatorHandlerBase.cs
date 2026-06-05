using System;
using System.Linq;
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

    protected const string ResetGroupMarker = "__RESETGRP__(";

    /// <summary>True when an extend/serialize projection contains restart-window reset-group markers.</summary>
    protected static bool HasResetGroupMarker(string joined) =>
        joined.Contains(ResetGroupMarker, StringComparison.Ordinal);

    /// <summary>Lowers __RESETGRP__(&lt;pred&gt;) markers (emitted by row_cumsum(value, restart) and friends)
    /// into a partitioned window. A window can't reference another window, and the predicate itself may be
    /// a window (g != prev(g) → g != LAG(g) OVER ()), so we layer three SELECTs:
    ///   L1: evaluate each distinct predicate to a boolean column _rbN  (one window level — LAG is fine)
    ///   L2: reset-group _rgN = running count of _rbN (SUM(CASE…) OVER rows — no nested window)
    ///   out: the marker becomes _rgN so the outer window partitions by it; helper cols are EXCLUDEd.</summary>
    protected string HoistResetGroups(string leftSql, string joined, params string[] alsoExclude)
    {
        var preds = new List<string>();
        var rewritten = new System.Text.StringBuilder();
        int i = 0;
        while (i < joined.Length)
        {
            int at = joined.IndexOf(ResetGroupMarker, i, StringComparison.Ordinal);
            if (at < 0) { rewritten.Append(joined, i, joined.Length - i); break; }
            rewritten.Append(joined, i, at - i);
            int start = at + ResetGroupMarker.Length, depth = 1, j = start;
            for (; j < joined.Length && depth > 0; j++)
            {
                if (joined[j] == '(') depth++;
                else if (joined[j] == ')') depth--;
            }
            var pred = joined.Substring(start, j - 1 - start);
            int idx = preds.IndexOf(pred);
            if (idx < 0) { idx = preds.Count; preds.Add(pred); }
            rewritten.Append($"_rg{idx}");
            i = j;
        }

        var l1Defs = preds.Select((p, n) => $"({p}) AS _rb{n}");
        var l1 = $"SELECT *, {string.Join(", ", l1Defs)} FROM ({leftSql})";
        var l2Defs = preds.Select((_, n) =>
            $"SUM(CASE WHEN _rb{n} THEN 1 ELSE 0 END) OVER (ROWS UNBOUNDED PRECEDING) AS _rg{n}");
        var l2 = $"SELECT *, {string.Join(", ", l2Defs)} FROM ({l1})";
        var dropCols = Enumerable.Range(0, preds.Count).SelectMany(n => new[] { $"_rb{n}", $"_rg{n}" })
            .Concat(alsoExclude).ToArray();
        return $"SELECT {Dialect.SelectExclude(dropCols)}, {rewritten} FROM ({l2})";
    }

    /// <summary>Extracts the FROM source, or wraps in parens as subquery.</summary>
    protected static string ExtractFrom(string sql)
    {
        if (IsSimpleSelectStar(sql))
            return sql.Substring(SelectStarPrefix.Length);
        return $"({sql})";
    }

    /// <summary>Always returns a form safe to follow with 'AS alias CROSS JOIN …' or similar.
    /// If the FROM-source contains a trailing WHERE / ORDER / GROUP / LIMIT / QUALIFY,
    /// wraps in parens; otherwise returns the bare source.</summary>
    protected static string ExtractFromAsRelation(string sql)
    {
        if (IsSimpleSelectStar(sql))
        {
            var rest = sql.Substring(SelectStarPrefix.Length);
            if (!HasTopLevelTail(rest, " WHERE ") &&
                !HasTopLevelTail(rest, " ORDER BY ") &&
                !HasTopLevelTail(rest, " GROUP BY ") &&
                !HasTopLevelTail(rest, " HAVING ") &&
                !HasTopLevelTail(rest, " LIMIT ") &&
                !HasTopLevelTail(rest, " QUALIFY "))
                return rest;
        }
        return $"({sql})";
    }

    private static bool HasTopLevelTail(string sql, string clause)
    {
        int depth = 0;
        bool inStr = false;
        char quote = ' ';
        for (int i = 0; i <= sql.Length - clause.Length; i++)
        {
            var c = sql[i];
            if (inStr) { if (c == quote) inStr = false; continue; }
            if (c == '\'' || c == '"') { inStr = true; quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && string.Compare(sql, i, clause, 0, clause.Length, StringComparison.OrdinalIgnoreCase) == 0)
                return true;
        }
        return false;
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

    /// <summary>Returns the key list of a top-level (paren-depth 0) trailing ORDER BY in
    /// <paramref name="sql"/> — e.g. "Timestamp ASC, Value ASC" — or null when there is none.
    /// Lets the next operator carry the established serialization order into its window functions
    /// (prev/next/row_number/row_cumsum), matching Kusto's "order by … | serialize/extend prev()".</summary>
    protected static string? ExtractTrailingOrderBy(string sql)
    {
        int depth = 0; bool inStr = false; char quote = ' '; int keysAt = -1;
        for (int i = 0; i < sql.Length; i++)
        {
            char c = sql[i];
            if (inStr) { if (c == quote) inStr = false; continue; }
            if (c == '\'' || c == '"') { inStr = true; quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && i + 10 <= sql.Length &&
                     string.Compare(sql, i, " ORDER BY ", 0, 10, StringComparison.OrdinalIgnoreCase) == 0)
                keysAt = i + 10;
        }
        if (keysAt < 0) return null;
        var keys = sql.Substring(keysAt).Trim();
        int lim = keys.IndexOf(" LIMIT ", StringComparison.OrdinalIgnoreCase);
        if (lim >= 0) keys = keys.Substring(0, lim).Trim();
        return keys.Length > 0 ? keys : null;
    }

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
