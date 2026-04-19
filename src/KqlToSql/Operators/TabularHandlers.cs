using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

internal class TabularHandlers : OperatorHandlerBase
{
    internal TabularHandlers(KqlToSqlConverter converter, ExpressionSqlBuilder expr)
        : base(converter, expr) { }

    internal string ApplyFilter(string leftSql, FilterOperator filter)
    {
        var condition = Expr.ConvertExpression(filter.Condition);

        // Only append WHERE directly if there's no ORDER BY, LIMIT, or GROUP BY trailing
        if (IsSimpleSelectStar(leftSql) &&
            !leftSql.Contains(" WHERE ", StringComparison.OrdinalIgnoreCase) &&
            !leftSql.Contains(" ORDER BY ", StringComparison.OrdinalIgnoreCase) &&
            !leftSql.Contains(" LIMIT ", StringComparison.OrdinalIgnoreCase) &&
            !leftSql.Contains(" GROUP BY ", StringComparison.OrdinalIgnoreCase))
            return $"{leftSql} WHERE {condition}";

        if (CanAppendWhere(leftSql))
            return $"{leftSql} AND {condition}";

        return $"SELECT * FROM ({leftSql}) WHERE {condition}";
    }

    internal string ApplyProject(string leftSql, ProjectOperator project)
    {
        var columns = project.Expressions.Select(se =>
        {
            if (se.Element is SimpleNamedExpression sne)
                return $"{Expr.ConvertExpression(sne.Expression)} AS {Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(sne.Name.ToString().Trim())}";
            // Bare JSON / path access (project DataMetadata.SectionName) → KQL auto-names
            // the output column as the underscore-joined path.
            var synthesized = SynthesizePathAlias(se.Element);
            var sql = Expr.ConvertExpression(se.Element);
            return synthesized != null ? $"{sql} AS {synthesized}" : sql;
        }).ToArray();

        return ReplaceSelectStar(leftSql, string.Join(", ", columns));
    }

    private static string? SynthesizePathAlias(SyntaxElement element)
    {
        // Drill through single-arg conversion wrappers like tostring(...), toreal(...), toint(...) —
        // KQL auto-names 'tostring(DataMetadata.SectionName)' as 'DataMetadata_SectionName' too.
        SyntaxNode? current = element as SyntaxNode;
        while (current is FunctionCallExpression fce && fce.ArgumentList.Expressions.Count == 1)
        {
            var fname = fce.Name.ToString().Trim().ToLowerInvariant();
            if (fname is "tostring" or "toreal" or "todouble" or "toint" or "tolong" or "tobool" or "tofloat" or "todatetime" or "todynamic")
                current = fce.ArgumentList.Expressions[0].Element;
            else break;
        }

        var segments = new List<string>();
        while (current is PathExpression pe)
        {
            segments.Insert(0, pe.Selector.ToString().Trim());
            current = pe.Expression;
        }
        if (segments.Count == 0) return null;
        if (current is NameReference nr)
            segments.Insert(0, nr.Name.ToString().Trim());
        else
            return null;
        return Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(string.Join("_", segments));
    }

    internal string ApplyProjectAway(string leftSql, ProjectAwayOperator projectAway)
    {
        var columns = projectAway.Expressions
            .Select(se => Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(se.Element.ToString().Trim()))
            .ToArray();
        return ReplaceSelectStar(leftSql, Dialect.SelectExclude(columns));
    }

    internal string ApplyProjectRename(string leftSql, ProjectRenameOperator projectRename)
    {
        var mappings = projectRename.Expressions.Select(se =>
        {
            if (se.Element is SimpleNamedExpression sne)
                return $"{Expr.ConvertExpression(sne.Expression)} AS {Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(sne.Name.ToString().Trim())}";
            throw new NotSupportedException("Unsupported project-rename expression");
        }).ToArray();

        return ReplaceSelectStar(leftSql, Dialect.SelectRename(mappings));
    }

    internal string ApplyProjectKeep(string leftSql, ProjectKeepOperator projectKeep)
    {
        var columns = projectKeep.Expressions
            .Select(se => Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(se.Element.ToString().Trim()))
            .ToArray();
        return ReplaceSelectStar(leftSql, string.Join(", ", columns));
    }

    internal string ApplyProjectReorder(string leftSql, ProjectReorderOperator projectReorder)
    {
        var columns = projectReorder.Expressions
            .Select(se => Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(se.Element.ToString().Trim()))
            .ToArray();
        return ReplaceSelectStar(leftSql, $"{string.Join(", ", columns)}, {Dialect.SelectExclude(columns)}");
    }

    internal string ApplyExtend(string leftSql, ExtendOperator extend)
    {
        var extras = new List<(string Expr, string Name)>();
        // KQL auto-numbers anonymous (unnamed, non-path, non-identifier) extend results Column1, Column2, ...
        int anonCounter = 0;
        foreach (var se in extend.Expressions)
        {
            if (se.Element is SimpleNamedExpression sne)
            {
                var name = sne.Name.ToString().Trim();
                var quotedName = Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(name);
                var convertedSql = Expr.ConvertExpression(sne.Expression);
                if (LooksLikeIntervalResult(sne.Expression, convertedSql))
                    Expr.MarkIntervalColumn(name);
                extras.Add(($"{convertedSql} AS {quotedName}", name));
            }
            else if (se.Element is CompoundNamedExpression cne)
            {
                // (a, b) = expr — expand each name as indexed access
                var rhs = Expr.ConvertExpression(cne.Expression);
                int idx = 0;
                foreach (var nameNode in cne.Names.Names)
                {
                    var n = nameNode.Element.ToString().Trim();
                    extras.Add(($"{rhs}[{idx + 1}] AS {n}", n));
                    idx++;
                }
            }
            else
            {
                // Bare expression in extend — KQL auto-names:
                //   DataMetadata.Section  → DataMetadata_Section (path join)
                //   NameReference         → unchanged (already a column)
                //   Anything else (CASE/iif/arithmetic) → Column1, Column2, ... (positional)
                var synthesized = SynthesizePathAlias(se.Element);
                var colSql = Expr.ConvertExpression(se.Element);
                if (synthesized != null)
                {
                    extras.Add(($"{colSql} AS {synthesized}", synthesized));
                }
                else if (se.Element is NameReference)
                {
                    extras.Add((colSql, colSql));
                }
                else
                {
                    anonCounter++;
                    var autoName = $"Column{anonCounter}";
                    extras.Add(($"{colSql} AS {autoName}", autoName));
                }
            }
        }

        // KQL extend replaces columns with the same name.
        // Check if any extended column name already exists as an alias at the OUTER level of leftSql
        // (paren depth 0). Inner-scope AS aliases (from deeply nested subqueries) may not be visible
        // to the enclosing FROM, so using EXCLUDE on them would cause 'column not found' errors.
        var columnsToExclude = extras
            .Where(e => HasTopLevelAlias(leftSql, e.Name))
            .Select(e => Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(e.Name))
            .ToArray();

        var joined = string.Join(", ", extras.Select(e => e.Expr));

        if (columnsToExclude.Length > 0)
        {
            var exclude = Dialect.SelectExclude(columnsToExclude);
            var from = ExtractFrom(leftSql);
            return $"SELECT {exclude}, {joined} FROM {from}";
        }

        return AppendToSelectStar(leftSql, joined);
    }

    internal string ApplySort(string leftSql, SortOperator sort)
    {
        var orderings = new List<string>();
        foreach (var se in sort.Expressions)
        {
            if (se.Element is OrderedExpression oe)
            {
                var expr = Expr.ConvertExpression(oe.Expression);
                var dir = oe.Ordering?.ToString().Trim().ToUpperInvariant() ?? "ASC";
                orderings.Add($"{expr} {dir}");
            }
            else
            {
                var expr = Expr.ConvertExpression(se.Element);
                orderings.Add($"{expr} DESC");
            }
        }

        // Wrap in a subquery if:
        //   (a) leftSql has a trailing ORDER BY (e.g. from | top) — prevents invalid double ORDER BY, or
        //   (b) leftSql contains a top-level GROUP BY — appending ORDER BY after GROUP BY can reference
        //       columns not in the aggregation output, causing DuckDB "must appear in GROUP BY" errors.
        if (HasTrailingOrderBy(leftSql) || HasTopLevelGroupBy(leftSql))
            return $"SELECT * FROM ({leftSql}) ORDER BY {string.Join(", ", orderings)}";
        return $"{leftSql} ORDER BY {string.Join(", ", orderings)}";
    }

    /// <summary>Returns true if 'AS name' appears at paren depth 0 in sql (outer SELECT level),
    /// meaning the column is visible from an enclosing FROM.</summary>
    private static bool HasTopLevelAlias(string sql, string name)
    {
        var patterns = new[] { $" AS {name} ", $" AS {name},", $" AS {name}\n", $" AS {name}\r",
                                $" AS \"{name}\" ", $" AS \"{name}\",", $" AS \"{name}\"\n", $" AS \"{name}\"\r" };
        foreach (var pat in patterns)
        {
            int depth = 0;
            bool inStr = false; char q = ' ';
            int start = 0;
            while (start <= sql.Length - pat.Length)
            {
                var c = sql[start];
                if (inStr) { if (c == q) inStr = false; start++; continue; }
                if (c == '\'' || c == '"') { inStr = true; q = c; start++; continue; }
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (depth == 0 && string.Compare(sql, start, pat, 0, pat.Length, StringComparison.OrdinalIgnoreCase) == 0)
                    return true;
                start++;
            }
        }
        // Also match trailing alias (AS name at end of leftSql)
        var endPat = $" AS {name}";
        if (sql.EndsWith(endPat, StringComparison.OrdinalIgnoreCase))
        {
            // Check depth at end
            int d = 0; bool s = false; char qq = ' ';
            foreach (var c in sql)
            {
                if (s) { if (c == qq) s = false; continue; }
                if (c == '\'' || c == '"') { s = true; qq = c; continue; }
                if (c == '(') d++;
                else if (c == ')') d--;
            }
            if (d == 0) return true;
        }
        return false;
    }

    private static bool LooksLikeIntervalResult(Expression sourceExpr, string convertedSql)
    {
        // Interval-producing emission signatures we recognize at the outer level:
        //   '(N * INTERVAL '1 ms')' — from our timespan literal emission
        //   'X - Y' where X,Y are timestamps → INTERVAL (detect via AST)
        //   'datetime_diff(...) * (N * INTERVAL ...)' → INTERVAL
        if (System.Text.RegularExpressions.Regex.IsMatch(convertedSql, @"^\s*\(\s*\d+\s*\*\s*INTERVAL\s+'")) return true;
        if (convertedSql.TrimEnd().EndsWith("millisecond')", StringComparison.OrdinalIgnoreCase)) return true;
        if (sourceExpr is BinaryExpression bin && bin.Kind == SyntaxKind.SubtractExpression)
        {
            bool leftLooksLikeTs = bin.Left is NameReference || (bin.Left is LiteralExpression ll && ll.Kind == SyntaxKind.DateTimeLiteralExpression);
            bool rightLooksLikeTs = bin.Right is NameReference || (bin.Right is LiteralExpression lr && lr.Kind == SyntaxKind.DateTimeLiteralExpression);
            if (leftLooksLikeTs && rightLooksLikeTs) return true;
        }
        return false;
    }

    private static bool HasTrailingOrderBy(string sql)
    {
        // Look for a top-level ORDER BY that is NOT wrapped in a subquery.
        int depth = 0;
        bool inStr = false;
        char quote = ' ';
        int lastOrderBy = -1;
        for (int i = 0; i <= sql.Length - 10; i++)
        {
            var c = sql[i];
            if (inStr) { if (c == quote) inStr = false; continue; }
            if (c == '\'' || c == '"') { inStr = true; quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && string.Compare(sql, i, " ORDER BY ", 0, 10, StringComparison.OrdinalIgnoreCase) == 0)
                lastOrderBy = i;
        }
        return lastOrderBy >= 0;
    }

    private static bool HasTopLevelGroupBy(string sql)
    {
        // Return true if there is a top-level GROUP BY (depth 0) in the SQL.
        int depth = 0;
        bool inStr = false;
        char quote = ' ';
        for (int i = 0; i <= sql.Length - 9; i++)
        {
            var c = sql[i];
            if (inStr) { if (c == quote) inStr = false; continue; }
            if (c == '\'' || c == '"') { inStr = true; quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && string.Compare(sql, i, " GROUP BY ", 0, 10, StringComparison.OrdinalIgnoreCase) == 0)
                return true;
        }
        return false;
    }

    internal string ApplyTake(string leftSql, TakeOperator take)
    {
        var count = Expr.ConvertExpression(take.Expression);
        return $"{leftSql} LIMIT {count}";
    }

    internal string ApplyTop(string leftSql, TopOperator top)
    {
        var count = Expr.ConvertExpression(top.Expression);

        string order;
        if (top.ByExpression is OrderedExpression oe)
        {
            var expr = Expr.ConvertExpression(oe.Expression);
            var dir = oe.Ordering?.ToString().Trim().ToUpperInvariant() ?? "DESC";
            order = $"{expr} {dir}";
        }
        else
        {
            var expr = Expr.ConvertExpression(top.ByExpression);
            order = $"{expr} DESC";
        }

        return $"{leftSql} ORDER BY {order} LIMIT {count}";
    }

    internal string ApplyCount(string leftSql, CountOperator count)
        => ReplaceSelectStar(leftSql, "COUNT(*) AS Count");

    internal string ApplyDistinct(string leftSql, DistinctOperator distinct)
    {
        var cols = string.Join(", ", distinct.Expressions.Select(e =>
        {
            var sql = Expr.ConvertExpression(e.Element);
            var synthesized = SynthesizePathAlias(e.Element);
            return synthesized != null ? $"{sql} AS {synthesized}" : sql;
        }));
        return ReplaceSelectStar(leftSql, $"DISTINCT {cols}");
    }
}
