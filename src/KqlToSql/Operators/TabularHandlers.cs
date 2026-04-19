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
                return $"{Expr.ConvertExpression(sne.Expression)} AS {sne.Name.ToString().Trim()}";
            return Expr.ConvertExpression(se.Element);
        }).ToArray();

        return ReplaceSelectStar(leftSql, string.Join(", ", columns));
    }

    internal string ApplyProjectAway(string leftSql, ProjectAwayOperator projectAway)
    {
        var columns = projectAway.Expressions.Select(se => se.Element.ToString().Trim()).ToArray();
        return ReplaceSelectStar(leftSql, Dialect.SelectExclude(columns));
    }

    internal string ApplyProjectRename(string leftSql, ProjectRenameOperator projectRename)
    {
        var mappings = projectRename.Expressions.Select(se =>
        {
            if (se.Element is SimpleNamedExpression sne)
                return $"{Expr.ConvertExpression(sne.Expression)} AS {sne.Name.ToString().Trim()}";
            throw new NotSupportedException("Unsupported project-rename expression");
        }).ToArray();

        return ReplaceSelectStar(leftSql, Dialect.SelectRename(mappings));
    }

    internal string ApplyProjectKeep(string leftSql, ProjectKeepOperator projectKeep)
    {
        var columns = projectKeep.Expressions.Select(se => se.Element.ToString().Trim()).ToArray();
        return ReplaceSelectStar(leftSql, string.Join(", ", columns));
    }

    internal string ApplyProjectReorder(string leftSql, ProjectReorderOperator projectReorder)
    {
        var columns = projectReorder.Expressions.Select(se => se.Element.ToString().Trim()).ToArray();
        return ReplaceSelectStar(leftSql, $"{string.Join(", ", columns)}, {Dialect.SelectExclude(columns)}");
    }

    internal string ApplyExtend(string leftSql, ExtendOperator extend)
    {
        var extras = new List<(string Expr, string Name)>();
        foreach (var se in extend.Expressions)
        {
            if (se.Element is SimpleNamedExpression sne)
            {
                var name = sne.Name.ToString().Trim();
                var quotedName = Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(name);
                extras.Add(($"{Expr.ConvertExpression(sne.Expression)} AS {quotedName}", name));
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
                // Bare column reference (identity extend) — just pass through, no-op
                var colName = Expr.ConvertExpression(se.Element);
                extras.Add((colName, colName));
            }
        }

        // KQL extend replaces columns with the same name.
        // Check if any extended column name already exists in the left SQL (from a prior extend).
        var columnsToExclude = extras
            .Where(e => leftSql.Contains($" AS {e.Name}", StringComparison.OrdinalIgnoreCase)
                     || leftSql.Contains($" AS {e.Name},", StringComparison.OrdinalIgnoreCase))
            .Select(e => e.Name)
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

        return $"{leftSql} ORDER BY {string.Join(", ", orderings)}";
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
        var cols = string.Join(", ", distinct.Expressions.Select(e => Expr.ConvertExpression(e.Element)));
        return ReplaceSelectStar(leftSql, $"DISTINCT {cols}");
    }
}
