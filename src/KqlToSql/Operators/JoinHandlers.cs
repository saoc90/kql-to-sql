using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

internal class JoinHandlers : OperatorHandlerBase
{
    internal JoinHandlers(KqlToSqlConverter converter, ExpressionSqlBuilder expr)
        : base(converter, expr)
    {
    }

    internal string ApplyJoin(string leftSql, JoinOperator join)
    {
        var kindParam = join.Parameters.FirstOrDefault(p => p.Name.ToString().Trim().Equals("kind", StringComparison.OrdinalIgnoreCase));
        var kind = kindParam?.Expression.ToString().Trim().ToLowerInvariant();
        // SEMI/ANTI joins are not a DuckDB join syntax — rewrite via EXISTS / NOT EXISTS.
        if (kind is "leftsemi" or "rightsemi" or "leftanti" or "anti" or "rightanti")
        {
            return ApplyFilterJoin(leftSql, join, kind);
        }
        var joinType = kind switch
        {
            null or "innerunique" => "INNER JOIN",
            "inner" => "INNER JOIN",
            "leftouter" => "LEFT OUTER JOIN",
            "rightouter" => "RIGHT OUTER JOIN",
            "fullouter" => "FULL OUTER JOIN",
            _ => throw new NotSupportedException($"Unsupported join kind {kind}")
        };

        if (join.ConditionClause is not JoinOnClause onClause)
        {
            throw new NotSupportedException("Only join on clause is supported");
        }

        var conditions = new List<string>();
        var leftKeys = new List<string>();
        foreach (var se in onClause.Expressions)
        {
            var expr = se.Element;
            if (expr is NameReference nr)
            {
                var name = nr.Name.ToString().Trim();
                // Strip brackets from ["ColName"] → "ColName" (quoted identifier for DuckDB)
                if (name.StartsWith("[") && name.EndsWith("]"))
                    name = name.Substring(1, name.Length - 2);
                name = ExpressionSqlBuilder.QuoteIdentifierIfReserved(name);
                leftKeys.Add(name);
                conditions.Add($"L.{name} = R.{name}");
            }
            else if (expr is BinaryExpression be && be.Kind == SyntaxKind.EqualExpression)
            {
                var left = Expr.ConvertExpression(be.Left, "L", "R");
                var right = Expr.ConvertExpression(be.Right, "L", "R");
                conditions.Add($"{left} = {right}");
                leftKeys.Add(ExpressionSqlBuilder.QuoteIdentifierIfReserved(ExpressionSqlBuilder.ExtractLeftKey(be.Left)));
            }
            else
            {
                // Try to convert as a generic expression (e.g. $left.X == $right.Y)
                try
                {
                    var convertedSql = Expr.ConvertExpression(expr, "L", "R");
                    conditions.Add(convertedSql);
                    // Best-effort: extract the left key from the expression for innerunique partitioning
                    leftKeys.Add(ExpressionSqlBuilder.QuoteIdentifierIfReserved(ExpressionSqlBuilder.ExtractLeftKey(expr)));
                }
                catch
                {
                    throw new NotSupportedException("Unsupported join condition");
                }
            }
        }

        if (kind is null or "innerunique")
        {
            var partitionBy = string.Join(", ", leftKeys);
            leftSql = Dialect.Qualify(leftSql, $"ROW_NUMBER() OVER (PARTITION BY {partitionBy}) = 1");
        }

        var rightSql = Converter.ConvertNode(join.Expression);

        // KQL join semantics: drop duplicate key columns from the right side.
        // DuckDB supports EXCLUDE syntax natively to avoid duplicate column names
        // (which cause 'ownKeys' proxy errors). PGlite handles duplicates in results.
        var rightColumns = Dialect.SelectExclude(leftKeys.ToArray());
        var selectClause = rightColumns.Contains("/*")
            ? "*"  // Dialect doesn't support EXCLUDE (e.g., PGlite) — fall back to SELECT *
            : $"L.*, R.{rightColumns}";

        return $"SELECT {selectClause} FROM {UnwrapFrom(leftSql)} AS L {joinType} {UnwrapFrom(rightSql)} AS R ON {string.Join(" AND ", conditions)}";
    }

    private string ApplyFilterJoin(string leftSql, JoinOperator join, string kind)
    {
        if (join.ConditionClause is not JoinOnClause onClause)
            throw new NotSupportedException("Only join on clause is supported");

        // Build conditions in terms of "outer" (the side whose rows are kept) and "inner" (the side used
        // only to filter). For SEMI → keep outer rows that have at least one matching inner row;
        // for ANTI → keep outer rows that have none.
        bool isAnti = kind is "leftanti" or "anti" or "rightanti";
        bool swap = kind is "rightsemi" or "rightanti";

        var rightSql = Converter.ConvertNode(join.Expression);

        var conditions = new List<string>();
        foreach (var se in onClause.Expressions)
        {
            var expr = se.Element;
            if (expr is NameReference nr)
            {
                var name = nr.Name.ToString().Trim();
                if (name.StartsWith("[") && name.EndsWith("]")) name = name.Substring(1, name.Length - 2);
                name = ExpressionSqlBuilder.QuoteIdentifierIfReserved(name);
                conditions.Add(swap ? $"R.{name} = L.{name}" : $"L.{name} = R.{name}");
            }
            else if (expr is BinaryExpression be && be.Kind == SyntaxKind.EqualExpression)
            {
                var l = Expr.ConvertExpression(be.Left, "L", "R");
                var r = Expr.ConvertExpression(be.Right, "L", "R");
                conditions.Add($"{l} = {r}");
            }
            else
            {
                conditions.Add(Expr.ConvertExpression(expr, "L", "R"));
            }
        }

        var outerSql = swap ? rightSql : leftSql;
        var innerSql = swap ? leftSql : rightSql;
        var outerAlias = "L";
        var innerAlias = "R";
        var pred = isAnti ? "NOT EXISTS" : "EXISTS";
        var existsClause = $"{pred} (SELECT 1 FROM {UnwrapFrom(innerSql)} AS {innerAlias} WHERE {string.Join(" AND ", conditions)})";
        return $"SELECT {outerAlias}.* FROM {UnwrapFrom(outerSql)} AS {outerAlias} WHERE {existsClause}";
    }

    internal string ApplyLookup(string leftSql, LookupOperator lookup)
    {
        var kindParam = lookup.Parameters.FirstOrDefault(p => p.Name.ToString().Trim().Equals("kind", StringComparison.OrdinalIgnoreCase));
        var kind = kindParam?.Expression.ToString().Trim().ToLowerInvariant();
        var joinType = kind switch
        {
            null or "leftouter" => "LEFT OUTER JOIN",
            "inner" => "INNER JOIN",
            _ => throw new NotSupportedException($"Unsupported lookup kind {kind}")
        };

        if (lookup.LookupClause is not JoinOnClause onClause)
        {
            throw new NotSupportedException("Only lookup on clause is supported");
        }

        var conditions = new List<string>();
        var leftKeys = new List<string>();
        foreach (var se in onClause.Expressions)
        {
            var expr = se.Element;
            if (expr is NameReference nr)
            {
                var name = nr.Name.ToString().Trim();
                // Strip brackets from ["ColName"] → "ColName" (quoted identifier for DuckDB)
                if (name.StartsWith("[") && name.EndsWith("]"))
                    name = name.Substring(1, name.Length - 2);
                name = ExpressionSqlBuilder.QuoteIdentifierIfReserved(name);
                leftKeys.Add(name);
                conditions.Add($"L.{name} = R.{name}");
            }
            else if (expr is BinaryExpression be && be.Kind == SyntaxKind.EqualExpression)
            {
                var left = Expr.ConvertExpression(be.Left, "L", "R");
                var right = Expr.ConvertExpression(be.Right, "L", "R");
                conditions.Add($"{left} = {right}");
                leftKeys.Add(ExpressionSqlBuilder.QuoteIdentifierIfReserved(ExpressionSqlBuilder.ExtractLeftKey(be.Left)));
            }
            else
            {
                throw new NotSupportedException("Unsupported lookup condition");
            }
        }

        var rightSql = Converter.ConvertNode(lookup.Expression);

        var rightColumns = Dialect.SelectExclude(leftKeys.ToArray());
        var selectClause = rightColumns.Contains("/*")
            ? "*"
            : $"L.*, R.{rightColumns}";

        return $"SELECT {selectClause} FROM {UnwrapFrom(leftSql)} AS L {joinType} {UnwrapFrom(rightSql)} AS R ON {string.Join(" AND ", conditions)}";
    }

    internal string ApplyUnion(string leftSql, UnionOperator union, Expression? leftExpression)
    {
        var withSourceParam = union.Parameters.FirstOrDefault(p => p.Name.ToString().Trim().Equals("withsource", StringComparison.OrdinalIgnoreCase));
        string? sourceColumn = withSourceParam != null ? withSourceParam.Expression.ToString().Trim() : null;

        var parts = new List<string>();
        if (sourceColumn != null)
        {
            var name = leftExpression?.ToString().Trim() ?? "";
            parts.Add($"(SELECT *, '{name}' AS {sourceColumn} FROM ({leftSql}))");
        }
        else
        {
            parts.Add($"({leftSql})");
        }

        foreach (var expr in union.Expressions)
        {
            var sql = Converter.ConvertNode(expr.Element);
            if (sourceColumn != null)
            {
                var name = expr.Element.ToString().Trim();
                sql = $"SELECT *, '{name}' AS {sourceColumn} FROM ({sql})";
            }
            parts.Add($"({sql})");
        }

        return string.Join(" UNION ALL ", parts);
    }

    internal string ConvertUnion(UnionOperator union)
    {
        var withSourceParam = union.Parameters.FirstOrDefault(p => p.Name.ToString().Trim().Equals("withsource", StringComparison.OrdinalIgnoreCase));
        string? sourceColumn = withSourceParam != null ? withSourceParam.Expression.ToString().Trim() : null;

        var parts = new List<string>();
        foreach (var expr in union.Expressions)
        {
            var sql = Converter.ConvertNode(expr.Element);
            if (sourceColumn != null)
            {
                var name = expr.Element.ToString().Trim();
                sql = $"SELECT *, '{name}' AS {sourceColumn} FROM ({sql})";
            }
            parts.Add($"({sql})");
        }

        return string.Join(" UNION ALL ", parts);
    }
}
