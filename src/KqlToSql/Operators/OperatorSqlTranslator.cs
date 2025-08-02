using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

internal class OperatorSqlTranslator
{
    private readonly KqlToSqlConverter _converter;

    internal OperatorSqlTranslator(KqlToSqlConverter converter)
    {
        _converter = converter;
    }

    internal string ApplyOperator(string leftSql, QueryOperator op)
    {
        return op switch
        {
            FilterOperator filter => ApplyFilter(leftSql, filter),
            ProjectOperator project => ApplyProject(leftSql, project),
            ProjectAwayOperator projectAway => ApplyProjectAway(leftSql, projectAway),
            ProjectRenameOperator projectRename => ApplyProjectRename(leftSql, projectRename),
            SummarizeOperator summarize => ApplySummarize(leftSql, summarize),
            SortOperator sort => ApplySort(leftSql, sort),
            ExtendOperator extend => ApplyExtend(leftSql, extend),
            TakeOperator take => ApplyTake(leftSql, take),
            TopOperator top => ApplyTop(leftSql, top),
            CountOperator count => ApplyCount(leftSql, count),
            DistinctOperator distinct => ApplyDistinct(leftSql, distinct),
            JoinOperator join => ApplyJoin(leftSql, join),
            _ => throw new NotSupportedException($"Unsupported operator {op.Kind}")
        };
    }

    private string ApplyFilter(string leftSql, FilterOperator filter)
    {
        var condition = ExpressionSqlBuilder.ConvertExpression(filter.Condition);
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase) &&
            !leftSql.Contains(" WHERE ", StringComparison.OrdinalIgnoreCase))
        {
            return $"{leftSql} WHERE {condition}";
        }

        if (leftSql.Contains(" WHERE ", StringComparison.OrdinalIgnoreCase))
        {
            return $"{leftSql} AND {condition}";
        }

        return $"SELECT * FROM ({leftSql}) WHERE {condition}";
    }

    private string ApplyProject(string leftSql, ProjectOperator project)
    {
        var columns = project.Expressions.Select(se => se.Element.ToString().Trim()).ToArray();
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT {string.Join(", ", columns)} FROM {rest}";
        }
        else
        {
            return $"SELECT {string.Join(", ", columns)} FROM ({leftSql})";
        }
    }

    private string ApplyProjectAway(string leftSql, ProjectAwayOperator projectAway)
    {
        var columns = projectAway.Expressions.Select(se => se.Element.ToString().Trim()).ToArray();
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT * EXCLUDE ({string.Join(", ", columns)}) FROM {rest}";
        }
        else
        {
            return $"SELECT * EXCLUDE ({string.Join(", ", columns)}) FROM ({leftSql})";
        }
    }

    private string ApplyProjectRename(string leftSql, ProjectRenameOperator projectRename)
    {
        var mappings = projectRename.Expressions.Select(se =>
        {
            if (se.Element is SimpleNamedExpression sne)
            {
                var expr = ExpressionSqlBuilder.ConvertExpression(sne.Expression);
                var name = sne.Name.ToString().Trim();
                return $"{expr} AS {name}";
            }
            throw new NotSupportedException("Unsupported project-rename expression");
        }).ToArray();

        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT * RENAME ({string.Join(", ", mappings)}) FROM {rest}";
        }
        else
        {
            return $"SELECT * RENAME ({string.Join(", ", mappings)}) FROM ({leftSql})";
        }
    }

    private string ApplySummarize(string leftSql, SummarizeOperator summarize)
    {
        var byColumns = summarize.ByClause?.Expressions
            .Select(e => ConvertByExpression(e.Element))
            .ToArray() ?? Array.Empty<(string Select, string Group)>();

        var aggregates = new List<string>();
        foreach (var agg in summarize.Aggregates)
        {
            aggregates.Add(ConvertAggregate(agg.Element));
        }

        var selectList = string.Join(", ", byColumns.Select(b => b.Select).Concat(aggregates));

        string fromSql;
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            fromSql = leftSql.Substring("SELECT * FROM ".Length);
        }
        else
        {
            fromSql = $"({leftSql})";
        }

        var sql = $"SELECT {selectList} FROM {fromSql}";
        if (byColumns.Length > 0)
        {
            sql += $" GROUP BY {string.Join(", ", byColumns.Select(b => b.Group))}";
        }
        return sql;
    }

    private (string Select, string Group) ConvertByExpression(Expression expr)
    {
        if (expr is SimpleNamedExpression sne)
        {
            var inner = ExpressionSqlBuilder.ConvertExpression(sne.Expression);
            var name = sne.Name.ToString().Trim();
            return ($"{inner} AS {name}", inner);
        }

        if (expr is FunctionCallExpression fce && fce.Name.ToString().Trim().Equals("bin", StringComparison.OrdinalIgnoreCase) &&
            fce.ArgumentList.Expressions.Count > 0 && fce.ArgumentList.Expressions[0].Element is NameReference nr)
        {
            var inner = ExpressionSqlBuilder.ConvertBin(fce, null, null);
            var name = nr.Name.ToString().Trim();
            return ($"{inner} AS {name}", inner);
        }

        var exp = ExpressionSqlBuilder.ConvertExpression(expr);
        return (exp, exp);
    }

    private string ApplySort(string leftSql, SortOperator sort)
    {
        var orderings = new List<string>();
        foreach (var se in sort.Expressions)
        {
            if (se.Element is OrderedExpression oe)
            {
                var expr = ExpressionSqlBuilder.ConvertExpression(oe.Expression);
                var dir = oe.Ordering?.ToString().Trim().ToUpperInvariant() ?? "ASC";
                orderings.Add($"{expr} {dir}");
            }
            else
            {
                var expr = ExpressionSqlBuilder.ConvertExpression(se.Element);
                orderings.Add($"{expr} ASC");
            }
        }

        return $"{leftSql} ORDER BY {string.Join(", ", orderings)}";
    }

    private string ApplyCount(string leftSql, CountOperator count)
    {
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT COUNT(*) AS Count FROM {rest}";
        }
        return $"SELECT COUNT(*) AS Count FROM ({leftSql})";
    }

    private string ApplyDistinct(string leftSql, DistinctOperator distinct)
    {
        var columns = distinct.Expressions.Select(e => ExpressionSqlBuilder.ConvertExpression(e.Element)).ToArray();
        var cols = string.Join(", ", columns);
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT DISTINCT {cols} FROM {rest}";
        }
        else
        {
            return $"SELECT DISTINCT {cols} FROM ({leftSql})";
        }
    }

    private string ApplyExtend(string leftSql, ExtendOperator extend)
    {
        var extras = extend.Expressions.Select(se =>
        {
            if (se.Element is SimpleNamedExpression sne)
            {
                return $"{ExpressionSqlBuilder.ConvertExpression(sne.Expression)} AS {sne.Name.ToString().Trim()}";
            }
            throw new NotSupportedException("Unsupported extend expression");
        }).ToArray();

        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT *, {string.Join(", ", extras)} FROM {rest}";
        }
        return $"SELECT *, {string.Join(", ", extras)} FROM ({leftSql})";
    }

    private string ApplyTake(string leftSql, TakeOperator take)
    {
        var count = ExpressionSqlBuilder.ConvertExpression(take.Expression);
        return $"{leftSql} LIMIT {count}";
    }

    private string ApplyTop(string leftSql, TopOperator top)
    {
        var count = ExpressionSqlBuilder.ConvertExpression(top.Expression);

        string order;
        if (top.ByExpression is OrderedExpression oe)
        {
            var expr = ExpressionSqlBuilder.ConvertExpression(oe.Expression);
            var dir = oe.Ordering?.ToString().Trim().ToUpperInvariant() ?? "DESC";
            order = $"{expr} {dir}";
        }
        else
        {
            var expr = ExpressionSqlBuilder.ConvertExpression(top.ByExpression);
            order = $"{expr} DESC";
        }

        return $"{leftSql} ORDER BY {order} LIMIT {count}";
    }

    private string ApplyJoin(string leftSql, JoinOperator join)
    {
        var kindParam = join.Parameters.FirstOrDefault(p => p.Name.ToString().Trim().Equals("kind", StringComparison.OrdinalIgnoreCase));
        var kind = kindParam?.Expression.ToString().Trim().ToLowerInvariant();
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
                leftKeys.Add(name);
                conditions.Add($"L.{name} = R.{name}");
            }
            else if (expr is BinaryExpression be && be.Kind == SyntaxKind.EqualExpression)
            {
                var left = ExpressionSqlBuilder.ConvertExpression(be.Left, "L", "R");
                var right = ExpressionSqlBuilder.ConvertExpression(be.Right, "L", "R");
                conditions.Add($"{left} = {right}");
                leftKeys.Add(ExpressionSqlBuilder.ExtractLeftKey(be.Left));
            }
            else
            {
                throw new NotSupportedException("Unsupported join condition");
            }
        }

        if (kind is null or "innerunique")
        {
            leftSql = $"SELECT * FROM ({leftSql}) QUALIFY ROW_NUMBER() OVER (PARTITION BY {string.Join(", ", leftKeys)}) = 1";
        }

        var rightSql = _converter.ConvertNode(join.Expression);
        return $"SELECT * FROM ({leftSql}) AS L {joinType} ({rightSql}) AS R ON {string.Join(" AND ", conditions)}";
    }

    private string ConvertAggregate(SyntaxNode node)
    {
        string? alias = null;
        Expression expr;
        if (node is SimpleNamedExpression sne)
        {
            alias = sne.Name.ToString().Trim();
            expr = sne.Expression;
        }
        else if (node is Expression e)
        {
            expr = e;
        }
        else
        {
            throw new NotSupportedException($"Unsupported aggregate node {node.Kind}");
        }

        if (expr is FunctionCallExpression fce)
        {
            var name = fce.Name.ToString().Trim().ToLowerInvariant();
            var args = fce.ArgumentList.Expressions.Select(a => ExpressionSqlBuilder.ConvertExpression(a.Element)).ToArray();
            alias ??= name switch
            {
                "count" => "count",
                _ when args.Length > 0 => $"{name}_{args[0]}",
                _ => name
            };

            var sqlFunc = name switch
            {
                "count" => "COUNT(*)",
                "sum" => $"SUM({args[0]})",
                "avg" => $"AVG({args[0]})",
                "min" => $"MIN({args[0]})",
                "max" => $"MAX({args[0]})",
                _ => throw new NotSupportedException($"Unsupported aggregate function {name}")
            };

            return $"{sqlFunc} AS {alias}";
        }

        throw new NotSupportedException($"Unsupported aggregate expression {expr.Kind}");
    }
}

