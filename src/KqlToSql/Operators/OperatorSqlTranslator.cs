using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

internal class OperatorSqlTranslator
{
    private readonly KqlToSqlConverter _converter;
    private readonly ExpressionSqlBuilder _expr;
    private ISqlDialect Dialect => _converter.Dialect;

    internal OperatorSqlTranslator(KqlToSqlConverter converter)
    {
        _converter = converter;
        _expr = new ExpressionSqlBuilder(converter.Dialect);
    }

    internal string ApplyOperator(string leftSql, QueryOperator op, Expression? leftExpression = null)
    {
        return op switch
        {
            FilterOperator filter => ApplyFilter(leftSql, filter),
            ProjectOperator project => ApplyProject(leftSql, project),
            ProjectAwayOperator projectAway => ApplyProjectAway(leftSql, projectAway),
            ProjectRenameOperator projectRename => ApplyProjectRename(leftSql, projectRename),
            ProjectKeepOperator projectKeep => ApplyProjectKeep(leftSql, projectKeep),
            ProjectReorderOperator projectReorder => ApplyProjectReorder(leftSql, projectReorder),
            SummarizeOperator summarize => ApplySummarize(leftSql, summarize),
            SortOperator sort => ApplySort(leftSql, sort),
            ExtendOperator extend => ApplyExtend(leftSql, extend),
            TakeOperator take => ApplyTake(leftSql, take),
            TopOperator top => ApplyTop(leftSql, top),
            CountOperator count => ApplyCount(leftSql, count),
            DistinctOperator distinct => ApplyDistinct(leftSql, distinct),
            JoinOperator join => ApplyJoin(leftSql, join),
            UnionOperator union => ApplyUnion(leftSql, union, leftExpression),
            MvExpandOperator mvExpand => ApplyMvExpand(leftSql, mvExpand),
            _ => throw new NotSupportedException($"Unsupported operator {op.Kind}")
        };
    }

    internal string ConvertRange(RangeOperator range)
    {
        var name = range.Name.Name.ToString().Trim();
        var start = _expr.ConvertExpression(range.From);
        var end = _expr.ConvertExpression(range.To);
        var step = _expr.ConvertExpression(range.Step);
        return Dialect.GenerateSeries(name, start, end, step);
    }

    internal string ConvertUnion(UnionOperator union)
    {
        var withSourceParam = union.Parameters.FirstOrDefault(p => p.Name.ToString().Trim().Equals("withsource", StringComparison.OrdinalIgnoreCase));
        string? sourceColumn = withSourceParam != null ? withSourceParam.Expression.ToString().Trim() : null;

        var parts = new List<string>();
        foreach (var expr in union.Expressions)
        {
            var sql = _converter.ConvertNode(expr.Element);
            if (sourceColumn != null)
            {
                var name = expr.Element.ToString().Trim();
                sql = $"SELECT *, '{name}' AS {sourceColumn} FROM ({sql})";
            }
            parts.Add($"({sql})");
        }

        return string.Join(" UNION ALL ", parts);
    }

    internal string ConvertPrint(PrintOperator print)
    {
        var parts = new List<string>();
        int i = 0;
        foreach (var expr in print.Expressions)
        {
            if (expr.Element is SimpleNamedExpression sne)
            {
                var name = sne.Name.ToString().Trim();
                var value = _expr.ConvertExpression(sne.Expression);
                parts.Add($"{value} AS {name}");
            }
            else
            {
                var value = _expr.ConvertExpression(expr.Element);
                parts.Add($"{value} AS print_{i++}");
            }
        }
        return $"SELECT {string.Join(", ", parts)}";
    }

    internal string ConvertDataTable(DataTableExpression dt)
    {
        var columnNames = new List<string>();
        foreach (var col in dt.Schema.Columns)
        {
            if (col.Element is NameAndTypeDeclaration nat)
            {
                columnNames.Add(nat.Name.ToString().Trim());
            }
        }

        var colCount = columnNames.Count;
        var values = dt.Values;
        var rows = new List<string>();

        for (int i = 0; i < values.Count; i += colCount)
        {
            var rowValues = new List<string>();
            for (int j = 0; j < colCount && (i + j) < values.Count; j++)
            {
                rowValues.Add(_expr.ConvertLiteralValue(values[i + j].Element));
            }
            rows.Add($"({string.Join(", ", rowValues)})");
        }

        return $"SELECT * FROM (VALUES {string.Join(", ", rows)}) AS t({string.Join(", ", columnNames)})";
    }

    private string ApplyUnion(string leftSql, UnionOperator union, Expression? leftExpression)
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
            var sql = _converter.ConvertNode(expr.Element);
            if (sourceColumn != null)
            {
                var name = expr.Element.ToString().Trim();
                sql = $"SELECT *, '{name}' AS {sourceColumn} FROM ({sql})";
            }
            parts.Add($"({sql})");
        }

        return string.Join(" UNION ALL ", parts);
    }

    private string ApplyMvExpand(string leftSql, MvExpandOperator mvExpand)
    {
        if (mvExpand.Expressions.Count != 1)
        {
            throw new NotSupportedException("mv-expand with multiple expressions is not supported");
        }

        if (mvExpand.Expressions[0].Element is not MvExpandExpression mve)
        {
            throw new NotSupportedException("Unexpected mv-expand expression type");
        }

        var column = mve.Expression.ToString().Trim();

        string sourceAlias = "t";
        string fromSql;
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            fromSql = $"{rest} AS {sourceAlias}";
        }
        else
        {
            fromSql = $"({leftSql}) AS {sourceAlias}";
        }

        var unnestAlias = "u";
        var excludeClause = Dialect.SelectExclude(new[] { column });
        var unnestClause = Dialect.Unnest(sourceAlias, column, unnestAlias);
        return $"SELECT {sourceAlias}.{excludeClause}, {unnestAlias}.value AS {column} FROM {fromSql} {unnestClause}";
    }

    private string ApplyFilter(string leftSql, FilterOperator filter)
    {
        var condition = _expr.ConvertExpression(filter.Condition);
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
        var columns = project.Expressions.Select(se =>
        {
            if (se.Element is SimpleNamedExpression sne)
            {
                return $"{_expr.ConvertExpression(sne.Expression)} AS {sne.Name.ToString().Trim()}";
            }
            return _expr.ConvertExpression(se.Element);
        }).ToArray();

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
        var excludeClause = Dialect.SelectExclude(columns);
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT {excludeClause} FROM {rest}";
        }
        else
        {
            return $"SELECT {excludeClause} FROM ({leftSql})";
        }
    }

    private string ApplyProjectRename(string leftSql, ProjectRenameOperator projectRename)
    {
        var mappings = projectRename.Expressions.Select(se =>
        {
            if (se.Element is SimpleNamedExpression sne)
            {
                var expr = _expr.ConvertExpression(sne.Expression);
                var name = sne.Name.ToString().Trim();
                return $"{expr} AS {name}";
            }
            throw new NotSupportedException("Unsupported project-rename expression");
        }).ToArray();

        var renameClause = Dialect.SelectRename(mappings);
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT {renameClause} FROM {rest}";
        }
        else
        {
            return $"SELECT {renameClause} FROM ({leftSql})";
        }
    }

    private string ApplyProjectKeep(string leftSql, ProjectKeepOperator projectKeep)
    {
        var columns = projectKeep.Expressions.Select(se => se.Element.ToString().Trim()).ToArray();
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

    private string ApplyProjectReorder(string leftSql, ProjectReorderOperator projectReorder)
    {
        var columns = projectReorder.Expressions.Select(se => se.Element.ToString().Trim()).ToArray();
        var excludeClause = Dialect.SelectExclude(columns);
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT {string.Join(", ", columns)}, {excludeClause} FROM {rest}";
        }
        else
        {
            return $"SELECT {string.Join(", ", columns)}, {excludeClause} FROM ({leftSql})";
        }
    }

    private string ApplySummarize(string leftSql, SummarizeOperator summarize)
    {
        var byColumns = summarize.ByClause?.Expressions
            .Select(e => ConvertByExpression(e.Element))
            .ToArray() ?? Array.Empty<(string Select, string Group)>();

        if (summarize.Aggregates.Count == 1 &&
            summarize.Aggregates[0].Element is Expression aggExpr &&
            aggExpr is FunctionCallExpression fce &&
            (fce.Name.ToString().Trim().Equals("arg_max", StringComparison.OrdinalIgnoreCase) ||
             fce.Name.ToString().Trim().Equals("arg_min", StringComparison.OrdinalIgnoreCase)) &&
            fce.ArgumentList.Expressions.Count == 2 &&
            fce.ArgumentList.Expressions[1].Element is StarExpression)
        {
            var extremumExpr = _expr.ConvertExpression(fce.ArgumentList.Expressions[0].Element);

            string fromSql;
            if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
            {
                fromSql = leftSql.Substring("SELECT * FROM ".Length);
            }
            else
            {
                fromSql = $"({leftSql})";
            }

            var partition = byColumns.Length > 0
                ? $"PARTITION BY {string.Join(", ", byColumns.Select(b => b.Group))} "
                : string.Empty;

            var direction = fce.Name.ToString().Trim().Equals("arg_min", StringComparison.OrdinalIgnoreCase)
                ? "ASC" : "DESC";

            var qualifyCondition = $"ROW_NUMBER() OVER ({partition}ORDER BY {extremumExpr} {direction}) = 1";
            return $"SELECT * FROM {fromSql} {Dialect.Qualify(qualifyCondition)}";
        }

        var aggregates = new List<string>();
        foreach (var agg in summarize.Aggregates)
        {
            aggregates.AddRange(ConvertAggregate(agg.Element));
        }

        var selectList = string.Join(", ", byColumns.Select(b => b.Select).Concat(aggregates));

        string finalFromSql;
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            finalFromSql = leftSql.Substring("SELECT * FROM ".Length);
        }
        else
        {
            finalFromSql = $"({leftSql})";
        }

        var sql = $"SELECT {selectList} FROM {finalFromSql}";
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
            var inner = _expr.ConvertExpression(sne.Expression);
            var name = sne.Name.ToString().Trim();
            return ($"{inner} AS {name}", inner);
        }

        if (expr is FunctionCallExpression fce && fce.Name.ToString().Trim().Equals("bin", StringComparison.OrdinalIgnoreCase) &&
            fce.ArgumentList.Expressions.Count > 0 && fce.ArgumentList.Expressions[0].Element is NameReference nr)
        {
            var inner = _expr.ConvertBin(fce, null, null);
            var name = nr.Name.ToString().Trim();
            return ($"{inner} AS {name}", inner);
        }

        var exp = _expr.ConvertExpression(expr);
        return (exp, exp);
    }

    private string ApplySort(string leftSql, SortOperator sort)
    {
        var orderings = new List<string>();
        foreach (var se in sort.Expressions)
        {
            if (se.Element is OrderedExpression oe)
            {
                var expr = _expr.ConvertExpression(oe.Expression);
                var dir = oe.Ordering?.ToString().Trim().ToUpperInvariant() ?? "ASC";
                orderings.Add($"{expr} {dir}");
            }
            else
            {
                var expr = _expr.ConvertExpression(se.Element);
                orderings.Add($"{expr} DESC");
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
        var columns = distinct.Expressions.Select(e => _expr.ConvertExpression(e.Element)).ToArray();
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
                return $"{_expr.ConvertExpression(sne.Expression)} AS {sne.Name.ToString().Trim()}";
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
        var count = _expr.ConvertExpression(take.Expression);
        return $"{leftSql} LIMIT {count}";
    }

    private string ApplyTop(string leftSql, TopOperator top)
    {
        var count = _expr.ConvertExpression(top.Expression);

        string order;
        if (top.ByExpression is OrderedExpression oe)
        {
            var expr = _expr.ConvertExpression(oe.Expression);
            var dir = oe.Ordering?.ToString().Trim().ToUpperInvariant() ?? "DESC";
            order = $"{expr} {dir}";
        }
        else
        {
            var expr = _expr.ConvertExpression(top.ByExpression);
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
                var left = _expr.ConvertExpression(be.Left, "L", "R");
                var right = _expr.ConvertExpression(be.Right, "L", "R");
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
            var qualifyCondition = $"ROW_NUMBER() OVER (PARTITION BY {string.Join(", ", leftKeys)}) = 1";
            leftSql = $"SELECT * FROM ({leftSql}) {Dialect.Qualify(qualifyCondition)}";
        }

        var rightSql = _converter.ConvertNode(join.Expression);
        return $"SELECT * FROM ({leftSql}) AS L {joinType} ({rightSql}) AS R ON {string.Join(" AND ", conditions)}";
    }

    private IEnumerable<string> ConvertAggregate(SyntaxNode node)
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
            if (name == "arg_max" || name == "arg_min")
            {
                var extremumExpr = _expr.ConvertExpression(fce.ArgumentList.Expressions[0].Element);
                var results = new List<string>();
                for (int i = 1; i < fce.ArgumentList.Expressions.Count; i++)
                {
                    var argNode = fce.ArgumentList.Expressions[i].Element;
                    var valueExpr = _expr.ConvertExpression(argNode);
                    string resultAlias;
                    if (i == 1 && alias != null)
                    {
                        resultAlias = alias;
                    }
                    else if (argNode is NameReference nr)
                    {
                        resultAlias = nr.Name.ToString().Trim();
                    }
                    else
                    {
                        resultAlias = alias ?? $"{name}_{i}";
                    }
                    results.Add($"{name}({valueExpr}, {extremumExpr}) AS {resultAlias}");
                }
                return results;
            }

            var args = fce.ArgumentList.Expressions.Select(a => _expr.ConvertExpression(a.Element)).ToArray();

            if (name == "percentiles")
            {
                var baseName = alias ?? (fce.ArgumentList.Expressions[0].Element is NameReference nr ? nr.Name.ToString().Trim() : "expr");
                var results = new List<string>();
                for (int i = 1; i < args.Length; i++)
                {
                    var p = args[i];
                    var resultAlias = i == 1 && alias != null ? alias : $"percentiles_{p}_{baseName}";
                    results.Add($"quantile_cont({args[0]}, {p} / 100.0) AS {resultAlias}");
                }
                return results;
            }

            if (name == "percentilesw")
            {
                var baseName = alias ?? (fce.ArgumentList.Expressions[0].Element is NameReference nr ? nr.Name.ToString().Trim() : "expr");
                var results = new List<string>();
                for (int i = 2; i < args.Length; i++)
                {
                    var p = args[i];
                    var resultAlias = i == 2 && alias != null ? alias : $"percentilesw_{p}_{baseName}";
                    results.Add($"quantile_cont({args[0]}, {p} / 100.0) AS {resultAlias}");
                }
                return results;
            }

            alias ??= name switch
            {
                "count" => "count",
                "countif" => "countif",
                "percentile" => $"percentile_{args[1]}_{args[0]}",
                "percentilew" => $"percentilew_{args[2]}_{args[0]}",
                _ when args.Length > 0 => $"{name}_{args[0]}",
                _ => name
            };

            var sqlFunc = Dialect.TryTranslateAggregate(name, args)
                ?? throw new NotSupportedException($"Unsupported aggregate function {name}");

            return new[] { $"{sqlFunc} AS {alias}" };
        }

        throw new NotSupportedException($"Unsupported aggregate expression {expr.Kind}");
    }
}

