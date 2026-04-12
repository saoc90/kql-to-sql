using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
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

    private static string UnwrapFromSql(string sql)
    {
        if (sql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = sql.Substring("SELECT * FROM ".Length);
            if (!rest.Contains(' '))
                return rest;
        }
        return $"({sql})";
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
            AsOperator => leftSql,
            SampleOperator sample => ApplySample(leftSql, sample),
            SampleDistinctOperator sampleDistinct => ApplySampleDistinct(leftSql, sampleDistinct),
            SerializeOperator serialize => ApplySerialize(leftSql, serialize),
            LookupOperator lookup => ApplyLookup(leftSql, lookup),
            ParseOperator parse => ApplyParse(leftSql, parse),
            ParseWhereOperator parseWhere => ApplyParseWhere(leftSql, parseWhere),
            TopHittersOperator topHitters => ApplyTopHitters(leftSql, topHitters),
            TopNestedOperator topNested => ApplyTopNested(leftSql, topNested),
            ConsumeOperator => leftSql,
            RenderOperator => leftSql,
            EvaluateOperator evaluate => ApplyEvaluate(leftSql, evaluate),
            MakeSeriesOperator makeSeries => ApplyMakeSeries(leftSql, makeSeries),
            MvApplyOperator mvApply => ApplyMvApply(leftSql, mvApply),
            ParseKvOperator parseKv => ApplyParseKv(leftSql, parseKv),
            GetSchemaOperator => ApplyGetSchema(leftSql),
            SearchOperator search => ApplySearch(leftSql, search),
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

        // Only append AND if WHERE is the last clause (no ORDER BY, GROUP BY, LIMIT after it)
        var whereIdx = leftSql.LastIndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
        if (whereIdx >= 0)
        {
            var afterWhere = leftSql[whereIdx..];
            if (!afterWhere.Contains(" ORDER BY ", StringComparison.OrdinalIgnoreCase) &&
                !afterWhere.Contains(" GROUP BY ", StringComparison.OrdinalIgnoreCase) &&
                !afterWhere.Contains(" HAVING ", StringComparison.OrdinalIgnoreCase) &&
                !afterWhere.Contains(" LIMIT ", StringComparison.OrdinalIgnoreCase))
            {
                return $"{leftSql} AND {condition}";
            }
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
            // fromSql may already be wrapped like "(subquery)" or just a table name
            var innerSql = fromSql.StartsWith("(") ? fromSql.Substring(1, fromSql.Length - 2) : $"SELECT * FROM {fromSql}";
            return Dialect.Qualify(innerSql, qualifyCondition);
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
            sql += Dialect.SupportsGroupByAll
                ? " GROUP BY ALL"
                : $" GROUP BY {string.Join(", ", byColumns.Select(b => b.Group))}";
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
            var partitionBy = string.Join(", ", leftKeys);
            leftSql = Dialect.Qualify(leftSql, $"ROW_NUMBER() OVER (PARTITION BY {partitionBy}) = 1");
        }

        var rightSql = _converter.ConvertNode(join.Expression);

        // KQL join semantics: drop duplicate key columns from the right side.
        // DuckDB supports EXCLUDE syntax natively to avoid duplicate column names
        // (which cause 'ownKeys' proxy errors). PGlite handles duplicates in results.
        var rightColumns = Dialect.SelectExclude(leftKeys.ToArray());
        var selectClause = rightColumns.Contains("/*")
            ? "*"  // Dialect doesn't support EXCLUDE (e.g., PGlite) — fall back to SELECT *
            : $"L.*, R.{rightColumns}";

        return $"SELECT {selectClause} FROM {UnwrapFromSql(leftSql)} AS L {joinType} {UnwrapFromSql(rightSql)} AS R ON {string.Join(" AND ", conditions)}";
    }

    private string ApplySample(string leftSql, SampleOperator sample)
    {
        var count = _expr.ConvertExpression(sample.Expression);

        // Use dialect-specific USING SAMPLE if available (DuckDB), otherwise ORDER BY RANDOM()
        string fromSql;
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
            fromSql = leftSql.Substring("SELECT * FROM ".Length);
        else
            fromSql = $"({leftSql})";

        var dialectSample = Dialect.SampleClause(fromSql, count);
        if (dialectSample != null)
            return dialectSample;

        return $"{leftSql} ORDER BY RANDOM() LIMIT {count}";
    }

    private string ApplySampleDistinct(string leftSql, SampleDistinctOperator sampleDistinct)
    {
        var count = _expr.ConvertExpression(sampleDistinct.Expression);
        var column = _expr.ConvertExpression(sampleDistinct.OfExpression);
        return $"SELECT DISTINCT {column} FROM ({leftSql}) ORDER BY RANDOM() LIMIT {count}";
    }

    private string ApplySerialize(string leftSql, SerializeOperator serialize)
    {
        if (serialize.Expressions.Count == 0)
        {
            return leftSql;
        }

        var extras = serialize.Expressions.Select(se =>
        {
            if (se.Element is SimpleNamedExpression sne)
            {
                return $"{_expr.ConvertExpression(sne.Expression)} AS {sne.Name.ToString().Trim()}";
            }
            return _expr.ConvertExpression(se.Element);
        }).ToArray();

        // When leftSql has ORDER BY + LIMIT, wrap in subquery first so window functions
        // only run on the limited result set, not the entire table.
        if (leftSql.Contains(" LIMIT ", StringComparison.OrdinalIgnoreCase))
        {
            return $"SELECT *, {string.Join(", ", extras)} FROM ({leftSql})";
        }

        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT *, {string.Join(", ", extras)} FROM {rest}";
        }
        return $"SELECT *, {string.Join(", ", extras)} FROM ({leftSql})";
    }

    private string ApplyLookup(string leftSql, LookupOperator lookup)
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
                throw new NotSupportedException("Unsupported lookup condition");
            }
        }

        var rightSql = _converter.ConvertNode(lookup.Expression);

        var rightColumns = Dialect.SelectExclude(leftKeys.ToArray());
        var selectClause = rightColumns.Contains("/*")
            ? "*"
            : $"L.*, R.{rightColumns}";

        return $"SELECT {selectClause} FROM {UnwrapFromSql(leftSql)} AS L {joinType} {UnwrapFromSql(rightSql)} AS R ON {string.Join(" AND ", conditions)}";
    }

    private string ApplyParse(string leftSql, ParseOperator parse)
    {
        var sourceExpr = _expr.ConvertExpression(parse.Expression);

        var regexParts = new List<string>();
        var captures = new List<(string Name, string? Type)>();

        for (int i = 0; i < parse.Patterns.Count; i++)
        {
            var pattern = parse.Patterns[i];
            if (pattern is StarExpression)
            {
                regexParts.Add(".*?");
            }
            else if (pattern is LiteralExpression lit)
            {
                var text = lit.ToString().Trim().Trim('\'', '"');
                if (text.StartsWith("@"))
                {
                    text = text.TrimStart('@').Trim('\'', '"');
                    regexParts.Add(text);
                }
                else
                {
                    regexParts.Add(Regex.Escape(text));
                }
            }
            else if (pattern is NameAndTypeDeclaration nat)
            {
                var name = nat.Name.ToString().Trim();
                var type = nat.Type?.ToString().Trim().TrimStart(':').ToLowerInvariant();
                captures.Add((name, type));
                regexParts.Add(GetCaptureRegex(type, i, parse.Patterns.Count));
            }
            else if (pattern is NameDeclaration nd)
            {
                var name = nd.Name.ToString().Trim();
                captures.Add((name, null));
                bool isLastCapture = IsLastCapture(i, parse.Patterns);
                regexParts.Add(isLastCapture ? "(.*)" : "(.*?)");
            }
        }

        // Ensure trailing .* if the pattern doesn't end with star
        if (parse.Patterns.Count > 0 && parse.Patterns[^1] is not StarExpression)
        {
            // The last capture is already greedy via (.*), no trailing needed
        }

        var regex = string.Join("", regexParts).Replace("'", "''");

        var columns = captures.Select((c, idx) =>
        {
            var extract = $"REGEXP_EXTRACT({sourceExpr}, '{regex}', {idx + 1})";
            if (c.Type != null && c.Type != "string")
            {
                var sqlType = Dialect.MapType(c.Type);
                extract = $"CAST({extract} AS {sqlType})";
            }
            return $"{extract} AS {c.Name}";
        }).ToArray();

        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT *, {string.Join(", ", columns)} FROM {rest}";
        }
        return $"SELECT *, {string.Join(", ", columns)} FROM ({leftSql})";
    }

    private static string GetCaptureRegex(string? type, int index, int totalPatterns)
    {
        return type switch
        {
            "long" or "int" => @"(-?\d+)",
            "real" or "double" or "decimal" => @"(-?\d+\.?\d*)",
            "bool" or "boolean" => @"(true|false)",
            _ => "(.*?)"
        };
    }

    private static bool IsLastCapture(int index, SyntaxList<SyntaxNode> patterns)
    {
        for (int j = index + 1; j < patterns.Count; j++)
        {
            if (patterns[j] is NameDeclaration or NameAndTypeDeclaration)
                return false;
            if (patterns[j] is LiteralExpression)
                return false;
        }
        return true;
    }

    private string ApplyParseWhere(string leftSql, ParseWhereOperator parseWhere)
    {
        var sourceExpr = _expr.ConvertExpression(parseWhere.Expression);

        var regexParts = new List<string>();
        var captures = new List<(string Name, string? Type)>();

        for (int i = 0; i < parseWhere.Patterns.Count; i++)
        {
            var pattern = parseWhere.Patterns[i];
            if (pattern is StarExpression)
            {
                regexParts.Add(".*?");
            }
            else if (pattern is LiteralExpression lit)
            {
                var text = lit.ToString().Trim().Trim('\'', '"');
                if (text.StartsWith("@"))
                {
                    text = text.TrimStart('@').Trim('\'', '"');
                    regexParts.Add(text);
                }
                else
                {
                    regexParts.Add(Regex.Escape(text));
                }
            }
            else if (pattern is NameAndTypeDeclaration nat)
            {
                var name = nat.Name.ToString().Trim();
                var type = nat.Type?.ToString().Trim().TrimStart(':').ToLowerInvariant();
                captures.Add((name, type));
                regexParts.Add(GetCaptureRegex(type, i, parseWhere.Patterns.Count));
            }
            else if (pattern is NameDeclaration nd)
            {
                var name = nd.Name.ToString().Trim();
                captures.Add((name, null));
                bool isLastCapture = IsLastCapture(i, parseWhere.Patterns);
                regexParts.Add(isLastCapture ? "(.*)" : "(.*?)");
            }
        }

        var regex = string.Join("", regexParts).Replace("'", "''");

        var columns = captures.Select((c, idx) =>
        {
            var rawExtract = $"REGEXP_EXTRACT({sourceExpr}, '{regex}', {idx + 1})";
            var selectExpr = rawExtract;
            if (c.Type != null && c.Type != "string")
            {
                var sqlType = Dialect.MapType(c.Type);
                selectExpr = $"CAST({rawExtract} AS {sqlType})";
            }
            return (SelectExpr: selectExpr, RawExtract: rawExtract, Name: c.Name);
        }).ToArray();

        var selectExprs = columns.Select(c => $"{c.SelectExpr} AS {c.Name}").ToArray();
        var filterConditions = columns.Select(c => $"{c.RawExtract} IS NOT NULL AND {c.RawExtract} <> ''").ToArray();

        string fromSql;
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            fromSql = leftSql.Substring("SELECT * FROM ".Length);
        }
        else
        {
            fromSql = $"({leftSql})";
        }

        return $"SELECT *, {string.Join(", ", selectExprs)} FROM {fromSql} WHERE {string.Join(" AND ", filterConditions)}";
    }

    private string ApplyTopHitters(string leftSql, TopHittersOperator topHitters)
    {
        var count = _expr.ConvertExpression(topHitters.Expression);
        var column = _expr.ConvertExpression(topHitters.OfExpression);

        string fromSql;
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            fromSql = leftSql.Substring("SELECT * FROM ".Length);
        }
        else
        {
            fromSql = $"({leftSql})";
        }

        // top-hitters optionally has a by clause for weighting
        if (topHitters.ByClause is TopHittersByClause byClause)
        {
            string byExpr;
            if (byClause.Expression is FunctionCallExpression bfce)
            {
                var name = bfce.Name.ToString().Trim().ToLowerInvariant();
                var bArgs = bfce.ArgumentList.Expressions
                    .Select(a => _expr.ConvertExpression(a.Element)).ToArray();
                byExpr = Dialect.TryTranslateAggregate(name, bArgs)
                    ?? $"{name}({string.Join(", ", bArgs)})";
            }
            else
            {
                byExpr = _expr.ConvertExpression(byClause.Expression);
            }
            return $"SELECT {column}, {byExpr} AS approximate_count FROM {fromSql} GROUP BY {column} ORDER BY approximate_count DESC LIMIT {count}";
        }

        return $"SELECT {column}, COUNT(*) AS approximate_count FROM {fromSql} GROUP BY {column} ORDER BY approximate_count DESC LIMIT {count}";
    }

    private string ApplyTopNested(string leftSql, TopNestedOperator topNested)
    {
        // top-nested produces nested top-N results.
        // Strategy: for each level, GROUP BY all accumulated columns from the original source,
        // rank within the parent group, then filter to top-N. Each level joins back to the
        // original source to access the new column.
        string originalFrom;
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            originalFrom = leftSql.Substring("SELECT * FROM ".Length);
        }
        else
        {
            originalFrom = $"({leftSql})";
        }

        var selectColumns = new List<string>();
        var prevGroupCols = new List<string>();
        string? prevFilteredSql = null;

        for (int i = 0; i < topNested.Clauses.Count; i++)
        {
            if (topNested.Clauses[i].Element is not TopNestedClause clause)
                continue;

            var count = _expr.ConvertExpression(clause.Expression);
            var column = _expr.ConvertExpression(clause.OfExpression);
            var aggExpr = clause.ByExpression;

            string aggSql;
            if (aggExpr is FunctionCallExpression fce)
            {
                var name = fce.Name.ToString().Trim().ToLowerInvariant();
                var args = fce.ArgumentList.Expressions
                    .Select(a => _expr.ConvertExpression(a.Element)).ToArray();
                aggSql = Dialect.TryTranslateAggregate(name, args)
                    ?? $"{name}({string.Join(", ", args)})";
            }
            else
            {
                aggSql = _expr.ConvertExpression(aggExpr);
            }

            var aggAlias = $"aggregated_{column}";
            selectColumns.Add(column);
            selectColumns.Add(aggAlias);

            var groupBy = new List<string>(prevGroupCols) { column };
            var partitionBy = prevGroupCols.Count > 0
                ? $"PARTITION BY {string.Join(", ", prevGroupCols)} "
                : "";

            // For level 0, aggregate from original source. For level 1+,
            // join previous filtered result back to original source to access the new column.
            string sourceSql;
            if (prevFilteredSql == null)
            {
                sourceSql = originalFrom;
            }
            else
            {
                var joinCond = string.Join(" AND ", prevGroupCols.Select(c => $"_src.{c} = _prev.{c}"));
                sourceSql = $"(SELECT * FROM {originalFrom}) AS _src INNER JOIN ({prevFilteredSql}) AS _prev ON {joinCond}";
            }

            // Build the inner select: group columns + new aggregate.
            // For level 1+, also carry forward previous aggregation columns from _prev.
            var groupColExprs = groupBy.Select(c => prevFilteredSql != null ? $"_src.{c}" : c).ToList();
            var prevAggCols = new List<string>();
            if (prevFilteredSql != null)
            {
                // Carry forward aggregation columns from previous levels
                for (int p = 0; p < i; p++)
                {
                    if (topNested.Clauses[p].Element is TopNestedClause prevClause)
                    {
                        var prevAggAlias = $"aggregated_{_expr.ConvertExpression(prevClause.OfExpression)}";
                        prevAggCols.Add($"_prev.{prevAggAlias}");
                    }
                }
            }
            var allSelectCols = groupColExprs.Concat(prevAggCols).Append($"{aggSql} AS {aggAlias}");
            var innerSelect = $"SELECT {string.Join(", ", allSelectCols)} FROM {sourceSql} GROUP BY {string.Join(", ", groupColExprs.Concat(prevAggCols))}";
            var ranked = $"SELECT *, ROW_NUMBER() OVER ({partitionBy}ORDER BY {aggAlias} DESC) AS _rn{i} FROM ({innerSelect})";
            prevFilteredSql = $"SELECT {string.Join(", ", selectColumns)} FROM ({ranked}) WHERE _rn{i} <= {count}";

            prevGroupCols.Add(column);
        }

        return prevFilteredSql ?? leftSql;
    }

    private string ApplyEvaluate(string leftSql, EvaluateOperator evaluate)
    {
        if (evaluate.FunctionCall is not FunctionCallExpression fce)
            throw new NotSupportedException("Unsupported evaluate expression");

        var pluginName = fce.Name.ToString().Trim().ToLowerInvariant();

        return pluginName switch
        {
            "pivot" => ApplyEvaluatePivot(leftSql, fce),
            "narrow" => ApplyEvaluateNarrow(leftSql),
            "bag_unpack" => ApplyEvaluateBagUnpack(leftSql, fce),
            _ => throw new NotSupportedException($"Unsupported evaluate plugin '{pluginName}'")
        };
    }

    private string ApplyEvaluatePivot(string leftSql, FunctionCallExpression fce)
    {
        // pivot(PivotColumn, AggFunction [, ColumnToPivot...])
        // DuckDB: PIVOT source ON pivotCol USING agg(valCol) [GROUP BY groupCols]
        var args = fce.ArgumentList.Expressions;
        if (args.Count < 2)
            throw new NotSupportedException("pivot() requires at least 2 arguments");

        var pivotCol = _expr.ConvertExpression(args[0].Element);

        // The aggregate function
        string aggSql;
        if (args[1].Element is FunctionCallExpression aggFce)
        {
            var name = aggFce.Name.ToString().Trim().ToLowerInvariant();
            var aggArgs = aggFce.ArgumentList.Expressions
                .Select(a => _expr.ConvertExpression(a.Element)).ToArray();
            aggSql = Dialect.TryTranslateAggregate(name, aggArgs)
                ?? $"{name}({string.Join(", ", aggArgs)})";
        }
        else
        {
            aggSql = _expr.ConvertExpression(args[1].Element);
        }

        // Optional: specific columns to pivot on (args 2+)
        var groupByCols = new List<string>();
        for (int i = 2; i < args.Count; i++)
        {
            groupByCols.Add(_expr.ConvertExpression(args[i].Element));
        }

        var groupByClause = groupByCols.Count > 0
            ? $" GROUP BY {string.Join(", ", groupByCols)}"
            : "";

        return $"PIVOT ({leftSql}) ON {pivotCol} USING {aggSql}{groupByClause}";
    }

    private string ApplyEvaluateNarrow(string leftSql)
    {
        // narrow() converts wide table to narrow (key-value) format
        // DuckDB: UNPIVOT source ON COLUMNS(*) INTO NAME "Column" VALUE "Value"
        return $"UNPIVOT ({leftSql}) ON COLUMNS(*) INTO NAME \"Column\" VALUE \"Value\"";
    }

    private string ApplyEvaluateBagUnpack(string leftSql, FunctionCallExpression fce)
    {
        // bag_unpack(column) unpacks a JSON/dynamic column into separate columns
        // In DuckDB, we can use json_each or struct unpacking
        if (fce.ArgumentList.Expressions.Count < 1)
            throw new NotSupportedException("bag_unpack() requires at least 1 argument");

        var column = _expr.ConvertExpression(fce.ArgumentList.Expressions[0].Element);

        string fromSql;
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            fromSql = leftSql.Substring("SELECT * FROM ".Length);
        }
        else
        {
            fromSql = $"({leftSql})";
        }

        var excludeClause = Dialect.SelectExclude(new[] { column });
        return $"SELECT {excludeClause}, UNNEST(from_json({column}, '{{}}')) FROM {fromSql}";
    }

    private string ApplyMakeSeries(string leftSql, MakeSeriesOperator makeSeries)
    {
        // make-series produces time-bucketed aggregation with filled gaps.
        // Strategy: generate_series for the time axis, CROSS JOIN with group-by keys,
        // LEFT JOIN with actual data aggregated into buckets.

        var onExpr = _expr.ConvertExpression(makeSeries.OnClause.Expression);

        // Parse range clause — supports both "from...to...step" and "in range(from, to, step)"
        string fromExpr, toExpr, stepExpr;
        if (makeSeries.RangeClause is MakeSeriesFromToStepClause fromToStep)
        {
            fromExpr = _expr.ConvertExpression(fromToStep.MakeSeriesFromClause.Expression);
            toExpr = _expr.ConvertExpression(fromToStep.MakeSeriesToClause.Expression);
            stepExpr = fromToStep.MakeSeriesStepClause.Expression.ToString().Trim();
        }
        else if (makeSeries.RangeClause is MakeSeriesInRangeClause inRange)
        {
            var rangeArgs = inRange.Arguments.Expressions;
            if (rangeArgs.Count < 3)
                throw new NotSupportedException("make-series in range() requires 3 arguments");
            fromExpr = _expr.ConvertExpression(rangeArgs[0].Element);
            toExpr = _expr.ConvertExpression(rangeArgs[1].Element);
            stepExpr = rangeArgs[2].Element.ToString().Trim();
        }
        else
        {
            throw new NotSupportedException($"Unsupported make-series range clause: {makeSeries.RangeClause.GetType().Name}");
        }

        // Parse step as interval
        string stepInterval;
        if (ExpressionSqlBuilder.TryParseTimespan(stepExpr, out var stepMs))
        {
            stepInterval = $"{stepMs} * INTERVAL '1 millisecond'";
        }
        else
        {
            stepInterval = $"INTERVAL '{stepExpr}'";
        }

        string fromSql;
        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
            fromSql = leftSql.Substring("SELECT * FROM ".Length);
        else
            fromSql = $"({leftSql})";

        // Build aggregate expressions
        var aggParts = new List<(string SelectExpr, string Alias, string? Default)>();
        foreach (var agg in makeSeries.Aggregates)
        {
            if (agg.Element is not MakeSeriesExpression mse) continue;

            string alias;
            string aggSql;
            if (mse.Expression is SimpleNamedExpression named)
            {
                alias = named.Name.ToString().Trim();
                if (named.Expression is FunctionCallExpression fce)
                {
                    var fname = fce.Name.ToString().Trim().ToLowerInvariant();
                    var fargs = fce.ArgumentList.Expressions.Select(a => _expr.ConvertExpression(a.Element)).ToArray();
                    aggSql = Dialect.TryTranslateAggregate(fname, fargs) ?? $"{fname}({string.Join(", ", fargs)})";
                }
                else
                {
                    aggSql = _expr.ConvertExpression(named.Expression);
                }
            }
            else if (mse.Expression is FunctionCallExpression directFce)
            {
                var fname = directFce.Name.ToString().Trim().ToLowerInvariant();
                var fargs = directFce.ArgumentList.Expressions.Select(a => _expr.ConvertExpression(a.Element)).ToArray();
                aggSql = Dialect.TryTranslateAggregate(fname, fargs) ?? $"{fname}({string.Join(", ", fargs)})";
                alias = fargs.Length > 0 ? $"{fname}_{fargs[0]}" : fname;
            }
            else
            {
                alias = "series_agg";
                aggSql = _expr.ConvertExpression(mse.Expression);
            }

            string? defaultVal = null;
            if (mse.DefaultExpression is DefaultExpressionClause dec)
            {
                defaultVal = _expr.ConvertExpression(dec.Expression);
            }

            aggParts.Add((aggSql, alias, defaultVal));
        }

        // Group-by columns
        var byColumns = new List<string>();
        if (makeSeries.ByClause is MakeSeriesByClause byClause)
        {
            byColumns.AddRange(byClause.Expressions.Select(e => _expr.ConvertExpression(e.Element)));
        }

        // Build: generate time series, cross join with by-columns, left join with aggregated data
        var timeSeries = $"SELECT generate_series AS _ts FROM generate_series({fromExpr}, {toExpr}, {stepInterval})";

        var aggSelectList = string.Join(", ", aggParts.Select(a =>
            a.Default != null
                ? $"LIST(COALESCE({a.Alias}_val, {a.Default}) ORDER BY _ts) AS {a.Alias}"
                : $"LIST({a.Alias}_val ORDER BY _ts) AS {a.Alias}"));

        var dataAggSelect = string.Join(", ", aggParts.Select(a => $"{a.SelectExpr} AS {a.Alias}_val"));
        var bucketExpr = $"TO_TIMESTAMP_MS(FLOOR(EPOCH_MS({onExpr})/{stepMs})*{stepMs})";

        var groupByData = byColumns.Count > 0
            ? $"{string.Join(", ", byColumns)}, "
            : "";

        var dataAgg = $"SELECT {groupByData}{bucketExpr} AS _bucket, {dataAggSelect} FROM {fromSql} GROUP BY {groupByData}_bucket";

        if (byColumns.Count > 0)
        {
            var bySelect = string.Join(", ", byColumns);
            var crossJoin = $"SELECT DISTINCT {bySelect} FROM {fromSql}";
            var tsAxis = $"SELECT _g.*, _t._ts FROM ({crossJoin}) AS _g CROSS JOIN ({timeSeries}) AS _t";
            var joinCond = string.Join(" AND ", byColumns.Select(c => $"_axis.{c} = _data.{c}").Append("_axis._ts = _data._bucket"));
            return $"SELECT {string.Join(", ", byColumns.Select(c => $"_axis.{c}"))}, LIST(_axis._ts ORDER BY _axis._ts) AS {onExpr}, {aggSelectList.Replace("_val", "_val")} FROM ({tsAxis}) AS _axis LEFT JOIN ({dataAgg}) AS _data ON {joinCond} GROUP BY {string.Join(", ", byColumns.Select(c => $"_axis.{c}"))}";
        }
        else
        {
            var joinCond = "_axis._ts = _data._bucket";
            return $"SELECT LIST(_axis._ts ORDER BY _axis._ts) AS {onExpr}, {aggSelectList} FROM ({timeSeries}) AS _axis LEFT JOIN ({dataAgg}) AS _data ON {joinCond}";
        }
    }

    private string ApplyMvApply(string leftSql, MvApplyOperator mvApply)
    {
        // mv-apply unnests a column, applies a sub-pipeline, and returns the result per row.
        if (mvApply.Expressions.Count != 1)
            throw new NotSupportedException("mv-apply with multiple expressions is not supported");

        if (mvApply.Expressions[0].Element is not MvApplyExpression mae)
            throw new NotSupportedException("Unexpected mv-apply expression type");

        var column = mae.Expression.ToString().Trim();

        // Convert the subquery (the inner pipeline applied to each unnested row)
        var subquerySql = _converter.ConvertNode(mvApply.Subquery.Expression);

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
        var unnestClause = Dialect.Unnest(sourceAlias, column, unnestAlias);

        // Replace the column reference in the subquery with the unnested value
        var modifiedSubquery = subquerySql
            .Replace($"SELECT * FROM {column}", $"SELECT {unnestAlias}.value AS {column}");

        // For summarize subqueries, wrap: unnest each row, apply subquery per group
        return $"SELECT {sourceAlias}.*, _sub.* FROM {fromSql} {unnestClause}, LATERAL ({subquerySql.Replace($"FROM {column}", $"FROM (SELECT {unnestAlias}.value AS {column})")}) AS _sub";
    }

    private string ApplyParseKv(string leftSql, ParseKvOperator parseKv)
    {
        var sourceExpr = _expr.ConvertExpression(parseKv.Expression);

        var columns = new List<(string Name, string Type)>();
        if (parseKv.Keys is RowSchema schema)
        {
            foreach (var col in schema.Columns)
            {
                if (col.Element is NameAndTypeDeclaration nat)
                {
                    var name = nat.Name.ToString().Trim();
                    var type = nat.Type?.ToString().Trim().TrimStart(':').ToLowerInvariant() ?? "string";
                    columns.Add((name, type));
                }
            }
        }

        // For each key, use REGEXP_EXTRACT to find key=value pairs
        var extractExprs = columns.Select(c =>
        {
            var extract = $"REGEXP_EXTRACT({sourceExpr}, '{c.Name}=([^,;\\s]+)', 1)";
            if (c.Type != "string")
            {
                var sqlType = Dialect.MapType(c.Type);
                extract = $"TRY_CAST({extract} AS {sqlType})";
            }
            return $"{extract} AS {c.Name}";
        }).ToArray();

        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            var rest = leftSql.Substring("SELECT * FROM ".Length);
            return $"SELECT *, {string.Join(", ", extractExprs)} FROM {rest}";
        }
        return $"SELECT *, {string.Join(", ", extractExprs)} FROM ({leftSql})";
    }

    internal string ConvertExternalData(ExternalDataExpression ed)
    {
        // Extract URI from the expression
        var uriText = ed.URIs.ToString().Trim().Trim('[', ']').Trim();
        var uri = uriText.Trim('\'', '"');

        // Determine format from URI extension or default to CSV
        if (uri.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
        {
            return $"SELECT * FROM read_parquet('{uri}')";
        }
        if (uri.EndsWith(".json", StringComparison.OrdinalIgnoreCase) || uri.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase))
        {
            return $"SELECT * FROM read_json_auto('{uri}')";
        }
        return $"SELECT * FROM read_csv_auto('{uri}')";
    }

    private string ApplyGetSchema(string leftSql)
    {
        // DuckDB doesn't support DESCRIBE on subqueries directly.
        // Use a LIMIT 0 query to get column info via DESCRIBE.
        return $"DESCRIBE SELECT * FROM ({leftSql}) LIMIT 0";
    }

    private string ApplySearch(string leftSql, SearchOperator search)
    {
        // search converts its condition to a WHERE clause.
        // For string literals, it applies 'has' semantics (case-insensitive contains).
        // For column:value, it applies to specific column.
        var condition = ConvertSearchCondition(search.Condition);

        if (leftSql.StartsWith("SELECT * FROM ", StringComparison.OrdinalIgnoreCase))
        {
            return $"{leftSql} WHERE {condition}";
        }
        return $"SELECT * FROM ({leftSql}) WHERE {condition}";
    }

    private string ConvertSearchCondition(Expression condition)
    {
        return condition switch
        {
            LiteralExpression lit when lit.Kind == SyntaxKind.StringLiteralExpression =>
                // Unqualified string → search all VARCHAR columns using CAST(row) trick
                // In practice, convert to: CAST(ROW TO VARCHAR) ILIKE '%term%'
                // Simpler: just use the term as a WHERE condition on concatenated columns
                $"CAST(({_expr.ConvertExpression(lit)}) AS VARCHAR) IS NOT NULL",
            BinaryExpression bin when bin.Kind == SyntaxKind.HasExpression =>
                $"{_expr.ConvertExpression(bin.Left)} {Dialect.CaseInsensitiveLike} '%' || {_expr.ConvertExpression(bin.Right)} || '%'",
            BinaryExpression bin when bin.Kind == SyntaxKind.EqualExpression =>
                $"{_expr.ConvertExpression(bin.Left)} = {_expr.ConvertExpression(bin.Right)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.AndExpression =>
                $"{ConvertSearchCondition(bin.Left)} AND {ConvertSearchCondition(bin.Right)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.OrExpression =>
                $"{ConvertSearchCondition(bin.Left)} OR {ConvertSearchCondition(bin.Right)}",
            _ => _expr.ConvertExpression(condition)
        };
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

