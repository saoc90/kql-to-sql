using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

internal class AdvancedHandlers : OperatorHandlerBase
{
    internal AdvancedHandlers(KqlToSqlConverter converter, ExpressionSqlBuilder expr)
        : base(converter, expr) { }

    internal string ApplyEvaluate(string leftSql, EvaluateOperator evaluate)
    {
        if (evaluate.FunctionCall is not FunctionCallExpression fce)
            throw new NotSupportedException("Unsupported evaluate expression");

        var pluginName = fce.Name.SimpleName.ToLowerInvariant();

        return pluginName switch
        {
            "pivot" => ApplyEvaluatePivot(leftSql, fce),
            "narrow" => ApplyEvaluateNarrow(leftSql),
            "bag_unpack" => ApplyEvaluateBagUnpack(leftSql, fce),
            _ => throw new NotSupportedException($"Unsupported evaluate plugin '{pluginName}'")
        };
    }

    internal string ApplyEvaluatePivot(string leftSql, FunctionCallExpression fce)
    {
        var args = fce.ArgumentList.Expressions;
        if (args.Count < 2)
            throw new NotSupportedException("pivot() requires at least 2 arguments");

        var pivotCol = Expr.ConvertExpression(args[0].Element);

        string aggSql;
        if (args[1].Element is FunctionCallExpression aggFce)
        {
            var name = aggFce.Name.SimpleName.ToLowerInvariant();
            var aggArgs = aggFce.ArgumentList.Expressions
                .Select(a => Expr.ConvertExpression(a.Element)).ToArray();
            // sum(INTERVAL) — DuckDB's SUM rejects INTERVAL; route through epoch-ms so the
            // aggregate operates on BIGINT and the result is reconstructed as INTERVAL. The
            // same path applies in the summarize handler; duplicated here because pivot's
            // USING clause consumes the aggregate SQL directly without going through it.
            if (aggFce.Is(Aggregates.Sum) && aggArgs.Length == 1
                && (Expr.IsIntervalExpression(aggArgs[0])
                    || HasIntervalNameReference(aggFce.ArgumentList.Expressions[0].Element)))
            {
                var ms = $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + ({aggArgs[0]}) AS TIMESTAMP))";
                aggSql = $"((SUM({ms})) * INTERVAL '1 millisecond')";
            }
            else
            {
                aggSql = Dialect.TryTranslateAggregate(name, aggArgs)
                    ?? $"{name}({string.Join(", ", aggArgs)})";
            }
        }
        else
        {
            aggSql = Expr.ConvertExpression(args[1].Element);
        }

        var groupByCols = new List<string>();
        for (int i = 2; i < args.Count; i++)
        {
            groupByCols.Add(Expr.ConvertExpression(args[i].Element));
        }

        var groupByClause = groupByCols.Count > 0
            ? $" GROUP BY {string.Join(", ", groupByCols)}"
            : "";

        // Pin the output schema by collecting downstream references to pivot-output columns.
        // Without IN, DuckDB's PIVOT emits columns for distinct runtime pivot-values only;
        // subsequent operators that bind those columns fail when the data is empty at EXPLAIN.
        var pinned = CollectPivotOutputColumns(fce, args);
        var inClause = pinned.Count > 0
            ? $" IN ({string.Join(", ", pinned.Select(QuoteStringLiteral))})"
            : "";

        return $"PIVOT ({leftSql}) ON {pivotCol}{inClause} USING {aggSql}{groupByClause}";
    }

    private static string QuoteStringLiteral(string s) => "'" + s.Replace("'", "''") + "'";

    private static List<string> CollectPivotOutputColumns(
        FunctionCallExpression pivotFce,
        Kusto.Language.Syntax.SyntaxList<SeparatedElement<Expression>> pivotArgs)
    {
        var exclude = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        AddIdentifierNames(pivotArgs[0].Element, exclude);
        if (pivotArgs[1].Element is FunctionCallExpression agg)
            foreach (var a in agg.ArgumentList.Expressions)
                AddIdentifierNames(a.Element, exclude);
        for (int i = 2; i < pivotArgs.Count; i++)
            AddIdentifierNames(pivotArgs[i].Element, exclude);

        var candidates = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void Add(string name)
        {
            if (string.IsNullOrEmpty(name)) return;
            if (exclude.Contains(name)) return;
            if (seen.Add(name)) candidates.Add(name);
        }

        var scan = pivotFce.Parent?.Parent?.Parent as PipeExpression;
        while (scan != null)
        {
            var op = scan.Operator;
            if (IsSchemaResetOperator(op)) break;

            foreach (var fce in op.GetDescendants<FunctionCallExpression>())
            {
                if (fce.Is(Functions.ColumnIfExists) && fce.ArgumentList.Expressions.Count >= 1
                    && fce.ArgumentList.Expressions[0].Element is LiteralExpression lit
                    && lit.Kind == SyntaxKind.StringLiteralExpression
                    && lit.LiteralValue is string s)
                {
                    Add(s);
                }
            }

            if (op is ExtendOperator ext)
            {
                foreach (var sep in ext.Expressions)
                {
                    if (sep.Element is SimpleNamedExpression sne
                        && sne.Name is NameDeclaration nd)
                    {
                        var lhs = nd.Name?.SimpleName;
                        if (!string.IsNullOrEmpty(lhs)
                            && sne.Expression.GetDescendants<NameReference>()
                                .Any(r => string.Equals(r.SimpleName, lhs, StringComparison.OrdinalIgnoreCase)))
                        {
                            Add(lhs!);
                        }
                    }
                }
            }

            if (op is ProjectOperator proj)
            {
                foreach (var sep in proj.Expressions)
                {
                    if (sep.Element is NameReference pnr && !string.IsNullOrEmpty(pnr.SimpleName))
                        Add(pnr.SimpleName);
                    else if (sep.Element is SimpleNamedExpression psne
                        && psne.Expression is NameReference prhs
                        && !string.IsNullOrEmpty(prhs.SimpleName))
                        Add(prhs.SimpleName);
                }
            }

            scan = scan.Parent as PipeExpression;
        }

        return candidates;
    }

    private static bool IsSchemaResetOperator(QueryOperator op) => op switch
    {
        SummarizeOperator => true,
        JoinOperator => true,
        UnionOperator => true,
        LookupOperator => true,
        MakeSeriesOperator => true,
        EvaluateOperator => true,
        _ => false,
    };

    private bool HasIntervalNameReference(SyntaxNode? node)
    {
        if (node == null) return false;
        if (node is NameReference nr && Expr.IsIntervalColumn(nr.Name.SimpleName))
            return true;
        foreach (var child in node.GetDescendants<NameReference>())
            if (Expr.IsIntervalColumn(child.Name.SimpleName)) return true;
        return false;
    }

    private static void AddIdentifierNames(Expression expr, HashSet<string> bag)
    {
        if (expr is NameReference nr && !string.IsNullOrEmpty(nr.SimpleName))
            bag.Add(nr.SimpleName);
        foreach (var inner in expr.GetDescendants<NameReference>())
            if (!string.IsNullOrEmpty(inner.SimpleName))
                bag.Add(inner.SimpleName);
    }

    internal string ApplyEvaluateNarrow(string leftSql)
    {
        return $"UNPIVOT ({leftSql}) ON COLUMNS(*) INTO NAME \"Column\" VALUE \"Value\"";
    }

    internal string ApplyEvaluateBagUnpack(string leftSql, FunctionCallExpression fce)
    {
        if (fce.ArgumentList.Expressions.Count < 1)
            throw new NotSupportedException("bag_unpack() requires at least 1 argument");

        var column = Expr.ConvertExpression(fce.ArgumentList.Expressions[0].Element);
        var fromSql = ExtractFrom(leftSql);
        var excludeClause = Dialect.SelectExclude(new[] { column });
        return $"SELECT {excludeClause}, UNNEST(from_json({column}, '{{}}')) FROM {fromSql}";
    }

    internal string ApplyMakeSeries(string leftSql, MakeSeriesOperator makeSeries)
    {
        var onExpr = Expr.ConvertExpression(makeSeries.OnClause.Expression);

        string fromExpr, toExpr, stepExpr;
        // Kusto `from F to T step S` is END-EXCLUSIVE; `in range(F, T, S)` reuses range() which is END-INCLUSIVE.
        bool inclusiveEnd;
        if (makeSeries.RangeClause is MakeSeriesFromToStepClause fromToStep)
        {
            fromExpr = Expr.ConvertExpression(fromToStep.MakeSeriesFromClause.Expression);
            toExpr = Expr.ConvertExpression(fromToStep.MakeSeriesToClause.Expression);
            stepExpr = fromToStep.MakeSeriesStepClause.Expression.ToString().Trim();
            inclusiveEnd = false;
        }
        else if (makeSeries.RangeClause is MakeSeriesInRangeClause inRange)
        {
            var rangeArgs = inRange.Arguments.Expressions;
            if (rangeArgs.Count < 3)
                throw new NotSupportedException("make-series in range() requires 3 arguments");
            fromExpr = Expr.ConvertExpression(rangeArgs[0].Element);
            toExpr = Expr.ConvertExpression(rangeArgs[1].Element);
            stepExpr = rangeArgs[2].Element.ToString().Trim();
            inclusiveEnd = true;
        }
        else
        {
            throw new NotSupportedException($"Unsupported make-series range clause: {makeSeries.RangeClause.GetType().Name}");
        }
        _ = inclusiveEnd;

        string stepInterval;
        if (ExpressionSqlBuilder.TryParseTimespan(stepExpr, out var stepMs))
        {
            stepInterval = $"{stepMs} * INTERVAL '1 millisecond'";
        }
        else
        {
            stepInterval = $"INTERVAL '{stepExpr}'";
        }

        var fromSql = ExtractFrom(leftSql);

        // Build aggregate expressions
        var aggParts = new List<(string SelectExpr, string Alias, string? Default)>();
        foreach (var agg in makeSeries.Aggregates)
        {
            if (agg.Element is not MakeSeriesExpression mse) continue;

            string alias;
            string aggSql;
            if (mse.Expression is SimpleNamedExpression named)
            {
                alias = named.Name.SimpleName;
                if (named.Expression is FunctionCallExpression fce)
                {
                    var fname = fce.Name.SimpleName.ToLowerInvariant();
                    var fargs = fce.ArgumentList.Expressions.Select(a => Expr.ConvertExpression(a.Element)).ToArray();
                    aggSql = Dialect.TryTranslateAggregate(fname, fargs) ?? $"{fname}({string.Join(", ", fargs)})";
                }
                else
                {
                    aggSql = Expr.ConvertExpression(named.Expression);
                }
            }
            else if (mse.Expression is FunctionCallExpression directFce)
            {
                var fname = directFce.Name.SimpleName.ToLowerInvariant();
                var fargs = directFce.ArgumentList.Expressions.Select(a => Expr.ConvertExpression(a.Element)).ToArray();
                aggSql = Dialect.TryTranslateAggregate(fname, fargs) ?? $"{fname}({string.Join(", ", fargs)})";
                alias = fargs.Length > 0 ? $"{fname}_{fargs[0]}" : fname;
            }
            else
            {
                alias = "series_agg";
                aggSql = Expr.ConvertExpression(mse.Expression);
            }

            string? defaultVal = null;
            if (mse.DefaultExpression is DefaultExpressionClause dec)
            {
                defaultVal = Expr.ConvertExpression(dec.Expression);
            }

            aggParts.Add((aggSql, alias, defaultVal));
        }

        // Group-by columns
        var byColumns = new List<string>();
        if (makeSeries.ByClause is MakeSeriesByClause byClause)
        {
            byColumns.AddRange(byClause.Expressions.Select(e => Expr.ConvertExpression(e.Element)));
        }

        // Build: generate time series, cross join with by-columns, left join with aggregated data.
        // Use interval math directly on the datetime bounds (no TIMESTAMP->BIGINT cast, which DuckDB rejects).
        // `range(from, to, step)` is END-EXCLUSIVE (matches Kusto `from..to..step`);
        // `generate_series(from, to, step)` is END-INCLUSIVE (matches Kusto `in range(...)`).
        var axisFunc = inclusiveEnd ? "generate_series" : "range";
        var timeSeries = $"SELECT UNNEST({axisFunc}({fromExpr}, {toExpr}, {stepInterval})) AS _ts";

        var aggSelectList = string.Join(", ", aggParts.Select(a =>
            a.Default != null
                ? $"LIST(COALESCE({a.Alias}_val, {a.Default}) ORDER BY _ts) AS {a.Alias}"
                : $"LIST({a.Alias}_val ORDER BY _ts) AS {a.Alias}"));

        var dataAggSelect = string.Join(", ", aggParts.Select(a => $"{a.SelectExpr} AS {a.Alias}_val"));
        var bucketExpr = $"EPOCH_MS(CAST(FLOOR(EPOCH_MS({onExpr})/{stepMs})*{stepMs} AS BIGINT))";

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

    internal string ApplyMvExpand(string leftSql, MvExpandOperator mvExpand)
    {
        // Each mv-expand slot is either a bare column, `name = expr`, or a bare expression.
        // For a bare expression Kusto names the output as the innermost identifier (same rule
        // toreal/tostring/parse_json auto-naming uses) — treat that identifier as the column
        // being replaced in place so EXCLUDE picks it up.
        var columns = new List<(string Name, string Source, bool ExcludeName)>();
        int fallbackIndex = 0;
        foreach (var se in mvExpand.Expressions)
        {
            if (se.Element is not MvExpandExpression mve)
                throw new NotSupportedException("Unexpected mv-expand expression type");

            if (mve.Expression is SimpleNamedExpression sne)
            {
                var name = sne.Name.SimpleName;
                var sourceSql = Expr.ConvertExpression(sne.Expression);
                columns.Add((name, sourceSql, false));
            }
            else if (mve.Expression is NameReference nr)
            {
                var bare = nr.SimpleName;
                columns.Add((bare, bare, true));
            }
            else
            {
                var sourceSql = Expr.ConvertExpression(mve.Expression);
                var innerName = TryGetInnerIdentifier(mve.Expression);
                if (innerName != null)
                    columns.Add((innerName, sourceSql, true));
                else
                    columns.Add(($"col_{fallbackIndex++}", sourceSql, false));
            }
        }

        // Single-column case: simple unnest
        string sourceAlias = "t";
        var fromSql = ExtractFromAsRelation(leftSql);
        var aliasedFrom = $"{fromSql} AS {sourceAlias}";

        if (columns.Count == 1)
        {
            var (name, source, excludeName) = columns[0];
            var unnestAlias = "u";
            var bareRef = source == name;
            var unnestSource = CoerceToUnnestable(
                bareRef ? $"{sourceAlias}.{name}" : source,
                forceJsonCoerce: bareRef && ColumnDefinedAsJson(leftSql, name));
            var unnestClause = $"CROSS JOIN UNNEST({unnestSource}) AS {unnestAlias}(value)";
            var excludePart = excludeName
                ? $"{sourceAlias}.{Dialect.SelectExclude(new[] { name })}"
                : $"{sourceAlias}.*";
            return $"SELECT {excludePart}, {unnestAlias}.value AS {name} FROM {aliasedFrom} {unnestClause}";
        }

        // Multi-column: chain unnests, each as a separate CROSS JOIN
        var clauses = new List<string>();
        for (int i = 0; i < columns.Count; i++)
        {
            var (name, source, _) = columns[i];
            var bareRef = source == name;
            var unnestSource = CoerceToUnnestable(
                bareRef ? $"{sourceAlias}.{name}" : source,
                forceJsonCoerce: bareRef && ColumnDefinedAsJson(leftSql, name));
            clauses.Add($"CROSS JOIN UNNEST({unnestSource}) AS u{i}(value)");
        }
        var existingNames = columns.Where(c => c.ExcludeName).Select(c => c.Name).ToArray();
        var excludeAll = existingNames.Length > 0 ? Dialect.SelectExclude(existingNames) : "*";
        var selectCols = string.Join(", ", columns.Select((c, i) => $"u{i}.value AS {c.Name}"));
        return $"SELECT {sourceAlias}.{excludeAll}, {selectCols} FROM {aliasedFrom} {string.Join(" ", clauses)}";
    }

    internal string ApplyMvApply(string leftSql, MvApplyOperator mvApply)
    {
        if (mvApply.Expressions.Count != 1)
            throw new NotSupportedException("mv-apply with multiple expressions is not supported");

        if (mvApply.Expressions[0].Element is not MvApplyExpression mae)
            throw new NotSupportedException("Unexpected mv-apply expression type");

        // mv-apply alias = expr on (...) — the LHS name is the column inside the subquery;
        // the RHS is the source expression in the outer scope.
        string columnName;
        string sourceSql;
        if (mae.Expression is SimpleNamedExpression sne)
        {
            columnName = sne.Name.SimpleName;
            sourceSql = Expr.ConvertExpression(sne.Expression);
        }
        else
        {
            var bare = mae.Expression.ToString().Trim();
            columnName = bare;
            sourceSql = bare;
        }

        var subquerySql = Converter.ConvertNode(mvApply.Subquery.Expression);

        string sourceAlias = "t";
        var fromSql = ExtractFromAsRelation(leftSql);
        var aliasedFrom = $"{fromSql} AS {sourceAlias}";

        var unnestAlias = "u";
        var unnestSource = sourceSql == columnName ? $"{sourceAlias}.{columnName}" : sourceSql;
        var unnestClause = $"CROSS JOIN UNNEST({unnestSource}) AS {unnestAlias}(value)";

        // KQL's mv-apply subquery operates on an implicit "virtual table" named after the element column.
        // The inner pipeline might emit FROM {columnName} (when it starts with a name-bound operator) or
        // it might emit FROM (SELECT *) (when it starts with extend/project/summarize with no source). We
        // patch both forms so the subquery has a concrete row source.
        var virtualSource = $"(SELECT {unnestAlias}.value AS {columnName})";
        var patched = subquerySql
            .Replace($"FROM {columnName}", $"FROM {virtualSource}")
            .Replace("FROM (SELECT *)", $"FROM {virtualSource}");
        return $"SELECT {sourceAlias}.*, _sub.* FROM {aliasedFrom} {unnestClause}, LATERAL ({patched}) AS _sub";
    }

    internal string ApplyTopHitters(string leftSql, TopHittersOperator topHitters)
    {
        var count = Expr.ConvertExpression(topHitters.Expression);
        var column = Expr.ConvertExpression(topHitters.OfExpression);
        var fromSql = ExtractFrom(leftSql);

        if (topHitters.ByClause is TopHittersByClause byClause)
        {
            string byExpr;
            if (byClause.Expression is FunctionCallExpression bfce)
            {
                var name = bfce.Name.SimpleName.ToLowerInvariant();
                var bArgs = bfce.ArgumentList.Expressions
                    .Select(a => Expr.ConvertExpression(a.Element)).ToArray();
                byExpr = Dialect.TryTranslateAggregate(name, bArgs)
                    ?? $"{name}({string.Join(", ", bArgs)})";
            }
            else
            {
                byExpr = Expr.ConvertExpression(byClause.Expression);
            }
            return $"SELECT {column}, {byExpr} AS approximate_count FROM {fromSql} GROUP BY {column} ORDER BY approximate_count DESC LIMIT {count}";
        }

        return $"SELECT {column}, COUNT(*) AS approximate_count FROM {fromSql} GROUP BY {column} ORDER BY approximate_count DESC LIMIT {count}";
    }

    internal string ApplyTopNested(string leftSql, TopNestedOperator topNested)
    {
        var originalFrom = ExtractFrom(leftSql);

        var selectColumns = new List<string>();
        var prevGroupCols = new List<string>();
        string? prevFilteredSql = null;

        // Kusto names a top-nested aggregate column after its explicit name when
        // one is given (e.g. `Cnt=count()` -> `Cnt`), otherwise `aggregated_<subject>`.
        string AggAliasFor(TopNestedClause c)
        {
            if (c.ByExpression is SimpleNamedExpression named)
                return Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(named.Name.SimpleName);
            return $"aggregated_{Expr.ConvertExpression(c.OfExpression)}";
        }

        for (int i = 0; i < topNested.Clauses.Count; i++)
        {
            if (topNested.Clauses[i].Element is not TopNestedClause clause)
                continue;

            var count = Expr.ConvertExpression(clause.Expression);
            var column = Expr.ConvertExpression(clause.OfExpression);
            var aggExpr = clause.ByExpression;

            string aggSql;
            if (aggExpr is SimpleNamedExpression sne && sne.Expression is FunctionCallExpression namedFce)
            {
                var name = namedFce.Name.SimpleName.ToLowerInvariant();
                var args = namedFce.ArgumentList.Expressions
                    .Select(a => Expr.ConvertExpression(a.Element)).ToArray();
                aggSql = Dialect.TryTranslateAggregate(name, args)
                    ?? $"{name}({string.Join(", ", args)})";
            }
            else if (aggExpr is FunctionCallExpression fce)
            {
                var name = fce.Name.SimpleName.ToLowerInvariant();
                var args = fce.ArgumentList.Expressions
                    .Select(a => Expr.ConvertExpression(a.Element)).ToArray();
                aggSql = Dialect.TryTranslateAggregate(name, args)
                    ?? $"{name}({string.Join(", ", args)})";
            }
            else
            {
                aggSql = Expr.ConvertExpression(aggExpr);
            }

            var aggAlias = AggAliasFor(clause);
            selectColumns.Add(column);
            selectColumns.Add(aggAlias);

            var groupBy = new List<string>(prevGroupCols) { column };
            var partitionBy = prevGroupCols.Count > 0
                ? $"PARTITION BY {string.Join(", ", prevGroupCols)} "
                : "";

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

            var groupColExprs = groupBy.Select(c => prevFilteredSql != null ? $"_src.{c}" : c).ToList();
            var prevAggCols = new List<string>();
            if (prevFilteredSql != null)
            {
                for (int p = 0; p < i; p++)
                {
                    if (topNested.Clauses[p].Element is TopNestedClause prevClause)
                    {
                        var prevAggAlias = AggAliasFor(prevClause);
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

    internal string ApplySample(string leftSql, SampleOperator sample)
    {
        var count = Expr.ConvertExpression(sample.Expression);
        var fromSql = ExtractFrom(leftSql);

        var dialectSample = Dialect.SampleClause(fromSql, count);
        if (dialectSample != null)
            return dialectSample;

        return $"{leftSql} ORDER BY RANDOM() LIMIT {count}";
    }

    internal string ApplySampleDistinct(string leftSql, SampleDistinctOperator sampleDistinct)
    {
        var count = Expr.ConvertExpression(sampleDistinct.Expression);
        var column = Expr.ConvertExpression(sampleDistinct.OfExpression);
        return $"SELECT DISTINCT {column} FROM ({leftSql}) ORDER BY RANDOM() LIMIT {count}";
    }

    internal string ApplySerialize(string leftSql, SerializeOperator serialize)
    {
        if (serialize.Expressions.Count == 0)
            return leftSql;

        // serialize fixes the row order; window functions in its expressions (row_number/prev/…)
        // must run in the order established by a preceding sort. Carry that order into the windows.
        Expr.SetWindowOrder(ExtractTrailingOrderBy(leftSql));
        string[] extras;
        try
        {
            extras = serialize.Expressions.Select(se =>
            {
                if (se.Element is SimpleNamedExpression sne)
                    return $"{Expr.ConvertExpression(sne.Expression)} AS {sne.Name.SimpleName}";
                return Expr.ConvertExpression(se.Element);
            }).ToArray();
        }
        finally { Expr.SetWindowOrder(null); }

        var joined = string.Join(", ", extras);

        if (HasLimit(leftSql))
            return $"SELECT *, {joined} FROM ({leftSql})";

        return AppendToSelectStar(leftSql, joined);
    }

    internal string ApplyGetSchema(string leftSql)
    {
        return $"DESCRIBE SELECT * FROM ({leftSql}) LIMIT 0";
    }

    internal string ConvertRange(RangeOperator range)
    {
        var name = range.Name.SimpleName;
        var start = Expr.ConvertExpression(range.From);
        var end = Expr.ConvertExpression(range.To);
        var step = Expr.ConvertExpression(range.Step);
        return Dialect.GenerateSeries(name, start, end, step);
    }

    internal string ConvertPrint(PrintOperator print)
    {
        var parts = new List<string>();
        int i = 0;
        foreach (var expr in print.Expressions)
        {
            if (expr.Element is SimpleNamedExpression sne)
            {
                var name = sne.Name.SimpleName;
                var value = Expr.ConvertExpression(sne.Expression);
                parts.Add($"{value} AS {name}");
            }
            else
            {
                var value = Expr.ConvertExpression(expr.Element);
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
                var cname = nat.Name.SimpleName;
                columnNames.Add(Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(cname));
                // Propagate KQL timespan columns so downstream sum/divide hit the epoch-ms path.
                var typeName = nat.Type?.ToString().Trim();
                if (string.Equals(typeName, "timespan", StringComparison.OrdinalIgnoreCase))
                    Expr.MarkIntervalColumn(cname);
            }
        }

        var colCount = columnNames.Count;
        var values = dt.Values;

        // Handle empty schema: datatable()[...] — infer columns as col0, col1, ...
        // Detect column count from value types if possible; otherwise assume 2 (key, value pairs)
        if (colCount == 0)
        {
            // Try to infer by looking at literal value types — pairs of string/int are common
            colCount = values.Count >= 2 ? 2 : 1;
            for (int k = 0; k < colCount; k++) columnNames.Add($"col{k}");
        }

        var rows = new List<string>();

        for (int i = 0; i < values.Count; i += colCount)
        {
            var rowValues = new List<string>();
            for (int j = 0; j < colCount && (i + j) < values.Count; j++)
            {
                rowValues.Add(Expr.ConvertLiteralValue(values[i + j].Element));
            }
            rows.Add($"({string.Join(", ", rowValues)})");
        }

        // Empty datatable: DuckDB rejects 'VALUES ) AS t(col)'. Emit a typed empty SELECT instead.
        if (rows.Count == 0)
        {
            var cols = string.Join(", ", columnNames.Select(n => $"NULL AS {n}"));
            return $"SELECT {cols} WHERE 1 = 0";
        }

        return $"SELECT * FROM (VALUES {string.Join(", ", rows)}) AS t({string.Join(", ", columnNames)})";
    }

    private static string CoerceToUnnestable(string source, bool forceJsonCoerce = false)
    {
        // DuckDB's UNNEST requires a LIST/ARRAY input. Our ConvertExpression may return a JSON
        // expression (parse_json(..) or CAST(.. AS JSON)) which isn't directly unnestable.
        // For JSON arrays, CAST AS JSON[] works. For JSON objects, CAST fails with
        // "UNNEST requires a single list as input". Use CASE to branch at runtime.
        //
        // The inline case is detected via the ::JSON / AS JSON suffix. When mv-expand targets a
        // bare column whose value was produced upstream by todynamic()/parse_json() (a JSON-typed
        // column reference like `t.arr`), the suffix is absent — the caller passes forceJsonCoerce
        // so the same CASE is applied. Native LIST columns must NOT be coerced (json_type would
        // mis-handle string elements), so the flag is only set for columns proven JSON upstream.
        var trimmed = source.TrimEnd();
        if (forceJsonCoerce ||
            trimmed.EndsWith("::JSON", StringComparison.OrdinalIgnoreCase) ||
            trimmed.EndsWith(" AS JSON)", StringComparison.OrdinalIgnoreCase))
        {
            return $"CASE WHEN json_type({source}) = 'ARRAY' THEN CAST({source} AS JSON[]) " +
                   $"ELSE list_transform(json_keys({source}), lambda k: json_extract({source}, '$.' || k)) END";
        }
        return source;
    }

    // Detects whether `columnName` is defined as a JSON-typed expression in the upstream SQL
    // (e.g. `CAST(Value AS JSON) AS arr` emitted by todynamic()/parse_json()). mv-expand of such a
    // bare column reference yields a JSON value that DuckDB's UNNEST rejects until coerced to a list.
    private static bool ColumnDefinedAsJson(string leftSql, string columnName)
    {
        // Match the JSON producer immediately followed by the column alias, optionally quoted:
        //   ... AS JSON) AS arr      |   ... AS JSON) AS "order"
        //   ... ::JSON AS arr        |   ... ::JSON AS "order"
        foreach (var marker in new[] { " AS JSON)", "::JSON" })
        {
            int idx = 0;
            while ((idx = leftSql.IndexOf(marker, idx, StringComparison.OrdinalIgnoreCase)) >= 0)
            {
                var after = leftSql.AsSpan(idx + marker.Length);
                int p = 0;
                while (p < after.Length && after[p] == ' ') p++;
                if (after.Slice(p).StartsWith("AS ", StringComparison.OrdinalIgnoreCase))
                {
                    p += 3;
                    while (p < after.Length && after[p] == ' ') p++;
                    var alias = after.Slice(p);
                    if (alias.Length > 0 && alias[0] == '"')
                        alias = alias.Slice(1);
                    if (alias.StartsWith(columnName, StringComparison.Ordinal))
                    {
                        // Ensure full-token match: next char must be a delimiter (quote, space, comma, paren) or end.
                        char next = alias.Length > columnName.Length ? alias[columnName.Length] : ' ';
                        if (next is '"' or ' ' or ',' or ')' or '\0')
                            return true;
                    }
                }
                idx += marker.Length;
            }
        }
        return false;
    }

    private static string? TryGetInnerIdentifier(SyntaxNode node)
    {
        // Drill through single-arg conversion wrappers (tostring/toreal/parse_json/...)
        // to the inner identifier — same rule live Kusto uses to auto-name mv-expand output.
        SyntaxNode? current = node;
        while (current is FunctionCallExpression fce && fce.ArgumentList.Expressions.Count == 1)
        {
            if (fce.IsAny(Functions.ToString, Functions.ToReal, Functions.ToDouble, Functions.ToInt, Functions.ToLong,
                    Functions.ToBool, Functions.ToDateTime, Functions.ToDynamic_, Functions.ParseJson)
                || fce.Name.SimpleName.Equals("tofloat", StringComparison.OrdinalIgnoreCase) // TODO: no Kusto.Language symbol for 'tofloat'
                || fce.Name.SimpleName.Equals("parsejson", StringComparison.OrdinalIgnoreCase)) // deprecated alias
                current = fce.ArgumentList.Expressions[0].Element;
            else break;
        }
        if (current is NameReference nr)
            return Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(nr.Name.SimpleName);
        return null;
    }

    internal string ConvertExternalData(ExternalDataExpression ed)
    {
        var uriText = ed.URIs.ToString().Trim().Trim('[', ']').Trim();
        var uri = uriText.Trim('\'', '"');

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
}
