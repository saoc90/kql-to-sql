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
            "bag_unpack" => ApplyEvaluateBagUnpack(leftSql, fce, evaluate),
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

    internal string ApplyEvaluateBagUnpack(string leftSql, FunctionCallExpression fce, EvaluateOperator evaluate)
    {
        if (fce.ArgumentList.Expressions.Count < 1)
            throw new NotSupportedException("bag_unpack() requires at least 1 argument");

        var column = Expr.ConvertExpression(fce.ArgumentList.Expressions[0].Element);

        // Optional second argument: a column-name prefix applied to every unpacked key.
        var prefix = string.Empty;
        if (fce.ArgumentList.Expressions.Count >= 2
            && fce.ArgumentList.Expressions[1].Element is LiteralExpression plit
            && plit.Kind == SyntaxKind.StringLiteralExpression
            && plit.LiteralValue is string ps)
        {
            prefix = ps;
        }

        // DuckDB needs the output columns known at plan time, so we infer the bag's keys from the dynamic
        // object literals feeding this column upstream. (from_json with an empty schema is what produced
        // the original "Empty object in JSON structure" error.) Kusto emits the unpacked columns sorted
        // alphabetically (ordinal), so we sort to match its column order/positions.
        var keys = CollectBagKeys(evaluate);
        keys.Sort(StringComparer.Ordinal);
        var fromSql = ExtractFrom(leftSql);

        if (keys.Count == 0)
        {
            // No statically-knowable keys: fall back to dropping the bag column (Kusto would emit no
            // unpacked columns for an empty/always-null bag anyway).
            return $"SELECT {Dialect.SelectExclude(new[] { column })} FROM {fromSql}";
        }

        // `d->>'$.key'` (json_extract_string) returns each scalar as clean unquoted text and nested
        // objects/arrays as their JSON text — matching how Kusto surfaces the unpacked values.
        var projected = keys.Select(k =>
        {
            var rawName = prefix + k;
            var outName = ExpressionSqlBuilder.QuoteIdentifierIfReserved(rawName);
            var path = "'$." + k.Replace("'", "''") + "'";
            // Values come out as VARCHAR (clean text). Their KQL type is unknown, so tag them dynamic-text:
            // a later numeric aggregate (sum/avg) coerces them, while string ops keep them as text.
            Expr.MarkDynamicTextColumn(rawName);
            return $"{column}->>{path} AS {outName}";
        });

        var excludeClause = Dialect.SelectExclude(new[] { column });
        return $"SELECT {excludeClause}, {string.Join(", ", projected)} FROM {fromSql}";
    }

    /// <summary>Collects, in first-seen order, the distinct keys of every dynamic object literal that
    /// appears upstream of this evaluate operator — these become the bag_unpack output columns.</summary>
    private static List<string> CollectBagKeys(EvaluateOperator evaluate)
    {
        var keys = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        // Walk up the pipe chain to the query root, then scan for dynamic object literals. Scanning from
        // the root keeps it simple and robust to where the bag was constructed (datatable, extend pack(), …).
        SyntaxNode? root = evaluate;
        while (root.Parent != null) root = root.Parent;

        foreach (var obj in root.GetDescendants<JsonObjectExpression>())
        {
            for (int i = 0; i < obj.Pairs.Count; i++)
            {
                var key = obj.Pairs[i].Element.Name.ValueText;
                if (!string.IsNullOrEmpty(key) && seen.Add(key))
                    keys.Add(key);
            }
        }
        return keys;
    }

    internal string ApplyMakeSeries(string leftSql, MakeSeriesOperator makeSeries)
    {
        var onExpr = Expr.ConvertExpression(makeSeries.OnClause.Expression);

        string fromExpr, toExpr, stepExpr;
        Expression stepNode;
        // Kusto `from F to T step S` is END-EXCLUSIVE; `in range(F, T, S)` reuses range() which is END-INCLUSIVE.
        bool inclusiveEnd;
        if (makeSeries.RangeClause is MakeSeriesFromToStepClause fromToStep)
        {
            stepNode = fromToStep.MakeSeriesStepClause.Expression;
            stepExpr = stepNode.ToString().Trim();
            // `from`/`to` are optional; when omitted Kusto infers them from the data's min/max of the
            // axis column. Emit scalar subqueries instead of dereferencing the (null) clauses.
            fromExpr = fromToStep.MakeSeriesFromClause != null
                ? Expr.ConvertExpression(fromToStep.MakeSeriesFromClause.Expression)
                : $"(SELECT MIN({onExpr}) FROM ({leftSql}))";
            toExpr = fromToStep.MakeSeriesToClause != null
                ? Expr.ConvertExpression(fromToStep.MakeSeriesToClause.Expression)
                : $"(SELECT MAX({onExpr}) FROM ({leftSql}))";
            // Explicit from..to is end-exclusive; an inferred range must include the max bucket.
            inclusiveEnd = fromToStep.MakeSeriesToClause == null;
        }
        else if (makeSeries.RangeClause is MakeSeriesInRangeClause inRange)
        {
            var rangeArgs = inRange.Arguments.Expressions;
            if (rangeArgs.Count < 3)
                throw new NotSupportedException("make-series in range() requires 3 arguments");
            fromExpr = Expr.ConvertExpression(rangeArgs[0].Element);
            toExpr = Expr.ConvertExpression(rangeArgs[1].Element);
            stepNode = rangeArgs[2].Element;
            stepExpr = stepNode.ToString().Trim();
            inclusiveEnd = true;
        }
        else
        {
            throw new NotSupportedException($"Unsupported make-series range clause: {makeSeries.RangeClause.GetType().Name}");
        }

        // The axis type is dictated by the step: a timespan literal (1d, 1h, 5m, ...) means a
        // datetime axis; anything else (a number) means a NUMERIC axis. DuckDB's range()/generate_series
        // need an INTERVAL step for a datetime axis but reject one on a numeric axis — emitting an
        // INTERVAL on a numeric axis is the root cause of "No function range(INTEGER,INTEGER,INTERVAL)".
        bool datetimeAxis = ExpressionSqlBuilder.TryParseTimespan(stepExpr, out var stepMs);

        var fromSql = ExtractFrom(leftSql);

        string timeSeries;        // SELECT ... AS _ts  — the generated axis grid points
        string bucketExpr;        // maps an on-axis value onto the grid point it belongs to
        if (datetimeAxis)
        {
            // `range(from, to, step)` is END-EXCLUSIVE (matches Kusto `from..to..step`);
            // `generate_series(from, to, step)` is END-INCLUSIVE (matches Kusto `in range(...)`).
            var axisFunc = inclusiveEnd ? "generate_series" : "range";
            var stepInterval = $"{stepMs} * INTERVAL '1 millisecond'";
            timeSeries = $"SELECT UNNEST({axisFunc}({fromExpr}, {toExpr}, {stepInterval})) AS _ts";
            // Floor each datetime to the step grid via epoch-ms, then reconstruct the timestamp.
            bucketExpr = $"EPOCH_MS(CAST(FLOOR(EPOCH_MS({onExpr})/{stepMs})*{stepMs} AS BIGINT))";
        }
        else
        {
            // Numeric axis. DuckDB's range()/generate_series only accept BIGINT steps, so build the grid
            // arithmetically from an index range — this also supports fractional steps. The point count is
            // ceil((to-from)/step), end-exclusive (+1 for the inclusive `in range(...)` form).
            var stepNum = Expr.ConvertExpression(stepNode);
            var extra = inclusiveEnd ? " + 1" : "";
            var npts = $"CAST(CEIL((({toExpr}) - ({fromExpr}))/({stepNum})) AS BIGINT){extra}";
            timeSeries = $"SELECT ({fromExpr}) + _i*({stepNum}) AS _ts FROM range(0, {npts}) AS _r(_i)";
            // Snap each value to its grid point: from + floor((on-from)/step)*step.
            bucketExpr = $"(({fromExpr}) + FLOOR((({onExpr}) - ({fromExpr}))/({stepNum}))*({stepNum}))";
        }
        _ = stepMs;

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

            // Gaps are filled with `default=` when given; otherwise Kusto fills numeric series with 0
            // (its type default). Always carry a fill value so the output array has no NULL holes.
            string defaultVal = "0";
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

        // Build: generate the axis grid (computed above as `timeSeries`), optionally cross join with
        // by-columns, then left join with the per-bucket aggregated data.
        var aggSelectList = string.Join(", ", aggParts.Select(a =>
            a.Default != null
                ? $"LIST(COALESCE({a.Alias}_val, {a.Default}) ORDER BY _ts) AS {a.Alias}"
                : $"LIST({a.Alias}_val ORDER BY _ts) AS {a.Alias}"));

        // DuckDB's SUM over integers yields HUGEINT, which surfaces to the host as a value type that
        // doesn't round-trip through JSON cleanly. Coerce numeric aggregates to DOUBLE so the resulting
        // dynamic array compares value-for-value against Kusto's (integer JSON canonicalizes 10.0 -> 10).
        var dataAggSelect = string.Join(", ", aggParts.Select(a => $"CAST({a.SelectExpr} AS DOUBLE) AS {a.Alias}_val"));

        var groupByData = byColumns.Count > 0
            ? $"{string.Join(", ", byColumns)}, "
            : "";

        var dataAgg = $"SELECT {groupByData}{bucketExpr} AS _bucket, {dataAggSelect} FROM {fromSql} GROUP BY {groupByData}_bucket";

        // Inside a dynamic array, Kusto renders each datetime as an ISO-8601 string with a trailing Z and
        // 7 fractional digits (e.g. 2020-01-01T00:00:00.0000000Z). DuckDB's default LIST<TIMESTAMP>->JSON
        // uses a different spelling, so format axis timestamps to Kusto's shape (numeric axes pass through).
        var axisElem = datetimeAxis
            ? "strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z'"
            : "_axis._ts";

        if (byColumns.Count > 0)
        {
            var bySelect = string.Join(", ", byColumns);
            var crossJoin = $"SELECT DISTINCT {bySelect} FROM {fromSql}";
            var tsAxis = $"SELECT _g.*, _t._ts FROM ({crossJoin}) AS _g CROSS JOIN ({timeSeries}) AS _t";
            var joinCond = string.Join(" AND ", byColumns.Select(c => $"_axis.{c} = _data.{c}").Append("_axis._ts = _data._bucket"));
            // Kusto column order: by-columns, then aggregates, then the axis (`on`) column last.
            return $"SELECT {string.Join(", ", byColumns.Select(c => $"_axis.{c}"))}, {aggSelectList}, LIST({axisElem} ORDER BY _axis._ts) AS {onExpr} FROM ({tsAxis}) AS _axis LEFT JOIN ({dataAgg}) AS _data ON {joinCond} GROUP BY {string.Join(", ", byColumns.Select(c => $"_axis.{c}"))}";
        }
        else
        {
            var joinCond = "_axis._ts = _data._bucket";
            // Kusto column order: aggregates first, then the axis (`on`) column last.
            return $"SELECT {aggSelectList}, LIST({axisElem} ORDER BY _axis._ts) AS {onExpr} FROM ({timeSeries}) AS _axis LEFT JOIN ({dataAgg}) AS _data ON {joinCond}";
        }
    }

    internal string ApplyMvExpand(string leftSql, MvExpandOperator mvExpand)
    {
        string sourceAlias = "t";

        // Each mv-expand slot is either a bare column, `name = expr`, or a bare expression.
        // For a bare expression Kusto names the output as the innermost identifier (same rule
        // toreal/tostring/parse_json auto-naming uses) — treat that identifier as the column
        // being replaced in place so EXCLUDE picks it up. Source/IsJson are resolved so a JSON
        // value (datatable dynamic column, parse_json/d.field result, chained mv-expand output) is
        // coerced to a list before UNNEST regardless of whether the slot renames its source.
        var columns = new List<(string Name, string Source, bool IsJson, bool ExcludeName, string? Cast)>();
        int fallbackIndex = 0;
        foreach (var se in mvExpand.Expressions)
        {
            if (se.Element is not MvExpandExpression mve)
                throw new NotSupportedException("Unexpected mv-expand expression type");

            // `mv-expand x = arr to typeof(long)` casts each expanded element to the declared type
            // (the elements arrive as JSON when the source was navigated/parsed).
            var cast = MvExpandCastType(mve);

            if (mve.Expression is SimpleNamedExpression sne)
            {
                var (src, isJson) = ResolveMvSource(sne.Expression, sourceAlias, leftSql);
                columns.Add((sne.Name.SimpleName, src, isJson, false, cast));
            }
            else if (mve.Expression is NameReference nr)
            {
                var (src, isJson) = ResolveMvSource(nr, sourceAlias, leftSql);
                columns.Add((nr.SimpleName, src, isJson, true, cast));
            }
            else
            {
                var (src, isJson) = ResolveMvSource(mve.Expression, sourceAlias, leftSql);
                var innerName = TryGetInnerIdentifier(mve.Expression);
                if (innerName != null)
                    columns.Add((innerName, src, isJson, true, cast));
                else
                    columns.Add(($"col_{fallbackIndex++}", src, isJson, false, cast));
            }
        }

        // mv-expand with_itemindex=<col> exposes the 0-based position of each expanded element.
        string? itemIndexName = ExtractWithItemIndex(mvExpand);

        // An uncast mv-expand output stays *dynamic* in Kusto; when the source was JSON the element
        // is a JSON value, so mark the output column JSON for downstream coercion (chained mv-expand).
        // A `to typeof(string)` output is a string (e.g. a bag_keys key) so `d[col]` is JSON key access.
        foreach (var c in columns)
        {
            if (c.Cast == null && c.IsJson)
                Expr.MarkJsonColumn(c.Name);
            else if (c.Cast == "VARCHAR")
                Expr.MarkStringColumn(c.Name);
        }

        // Wrap leftSql as a derived table with a single fresh alias. (ExtractFromAsRelation can return
        // a relation that already carries its own alias — e.g. `(VALUES ...) AS t(d)` for a datatable —
        // which would produce an invalid double alias `... AS t(d) AS t`.)
        var aliasedFrom = $"({leftSql}) AS {sourceAlias}";

        // Single-column case: simple unnest (with optional with_itemindex).
        if (columns.Count == 1)
        {
            var (name, source, isJson, excludeName, cast) = columns[0];
            var unnestAlias = "u";
            var coerced = CoerceToUnnestable(source, forceJsonCoerce: isJson);
            var excludePart = excludeName
                ? $"{sourceAlias}.{Dialect.SelectExclude(new[] { name })}"
                : $"{sourceAlias}.*";

            if (itemIndexName != null)
            {
                // with_itemindex: zip each element with its 0-based position. DuckDB combines multiple
                // UNNESTs in one SELECT positionally, so UNNEST(list) and UNNEST(range(0,len)) align.
                var valExpr = cast != null ? $"CAST({unnestAlias}.val AS {cast})" : $"{unnestAlias}.val";
                return $"SELECT {excludePart}, {unnestAlias}.idx AS {itemIndexName}, {valExpr} AS {name} " +
                       $"FROM {aliasedFrom}, LATERAL (SELECT UNNEST({coerced}) AS val, " +
                       $"UNNEST(range(0, len({coerced}))) AS idx) AS {unnestAlias}";
            }

            var unnestClause = $"CROSS JOIN UNNEST({coerced}) AS {unnestAlias}(value)";
            var valueExpr = cast != null ? $"CAST({unnestAlias}.value AS {cast})" : $"{unnestAlias}.value";
            // When mv-expand replaces a column in place, keep its ORIGINAL position (Kusto does) via
            // * REPLACE, rather than EXCLUDE + append-at-end which reorders the schema.
            if (excludeName && Dialect.SelectReplace(sourceAlias, name, valueExpr) is { } replace)
                return $"SELECT {replace} FROM {aliasedFrom} {unnestClause}";
            return $"SELECT {excludePart}, {valueExpr} AS {name} FROM {aliasedFrom} {unnestClause}";
        }

        // Multi-column mv-expand expands the lists in PARALLEL (positionally zipped, shorter padded
        // with null) — not as a cross product. DuckDB combines multiple UNNESTs in one SELECT
        // positionally, so emit them together inside a single LATERAL subquery.
        var unnests = new List<string>();
        for (int i = 0; i < columns.Count; i++)
        {
            var c = columns[i];
            var unnestSource = CoerceToUnnestable(c.Source, forceJsonCoerce: c.IsJson);
            unnests.Add($"UNNEST({unnestSource}) AS v{i}");
        }
        var existingNames = columns.Where(c => c.ExcludeName).Select(c => c.Name).ToArray();
        var excludeAll = existingNames.Length > 0 ? Dialect.SelectExclude(existingNames) : "*";
        var selectCols = string.Join(", ", columns.Select((c, i) =>
            c.Cast != null ? $"CAST(u.v{i} AS {c.Cast}) AS {c.Name}" : $"u.v{i} AS {c.Name}"));
        return $"SELECT {sourceAlias}.{excludeAll}, {selectCols} FROM {aliasedFrom}, " +
               $"LATERAL (SELECT {string.Join(", ", unnests)}) AS u";
    }

    /// <summary>Resolves an mv-expand/mv-apply source expression to (unnestSql, isJson). A bare column
    /// reference is qualified with the table alias and classified via tracked JSON columns / upstream SQL;
    /// any other expression is converted and classified by its JSON markers.</summary>
    private (string Sql, bool IsJson) ResolveMvSource(Expression sourceNode, string sourceAlias, string leftSql)
    {
        if (sourceNode is NameReference nr)
        {
            var col = nr.SimpleName;
            var isJson = Expr.IsJsonColumn(col) || ColumnDefinedAsJson(leftSql, col);
            return ($"{sourceAlias}.{Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(col)}", isJson);
        }
        var sql = Expr.ConvertExpression(sourceNode);
        return (sql, Expressions.ExpressionSqlBuilder.LooksLikeJsonResult(sql));
    }

    /// <summary>Extracts the column name from an mv-expand `with_itemindex=<name>` parameter, or null.</summary>
    private static string? ExtractWithItemIndex(MvExpandOperator mvExpand)
    {
        for (int i = 0; i < mvExpand.Parameters.Count; i++)
        {
            if (mvExpand.Parameters[i] is NamedParameter np &&
                np.Name.SimpleName.Equals("with_itemindex", StringComparison.OrdinalIgnoreCase))
                return (np.Expression as NameReference)?.SimpleName ?? np.Expression.ToString().Trim();
        }
        return null;
    }

    /// <summary>Maps an mv-expand `to typeof(T)` clause to the SQL cast type (null = no cast / dynamic).</summary>
    private static string? MvExpandCastType(MvExpandExpression mve)
    {
        var t = mve.ToTypeOf?.TypeOf?.ToString();
        if (string.IsNullOrEmpty(t)) return null;
        var m = System.Text.RegularExpressions.Regex.Match(t, @"typeof\s*\(\s*(\w+)",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        return (m.Success ? m.Groups[1].Value.ToLowerInvariant() : null) switch
        {
            "long" => "BIGINT",
            "int" => "INTEGER",
            "real" or "double" => "DOUBLE",
            "decimal" => "DECIMAL",
            "string" => "VARCHAR",
            "bool" or "boolean" => "BOOLEAN",
            "datetime" => "TIMESTAMP",
            "timespan" => "INTERVAL",
            "guid" => "UUID",
            _ => null,
        };
    }

    internal string ApplyMvApply(string leftSql, MvApplyOperator mvApply)
    {
        if (mvApply.Expressions.Count != 1)
            throw new NotSupportedException("mv-apply with multiple expressions is not supported");

        if (mvApply.Expressions[0].Element is not MvApplyExpression mae)
            throw new NotSupportedException("Unexpected mv-apply expression type");

        // mv-apply alias = expr on (...) — the LHS name is the column inside the subquery;
        // the RHS is the source expression in the outer scope.
        string sourceAlias = "t";
        string columnName;
        string unnestSource;
        bool elementIsJson;
        if (mae.Expression is SimpleNamedExpression sne)
        {
            columnName = sne.Name.SimpleName;
            var (src, isJson) = ResolveMvSource(sne.Expression, sourceAlias, leftSql);
            unnestSource = CoerceToUnnestable(src, forceJsonCoerce: isJson);
            elementIsJson = isJson;
        }
        else
        {
            var (src, isJson) = ResolveMvSource(mae.Expression, sourceAlias, leftSql);
            columnName = mae.Expression is NameReference nrc ? nrc.SimpleName : mae.Expression.ToString().Trim();
            unnestSource = CoerceToUnnestable(src, forceJsonCoerce: isJson);
            elementIsJson = isJson;
        }

        // The unnested element column is a JSON value when the source was JSON; mark it so array
        // functions inside the on-subquery (e.g. array_sum(row)) coerce it. Mark BEFORE converting
        // the subquery so the marking is visible while it is translated.
        if (elementIsJson) Expr.MarkJsonColumn(columnName);

        var subquerySql = Converter.ConvertNode(mvApply.Subquery.Expression);

        var fromSql = ExtractFromAsRelation(leftSql);
        var aliasedFrom = $"{fromSql} AS {sourceAlias}";

        var unnestAlias = "u";
        // KQL mv-apply expands each source row's array into a SUBTABLE and runs the subquery ONCE over the
        // whole subtable (so `summarize` collapses to one row per source row, `extend` yields one row per
        // element). The UNNEST therefore lives INSIDE the LATERAL as the subquery's virtual source — not as
        // an outer CROSS JOIN (which would run the subquery per element). The subquery's implicit source is
        // named after the element column; it emits FROM {columnName} or FROM (SELECT *), patched here.
        var virtualSource = $"(SELECT {unnestAlias}.value AS {columnName} FROM UNNEST({unnestSource}) AS {unnestAlias}(value))";
        var patched = subquerySql
            .Replace($"FROM {columnName}", $"FROM {virtualSource}")
            .Replace("FROM (SELECT *)", $"FROM {virtualSource}");
        return $"SELECT {sourceAlias}.*, _sub.* FROM {aliasedFrom}, LATERAL ({patched}) AS _sub";
    }

    internal string ApplyTopHitters(string leftSql, TopHittersOperator topHitters)
    {
        var count = Expr.ConvertExpression(topHitters.Expression);
        var column = Expr.ConvertExpression(topHitters.OfExpression);
        var fromSql = ExtractFrom(leftSql);

        // Determine the per-group weight and the output metric column name.
        //  - plain `top-hitters N of g`        → weight = COUNT(*),   column = approximate_count_<g>
        //  - `top-hitters N of g by v`         → weight = SUM(v),     column = approximate_sum_<v>
        // Kusto's `by` clause takes a plain numeric column (it is the summed weight); aggregate calls
        // like count()/sum() are rejected by Kusto, so we never group on the raw column here (which was
        // the root cause of "column must appear in the GROUP BY clause").
        string weightExpr, metricAlias;
        if (topHitters.ByClause is TopHittersByClause byClause)
        {
            var byExpr = Expr.ConvertExpression(byClause.Expression);
            weightExpr = $"SUM({byExpr})";
            metricAlias = $"approximate_sum_{SimpleNameOf(byClause.Expression) ?? byExpr}";
        }
        else
        {
            weightExpr = "COUNT(*)";
            metricAlias = $"approximate_count_{SimpleNameOf(topHitters.OfExpression) ?? column}";
        }
        metricAlias = ExpressionSqlBuilder.QuoteIdentifierIfReserved(metricAlias);

        // Kusto selects the heaviest N groups, then returns them ordered by weight ASCENDING (ties broken
        // by the group value). Select with weight DESC + group DESC, then re-sort the survivors ascending.
        var ranked = $"SELECT {column}, {weightExpr} AS {metricAlias} FROM {fromSql} " +
                     $"GROUP BY {column} ORDER BY {metricAlias} DESC, {column} DESC LIMIT {count}";
        return $"SELECT * FROM ({ranked}) ORDER BY {metricAlias} ASC, {column} ASC";
    }

    /// <summary>The bare identifier of an expression when it is a simple column reference, else null.</summary>
    private static string? SimpleNameOf(Expression expr) =>
        expr is NameReference nr && !string.IsNullOrEmpty(nr.SimpleName) ? nr.SimpleName : null;

    internal string ApplyTopNested(string leftSql, TopNestedOperator topNested)
    {
        var originalFrom = ExtractFrom(leftSql);

        var selectColumns = new List<string>();   // internal carry-forward names (collision-proof agg aliases)
        var outColumns = new List<string>();       // final projection: maps internal agg names back to Kusto names
        var prevGroupCols = new List<string>();
        string? prevFilteredSql = null;

        // Aggregate columns get a unique internal alias (_tnagg{i}) for all internal use (ranking, joins,
        // carry-forward) and are mapped to their Kusto output name only in the final projection. This avoids
        // a DuckDB case-folding collision when an aggregate's Kusto name differs only in case from an `of`
        // column (e.g. `top-nested of sub by Sub=sum(v)` — DuckDB treats `sub` and `Sub` as the same column,
        // so `ORDER BY Sub` and the final select would otherwise bind to the `sub` group key, not the sum).
        static string AggInternal(int clauseIndex) => $"_tnagg{clauseIndex}";

        // Kusto names a top-nested aggregate column after its explicit name when
        // one is given (e.g. `Cnt=count()` -> `Cnt`), otherwise `aggregated_<subject>`.
        string AggAliasFor(TopNestedClause c)
        {
            // `by <agg> asc|desc` wraps the aggregate in an OrderedExpression — unwrap to find the name.
            var by = c.ByExpression is OrderedExpression obe ? obe.Expression : c.ByExpression;
            if (by is SimpleNamedExpression named)
                return Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(named.Name.SimpleName);
            return $"aggregated_{Expr.ConvertExpression(c.OfExpression)}";
        }

        for (int i = 0; i < topNested.Clauses.Count; i++)
        {
            if (topNested.Clauses[i].Element is not TopNestedClause clause)
                continue;

            // Count (the N in `top-nested N of ...`) is optional; absent => no row limit (all values).
            var count = clause.Expression != null ? Expr.ConvertExpression(clause.Expression) : null;
            var column = Expr.ConvertExpression(clause.OfExpression);
            var aggExpr = clause.ByExpression;
            // `by <agg> asc|desc` controls the ranking direction (default DESC = "top").
            var direction = "DESC";
            if (aggExpr is OrderedExpression oe)
            {
                direction = string.Equals(oe.Ordering?.ToString().Trim(), "asc", StringComparison.OrdinalIgnoreCase) ? "ASC" : "DESC";
                aggExpr = oe.Expression;
            }

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

            var aggAlias = AggAliasFor(clause);   // Kusto output name (applied only in the final projection)
            var aggInternal = AggInternal(i);     // collision-proof internal name used everywhere upstream
            selectColumns.Add(column);
            selectColumns.Add(aggInternal);
            outColumns.Add(column);
            outColumns.Add($"{aggInternal} AS {aggAlias}");

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
                    if (topNested.Clauses[p].Element is TopNestedClause)
                        prevAggCols.Add($"_prev.{AggInternal(p)}");
                }
            }
            var allSelectCols = groupColExprs.Concat(prevAggCols).Append($"{aggSql} AS {aggInternal}");
            var innerSelect = $"SELECT {string.Join(", ", allSelectCols)} FROM {sourceSql} GROUP BY {string.Join(", ", groupColExprs.Concat(prevAggCols))}";
            var ranked = $"SELECT *, ROW_NUMBER() OVER ({partitionBy}ORDER BY {aggInternal} {direction}) AS _rn{i} FROM ({innerSelect})";
            prevFilteredSql = count != null
                ? $"SELECT {string.Join(", ", selectColumns)} FROM ({ranked}) WHERE _rn{i} <= {count}"
                : $"SELECT {string.Join(", ", selectColumns)} FROM ({ranked})";

            prevGroupCols.Add(column);
        }

        // Map the internal agg aliases to their Kusto output names in one final projection. The `of` columns
        // and the (distinct) internal agg names never collide case-insensitively, so this binds unambiguously.
        return prevFilteredSql == null ? leftSql : $"SELECT {string.Join(", ", outColumns)} FROM ({prevFilteredSql})";
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

        // serialize col = row_cumsum(v, restart) emits reset-group markers — hoist them like extend does.
        if (HasResetGroupMarker(joined))
            return HoistResetGroups(leftSql, joined);

        if (HasLimit(leftSql))
            return $"SELECT *, {joined} FROM ({leftSql})";

        return AppendToSelectStar(leftSql, joined);
    }

    internal string ApplyGetSchema(string leftSql)
    {
        // Kusto getschema returns one row per column with exactly these columns:
        //   ColumnName | ColumnOrdinal (0-based) | DataType (CLR name) | ColumnType (KQL name)
        // DuckDB's DESCRIBE has a different shape, so wrap it and map the DuckDB type onto KQL/CLR types.
        const string ctCase =
            "CASE " +
            "WHEN column_type IN ('BIGINT','HUGEINT','UBIGINT','UHUGEINT') THEN 'long' " +
            "WHEN column_type IN ('INTEGER','SMALLINT','TINYINT','UINTEGER','USMALLINT','UTINYINT') THEN 'int' " +
            "WHEN column_type IN ('DOUBLE','FLOAT','REAL') THEN 'real' " +
            "WHEN column_type LIKE 'DECIMAL%' THEN 'decimal' " +
            "WHEN column_type IN ('VARCHAR','TEXT','CHAR','BLOB') THEN 'string' " +
            "WHEN column_type = 'BOOLEAN' THEN 'bool' " +
            "WHEN column_type LIKE 'TIMESTAMP%' OR column_type = 'DATE' THEN 'datetime' " +
            "WHEN column_type LIKE 'INTERVAL%' OR column_type LIKE 'TIME%' THEN 'timespan' " +
            "WHEN column_type = 'JSON' THEN 'dynamic' " +
            "WHEN column_type = 'UUID' THEN 'guid' " +
            "WHEN column_type LIKE '%[]' OR column_type LIKE 'STRUCT%' OR column_type LIKE 'MAP%' OR column_type LIKE 'LIST%' THEN 'dynamic' " +
            "ELSE 'string' END";
        const string dtCase =
            "CASE \"ColumnType\" " +
            "WHEN 'long' THEN 'System.Int64' WHEN 'int' THEN 'System.Int32' " +
            "WHEN 'real' THEN 'System.Double' WHEN 'decimal' THEN 'System.Data.SqlTypes.SqlDecimal' " +
            "WHEN 'string' THEN 'System.String' WHEN 'bool' THEN 'System.SByte' " +
            "WHEN 'datetime' THEN 'System.DateTime' WHEN 'timespan' THEN 'System.TimeSpan' " +
            "WHEN 'dynamic' THEN 'System.Object' WHEN 'guid' THEN 'System.Guid' " +
            "ELSE 'System.String' END";

        var inner =
            $"SELECT column_name AS \"ColumnName\", " +
            $"CAST(ROW_NUMBER() OVER () - 1 AS BIGINT) AS \"ColumnOrdinal\", " +
            $"{ctCase} AS \"ColumnType\" " +
            $"FROM (DESCRIBE SELECT * FROM ({leftSql}) LIMIT 0)";

        return $"SELECT \"ColumnName\", \"ColumnOrdinal\", {dtCase} AS \"DataType\", \"ColumnType\" FROM ({inner})";
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
                // Tag JSON/dynamic results so a later bare reference (d[i], array funcs) navigates as JSON.
                if (Expressions.ExpressionSqlBuilder.LooksLikeJsonResult(value))
                    Expr.MarkJsonColumn(name);
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
        // SQL type to force per column (null = let DuckDB infer). A KQL `long` literal that fits in
        // int32 would otherwise be inferred as INTEGER, breaking long arithmetic (overflow) and
        // getschema; pin it to BIGINT so the declared schema is honored.
        var colCastTypes = new List<string?>();
        // Columns declared `dynamic` are rendered as a uniform JSON column (every cell CAST AS JSON),
        // so a datatable mixing object and array values — dynamic({...}) and dynamic([...]) — produces
        // ONE JSON-typed column instead of an irreconcilable (JSON, INTEGER[]) mix that breaks UNNEST.
        var isDynamicCol = new List<bool>();
        foreach (var col in dt.Schema.Columns)
        {
            if (col.Element is NameAndTypeDeclaration nat)
            {
                var cname = nat.Name.SimpleName;
                columnNames.Add(Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(cname));
                // Propagate KQL timespan columns so downstream sum/divide hit the epoch-ms path.
                var typeName = nat.Type?.ToString().Trim();
                if (string.Equals(typeName, "timespan", StringComparison.OrdinalIgnoreCase))
                {
                    Expr.MarkIntervalColumn(cname);
                    colCastTypes.Add(null);
                    isDynamicCol.Add(false);
                }
                else if (string.Equals(typeName, "datetime", StringComparison.OrdinalIgnoreCase))
                {
                    Expr.MarkDateTimeColumn(cname);
                    colCastTypes.Add(null);
                    isDynamicCol.Add(false);
                }
                else if (string.Equals(typeName, "bool", StringComparison.OrdinalIgnoreCase)
                      || string.Equals(typeName, "boolean", StringComparison.OrdinalIgnoreCase))
                {
                    Expr.MarkBoolColumn(cname);
                    colCastTypes.Add(null);
                    isDynamicCol.Add(false);
                }
                // Propagate integer columns so '/' and '%' use KQL's truncating/Euclidean semantics.
                else if (string.Equals(typeName, "long", StringComparison.OrdinalIgnoreCase))
                {
                    Expr.MarkIntegerColumn(cname);
                    colCastTypes.Add("BIGINT");
                    isDynamicCol.Add(false);
                }
                else if (string.Equals(typeName, "int", StringComparison.OrdinalIgnoreCase))
                {
                    Expr.MarkIntegerColumn(cname);
                    colCastTypes.Add(null);
                    isDynamicCol.Add(false);
                }
                // KQL real == double. DuckDB infers decimal-looking literals (1.5) as DECIMAL, which
                // diverges in getschema (decimal vs real) and in division precision; pin to DOUBLE.
                else if (string.Equals(typeName, "real", StringComparison.OrdinalIgnoreCase)
                      || string.Equals(typeName, "double", StringComparison.OrdinalIgnoreCase))
                {
                    colCastTypes.Add("DOUBLE");
                    isDynamicCol.Add(false);
                }
                // dynamic columns become a single uniform JSON column (see isDynamicCol comment above).
                else if (string.Equals(typeName, "dynamic", StringComparison.OrdinalIgnoreCase))
                {
                    Expr.MarkJsonColumn(cname);
                    colCastTypes.Add("JSON");
                    isDynamicCol.Add(true);
                }
                else
                {
                    colCastTypes.Add(null);
                    isDynamicCol.Add(false);
                }
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

        string? CastType(int j) => j < colCastTypes.Count ? colCastTypes[j] : null;

        var rows = new List<string>();

        for (int i = 0; i < values.Count; i += colCount)
        {
            var rowValues = new List<string>();
            for (int j = 0; j < colCount && (i + j) < values.Count; j++)
            {
                var v = Expr.ConvertLiteralValue(values[i + j].Element);
                var ct = CastType(j);
                rowValues.Add(ct is null ? v : $"CAST({v} AS {ct})");
            }
            rows.Add($"({string.Join(", ", rowValues)})");
        }

        // Empty datatable: DuckDB rejects 'VALUES ) AS t(col)'. Emit a typed empty SELECT instead.
        if (rows.Count == 0)
        {
            var cols = string.Join(", ", columnNames.Select((n, j) =>
            {
                var ct = CastType(j);
                return ct is null ? $"NULL AS {n}" : $"CAST(NULL AS {ct}) AS {n}";
            }));
            return $"SELECT {cols} WHERE 1 = 0";
        }

        // When a dynamic (JSON) column is present, project it explicitly as `CAST(col AS JSON) AS col`
        // so the JSON type is visible to downstream JSON-detection (mv-expand's CoerceToUnnestable,
        // array functions) — `SELECT *` would hide the column's JSON-ness behind a bare reference.
        bool IsDyn(int j) => j < isDynamicCol.Count && isDynamicCol[j];
        if (isDynamicCol.Any(d => d))
        {
            var proj = string.Join(", ", columnNames.Select((n, j) =>
                IsDyn(j) ? $"CAST({n} AS JSON) AS {n}" : n));
            return $"SELECT {proj} FROM (VALUES {string.Join(", ", rows)}) AS t({string.Join(", ", columnNames)})";
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
            trimmed.EndsWith(" AS JSON)", StringComparison.OrdinalIgnoreCase) ||
            // Dynamic property/index navigation (d.k / d["k"] / d[i]) yields a json_extract(...) value;
            // it's JSON, not a native LIST, so it must be coerced before UNNEST.
            trimmed.StartsWith("json_extract(", StringComparison.OrdinalIgnoreCase))
        {
            // Kusto mv-expand of a dynamic value:
            //   array      → one row per element
            //   empty []   → zero rows
            //   object bag → one row per top-level property, each a single-key bag {"k":v}
            //   null       → one row holding null
            //   scalar     → one row holding the scalar
            // Every branch yields JSON[] so the UNNEST column type is uniform.
            return $"CASE " +
                   $"WHEN {source} IS NULL THEN CAST([NULL] AS JSON[]) " +
                   $"WHEN json_type({source}) = 'ARRAY' THEN CAST({source} AS JSON[]) " +
                   $"WHEN json_type({source}) = 'OBJECT' THEN list_transform(json_keys({source}), " +
                       $"lambda k: json_object(k, json_extract({source}, '$.\"' || k || '\"'))) " +
                   $"ELSE CAST([{source}] AS JSON[]) END";
        }
        return source;
    }

    // JSON-producing SQL builtins: when a column is `extend`ed/projected from one of these (or a
    // ::JSON / CAST(.. AS JSON) cast), the column is JSON-typed, so mv-expand must coerce it to a
    // list before UNNEST. (todynamic/parse_json → CAST AS JSON; d.field → json_extract; array funcs
    // over dynamic → TO_JSON(...); bag ops → json_object/json_merge_patch.)
    private static readonly string[] JsonProducerPrefixes =
    {
        "json_extract(", "json_object(", "json_merge_patch(", "to_json(", "json_array(",
        "json_group_array(", "json_quote(", "json_group_object(",
    };

    // Detects whether `columnName` is defined as a JSON-typed expression in the upstream SQL. Finds the
    // column's `AS <name>` alias, walks backward (paren-aware) to the start of its defining expression,
    // then classifies that expression as JSON or not.
    private static bool ColumnDefinedAsJson(string leftSql, string columnName)
    {
        foreach (var (start, end) in FindAliasDefinitions(leftSql, columnName))
        {
            var expr = leftSql.Substring(start, end - start).Trim();
            if (expr.EndsWith("::JSON", StringComparison.OrdinalIgnoreCase) ||
                expr.EndsWith(" AS JSON)", StringComparison.OrdinalIgnoreCase))
                return true;
            var lower = expr.TrimStart('(').TrimStart();
            foreach (var p in JsonProducerPrefixes)
                if (lower.StartsWith(p, StringComparison.OrdinalIgnoreCase))
                    return true;
        }
        return false;
    }

    // Yields (start,end) substring ranges of the defining expression for each `AS columnName` alias
    // in the SQL (the text between the previous top-level boundary — comma or SELECT — and the `AS`).
    private static IEnumerable<(int start, int end)> FindAliasDefinitions(string sql, string columnName)
    {
        int search = 0;
        while (true)
        {
            int asIdx = IndexOfAlias(sql, columnName, search);
            if (asIdx < 0) yield break;
            search = asIdx + 1;

            // Walk backward from just before " AS" to the start of the defining expression.
            int i = asIdx - 1;
            while (i >= 0 && sql[i] == ' ') i--;
            int exprEnd = i + 1;
            int depth = 0;
            for (; i >= 0; i--)
            {
                char c = sql[i];
                if (c == ')') depth++;
                else if (c == '(')
                {
                    if (depth == 0) break;   // start of enclosing SELECT/paren
                    depth--;
                }
                else if (c == ',' && depth == 0) break;  // previous column boundary
            }
            int exprStart = i + 1;
            // Skip a leading "SELECT " / "SELECT * EXCLUDE(..)," is already handled by the comma break.
            yield return (exprStart, exprEnd);
        }
    }

    // Finds the index of " AS <columnName>" (optionally quoted), as a whole token, from `from`.
    private static int IndexOfAlias(string sql, string columnName, int from)
    {
        foreach (var form in new[] { $" AS {columnName}", $" AS \"{columnName}\"" })
        {
            int idx = sql.IndexOf(form, from, StringComparison.Ordinal);
            while (idx >= 0)
            {
                int afterPos = idx + form.Length;
                char next = afterPos < sql.Length ? sql[afterPos] : '\0';
                if (next is ' ' or ',' or ')' or '\0' or '\n')
                    return idx;
                idx = sql.IndexOf(form, idx + 1, StringComparison.Ordinal);
            }
        }
        return -1;
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
