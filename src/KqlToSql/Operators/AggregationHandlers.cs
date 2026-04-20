using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

internal sealed class AggregationHandlers : OperatorHandlerBase
{
    internal AggregationHandlers(KqlToSqlConverter converter, ExpressionSqlBuilder expr)
        : base(converter, expr) { }

    internal string ApplySummarize(string leftSql, SummarizeOperator summarize)
    {
        var byColumns = summarize.ByClause?.Expressions
            .Select(e => ConvertByExpression(e.Element))
            .ToArray() ?? Array.Empty<(string Select, string Group)>();

        if (summarize.Aggregates.Count == 1 &&
            summarize.Aggregates[0].Element is Expression aggExpr &&
            aggExpr is FunctionCallExpression fce &&
            (fce.IsAny(Aggregates.ArgMax, Aggregates.ArgMin, Aggregates.ArgMax_Deprecated, Aggregates.ArgMin_Deprecated)) &&
            fce.ArgumentList.Expressions.Count == 2 &&
            fce.ArgumentList.Expressions[1].Element is StarExpression)
        {
            var extremumExpr = Expr.ConvertExpression(fce.ArgumentList.Expressions[0].Element);
            var fromSql = ExtractFrom(leftSql);

            var partition = byColumns.Length > 0
                ? $"PARTITION BY {string.Join(", ", byColumns.Select(b => b.Group))} "
                : string.Empty;

            var direction = fce.Is(Aggregates.ArgMin) ? "ASC" : "DESC";

            var qualifyCondition = $"ROW_NUMBER() OVER ({partition}ORDER BY {extremumExpr} {direction}) = 1";
            var innerSql = IsFullyParenthesized(fromSql) ? fromSql.Substring(1, fromSql.Length - 2) : $"SELECT * FROM {fromSql}";
            return Dialect.Qualify(innerSql, qualifyCondition);
        }

        var aggregates = new List<string>();
        // Live Kusto suffixes repeated arg_max/arg_min key aliases: Timestamp, Timestamp1, Timestamp2, ...
        var keyAliasCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var agg in summarize.Aggregates)
        {
            aggregates.AddRange(ConvertAggregate(agg.Element, keyAliasCounts));
        }

        var selectList = string.Join(", ", byColumns.Select(b => b.Select).Concat(aggregates));

        // Use ExtractFromAsRelation so a trailing ORDER BY / LIMIT on leftSql doesn't emit
        // 'FROM T ORDER BY X GROUP BY ALL' (which DuckDB rejects).
        var sql = $"SELECT {selectList} FROM {ExtractFromAsRelation(leftSql)}";
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
            var inner = Expr.ConvertExpression(sne.Expression);
            var name = sne.Name.ToString().Trim();
            return ($"{inner} AS {name}", inner);
        }

        if (expr is FunctionCallExpression fce &&
            (fce.Is(Functions.Bin) || fce.Is(Functions.BinAt)) &&
            fce.ArgumentList.Expressions.Count > 0 && fce.ArgumentList.Expressions[0].Element is NameReference nr)
        {
            var inner = Expr.ConvertExpression(fce);
            var name = nr.Name.SimpleName;
            return ($"{inner} AS {name}", inner);
        }

        // Bare path (DataMetadata.Section) — auto-name as KQL does.
        var synthesized = SynthesizePathAliasForBy(expr);
        var exp = Expr.ConvertExpression(expr);
        if (synthesized != null)
            return ($"{exp} AS {synthesized}", exp);
        return (exp, exp);
    }

    private static string? SynthesizePathAliasForBy(Expression expr)
    {
        var segments = new List<string>();
        Expression? current = expr;
        while (current is PathExpression pe)
        {
            segments.Insert(0, pe.Selector.ToString().Trim());
            current = pe.Expression;
        }
        if (segments.Count == 0) return null;
        if (current is NameReference baseRef)
            segments.Insert(0, baseRef.Name.ToString().Trim());
        else
            return null;
        return Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(string.Join("_", segments));
    }

    internal IEnumerable<string> ConvertAggregate(SyntaxNode node)
        => ConvertAggregate(node, null);

    internal IEnumerable<string> ConvertAggregate(SyntaxNode node, Dictionary<string, int>? keyAliasCounts)
    {
        string? alias = null;
        string[]? compoundAliases = null;
        Expression expr;
        if (node is SimpleNamedExpression sne)
        {
            alias = Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(sne.Name.ToString().Trim());
            expr = sne.Expression;
        }
        else if (node is CompoundNamedExpression cne)
        {
            // KQL: (name1, name2, ...) = arg_max/arg_min(key, v1, v2, ...)
            //   Each LHS name becomes the output alias of the corresponding aggregate result:
            //   name1 for the key, name2+ for each value arg.
            compoundAliases = cne.Names.Names
                .Select(n => Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(n.Element.Name.SimpleName))
                .ToArray();
            expr = cne.Expression;
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
            var name = fce.Name.SimpleName.ToLowerInvariant();
            if (fce.IsAny(Aggregates.ArgMax, Aggregates.ArgMin, Aggregates.ArgMax_Deprecated, Aggregates.ArgMin_Deprecated))
            {
                // KQL: arg_max(key, v1, v2=alias, ...) by ...
                //   output includes key (max/min of key) + each value at the extremum row.
                //   Outer alias applies to the key output: (K = arg_max(key, v1))  → key output named K.
                //   Tuple form:  (K, V1, V2) = arg_max(key, v1, v2) → key aliased as K, values as V1/V2.
                var keyNode = fce.ArgumentList.Expressions[0].Element;
                var extremumExpr = Expr.ConvertExpression(keyNode);
                var explicitKeyAlias = compoundAliases is { Length: > 0 } ? compoundAliases[0] : alias;
                var baseKeyAlias = explicitKeyAlias ?? (keyNode is NameReference knr ? knr.Name.SimpleName : name);
                // Live Kusto: repeated auto-named columns in one summarize suffix 2nd/3rd as <name>1, <name>2, ...
                // Explicit outer alias bypasses the counter on the key.
                var keyAlias = SuffixIfCollides(baseKeyAlias, explicitKeyAlias != null, keyAliasCounts);
                var keyAgg = fce.IsAny(Aggregates.ArgMax, Aggregates.ArgMax_Deprecated) ? "MAX" : "MIN";
                var results = new List<string> { $"{keyAgg}({extremumExpr}) AS {keyAlias}" };

                for (int i = 1; i < fce.ArgumentList.Expressions.Count; i++)
                {
                    var argNode = fce.ArgumentList.Expressions[i].Element;
                    string innerExpr;
                    string resultAlias;
                    bool explicitValueAlias = false;
                    if (argNode is SimpleNamedExpression vsne)
                    {
                        innerExpr = Expr.ConvertExpression(vsne.Expression);
                        resultAlias = vsne.Name.SimpleName;
                        explicitValueAlias = true;
                    }
                    else if (compoundAliases is { } aliases && i < aliases.Length)
                    {
                        innerExpr = Expr.ConvertExpression(argNode);
                        resultAlias = aliases[i];
                        explicitValueAlias = true;
                    }
                    else
                    {
                        innerExpr = Expr.ConvertExpression(argNode);
                        // Match live Kusto: non-key arg auto-name is the inner identifier
                        // (e.g. todouble(Value) → Value, toreal(X) → X, bare V → V).
                        resultAlias = TryGetInnerIdentifier(argNode) ?? $"{name}_{i}";
                    }
                    resultAlias = SuffixIfCollides(resultAlias, explicitValueAlias, keyAliasCounts);
                    results.Add($"{name.ToUpperInvariant()}({innerExpr}, {extremumExpr}) AS {resultAlias}");
                }
                return results;
            }

            var args = fce.ArgumentList.Expressions.Select(a => Expr.ConvertExpression(a.Element)).ToArray();

            if (fce.Is(Aggregates.Percentiles))
            {
                var baseName = alias ?? (fce.ArgumentList.Expressions[0].Element is NameReference nr ? nr.Name.SimpleName : "expr");
                var results = new List<string>();
                for (int i = 1; i < args.Length; i++)
                {
                    var p = args[i];
                    var resultAlias = i == 1 && alias != null ? alias : $"percentiles_{p}_{baseName}";
                    results.Add($"quantile_cont({args[0]}, {p} / 100.0) AS {resultAlias}");
                }
                return results;
            }

            if (fce.Is(Aggregates.PercentilesW))
            {
                var baseName = alias ?? (fce.ArgumentList.Expressions[0].Element is NameReference nr ? nr.Name.SimpleName : "expr");
                var results = new List<string>();
                for (int i = 2; i < args.Length; i++)
                {
                    var p = args[i];
                    var resultAlias = i == 2 && alias != null ? alias : $"percentilesw_{p}_{baseName}";
                    results.Add($"quantile_cont({args[0]}, {p} / 100.0) AS {resultAlias}");
                }
                return results;
            }

            if (fce.Is(Aggregates.PercentilesArray))
            {
                var percentileList = string.Join(", ", args.Skip(1).Select(p => $"{p} / 100.0"));
                alias ??= "percentiles_array";
                return new[] { $"quantile_cont({args[0]}, [{percentileList}]) AS {alias}" };
            }

            if (fce.Is(Aggregates.PercentilesWArray))
            {
                var percentileList = string.Join(", ", args.Skip(2).Select(p => $"{p} / 100.0"));
                alias ??= "percentilesw_array";
                return new[] { $"quantile_cont({args[0]}, [{percentileList}]) AS {alias}" };
            }

            alias ??= DeriveAutoAlias(fce, name, args);

            // When summing an interval-typed expression, rewrite to epoch-ms math so DuckDB's
            // SUM (which rejects INTERVAL) operates on milliseconds and the alias carries back
            // as INTERVAL. Matches:
            //   sum(Duration)                       — bare identifier tagged interval
            //   sum(Duration * WaterfallSign)       — interval column × numeric → interval
            //   sum(ABS(Duration))                  — ABS(interval) → interval
            //   sum(case(..., timespan, ...))       — already handled by dialect (contains INTERVAL literal)
            if (fce.Is(Aggregates.Sum) && args.Length == 1 && Expr.IsIntervalExpression(args[0]))
            {
                var ms = $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + ({args[0]}) AS TIMESTAMP))";
                Expr.MarkIntervalColumn(alias.Trim('"'));
                return new[] { $"((SUM({ms})) * INTERVAL '1 millisecond') AS {alias}" };
            }
            if (fce.Is(Aggregates.SumIf) && args.Length == 2 && Expr.IsIntervalExpression(args[0]))
            {
                var ms = $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + ({args[0]}) AS TIMESTAMP))";
                Expr.MarkIntervalColumn(alias.Trim('"'));
                return new[] { $"((SUM({ms}) FILTER (WHERE {args[1]})) * INTERVAL '1 millisecond') AS {alias}" };
            }

            var sqlFunc = Dialect.TryTranslateAggregate(name, args);
            if (sqlFunc != null)
            {
                // If the dialect wrapped an interval-producing aggregate (e.g. sum/sumif over a
                // timespan case), record the alias so a downstream `sum(<alias>)` picks up the
                // interval-typed path rather than emitting plain SUM over INTERVAL.
                if (Expr.IsIntervalExpression(sqlFunc))
                    Expr.MarkIntervalColumn(alias.Trim('"'));
                return new[] { $"{sqlFunc} AS {alias}" };
            }

            // Not a known aggregate — try as scalar expression (e.g. datetime_diff wrapping aggregates)
            var scalarResult = Dialect.TryTranslateFunction(name, args);
            if (scalarResult != null || Expr.IsKnownScalarFunction(name))
            {
                var scalarSql = Expr.ConvertExpression(expr);
                alias ??= name;
                return new[] { $"{scalarSql} AS {alias}" };
            }

            throw new NotSupportedException($"Unsupported aggregate function {name}");
        }

        // Non-function expression in summarize (e.g. arithmetic on aggregates)
        var exprSql2 = Expr.ConvertExpression(expr);
        if (alias != null && Expr.IsIntervalExpression(exprSql2))
            Expr.MarkIntervalColumn(alias.Trim('"'));
        return new[] { $"{exprSql2} AS {alias ?? "expr"}" };
    }

    private static string SuffixIfCollides(string baseAlias, bool isExplicit, Dictionary<string, int>? counts)
    {
        // Explicit aliases (user-chosen) bypass the auto-suffix counter — user's choice wins even on collision.
        if (counts == null || isExplicit) return baseAlias;
        counts.TryGetValue(baseAlias, out var n);
        counts[baseAlias] = n + 1;
        return n == 0 ? baseAlias : $"{baseAlias}{n}";
    }

    private string DeriveAutoAlias(FunctionCallExpression fce, string name, string[] args)
    {
        // Match real Kusto's auto-naming (verified against a live cluster):
        //   count() / countif(..)          → count_ / countif_
        //   sum(x)/avg(x)/dcount(x)/...    → <name>_<col>
        //   percentile(x, 50)              → percentile_x_50   (col before percentile)
        //   make_list(x) / make_set(x)     → list_x / set_x    (strip 'make_' prefix)
        //   take_any(x) / take_anyif(x,..) → x                 (inner identifier)
        if (fce.Is(Aggregates.Count))   return "count_";
        if (fce.Is(Aggregates.CountIf)) return "countif_";
        if (fce.Is(Aggregates.Percentile))  return $"percentile_{SafeAliasPart(args[0])}_{args[1]}";
        if (fce.Is(Aggregates.PercentileW)) return $"percentilew_{SafeAliasPart(args[0])}_{args[2]}";
        if (fce.IsAny(Aggregates.MakeList, Aggregates.MakeListIf, Aggregates.MakeListWithNulls, Aggregates.MakeList_Deprecated))
            return $"list_{SafeAliasPart(args[0])}";
        if (fce.IsAny(Aggregates.MakeSet, Aggregates.MakeSetIf, Aggregates.MakeSet_Deprecated))
            return $"set_{SafeAliasPart(args[0])}";
        if (fce.IsAny(Aggregates.MakeBag, Aggregates.MakeBagIf))
            return $"bag_{SafeAliasPart(args[0])}";
        if (fce.IsAny(Aggregates.TakeAny, Aggregates.TakeAnyIf) && fce.ArgumentList.Expressions.Count > 0)
        {
            var inner = TryGetInnerIdentifier(fce.ArgumentList.Expressions[0].Element);
            if (inner != null) return inner;
        }
        if (args.Length > 0) return $"{name}_{SafeAliasPart(args[0])}";
        return name;
    }

    private static string? TryGetInnerIdentifier(SyntaxNode node)
    {
        // Drill through single-arg conversion wrappers (toreal/todouble/tostring/...) to the
        // inner identifier — same rule live Kusto uses to auto-name arg_max/arg_min value args.
        SyntaxNode? current = node;
        while (current is FunctionCallExpression fce && fce.ArgumentList.Expressions.Count == 1)
        {
            if (fce.IsAny(Functions.ToString, Functions.ToReal, Functions.ToDouble,
                          Functions.ToInt, Functions.ToLong, Functions.ToBool,
                          Functions.ToDateTime, Functions.ToDynamic_, Functions.ParseJson))
                current = fce.ArgumentList.Expressions[0].Element;
            else break;
        }
        if (current is NameReference nr)
            return Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(nr.Name.SimpleName);
        return null;
    }

    private static bool IsBareIdentifier(string sql)
    {
        if (string.IsNullOrEmpty(sql)) return false;
        foreach (var c in sql)
            if (!(char.IsLetterOrDigit(c) || c == '_')) return false;
        return char.IsLetter(sql[0]) || sql[0] == '_';
    }

    private static string SafeAliasPart(string sqlFragment)
    {
        // Aliases must be identifier-safe. When the argument is a function call / expression,
        // strip to word characters so e.g. 'STRING_SPLIT(Category, ''-'')' becomes 'STRING_SPLIT_Category'.
        var sb = new System.Text.StringBuilder();
        foreach (var c in sqlFragment)
            sb.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        var s = sb.ToString().Trim('_');
        while (s.Contains("__")) s = s.Replace("__", "_");
        return string.IsNullOrEmpty(s) ? "expr" : s;
    }
}
