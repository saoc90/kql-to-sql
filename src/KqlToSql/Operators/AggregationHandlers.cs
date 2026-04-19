using System;
using System.Collections.Generic;
using System.Linq;
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
            (fce.Name.ToString().Trim().Equals("arg_max", StringComparison.OrdinalIgnoreCase) ||
             fce.Name.ToString().Trim().Equals("arg_min", StringComparison.OrdinalIgnoreCase)) &&
            fce.ArgumentList.Expressions.Count == 2 &&
            fce.ArgumentList.Expressions[1].Element is StarExpression)
        {
            var extremumExpr = Expr.ConvertExpression(fce.ArgumentList.Expressions[0].Element);
            var fromSql = ExtractFrom(leftSql);

            var partition = byColumns.Length > 0
                ? $"PARTITION BY {string.Join(", ", byColumns.Select(b => b.Group))} "
                : string.Empty;

            var direction = fce.Name.ToString().Trim().Equals("arg_min", StringComparison.OrdinalIgnoreCase)
                ? "ASC" : "DESC";

            var qualifyCondition = $"ROW_NUMBER() OVER ({partition}ORDER BY {extremumExpr} {direction}) = 1";
            var innerSql = IsFullyParenthesized(fromSql) ? fromSql.Substring(1, fromSql.Length - 2) : $"SELECT * FROM {fromSql}";
            return Dialect.Qualify(innerSql, qualifyCondition);
        }

        var aggregates = new List<string>();
        foreach (var agg in summarize.Aggregates)
        {
            aggregates.AddRange(ConvertAggregate(agg.Element));
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

        if (expr is FunctionCallExpression fce && fce.Name.ToString().Trim().Equals("bin", StringComparison.OrdinalIgnoreCase) &&
            fce.ArgumentList.Expressions.Count > 0 && fce.ArgumentList.Expressions[0].Element is NameReference nr)
        {
            var inner = Expr.ConvertBin(fce, null, null);
            var name = nr.Name.ToString().Trim();
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
    {
        string? alias = null;
        Expression expr;
        if (node is SimpleNamedExpression sne)
        {
            alias = Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(sne.Name.ToString().Trim());
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
                // KQL: arg_max(key, v1, v2=alias, ...) by ...
                //   output includes key (max/min of key) + each value at the extremum row.
                // Outer alias applies to the key output: (K = arg_max(key, v1))  → key output named K.
                var keyNode = fce.ArgumentList.Expressions[0].Element;
                var extremumExpr = Expr.ConvertExpression(keyNode);
                var keyAlias = alias ?? (keyNode is NameReference knr ? knr.Name.ToString().Trim() : name);
                var keyAgg = name == "arg_max" ? "MAX" : "MIN";
                var results = new List<string> { $"{keyAgg}({extremumExpr}) AS {keyAlias}" };

                for (int i = 1; i < fce.ArgumentList.Expressions.Count; i++)
                {
                    var argNode = fce.ArgumentList.Expressions[i].Element;
                    string innerExpr;
                    string resultAlias;
                    if (argNode is SimpleNamedExpression vsne)
                    {
                        innerExpr = Expr.ConvertExpression(vsne.Expression);
                        resultAlias = vsne.Name.ToString().Trim();
                    }
                    else if (argNode is NameReference vnr)
                    {
                        innerExpr = Expr.ConvertExpression(argNode);
                        resultAlias = vnr.Name.ToString().Trim();
                    }
                    else
                    {
                        innerExpr = Expr.ConvertExpression(argNode);
                        resultAlias = $"{name}_{i}";
                    }
                    results.Add($"{name.ToUpperInvariant()}({innerExpr}, {extremumExpr}) AS {resultAlias}");
                }
                return results;
            }

            var args = fce.ArgumentList.Expressions.Select(a => Expr.ConvertExpression(a.Element)).ToArray();

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

            if (name == "percentiles_array")
            {
                var percentileList = string.Join(", ", args.Skip(1).Select(p => $"{p} / 100.0"));
                alias ??= "percentiles_array";
                return new[] { $"quantile_cont({args[0]}, [{percentileList}]) AS {alias}" };
            }

            if (name == "percentilesw_array")
            {
                var percentileList = string.Join(", ", args.Skip(2).Select(p => $"{p} / 100.0"));
                alias ??= "percentilesw_array";
                return new[] { $"quantile_cont({args[0]}, [{percentileList}]) AS {alias}" };
            }

            alias ??= name switch
            {
                "count" => "count",
                "countif" => "countif",
                "percentile" => $"percentile_{args[1]}_{SafeAliasPart(args[0])}",
                "percentilew" => $"percentilew_{args[2]}_{SafeAliasPart(args[0])}",
                _ when args.Length > 0 => $"{name}_{SafeAliasPart(args[0])}",
                _ => name
            };

            // When summing a column we recorded as interval-typed upstream, rewrite to epoch-ms math
            // before dispatching to the dialect so the emission bypasses the plain SUM path.
            if (name == "sum" && args.Length == 1 && IsBareIdentifier(args[0]) && Expr.IsIntervalColumn(args[0]))
            {
                var ms = $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + {args[0]} AS TIMESTAMP))";
                return new[] { $"((SUM({ms})) * INTERVAL '1 millisecond') AS {alias}" };
            }
            if (name == "sumif" && args.Length == 2 && IsBareIdentifier(args[0]) && Expr.IsIntervalColumn(args[0]))
            {
                var ms = $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + {args[0]} AS TIMESTAMP))";
                return new[] { $"((SUM({ms}) FILTER (WHERE {args[1]})) * INTERVAL '1 millisecond') AS {alias}" };
            }

            var sqlFunc = Dialect.TryTranslateAggregate(name, args);
            if (sqlFunc != null)
            {
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
        return new[] { $"{exprSql2} AS {alias ?? "expr"}" };
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
