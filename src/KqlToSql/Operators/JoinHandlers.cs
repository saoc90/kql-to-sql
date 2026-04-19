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

    internal string ApplyJoin(string leftSql, JoinOperator join, Expression? leftExpression = null)
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

        // KQL join semantics: preserve left duplicate keys; any R column whose name also
        // appears on L becomes <name>1 on the output. If both sides are enumerable we emit
        // per-column aliases so later `| project Value1` works; otherwise fall back to
        // `SELECT L.*, R.* EXCLUDE (keys)` (DuckDB) or `L.*, R.*` (dialects without EXCLUDE).
        var leftCols = TryEnumerateColumns(leftExpression);
        var rightCols = TryEnumerateColumns(join.Expression);
        var keySet = new HashSet<string>(leftKeys.Select(UnquoteIdent), StringComparer.OrdinalIgnoreCase);
        var selectClause = BuildJoinSelectClause(leftCols, rightCols, keySet);
        if (selectClause == null)
        {
            var rightColumns = Dialect.SelectExclude(leftKeys.ToArray());
            selectClause = rightColumns.Contains("/*")
                ? "*"  // Dialect doesn't support EXCLUDE (e.g., PGlite) — fall back to SELECT *
                : $"L.*, R.{rightColumns}";
        }

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

    internal string ApplyLookup(string leftSql, LookupOperator lookup, Expression? leftExpression = null)
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

        var leftCols = TryEnumerateColumns(leftExpression);
        var rightCols = TryEnumerateColumns(lookup.Expression);
        var keySet = new HashSet<string>(leftKeys.Select(UnquoteIdent), StringComparer.OrdinalIgnoreCase);
        var selectClause = BuildJoinSelectClause(leftCols, rightCols, keySet);
        if (selectClause == null)
        {
            var rightColumns = Dialect.SelectExclude(leftKeys.ToArray());
            selectClause = rightColumns.Contains("/*")
                ? "*"
                : $"L.*, R.{rightColumns}";
        }

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

        // Use DuckDB 'UNION ALL BY NAME' so column sets can differ — missing columns fill NULL.
        // KQL's union semantics are by-name, not positional.
        return string.Join(" UNION ALL BY NAME ", parts);
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

        // Use DuckDB 'UNION ALL BY NAME' so column sets can differ — missing columns fill NULL.
        // KQL's union semantics are by-name, not positional.
        return string.Join(" UNION ALL BY NAME ", parts);
    }

    // ─── Column enumeration for join-duplicate suffixing ────────────────────────
    // Recursively walks a KQL expression and returns the list of output column names
    // it produces, or null if the shape is unknown (base tables, unresolved functions).
    // The caller falls back to `SELECT L.*, R.* EXCLUDE (keys)` when either side is null.

    private static string UnquoteIdent(string id)
    {
        if (id.Length >= 2 && id[0] == '"' && id[^1] == '"')
            return id.Substring(1, id.Length - 2).Replace("\"\"", "\"");
        return id;
    }

    private string? BuildJoinSelectClause(IReadOnlyList<string>? leftCols, IReadOnlyList<string>? rightCols, HashSet<string> keys)
    {
        if (leftCols == null || rightCols == null) return null;
        var leftSet = new HashSet<string>(leftCols, StringComparer.OrdinalIgnoreCase);
        var parts = new List<string> { "L.*" };
        foreach (var rc in rightCols)
        {
            if (keys.Contains(rc)) continue;  // join key — drop from right side
            var quoted = ExpressionSqlBuilder.QuoteIdentifierIfReserved(rc);
            if (leftSet.Contains(rc))
            {
                var suffixed = ExpressionSqlBuilder.QuoteIdentifierIfReserved(rc + "1");
                parts.Add($"R.{quoted} AS {suffixed}");
            }
            else
            {
                parts.Add($"R.{quoted}");
            }
        }
        return string.Join(", ", parts);
    }

    private IReadOnlyList<string>? TryEnumerateColumns(Expression? expr)
    {
        if (expr == null) return null;
        return EnumerateColumns(expr, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
    }

    private IReadOnlyList<string>? EnumerateColumns(Expression expr, HashSet<string> visiting)
    {
        switch (expr)
        {
            case ParenthesizedExpression pe:
                return EnumerateColumns(pe.Expression, visiting);
            case MaterializeExpression me:
                return EnumerateColumns(me.Expression, visiting);
            case PipeExpression pipe:
            {
                var inputCols = EnumerateColumns(pipe.Expression, visiting);
                return ApplyOperatorToCols(inputCols, pipe.Operator);
            }
            case NameReference nr:
            {
                var name = nr.Name.ToString().Trim();
                if (name.StartsWith("[") && name.EndsWith("]"))
                    name = name.Substring(1, name.Length - 2).Trim('"', '\'');
                if (visiting.Contains(name)) return null;
                if (Converter.TryGetCteExpression(name, out var cteExpr))
                {
                    visiting.Add(name);
                    try { return EnumerateColumns(cteExpr, visiting); }
                    finally { visiting.Remove(name); }
                }
                if (Converter.TryGetWellKnownColumns(name, out var cols))
                    return cols.ToList();
                return null; // base table — unknown schema
            }
            case FunctionCallExpression fce:
            {
                var fname = fce.Name.ToString().Trim();
                if (visiting.Contains(fname)) return null;  // recursion guard
                if (Converter.TryGetUserFunctionBody(fname, out var body) && body.Expression != null)
                {
                    visiting.Add(fname);
                    try { return EnumerateColumns(body.Expression, visiting); }
                    finally { visiting.Remove(fname); }
                }
                return null;
            }
            default:
                return null;
        }
    }

    private IReadOnlyList<string>? ApplyOperatorToCols(IReadOnlyList<string>? input, QueryOperator op)
    {
        switch (op)
        {
            case ProjectOperator proj:
                return ExtractNames(proj.Expressions);
            case ProjectKeepOperator keep:
            {
                var kept = new List<string>();
                foreach (var se in keep.Expressions)
                {
                    if (se.Element is NameReference knr) kept.Add(knr.Name.ToString().Trim());
                    else return null;
                }
                return kept;
            }
            case ProjectAwayOperator away:
            {
                if (input == null) return null;
                var rm = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var se in away.Expressions) rm.Add(se.Element.ToString().Trim());
                return input.Where(c => !rm.Contains(c)).ToList();
            }
            case ProjectRenameOperator ren:
            {
                if (input == null) return null;
                var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var se in ren.Expressions)
                {
                    if (se.Element is SimpleNamedExpression sne && sne.Expression is NameReference old)
                        map[old.Name.ToString().Trim()] = sne.Name.ToString().Trim();
                }
                return input.Select(c => map.TryGetValue(c, out var n) ? n : c).ToList();
            }
            case ExtendOperator ext:
            {
                if (input == null) return null;
                var existing = new HashSet<string>(input, StringComparer.OrdinalIgnoreCase);
                var result = new List<string>(input);
                foreach (var se in ext.Expressions)
                {
                    if (se.Element is SimpleNamedExpression sne)
                    {
                        var n = sne.Name.ToString().Trim();
                        if (!existing.Contains(n)) { result.Add(n); existing.Add(n); }
                    }
                }
                return result;
            }
            case SummarizeOperator sum:
            {
                var cols = new List<string>();
                if (sum.ByClause != null)
                {
                    foreach (var se in sum.ByClause.Expressions)
                    {
                        if (se.Element is SimpleNamedExpression byNamed) cols.Add(byNamed.Name.ToString().Trim());
                        else if (se.Element is NameReference byNr) cols.Add(byNr.Name.ToString().Trim());
                        else return null;
                    }
                }
                foreach (var agg in sum.Aggregates)
                {
                    if (agg.Element is SimpleNamedExpression aggNamed) cols.Add(aggNamed.Name.ToString().Trim());
                    else return null;
                }
                return cols;
            }
            case DistinctOperator dist:
            {
                // `distinct *` (no args) keeps the input column set; `distinct Col1, Col2, …`
                // narrows to those expressions' names (bare NameReference or Name = expr).
                if (dist.Expressions.Count == 0) return input;
                var names = new List<string>();
                foreach (var se in dist.Expressions)
                {
                    if (se.Element is NameReference dnr) names.Add(dnr.Name.ToString().Trim());
                    else if (se.Element is SimpleNamedExpression dsne) names.Add(dsne.Name.ToString().Trim());
                    else return null;
                }
                return names;
            }
            case FilterOperator:
            case SortOperator:
            case TakeOperator:
            case TopOperator:
            case SerializeOperator:
            case AsOperator:
                return input;
            case MvExpandOperator mve:
            {
                // mv-expand keeps the input column set when the target is an existing column.
                // When the target is `name = expr` and name is new, it's added to the set.
                if (input == null) return null;
                var existing = new HashSet<string>(input, StringComparer.OrdinalIgnoreCase);
                var result = new List<string>(input);
                foreach (var se in mve.Expressions)
                {
                    if (se.Element is MvExpandExpression mvx && mvx.Expression is SimpleNamedExpression sne)
                    {
                        var n = sne.Name.ToString().Trim();
                        if (!existing.Contains(n)) { result.Add(n); existing.Add(n); }
                    }
                }
                return result;
            }
            default:
                return null;
        }
    }

    private static List<string>? ExtractNames(SyntaxList<SeparatedElement<Expression>> exprs)
    {
        var names = new List<string>();
        foreach (var se in exprs)
        {
            if (se.Element is SimpleNamedExpression sne)
                names.Add(sne.Name.ToString().Trim());
            else if (se.Element is NameReference nr)
                names.Add(nr.Name.ToString().Trim());
            else
                return null;
        }
        return names;
    }
}
