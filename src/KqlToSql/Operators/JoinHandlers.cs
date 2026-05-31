using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language;
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
        var kindParam = join.Parameters.FirstOrDefault(p => p.Name.SimpleName.Equals("kind", StringComparison.OrdinalIgnoreCase));
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
                var name = ExpressionSqlBuilder.QuoteIdentifierIfReserved(nr.SimpleName);
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
            // Schema not enumerable on one side — rename the right-side join keys to `<key>1`
            // so downstream references to the suffixed form bind. Using EXCLUDE here instead
            // would drop the keys entirely and break queries that project them.
            var renames = leftKeys.Select(k =>
                $"{k} AS {ExpressionSqlBuilder.QuoteIdentifierIfReserved(UnquoteIdent(k) + "1")}").ToArray();
            var rightColumns = Dialect.SelectRename(renames);
            selectClause = rightColumns.Contains("/*")
                ? "*"
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
                var name = ExpressionSqlBuilder.QuoteIdentifierIfReserved(nr.SimpleName);
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
        var kindParam = lookup.Parameters.FirstOrDefault(p => p.Name.SimpleName.Equals("kind", StringComparison.OrdinalIgnoreCase));
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
        // Lookup drops the RIGHT-side join-key columns (Kusto keeps only the left copy),
        // unlike join which suffixes them. Track the right key names separately.
        var rightKeys = new List<string>();
        foreach (var se in onClause.Expressions)
        {
            var expr = se.Element;
            if (expr is NameReference nr)
            {
                var name = ExpressionSqlBuilder.QuoteIdentifierIfReserved(nr.SimpleName);
                leftKeys.Add(name);
                rightKeys.Add(name);
                conditions.Add($"L.{name} = R.{name}");
            }
            else if (expr is BinaryExpression be && be.Kind == SyntaxKind.EqualExpression)
            {
                var left = Expr.ConvertExpression(be.Left, "L", "R");
                var right = Expr.ConvertExpression(be.Right, "L", "R");
                conditions.Add($"{left} = {right}");
                leftKeys.Add(ExpressionSqlBuilder.QuoteIdentifierIfReserved(ExpressionSqlBuilder.ExtractLeftKey(be.Left)));
                rightKeys.Add(ExpressionSqlBuilder.QuoteIdentifierIfReserved(ExpressionSqlBuilder.ExtractRightKey(be.Right)));
            }
            else
            {
                throw new NotSupportedException("Unsupported lookup condition");
            }
        }

        var rightSql = Converter.ConvertNode(lookup.Expression);

        var leftCols = TryEnumerateColumns(leftExpression);
        var rightCols = TryEnumerateColumns(lookup.Expression);
        var rightKeySet = new HashSet<string>(rightKeys.Select(UnquoteIdent), StringComparer.OrdinalIgnoreCase);
        var selectClause = BuildLookupSelectClause(leftCols, rightCols, rightKeySet);
        if (selectClause == null)
        {
            // Drop the RIGHT-side key columns (not the left keys): `R.* EXCLUDE (<rightKeys>)`.
            var rightColumns = Dialect.SelectExclude(rightKeys.ToArray());
            selectClause = rightColumns.Contains("/*")
                ? "*"
                : $"L.*, R.{rightColumns}";
        }

        return $"SELECT {selectClause} FROM {UnwrapFrom(leftSql)} AS L {joinType} {UnwrapFrom(rightSql)} AS R ON {string.Join(" AND ", conditions)}";
    }

    internal string ApplyUnion(string leftSql, UnionOperator union, Expression? leftExpression)
    {
        var withSourceParam = union.Parameters.FirstOrDefault(p => p.Name.SimpleName.Equals("withsource", StringComparison.OrdinalIgnoreCase));
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
            AppendUnionExpression(expr.Element, sourceColumn, parts);

        // Use DuckDB 'UNION ALL BY NAME' so column sets can differ — missing columns fill NULL.
        // KQL's union semantics are by-name, not positional.
        return string.Join(" UNION ALL BY NAME ", parts);
    }

    internal string ConvertUnion(UnionOperator union)
    {
        var withSourceParam = union.Parameters.FirstOrDefault(p => p.Name.SimpleName.Equals("withsource", StringComparison.OrdinalIgnoreCase));
        string? sourceColumn = withSourceParam != null ? withSourceParam.Expression.ToString().Trim() : null;

        var parts = new List<string>();
        foreach (var expr in union.Expressions)
            AppendUnionExpression(expr.Element, sourceColumn, parts);

        // Use DuckDB 'UNION ALL BY NAME' so column sets can differ — missing columns fill NULL.
        // KQL's union semantics are by-name, not positional.
        return string.Join(" UNION ALL BY NAME ", parts);
    }

    /// <summary>Converts one `union` operand into one-or-more SQL parts. A wildcard table-set
    /// reference (`union Table_States*`, parsed as a NameReference whose Name is a WildcardedName)
    /// is expanded — verified against live Kusto — to a UNION ALL of every in-scope let-defined
    /// view whose name matches the pattern (case-sensitive, `*` = zero-or-more chars). A plain
    /// (non-wildcard) operand keeps the original single-part behavior.</summary>
    private void AppendUnionExpression(Expression element, string? sourceColumn, List<string> parts)
    {
        if (element is NameReference nr && nr.Name is WildcardedName wild)
        {
            foreach (var cteName in Converter.CteNames)
            {
                if (!WildcardMatches(wild.SimpleName, cteName))
                    continue;
                var quoted = ExpressionSqlBuilder.QuoteIdentifierIfReserved(cteName);
                var matchSql = $"SELECT * FROM {quoted}";
                if (sourceColumn != null)
                    matchSql = $"SELECT *, '{cteName}' AS {sourceColumn} FROM ({matchSql})";
                parts.Add($"({matchSql})");
            }
            return;
        }

        var sql = Converter.ConvertNode(element);
        if (sourceColumn != null)
        {
            var name = element.ToString().Trim();
            sql = $"SELECT *, '{name}' AS {sourceColumn} FROM ({sql})";
        }
        parts.Add($"({sql})");
    }

    /// <summary>Glob match for a Kusto wildcard table pattern. `*` matches zero-or-more characters;
    /// the literal segments between `*`s must appear in order. Case-sensitive, matching Kusto's
    /// case-sensitive entity-name resolution (verified live: a lowercase pattern matches nothing).</summary>
    private static bool WildcardMatches(string pattern, string candidate)
    {
        var segments = pattern.Split('*');
        int pos = 0;
        for (int s = 0; s < segments.Length; s++)
        {
            var seg = segments[s];
            if (seg.Length == 0) continue;
            if (s == 0)
            {
                if (!candidate.StartsWith(seg, StringComparison.Ordinal)) return false;
                pos = seg.Length;
            }
            else if (s == segments.Length - 1)
            {
                if (!candidate.EndsWith(seg, StringComparison.Ordinal)) return false;
                if (candidate.Length - seg.Length < pos) return false;
            }
            else
            {
                int idx = candidate.IndexOf(seg, pos, StringComparison.Ordinal);
                if (idx < 0) return false;
                pos = idx + seg.Length;
            }
        }
        return true;
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
        // Live Kusto: every join kind (inner, innerunique, leftouter, rightouter, fullouter)
        // keeps both sides' copy of the join key; the right-side copy is suffixed `1` just
        // like any other duplicate column.
        var parts = new List<string> { "L.*" };
        // `used` grows as we alias, so a right column colliding with the left set (or with an
        // already-emitted suffix) takes the NEXT free index. This matches KQL across a chain of
        // joins: a `Ts` column joined 4 times becomes Ts1, Ts2, Ts3, Ts4 (not Ts1 four times).
        var used = new HashSet<string>(leftSet, StringComparer.OrdinalIgnoreCase);
        foreach (var rc in rightCols)
        {
            var quoted = ExpressionSqlBuilder.QuoteIdentifierIfReserved(rc);
            if (used.Contains(rc))
            {
                var name = NextFreeSuffix(rc, used);
                used.Add(name);
                parts.Add($"R.{quoted} AS {ExpressionSqlBuilder.QuoteIdentifierIfReserved(name)}");
            }
            else
            {
                used.Add(rc);
                parts.Add($"R.{quoted}");
            }
        }
        return string.Join(", ", parts);
    }

    /// <summary>The lowest <c>name{n}</c> (n≥1) not already in <paramref name="used"/> — KQL's
    /// duplicate-column suffixing across joins.</summary>
    private static string NextFreeSuffix(string name, HashSet<string> used)
    {
        int n = 1; string cand;
        do { cand = name + n; n++; } while (used.Contains(cand));
        return cand;
    }

    /// <summary>Select clause for `lookup`: keeps every left column, then every right column
    /// EXCEPT the right-side join keys (Kusto keeps only the left copy of a matched key).
    /// Remaining right columns that collide with the left set are suffixed Col1→Col11, matching
    /// Kusto. Returns null when either side's schema is unknown so the caller falls back to
    /// `R.* EXCLUDE (rightKeys)`.</summary>
    private string? BuildLookupSelectClause(IReadOnlyList<string>? leftCols, IReadOnlyList<string>? rightCols, HashSet<string> rightKeys)
    {
        if (leftCols == null || rightCols == null) return null;
        var parts = new List<string> { "L.*" };
        var used = new HashSet<string>(leftCols, StringComparer.OrdinalIgnoreCase);
        foreach (var rc in rightCols)
        {
            if (rightKeys.Contains(rc)) continue; // right-side join key — dropped by lookup
            var quoted = ExpressionSqlBuilder.QuoteIdentifierIfReserved(rc);
            if (used.Contains(rc))
            {
                var name = NextFreeSuffix(rc, used);
                used.Add(name);
                parts.Add($"R.{quoted} AS {ExpressionSqlBuilder.QuoteIdentifierIfReserved(name)}");
            }
            else
            {
                used.Add(rc);
                parts.Add($"R.{quoted}");
            }
        }
        return string.Join(", ", parts);
    }

    /// <summary>Output column names of a join that keeps both sides: left columns, then right columns
    /// with collisions suffixed to the next free index (used both to emit the SELECT and to enumerate
    /// a join's columns for a downstream operator).</summary>
    private static List<string> MergeJoinColumns(IReadOnlyList<string> leftCols, IReadOnlyList<string> rightCols)
    {
        var used = new HashSet<string>(leftCols, StringComparer.OrdinalIgnoreCase);
        var outCols = new List<string>(leftCols);
        foreach (var rc in rightCols)
        {
            var name = used.Contains(rc) ? NextFreeSuffix(rc, used) : rc;
            used.Add(name); outCols.Add(name);
        }
        return outCols;
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
                var name = nr.SimpleName;
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
                var fname = fce.Name.SimpleName;
                if (visiting.Contains(fname)) return null;  // recursion guard
                if (Converter.TryGetUserFunctionBody(fname, out var body) && body.Expression != null)
                {
                    visiting.Add(fname);
                    try { return EnumerateColumns(body.Expression, visiting); }
                    finally { visiting.Remove(fname); }
                }
                return null;
            }
            case FunctionDeclaration funcDecl when funcDecl.Body?.Expression != null:
                // `let B = view() { tabular-pipeline }` stores a FunctionDeclaration as the CTE's
                // backing expression. Walk into the function body's tabular result so the outer
                // join can enumerate its output columns.
                return EnumerateColumns(funcDecl.Body.Expression, visiting);
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
                    if (se.Element is NameReference knr) kept.Add(knr.SimpleName);
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
                        map[old.SimpleName] = sne.Name.SimpleName;
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
                        var n = sne.Name.SimpleName;
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
                        if (se.Element is SimpleNamedExpression byNamed) cols.Add(byNamed.Name.SimpleName);
                        else if (se.Element is NameReference byNr) cols.Add(byNr.SimpleName);
                        else return null;
                    }
                }
                var keyCounts = new Dictionary<string, int>(StringComparer.Ordinal);
                foreach (var agg in sum.Aggregates)
                {
                    if (agg.Element is SimpleNamedExpression aggNamed)
                    {
                        cols.Add(aggNamed.Name.SimpleName);
                    }
                    else if (agg.Element is FunctionCallExpression aggFce &&
                             aggFce.IsAny(Aggregates.ArgMax, Aggregates.ArgMin,
                                          Aggregates.ArgMax_Deprecated, Aggregates.ArgMin_Deprecated))
                    {
                        var keyName = aggFce.ArgumentList.Expressions[0].Element is NameReference keyNr
                            ? keyNr.Name.SimpleName : "expr";
                        keyCounts.TryGetValue(keyName, out var ki);
                        keyCounts[keyName] = ki + 1;
                        cols.Add(ki == 0 ? keyName : $"{keyName}{ki}");
                        for (int i = 1; i < aggFce.ArgumentList.Expressions.Count; i++)
                        {
                            var v = aggFce.ArgumentList.Expressions[i].Element;
                            if (v is SimpleNamedExpression vNamed) cols.Add(vNamed.Name.SimpleName);
                            else if (v is NameReference vNr) cols.Add(vNr.Name.SimpleName);
                            else return null; // e.g. arg_max(k, *) — can't enumerate wildcard
                        }
                    }
                    else
                    {
                        return null;
                    }
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
                    if (se.Element is NameReference dnr) names.Add(dnr.SimpleName);
                    else if (se.Element is SimpleNamedExpression dsne) names.Add(dsne.Name.SimpleName);
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
            case JoinOperator jop:
            {
                // Enumerate a join's output so a chain of joins (and a later project-away of the
                // suffixed duplicates, e.g. Ts1..Ts4) resolves. Approximate all kinds as both-sides
                // merge — semi/anti are rarely followed by a project-away of right columns.
                if (input == null || jop.Expression == null) return null;
                var rcols = TryEnumerateColumns(jop.Expression);
                return rcols == null ? null : MergeJoinColumns(input, rcols);
            }
            case LookupOperator lop:
            {
                if (input == null || lop.Expression == null) return null;
                var rcols = TryEnumerateColumns(lop.Expression);
                return rcols == null ? null : MergeJoinColumns(input, rcols);
            }
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
                        var n = sne.Name.SimpleName;
                        if (!existing.Contains(n)) { result.Add(n); existing.Add(n); }
                    }
                }
                return result;
            }
            case EvaluateOperator evalOp when evalOp.FunctionCall is FunctionCallExpression efce
                && string.Equals(efce.Name.SimpleName, "pivot", StringComparison.OrdinalIgnoreCase):
            {
                // evaluate pivot(pivotCol, agg [, groupCols...]) → passthrough (or groupCols) + IN values.
                // Matches ApplyEvaluatePivot's IN-list inference so downstream joins can emit
                // per-column aliases and dedup the <key>1 suffix on collisions.
                var pivotArgs = efce.ArgumentList.Expressions;
                if (pivotArgs.Count < 2) return null;

                var pivotColName = pivotArgs[0].Element is NameReference pnr ? pnr.Name.SimpleName : null;
                var aggArgNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (pivotArgs[1].Element is FunctionCallExpression aggCall)
                    foreach (var a in aggCall.ArgumentList.Expressions)
                        foreach (var inner in a.Element.GetDescendants<NameReference>())
                            if (!string.IsNullOrEmpty(inner.SimpleName))
                                aggArgNames.Add(inner.SimpleName);

                List<string> baseCols;
                if (pivotArgs.Count > 2)
                {
                    baseCols = new List<string>();
                    for (int i = 2; i < pivotArgs.Count; i++)
                        if (pivotArgs[i].Element is NameReference gnr)
                            baseCols.Add(gnr.Name.SimpleName);
                }
                else if (input != null)
                {
                    baseCols = input.Where(c =>
                            !string.Equals(c, pivotColName, StringComparison.OrdinalIgnoreCase)
                            && !aggArgNames.Contains(c))
                        .ToList();
                }
                else
                {
                    return null;
                }

                var pivotValues = CollectPivotPinnedValues(efce, pivotArgs);
                var seen = new HashSet<string>(baseCols, StringComparer.OrdinalIgnoreCase);
                foreach (var v in pivotValues)
                    if (seen.Add(v)) baseCols.Add(v);
                return baseCols;
            }
            default:
                return null;
        }
    }

    private static List<string> CollectPivotPinnedValues(
        FunctionCallExpression pivotFce,
        SyntaxList<SeparatedElement<Expression>> pivotArgs)
    {
        var exclude = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var inner in pivotArgs[0].Element.GetDescendants<NameReference>())
            if (!string.IsNullOrEmpty(inner.SimpleName)) exclude.Add(inner.SimpleName);
        if (pivotArgs[1].Element is FunctionCallExpression agg)
            foreach (var a in agg.ArgumentList.Expressions)
                foreach (var inner in a.Element.GetDescendants<NameReference>())
                    if (!string.IsNullOrEmpty(inner.SimpleName)) exclude.Add(inner.SimpleName);
        for (int i = 2; i < pivotArgs.Count; i++)
            foreach (var inner in pivotArgs[i].Element.GetDescendants<NameReference>())
                if (!string.IsNullOrEmpty(inner.SimpleName)) exclude.Add(inner.SimpleName);

        var candidates = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        void Add(string name)
        {
            if (string.IsNullOrEmpty(name) || exclude.Contains(name)) return;
            if (seen.Add(name)) candidates.Add(name);
        }

        var scan = pivotFce.Parent?.Parent?.Parent as PipeExpression;
        while (scan != null)
        {
            var op = scan.Operator;
            if (op is SummarizeOperator or JoinOperator or UnionOperator
                or LookupOperator or MakeSeriesOperator or EvaluateOperator) break;

            foreach (var fce in op.GetDescendants<FunctionCallExpression>())
            {
                if (string.Equals(fce.Name.SimpleName, "column_ifexists", StringComparison.OrdinalIgnoreCase)
                    && fce.ArgumentList.Expressions.Count >= 1
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
                    if (sep.Element is SimpleNamedExpression sne && sne.Name is NameDeclaration nd)
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

    private static List<string>? ExtractNames(SyntaxList<SeparatedElement<Expression>> exprs)
    {
        var names = new List<string>();
        foreach (var se in exprs)
        {
            if (se.Element is SimpleNamedExpression sne)
                names.Add(sne.Name.SimpleName);
            else if (se.Element is NameReference nr)
                names.Add(nr.SimpleName);
            else
                return null;
        }
        return names;
    }
}
