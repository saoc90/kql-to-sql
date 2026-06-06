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
                // KQL join keys treat null == null as equal; SQL `=` does not, so use the
                // null-safe comparison (IS NOT DISTINCT FROM) to match the Kusto oracle.
                conditions.Add($"L.{name} IS NOT DISTINCT FROM R.{name}");
            }
            else if (expr is BinaryExpression be && be.Kind == SyntaxKind.EqualExpression)
            {
                var left = Expr.ConvertExpression(be.Left, "L", "R");
                var right = Expr.ConvertExpression(be.Right, "L", "R");
                conditions.Add($"{left} IS NOT DISTINCT FROM {right}");
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
        // Outer joins pad the unmatched side's STRING columns with '' (not NULL) — verified
        // against the Kusto oracle (only string-typed columns are padded; numerics stay NULL).
        // LEFT→right side nullable, RIGHT→left side nullable, FULL→both nullable, INNER→neither.
        bool leftNullable = kind is "rightouter" or "fullouter";
        bool rightNullable = kind is "leftouter" or "fullouter";
        var leftStr = leftNullable ? TryEnumerateStringColumns(leftExpression) : null;
        var rightStr = rightNullable ? TryEnumerateStringColumns(join.Expression) : null;
        var selectClause = BuildJoinSelectClause(leftCols, rightCols, keySet, leftStr, rightStr);
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
                // Null-safe key match: Kusto treats null == null as equal (see ApplyJoin).
                conditions.Add($"L.{name} IS NOT DISTINCT FROM R.{name}");
            }
            else if (expr is BinaryExpression be && be.Kind == SyntaxKind.EqualExpression)
            {
                var left = Expr.ConvertExpression(be.Left, "L", "R");
                var right = Expr.ConvertExpression(be.Right, "L", "R");
                conditions.Add($"{left} IS NOT DISTINCT FROM {right}");
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
        // lookup defaults to LEFT OUTER JOIN: unmatched right rows pad STRING columns with ''
        // (not NULL), matching the Kusto oracle. kind=inner never pads.
        var rightStr = joinType.StartsWith("LEFT", StringComparison.OrdinalIgnoreCase)
            ? TryEnumerateStringColumns(lookup.Expression)
            : null;
        var selectClause = BuildLookupSelectClause(leftCols, rightCols, rightKeySet, rightStr);
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

    /// <summary>One operand of a union, paired with the AST expression it came from (for type/column
    /// enumeration) and the source label Kusto assigns it. <c>WildcardName</c> is set when the operand
    /// is a member expanded from a wildcard table-set pattern (which Kusto labels by table name, not
    /// positionally).</summary>
    private readonly struct UnionOperand
    {
        public UnionOperand(string sql, Expression? expr, string? wildcardName)
        { Sql = sql; Expr = expr; WildcardName = wildcardName; }
        public string Sql { get; }
        public Expression? Expr { get; }
        public string? WildcardName { get; }
    }

    internal string ApplyUnion(string leftSql, UnionOperator union, Expression? leftExpression)
    {
        var operands = new List<UnionOperand>
        {
            new UnionOperand(leftSql, leftExpression, null)
        };
        foreach (var expr in union.Expressions)
            CollectUnionOperands(expr.Element, operands);
        return BuildUnion(union, operands);
    }

    internal string ConvertUnion(UnionOperator union)
    {
        var operands = new List<UnionOperand>();
        foreach (var expr in union.Expressions)
            CollectUnionOperands(expr.Element, operands);
        return BuildUnion(union, operands);
    }

    /// <summary>Emits the UNION ALL of all operands. Each operand row is labelled per Kusto's
    /// <c>withsource</c> rule (positional <c>union_arg{N}</c> for ordinary operands; the table name
    /// for wildcard-expanded members). STRING columns that are absent from an operand but present
    /// (as strings) in another are padded with <c>''</c> — Kusto fills missing string cells with the
    /// empty string, not NULL (verified against the oracle); non-string missing cells stay NULL via
    /// DuckDB's UNION ALL BY NAME.</summary>
    private string BuildUnion(UnionOperator union, List<UnionOperand> operands)
    {
        var withSourceParam = union.Parameters.FirstOrDefault(p => p.Name.SimpleName.Equals("withsource", StringComparison.OrdinalIgnoreCase));
        string? sourceColumn = withSourceParam != null ? withSourceParam.Expression.ToString().Trim() : null;

        // Global string-column set and full column set across operands (only where enumerable).
        var globalStringCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var perOperandCols = new List<HashSet<string>?>();
        foreach (var op in operands)
        {
            var cols = op.Expr != null ? TryEnumerateColumns(op.Expr) : null;
            var strs = op.Expr != null ? TryEnumerateStringColumns(op.Expr) : null;
            perOperandCols.Add(cols != null ? new HashSet<string>(cols, StringComparer.OrdinalIgnoreCase) : null);
            if (strs != null) foreach (var s in strs) globalStringCols.Add(s);
        }

        var parts = new List<string>();
        for (int i = 0; i < operands.Count; i++)
        {
            var op = operands[i];
            var label = op.WildcardName ?? $"union_arg{i}";

            // Pad string columns this operand lacks but that exist (as strings) elsewhere → '' AS col.
            var pads = new List<string>();
            var own = perOperandCols[i];
            if (own != null)
                foreach (var sc in globalStringCols)
                    if (!own.Contains(sc))
                        pads.Add($"'' AS {ExpressionSqlBuilder.QuoteIdentifierIfReserved(sc)}");

            // Kusto places the withsource column FIRST in the output schema, so project it before '*'.
            var prefix = sourceColumn != null ? $"'{label}' AS {sourceColumn}, " : "";
            var selectExtras = string.Join("", pads.Select(p => ", " + p));

            var sql = (prefix.Length > 0 || selectExtras.Length > 0)
                ? $"SELECT {prefix}*{selectExtras} FROM ({op.Sql})"
                : op.Sql;
            parts.Add($"({sql})");
        }

        // Use DuckDB 'UNION ALL BY NAME' so column sets can differ — missing columns fill NULL.
        // KQL's union semantics are by-name, not positional.
        return string.Join(" UNION ALL BY NAME ", parts);
    }

    /// <summary>Flattens one `union` operand into the operand list. A wildcard table-set reference
    /// (`union Table_States*`, parsed as a NameReference whose Name is a WildcardedName) expands —
    /// verified against live Kusto — to every in-scope let-defined view whose name matches the
    /// pattern (case-sensitive, `*` = zero-or-more chars), each labelled by its name. A plain operand
    /// becomes a single positionally-labelled entry.</summary>
    private void CollectUnionOperands(Expression element, List<UnionOperand> operands)
    {
        if (element is NameReference nr && nr.Name is WildcardedName wild)
        {
            foreach (var cteName in Converter.CteNames)
            {
                if (!WildcardMatches(wild.SimpleName, cteName))
                    continue;
                var quoted = ExpressionSqlBuilder.QuoteIdentifierIfReserved(cteName);
                Expression? cteExpr = Converter.TryGetCteExpression(cteName, out var e) ? e : null;
                operands.Add(new UnionOperand($"SELECT * FROM {quoted}", cteExpr, cteName));
            }
            return;
        }

        operands.Add(new UnionOperand(Converter.ConvertNode(element), element, null));
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

    private string? BuildJoinSelectClause(IReadOnlyList<string>? leftCols, IReadOnlyList<string>? rightCols,
        HashSet<string> keys, HashSet<string>? leftStringCols = null, HashSet<string>? rightStringCols = null)
    {
        if (leftCols == null || rightCols == null) return null;
        var leftSet = new HashSet<string>(leftCols, StringComparer.OrdinalIgnoreCase);
        // Live Kusto: every join kind (inner, innerunique, leftouter, rightouter, fullouter)
        // keeps both sides' copy of the join key; the right-side copy is suffixed `1` just
        // like any other duplicate column.
        // On an outer join, unmatched STRING cells of the nullable side become '' (not NULL);
        // wrap each known-string column of that side in COALESCE(..., '').
        // Keep the cheap `L.*` form unless the left side is the nullable one (right/full outer)
        // and has known string columns to pad — only then enumerate the left columns explicitly.
        var parts = new List<string>();
        if (leftStringCols != null && leftStringCols.Count > 0)
        {
            foreach (var lc in leftCols)
            {
                var quoted = ExpressionSqlBuilder.QuoteIdentifierIfReserved(lc);
                parts.Add(leftStringCols.Contains(lc)
                    ? $"COALESCE(L.{quoted}, '') AS {quoted}"
                    : $"L.{quoted}");
            }
        }
        else
        {
            parts.Add("L.*");
        }
        // `used` grows as we alias, so a right column colliding with the left set (or with an
        // already-emitted suffix) takes the NEXT free index. This matches KQL across a chain of
        // joins: a `Ts` column joined 4 times becomes Ts1, Ts2, Ts3, Ts4 (not Ts1 four times).
        var used = new HashSet<string>(leftSet, StringComparer.OrdinalIgnoreCase);
        foreach (var rc in rightCols)
        {
            var quoted = ExpressionSqlBuilder.QuoteIdentifierIfReserved(rc);
            var pad = rightStringCols != null && rightStringCols.Contains(rc);
            var rhs = pad ? $"COALESCE(R.{quoted}, '')" : $"R.{quoted}";
            if (used.Contains(rc))
            {
                var name = NextFreeSuffix(rc, used);
                used.Add(name);
                parts.Add($"{rhs} AS {ExpressionSqlBuilder.QuoteIdentifierIfReserved(name)}");
            }
            else
            {
                used.Add(rc);
                parts.Add(pad ? $"{rhs} AS {quoted}" : rhs);
            }
        }
        return string.Join(", ", parts);
    }

    /// <summary>Derives the KQL auto-name of a bare (unaliased) aggregate, for column enumeration.
    /// Mirrors AggregationHandlers.DeriveAutoAlias for the AST-only cases. Returns null when the shape
    /// can't be named statically (so the caller treats the schema as unknown).</summary>
    private static string? AggAutoAlias(FunctionCallExpression fce)
    {
        var fn = fce.Name.SimpleName.ToLowerInvariant();
        var argc = fce.ArgumentList.Expressions.Count;
        string? inner = argc > 0 ? AggInnerIdentifier(fce.ArgumentList.Expressions[0].Element) : null;
        switch (fn)
        {
            case "count": return "count_";
            case "countif": return "countif_";
            case "dcountif": return "dcountif_";
            case "make_list" or "make_list_if" or "makelist" or "make_list_with_nulls":
                return inner != null ? $"list_{inner}" : null;
            case "make_set" or "make_set_if" or "makeset":
                return inner != null ? $"set_{inner}" : null;
            case "make_bag" or "make_bag_if":
                return inner != null ? $"bag_{inner}" : null;
            case "take_any" or "take_anyif":
                return inner; // keeps the inner identifier
            case "percentile" or "percentiles" when argc >= 2:
                return inner != null ? $"percentile_{inner}_{fce.ArgumentList.Expressions[1].Element.ToString().Trim()}" : null;
        }
        if (inner != null) return $"{fn}_{inner}";
        return $"{fn}_";
    }

    /// <summary>The inner identifier of an aggregate argument (drilling through conversion wrappers).</summary>
    private static string? AggInnerIdentifier(SyntaxNode node)
    {
        SyntaxNode? cur = node;
        while (cur is FunctionCallExpression f && f.ArgumentList.Expressions.Count == 1)
            cur = f.ArgumentList.Expressions[0].Element;
        return cur switch
        {
            NameReference nr => nr.SimpleName,
            SimpleNamedExpression sne => sne.Name.SimpleName,
            _ => null,
        };
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
    private string? BuildLookupSelectClause(IReadOnlyList<string>? leftCols, IReadOnlyList<string>? rightCols,
        HashSet<string> rightKeys, HashSet<string>? rightStringCols = null)
    {
        if (leftCols == null || rightCols == null) return null;
        var parts = new List<string> { "L.*" };
        var used = new HashSet<string>(leftCols, StringComparer.OrdinalIgnoreCase);
        foreach (var rc in rightCols)
        {
            if (rightKeys.Contains(rc)) continue; // right-side join key — dropped by lookup
            var quoted = ExpressionSqlBuilder.QuoteIdentifierIfReserved(rc);
            var pad = rightStringCols != null && rightStringCols.Contains(rc);
            var rhs = pad ? $"COALESCE(R.{quoted}, '')" : $"R.{quoted}";
            if (used.Contains(rc))
            {
                var name = NextFreeSuffix(rc, used);
                used.Add(name);
                parts.Add($"{rhs} AS {ExpressionSqlBuilder.QuoteIdentifierIfReserved(name)}");
            }
            else
            {
                used.Add(rc);
                parts.Add(pad ? $"{rhs} AS {quoted}" : rhs);
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

    // ─── Column TYPE tracking for outer-join / union string padding ──────────────
    // Kusto fills unmatched cells of STRING columns on the nullable side of an outer join
    // (and missing STRING columns in a union) with '' rather than NULL. To replicate this we
    // need to know which output columns are string-typed. We can determine this exactly for
    // datatable schemas and propagate it through the pass-through / projection operators that
    // make up the join/union family. Columns whose type we cannot determine are simply omitted
    // from the result set (and therefore not padded), which preserves the prior NULL behavior
    // for unknown schemas rather than risking an incorrect '' on a non-string column.

    private static readonly HashSet<string> StringTypeTokens =
        new(StringComparer.OrdinalIgnoreCase) { "string" };

    private HashSet<string>? TryEnumerateStringColumns(Expression? expr)
    {
        if (expr == null) return null;
        var map = EnumerateColumnTypes(expr, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
        if (map == null) return null;
        return new HashSet<string>(
            map.Where(kv => kv.Value).Select(kv => kv.Key),
            StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>Maps each enumerable output column name → true when it is known to be string-typed.
    /// Returns null when the shape is unknown. Columns whose type is indeterminate are recorded as
    /// false (treated as non-string → not padded).</summary>
    private Dictionary<string, bool>? EnumerateColumnTypes(Expression expr, HashSet<string> visiting)
    {
        switch (expr)
        {
            case ParenthesizedExpression pe:
                return EnumerateColumnTypes(pe.Expression, visiting);
            case MaterializeExpression me:
                return EnumerateColumnTypes(me.Expression, visiting);
            case DataTableExpression dt:
                return DataTableColumnTypes(dt);
            case PipeExpression pipe:
            {
                var input = EnumerateColumnTypes(pipe.Expression, visiting);
                return ApplyOperatorToTypes(input, pipe.Operator);
            }
            case NameReference nr:
            {
                var name = nr.SimpleName;
                if (visiting.Contains(name)) return null;
                if (Converter.TryGetCteExpression(name, out var cteExpr))
                {
                    visiting.Add(name);
                    try { return EnumerateColumnTypes(cteExpr, visiting); }
                    finally { visiting.Remove(name); }
                }
                return null; // base / well-known table — types unknown here
            }
            case FunctionCallExpression fce:
            {
                var fname = fce.Name.SimpleName;
                if (visiting.Contains(fname)) return null;
                if (Converter.TryGetUserFunctionBody(fname, out var body) && body.Expression != null)
                {
                    visiting.Add(fname);
                    try { return EnumerateColumnTypes(body.Expression, visiting); }
                    finally { visiting.Remove(fname); }
                }
                return null;
            }
            case FunctionDeclaration funcDecl when funcDecl.Body?.Expression != null:
                return EnumerateColumnTypes(funcDecl.Body.Expression, visiting);
            default:
                return null;
        }
    }

    /// <summary>Propagates string-ness across the operators that show up in the join/union family.
    /// Only operators whose effect on column types is known are handled; anything else returns null
    /// (unknown shape → caller skips padding).</summary>
    private Dictionary<string, bool>? ApplyOperatorToTypes(Dictionary<string, bool>? input, QueryOperator op)
    {
        switch (op)
        {
            case FilterOperator:
            case SortOperator:
            case TakeOperator:
            case TopOperator:
            case SerializeOperator:
            case AsOperator:
                return input;
            case ProjectOperator proj:
            {
                var result = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                foreach (var se in proj.Expressions)
                {
                    if (se.Element is NameReference pnr)
                        result[pnr.SimpleName] = input != null && input.TryGetValue(pnr.SimpleName, out var b) && b;
                    else if (se.Element is SimpleNamedExpression sne)
                        result[sne.Name.SimpleName] = IsStringExpression(sne.Expression, input);
                    else
                        return null;
                }
                return result;
            }
            case ProjectKeepOperator keep:
            {
                if (input == null) return null;
                var result = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                foreach (var se in keep.Expressions)
                {
                    if (se.Element is NameReference knr)
                        result[knr.SimpleName] = input.TryGetValue(knr.SimpleName, out var b) && b;
                    else return null;
                }
                return result;
            }
            case ProjectAwayOperator away:
            {
                if (input == null) return null;
                var rm = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var se in away.Expressions) rm.Add(se.Element.ToString().Trim());
                return input.Where(kv => !rm.Contains(kv.Key))
                    .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
            }
            case ProjectRenameOperator ren:
            {
                if (input == null) return null;
                var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var se in ren.Expressions)
                    if (se.Element is SimpleNamedExpression sne && sne.Expression is NameReference old)
                        map[old.SimpleName] = sne.Name.SimpleName;
                var result = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                foreach (var kv in input)
                    result[map.TryGetValue(kv.Key, out var n) ? n : kv.Key] = kv.Value;
                return result;
            }
            case ExtendOperator ext:
            {
                if (input == null) return null;
                var result = new Dictionary<string, bool>(input, StringComparer.OrdinalIgnoreCase);
                foreach (var se in ext.Expressions)
                    if (se.Element is SimpleNamedExpression sne)
                        result[sne.Name.SimpleName] = IsStringExpression(sne.Expression, input);
                return result;
            }
            case DistinctOperator dist:
            {
                if (dist.Expressions.Count == 0) return input;
                if (input == null) return null;
                var result = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                foreach (var se in dist.Expressions)
                {
                    if (se.Element is NameReference dnr)
                        result[dnr.SimpleName] = input.TryGetValue(dnr.SimpleName, out var b) && b;
                    else if (se.Element is SimpleNamedExpression dsne)
                        result[dsne.Name.SimpleName] = IsStringExpression(dsne.Expression, input);
                    else return null;
                }
                return result;
            }
            default:
                return null;
        }
    }

    /// <summary>Best-effort: is this projected/extended expression string-typed? True only for forms
    /// we can prove (string literal, a string-typed input column, or strcat/tostring/etc.).</summary>
    private static bool IsStringExpression(Expression e, Dictionary<string, bool>? input)
    {
        switch (e)
        {
            case LiteralExpression lit:
                return lit.Kind == SyntaxKind.StringLiteralExpression;
            case ParenthesizedExpression pe:
                return IsStringExpression(pe.Expression, input);
            case NameReference nr:
                return input != null && input.TryGetValue(nr.SimpleName, out var b) && b;
            case FunctionCallExpression fce:
            {
                var n = fce.Name.SimpleName;
                return n.Equals("strcat", StringComparison.OrdinalIgnoreCase)
                    || n.Equals("tostring", StringComparison.OrdinalIgnoreCase)
                    || n.Equals("toupper", StringComparison.OrdinalIgnoreCase)
                    || n.Equals("tolower", StringComparison.OrdinalIgnoreCase)
                    || n.Equals("substring", StringComparison.OrdinalIgnoreCase)
                    || n.Equals("replace_string", StringComparison.OrdinalIgnoreCase)
                    || n.Equals("trim", StringComparison.OrdinalIgnoreCase);
            }
            default:
                return false;
        }
    }

    private static List<string> DataTableColumnNames(DataTableExpression dt)
    {
        var names = new List<string>();
        foreach (var col in dt.Schema.Columns)
            if (col.Element is NameAndTypeDeclaration nat)
                names.Add(nat.Name.SimpleName);
        return names;
    }

    private static Dictionary<string, bool> DataTableColumnTypes(DataTableExpression dt)
    {
        var map = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        foreach (var col in dt.Schema.Columns)
            if (col.Element is NameAndTypeDeclaration nat)
            {
                var typeName = nat.Type?.ToString().Trim() ?? "";
                map[nat.Name.SimpleName] = StringTypeTokens.Contains(typeName);
            }
        return map;
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
            case DataTableExpression dt:
                return DataTableColumnNames(dt);
            case RangeOperator rng:
                // `range x from .. to .. step ..` produces a single column named after the variable.
                return new List<string> { rng.Name.SimpleName };
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
                    else if (agg.Element is FunctionCallExpression bareAgg)
                    {
                        // Bare aggregate (count(), sum(x), make_list(x), …) → Kusto auto-name (count_, sum_x, list_x).
                        var alias = AggAutoAlias(bareAgg);
                        if (alias == null) return null;
                        cols.Add(alias);
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
