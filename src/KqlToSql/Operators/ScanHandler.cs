using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

/// <summary>
/// Translates the KQL `scan` operator for single-step (non-pattern-matching) bodies.
///
/// Reference semantics inside a step (the crux of the lowering):
/// - A <b>step-prefixed</b> reference `s.col` reads the value of `col` from the PREVIOUS row
///   (the declared default on the first row). This is the running state that must be carried
///   across rows — e.g. `cum = s.cum + v` is a cumulative sum.
/// - A <b>bare</b> reference `col` reads the value of `col` within the CURRENT row only: it starts
///   at the declared default at the top of each row and is updated by assignments executed earlier
///   in the same step. It does NOT carry across rows — e.g. `cum = cum + v` is just `default + v`.
///
/// Lowering strategies:
/// - No carried (step-prefixed) state anywhere → a plain per-row `SELECT *, &lt;expr&gt; ...`, with
///   bare declared references resolved to their defaults / earlier same-step assignments.
/// - Carried state expressible as one independent window aggregate (cumulative sum, running
///   max/min, forward-fill, cumulative-with-reset) → `SELECT *, &lt;window&gt; ...`.
/// - Otherwise (a genuine recurrence such as an EMA `ema = 0.5*v + 0.5*s.ema`) → a recursive CTE
///   that walks the rows in serialization order, carrying the declared state from row to row.
///
/// Does NOT support:
/// - Multiple steps (pattern matching across rows)
/// - Match ID / with_match_id
/// - Conditions other than `true`
/// - Output modes (all/last/none)
/// </summary>
internal sealed class ScanHandler : OperatorHandlerBase
{
    internal ScanHandler(KqlToSqlConverter converter, ExpressionSqlBuilder expr)
        : base(converter, expr) { }

    internal string ApplyScan(string leftSql, ScanOperator scan)
    {
        if (scan.Steps.Count != 1)
            throw new NotSupportedException("scan with multiple steps is not supported (pattern matching)");

        var step = scan.Steps[0];

        // Only support `true` condition
        if (step.Condition is not LiteralExpression lit ||
            !lit.ToString().Trim().Equals("true", StringComparison.OrdinalIgnoreCase))
            throw new NotSupportedException("scan with non-`true` step condition is not supported");

        // Extract declared columns, their defaults and (for the recursive path) their SQL types,
        // preserving declaration order.
        var declaredOrder = new List<string>();
        var declaredDefaults = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var declaredTypes = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        if (scan.DeclareClause != null)
        {
            foreach (var declEl in scan.DeclareClause.Declarations)
            {
                var param = declEl.Element;
                var name = param.NameAndType.Name.SimpleName;
                var defaultSql = "NULL";
                if (param.DefaultValue?.Value is Expression defExpr)
                {
                    try { defaultSql = Expr.ConvertExpression(defExpr); } catch { }
                }
                if (!declaredDefaults.ContainsKey(name)) declaredOrder.Add(name);
                declaredDefaults[name] = defaultSql;
                declaredTypes[name] = MapDeclaredType(param.NameAndType.Type?.ToString());
            }
        }

        var declaredSet = new HashSet<string>(declaredOrder, StringComparer.OrdinalIgnoreCase);
        var stepName = step.Name.Name.SimpleName;

        // Collect the assignments (column ← expression) in source order.
        var assignments = new List<(string Col, Expression Value)>();
        if (step.ComputationClause != null)
        {
            foreach (var assignEl in step.ComputationClause.Assignments)
            {
                var ass = assignEl.Element;
                assignments.Add((ass.Name.SimpleName, ass.Expression));
            }
        }

        if (assignments.Count == 0)
            throw new NotSupportedException("scan step has no assignments");

        // Does any assignment read carried (step-prefixed) state? Only then is row-to-row carry needed.
        bool anyCarried = assignments.Any(a => ContainsCarriedState(a.Value, stepName, declaredSet));

        if (!anyCarried)
        {
            // Pure per-row computation: bare declared refs resolve to defaults / earlier same-step
            // assignments. No window or recursion required.
            var perRow = BuildPerRowAssignments(assignments, stepName, declaredSet, declaredDefaults);
            var fromSql0 = ExtractFrom(leftSql);
            return $"SELECT *, {string.Join(", ", perRow)} FROM {fromSql0}";
        }

        // Carried state present. First try to express every assignment as one independent window
        // aggregate (keeps the output compact and set-based).
        var windowClauses = new List<string>();
        bool allWindow = true;
        foreach (var (col, val) in assignments)
        {
            var w = TryTranslateWindowAssignment(col, val, stepName, declaredSet, declaredDefaults);
            if (w == null) { allWindow = false; break; }
            windowClauses.Add($"{w} AS {ExpressionSqlBuilder.QuoteIdentifierIfReserved(col)}");
        }

        if (allWindow)
        {
            var fromSql = ExtractFrom(leftSql);
            return $"SELECT *, {string.Join(", ", windowClauses)} FROM {fromSql}";
        }

        // General recurrence → recursive CTE carrying state row-by-row.
        return BuildRecursiveScan(leftSql, stepName, assignments, declaredOrder, declaredSet, declaredDefaults, declaredTypes);
    }

    // ---------------------------------------------------------------------------------------------
    // Pure per-row lowering (no carried state). Bare declared references resolve to the declared
    // default, or — if the column was assigned earlier in the same step — that assignment's value.
    // ---------------------------------------------------------------------------------------------
    private List<string> BuildPerRowAssignments(
        List<(string Col, Expression Value)> assignments,
        string stepName,
        HashSet<string> declared,
        Dictionary<string, string> defaults)
    {
        // Tracks the current per-row SQL for each declared column as we walk the assignments.
        var current = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var d in declared)
            current[d] = defaults.TryGetValue(d, out var def) ? def : "NULL";

        var clauses = new List<string>();
        foreach (var (col, val) in assignments)
        {
            // Bare ref → current value so far; carried refs cannot appear here (anyCarried == false).
            var sql = RenderScanExpr(val, stepName, declared,
                name => current.TryGetValue(name, out var c) ? $"({c})" : "NULL",
                srcQualifier: null);
            if (declared.Contains(col)) current[col] = sql;
            clauses.Add($"{sql} AS {ExpressionSqlBuilder.QuoteIdentifierIfReserved(col)}");
        }
        return clauses;
    }

    // ---------------------------------------------------------------------------------------------
    // Window-function lowering for carried state. Returns the SQL window expression, or null when
    // the assignment is not expressible as a single independent window aggregate.
    // ---------------------------------------------------------------------------------------------
    private string? TryTranslateWindowAssignment(
        string column,
        Expression value,
        string stepName,
        HashSet<string> declared,
        Dictionary<string, string> defaults)
    {
        // Pattern 1: Forward fill — col = iff(isempty(X), s.col, X)
        if (TryParseForwardFill(value, column, stepName, out var fillSource, out var castType, out var emptyKind))
        {
            var defaultSql = defaults.TryGetValue(column, out var def) ? def : "NULL";
            var sourceNormalized = emptyKind == "isempty"
                ? $"CASE WHEN ({fillSource}) IS NULL OR CAST({fillSource} AS VARCHAR) = '' THEN NULL ELSE {fillSource} END"
                : $"CASE WHEN ({fillSource}) IS NULL THEN NULL ELSE {fillSource} END";
            var sourceExprCast = castType != null ? $"CAST({sourceNormalized} AS {castType})" : sourceNormalized;
            return $"COALESCE(LAST_VALUE({sourceExprCast} IGNORE NULLS) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), {defaultSql})";
        }

        // Pattern 2: Cumulative sum — col = s.col + expr  OR  col = expr + s.col
        if (value is BinaryExpression addBin && addBin.Kind == SyntaxKind.AddExpression)
        {
            Expression? delta = null;
            if (IsCarriedState(addBin.Left, stepName, column)) delta = addBin.Right;
            else if (IsCarriedState(addBin.Right, stepName, column)) delta = addBin.Left;

            // The delta must itself be carried-state-free (otherwise it is a non-linear recurrence).
            if (delta != null && !ContainsCarriedState(delta, stepName, declared))
            {
                var deltaSql = Expr.ConvertExpression(delta);
                var defaultSql = defaults.TryGetValue(column, out var def) ? def : "0";
                return $"COALESCE(SUM({deltaSql}) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), {defaultSql})";
            }
        }

        // Pattern 2b: Running max / min — col = iff(X > s.col, X, s.col) (and < / swapped variants).
        if (TryParseRunningExtreme(value, column, stepName, declared, out var extremeFn, out var extremeArg))
        {
            var defaultSql = defaults.TryGetValue(column, out var def) ? def : "NULL";
            var clamp = extremeFn == "MAX" ? "GREATEST" : "LEAST";
            var running = $"{extremeFn}({extremeArg}) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
            return $"{clamp}({running}, {defaultSql})";
        }

        // Pattern 3: Cumulative sum with reset — col = iff(s.col >= threshold, reset, s.col + delta)
        if (value is FunctionCallExpression iffFce &&
            (iffFce.Is(Functions.Iff) || iffFce.Is(Functions.Iif)) &&
            iffFce.ArgumentList.Expressions.Count == 3)
        {
            var cond = iffFce.ArgumentList.Expressions[0].Element;
            var resetValue = iffFce.ArgumentList.Expressions[1].Element;
            var contValue = iffFce.ArgumentList.Expressions[2].Element;

            if (ContainsCarriedState(cond, stepName, declared) &&
                contValue is BinaryExpression contAdd && contAdd.Kind == SyntaxKind.AddExpression &&
                !ContainsCarriedState(resetValue, stepName, declared))
            {
                Expression? delta = null;
                if (IsCarriedState(contAdd.Left, stepName, column)) delta = contAdd.Right;
                else if (IsCarriedState(contAdd.Right, stepName, column)) delta = contAdd.Left;

                if (delta != null && !ContainsCarriedState(delta, stepName, declared))
                {
                    var deltaSql = Expr.ConvertExpression(delta);
                    var resetSql = Expr.ConvertExpression(resetValue);
                    var priorCumsum = $"COALESCE(SUM({deltaSql}) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)";
                    var condSql = SubstituteCarriedState(cond, stepName, column, priorCumsum);
                    return $"CASE WHEN {condSql} THEN {resetSql} ELSE {priorCumsum} + {deltaSql} END";
                }
            }
        }

        // `col = s.col` alone → the running value never changes from its seed.
        if (IsCarriedState(value, stepName, column))
            return defaults.TryGetValue(column, out var def) ? def : "NULL";

        // References carried state but matches no window pattern → caller falls back to recursion.
        return null;
    }

    // ---------------------------------------------------------------------------------------------
    // Recursive-CTE lowering. Fully general for single-step scans, including genuine recurrences
    // (EMA) that no single window aggregate can express.
    // ---------------------------------------------------------------------------------------------
    private string BuildRecursiveScan(
        string leftSql,
        string stepName,
        List<(string Col, Expression Value)> assignments,
        List<string> declaredOrder,
        HashSet<string> declared,
        Dictionary<string, string> defaults,
        Dictionary<string, string?> types)
    {
        // Columns carried by the recursion: every declared column plus any assignment target.
        var stateCols = new List<string>(declaredOrder);
        foreach (var (col, _) in assignments)
            if (!stateCols.Contains(col, StringComparer.OrdinalIgnoreCase)) stateCols.Add(col);

        var orderKeys = ExtractTrailingOrderBy(leftSql);
        var orderClause = string.IsNullOrWhiteSpace(orderKeys) ? "" : $"ORDER BY {orderKeys}";
        var fromSql = ExtractFrom(leftSql);

        // Per-row substitution maps for a declared column reference.
        //   anchor (row 1): bare → default; carried s.col → default.
        //   recursive (row n): bare → default; carried s.col → p.col (prior row).
        // (Bare refs reset to the default each row; only carried refs read prior state.)
        string Default(string name) => defaults.TryGetValue(name, out var d) ? d : "NULL";
        string Prior(string name) => $"p.{ExpressionSqlBuilder.QuoteIdentifierIfReserved(name)}";

        // Cast each state column to its declared SQL type so the carried type is stable across the
        // recursion. This matters for `real` columns: without it DuckDB infers DECIMAL from decimal
        // literals and accumulates scale-rounding error (e.g. an EMA drifting from Kusto's double).
        string Typed(string sql, string col) =>
            types.TryGetValue(col, out var t) && t != null ? $"CAST({sql} AS {t})" : sql;

        var anchorCols = new List<string>();
        var recCols = new List<string>();
        foreach (var col in stateCols)
        {
            var quoted = ExpressionSqlBuilder.QuoteIdentifierIfReserved(col);
            var assign = assignments.FirstOrDefault(a => string.Equals(a.Col, col, StringComparison.OrdinalIgnoreCase));
            if (assign.Value != null)
            {
                anchorCols.Add($"{Typed(RenderScanExpr(assign.Value, stepName, declared, Default, "s", carriedRepl: Default), col)} AS {quoted}");
                recCols.Add($"{Typed(RenderScanExpr(assign.Value, stepName, declared, Default, "s", carriedRepl: Prior), col)} AS {quoted}");
            }
            else
            {
                // Declared but never assigned: keep the prior value (default on the first row).
                anchorCols.Add($"{Typed(Default(col), col)} AS {quoted}");
                recCols.Add($"{Prior(col)} AS {quoted}");
            }
        }

        var stateColList = string.Join(", ", stateCols.Select(ExpressionSqlBuilder.QuoteIdentifierIfReserved));
        var finalCols = string.Join(", ", stateCols.Select(c => $"__scan.{ExpressionSqlBuilder.QuoteIdentifierIfReserved(c)}"));

        var src = $"SELECT *, ROW_NUMBER() OVER ({orderClause}) AS __rn FROM {fromSql}";
        var anchor = $"SELECT s.__rn, {string.Join(", ", anchorCols)} FROM __src AS s WHERE s.__rn = 1";
        var recursive =
            $"SELECT s.__rn, {string.Join(", ", recCols)} " +
            $"FROM __scan AS p JOIN __src AS s ON s.__rn = p.__rn + 1";

        return
            $"WITH RECURSIVE __src AS ({src}), " +
            $"__scan(__rn, {stateColList}) AS ({anchor} UNION ALL {recursive}) " +
            $"SELECT __src.* EXCLUDE (__rn), {finalCols} " +
            $"FROM __src JOIN __scan USING (__rn) ORDER BY __src.__rn";
    }

    /// <summary>
    /// Convert a scan RHS expression to SQL. Bare declared-column references are routed through
    /// <paramref name="bareRepl"/>; step-prefixed (carried-state) references through
    /// <paramref name="carriedRepl"/> (defaults to <paramref name="bareRepl"/> when not given).
    /// Ordinary input columns are left bare so they resolve against the source relation.
    /// </summary>
    private string RenderScanExpr(
        Expression e,
        string stepName,
        HashSet<string> declared,
        Func<string, string> bareRepl,
        string? srcQualifier,
        Func<string, string>? carriedRepl = null)
    {
        carriedRepl ??= bareRepl;

        switch (e)
        {
            case ParenthesizedExpression pe:
                return $"({RenderScanExpr(pe.Expression, stepName, declared, bareRepl, srcQualifier, carriedRepl)})";

            case PrefixUnaryExpression pu when pu.Kind == SyntaxKind.UnaryMinusExpression:
            {
                var inner = RenderScanExpr(pu.Expression, stepName, declared, bareRepl, srcQualifier, carriedRepl);
                return inner.StartsWith("-", StringComparison.Ordinal) ? $"(- {inner})" : $"(-{inner})";
            }
            case PrefixUnaryExpression pu when pu.Kind == SyntaxKind.UnaryPlusExpression:
                return RenderScanExpr(pu.Expression, stepName, declared, bareRepl, srcQualifier, carriedRepl);

            // Carried-state ref via step prefix: stepName.column
            case PathExpression path
                when path.Expression is NameReference pnr &&
                     string.Equals(pnr.Name.SimpleName, stepName, StringComparison.OrdinalIgnoreCase) &&
                     path.Selector is NameReference psel && declared.Contains(psel.Name.SimpleName):
                return carriedRepl(psel.Name.SimpleName);

            // Bare reference: declared column → per-row value; otherwise an input column.
            case NameReference nr when declared.Contains(nr.Name.SimpleName):
                return bareRepl(nr.Name.SimpleName);

            case BinaryExpression bin when BinaryOp(bin.Kind) is { } op:
            {
                var l = RenderScanExpr(bin.Left, stepName, declared, bareRepl, srcQualifier, carriedRepl);
                var r = RenderScanExpr(bin.Right, stepName, declared, bareRepl, srcQualifier, carriedRepl);
                return $"{l} {op} {r}";
            }

            case FunctionCallExpression fce
                when (fce.Is(Functions.Iff) || fce.Is(Functions.Iif)) &&
                     fce.ArgumentList.Expressions.Count == 3:
            {
                var c = RenderScanExpr(fce.ArgumentList.Expressions[0].Element, stepName, declared, bareRepl, srcQualifier, carriedRepl);
                var t = RenderScanExpr(fce.ArgumentList.Expressions[1].Element, stepName, declared, bareRepl, srcQualifier, carriedRepl);
                var f = RenderScanExpr(fce.ArgumentList.Expressions[2].Element, stepName, declared, bareRepl, srcQualifier, carriedRepl);
                return $"CASE WHEN {c} THEN {t} ELSE {f} END";
            }

            case FunctionCallExpression fce
                when (fce.Is(Functions.MaxOf) || fce.Is(Functions.MinOf)) &&
                     fce.ArgumentList.Expressions.Count >= 2:
            {
                var fn = fce.Is(Functions.MaxOf) ? "GREATEST" : "LEAST";
                var args = fce.ArgumentList.Expressions
                    .Select(a => RenderScanExpr(a.Element, stepName, declared, bareRepl, srcQualifier, carriedRepl));
                return $"{fn}({string.Join(", ", args)})";
            }

            case FunctionCallExpression fce
                when fce.Is(Functions.Coalesce) && fce.ArgumentList.Expressions.Count >= 1:
            {
                var args = fce.ArgumentList.Expressions
                    .Select(a => RenderScanExpr(a.Element, stepName, declared, bareRepl, srcQualifier, carriedRepl));
                return $"COALESCE({string.Join(", ", args)})";
            }
        }

        // No declared reference anywhere below this node → defer to the full expression converter.
        if (!ReferencesDeclared(e, stepName, declared))
            return Expr.ConvertExpression(e);

        throw new NotSupportedException(
            $"scan step expression not supported in stateful lowering: {e.Kind}");
    }

    /// <summary>Map a KQL scalar type (as written in a `declare`) to a DuckDB SQL type, or null
    /// when no explicit cast is warranted. Used to pin the carried type in the recursive CTE.</summary>
    private static string? MapDeclaredType(string? kqlType)
    {
        var t = kqlType?.Trim().TrimStart(':').Trim().ToLowerInvariant();
        return t switch
        {
            "real" or "double" => "DOUBLE",
            "long" => "BIGINT",
            "int" => "INTEGER",
            "decimal" => "DOUBLE",
            "datetime" => "TIMESTAMP",
            "timespan" => "INTERVAL",
            "bool" or "boolean" => "BOOLEAN",
            "string" => "VARCHAR",
            _ => null
        };
    }

    private static string? BinaryOp(SyntaxKind kind) => kind switch
    {
        SyntaxKind.AddExpression => "+",
        SyntaxKind.SubtractExpression => "-",
        SyntaxKind.MultiplyExpression => "*",
        SyntaxKind.DivideExpression => "/",
        SyntaxKind.ModuloExpression => "%",
        SyntaxKind.GreaterThanExpression => ">",
        SyntaxKind.GreaterThanOrEqualExpression => ">=",
        SyntaxKind.LessThanExpression => "<",
        SyntaxKind.LessThanOrEqualExpression => "<=",
        SyntaxKind.EqualExpression => "=",
        SyntaxKind.NotEqualExpression => "<>",
        SyntaxKind.AndExpression => "AND",
        SyntaxKind.OrExpression => "OR",
        _ => null
    };

    /// <summary>
    /// Detect a running extreme on carried state: `iff(X &gt; s.col, X, s.col)` / `iff(s.col &gt; X, s.col, X)`
    /// and the &lt; variants, where exactly one operand is the carried state and X is carried-state-free.
    /// </summary>
    private bool TryParseRunningExtreme(
        Expression value, string column, string stepName,
        HashSet<string> declared,
        out string fn, out string arg)
    {
        fn = ""; arg = "";
        if (value is not FunctionCallExpression fce) return false;
        if (!fce.Is(Functions.Iff) && !fce.Is(Functions.Iif)) return false;
        if (fce.ArgumentList.Expressions.Count != 3) return false;

        if (fce.ArgumentList.Expressions[0].Element is not BinaryExpression cond) return false;
        var trueBranch = fce.ArgumentList.Expressions[1].Element;
        var falseBranch = fce.ArgumentList.Expressions[2].Element;

        bool greater = cond.Kind is SyntaxKind.GreaterThanExpression or SyntaxKind.GreaterThanOrEqualExpression;
        bool less = cond.Kind is SyntaxKind.LessThanExpression or SyntaxKind.LessThanOrEqualExpression;
        if (!greater && !less) return false;

        var left = cond.Left;
        var right = cond.Right;
        bool leftIsState = IsCarriedState(left, stepName, column);
        bool rightIsState = IsCarriedState(right, stepName, column);
        if (leftIsState == rightIsState) return false; // need exactly one carried-state operand

        Expression x = leftIsState ? right : left;
        if (ContainsCarriedState(x, stepName, declared)) return false;

        // For `left > right`: the larger operand is `left`; for `<`, the larger is `right`.
        Expression largerBranch = greater ? left : right;
        Expression smallerBranch = greater ? right : left;

        bool isMax;
        if (ExpressionsMatch(trueBranch, largerBranch) && ExpressionsMatch(falseBranch, smallerBranch))
            isMax = true;
        else if (ExpressionsMatch(trueBranch, smallerBranch) && ExpressionsMatch(falseBranch, largerBranch))
            isMax = false;
        else
            return false;

        fn = isMax ? "MAX" : "MIN";
        arg = Expr.ConvertExpression(x);
        return true;
    }

    // ---------------------------------------------------------------------------------------------
    // Carried-state detection. ONLY the step-prefixed form `stepName.column` is carried state;
    // a bare `column` is a per-row reference (handled separately) and is NOT carried state.
    // ---------------------------------------------------------------------------------------------

    private static bool IsCarriedState(Expression expr, string stepName, string column)
        => expr is PathExpression pe &&
           pe.Expression is NameReference nr &&
           string.Equals(nr.Name.SimpleName, stepName, StringComparison.OrdinalIgnoreCase) &&
           pe.Selector is NameReference selector &&
           string.Equals(selector.Name.SimpleName, column, StringComparison.OrdinalIgnoreCase);

    private static bool ContainsCarriedState(Expression expr, string stepName, string column)
    {
        if (IsCarriedState(expr, stepName, column)) return true;
        foreach (var pe in expr.GetDescendants<PathExpression>())
            if (IsCarriedState(pe, stepName, column)) return true;
        return false;
    }

    /// <summary>True if the expression reads carried state for ANY declared column.</summary>
    private static bool ContainsCarriedState(Expression expr, string stepName, HashSet<string> declared)
    {
        if (expr is PathExpression self && self.Expression is NameReference snr &&
            string.Equals(snr.Name.SimpleName, stepName, StringComparison.OrdinalIgnoreCase) &&
            self.Selector is NameReference ssel && declared.Contains(ssel.Name.SimpleName))
            return true;
        foreach (var pe in expr.GetDescendants<PathExpression>())
            if (pe.Expression is NameReference nr &&
                string.Equals(nr.Name.SimpleName, stepName, StringComparison.OrdinalIgnoreCase) &&
                pe.Selector is NameReference selector && declared.Contains(selector.Name.SimpleName))
                return true;
        return false;
    }

    /// <summary>True if the expression references any declared column (bare or step-prefixed).</summary>
    private static bool ReferencesDeclared(Expression expr, string stepName, HashSet<string> declared)
    {
        if (ContainsCarriedState(expr, stepName, declared)) return true;
        if (expr is NameReference selfNr && declared.Contains(selfNr.Name.SimpleName)) return true;
        foreach (var nr in expr.GetDescendants<NameReference>())
        {
            if (nr.Parent is PathExpression p && p.Selector == nr) continue; // selector half of a path
            if (declared.Contains(nr.Name.SimpleName)) return true;
        }
        return false;
    }

    /// <summary>
    /// Detect the forward-fill idiom: `iff(isempty(X), s.col, X)` or `iff(isnull(X), s.col, X)`
    /// with optional type casting of X in the false branch.
    /// </summary>
    private bool TryParseForwardFill(Expression value, string column, string stepName,
                                      out string fillSource, out string? castType, out string emptyKind)
    {
        fillSource = "";
        castType = null;
        emptyKind = "isnull";

        if (value is not FunctionCallExpression fce) return false;
        if (!fce.Is(Functions.Iff) && !fce.Is(Functions.Iif)) return false;
        if (fce.ArgumentList.Expressions.Count != 3) return false;

        var cond = fce.ArgumentList.Expressions[0].Element;
        var trueBranch = fce.ArgumentList.Expressions[1].Element;
        var falseBranch = fce.ArgumentList.Expressions[2].Element;

        // Condition must be isempty(X) or isnull(X)
        if (cond is not FunctionCallExpression condFce) return false;
        if (!condFce.Is(Functions.IsEmpty) && !condFce.Is(Functions.IsNull)) return false;
        if (condFce.ArgumentList.Expressions.Count != 1) return false;
        emptyKind = condFce.Is(Functions.IsEmpty) ? "isempty" : "isnull";

        var sourceExpr = condFce.ArgumentList.Expressions[0].Element;

        // True branch must be the carried state for this column (s.col).
        if (!IsCarriedState(trueBranch, stepName, column)) return false;

        // False branch = same source (optionally cast)
        if (ExpressionsMatch(sourceExpr, falseBranch))
        {
            fillSource = Expr.ConvertExpression(sourceExpr);
            return true;
        }

        if (falseBranch is FunctionCallExpression falseFce && falseFce.ArgumentList.Expressions.Count == 1)
        {
            string? mappedType = null;
            if (falseFce.Is(Functions.ToInt)) mappedType = "INTEGER";
            else if (falseFce.Is(Functions.ToLong)) mappedType = "BIGINT";
            else if (falseFce.Is(Functions.ToReal) || falseFce.Is(Functions.ToDouble)) mappedType = "DOUBLE";
            else if (falseFce.Is(Functions.ToString)) mappedType = "VARCHAR";
            else if (falseFce.Is(Functions.ToBool)) mappedType = "BOOLEAN";
            else if (falseFce.Is(Functions.ToDateTime)) mappedType = "TIMESTAMP";
            else if (falseFce.Is(Functions.ToTimespan)) mappedType = "INTERVAL";
            if (mappedType != null &&
                ExpressionsMatch(falseFce.ArgumentList.Expressions[0].Element, sourceExpr))
            {
                fillSource = Expr.ConvertExpression(sourceExpr);
                castType = mappedType;
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Convert an expression to SQL, substituting carried-state references to <paramref name="column"/>
    /// with <paramref name="replacement"/>. Used by the cumulative-with-reset condition.
    /// </summary>
    private string SubstituteCarriedState(Expression expr, string stepName, string column, string replacement)
        => ConvertWithCarriedSub(expr, stepName, column, replacement);

    private string ConvertWithCarriedSub(Expression e, string stepName, string column, string replacement)
    {
        if (IsCarriedState(e, stepName, column)) return $"({replacement})";

        if (e is ParenthesizedExpression pe)
            return $"({ConvertWithCarriedSub(pe.Expression, stepName, column, replacement)})";

        if (e is BinaryExpression bin && BinaryOp(bin.Kind) is { } op)
        {
            var l = ConvertWithCarriedSub(bin.Left, stepName, column, replacement);
            var r = ConvertWithCarriedSub(bin.Right, stepName, column, replacement);
            return $"{l} {op} {r}";
        }

        return Expr.ConvertExpression(e);
    }

    private static bool ExpressionsMatch(Expression a, Expression b)
    {
        return a.ToString().Trim() == b.ToString().Trim();
    }
}
