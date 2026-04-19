using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

/// <summary>
/// Translates the KQL `scan` operator for the three most common patterns:
/// - Cumulative aggregation: `col = s.col + expr` → SUM(expr) OVER (...)
/// - Forward fill: `col = iff(isempty/isnull(X), s.col, X)` → LAST_VALUE(X IGNORE NULLS) OVER (...)
/// - Cumulative with reset: `col = iff(s.col >= X, reset, s.col + delta)` → partitioned SUM
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

        // Extract declared columns and their defaults
        var declaredDefaults = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (scan.DeclareClause != null)
        {
            foreach (var declEl in scan.DeclareClause.Declarations)
            {
                var param = declEl.Element;
                var name = param.NameAndType.Name.ToString().Trim();
                var defaultSql = "NULL";
                if (param.DefaultValue?.Value is Expression defExpr)
                {
                    try { defaultSql = Expr.ConvertExpression(defExpr); } catch { }
                }
                declaredDefaults[name] = defaultSql;
            }
        }

        var stepName = step.Name.Name.ToString().Trim();

        // Walk assignments
        var selectClauses = new List<string>();
        if (step.ComputationClause != null)
        {
            foreach (var assignEl in step.ComputationClause.Assignments)
            {
                var ass = assignEl.Element;
                var colName = ass.Name.ToString().Trim();
                var valExpr = ass.Expression;
                var sqlExpr = TranslateAssignment(colName, valExpr, stepName, declaredDefaults);
                selectClauses.Add($"{sqlExpr} AS {colName}");
            }
        }

        if (selectClauses.Count == 0)
            throw new NotSupportedException("scan step has no assignments");

        var fromSql = ExtractFrom(leftSql);
        return $"SELECT *, {string.Join(", ", selectClauses)} FROM {fromSql}";
    }

    /// <summary>Translate a scan step assignment into a SQL window expression.</summary>
    private string TranslateAssignment(
        string column,
        Expression value,
        string stepName,
        Dictionary<string, string> defaults)
    {
        // Pattern 1: Forward fill — col = iff(isempty(X), s.col, X)
        if (TryParseForwardFill(value, column, stepName, out var fillSource, out var castType, out var emptyKind))
        {
            var defaultSql = defaults.TryGetValue(column, out var def) ? def : "NULL";
            // Normalize "empty" values to NULL so LAST_VALUE IGNORE NULLS skips them
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
            if (IsStateRef(addBin.Left, stepName, column)) delta = addBin.Right;
            else if (IsStateRef(addBin.Right, stepName, column)) delta = addBin.Left;

            if (delta != null)
            {
                var deltaSql = Expr.ConvertExpression(delta);
                var defaultSql = defaults.TryGetValue(column, out var def) ? def : "0";
                return $"COALESCE(SUM({deltaSql}) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), {defaultSql})";
            }
        }

        // Pattern 3: Cumulative sum with reset —
        //   col = iff(s.col >= threshold, reset, s.col + delta)
        // Approximation using gaps-and-islands with prior-row state.
        if (value is FunctionCallExpression iffFce &&
            (iffFce.Name.ToString().Trim().ToLowerInvariant() is "iff" or "iif") &&
            iffFce.ArgumentList.Expressions.Count == 3)
        {
            var cond = iffFce.ArgumentList.Expressions[0].Element;
            var resetValue = iffFce.ArgumentList.Expressions[1].Element;
            var contValue = iffFce.ArgumentList.Expressions[2].Element;

            if (ContainsStateRef(cond, stepName, column) &&
                contValue is BinaryExpression contAdd && contAdd.Kind == SyntaxKind.AddExpression)
            {
                Expression? delta = null;
                if (IsStateRef(contAdd.Left, stepName, column)) delta = contAdd.Right;
                else if (IsStateRef(contAdd.Right, stepName, column)) delta = contAdd.Left;

                if (delta != null)
                {
                    var deltaSql = Expr.ConvertExpression(delta);
                    var resetSql = Expr.ConvertExpression(resetValue);
                    // Approximate prior cumsum by excluding current row from window.
                    var priorCumsum = $"COALESCE(SUM({deltaSql}) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)";
                    var condSql = SubstituteStateRef(cond, stepName, column, priorCumsum);
                    return $"CASE WHEN {condSql} THEN {resetSql} ELSE {priorCumsum} + {deltaSql} END";
                }
            }
        }

        // Fallback for `col = s.col` alone → return default
        if (IsStateRef(value, stepName, column))
        {
            return defaults.TryGetValue(column, out var def) ? def : "NULL";
        }

        // Best-effort: convert the expression directly (loses stateful semantics)
        try { return Expr.ConvertExpression(value); }
        catch
        {
            return defaults.TryGetValue(column, out var def) ? def : "NULL";
        }
    }

    /// <summary>True if expr is `stepName.column` — a state reference to the prior value of `column`.</summary>
    private static bool IsStateRef(Expression expr, string stepName, string column)
    {
        if (expr is PathExpression pe &&
            pe.Expression is NameReference nr &&
            string.Equals(nr.Name.ToString().Trim(), stepName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(pe.Selector.ToString().Trim(), column, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }
        return false;
    }

    private static bool ContainsStateRef(Expression expr, string stepName, string column)
    {
        if (IsStateRef(expr, stepName, column)) return true;
        foreach (var pe in expr.GetDescendants<PathExpression>())
        {
            if (pe.Expression is NameReference nr &&
                string.Equals(nr.Name.ToString().Trim(), stepName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(pe.Selector.ToString().Trim(), column, StringComparison.OrdinalIgnoreCase))
                return true;
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
        var fname = fce.Name.ToString().Trim().ToLowerInvariant();
        if (fname != "iff" && fname != "iif") return false;
        if (fce.ArgumentList.Expressions.Count != 3) return false;

        var cond = fce.ArgumentList.Expressions[0].Element;
        var trueBranch = fce.ArgumentList.Expressions[1].Element;
        var falseBranch = fce.ArgumentList.Expressions[2].Element;

        // Condition must be isempty(X) or isnull(X)
        if (cond is not FunctionCallExpression condFce) return false;
        var condName = condFce.Name.ToString().Trim().ToLowerInvariant();
        if (condName != "isempty" && condName != "isnull") return false;
        if (condFce.ArgumentList.Expressions.Count != 1) return false;
        emptyKind = condName;

        var sourceExpr = condFce.ArgumentList.Expressions[0].Element;

        // True branch must be state reference s.col
        if (!IsStateRef(trueBranch, stepName, column)) return false;

        // False branch = same source (optionally cast)
        if (ExpressionsMatch(sourceExpr, falseBranch))
        {
            fillSource = Expr.ConvertExpression(sourceExpr);
            return true;
        }

        if (falseBranch is FunctionCallExpression falseFce && falseFce.ArgumentList.Expressions.Count == 1)
        {
            var castFname = falseFce.Name.ToString().Trim().ToLowerInvariant();
            var typeMap = new Dictionary<string, string>
            {
                ["toint"] = "INTEGER", ["tolong"] = "BIGINT",
                ["toreal"] = "DOUBLE", ["todouble"] = "DOUBLE",
                ["tostring"] = "VARCHAR", ["tobool"] = "BOOLEAN",
                ["todatetime"] = "TIMESTAMP", ["totimespan"] = "INTERVAL"
            };
            if (typeMap.TryGetValue(castFname, out var mappedType) &&
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
    /// Convert an expression to SQL, substituting `stepName.column` references with `replacement`.
    /// Uses a placeholder round-trip to avoid the JSON-path handling in ConvertExpression.
    /// </summary>
    private string SubstituteStateRef(Expression expr, string stepName, string column, string replacement)
    {
        // Walk the expression tree manually for simple binary/comparison patterns
        return ConvertWithStateSub(expr, stepName, column, replacement);
    }

    private string ConvertWithStateSub(Expression e, string stepName, string column, string replacement)
    {
        // Direct state ref: s.col → replacement
        if (IsStateRef(e, stepName, column)) return $"({replacement})";

        // Recursively handle comparisons and arithmetic containing state refs
        if (e is BinaryExpression bin)
        {
            var op = bin.Kind switch
            {
                SyntaxKind.GreaterThanExpression => ">",
                SyntaxKind.GreaterThanOrEqualExpression => ">=",
                SyntaxKind.LessThanExpression => "<",
                SyntaxKind.LessThanOrEqualExpression => "<=",
                SyntaxKind.EqualExpression => "=",
                SyntaxKind.NotEqualExpression => "<>",
                SyntaxKind.AddExpression => "+",
                SyntaxKind.SubtractExpression => "-",
                SyntaxKind.MultiplyExpression => "*",
                SyntaxKind.DivideExpression => "/",
                SyntaxKind.AndExpression => "AND",
                SyntaxKind.OrExpression => "OR",
                _ => ""
            };
            if (!string.IsNullOrEmpty(op))
            {
                var l = ConvertWithStateSub(bin.Left, stepName, column, replacement);
                var r = ConvertWithStateSub(bin.Right, stepName, column, replacement);
                return $"{l} {op} {r}";
            }
        }

        // Default: convert normally (no state refs expected in this branch)
        return Expr.ConvertExpression(e);
    }

    private static bool ExpressionsMatch(Expression a, Expression b)
    {
        return a.ToString().Trim() == b.ToString().Trim();
    }
}
