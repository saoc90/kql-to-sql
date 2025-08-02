using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Kusto.Language.Syntax;

namespace KqlToSql.Expressions;

internal static class ExpressionSqlBuilder
{
    private static readonly Dictionary<string, string> CastFunctionMap = new(StringComparer.OrdinalIgnoreCase)
    {
        ["tobool"] = "BOOLEAN",
        ["toboolean"] = "BOOLEAN",
        ["bool"] = "BOOLEAN",
        ["boolean"] = "BOOLEAN",
        ["toint"] = "INTEGER",
        ["int"] = "INTEGER",
        ["tolong"] = "BIGINT",
        ["long"] = "BIGINT",
        ["tostring"] = "TEXT",
        ["string"] = "TEXT",
        ["todouble"] = "DOUBLE",
        ["toreal"] = "DOUBLE",
        ["tofloat"] = "DOUBLE",
        ["double"] = "DOUBLE",
        ["real"] = "DOUBLE",
        ["float"] = "DOUBLE",
        ["todecimal"] = "DECIMAL",
        ["decimal"] = "DECIMAL",
        ["todatetime"] = "TIMESTAMP",
        ["datetime"] = "TIMESTAMP",
        ["totimespan"] = "INTERVAL",
        ["timespan"] = "INTERVAL"
    };
    internal static string ConvertExpression(Expression expr, string? leftAlias = null, string? rightAlias = null)
    {
        if (TryConvertDynamicAccess(expr, leftAlias, rightAlias, out var dynamicSql))
        {
            return dynamicSql;
        }

        return expr switch
        {
            BinaryExpression bin when bin.Kind == SyntaxKind.EqualExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} = {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.NotEqualExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} <> {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.EqualTildeExpression =>
                $"UPPER({ConvertExpression(bin.Left, leftAlias, rightAlias)}) = UPPER({ConvertExpression(bin.Right, leftAlias, rightAlias)})",
            BinaryExpression bin when bin.Kind == SyntaxKind.BangTildeExpression =>
                $"UPPER({ConvertExpression(bin.Left, leftAlias, rightAlias)}) <> UPPER({ConvertExpression(bin.Right, leftAlias, rightAlias)})",
            BinaryExpression bin when bin.Kind == SyntaxKind.GreaterThanExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} > {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.LessThanExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} < {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.GreaterThanOrEqualExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} >= {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.LessThanOrEqualExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} <= {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.AddExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} + {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.AndExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} AND {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.OrExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} OR {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.HasExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", false),
            BinaryExpression bin when bin.Kind == SyntaxKind.HasCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", true),
            BinaryExpression bin when bin.Kind == SyntaxKind.ContainsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", false),
            BinaryExpression bin when bin.Kind == SyntaxKind.ContainsCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", false, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", true, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotContainsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", false, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotContainsCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", true, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.StartsWithExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "", "%", false),
            BinaryExpression bin when bin.Kind == SyntaxKind.StartsWithCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "", "%", true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotStartsWithExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "", "%", false, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotStartsWithCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "", "%", true, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.EndsWithExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "", false),
            BinaryExpression bin when bin.Kind == SyntaxKind.EndsWithCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "", true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotEndsWithExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "", false, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotEndsWithCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "", true, true),
            BetweenExpression be when be.Kind == SyntaxKind.BetweenExpression =>
                ConvertBetween(be, leftAlias, rightAlias, false),
            BetweenExpression be when be.Kind == SyntaxKind.NotBetweenExpression =>
                ConvertBetween(be, leftAlias, rightAlias, true),
            InExpression inExpr => ConvertInExpression(inExpr, leftAlias, rightAlias),
            NameReference nr => nr.Name.ToString().Trim() switch
            {
                "$left" => leftAlias ?? "$left",
                "$right" => rightAlias ?? "$right",
                var name => name
            },
            PathExpression pe =>
                $"{ConvertExpression(pe.Expression, leftAlias, rightAlias)}.{pe.Selector}",
            LiteralExpression lit when lit.Kind == SyntaxKind.DateTimeLiteralExpression =>
                ConvertDateTimeLiteral(lit),
            LiteralExpression lit => lit.ToString().Trim(),
            FunctionCallExpression fce =>
                ConvertFunctionCall(fce, leftAlias, rightAlias),
            ParenthesizedExpression pe => $"({ConvertExpression(pe.Expression, leftAlias, rightAlias)})",
            _ => throw new NotSupportedException($"Unsupported expression {expr.Kind}")
        };
    }

    private static string ConvertFunctionCall(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        var name = fce.Name.ToString().Trim();
        var lower = name.ToLowerInvariant();
        if (CastFunctionMap.TryGetValue(lower, out var sqlType))
        {
            if (fce.ArgumentList.Expressions.Count != 1)
            {
                throw new NotSupportedException($"{name} expects exactly one argument");
            }
            var arg = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
            return $"CAST({arg} AS {sqlType})";
        }

        return lower switch
        {
            "bin" => ConvertBin(fce, leftAlias, rightAlias),
            "bag_pack" => $"json_object({string.Join(", ", fce.ArgumentList.Expressions.Select(a => ConvertExpression(a.Element, leftAlias, rightAlias)))})",
            "tolower" => $"LOWER({ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias)})",
            "toupper" => $"UPPER({ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias)})",
            "strlen" => $"LENGTH({ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias)})",
            "substring" => ConvertSubstring(fce, leftAlias, rightAlias),
            "now" => "NOW()",
            "ago" => ConvertAgo(fce, leftAlias, rightAlias),
            "iif" => ConvertIif(fce, leftAlias, rightAlias),
            "switch" => ConvertSwitch(fce, leftAlias, rightAlias),
            _ => $"{name}({string.Join(", ", fce.ArgumentList.Expressions.Select(a => ConvertExpression(a.Element, leftAlias, rightAlias)))})"
        };
    }

    private static string ConvertIif(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count != 3)
        {
            throw new NotSupportedException("iif() expects exactly three arguments");
        }

        var condition = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        var trueExpr = ConvertExpression(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
        var falseExpr = ConvertExpression(fce.ArgumentList.Expressions[2].Element, leftAlias, rightAlias);
        return $"CASE WHEN {condition} THEN {trueExpr} ELSE {falseExpr} END";
    }

    private static string ConvertSwitch(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        var args = fce.ArgumentList.Expressions;
        if (args.Count < 4 || args.Count % 2 != 0)
        {
            throw new NotSupportedException("switch() expects an expression, pairs of values and results, and a default result");
        }

        var expr = ConvertExpression(args[0].Element, leftAlias, rightAlias);
        var cases = new List<string>();
        for (var i = 1; i < args.Count - 1; i += 2)
        {
            var value = ConvertExpression(args[i].Element, leftAlias, rightAlias);
            var result = ConvertExpression(args[i + 1].Element, leftAlias, rightAlias);
            cases.Add($"WHEN {value} THEN {result}");
        }
        var defaultExpr = ConvertExpression(args[^1].Element, leftAlias, rightAlias);
        return $"CASE {expr} {string.Join(" ", cases)} ELSE {defaultExpr} END";
    }

    private static string ConvertSubstring(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count is < 2 or > 3)
        {
            throw new NotSupportedException("substring() expects two or three arguments");
        }

        var text = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        var start = ConvertExpression(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
        var startExpr = $"({start}) + 1";
        if (fce.ArgumentList.Expressions.Count == 3)
        {
            var length = ConvertExpression(fce.ArgumentList.Expressions[2].Element, leftAlias, rightAlias);
            return $"SUBSTR({text}, {startExpr}, {length})";
        }

        return $"SUBSTR({text}, {startExpr})";
    }

    private static string ConvertAgo(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count != 1)
        {
            throw new NotSupportedException("ago() expects one argument");
        }

        var argExpr = fce.ArgumentList.Expressions[0].Element;
        if (argExpr is LiteralExpression lit)
        {
            var text = lit.ToString().Trim().Trim('\'', '\"');
            if (TryParseTimespan(text, out var ms))
            {
                return $"NOW() - {ms} * INTERVAL '1 millisecond'";
            }
        }

        var arg = ConvertExpression(argExpr, leftAlias, rightAlias);
        return $"NOW() - ({arg}) * INTERVAL '1 millisecond'";
    }

    internal static string ConvertInExpression(InExpression inExpr, string? leftAlias, string? rightAlias)
    {
        var left = ConvertExpression(inExpr.Left, leftAlias, rightAlias);
        if (inExpr.Right is not ExpressionList list)
        {
            throw new NotSupportedException("Only expression lists are supported for in operator");
        }

        var items = list.Expressions.Select(e => ConvertExpression(e.Element, leftAlias, rightAlias)).ToArray();

        var caseInsensitive = inExpr.Kind == SyntaxKind.InCsExpression || inExpr.Kind == SyntaxKind.NotInCsExpression;
        if (caseInsensitive)
        {
            left = $"UPPER({left})";
            for (var i = 0; i < items.Length; i++)
            {
                var item = items[i];
                if (item.StartsWith("'", StringComparison.Ordinal) && item.EndsWith("'", StringComparison.Ordinal))
                {
                    items[i] = $"'{item[1..^1].ToUpperInvariant()}'";
                }
            }
        }

        var op = (inExpr.Kind == SyntaxKind.NotInExpression || inExpr.Kind == SyntaxKind.NotInCsExpression) ? "NOT IN" : "IN";
        return $"{left} {op} ({string.Join(", ", items)})";
    }

    internal static string ConvertBetween(BetweenExpression bin, string? leftAlias, string? rightAlias, bool negated)
    {
        var left = ConvertExpression(bin.Left, leftAlias, rightAlias);
        var couple = bin.Right;
        var lower = ConvertExpression(couple.First, leftAlias, rightAlias);
        var upper = ConvertExpression(couple.Second, leftAlias, rightAlias);
        var expr = $"{left} BETWEEN {lower} AND {upper}";
        return negated ? $"NOT ({expr})" : expr;
    }

    private static string ConvertLike(BinaryExpression bin, string? leftAlias, string? rightAlias, string prefix, string suffix, bool caseSensitive, bool negated = false)
    {
        var left = ConvertExpression(bin.Left, leftAlias, rightAlias);
        string pattern;
        if (bin.Right is LiteralExpression lit)
        {
            var text = lit.ToString().Trim().Trim('\'', '\"');
            pattern = $"'{prefix}{text}{suffix}'";
        }
        else
        {
            var right = ConvertExpression(bin.Right, leftAlias, rightAlias);
            var parts = new List<string>();
            if (!string.IsNullOrEmpty(prefix))
            {
                parts.Add($"'{prefix}'");
            }
            parts.Add(right);
            if (!string.IsNullOrEmpty(suffix))
            {
                parts.Add($"'{suffix}'");
            }
            pattern = string.Join(" || ", parts);
        }

        var like = caseSensitive ? "LIKE" : "ILIKE";
        if (negated)
        {
            return $"{left} NOT {like} {pattern}";
        }
        return $"{left} {like} {pattern}";
    }

    internal static string ConvertDateTimeLiteral(LiteralExpression lit)
    {
        var text = lit.ToString().Trim();
        if (text.StartsWith("datetime(", StringComparison.OrdinalIgnoreCase) && text.EndsWith(")"))
        {
            text = text[9..^1];
        }
        text = text.Trim('"', '\'');
        if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dt))
        {
            return $"TIMESTAMP '{dt:yyyy-MM-dd HH:mm:ss}'";
        }
        return $"TIMESTAMP '{text}'";
    }

    private static bool TryConvertDynamicAccess(Expression expr, string? leftAlias, string? rightAlias, out string sql)
    {
        sql = string.Empty;
        var segments = new Stack<string>();
        Expression current = expr;
        while (true)
        {
            switch (current)
            {
                case PathExpression pe:
                    if (pe.Expression is NameReference nr && (nr.Name.ToString().Trim() == "$left" || nr.Name.ToString().Trim() == "$right"))
                    {
                        return false;
                    }
                    segments.Push(pe.Selector.ToString().Trim());
                    current = pe.Expression;
                    continue;
                case ElementExpression ee:
                    if (ee.Selector is LiteralExpression litSel)
                    {
                        var key = litSel.ToString().Trim().Trim('\'', '"');
                        segments.Push(key);
                        current = ee.Expression;
                        continue;
                    }
                    if (ee.Selector is BracketedExpression be && be.Expression is LiteralExpression lit)
                    {
                        var key = lit.ToString().Trim().Trim('\'', '"');
                        segments.Push(key);
                        current = ee.Expression;
                        continue;
                    }
                    return false;
                default:
                    if (segments.Count == 0)
                    {
                        return false;
                    }
                    var baseSql = ConvertExpression(current, leftAlias, rightAlias);
                    var path = string.Join('.', segments);
                    sql = $"trim(both '\"' from json_extract({baseSql}, '$.{path}'))";
                    return true;
            }
        }
    }

    internal static string ConvertBin(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count != 2)
        {
            throw new NotSupportedException("bin() requires two arguments");
        }

        var value = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        var sizeExpr = fce.ArgumentList.Expressions[1].Element;

        if (sizeExpr is LiteralExpression lit)
        {
            var text = lit.ToString().Trim().Trim('"', '\'');
            if (TryParseTimespan(text, out var ms))
            {
                return $"TO_TIMESTAMP_MS(FLOOR(EPOCH_MS({value})/{ms})*{ms})";
            }
        }

        var size = ConvertExpression(sizeExpr, leftAlias, rightAlias);
        return $"FLOOR(({value})/({size}))*({size})";
    }

    internal static string ExtractLeftKey(Expression expr)
    {
        return expr switch
        {
            NameReference nr => nr.Name.ToString().Trim(),
            PathExpression pe when pe.Expression is NameReference nr && nr.Name.ToString().Trim() == "$left" => pe.Selector.ToString().Trim(),
            BinaryExpression be when be.Kind == SyntaxKind.EqualExpression => ExtractLeftKey(be.Left),
            _ => throw new NotSupportedException("Unsupported join key expression")
        };
    }

    internal static bool TryParseTimespan(string text, out long milliseconds)
    {
        milliseconds = 0;
        if (string.IsNullOrEmpty(text))
            return false;

        static bool ParseUnit(string number, long factor, out long ms)
        {
            ms = 0;
            if (long.TryParse(number, out var v))
            {
                ms = v * factor;
                return true;
            }
            return false;
        }

        return text switch
        {
            var t when t.EndsWith("ms", StringComparison.OrdinalIgnoreCase) && ParseUnit(t[..^2], 1, out milliseconds) => true,
            var t when t.EndsWith("s", StringComparison.OrdinalIgnoreCase) && ParseUnit(t[..^1], 1000, out milliseconds) => true,
            var t when t.EndsWith("m", StringComparison.OrdinalIgnoreCase) && ParseUnit(t[..^1], 60_000, out milliseconds) => true,
            var t when t.EndsWith("h", StringComparison.OrdinalIgnoreCase) && ParseUnit(t[..^1], 3_600_000, out milliseconds) => true,
            var t when t.EndsWith("d", StringComparison.OrdinalIgnoreCase) && ParseUnit(t[..^1], 86_400_000, out milliseconds) => true,
            _ => false
        };
    }
}

