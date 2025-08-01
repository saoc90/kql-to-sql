using System;
using System.Linq;
using Kusto.Language.Syntax;

namespace KqlToSql.Expressions;

internal static class ExpressionSqlBuilder
{
    internal static string ConvertExpression(Expression expr, string? leftAlias = null, string? rightAlias = null)
    {
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
                ConvertHas(bin, leftAlias, rightAlias, false),
            BinaryExpression bin when bin.Kind == SyntaxKind.HasCsExpression =>
                ConvertHas(bin, leftAlias, rightAlias, true),
            InExpression inExpr => ConvertInExpression(inExpr, leftAlias, rightAlias),
            NameReference nr => nr.Name.ToString().Trim() switch
            {
                "$left" => leftAlias ?? "$left",
                "$right" => rightAlias ?? "$right",
                var name => name
            },
            PathExpression pe =>
                $"{ConvertExpression(pe.Expression, leftAlias, rightAlias)}.{pe.Selector}",
            LiteralExpression lit => lit.ToString().Trim(),
            FunctionCallExpression fce =>
                fce.Name.ToString().Trim().ToLowerInvariant() switch
                {
                    "bin" => ConvertBin(fce, leftAlias, rightAlias),
                    _ => $"{fce.Name}({string.Join(", ", fce.ArgumentList.Expressions.Select(a => ConvertExpression(a.Element, leftAlias, rightAlias)))})"
                },
            ParenthesizedExpression pe => $"({ConvertExpression(pe.Expression, leftAlias, rightAlias)})",
            _ => throw new NotSupportedException($"Unsupported expression {expr.Kind}")
        };
    }

    internal static string ConvertInExpression(InExpression inExpr, string? leftAlias, string? rightAlias)
    {
        var left = ConvertExpression(inExpr.Left, leftAlias, rightAlias);
        if (inExpr.Right is not ExpressionList list)
        {
            throw new NotSupportedException("Only expression lists are supported for in operator");
        }

        var items = list.Expressions.Select(e => ConvertExpression(e.Element, leftAlias, rightAlias)).ToArray();

        if (inExpr.Kind == SyntaxKind.InCsExpression)
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

        return $"{left} IN ({string.Join(", ", items)})";
    }

    private static string ConvertHas(BinaryExpression bin, string? leftAlias, string? rightAlias, bool caseSensitive)
    {
        var left = ConvertExpression(bin.Left, leftAlias, rightAlias);
        string pattern;
        if (bin.Right is LiteralExpression lit)
        {
            var text = lit.ToString().Trim().Trim('\'', '\"');
            pattern = $"'%{text}%'";
        }
        else
        {
            var right = ConvertExpression(bin.Right, leftAlias, rightAlias);
            pattern = $"'%' || {right} || '%'";
        }

        var like = caseSensitive ? "LIKE" : "ILIKE";
        return $"{left} {like} {pattern}";
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

