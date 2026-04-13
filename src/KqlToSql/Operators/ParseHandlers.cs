using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

internal sealed class ParseHandlers : OperatorHandlerBase
{
    internal ParseHandlers(KqlToSqlConverter converter, ExpressionSqlBuilder expr)
        : base(converter, expr) { }

    internal string ApplyParse(string leftSql, ParseOperator parse)
    {
        var sourceExpr = Expr.ConvertExpression(parse.Expression);
        var (regex, captures) = BuildRegex(parse.Patterns);

        var columns = captures.Select((c, idx) =>
        {
            var extract = $"REGEXP_EXTRACT({sourceExpr}, '{regex}', {idx + 1})";
            if (c.Type != null && c.Type != "string")
                extract = $"CAST({extract} AS {Dialect.MapType(c.Type)})";
            return $"{extract} AS {c.Name}";
        }).ToArray();

        return AppendToSelectStar(leftSql, string.Join(", ", columns));
    }

    internal string ApplyParseWhere(string leftSql, ParseWhereOperator parseWhere)
    {
        var sourceExpr = Expr.ConvertExpression(parseWhere.Expression);
        var (regex, captures) = BuildRegex(parseWhere.Patterns);

        var columns = captures.Select((c, idx) =>
        {
            var rawExtract = $"REGEXP_EXTRACT({sourceExpr}, '{regex}', {idx + 1})";
            var selectExpr = rawExtract;
            if (c.Type != null && c.Type != "string")
                selectExpr = $"CAST({rawExtract} AS {Dialect.MapType(c.Type)})";
            return (SelectExpr: selectExpr, RawExtract: rawExtract, Name: c.Name);
        }).ToArray();

        var selectExprs = columns.Select(c => $"{c.SelectExpr} AS {c.Name}").ToArray();
        var filterConditions = columns.Select(c => $"{c.RawExtract} IS NOT NULL AND {c.RawExtract} <> ''").ToArray();

        return $"SELECT *, {string.Join(", ", selectExprs)} FROM {ExtractFrom(leftSql)} WHERE {string.Join(" AND ", filterConditions)}";
    }

    internal string ApplyParseKv(string leftSql, ParseKvOperator parseKv)
    {
        var sourceExpr = Expr.ConvertExpression(parseKv.Expression);

        var columns = new List<(string Name, string Type)>();
        if (parseKv.Keys is RowSchema schema)
        {
            foreach (var col in schema.Columns)
            {
                if (col.Element is NameAndTypeDeclaration nat)
                {
                    var name = nat.Name.ToString().Trim();
                    var type = nat.Type?.ToString().Trim().TrimStart(':').ToLowerInvariant() ?? "string";
                    columns.Add((name, type));
                }
            }
        }

        var extractExprs = columns.Select(c =>
        {
            var extract = $"REGEXP_EXTRACT({sourceExpr}, '{c.Name}=([^,;\\s]+)', 1)";
            if (c.Type != "string")
                extract = $"TRY_CAST({extract} AS {Dialect.MapType(c.Type)})";
            return $"{extract} AS {c.Name}";
        }).ToArray();

        return AppendToSelectStar(leftSql, string.Join(", ", extractExprs));
    }

    internal string ApplySearch(string leftSql, SearchOperator search)
    {
        var condition = ConvertSearchCondition(search.Condition);

        if (IsSimpleSelectStar(leftSql))
            return $"{leftSql} WHERE {condition}";
        return $"SELECT * FROM ({leftSql}) WHERE {condition}";
    }

    private string ConvertSearchCondition(Expression condition)
    {
        return condition switch
        {
            LiteralExpression lit when lit.Kind == SyntaxKind.StringLiteralExpression =>
                $"CAST(({Expr.ConvertExpression(lit)}) AS VARCHAR) IS NOT NULL",
            BinaryExpression bin when bin.Kind == SyntaxKind.HasExpression =>
                $"{Expr.ConvertExpression(bin.Left)} {Dialect.CaseInsensitiveLike} '%' || {Expr.ConvertExpression(bin.Right)} || '%'",
            BinaryExpression bin when bin.Kind == SyntaxKind.EqualExpression =>
                $"{Expr.ConvertExpression(bin.Left)} = {Expr.ConvertExpression(bin.Right)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.AndExpression =>
                $"{ConvertSearchCondition(bin.Left)} AND {ConvertSearchCondition(bin.Right)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.OrExpression =>
                $"{ConvertSearchCondition(bin.Left)} OR {ConvertSearchCondition(bin.Right)}",
            _ => Expr.ConvertExpression(condition)
        };
    }

    private (string Regex, List<(string Name, string? Type)> Captures) BuildRegex(SyntaxList<SyntaxNode> patterns)
    {
        var regexParts = new List<string>();
        var captures = new List<(string Name, string? Type)>();

        for (int i = 0; i < patterns.Count; i++)
        {
            var pattern = patterns[i];
            if (pattern is StarExpression)
            {
                regexParts.Add(".*?");
            }
            else if (pattern is LiteralExpression lit)
            {
                var text = lit.ToString().Trim().Trim('\'', '"');
                if (text.StartsWith("@"))
                    regexParts.Add(text.TrimStart('@').Trim('\'', '"'));
                else
                    regexParts.Add(Regex.Escape(text));
            }
            else if (pattern is NameAndTypeDeclaration nat)
            {
                var name = nat.Name.ToString().Trim();
                var type = nat.Type?.ToString().Trim().TrimStart(':').ToLowerInvariant();
                captures.Add((name, type));
                regexParts.Add(GetCaptureRegex(type));
            }
            else if (pattern is NameDeclaration nd)
            {
                var name = nd.Name.ToString().Trim();
                captures.Add((name, null));
                bool isLast = IsLastCapture(i, patterns);
                regexParts.Add(isLast ? "(.*)" : "(.*?)");
            }
        }

        return (string.Join("", regexParts).Replace("'", "''"), captures);
    }

    private static string GetCaptureRegex(string? type)
    {
        return type switch
        {
            "long" or "int" => @"(-?\d+)",
            "real" or "double" or "decimal" => @"(-?\d+\.?\d*)",
            "bool" or "boolean" => @"(true|false)",
            _ => "(.*?)"
        };
    }

    private static bool IsLastCapture(int index, SyntaxList<SyntaxNode> patterns)
    {
        for (int j = index + 1; j < patterns.Count; j++)
        {
            if (patterns[j] is NameDeclaration or NameAndTypeDeclaration)
                return false;
            if (patterns[j] is LiteralExpression)
                return false;
        }
        return true;
    }
}
