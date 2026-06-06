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
        var regexMode = IsRegexKind(parse.Parameters);
        var (regex, captures) = BuildRegex(parse.Patterns, regexMode);

        var columns = captures.Select((c, idx) =>
        {
            var extract = $"REGEXP_EXTRACT({sourceExpr}, '{regex}', {idx + 1})";
            if (c.Type != null && c.Type != "string")
                extract = Dialect.SafeCast(extract, Dialect.MapType(c.Type));
            return $"{extract} AS {c.Name}";
        }).ToArray();

        return AppendToSelectStar(leftSql, string.Join(", ", columns));
    }

    internal string ApplyParseWhere(string leftSql, ParseWhereOperator parseWhere)
    {
        var sourceExpr = Expr.ConvertExpression(parseWhere.Expression);
        var regexMode = IsRegexKind(parseWhere.Parameters);
        var (regex, captures) = BuildRegex(parseWhere.Patterns, regexMode);

        var columns = captures.Select((c, idx) =>
        {
            var rawExtract = $"REGEXP_EXTRACT({sourceExpr}, '{regex}', {idx + 1})";
            var selectExpr = rawExtract;
            if (c.Type != null && c.Type != "string")
                selectExpr = $"TRY_CAST({rawExtract} AS {Dialect.MapType(c.Type)})";
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
                    var name = nat.Name.SimpleName;
                    var type = nat.Type?.ToString().Trim().TrimStart(':').ToLowerInvariant() ?? "string";
                    columns.Add((name, type));
                }
            }
        }

        // Resolve the pair/kv delimiters. Kusto's parse-kv defaults are kv_delimiter='=' and
        // pair_delimiter=',' when no `with (...)` clause overrides them.
        var pairDelim = ",";
        var kvDelim = "=";
        if (parseKv.WithClause is ParseKvWithClause with)
        {
            foreach (var np in with.GetDescendants<NamedParameter>())
            {
                var optName = np.Name?.SimpleName;
                var optVal = (np.Expression as LiteralExpression)?.LiteralValue?.ToString();
                if (optVal == null) continue;
                if (string.Equals(optName, "pair_delimiter", StringComparison.OrdinalIgnoreCase))
                    pairDelim = optVal;
                else if (string.Equals(optName, "kv_delimiter", StringComparison.OrdinalIgnoreCase))
                    kvDelim = optVal;
            }
        }

        // Per key K: match "(start | pairDelim) optional-ws K optional-ws kvDelim optional-ws (value)"
        // where the value runs up to the next pair delimiter, then TRIM surrounding whitespace.
        // Group 1 is the leading boundary, group 2 is the captured value.
        var pairBoundary = RegexEscape(pairDelim);
        var pairInClass = RegexCharClassEscape(pairDelim);
        var kvLiteral = RegexEscape(kvDelim);

        var extractExprs = columns.Select(c =>
        {
            var keyLiteral = RegexEscape(c.Name);
            var regex = $@"(^|{pairBoundary})\s*{keyLiteral}\s*{kvLiteral}\s*([^{pairInClass}]*)".Replace("'", "''");
            var extract = $"TRIM(REGEXP_EXTRACT({sourceExpr}, '{regex}', 2))";
            if (c.Type != "string")
                extract = Dialect.SafeCast(extract, Dialect.MapType(c.Type));
            return $"{extract} AS {c.Name}";
        }).ToArray();

        return AppendToSelectStar(leftSql, string.Join(", ", extractExprs));
    }

    /// <summary>Escapes a literal delimiter for use as a normal regex fragment (RE2/DuckDB).</summary>
    private static string RegexEscape(string s) => Regex.Escape(s);

    /// <summary>Escapes a literal delimiter for safe use inside a regex character class <c>[^ ... ]</c>.
    /// Inside a class only <c>\</c>, <c>]</c>, <c>^</c> and <c>-</c> need escaping.</summary>
    private static string RegexCharClassEscape(string s)
    {
        var sb = new System.Text.StringBuilder(s.Length * 2);
        foreach (var ch in s)
        {
            if (ch is '\\' or ']' or '^' or '-')
                sb.Append('\\');
            sb.Append(ch);
        }
        return sb.ToString();
    }

    internal string ApplySearch(string leftSql, SearchOperator search)
    {
        bool caseSensitive = IsCaseSensitiveSearch(search.Parameters);
        var columns = ResolveSearchColumns(search);
        var condition = ConvertSearchCondition(search.Condition, columns, caseSensitive);

        // Kusto prepends a synthetic '$table' source column to every search result. For an inline
        // datatable / piped source it is the literal "search_arg0"; for a bare named-table source it
        // is the table name. Emit it as the first projected column.
        var tableName = SearchSourceTableName(search);
        var tableCol = $"'{tableName.Replace("'", "''")}' AS \"$table\"";

        // ExtractFromAsRelation returns the bare relation for a simple `SELECT * FROM <rel>` with no
        // trailing WHERE/ORDER/…; otherwise it parenthesizes the whole left query. Either form is a
        // valid FROM source for our new SELECT, so the search WHERE always attaches at the right scope.
        return $"SELECT {tableCol}, * FROM {ExtractFromAsRelation(leftSql)} WHERE {condition}";
    }

    /// <summary>True when the search was invoked with <c>kind=case_sensitive</c>. Kusto search is
    /// case-insensitive by default (kind=case_insensitive).</summary>
    private static bool IsCaseSensitiveSearch(SyntaxList<NamedParameter> parameters)
    {
        foreach (var p in parameters)
        {
            if (string.Equals(p.Name?.SimpleName, "kind", StringComparison.OrdinalIgnoreCase))
            {
                var val = (p.Expression as LiteralExpression)?.LiteralValue?.ToString()
                          ?? p.Expression?.ToString().Trim();
                return string.Equals(val, "case_sensitive", StringComparison.OrdinalIgnoreCase);
            }
        }
        return false;
    }

    /// <summary>The value Kusto reports in the synthetic <c>$table</c> column. A search over a bare
    /// named table reports the table name; any other source (inline datatable, piped expression,
    /// union, …) reports the constant "search_arg0".</summary>
    private static string SearchSourceTableName(SearchOperator search)
    {
        if (search.Parent is PipeExpression pipe && pipe.Expression is NameReference nr)
            return nr.SimpleName;
        return "search_arg0";
    }

    /// <summary>Enumerates the input column names feeding a search operator by re-analyzing the query
    /// with the bound semantic model. Returns an empty list when binding is unavailable, in which case
    /// bare-term search falls back to a whole-row text match.</summary>
    private static List<string> ResolveSearchColumns(SearchOperator search)
    {
        var result = new List<string>();
        try
        {
            var rootText = search.Root.ToString();
            var analyzed = Kusto.Language.KustoCode.ParseAndAnalyze(rootText);
            // Match the search operator positionally (there is normally a single one).
            var analyzedSearch = analyzed.Syntax.GetDescendants<SearchOperator>().FirstOrDefault();
            if (analyzedSearch?.Parent is PipeExpression pipe &&
                pipe.Expression.ResultType is Kusto.Language.Symbols.TableSymbol ts)
            {
                foreach (var col in ts.Columns)
                    result.Add(col.Name);
            }
        }
        catch
        {
            // Binding can fail for queries that reference unknown tables/functions — fall back.
        }
        return result;
    }

    private string ConvertSearchCondition(Expression condition, List<string> columns, bool caseSensitive)
    {
        switch (condition)
        {
            case StarExpression:
                // `search *` matches every row.
                return "1 = 1";

            case ParenthesizedExpression pe:
                return $"({ConvertSearchCondition(pe.Expression, columns, caseSensitive)})";

            // Bare string term — match the term (word-boundary, Kusto `has`-style) against any column.
            case LiteralExpression lit when lit.Kind == SyntaxKind.StringLiteralExpression:
                return TermMatchAnyColumn(lit.LiteralValue?.ToString() ?? "", columns, caseSensitive);

            // Column-scoped term: col:"value" (parsed as a binary SearchExpression with ':' operator).
            case BinaryExpression bin when bin.Kind == SyntaxKind.SearchExpression:
                return ColumnScopedTerm(bin, caseSensitive);

            case BinaryExpression bin when bin.Kind == SyntaxKind.AndExpression:
                return $"({ConvertSearchCondition(bin.Left, columns, caseSensitive)} AND {ConvertSearchCondition(bin.Right, columns, caseSensitive)})";

            case BinaryExpression bin when bin.Kind == SyntaxKind.OrExpression:
                return $"({ConvertSearchCondition(bin.Left, columns, caseSensitive)} OR {ConvertSearchCondition(bin.Right, columns, caseSensitive)})";

            // not(<search-term>) negates a term/sub-condition (recurse so a bare term stays a term match,
            // not NOT('string') which DuckDB can't cast to BOOL).
            case FunctionCallExpression nf when nf.Name.SimpleName.Equals("not", StringComparison.OrdinalIgnoreCase)
                                                && nf.ArgumentList.Expressions.Count == 1:
                return $"(NOT {ConvertSearchCondition(nf.ArgumentList.Expressions[0].Element, columns, caseSensitive)})";

            // Any other predicate (col == 'x', col > 1, col has 'y', col between (..), …) is a normal scalar predicate.
            default:
                return Expr.ConvertExpression(condition);
        }
    }

    /// <summary>Column-scoped search term <c>col:"value"</c> — a term match against a single column.</summary>
    private string ColumnScopedTerm(BinaryExpression bin, bool caseSensitive)
    {
        var colSql = Expr.ConvertExpression(bin.Left);
        if (bin.Right is LiteralExpression lit && lit.Kind == SyntaxKind.StringLiteralExpression)
            return TermMatch(colSql, lit.LiteralValue?.ToString() ?? "", caseSensitive);
        // Non-string RHS — fall back to equality.
        return $"{colSql} = {Expr.ConvertExpression(bin.Right)}";
    }

    /// <summary>Builds a predicate that is true when <paramref name="term"/> appears as a whole term in
    /// any of <paramref name="columns"/>. Kusto tokenizes on non-alphanumeric characters, so a term
    /// matches when it is delimited by string start/end or a non-alphanumeric boundary.</summary>
    private string TermMatchAnyColumn(string term, List<string> columns, bool caseSensitive)
    {
        if (columns.Count == 0)
            return "1 = 0"; // no columns to match against
        var parts = columns
            .Select(c => TermMatch(Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(c), term, caseSensitive))
            .ToArray();
        return parts.Length == 1 ? parts[0] : $"({string.Join(" OR ", parts)})";
    }

    /// <summary>A single-column whole-term match. Empty term matches every (non-null) row, as Kusto's
    /// empty search term does.</summary>
    private string TermMatch(string colSql, string term, bool caseSensitive)
    {
        var valueExpr = $"CAST({colSql} AS VARCHAR)";
        if (term.Length == 0)
            return $"{valueExpr} IS NOT NULL";

        // Word boundary = string start/end or any non-alphanumeric character (Kusto treats '_' as a
        // boundary too, so the class is strictly [A-Za-z0-9]).
        // Case-insensitive (the default) is implemented by lowercasing both the input and the term.
        var matchTerm = caseSensitive ? term : term.ToLowerInvariant();
        string pattern;
        if (matchTerm.Contains('*') || matchTerm.Contains('?'))
        {
            // Search wildcards: '*' matches any run of term characters, '?' a single one. A leading/
            // trailing '*' drops that side's term-boundary anchor (so web* = a term starting with "web").
            bool anchorStart = !matchTerm.StartsWith("*", StringComparison.Ordinal);
            bool anchorEnd = !matchTerm.EndsWith("*", StringComparison.Ordinal);
            var core = string.Concat(matchTerm.Select(ch => ch switch
            {
                '*' => "[A-Za-z0-9]*",
                '?' => "[A-Za-z0-9]",
                _ => Regex.Escape(ch.ToString()),
            }));
            pattern = ((anchorStart ? "(^|[^A-Za-z0-9])" : "") + core + (anchorEnd ? "([^A-Za-z0-9]|$)" : "")).Replace("'", "''");
        }
        else
        {
            var escaped = Regex.Escape(matchTerm);
            pattern = $"(^|[^A-Za-z0-9]){escaped}([^A-Za-z0-9]|$)".Replace("'", "''");
        }
        var subject = caseSensitive ? valueExpr : $"LOWER({valueExpr})";
        return $"REGEXP_MATCHES({subject}, '{pattern}')";
    }

    /// <summary>True when the parse/parse-where operator was invoked with <c>kind=regex</c>.
    /// In regex mode string literals are raw regex fragments (not escaped) and string column
    /// captures are greedy <c>(.*)</c>. In simple/relaxed mode literals are escaped and only the
    /// final unterminated string column is greedy.</summary>
    private static bool IsRegexKind(SyntaxList<NamedParameter> parameters)
    {
        foreach (var p in parameters)
        {
            if (string.Equals(p.Name?.SimpleName, "kind", StringComparison.OrdinalIgnoreCase))
            {
                var val = (p.Expression as LiteralExpression)?.LiteralValue?.ToString()
                          ?? p.Expression?.ToString().Trim();
                return string.Equals(val, "regex", StringComparison.OrdinalIgnoreCase);
            }
        }
        return false;
    }

    private (string Regex, List<(string Name, string? Type)> Captures) BuildRegex(
        SyntaxList<SyntaxNode> patterns, bool regexMode)
    {
        var regexParts = new List<string>();
        var captures = new List<(string Name, string? Type)>();

        for (int i = 0; i < patterns.Count; i++)
        {
            var pattern = patterns[i];
            if (pattern is StarExpression)
            {
                // A '*' placeholder skips an arbitrary (non-captured) span.
                regexParts.Add(".*?");
            }
            else if (pattern is LiteralExpression lit)
            {
                var text = lit.LiteralValue?.ToString() ?? "";
                // kind=regex: the literal is a raw regex fragment, used verbatim.
                // kind=simple/relaxed: the literal is matched literally, so escape regex metachars.
                regexParts.Add(regexMode ? text : Regex.Escape(text));
            }
            else if (pattern is NameAndTypeDeclaration nat)
            {
                var name = nat.Name.SimpleName;
                var type = nat.Type?.ToString().Trim().TrimStart(':').ToLowerInvariant();
                captures.Add((name, type));
                regexParts.Add(GetCaptureRegex(type, IsLastCapture(i, patterns), regexMode));
            }
            else if (pattern is NameDeclaration nd)
            {
                var name = nd.Name.SimpleName;
                captures.Add((name, null));
                regexParts.Add(GetCaptureRegex(null, IsLastCapture(i, patterns), regexMode));
            }
        }

        return (string.Join("", regexParts).Replace("'", "''"), captures);
    }

    /// <summary>Capture-group regex for a parse column.
    /// Typed numeric/bool columns always use their value-shaped pattern. String/untyped columns
    /// are greedy <c>(.*)</c> in regex mode, and in simple mode are greedy only when they are the
    /// last token (the "rest of the line" capture), otherwise non-greedy <c>(.*?)</c>.</summary>
    private static string GetCaptureRegex(string? type, bool isLast, bool regexMode)
    {
        switch (type)
        {
            case "long":
            case "int":
                return @"(-?\d+)";
            case "real":
            case "double":
            case "decimal":
                return @"(-?\d+\.?\d*)";
            case "bool":
            case "boolean":
                return @"(true|false)";
            default:
                // string / untyped: Kusto uses a greedy capture in regex mode and for the trailing
                // column in simple mode; intermediate simple-mode columns are non-greedy.
                return (regexMode || isLast) ? "(.*)" : "(.*?)";
        }
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
