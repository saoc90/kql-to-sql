using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Kusto.Language.Syntax;

namespace KqlToSql.Expressions;

internal class ExpressionSqlBuilder
{
    private readonly ISqlDialect _dialect;
    private Func<SyntaxNode, string>? _nodeConverter;
    private Dictionary<string, string>? _scalarLets;

    internal ExpressionSqlBuilder(ISqlDialect dialect)
    {
        _dialect = dialect;
    }

    /// <summary>Sets the callback used to convert full KQL sub-expressions (e.g. toscalar pipelines).</summary>
    internal void SetNodeConverter(Func<SyntaxNode, string> converter) => _nodeConverter = converter;

    /// <summary>Sets the scalar let bindings for inline substitution.</summary>
    internal void SetScalarLets(Dictionary<string, string> scalarLets) => _scalarLets = scalarLets;

    private Dictionary<string, (string sql, bool materialized)>? _ctes;
    /// <summary>Sets the CTE definitions for body-inspection heuristics (e.g. detecting array-producing CTEs).</summary>
    internal void SetCtes(Dictionary<string, (string sql, bool materialized)> ctes) => _ctes = ctes;

    private readonly HashSet<string> _intervalColumns = new(StringComparer.OrdinalIgnoreCase);
    /// <summary>Records a column as interval-typed (from extend/project) so later sum() calls know to use epoch math.</summary>
    internal void MarkIntervalColumn(string name) => _intervalColumns.Add(name);
    /// <summary>Returns true if the named column was previously marked as interval-typed.</summary>
    internal bool IsIntervalColumn(string name) => _intervalColumns.Contains(name);

    private Dictionary<string, (string[] paramNames, Kusto.Language.Syntax.FunctionBody body)>? _userFunctions;
    /// <summary>Sets user-defined parameterized functions for inline expansion.</summary>
    internal void SetUserFunctions(Dictionary<string, (string[] paramNames, Kusto.Language.Syntax.FunctionBody body)> funcs) => _userFunctions = funcs;

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
        ["timespan"] = "INTERVAL",
        ["toguid"] = "UUID",
        ["guid"] = "UUID"
    };
    internal string ConvertExpression(Expression expr, string? leftAlias = null, string? rightAlias = null)
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
            BinaryExpression bin when bin.Kind == SyntaxKind.SubtractExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} - {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.MultiplyExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} * {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.DivideExpression =>
                ConvertDivide(bin, leftAlias, rightAlias),
            BinaryExpression bin when bin.Kind == SyntaxKind.ModuloExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} % {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.AndExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} AND {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.OrExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} OR {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.MatchesRegexExpression =>
                TryConvertRegexToLike(bin, leftAlias, rightAlias)
                ?? $"REGEXP_MATCHES({ConvertExpression(bin.Left, leftAlias, rightAlias)}, {ConvertExpression(bin.Right, leftAlias, rightAlias)})",
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
            BinaryExpression bin when bin.Kind == SyntaxKind.HasPrefixExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", false),
            BinaryExpression bin when bin.Kind == SyntaxKind.HasPrefixCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasPrefixExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", false, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasPrefixCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", true, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.HasSuffixExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", false),
            BinaryExpression bin when bin.Kind == SyntaxKind.HasSuffixCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasSuffixExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", false, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasSuffixCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", true, true),
            HasAnyExpression hae =>
                ConvertHasAny(hae, leftAlias, rightAlias, false),
            HasAllExpression hae =>
                ConvertHasAll(hae, leftAlias, rightAlias, false),
            BetweenExpression be when be.Kind == SyntaxKind.BetweenExpression =>
                ConvertBetween(be, leftAlias, rightAlias, false),
            BetweenExpression be when be.Kind == SyntaxKind.NotBetweenExpression =>
                ConvertBetween(be, leftAlias, rightAlias, true),
            InExpression inExpr => ConvertInExpression(inExpr, leftAlias, rightAlias),
            NameReference nr => ResolveNameReference(nr, leftAlias, rightAlias),
            NameDeclaration nd => nd.Name.ToString().Trim(),
            PathExpression pe =>
                $"{ConvertExpression(pe.Expression, leftAlias, rightAlias)}.{pe.Selector}",
            LiteralExpression lit when lit.Kind == SyntaxKind.DateTimeLiteralExpression =>
                ConvertDateTimeLiteral(lit),
            LiteralExpression lit when lit.Kind == SyntaxKind.TimespanLiteralExpression =>
                ConvertTimespanLiteral(lit),
            LiteralExpression lit when lit.Kind == SyntaxKind.StringLiteralExpression =>
                ConvertStringLiteral(lit),
            LiteralExpression lit when lit.Kind == SyntaxKind.NullLiteralExpression => "NULL",
            LiteralExpression lit => ConvertNumericOrOtherLiteral(lit),
            CompoundStringLiteralExpression cs => ConvertCompoundString(cs),
            PrefixUnaryExpression pu when pu.Kind == SyntaxKind.UnaryMinusExpression =>
                $"(-{ConvertExpression(pu.Expression, leftAlias, rightAlias)})",
            PrefixUnaryExpression pu when pu.Kind == SyntaxKind.UnaryPlusExpression =>
                ConvertExpression(pu.Expression, leftAlias, rightAlias),
            ToScalarExpression tse => ConvertToScalar(tse, leftAlias, rightAlias),
            FunctionCallExpression fce =>
                ConvertFunctionCall(fce, leftAlias, rightAlias),
            ParenthesizedExpression pe => $"({ConvertExpression(pe.Expression, leftAlias, rightAlias)})",
            DynamicExpression de => ConvertDynamic(de, leftAlias, rightAlias),
            SimpleNamedExpression sne2 => $"{ConvertExpression(sne2.Expression, leftAlias, rightAlias)} AS {sne2.Name.ToString().Trim()}",
            CompoundNamedExpression cne2 => ConvertExpression(cne2.Expression, leftAlias, rightAlias),
            PipeExpression pe2 when _nodeConverter != null => $"({_nodeConverter(pe2)})",
            ElementExpression ee => ConvertElementAccess(ee, leftAlias, rightAlias),
            BracketedExpression be => ConvertExpression(be.Expression, leftAlias, rightAlias),
            JsonArrayExpression jae => $"LIST_VALUE({string.Join(", ", jae.Values.Select(v => ConvertExpression(v.Element, leftAlias, rightAlias)))})",
            JsonObjectExpression joe => $"'{joe}'::JSON",
            _ => throw new NotSupportedException($"Unsupported expression {expr.Kind}")
        };
    }

    private static readonly HashSet<string> KnownScalarFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "bin", "substring", "ago", "iif", "iff", "case", "bin_at", "trim", "trim_start", "trim_end",
        "strcat_delim", "extract", "toscalar", "round", "datetime_add", "datetime_diff",
        "strlen", "tolower", "toupper", "now", "pack_array", "abs", "acos", "asin", "atan", "atan2",
        "ceiling", "floor", "sqrt", "exp", "log", "log10", "pow", "rand", "parse_json", "todynamic",
        "format_datetime", "isnull", "isempty", "coalesce", "not", "isnotempty", "isnotnull",
        "strcat", "replace_string", "indexof", "countof", "reverse", "split", "sign", "log2",
        "cos", "sin", "tan", "min_of", "max_of", "row_number", "prev", "next",
        "dayofweek", "dayofmonth", "dayofyear", "getmonth", "getyear", "monthofyear", "weekofyear",
        "hourofday", "minuteofhour", "secondofminute", "make_datetime", "make_timespan",
        "startofday", "startofweek", "startofmonth", "startofyear",
        "endofday", "endofweek", "endofmonth", "endofyear",
        "datetime_part", "format_timespan", "parse_url", "hash", "hash_md5",
        "toreal", "todouble", "toint", "tolong", "tostring", "tobool",
        "binary_and", "binary_or", "binary_xor", "binary_not",
        "totimespan", "timespan", "todecimal", "tofloat",
        "format_datetime", "format_timespan", "datetime_add", "datetime_diff", "datetime_part",
        "startofday", "startofweek", "startofmonth", "startofyear",
        "endofday", "endofweek", "endofmonth", "endofyear",
        "replace_string", "replace_regex", "indexof", "countof", "split", "extract", "extract_all",
        "strcat", "strcat_delim", "substring", "trim", "trim_start", "trim_end",
        "coalesce", "isnull", "isempty", "isnotempty", "isnotnull",
        "parse_json", "todynamic", "parse_url", "parse_csv",
        "array_length", "array_index_of", "array_sort_asc", "array_sort_desc",
        "bag_keys", "bag_has_key", "set_has_element",
        "hash", "hash_md5", "hash_sha256",
    };

    internal bool IsKnownScalarFunction(string name) =>
        KnownScalarFunctions.Contains(name) ||
        (_userFunctions != null &&
         (_userFunctions.ContainsKey(name) ||
          _userFunctions.Keys.Any(k => string.Equals(k, name, StringComparison.OrdinalIgnoreCase))));

    private string ConvertElementAccess(ElementExpression ee, string? leftAlias, string? rightAlias)
    {
        var baseExpr = ConvertExpression(ee.Expression, leftAlias, rightAlias);
        // The selector is a BracketedExpression wrapping the index
        var indexExpr = ee.Selector is BracketedExpression be
            ? ConvertExpression(be.Expression, leftAlias, rightAlias)
            : ConvertExpression(ee.Selector, leftAlias, rightAlias);

        // If the base is a JSON object (e.g. from dynamic({...})), the bracket access needs a json path lookup
        // rather than LIST indexing. Detect the ::JSON suffix produced by ConvertDynamic.
        if (baseExpr.TrimEnd().EndsWith("::JSON", StringComparison.OrdinalIgnoreCase))
        {
            return $"json_extract_string({baseExpr}, '$.' || {indexExpr})";
        }

        // KQL uses 0-based indexing, DuckDB LIST uses 1-based
        return $"{baseExpr}[{indexExpr} + 1]";
    }

    private string ConvertDynamic(DynamicExpression de, string? leftAlias, string? rightAlias)
    {
        // dynamic(["a","b"]) → LIST_VALUE('a', 'b') for DuckDB arrays
        // dynamic(null) → NULL
        var inner = de.GetDescendants<JsonArrayExpression>().FirstOrDefault();
        if (inner != null)
        {
            var elements = inner.Values
                .Select(v => ConvertExpression(v.Element, leftAlias, rightAlias))
                .ToArray();
            return $"LIST_VALUE({string.Join(", ", elements)})";
        }

        // dynamic({key:val}) → JSON object
        var obj = de.GetDescendants<JsonObjectExpression>().FirstOrDefault();
        if (obj != null)
        {
            return $"'{de.ToString().Trim()}'::JSON";
        }

        // dynamic(null)
        var text = de.ToString().Trim();
        if (text.Contains("null", StringComparison.OrdinalIgnoreCase))
            return "NULL";

        return $"'{text}'::JSON";
    }

    private static readonly HashSet<string> DuckDbReservedWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "end", "order", "group", "having", "where", "select", "from", "join", "union",
        "case", "when", "then", "else", "and", "or", "not", "null", "true", "false",
        "as", "on", "in", "between", "like", "is", "by", "asc", "desc", "distinct",
        "primary", "foreign", "key", "references", "all", "any", "some", "exists",
        "limit", "offset", "with", "into", "values", "inner", "outer", "left", "right",
        "full", "cross", "natural", "using", "window", "over", "partition"
    };

    internal static string QuoteIdentifierIfReserved(string name)
    {
        // Strip existing brackets like ["foo"] or ['foo']
        if (name.StartsWith("[") && name.EndsWith("]"))
        {
            name = name.Substring(1, name.Length - 2);
            if ((name.StartsWith("\"") && name.EndsWith("\"")) ||
                (name.StartsWith("'") && name.EndsWith("'")))
                name = name.Substring(1, name.Length - 2);
        }
        if (name.StartsWith("\"") && name.EndsWith("\""))
            return name;
        bool needsQuoting = DuckDbReservedWords.Contains(name) || NeedsIdentifierQuoting(name);
        return needsQuoting ? $"\"{name.Replace("\"", "\"\"")}\"" : name;
    }

    private static bool NeedsIdentifierQuoting(string name)
    {
        if (string.IsNullOrEmpty(name)) return false;
        if (!(char.IsLetter(name[0]) || name[0] == '_')) return true;
        foreach (var c in name)
            if (!(char.IsLetterOrDigit(c) || c == '_')) return true;
        return false;
    }

    private string ResolveNameReference(NameReference nr, string? leftAlias, string? rightAlias)
    {
        var name = nr.Name.ToString().Trim();
        if (name == "$left") return leftAlias ?? "$left";
        if (name == "$right") return rightAlias ?? "$right";
        if (_scalarLets != null && _scalarLets.TryGetValue(name, out var scalarSql))
            return scalarSql;
        return QuoteIdentifierIfReserved(name);
    }

    private static string ConvertStringLiteral(LiteralExpression lit)
    {
        var text = lit.ToString().Trim().Trim('"', '\'');
        text = text.Replace("'", "''");
        return $"'{text}'";
    }

    private static string ConvertCompoundString(CompoundStringLiteralExpression cs)
    {
        // KQL escaped quotes: 'O''Brien' is parsed as CompoundStringLiteralExpression
        var text = cs.ToString().Trim().Trim('\'', '"');
        text = text.Replace("''", "'").Replace("'", "''");
        return $"'{text}'";
    }

    private static string ConvertTimespanLiteral(LiteralExpression lit)
    {
        var text = lit.ToString().Trim();
        if (TryParseTimespan(text, out var ms))
        {
            return $"({ms} * INTERVAL '1 millisecond')";
        }
        return $"INTERVAL '{text}'";
    }

    private string ConvertFunctionCall(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        var name = fce.Name.ToString().Trim();
        var lower = name.ToLowerInvariant();
        if (CastFunctionMap.TryGetValue(lower, out var sqlType))
        {
            if (fce.ArgumentList.Expressions.Count != 1)
            {
                throw new NotSupportedException($"{name} expects exactly one argument");
            }
            // Strip SimpleNamedExpression wrapper — inner 'alias = expr' would leak 'AS alias' into
            // the CAST expression (producing invalid 'CAST(X AS alias AS TIMESTAMP)').
            var argNode = fce.ArgumentList.Expressions[0].Element is SimpleNamedExpression sneArg
                ? sneArg.Expression
                : fce.ArgumentList.Expressions[0].Element;
            var arg = ConvertExpression(argNode, leftAlias, rightAlias);
            // KQL type conversions return null on failure (e.g. tolong('') → null)
            return _dialect.SafeCast(arg, sqlType);
        }

        // Handle functions with structural conversion logic
        switch (lower)
        {
            case "bin": return ConvertBin(fce, leftAlias, rightAlias);
            case "substring": return ConvertSubstring(fce, leftAlias, rightAlias);
            case "ago": return ConvertAgo(fce, leftAlias, rightAlias);
            case "iif" or "iff": return ConvertIif(fce, leftAlias, rightAlias);
            case "case": return ConvertCase(fce, leftAlias, rightAlias);
            case "bin_at": return ConvertBinAt(fce, leftAlias, rightAlias);
            case "trim": return ConvertTrim(fce, leftAlias, rightAlias);
            case "trim_start": return ConvertTrimStart(fce, leftAlias, rightAlias);
            case "trim_end": return ConvertTrimEnd(fce, leftAlias, rightAlias);
            case "strcat_delim": return ConvertStrcatDelim(fce, leftAlias, rightAlias);
            case "extract": return ConvertExtract(fce, leftAlias, rightAlias);
            case "toscalar": return ConvertToscalar(fce, leftAlias, rightAlias);
            case "round": return ConvertRound(fce, leftAlias, rightAlias);
            case "datetime_add": return ConvertDatetimeAdd(fce, leftAlias, rightAlias);
            case "datetime_diff": return ConvertDatetimeDiff(fce, leftAlias, rightAlias);
        }

        // Delegate to dialect for engine-specific function translation.
        // Strip SimpleNamedExpression wrappers from function arguments — ConvertExpression would
        // otherwise emit "X AS alias" *inside* the function call, which is invalid SQL.
        var args = fce.ArgumentList.Expressions
            .Select(a => ConvertExpression(
                a.Element is SimpleNamedExpression snArg ? snArg.Expression : a.Element,
                leftAlias, rightAlias))
            .ToArray();

        // sum/sumif on an interval column — rewrite to epoch-ms arithmetic so it type-checks in DuckDB.
        if ((lower == "sum" || lower == "sumif") && args.Length >= 1 && IsBareIdentifier(args[0]) && IsIntervalColumn(args[0]))
        {
            var ms = $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + {args[0]} AS TIMESTAMP))";
            var filter = lower == "sumif" && args.Length >= 2 ? args[1] : null;
            var inner = filter is null ? $"SUM({ms})" : $"SUM({ms}) FILTER (WHERE {filter})";
            return $"(({inner}) * INTERVAL '1 millisecond')";
        }

        var dialectResult = _dialect.TryTranslateFunction(lower, args);
        if (dialectResult != null)
        {
            return dialectResult;
        }

        // User-defined parameterized function: inline the body with arg substitution
        var actualName = name;
        if (_userFunctions != null && !_userFunctions.ContainsKey(name))
        {
            var match = _userFunctions.Keys.FirstOrDefault(k => string.Equals(k, name, StringComparison.OrdinalIgnoreCase));
            if (match != null) actualName = match;
        }
        if (_userFunctions != null && _userFunctions.TryGetValue(actualName, out var userFunc))
        {
            var savedScalars = new Dictionary<string, string>();
            for (int i = 0; i < Math.Min(userFunc.paramNames.Length, args.Length); i++)
            {
                var pname = userFunc.paramNames[i];
                if (_scalarLets != null && _scalarLets.TryGetValue(pname, out var prev))
                    savedScalars[pname] = prev;
                _scalarLets ??= new Dictionary<string, string>();
                _scalarLets[pname] = args[i];
            }

            // Process nested let statements in the function body as scalar bindings
            var nestedLets = userFunc.body.GetDescendants<LetStatement>().ToList();
            var addedLets = new List<string>();
            foreach (var ls in nestedLets)
            {
                var lname = ls.Name.ToString().Trim();
                try
                {
                    // Try to convert as a scalar binding
                    if (ls.Expression is LiteralExpression || ls.Expression is FunctionCallExpression ||
                        ls.Expression is BinaryExpression || ls.Expression is ElementExpression ||
                        ls.Expression is DynamicExpression)
                    {
                        _scalarLets[lname] = ConvertExpression(ls.Expression, leftAlias, rightAlias);
                        addedLets.Add(lname);
                    }
                }
                catch { }
            }

            // Find the result expression: prefer FunctionBody.Expression (the authoritative result
            // expression), then fall back to node converter or name.
            string result;
            try
            {
                if (userFunc.body.Expression != null)
                    result = ConvertExpression(userFunc.body.Expression, leftAlias, rightAlias);
                else if (_nodeConverter != null)
                    result = _nodeConverter(userFunc.body);
                else
                    result = name;
            }
            catch
            {
                result = name;
            }

            // Restore previous scalar values for params
            foreach (var pname in userFunc.paramNames)
            {
                if (savedScalars.TryGetValue(pname, out var prev))
                    _scalarLets![pname] = prev;
                else
                    _scalarLets?.Remove(pname);
            }
            // Remove scoped nested let bindings
            foreach (var lname in addedLets) _scalarLets?.Remove(lname);

            return result;
        }

        // Fallback: pass through as-is
        return $"{name}({string.Join(", ", args)})";
    }

    private string ConvertIif(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count != 3)
        {
            throw new NotSupportedException("iif() expects exactly three arguments");
        }

        var condition = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        // KQL permits inline aliasing of the true/false branch: iif(cond, alias = X, Y) / iif(cond, X, alias = Y).
        // SimpleNamedExpression inside function args would naively emit "X AS alias" *inside* the CASE branch,
        // producing invalid SQL (CASE WHEN ... THEN X AS alias ELSE ... END). Extract aliases and attach
        // them to the outer CASE expression instead.
        var (trueExpr, trueAlias) = ConvertBranchExtractAlias(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
        var (falseExpr, falseAlias) = ConvertBranchExtractAlias(fce.ArgumentList.Expressions[2].Element, leftAlias, rightAlias);
        (trueExpr, falseExpr) = AlignStringBranches(trueExpr, falseExpr);
        var caseSql = $"CASE WHEN {condition} THEN {trueExpr} ELSE {falseExpr} END";
        var outerAlias = trueAlias ?? falseAlias;
        return outerAlias != null ? $"{caseSql} AS {QuoteIdentifierIfReserved(outerAlias)}" : caseSql;
    }

    private (string Expr, string? Alias) ConvertBranchExtractAlias(Expression expr, string? leftAlias, string? rightAlias)
    {
        if (expr is SimpleNamedExpression sne)
        {
            var inner = ConvertExpression(sne.Expression, leftAlias, rightAlias);
            return (inner, sne.Name.ToString().Trim());
        }
        return (ConvertExpression(expr, leftAlias, rightAlias), null);
    }

    private static bool IsStringLiteral(string expr) =>
        expr.StartsWith('\'') && expr.EndsWith('\'');

    private static bool IsBareIdentifier(string expr) =>
        System.Text.RegularExpressions.Regex.IsMatch(expr, @"^[A-Za-z_][A-Za-z0-9_]*$");

    private static string CoerceBranch(string expr, string t) =>
        IsBareIdentifier(expr) ? $"TRY_CAST({expr} AS {t})" : expr;

    private static (string True, string False) AlignStringBranches(string trueExpr, string falseExpr)
    {
        bool trueIsString = IsStringLiteral(trueExpr);
        bool falseIsString = IsStringLiteral(falseExpr);
        if (trueIsString && IsBareIdentifier(falseExpr))
            return (trueExpr, $"TRY_CAST({falseExpr} AS VARCHAR)");
        if (falseIsString && IsBareIdentifier(trueExpr))
            return ($"TRY_CAST({trueExpr} AS VARCHAR)", falseExpr);
        return (trueExpr, falseExpr);
    }

    // Detect pattern: isnotempty(A) / isempty(A) paired with THEN=A → A is confirmed VARCHAR.
    // Returns identifiers known to be VARCHAR from such pairs.
    private static HashSet<string> StringContextFromConditions(IReadOnlyList<SeparatedElement<Expression>> args)
    {
        var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Count - 1; i += 2)
            if (args[i].Element is FunctionCallExpression c &&
                c.Name.ToString().Trim().ToLowerInvariant() is "isnotempty" or "isempty" &&
                c.ArgumentList.Expressions.Count == 1 &&
                c.ArgumentList.Expressions[0].Element is NameReference ca &&
                args[i + 1].Element is NameReference th &&
                ca.Name.ToString().Trim() == th.Name.ToString().Trim())
                ids.Add(th.Name.ToString().Trim());
        return ids;
    }

    private string ConvertCase(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        var args = fce.ArgumentList.Expressions;
        if (args.Count < 3 || args.Count % 2 == 0)
        {
            throw new NotSupportedException("case() expects pairs of conditions and results, and a default result");
        }

        var cases = new List<string>();
        var thenExprs = new List<string>();
        for (var i = 0; i < args.Count - 1; i += 2)
        {
            thenExprs.Add(ConvertExpression(args[i + 1].Element, leftAlias, rightAlias));
        }
        var defaultExpr = ConvertExpression(args[^1].Element, leftAlias, rightAlias);

        // If any THEN/ELSE branch is a string literal, cast all bare-identifier branches to VARCHAR.
        var allBranches = thenExprs.Append(defaultExpr).ToList();
        bool hasStringLiteral = allBranches.Any(IsStringLiteral);
        if (hasStringLiteral)
        {
            thenExprs = thenExprs.Select(e => CoerceBranch(e, "VARCHAR")).ToList();
            defaultExpr = CoerceBranch(defaultExpr, "VARCHAR");
        }
        else
        {
            // Pattern: case(isnotempty(A), A, B) — A is VARCHAR; wrap only branches that are bare ids != A.
            var strIds = StringContextFromConditions(args);
            if (strIds.Count > 0)
            {
                thenExprs = thenExprs.Select(e => !strIds.Contains(e) ? CoerceBranch(e, "VARCHAR") : e).ToList();
                if (!strIds.Contains(defaultExpr)) defaultExpr = CoerceBranch(defaultExpr, "VARCHAR");
            }
        }

        for (var i = 0; i < args.Count - 1; i += 2)
        {
            var condition = ConvertExpression(args[i].Element, leftAlias, rightAlias);
            cases.Add($"WHEN {condition} THEN {thenExprs[i / 2]}");
        }
        return $"CASE {string.Join(" ", cases)} ELSE {defaultExpr} END";
    }

    private string ConvertSubstring(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count is < 2 or > 3)
        {
            throw new NotSupportedException("substring() expects two or three arguments");
        }

        var text = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        var start = ConvertExpression(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
        var startExpr = $"({start}) + 1";
        // KQL substring() accepts numeric/other types and coerces to string; DuckDB's SUBSTR requires VARCHAR.
        var textCast = $"CAST({text} AS VARCHAR)";
        if (fce.ArgumentList.Expressions.Count == 3)
        {
            var length = ConvertExpression(fce.ArgumentList.Expressions[2].Element, leftAlias, rightAlias);
            return $"SUBSTR({textCast}, {startExpr}, {length})";
        }

        return $"SUBSTR({textCast}, {startExpr})";
    }

    private string ConvertAgo(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
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

    internal string ConvertInExpression(InExpression inExpr, string? leftAlias, string? rightAlias)
    {
        var left = ConvertExpression(inExpr.Left, leftAlias, rightAlias);
        var list = inExpr.Right;

        // When the RHS is a single NameReference that refers to a CTE / table / scalar-let bound to a query,
        // KQL interprets it as 'col IN (SELECT <first-column> FROM <name>)'. DuckDB rejects bare identifiers
        // on the RHS of IN, so expand to a subquery form.
        if (list.Expressions.Count == 1 && list.Expressions[0].Element is NameReference tblRef)
        {
            var refName = tblRef.Name.ToString().Trim();
            bool isScalar = _scalarLets != null && _scalarLets.ContainsKey(refName);
            if (!isScalar && !string.Equals(refName, "$left", StringComparison.Ordinal) && !string.Equals(refName, "$right", StringComparison.Ordinal))
            {
                var negate = inExpr.Kind == SyntaxKind.NotInExpression || inExpr.Kind == SyntaxKind.NotInCsExpression;
                // If the referenced CTE wraps a single-row LIST aggregate, the value is an array —
                // use list_contains instead of IN so DuckDB doesn't complain about T vs T[].
                if (_ctes != null && _ctes.TryGetValue(refName, out var cte) && CteProducesArrayColumn(cte.sql))
                {
                    var match = $"LIST_CONTAINS((SELECT * FROM {refName}), {left})";
                    return negate ? $"NOT ({match})" : match;
                }
                var inOp = negate ? "NOT IN" : "IN";
                return $"{left} {inOp} (SELECT * FROM {refName})";
            }
        }

        // When dynamic(["a","b"]) is used, the expression list contains a single DynamicExpression.
        // Expand its array elements as individual IN values.
        string[] items;
        if (list.Expressions.Count == 1 && list.Expressions[0].Element is DynamicExpression de)
        {
            var array = de.GetDescendants<JsonArrayExpression>().FirstOrDefault();
            if (array != null)
            {
                items = array.Values.Select(v => ConvertExpression(v.Element, leftAlias, rightAlias)).ToArray();
            }
            else
            {
                items = new[] { ConvertExpression(de, leftAlias, rightAlias) };
            }
        }
        else
        {
            items = list.Expressions.Select(e => ConvertExpression(e.Element, leftAlias, rightAlias)).ToArray();
        }

        // DuckDB can't compare VARCHAR against VARCHAR[] in IN. When the single RHS item is an
        // array-producing expression (subquery returning a LIST, or a LIST_VALUE literal, or a
        // scalar-let bound to an array), fall back to list_contains which accepts an array.
        if (items.Length == 1 && IsArrayLikeExpression(items[0]))
        {
            var arrayExpr = items[0];
            var negate = inExpr.Kind == SyntaxKind.NotInExpression || inExpr.Kind == SyntaxKind.NotInCsExpression;
            var caseInsensitive2 = inExpr.Kind == SyntaxKind.InCsExpression || inExpr.Kind == SyntaxKind.NotInCsExpression;
            var leftCmp = caseInsensitive2 ? $"UPPER({left})" : left;
            var match = caseInsensitive2
                ? $"LEN(LIST_FILTER({arrayExpr}, x -> UPPER(x) = {leftCmp})) > 0"
                : $"LIST_CONTAINS({arrayExpr}, {leftCmp})";
            return negate ? $"NOT ({match})" : match;
        }

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

    private static bool IsArrayLikeExpression(string sql)
    {
        var t = sql.TrimStart('(').TrimEnd(')').TrimStart();
        if (t.StartsWith("LIST_VALUE", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("LIST(", StringComparison.OrdinalIgnoreCase)) return true;
        // Subquery that selects a LIST(...) aggregate → its one column is an array.
        if (t.StartsWith("SELECT LIST(", StringComparison.OrdinalIgnoreCase)) return true;
        // ( SELECT LIST(...) FROM ... LIMIT 1 ) from toscalar-on-make_list
        if (sql.Contains("SELECT LIST(", StringComparison.OrdinalIgnoreCase) && sql.Contains("LIMIT 1")) return true;
        return false;
    }

    private static bool CteProducesArrayColumn(string cteSql)
    {
        // Heuristic: the CTE's outermost SELECT list contains a LIST(...) aggregate,
        // meaning the column is an array. Not a full parser — good enough for common cases.
        return cteSql.Contains("SELECT LIST(", StringComparison.OrdinalIgnoreCase) ||
               cteSql.Contains(" LIST(", StringComparison.OrdinalIgnoreCase);
    }

    internal string ConvertBetween(BetweenExpression bin, string? leftAlias, string? rightAlias, bool negated)
    {
        var left = ConvertExpression(bin.Left, leftAlias, rightAlias);
        var couple = bin.Right;
        var lower = ConvertExpression(couple.First, leftAlias, rightAlias);
        var upper = ConvertExpression(couple.Second, leftAlias, rightAlias);
        var expr = $"{left} BETWEEN {lower} AND {upper}";
        return negated ? $"NOT ({expr})" : expr;
    }

    private string ConvertHasAny(HasAnyExpression expr, string? leftAlias, string? rightAlias, bool caseSensitive, bool negated = false)
    {
        return ConvertHasAnyAll(expr.Left, expr.Right, leftAlias, rightAlias, caseSensitive, negated, isAny: true);
    }

    private string ConvertHasAll(HasAllExpression expr, string? leftAlias, string? rightAlias, bool caseSensitive, bool negated = false)
    {
        return ConvertHasAnyAll(expr.Left, expr.Right, leftAlias, rightAlias, caseSensitive, negated, isAny: false);
    }

    private string ConvertHasAnyAll(Expression leftExpr, ExpressionList list, string? leftAlias, string? rightAlias, bool caseSensitive, bool negated, bool isAny)
    {
        var left = ConvertExpression(leftExpr, leftAlias, rightAlias);
        var like = caseSensitive ? "LIKE" : _dialect.CaseInsensitiveLike;

        // Detect single non-literal term that resolves to an array expression — emit a runtime
        // list_filter/list_transform form so we don't produce the invalid '%' || LIST_VALUE(...) || '%'.
        if (list.Expressions.Count == 1)
        {
            var onlyTerm = ConvertExpression(list.Expressions[0].Element, leftAlias, rightAlias);
            bool isStringLiteral = onlyTerm.StartsWith("'", StringComparison.Ordinal) && onlyTerm.EndsWith("'", StringComparison.Ordinal);
            if (!isStringLiteral)
            {
                var matchExpr = $"{left} {like} '%' || term || '%'";
                var anyMatch = $"LEN(LIST_FILTER({onlyTerm}, term -> {matchExpr})) > 0";
                if (isAny)
                    return negated ? $"NOT ({anyMatch})" : anyMatch;
                // has_all: every element must match. NOT has_all = some element fails to match.
                var allMatch = $"LEN(LIST_FILTER({onlyTerm}, term -> NOT ({matchExpr}))) = 0";
                return negated ? $"NOT ({allMatch})" : allMatch;
            }
        }

        var conditions = new List<string>();
        foreach (var e in list.Expressions)
        {
            var term = ConvertExpression(e.Element, leftAlias, rightAlias);
            string pattern;
            if (term.StartsWith("'", StringComparison.Ordinal) && term.EndsWith("'", StringComparison.Ordinal))
            {
                pattern = $"'%{term[1..^1]}%'";
            }
            else
            {
                pattern = $"'%' || {term} || '%'";
            }
            conditions.Add(negated ? $"{left} NOT {like} {pattern}" : $"{left} {like} {pattern}");
        }
        var sep = isAny
            ? (negated ? " AND " : " OR ")
            : (negated ? " OR " : " AND ");
        return string.Join(sep, conditions);
    }

    private string? TryConvertRegexToLike(BinaryExpression bin, string? leftAlias, string? rightAlias)
    {
        if (bin.Right is not LiteralExpression lit) return null;
        var pattern = lit.ToString().Trim().Trim('"', '\'');
        var col = ConvertExpression(bin.Left, leftAlias, rightAlias);

        var branches = pattern.Split('|');
        var likeClauses = new List<string>();
        foreach (var branch in branches)
        {
            var b = branch.Trim();
            if (!b.StartsWith("^")) return null;
            var body = b[1..];
            // Bail if the body contains regex metacharacters
            if (System.Text.RegularExpressions.Regex.IsMatch(body.TrimEnd('$'), @"[.*+?\[\](){}\\|^$]"))
                return null;
            if (body.EndsWith("$"))
                likeClauses.Add($"{col} = '{body[..^1]}'");
            else
                likeClauses.Add($"{col} LIKE '{body}%'");
        }
        return likeClauses.Count == 1 ? likeClauses[0] : $"({string.Join(" OR ", likeClauses)})";
    }

    private string ConvertLike(BinaryExpression bin, string? leftAlias, string? rightAlias, string prefix, string suffix, bool caseSensitive, bool negated = false)
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

        var like = caseSensitive ? "LIKE" : _dialect.CaseInsensitiveLike;
        if (negated)
        {
            return $"{left} NOT {like} {pattern}";
        }
        return $"{left} {like} {pattern}";
    }

    private string ConvertNumericOrOtherLiteral(LiteralExpression lit)
    {
        // KQL typed numeric/bool literals — int(N), long(N), double(N), bool(X) — parse as a LiteralExpression
        // whose text looks like "int(0)" or "long(null)". The LiteralValue holds the parsed value (null for (null)).
        //
        // DuckDB treats 'int' / 'long' / 'double' as function calls if we pass the text through verbatim.
        // Emit CAST(NULL AS <type>) for null payloads, otherwise the numeric literal form.
        if (lit.LiteralValue == null)
        {
            var text = lit.ToString().Trim();
            if (lit.Kind == SyntaxKind.BooleanLiteralExpression)
            {
                if (text.Contains("true", StringComparison.OrdinalIgnoreCase)) return "TRUE";
                if (text.Contains("false", StringComparison.OrdinalIgnoreCase)) return "FALSE";
            }
            if (text.Contains("null", StringComparison.OrdinalIgnoreCase))
            {
                var sqlType = lit.Kind switch
                {
                    SyntaxKind.LongLiteralExpression => "BIGINT",
                    SyntaxKind.IntLiteralExpression => "INTEGER",
                    SyntaxKind.RealLiteralExpression => "DOUBLE",
                    SyntaxKind.BooleanLiteralExpression => "BOOLEAN",
                    SyntaxKind.DecimalLiteralExpression => "DECIMAL",
                    _ => null
                };
                return sqlType != null ? $"CAST(NULL AS {sqlType})" : "NULL";
            }
        }
        else if (lit.Kind is SyntaxKind.LongLiteralExpression
                    or SyntaxKind.IntLiteralExpression
                    or SyntaxKind.RealLiteralExpression
                    or SyntaxKind.DecimalLiteralExpression)
        {
            // Use the parsed value (as SQL number) rather than text, so "int(0)" → "0" not "int(0)".
            return System.Convert.ToString(lit.LiteralValue, CultureInfo.InvariantCulture) ?? lit.ToString().Trim();
        }
        else if (lit.Kind == SyntaxKind.BooleanLiteralExpression)
        {
            return lit.LiteralValue is true ? "TRUE" : "FALSE";
        }
        return lit.ToString().Trim();
    }

    internal static string ConvertDateTimeLiteral(LiteralExpression lit)
    {
        var text = lit.ToString().Trim();
        if (text.StartsWith("datetime(", StringComparison.OrdinalIgnoreCase) && text.EndsWith(")"))
        {
            text = text[9..^1];
        }
        text = text.Trim('"', '\'').Trim();

        // KQL allows partial datetime literals that DateTime.TryParse rejects or mis-parses:
        //   datetime(0001)        → year-only → 0001-01-01 00:00:00
        //   datetime(2026-04)     → year-month → 2026-04-01 00:00:00
        if (System.Text.RegularExpressions.Regex.IsMatch(text, "^\\d{1,4}$"))
        {
            var year = int.Parse(text, CultureInfo.InvariantCulture);
            return $"TIMESTAMP '{year:D4}-01-01 00:00:00'";
        }
        if (System.Text.RegularExpressions.Regex.IsMatch(text, "^\\d{1,4}-\\d{1,2}$"))
        {
            var parts = text.Split('-');
            return $"TIMESTAMP '{int.Parse(parts[0]):D4}-{int.Parse(parts[1]):D2}-01 00:00:00'";
        }

        if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dt))
        {
            return $"TIMESTAMP '{dt:yyyy-MM-dd HH:mm:ss}'";
        }
        return $"TIMESTAMP '{text}'";
    }

    private bool TryConvertDynamicAccess(Expression expr, string? leftAlias, string? rightAlias, out string sql)
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
                    sql = _dialect.JsonAccess(baseSql, path);
                    return true;
            }
        }
    }

    internal string ConvertBin(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
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
                return $"EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST({value} AS TIMESTAMP))/{ms})*{ms} AS BIGINT))";
            }
        }

        var size = ConvertExpression(sizeExpr, leftAlias, rightAlias);
        // If the size is an INTERVAL-typed expression (from a timespan let or inline timespan arithmetic),
        // bare arithmetic TIMESTAMP / INTERVAL is illegal. Reduce both to milliseconds and round-trip.
        if (IsIntervalExpression(size))
        {
            var sizeMs = $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + ({size}) AS TIMESTAMP))";
            return $"EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST({value} AS TIMESTAMP))/{sizeMs})*{sizeMs} AS BIGINT))";
        }
        return $"FLOOR(({value})/({size}))*({size})";
    }

    private string ConvertBinAt(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        // bin_at(value, bin_size, fixed_point)
        if (fce.ArgumentList.Expressions.Count != 3)
            throw new NotSupportedException("bin_at() requires three arguments");

        var value = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        var sizeExpr = fce.ArgumentList.Expressions[1].Element;
        var fixedPoint = ConvertExpression(fce.ArgumentList.Expressions[2].Element, leftAlias, rightAlias);

        if (sizeExpr is LiteralExpression lit)
        {
            var text = lit.ToString().Trim().Trim('"', '\'');
            if (TryParseTimespan(text, out var ms))
            {
                return $"EPOCH_MS(CAST(FLOOR((EPOCH_MS(CAST({value} AS TIMESTAMP)) - EPOCH_MS(CAST({fixedPoint} AS TIMESTAMP)))/{ms})*{ms} + EPOCH_MS(CAST({fixedPoint} AS TIMESTAMP)) AS BIGINT))";
            }
        }

        var size = ConvertExpression(sizeExpr, leftAlias, rightAlias);
        if (IsIntervalExpression(size))
        {
            var sizeMs = $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + ({size}) AS TIMESTAMP))";
            var fixedMs = $"EPOCH_MS(CAST({fixedPoint} AS TIMESTAMP))";
            return $"EPOCH_MS(CAST(FLOOR((EPOCH_MS(CAST({value} AS TIMESTAMP)) - {fixedMs})/{sizeMs})*{sizeMs} + {fixedMs} AS BIGINT))";
        }
        return $"FLOOR(({value} - {fixedPoint})/({size}))*({size}) + {fixedPoint}";
    }

    private static bool IsIntervalExpression(string sql)
        => sql.Contains("INTERVAL ", StringComparison.OrdinalIgnoreCase);

    private string ConvertDivide(BinaryExpression bin, string? leftAlias, string? rightAlias)
    {
        var left = ConvertExpression(bin.Left, leftAlias, rightAlias);
        var right = ConvertExpression(bin.Right, leftAlias, rightAlias);
        // DuckDB rejects TIMESTAMP/INTERVAL and INTERVAL/INTERVAL directly. KQL allows these
        // to express 'how many intervals fit in this duration / distance from epoch'.
        // Convert both operands to epoch-milliseconds when we detect interval-typed operands.
        bool rightIsInterval = IsIntervalExpression(right);
        bool leftIsInterval = IsIntervalExpression(left);
        bool leftIsTimestampLike = bin.Left is NameReference || bin.Left is LiteralExpression lit &&
                                   lit.Kind == SyntaxKind.DateTimeLiteralExpression;

        if (rightIsInterval && leftIsInterval)
        {
            return $"(EXTRACT(EPOCH FROM ({left})) / EXTRACT(EPOCH FROM ({right})))";
        }
        if (rightIsInterval && (leftIsTimestampLike || LooksLikeTimestampExpression(left)))
        {
            return $"(EPOCH_MS(CAST({left} AS TIMESTAMP)) / EXTRACT(MILLISECOND FROM ({right})))";
        }
        // VARCHAR / numeric — cast left to DOUBLE so DuckDB can divide.
        bool leftLooksVarchar = left.StartsWith("trim(", StringComparison.OrdinalIgnoreCase) ||
                               left.Contains("json_extract(") ||
                               left.Contains("::VARCHAR") ||
                               left.TrimStart('(').StartsWith("TRY_CAST(", StringComparison.OrdinalIgnoreCase) && left.Contains("AS TEXT") ||
                               left.Contains("AS VARCHAR");
        bool rightIsNumericLiteral = System.Text.RegularExpressions.Regex.IsMatch(right, @"^\s*-?\d+(\.\d+)?\s*$");
        if (leftLooksVarchar && rightIsNumericLiteral)
            return $"TRY_CAST({left} AS DOUBLE) / {right}";

        return $"{left} / {right}";
    }

    private static bool LooksLikeTimestampExpression(string sql)
    {
        return sql.Contains("TIMESTAMP '", StringComparison.OrdinalIgnoreCase) ||
               sql.Contains("CAST(", StringComparison.OrdinalIgnoreCase) && sql.Contains("AS TIMESTAMP", StringComparison.OrdinalIgnoreCase);
    }

    internal static string ExtractLeftKey(Expression expr)
    {
        return expr switch
        {
            NameReference nr => nr.Name.ToString().Trim(),
            PathExpression pe when pe.Expression is NameReference nr && nr.Name.ToString().Trim() == "$left" => pe.Selector.ToString().Trim(),
            BinaryExpression be when be.Kind == SyntaxKind.EqualExpression => ExtractLeftKey(be.Left),
            BinaryExpression be when be.Kind == SyntaxKind.AndExpression => ExtractLeftKey(be.Left),
            ParenthesizedExpression pe2 => ExtractLeftKey(pe2.Expression),
            _ => "_joinkey"
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

    private string ConvertTrim(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count != 2)
            throw new NotSupportedException("trim() expects two arguments");
        var chars = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        var text = ConvertExpression(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
        return $"TRIM({text}, {chars})";
    }

    private string ConvertTrimStart(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count != 2)
            throw new NotSupportedException("trim_start() expects two arguments");
        var chars = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        var text = ConvertExpression(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
        return $"LTRIM({text}, {chars})";
    }

    private string ConvertTrimEnd(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count != 2)
            throw new NotSupportedException("trim_end() expects two arguments");
        var chars = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        var text = ConvertExpression(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
        return $"RTRIM({text}, {chars})";
    }

    private string ConvertStrcatDelim(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count < 2)
            throw new NotSupportedException("strcat_delim() expects at least a delimiter and one value");
        var delim = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        var values = fce.ArgumentList.Expressions.Skip(1).Select(a => ConvertExpression(a.Element, leftAlias, rightAlias));
        return $"CONCAT_WS({delim}, {string.Join(", ", values)})";
    }

    private string ConvertExtract(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count < 3)
            throw new NotSupportedException("extract() expects at least three arguments");
        var regex = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        var group = ConvertExpression(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
        var text = ConvertExpression(fce.ArgumentList.Expressions[2].Element, leftAlias, rightAlias);
        return $"REGEXP_EXTRACT({text}, {regex}, {group})";
    }

    private string ConvertToscalar(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count != 1)
            throw new NotSupportedException("toscalar() expects exactly one argument");
        var inner = ConvertExpression(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        return WrapAsScalarSubquery(inner);
    }

    private string ConvertToScalar(ToScalarExpression tse, string? leftAlias, string? rightAlias)
    {
        // toscalar() with an embedded pipeline (e.g. toscalar(T | count)) is parsed
        // as ToScalarExpression, not FunctionCallExpression. Use the node converter
        // to translate the inner pipeline to SQL, then wrap as scalar subquery.
        if (_nodeConverter != null)
        {
            var innerSql = _nodeConverter(tse.Expression);
            return WrapAsScalarSubquery(innerSql);
        }
        // Fallback for simple expressions
        return WrapAsScalarSubquery(ConvertExpression(tse.Expression, leftAlias, rightAlias));
    }

    private static string WrapAsScalarSubquery(string inner)
    {
        var t = inner.TrimStart().TrimStart('(').TrimStart();
        bool isTabular = t.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) ||
                         t.StartsWith("WITH", StringComparison.OrdinalIgnoreCase) ||
                         t.StartsWith("VALUES", StringComparison.OrdinalIgnoreCase);
        if (!isTabular)
        {
            // Already a scalar expression (e.g. a scalar_let substitution or arithmetic) — don't re-wrap.
            return inner;
        }
        // Avoid appending another LIMIT 1 if the inner SELECT already ends with one.
        var innerTrimmed = inner.TrimEnd().TrimEnd(')').TrimEnd();
        if (innerTrimmed.EndsWith("LIMIT 1", StringComparison.OrdinalIgnoreCase))
            return inner.TrimStart().StartsWith("(") ? inner : $"({inner})";
        return $"({inner} LIMIT 1)";
    }

    private string ConvertRound(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        var value = ConvertArgument(fce.ArgumentList.Expressions[0].Element, leftAlias, rightAlias);
        if (fce.ArgumentList.Expressions.Count >= 2)
        {
            var precision = ConvertArgument(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
            return $"ROUND({value}, {precision})";
        }
        return $"ROUND({value})";
    }

    /// <summary>Converts a function-call argument, stripping SimpleNamedExpression wrappers so
    /// we don't leak 'X AS alias' into the middle of a function-arg list.</summary>
    private string ConvertArgument(Expression expr, string? leftAlias, string? rightAlias)
    {
        if (expr is SimpleNamedExpression sne)
            return ConvertExpression(sne.Expression, leftAlias, rightAlias);
        return ConvertExpression(expr, leftAlias, rightAlias);
    }

    private string ConvertDatetimeAdd(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count != 3)
            throw new NotSupportedException("datetime_add() expects three arguments");
        var part = fce.ArgumentList.Expressions[0].Element.ToString().Trim().Trim('\'', '"').ToLowerInvariant();
        var amount = ConvertExpression(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
        var dt = ConvertExpression(fce.ArgumentList.Expressions[2].Element, leftAlias, rightAlias);
        return $"{dt} + {amount} * INTERVAL '1 {part}'";
    }

    private string ConvertDatetimeDiff(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        if (fce.ArgumentList.Expressions.Count != 3)
            throw new NotSupportedException("datetime_diff() expects three arguments");
        var part = fce.ArgumentList.Expressions[0].Element.ToString().Trim().Trim('\'', '"').ToLowerInvariant();
        var dt1 = ConvertExpression(fce.ArgumentList.Expressions[1].Element, leftAlias, rightAlias);
        var dt2 = ConvertExpression(fce.ArgumentList.Expressions[2].Element, leftAlias, rightAlias);
        return $"DATE_DIFF('{part}', {dt2}, {dt1})";
    }

    internal string ConvertLiteralValue(Expression expr)
    {
        return expr switch
        {
            LiteralExpression lit when lit.Kind == SyntaxKind.StringLiteralExpression => ConvertStringLiteral(lit),
            LiteralExpression lit when lit.Kind == SyntaxKind.DateTimeLiteralExpression => ConvertDateTimeLiteral(lit),
            LiteralExpression lit when lit.Kind == SyntaxKind.TimespanLiteralExpression => ConvertTimespanLiteral(lit),
            LiteralExpression lit when lit.Kind == SyntaxKind.BooleanLiteralExpression => lit.ToString().Trim().ToLowerInvariant() == "true" ? "TRUE" : "FALSE",
            LiteralExpression lit when lit.Kind == SyntaxKind.NullLiteralExpression => "NULL",
            LiteralExpression lit => ConvertNumericOrOtherLiteral(lit),
            CompoundStringLiteralExpression cs => ConvertCompoundString(cs),
            _ => ConvertExpression(expr)
        };
    }
}

