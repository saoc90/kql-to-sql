using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Kusto.Language;
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

    /// <summary>The ORDER BY key list of the most recent serialize/sort, scoped around a single
    /// operator's expression conversion. When set, serialization-order window functions
    /// (prev/next/row_number/row_cumsum/row_rank) emit OVER (ORDER BY …) instead of OVER () so DuckDB
    /// honours the established row order (an empty OVER() is otherwise free to use scan order, which
    /// can diverge from Kusto's serialized order on tied/non-unique sort keys).</summary>
    private string? _windowOrderBy;
    internal void SetWindowOrder(string? orderBy) => _windowOrderBy = orderBy;

    /// <summary>Injects the active serialization order into a window-function SQL fragment produced by
    /// the dialect (LAG/LEAD/ROW_NUMBER/SUM … OVER () | OVER (ROWS …)). No-op when no order is active
    /// or the fragment carries no bare window frame.</summary>
    private string InjectWindowOrder(string sql)
    {
        if (_windowOrderBy is null) return sql;
        if (sql.Contains(" OVER ()", StringComparison.Ordinal))
            sql = sql.Replace(" OVER ()", $" OVER (ORDER BY {_windowOrderBy})");
        if (sql.Contains(" OVER (ROWS ", StringComparison.Ordinal))
            sql = sql.Replace(" OVER (ROWS ", $" OVER (ORDER BY {_windowOrderBy} ROWS ");
        return sql;
    }

    private readonly HashSet<string> _intervalColumns = new(StringComparer.OrdinalIgnoreCase);
    /// <summary>Records a column as interval-typed (from extend/project) so later sum() calls know to use epoch math.</summary>
    internal void MarkIntervalColumn(string name) => _intervalColumns.Add(name);
    /// <summary>Returns true if the named column was previously marked as interval-typed.</summary>
    internal bool IsIntervalColumn(string name) => _intervalColumns.Contains(name);
    /// <summary>Clears interval column tracking — call once per top-level Convert() to avoid cross-query stale state.</summary>
    internal void ClearIntervalColumns() { _intervalColumns.Clear(); _integerColumns.Clear(); }

    private readonly HashSet<string> _integerColumns = new(StringComparer.OrdinalIgnoreCase);
    /// <summary>Records a column as KQL-integer-typed (e.g. a summarize count()/dcount() result) so a
    /// downstream `col / N` uses KQL integer (truncating) division instead of DuckDB real division.</summary>
    internal void MarkIntegerColumn(string name) => _integerColumns.Add(name);

    private Dictionary<string, (string[] paramNames, Kusto.Language.Syntax.Expression?[] paramDefaults, Kusto.Language.Syntax.FunctionBody body)>? _userFunctions;
    /// <summary>Sets user-defined parameterized functions for inline expansion.</summary>
    internal void SetUserFunctions(Dictionary<string, (string[] paramNames, Kusto.Language.Syntax.Expression?[] paramDefaults, Kusto.Language.Syntax.FunctionBody body)> funcs) => _userFunctions = funcs;

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
            // KQL `!=` keeps nulls: `null != <non-null>` is true (e.g. `where IsDeleted != true` keeps
            // rows where IsDeleted is null), whereas SQL `x <> v` yields NULL → drops them. IS DISTINCT
            // FROM (supported by DuckDB and Postgres) reproduces KQL's null-keeping behaviour.
            BinaryExpression bin when bin.Kind == SyntaxKind.NotEqualExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} IS DISTINCT FROM {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.EqualTildeExpression =>
                $"UPPER({ConvertExpression(bin.Left, leftAlias, rightAlias)}) = UPPER({ConvertExpression(bin.Right, leftAlias, rightAlias)})",
            BinaryExpression bin when bin.Kind == SyntaxKind.BangTildeExpression =>
                $"UPPER({ConvertExpression(bin.Left, leftAlias, rightAlias)}) IS DISTINCT FROM UPPER({ConvertExpression(bin.Right, leftAlias, rightAlias)})",
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
                ConvertModulo(bin, leftAlias, rightAlias),
            BinaryExpression bin when bin.Kind == SyntaxKind.AndExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} AND {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.OrExpression =>
                $"{ConvertExpression(bin.Left, leftAlias, rightAlias)} OR {ConvertExpression(bin.Right, leftAlias, rightAlias)}",
            BinaryExpression bin when bin.Kind == SyntaxKind.MatchesRegexExpression =>
                TryConvertRegexToLike(bin, leftAlias, rightAlias)
                ?? $"REGEXP_MATCHES({ConvertExpression(bin.Left, leftAlias, rightAlias)}, {ConvertExpression(bin.Right, leftAlias, rightAlias)})",
            BinaryExpression bin when bin.Kind == SyntaxKind.HasExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "term", false),
            BinaryExpression bin when bin.Kind == SyntaxKind.HasCsExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "term", true),
            BinaryExpression bin when bin.Kind == SyntaxKind.ContainsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", false),
            BinaryExpression bin when bin.Kind == SyntaxKind.ContainsCsExpression =>
                ConvertLike(bin, leftAlias, rightAlias, "%", "%", true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "term", false, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasCsExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "term", true, true),
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
                ConvertHasTerm(bin, leftAlias, rightAlias, "prefix", false),
            BinaryExpression bin when bin.Kind == SyntaxKind.HasPrefixCsExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "prefix", true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasPrefixExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "prefix", false, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasPrefixCsExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "prefix", true, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.HasSuffixExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "suffix", false),
            BinaryExpression bin when bin.Kind == SyntaxKind.HasSuffixCsExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "suffix", true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasSuffixExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "suffix", false, true),
            BinaryExpression bin when bin.Kind == SyntaxKind.NotHasSuffixCsExpression =>
                ConvertHasTerm(bin, leftAlias, rightAlias, "suffix", true, true),
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
            NameDeclaration nd => nd.SimpleName,
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
                // Insert a space when the operand's SQL starts with '-' so `--` doesn't form
                // a SQL line-comment (e.g. unary minus of a negative-substituted scalar let).
                ConvertExpression(pu.Expression, leftAlias, rightAlias) is var _inner
                    && _inner.StartsWith("-", StringComparison.Ordinal)
                    ? $"(- {_inner})"
                    : $"(-{_inner})",
            PrefixUnaryExpression pu when pu.Kind == SyntaxKind.UnaryPlusExpression =>
                ConvertExpression(pu.Expression, leftAlias, rightAlias),
            ToScalarExpression tse => ConvertToScalar(tse, leftAlias, rightAlias),
            FunctionCallExpression fce =>
                ConvertFunctionCall(fce, leftAlias, rightAlias),
            ParenthesizedExpression pe => $"({ConvertExpression(pe.Expression, leftAlias, rightAlias)})",
            DynamicExpression de => ConvertDynamic(de, leftAlias, rightAlias),
            SimpleNamedExpression sne2 => $"{ConvertExpression(sne2.Expression, leftAlias, rightAlias)} AS {sne2.Name.SimpleName}",
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
        var indexNode = ee.Selector is BracketedExpression be ? be.Expression : ee.Selector;
        var indexExpr = ConvertExpression(indexNode, leftAlias, rightAlias);

        // String index → JSON dict access (KQL: dict["key"] or dict[strCol])
        // Integer index → LIST element access (KQL arrays are 0-based, DuckDB LIST is 1-based)
        bool isStringIndex =
            indexNode is LiteralExpression lit && lit.Kind == SyntaxKind.StringLiteralExpression
            || indexNode is CompoundStringLiteralExpression
            || indexNode is FunctionCallExpression fce && IsStringReturningFunction(fce)
            // The index may be a string-typed variable (e.g. a `: string` function parameter like
            // `dm[field]`) that resolved to a quoted SQL string literal — dict key, not array index.
            || indexExpr.TrimStart().StartsWith("'", StringComparison.Ordinal);

        bool baseIsJson = IsJsonBaseExpr(baseExpr);

        // Base is a NameReference to a scalar CTE (e.g. `let mapJobToRecipe = view() { toscalar(...) }`).
        // The CTE can't be array-subscripted; the KQL semantics are JSON dict access. Inline a subquery
        // against the CTE and json_extract the key. Single-column scalar CTEs emit one scalar row — the
        // subquery yields that value directly, which json_extract can walk by the string key.
        if (ee.Expression is NameReference cteRef && _ctes != null
            && _ctes.ContainsKey(cteRef.Name.SimpleName))
        {
            return _dialect.JsonIndexByKey($"(SELECT * FROM {cteRef.Name.SimpleName} LIMIT 1)", indexExpr);
        }

        // JSON base (dynamic object/array literal, json_extract result, bag_pack/json_object, etc):
        // index/key access returns the navigable JSON value so chained .[]/array funcs work AND the
        // result matches the oracle's dynamic JSON serialization.
        if (baseIsJson)
        {
            return isStringIndex
                ? _dialect.JsonIndexByKey(baseExpr, indexExpr)
                : _dialect.JsonIndexByPosition(baseExpr, indexExpr);
        }

        // String index on a non-JSON base → JSON dict access (cast base to JSON).
        if (isStringIndex)
        {
            return _dialect.JsonIndexByKey(baseExpr, indexExpr);
        }

        // KQL uses 0-based indexing, DuckDB LIST uses 1-based.
        return $"{baseExpr}[{indexExpr} + 1]";
    }

    // True when the SQL expression evaluates to a JSON value (and so must be navigated with json_extract
    // rather than native LIST subscripting). Covers dynamic literals (::JSON), json_extract results,
    // and JSON-producing builtins (json_object/bag_pack, json_merge_patch, etc).
    private static bool IsJsonBaseExpr(string baseExpr)
    {
        var t = baseExpr.TrimEnd();
        if (t.EndsWith("::JSON", StringComparison.OrdinalIgnoreCase)) return true;
        var s = t.TrimStart('(').TrimStart();
        return s.StartsWith("json_extract(", StringComparison.OrdinalIgnoreCase)
            || s.StartsWith("json_object(", StringComparison.OrdinalIgnoreCase)
            || s.StartsWith("json_merge_patch(", StringComparison.OrdinalIgnoreCase)
            || s.StartsWith("JSON_MERGE_PATCH(", StringComparison.OrdinalIgnoreCase)
            || s.StartsWith("to_json(", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsStringReturningFunction(FunctionCallExpression fce) =>
        fce.IsAny(Functions.ToString, Functions.Strcat, Functions.ToUpper, Functions.ToLower,
                  Functions.Substring, Functions.Replace, Functions.Trim, Functions.TrimStart,
                  Functions.TrimEnd);

    private string ConvertDynamic(DynamicExpression de, string? leftAlias, string? rightAlias)
    {
        // Inspect the DIRECT child expression (not arbitrary descendants — a descendant search would
        // mis-classify dynamic({"a":[1,2]}) as an array because it'd find the nested [1,2] first).
        var child = de.Expression;

        // dynamic([...]) → LIST_VALUE so native array functions keep working on top-level arrays.
        // Nested objects/arrays inside the array are serialized to JSON so they remain navigable.
        if (child is JsonArrayExpression topArr)
        {
            var elements = topArr.Values
                .Select(v => ConvertDynamicElementSql(v.Element, leftAlias, rightAlias))
                .ToArray();
            return $"LIST_VALUE({string.Join(", ", elements)})";
        }

        // dynamic({key:val}) → strict JSON object, recursively serialized (nested objects/arrays kept).
        if (child is JsonObjectExpression topObj)
            return $"'{SerializeDynamicJson(topObj).Replace("'", "''")}'::JSON";

        // dynamic(null) / dynamic(<scalar literal>)
        if (child is LiteralExpression clit)
        {
            if (clit.Kind == SyntaxKind.NullLiteralExpression) return "NULL";
            return $"'{SerializeDynamicJson(clit).Replace("'", "''")}'::JSON";
        }

        // Fallback: any other shape — recursively serialize to JSON.
        var text = de.ToString().Trim();
        if (text.Contains("null", StringComparison.OrdinalIgnoreCase))
            return "NULL";
        return $"'{SerializeDynamicJson(child).Replace("'", "''")}'::JSON";
    }

    // Emit the SQL for one element of a top-level dynamic array (LIST_VALUE). Scalars pass through
    // ConvertExpression (so 1 stays an INTEGER element, 'x' a VARCHAR element). Nested objects/arrays
    // are serialized to JSON text so the LIST element is navigable JSON.
    private string ConvertDynamicElementSql(Expression expr, string? leftAlias, string? rightAlias)
    {
        if (expr is JsonObjectExpression or JsonArrayExpression)
        {
            var json = SerializeDynamicJson(expr);
            return $"'{json.Replace("'", "''")}'::JSON";
        }
        return ConvertExpression(expr, leftAlias, rightAlias);
    }

    // Recursively serialize a dynamic literal/object/array expression to a JSON text fragment
    // (no SQL expressions). Handles nested objects and arrays so dynamic({"a":{"b":[1,2]}}) round-trips.
    private static string SerializeDynamicJson(Expression expr)
    {
        switch (expr)
        {
            case JsonObjectExpression obj:
            {
                var pairs = Enumerable.Range(0, obj.Pairs.Count)
                    .Select(i => obj.Pairs[i].Element)
                    .Select(p =>
                    {
                        var key = p.Name.ValueText.Replace("\\", "\\\\").Replace("\"", "\\\"");
                        return $"\"{key}\":{SerializeDynamicJson(p.Value)}";
                    });
                return $"{{{string.Join(",", pairs)}}}";
            }
            case JsonArrayExpression arr:
            {
                var items = arr.Values.Select(v => SerializeDynamicJson(v.Element));
                return $"[{string.Join(",", items)}]";
            }
            case ParenthesizedExpression pe:
                return SerializeDynamicJson(pe.Expression);
            case PrefixUnaryExpression pu when pu.Kind == SyntaxKind.UnaryMinusExpression:
            {
                var inner = SerializeDynamicJson(pu.Expression);
                return inner.StartsWith("-", StringComparison.Ordinal) ? inner : $"-{inner}";
            }
            case PrefixUnaryExpression pu when pu.Kind == SyntaxKind.UnaryPlusExpression:
                return SerializeDynamicJson(pu.Expression);
            case LiteralExpression lit:
                return SerializeDynamicLiteral(lit);
            default:
                return "null";
        }
    }

    // Serialize a single dynamic literal value as JSON-compatible text.
    private static string SerializeDynamicLiteral(LiteralExpression lit)
    {
        if (lit.Kind == SyntaxKind.NullLiteralExpression) return "null";
        if (lit.Kind == SyntaxKind.BooleanLiteralExpression)
            return lit.LiteralValue is true ? "true" : "false";
        if (lit.Kind is SyntaxKind.LongLiteralExpression or SyntaxKind.IntLiteralExpression
                or SyntaxKind.RealLiteralExpression or SyntaxKind.DecimalLiteralExpression)
            return System.Convert.ToString(lit.LiteralValue, CultureInfo.InvariantCulture) ?? "null";
        if (lit.Kind == SyntaxKind.StringLiteralExpression && lit.LiteralValue is string sv)
        {
            var escaped = sv.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n").Replace("\r", "\\r").Replace("\t", "\\t");
            return $"\"{escaped}\"";
        }
        // Timespan, DateTime, or any other literal: wrap raw text as JSON string
        var raw = lit.ToString().Trim().Trim('"', '\'');
        return $"\"{raw.Replace("\\", "\\\\").Replace("\"", "\\\"")}\"";
    }

    private static readonly HashSet<string> DuckDbReservedWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "end", "order", "group", "having", "where", "select", "from", "join", "union",
        "case", "when", "then", "else", "and", "or", "not", "null", "true", "false",
        "as", "on", "in", "between", "like", "is", "by", "asc", "desc", "distinct",
        "primary", "foreign", "key", "references", "all", "any", "some", "exists",
        "limit", "offset", "with", "into", "values", "inner", "outer", "left", "right",
        "full", "cross", "natural", "using", "window", "over", "partition",
        "table",
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
        var name = nr.SimpleName;
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
        // KQL 'tick' = 100ns. DuckDB INTERVAL has no 'tick' unit (and is µs-resolution), so express
        // N ticks as N/10 microseconds. (Sub-µs values round to 0µs, within comparison tolerance.)
        var tickMatch = System.Text.RegularExpressions.Regex.Match(
            text, @"^(-?\d+(?:\.\d+)?)\s*ticks?$", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (tickMatch.Success)
            return $"(({tickMatch.Groups[1].Value} / 10.0) * INTERVAL '1 microsecond')";
        if (TryParseTimespan(text, out var ms))
        {
            return $"({ms} * INTERVAL '1 millisecond')";
        }
        return $"INTERVAL '{text}'";
    }

    private string ConvertFunctionCall(FunctionCallExpression fce, string? leftAlias, string? rightAlias)
    {
        var name = fce.Name.SimpleName;
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
            // todatetime()/datetime() use the dialect's lenient datetime parser (KQL parses many
            // textual formats, not just ISO-8601); other conversions are null-safe casts.
            if (string.Equals(sqlType, "TIMESTAMP", StringComparison.OrdinalIgnoreCase))
                return _dialect.ParseDateTime(arg);
            // KQL type conversions return null on failure (e.g. tolong('') → null)
            return _dialect.SafeCast(arg, sqlType);
        }

        // Handle functions with structural conversion logic
        if (fce.Is(Functions.Bin)) return ConvertBin(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.Substring)) return ConvertSubstring(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.Ago)) return ConvertAgo(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.Iif) || fce.Is(Functions.Iff)) return ConvertIif(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.Case)) return ConvertCase(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.BinAt)) return ConvertBinAt(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.Trim)) return ConvertTrim(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.TrimStart)) return ConvertTrimStart(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.TrimEnd)) return ConvertTrimEnd(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.StrcatDelim)) return ConvertStrcatDelim(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.Extract)) return ConvertExtract(fce, leftAlias, rightAlias);
        if (lower == "toscalar") return ConvertToscalar(fce, leftAlias, rightAlias); // TODO: no Kusto.Language symbol for 'toscalar'
        if (fce.Is(Functions.Round)) return ConvertRound(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.DatetimeAdd)) return ConvertDatetimeAdd(fce, leftAlias, rightAlias);
        if (fce.Is(Functions.DatetimeDiff)) return ConvertDatetimeDiff(fce, leftAlias, rightAlias);

        // Delegate to dialect for engine-specific function translation.
        // Strip SimpleNamedExpression wrappers from function arguments — ConvertExpression would
        // otherwise emit "X AS alias" *inside* the function call, which is invalid SQL.
        var args = fce.ArgumentList.Expressions
            .Select(a => ConvertExpression(
                a.Element is SimpleNamedExpression snArg ? snArg.Expression : a.Element,
                leftAlias, rightAlias))
            .ToArray();

        // sum/sumif on an interval column — rewrite to epoch-ms arithmetic so it type-checks in DuckDB.
        if ((fce.Is(Aggregates.Sum) || fce.Is(Aggregates.SumIf)) && args.Length >= 1 && IsBareIdentifier(args[0]) && IsIntervalColumn(args[0]))
        {
            var ms = $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + {args[0]} AS TIMESTAMP))";
            var filter = fce.Is(Aggregates.SumIf) && args.Length >= 2 ? args[1] : null;
            var inner = filter is null ? $"SUM({ms})" : $"SUM({ms}) FILTER (WHERE {filter})";
            return $"(({inner}) * INTERVAL '1 millisecond')";
        }

        var dialectResult = _dialect.TryTranslateFunction(lower, args);
        if (dialectResult != null)
        {
            return InjectWindowOrder(dialectResult);
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
            for (int i = 0; i < userFunc.paramNames.Length; i++)
            {
                var pname = userFunc.paramNames[i];
                string? bound = null;
                if (i < args.Length) bound = args[i];
                else if (userFunc.paramDefaults[i] is Expression defExpr)
                {
                    try { bound = ConvertExpression(defExpr, leftAlias, rightAlias); } catch { }
                }
                if (bound == null) continue;
                if (_scalarLets != null && _scalarLets.TryGetValue(pname, out var prev))
                    savedScalars[pname] = prev;
                _scalarLets ??= new Dictionary<string, string>();
                _scalarLets[pname] = bound;
            }

            // Process nested let statements in the function body as scalar bindings
            var nestedLets = userFunc.body.GetDescendants<LetStatement>().ToList();
            var addedLets = new List<string>();
            foreach (var ls in nestedLets)
            {
                var lname = ls.Name.SimpleName;
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
            return (inner, sne.Name.SimpleName);
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
                (c.Is(Functions.IsNotEmpty) || c.Is(Functions.IsEmpty)) &&
                c.ArgumentList.Expressions.Count == 1 &&
                c.ArgumentList.Expressions[0].Element is NameReference ca &&
                args[i + 1].Element is NameReference th &&
                ca.Name.SimpleName == th.Name.SimpleName)
                ids.Add(th.Name.SimpleName);
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
            var refName = tblRef.SimpleName;
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

        // AST-level detection: each RHS element that wraps a make_list/make_set aggregate
        // (e.g. `in (toscalar(T | summarize make_list(x)))`) produces a dynamic array. KQL
        // unwraps such an array and tests element membership, so the DuckDB equivalent must use
        // list_contains, not IN. The make_list/make_set aggregates are emitted as
        // COALESCE(LIST(...), []), which the string heuristic below does not recognize.
        var astArrayElement = list.Expressions.Count == items.Length
            && Enumerable.Range(0, items.Length).All(i => IsArrayProducingExpression(list.Expressions[i].Element));

        // DuckDB can't compare VARCHAR against VARCHAR[] in IN. When all RHS items are array-
        // producing expressions (subqueries returning LISTs, LIST_VALUE literals, or scalar-lets
        // bound to arrays), concat them into a single flat list and use list_contains.
        if (items.Length >= 1 && (astArrayElement || items.All(IsArrayLikeExpression)))
        {
            var arrayExpr = items.Length == 1
                ? items[0]
                : $"LIST_CONCAT({string.Join(", ", items)})";
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

    /// <summary>True when the KQL expression evaluates to a dynamic array — currently detected by an
    /// embedded make_list/make_set aggregate (e.g. `toscalar(T | summarize make_list(x))`), whose
    /// emitted SQL is COALESCE(LIST(...), []) and is not caught by the SELECT-LIST( string heuristic.</summary>
    private static bool IsArrayProducingExpression(Expression element)
    {
        return element.GetDescendantsOrSelf<FunctionCallExpression>()
            .Any(fce => fce.IsAny(
                Aggregates.MakeList, Aggregates.MakeListIf, Aggregates.MakeListWithNulls, Aggregates.MakeList_Deprecated,
                Aggregates.MakeSet, Aggregates.MakeSetIf, Aggregates.MakeSet_Deprecated));
    }

    private static bool IsArrayLikeExpression(string sql)
    {
        var t = sql.TrimStart('(').TrimEnd(')').TrimStart();
        if (t.StartsWith("LIST_VALUE", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("LIST_CONCAT", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("LIST_DISTINCT", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("LIST_FILTER", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("LIST_TRANSFORM", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("FLATTEN(", StringComparison.OrdinalIgnoreCase)) return true;
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

    /// <summary>
    /// KQL term operators (has/has_cs, hasprefix, hassuffix) match whole TERMS — maximal runs of
    /// word characters — not arbitrary substrings (unlike contains/startswith/endswith). Translate to
    /// a regex with word boundaries:  has → \bterm\b,  hasprefix → \bterm,  hassuffix → term\b.
    /// Case-insensitivity is expressed with the inline (?i) flag. Falls back to substring LIKE only
    /// when the right side is not a literal (a static regex can't be built from a runtime value).
    /// </summary>
    private string ConvertHasTerm(BinaryExpression bin, string? leftAlias, string? rightAlias,
        string kind, bool caseSensitive, bool negated = false)
    {
        if (bin.Right is not LiteralExpression lit)
        {
            // Non-literal RHS: keep the previous substring approximation.
            return ConvertLike(bin, leftAlias, rightAlias, "%", "%", caseSensitive, negated);
        }

        var left = ConvertExpression(bin.Left, leftAlias, rightAlias);
        var term = RegexEscape(lit.ToString().Trim().Trim('\'', '"'));
        var body = kind switch
        {
            "prefix" => $"\\b{term}",
            "suffix" => $"{term}\\b",
            _ => $"\\b{term}\\b",
        };
        var pattern = (caseSensitive ? "" : "(?i)") + body;
        var sqlPattern = "'" + pattern.Replace("'", "''") + "'";
        var test = $"regexp_matches(CAST({left} AS VARCHAR), {sqlPattern})";
        return negated ? $"NOT {test}" : test;
    }

    /// <summary>Escape RE2 metacharacters so a literal term is matched verbatim inside a regex.</summary>
    private static string RegexEscape(string s)
    {
        var sb = new System.Text.StringBuilder(s.Length + 8);
        foreach (var c in s)
        {
            if (".^$*+?()[]{}|\\".IndexOf(c) >= 0) sb.Append('\\');
            sb.Append(c);
        }
        return sb.ToString();
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
                // bool(0) / bool(1) — numeric argument
                var m = System.Text.RegularExpressions.Regex.Match(text, @"bool\s*\(\s*(-?\d+)\s*\)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                if (m.Success) return m.Groups[1].Value == "0" ? "FALSE" : "TRUE";
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
                    or SyntaxKind.IntLiteralExpression)
        {
            // Use the parsed value (as SQL number) rather than text, so "int(0)" → "0" not "int(0)".
            return System.Convert.ToString(lit.LiteralValue, CultureInfo.InvariantCulture) ?? lit.ToString().Trim();
        }
        else if (lit.Kind is SyntaxKind.RealLiteralExpression
                    or SyntaxKind.DecimalLiteralExpression)
        {
            // Special IEEE values: real(nan)/real(+inf)/real(-inf). The bareword NaN/Infinity is not
            // valid SQL; DuckDB/Postgres accept the quoted-cast form. Detect via the parsed value and
            // (as a fallback) the literal text, since some forms don't round-trip through LiteralValue.
            if (lit.LiteralValue is double dv)
            {
                if (double.IsNaN(dv)) return "CAST('nan' AS DOUBLE)";
                if (double.IsPositiveInfinity(dv)) return "CAST('inf' AS DOUBLE)";
                if (double.IsNegativeInfinity(dv)) return "CAST('-inf' AS DOUBLE)";
            }
            var lower = lit.ToString().Trim().ToLowerInvariant();
            if (lower.Contains("nan")) return "CAST('nan' AS DOUBLE)";
            if (lower.Contains("-inf")) return "CAST('-inf' AS DOUBLE)";
            if (lower.Contains("inf")) return "CAST('inf' AS DOUBLE)";

            // Preserve real-ness: a real literal like 10.0 must not render as "10" — an engine whose
            // '/' integer-divides (e.g. Postgres) would then treat 10.0/4 as integer division. Append
            // ".0" when the value renders as a plain integer so it stays a real/decimal in SQL.
            var s = System.Convert.ToString(lit.LiteralValue, CultureInfo.InvariantCulture) ?? lit.ToString().Trim();
            var body = s.StartsWith("-", StringComparison.Ordinal) ? s.Substring(1) : s;
            if (body.Length > 0 && body.All(char.IsDigit)) s += ".0";
            return s;
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
            // Preserve sub-second precision (DuckDB TIMESTAMP is microsecond) — formatting as
            // 'HH:mm:ss' alone silently truncated datetime(...123) to whole seconds, breaking
            // format_datetime(..., 'fff') and any sub-second comparison.
            return dt.Ticks % TimeSpan.TicksPerSecond == 0
                ? $"TIMESTAMP '{dt:yyyy-MM-dd HH:mm:ss}'"
                : $"TIMESTAMP '{dt:yyyy-MM-dd HH:mm:ss.ffffff}'";
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
                    if (pe.Expression is NameReference nr && (nr.SimpleName == "$left" || nr.SimpleName == "$right"))
                    {
                        return false;
                    }
                    segments.Push((pe.Selector as NameReference)?.Name.SimpleName ?? pe.Selector.ToString().Trim());
                    current = pe.Expression;
                    continue;
                case ElementExpression ee:
                    // Only string-literal selectors are JSON dict-key access (obj["key"], obj['key']).
                    // A numeric index selector is array indexing — bail so it falls through to
                    // ConvertElementAccess, which emits correct 1-based native-LIST indexing
                    // (split()/dynamic([...]) produce native VARCHAR[]/LIST_VALUE, not JSON).
                    if (ee.Selector is LiteralExpression litSel
                        && litSel.Kind == SyntaxKind.StringLiteralExpression)
                    {
                        var key = litSel.ToString().Trim().Trim('\'', '"');
                        segments.Push(key);
                        current = ee.Expression;
                        continue;
                    }
                    if (ee.Selector is BracketedExpression be
                        && (be.Expression is CompoundStringLiteralExpression
                            || be.Expression is LiteralExpression lit
                               && lit.Kind == SyntaxKind.StringLiteralExpression))
                    {
                        var key = be.Expression.ToString().Trim().Trim('\'', '"');
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
                var em = _dialect.EpochMillis(value);
                return _dialect.TimestampFromMillis($"FLOOR({em}/{ms})*{ms}");
            }
        }

        var size = ConvertExpression(sizeExpr, leftAlias, rightAlias);
        // If the size is an INTERVAL-typed expression (from a timespan let or inline timespan arithmetic),
        // bare arithmetic TIMESTAMP / INTERVAL is illegal. Reduce both to milliseconds and round-trip.
        if (IsIntervalExpression(size))
        {
            var sizeMs = _dialect.IntervalMillis(size);
            var em = _dialect.EpochMillis(value);
            return _dialect.TimestampFromMillis($"FLOOR({em}/{sizeMs})*{sizeMs}");
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
                var emV = _dialect.EpochMillis(value);
                var emF = _dialect.EpochMillis(fixedPoint);
                return _dialect.TimestampFromMillis($"FLOOR(({emV} - {emF})/{ms})*{ms} + {emF}");
            }
        }

        var size = ConvertExpression(sizeExpr, leftAlias, rightAlias);
        if (IsIntervalExpression(size))
        {
            var sizeMs = _dialect.IntervalMillis(size);
            var emV = _dialect.EpochMillis(value);
            var emF = _dialect.EpochMillis(fixedPoint);
            return _dialect.TimestampFromMillis($"FLOOR(({emV} - {emF})/{sizeMs})*{sizeMs} + {emF}");
        }
        return $"FLOOR(({value} - {fixedPoint})/({size}))*({size}) + {fixedPoint}";
    }

    internal bool IsIntervalExpression(string sql)
    {
        // A CAST/TRY_CAST to a numeric scalar type is NOT an interval, even if the inner expr contains INTERVAL.
        if (System.Text.RegularExpressions.Regex.IsMatch(sql,
                @"AS\s+(DOUBLE|BIGINT|INTEGER|FLOAT|REAL|NUMERIC)\s*\)+\s*$",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            return false;
        // Outer constructs that always produce numbers even if inner expressions touch INTERVAL:
        //   EXTRACT(...)  → BIGINT/DOUBLE
        //   EPOCH_MS(...) → BIGINT
        //   A whole expression that is only a pure aggregate call over numerics (no trailing
        //   binary op that could re-introduce interval) — e.g. SUM(DOUBLE * EPOCH_MS(..)/..).
        var trimmed = sql.TrimStart('(').TrimStart();
        if (trimmed.StartsWith("EXTRACT(", StringComparison.OrdinalIgnoreCase) ||
            trimmed.StartsWith("EPOCH_MS(", StringComparison.OrdinalIgnoreCase))
            return false;
        if (IsPureAggregateCall(trimmed))
            return false;
        return sql.Contains("INTERVAL ", StringComparison.OrdinalIgnoreCase)
               || sql.Contains("AS INTERVAL", StringComparison.OrdinalIgnoreCase)
               || (IsBareIdentifier(sql) && IsIntervalColumn(sql.Trim('"')));
    }

    private static bool IsPureAggregateCall(string trimmed)
    {
        // Does the expression open with SUM(/AVG(/... and have its FIRST paren-group extend to end?
        var aggHead = System.Text.RegularExpressions.Regex.Match(trimmed,
            @"^(SUM|MIN|MAX|AVG|COUNT|MEDIAN|STDDEV|QUANTILE|PRODUCT)\s*\(",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (!aggHead.Success) return false;
        // Walk parens from the opening `(` to find its match. If that closes at the last char
        // (allowing a FILTER(...) suffix), the expression is pure-aggregate — no `* INTERVAL` tail.
        int depth = 0;
        int openIdx = aggHead.Length - 1;
        for (int i = openIdx; i < trimmed.Length; i++)
        {
            if (trimmed[i] == '(') depth++;
            else if (trimmed[i] == ')')
            {
                depth--;
                if (depth == 0)
                {
                    var rest = trimmed[(i + 1)..].TrimStart();
                    // Accept a trailing `FILTER (WHERE ...)` clause; anything else means the
                    // aggregate's result participates in further arithmetic.
                    if (rest.Length == 0) return true;
                    if (rest.StartsWith("FILTER", StringComparison.OrdinalIgnoreCase))
                    {
                        var fmatch = System.Text.RegularExpressions.Regex.Match(rest,
                            @"^FILTER\s*\(", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                        if (!fmatch.Success) return false;
                        depth = 1;
                        int j = fmatch.Length;
                        for (; j < rest.Length && depth > 0; j++)
                        {
                            if (rest[j] == '(') depth++;
                            else if (rest[j] == ')') depth--;
                        }
                        return depth == 0 && rest[j..].Trim().Length == 0;
                    }
                    return false;
                }
            }
        }
        return false;
    }

    private string ConvertDivide(BinaryExpression bin, string? leftAlias, string? rightAlias)
    {
        var left = ConvertExpression(bin.Left, leftAlias, rightAlias);
        var right = ConvertExpression(bin.Right, leftAlias, rightAlias);
        // DuckDB rejects TIMESTAMP/INTERVAL and INTERVAL/INTERVAL directly. KQL allows these
        // to express 'how many intervals fit in this duration / distance from epoch'.
        // Convert both operands to epoch-seconds when we detect interval-typed operands.
        // Classify operand kinds from the AST first (datetime vs timespan vs number); fall back
        // to the textual/marked-column heuristics only when the AST type is Unknown. The AST
        // pass is essential because `datetime - timespan` emits SQL containing INTERVAL, which
        // the text heuristic would otherwise mis-read as a timespan numerator.
        var leftKind = InferScalarKind(bin.Left);
        var rightKind = InferScalarKind(bin.Right);

        bool rightIsInterval = rightKind == ScalarKind.TimeSpan
                               || (rightKind == ScalarKind.Unknown && (IsIntervalExpression(right) || AstReferencesIntervalColumn(bin.Right)));
        bool leftIsInterval = leftKind == ScalarKind.TimeSpan
                              || (leftKind == ScalarKind.Unknown && (IsIntervalExpression(left) || AstReferencesIntervalColumn(bin.Left)));
        bool leftIsTimestampLike = bin.Left is NameReference || bin.Left is LiteralExpression lit &&
                                   lit.Kind == SyntaxKind.DateTimeLiteralExpression;

        // datetime / timespan → "how many timespans since the epoch". Kusto measures ticks since
        // 0001-01-01 (datetime(0001)/1s == 0), so the numerator must be seconds-since-year-1, not
        // the DuckDB EXTRACT(EPOCH ...) value which is seconds-since-1970. Subtracting the epoch of
        // 0001-01-01 (a negative constant DuckDB folds) shifts to the Kusto origin.
        if (rightIsInterval && !leftIsInterval &&
            (leftKind == ScalarKind.DateTime || (leftKind == ScalarKind.Unknown && (leftIsTimestampLike || LooksLikeTimestampExpression(left)))))
        {
            return $"((EXTRACT(EPOCH FROM ({left})) - EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00')) / EXTRACT(EPOCH FROM ({right})))";
        }
        if (rightIsInterval && leftIsInterval)
        {
            return $"(EXTRACT(EPOCH FROM ({left})) / EXTRACT(EPOCH FROM ({right})))";
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

        // KQL integer division: long/long (or int/int) truncates toward zero (7/2 -> 3, -7/2 -> -3),
        // whereas DuckDB '/' is real division. Only apply when BOTH operands are statically integer
        // (literals or integer-returning functions like count/toint/binary_and) so we never mis-coerce
        // a real or an unknown-typed column; those keep real division (DuckDB's '/').
        // NULLIF(right,0): KQL integer division by zero yields null (real division yields Infinity);
        // NULLIF makes the truncating path return NULL too, and avoids casting Infinity to BIGINT.
        // Skip on engines whose `/` already truncates integer operands (PostgreSQL) — there the plain
        // `left / right` below is already correct, and DuckDB's `CAST(... AS DOUBLE)` syntax is invalid.
        if (!_dialect.NativeIntegerDivision && IsIntegerKqlExpr(bin.Left) && IsIntegerKqlExpr(bin.Right))
            return $"CAST(TRUNC(CAST({left} AS DOUBLE) / NULLIF({right}, 0)) AS BIGINT)";

        return $"{left} / {right}";
    }

    /// <summary>
    /// KQL modulo is the Euclidean remainder: the result is always in [0, |divisor|), so
    /// -5 % 3 == 1 (not -2 as SQL '%' gives). Rewrite integer modulo as ((a % b) + |b|) % |b|.
    /// Real/unknown operands keep the engine's native '%'.
    /// </summary>
    private string ConvertModulo(BinaryExpression bin, string? leftAlias, string? rightAlias)
    {
        var left = ConvertExpression(bin.Left, leftAlias, rightAlias);
        var right = ConvertExpression(bin.Right, leftAlias, rightAlias);
        if (IsIntegerKqlExpr(bin.Left) && IsIntegerKqlExpr(bin.Right))
            return $"((({left}) % NULLIF({right}, 0)) + ABS({right})) % NULLIF({right}, 0)";
        return $"{left} % {right}";
    }

    /// <summary>True when an expression is statically a KQL integer (long/int): integer literals,
    /// integer-returning functions (count/countif/dcount/dcountif/toint/tolong/binary_*), or arithmetic
    /// over such operands. Used to pick KQL's integer (truncating) division. Conservative: an unbound
    /// column or any real operand yields false, so real division is preserved.</summary>
    private bool IsIntegerKqlExpr(Expression? e)
    {
        switch (e)
        {
            case ParenthesizedExpression pe: return IsIntegerKqlExpr(pe.Expression);
            case PrefixUnaryExpression pu: return IsIntegerKqlExpr(pu.Expression);
            case NameReference nr: return _integerColumns.Contains(nr.SimpleName);
            case LiteralExpression lit:
                return lit.Kind is SyntaxKind.LongLiteralExpression or SyntaxKind.IntLiteralExpression;
            case FunctionCallExpression fce:
            {
                var fn = fce.Name.SimpleName.ToLowerInvariant();
                return fn is "toint" or "tolong" or "int" or "long" or "count" or "countif"
                    or "dcount" or "dcountif" or "binary_and" or "binary_or" or "binary_xor" or "binary_not";
            }
            case BinaryExpression be:
                return be.Kind is SyntaxKind.AddExpression or SyntaxKind.SubtractExpression
                       or SyntaxKind.MultiplyExpression or SyntaxKind.DivideExpression or SyntaxKind.ModuloExpression
                       && IsIntegerKqlExpr(be.Left) && IsIntegerKqlExpr(be.Right);
            default: return false;
        }
    }

    /// <summary>Coarse KQL scalar type, inferred structurally from the AST so we can pick the
    /// correct DuckDB arithmetic (e.g. datetime/timespan vs timespan/timespan).</summary>
    private enum ScalarKind { Unknown, DateTime, TimeSpan, Number }

    /// <summary>Infers whether a scalar KQL expression yields a datetime, timespan, or number,
    /// following KQL's arithmetic type algebra (datetime±timespan=datetime, datetime-datetime=timespan,
    /// number*timespan=timespan, datetime/timespan=number, …). Returns Unknown when it can't tell
    /// (e.g. an unbound column reference), letting callers fall back to textual heuristics.</summary>
    private ScalarKind InferScalarKind(Expression? e)
    {
        switch (e)
        {
            case null: return ScalarKind.Unknown;
            case ParenthesizedExpression pe: return InferScalarKind(pe.Expression);
            case LiteralExpression lit:
                return lit.Kind switch
                {
                    SyntaxKind.DateTimeLiteralExpression => ScalarKind.DateTime,
                    SyntaxKind.TimespanLiteralExpression => ScalarKind.TimeSpan,
                    SyntaxKind.LongLiteralExpression or SyntaxKind.IntLiteralExpression
                        or SyntaxKind.RealLiteralExpression => ScalarKind.Number,
                    _ => ScalarKind.Unknown
                };
            case NameReference nr:
                if (IsIntervalColumn(nr.SimpleName)) return ScalarKind.TimeSpan;
                // A scalar `let` resolves to our own generated SQL — classify it from that text
                // (deterministic, self-produced; not user-token munging).
                if (_scalarLets != null && _scalarLets.TryGetValue(nr.SimpleName, out var s))
                    return ClassifyGeneratedScalarSql(s);
                return ScalarKind.Unknown;
            case BinaryExpression be:
            {
                var l = InferScalarKind(be.Left);
                var r = InferScalarKind(be.Right);
                switch (be.Kind)
                {
                    case SyntaxKind.AddExpression:
                        if (l == ScalarKind.DateTime || r == ScalarKind.DateTime) return ScalarKind.DateTime;
                        if (l == ScalarKind.TimeSpan && r == ScalarKind.TimeSpan) return ScalarKind.TimeSpan;
                        if (l == ScalarKind.Number && r == ScalarKind.Number) return ScalarKind.Number;
                        // An unbound column combined with a timespan is datetime arithmetic: timespan columns
                        // are tracked as TimeSpan, and `number + timespan` is invalid KQL, so the Unknown
                        // operand must be a datetime → datetime. This lets `(Timestamp ± 150s) / 300s` on a
                        // bare datetime column (no schema) reach the year-0001 rebase path in ConvertDivide.
                        if ((l == ScalarKind.Unknown && r == ScalarKind.TimeSpan) ||
                            (l == ScalarKind.TimeSpan && r == ScalarKind.Unknown)) return ScalarKind.DateTime;
                        break;
                    case SyntaxKind.SubtractExpression:
                        if (l == ScalarKind.DateTime && r == ScalarKind.DateTime) return ScalarKind.TimeSpan;
                        if (l == ScalarKind.DateTime && r == ScalarKind.TimeSpan) return ScalarKind.DateTime;
                        if (l == ScalarKind.TimeSpan && r == ScalarKind.TimeSpan) return ScalarKind.TimeSpan;
                        if (l == ScalarKind.Number && r == ScalarKind.Number) return ScalarKind.Number;
                        // `col - timespan` with an unbound (non-interval) column is datetime arithmetic → datetime.
                        if (l == ScalarKind.Unknown && r == ScalarKind.TimeSpan) return ScalarKind.DateTime;
                        // `col - datetime` → timespan (only datetime-datetime subtraction is valid here), so
                        // `(Timestamp - datetime(...)) / 1m` routes to the timespan/timespan ratio, not the rebase.
                        if (l == ScalarKind.Unknown && r == ScalarKind.DateTime) return ScalarKind.TimeSpan;
                        break;
                    case SyntaxKind.MultiplyExpression:
                        if (l == ScalarKind.TimeSpan || r == ScalarKind.TimeSpan) return ScalarKind.TimeSpan;
                        if (l == ScalarKind.Number && r == ScalarKind.Number) return ScalarKind.Number;
                        break;
                    case SyntaxKind.DivideExpression:
                        if (l == ScalarKind.DateTime && r == ScalarKind.TimeSpan) return ScalarKind.Number;
                        if (l == ScalarKind.TimeSpan && r == ScalarKind.TimeSpan) return ScalarKind.Number;
                        if (l == ScalarKind.TimeSpan && r == ScalarKind.Number) return ScalarKind.TimeSpan;
                        if (l == ScalarKind.Number && r == ScalarKind.Number) return ScalarKind.Number;
                        break;
                }
                return ScalarKind.Unknown;
            }
            case FunctionCallExpression fce:
            {
                var fn = fce.Name.SimpleName.ToLowerInvariant();
                if (fn is "now" or "ago" or "todatetime" or "datetime_add" or "make_datetime"
                    or "startofday" or "startofweek" or "startofmonth" or "startofyear"
                    or "endofday" or "endofweek" or "endofmonth" or "endofyear" or "bin_at")
                    return ScalarKind.DateTime;
                if (fn is "totimespan" or "timespan" or "make_timespan" or "format_timespan")
                    return ScalarKind.TimeSpan;
                if (fn is "datetime_diff" or "toint" or "tolong" or "todouble" or "toreal"
                    or "tofloat" or "todecimal" or "abs" or "floor" or "ceiling" or "round")
                    return ScalarKind.Number;
                return ScalarKind.Unknown;
            }
            default:
                return ScalarKind.Unknown;
        }
    }

    /// <summary>Classifies a previously-generated scalar-let SQL string (our own output) as a
    /// datetime or timespan so <see cref="InferScalarKind"/> can resolve `let` references.</summary>
    private static ScalarKind ClassifyGeneratedScalarSql(string sql)
    {
        var t = sql.Trim().TrimStart('(').TrimStart();
        if (t.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase)) return ScalarKind.DateTime;
        if (sql.Contains("INTERVAL '1 millisecond'", StringComparison.OrdinalIgnoreCase)) return ScalarKind.TimeSpan;
        return ScalarKind.Unknown;
    }

    private bool AstReferencesIntervalColumn(SyntaxNode? node)
    {
        // Walk AST rather than inspecting SQL text so string-literal contents don't
        // false-positive. Recurse through simple wrappers (ParenthesizedExpression) so
        // `(DurationTotal)` still resolves. Keeps the textual IsIntervalExpression as
        // the first check; this covers operand shapes like `DurationTotal` that lost
        // the INTERVAL signature after CTE-boundary emission.
        return node switch
        {
            NameReference nr => IsIntervalColumn(nr.Name.SimpleName),
            ParenthesizedExpression pe => AstReferencesIntervalColumn(pe.Expression),
            _ => false
        };
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
            NameReference nr => nr.SimpleName,
            PathExpression pe when pe.Expression is NameReference nr && nr.SimpleName == "$left" => (pe.Selector as NameReference)?.SimpleName ?? pe.Selector.ToString().Trim(),
            BinaryExpression be when be.Kind == SyntaxKind.EqualExpression => ExtractLeftKey(be.Left),
            BinaryExpression be when be.Kind == SyntaxKind.AndExpression => ExtractLeftKey(be.Left),
            ParenthesizedExpression pe2 => ExtractLeftKey(pe2.Expression),
            _ => "_joinkey"
        };
    }

    /// <summary>The right-side key name of a join/lookup `on` element. A bare `NameReference`
    /// (`Key`) keys on the same name on both sides; `$left.X == $right.Y` keys on `Y`.
    /// Used by `lookup` to drop the right-side key column (Kusto keeps the left copy only).</summary>
    internal static string ExtractRightKey(Expression expr)
    {
        return expr switch
        {
            NameReference nr => nr.SimpleName,
            PathExpression pe when pe.Expression is NameReference nr && nr.SimpleName == "$right" => (pe.Selector as NameReference)?.SimpleName ?? pe.Selector.ToString().Trim(),
            BinaryExpression be when be.Kind == SyntaxKind.EqualExpression => ExtractRightKey(be.Right),
            BinaryExpression be when be.Kind == SyntaxKind.AndExpression => ExtractRightKey(be.Right),
            ParenthesizedExpression pe2 => ExtractRightKey(pe2.Expression),
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
        // Other LIMIT N (e.g. from `| take N`) — wrap in a subquery so the outer LIMIT 1
        // attaches to the wrapper, not a trailing second LIMIT token.
        if (System.Text.RegularExpressions.Regex.IsMatch(innerTrimmed,
                @"\bLIMIT\s+\d+\s*$", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            return $"(SELECT * FROM ({inner}) LIMIT 1)";
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

