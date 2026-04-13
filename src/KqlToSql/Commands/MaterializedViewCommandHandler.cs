using System;
using System.Text.RegularExpressions;

namespace KqlToSql.Commands;

internal sealed class MaterializedViewCommandHandler
{
    private readonly KqlToSqlConverter _converter;

    internal MaterializedViewCommandHandler(KqlToSqlConverter converter) => _converter = converter;

    internal bool TryTranslate(string text, out string sql)
    {
        sql = null!;

        if (text.StartsWith(".create-or-alter materialized-view", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateCreateOrAlter(text); return true; }

        if (text.StartsWith(".create materialized-view", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateCreate(text); return true; }

        if (text.StartsWith(".drop materialized-view", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateDrop(text); return true; }

        if (text.StartsWith(".show materialized-views", StringComparison.OrdinalIgnoreCase))
        { sql = "SELECT schemaname, matviewname, matviewowner, definition FROM pg_matviews"; return true; }

        if (text.StartsWith(".show materialized-view", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateShow(text); return true; }

        return false;
    }

    private string TranslateCreate(string text)
    {
        var (name, query) = ParseBody(text, @"\.create\s+(?:async\s+)?(?:ifnotexists\s+)?materialized-view(?:\s+with\s*\([^)]*\))?\s+(\w+)\s+on\s+table\s+\w+\s*\{(.*)\}");
        return $"CREATE MATERIALIZED VIEW {name} AS {_converter.Convert(query)}";
    }

    private string TranslateCreateOrAlter(string text)
    {
        var (name, query) = ParseBody(text, @"\.create-or-alter\s+materialized-view(?:\s+with\s*\([^)]*\))?\s+(\w+)\s+on\s+table\s+\w+\s*\{(.*)\}");
        var sql = _converter.Convert(query);
        return $"DROP MATERIALIZED VIEW IF EXISTS {name}; CREATE MATERIALIZED VIEW {name} AS {sql}";
    }

    private static string TranslateDrop(string text)
    {
        var match = Regex.Match(text, @"\.drop\s+materialized-view\s+(\w+)(\s+ifexists)?", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed drop materialized-view command");
        var ifExists = match.Groups[2].Success;
        return $"DROP MATERIALIZED VIEW {CommandParsingUtils.IfExistsClause(ifExists)}{match.Groups[1].Value}";
    }

    private static string TranslateShow(string text)
    {
        var match = Regex.Match(text, @"\.show\s+materialized-view\s+(\w+)", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed show materialized-view command");
        return $"SELECT * FROM pg_matviews WHERE matviewname = '{match.Groups[1].Value}'";
    }

    private static (string Name, string Query) ParseBody(string text, string pattern)
    {
        var match = Regex.Match(text, pattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed materialized-view command");
        return (match.Groups[1].Value, match.Groups[2].Value.Trim());
    }
}
