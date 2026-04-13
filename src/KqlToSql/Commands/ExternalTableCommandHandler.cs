using System;
using System.Text.RegularExpressions;

namespace KqlToSql.Commands;

internal sealed class ExternalTableCommandHandler
{
    internal bool TryTranslate(string text, out string sql)
    {
        sql = null!;

        if (text.StartsWith(".create-or-alter external table", StringComparison.OrdinalIgnoreCase))
        { sql = BuildCreateSql(text, "CREATE OR REPLACE VIEW"); return true; }

        if (text.StartsWith(".create external table", StringComparison.OrdinalIgnoreCase))
        { sql = BuildCreateSql(text, "CREATE VIEW"); return true; }

        if (text.StartsWith(".drop external table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateDrop(text); return true; }

        if (text.StartsWith(".show external tables", StringComparison.OrdinalIgnoreCase))
        { sql = "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'"; return true; }

        if (text.StartsWith(".show external table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateShow(text); return true; }

        return false;
    }

    private static string BuildCreateSql(string text, string createClause)
    {
        var match = Regex.Match(text,
            @"\.create(?:-or-alter)?\s+external\s+table\s+(\w+)\s*\([^)]*\)\s+kind=\w+\s+dataformat=(\w+)\s+\('([^']*)'\)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success) throw new NotSupportedException("Malformed external table command");
        var name = match.Groups[1].Value;
        var format = match.Groups[2].Value.ToLowerInvariant();
        var path = match.Groups[3].Value;
        var readFunc = format switch
        {
            "parquet" => $"read_parquet('{path}')",
            "csv" => $"read_csv_auto('{path}')",
            "json" or "jsonl" => $"read_json_auto('{path}')",
            _ => throw new NotSupportedException($"Unsupported external table format: {format}")
        };
        return $"{createClause} {name} AS SELECT * FROM {readFunc}";
    }

    private static string TranslateDrop(string text)
    {
        var match = Regex.Match(text, @"\.drop\s+external\s+table\s+(\w+)(\s+ifexists)?", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed drop external table command");
        var ifExists = match.Groups[2].Success;
        return $"DROP VIEW {CommandParsingUtils.IfExistsClause(ifExists)}{match.Groups[1].Value}";
    }

    private static string TranslateShow(string text)
    {
        var match = Regex.Match(text, @"\.show\s+external\s+table\s+(\w+)", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed show external table command");
        return $"DESCRIBE {match.Groups[1].Value}";
    }
}
