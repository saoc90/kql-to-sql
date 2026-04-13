using System;
using System.Text.RegularExpressions;

namespace KqlToSql.Commands;

internal sealed class DataCommandHandler
{
    private readonly KqlToSqlConverter _converter;
    private static readonly string DataCmdPattern = @"(?:async\s+)?(\w+)(?:\s+with\s*\([^)]*\))?\s*<\|\s*(.*)";

    internal DataCommandHandler(KqlToSqlConverter converter) => _converter = converter;

    internal bool TryTranslate(string text, out string sql)
    {
        sql = null!;

        if (text.StartsWith(".ingest", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateIngest(text); return true; }

        if (text.StartsWith(".set-or-replace", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateSetOrReplace(text); return true; }

        if (text.StartsWith(".set-or-append", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateSetOrAppend(text); return true; }

        if (text.StartsWith(".set", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateSet(text); return true; }

        if (text.StartsWith(".append", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateAppend(text); return true; }

        if (text.StartsWith(".export", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateExport(text); return true; }

        if (text.StartsWith(".purge table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateDeleteOrPurge(text, @"\.purge\s+table\s+(\w+)\s+records\s+<\|\s*where\s+(.*)"); return true; }

        if (text.StartsWith(".delete table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateDeleteOrPurge(text, @"\.delete\s+table\s+(\w+)\s+records\s+<\|\s*where\s+(.*)"); return true; }

        return false;
    }

    private static string TranslateIngest(string text)
    {
        if (text.Contains("inline", StringComparison.OrdinalIgnoreCase))
        {
            var match = Regex.Match(text, @"\.ingest\s+inline\s+into\s+table\s+(\w+)\s+<\|\s*(.*)", RegexOptions.Singleline);
            if (!match.Success) throw new NotSupportedException("Malformed ingest inline command");
            var table = match.Groups[1].Value;
            var data = match.Groups[2].Value.Trim();
            var rows = data.Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);
            var sqlRows = rows.Select(r =>
            {
                var values = r.Split(',').Select(v => v.Trim())
                    .Select(v => long.TryParse(v, out _) ? v : $"'{v.Replace("'", "''")}'");
                return "(" + string.Join(", ", values) + ")";
            });
            return $"INSERT INTO {table} VALUES {string.Join(", ", sqlRows)}";
        }
        else
        {
            var match = Regex.Match(text, @"\.ingest\s+into\s+table\s+(\w+)\s+'([^']+)'", RegexOptions.Singleline);
            if (!match.Success) throw new NotSupportedException("Malformed ingest command");
            return $"COPY {match.Groups[1].Value} FROM '{match.Groups[2].Value}' (HEADER, AUTO_DETECT TRUE)";
        }
    }

    private string TranslateSet(string text)
    {
        var (table, sql) = ParseDataCmd(text, @"\.set\s+" + DataCmdPattern);
        return $"CREATE TABLE {table} AS ({sql})";
    }

    private string TranslateAppend(string text)
    {
        var (table, sql) = ParseDataCmd(text, @"\.append\s+" + DataCmdPattern);
        return $"INSERT INTO {table} {sql}";
    }

    private string TranslateSetOrAppend(string text)
    {
        var (table, sql) = ParseDataCmd(text, @"\.set-or-append\s+" + DataCmdPattern);
        return $"CREATE TABLE IF NOT EXISTS {table} AS ({sql})";
    }

    private string TranslateSetOrReplace(string text)
    {
        var (table, sql) = ParseDataCmd(text, @"\.set-or-replace\s+" + DataCmdPattern);
        return $"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} AS ({sql})";
    }

    private string TranslateExport(string text)
    {
        var match = Regex.Match(text, @"\.export\s+to\s+(\w+)\s+\('([^']*)'\)\s*<\|\s*(.*)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed export command");
        var format = match.Groups[1].Value.ToLowerInvariant();
        var path = match.Groups[2].Value;
        var sql = _converter.Convert(match.Groups[3].Value.Trim());
        return $"COPY ({sql}) TO '{path}' (FORMAT {format})";
    }

    private string TranslateDeleteOrPurge(string text, string pattern)
    {
        var match = Regex.Match(text, pattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed purge/delete command");
        var table = match.Groups[1].Value;
        var predicate = match.Groups[2].Value.Trim();
        var selectSql = _converter.Convert($"{table} | where {predicate}");
        var prefix = $"SELECT * FROM {table} WHERE ";
        if (selectSql.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return $"DELETE FROM {table} WHERE {selectSql.Substring(prefix.Length)}";
        throw new NotSupportedException("Could not extract WHERE clause for delete/purge");
    }

    private (string Table, string Sql) ParseDataCmd(string text, string pattern)
    {
        var match = Regex.Match(text, pattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed data command");
        return (match.Groups[1].Value, _converter.Convert(match.Groups[2].Value.Trim()));
    }
}
