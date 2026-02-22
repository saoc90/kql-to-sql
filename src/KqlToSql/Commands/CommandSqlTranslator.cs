using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace KqlToSql.Commands;

public class CommandSqlTranslator
{
    private readonly KqlToSqlConverter _converter;

    public CommandSqlTranslator(KqlToSqlConverter converter)
    {
        _converter = converter;
    }

    public string Translate(string kqlText)
    {
        var text = kqlText.Trim();
        if (text.StartsWith(".ingest", StringComparison.OrdinalIgnoreCase))
            return TranslateIngest(text);
        if (text.StartsWith(".create table", StringComparison.OrdinalIgnoreCase))
            return TranslateCreateTable(text);
        if (text.StartsWith(".view", StringComparison.OrdinalIgnoreCase))
            return TranslateView(text);
        throw new NotSupportedException("Unsupported command");
    }

    private static string TranslateIngest(string text)
    {
        if (text.Contains("inline", StringComparison.OrdinalIgnoreCase))
        {
            var match = Regex.Match(text, @"\.ingest\s+inline\s+into\s+table\s+(\w+)\s+<\|\s*(.*)", RegexOptions.Singleline);
            if (!match.Success)
                throw new NotSupportedException("Malformed ingest inline command");
            var table = match.Groups[1].Value;
            var data = match.Groups[2].Value.Trim();
            var rows = data.Split(new[] {'\n'}, StringSplitOptions.RemoveEmptyEntries);
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
            if (!match.Success)
                throw new NotSupportedException("Malformed ingest command");
            var table = match.Groups[1].Value;
            var path = match.Groups[2].Value;
            return $"COPY {table} FROM '{path}' (HEADER, AUTO_DETECT TRUE)";
        }
    }

    private string TranslateView(string text)
    {
        var match = Regex.Match(text, @"\.view\s+(\w+)\s+<\|\s*(.*)", RegexOptions.Singleline);
        if (!match.Success)
            throw new NotSupportedException("Malformed view command");
        var name = match.Groups[1].Value;
        var query = match.Groups[2].Value;
        var sql = _converter.Convert(query);
        return $"CREATE VIEW {name} AS {sql}";
    }

    private string TranslateCreateTable(string text)
    {
        var match = Regex.Match(text, @"\.create\s+table\s+(\w+)\s*\(([^)]*)\)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success)
            throw new NotSupportedException("Malformed create table command");

        var table = match.Groups[1].Value;
        var columnsPart = match.Groups[2].Value;
        var columns = columnsPart.Split(',')
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .Select(c =>
            {
                var parts = c.Split(':', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length != 2)
                    throw new NotSupportedException("Invalid column definition");
                var colName = parts[0].Trim();
                var type = _converter.Dialect.MapType(parts[1].Trim());
                return $"{colName} {type}";
            });

        return $"CREATE TABLE {table} ({string.Join(", ", columns)})";
    }
}
