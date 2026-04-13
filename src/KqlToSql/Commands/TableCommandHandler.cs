using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace KqlToSql.Commands;

internal sealed class TableCommandHandler
{
    private readonly KqlToSqlConverter _converter;

    internal TableCommandHandler(KqlToSqlConverter converter) => _converter = converter;

    internal bool TryTranslate(string text, out string sql)
    {
        sql = null!;

        if (text.StartsWith(".create tables", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateCreateMultiple(text); return true; }

        if (text.StartsWith(".create-merge table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateCreateMerge(text); return true; }

        if (text.StartsWith(".create table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateCreate(text); return true; }

        if (text.StartsWith(".drop tables", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateDropMultiple(text); return true; }

        if (Regex.IsMatch(text, @"^\.drop\s+table\s+\w+\s+columns\s*\(", RegexOptions.IgnoreCase))
        { sql = TranslateDropColumns(text); return true; }

        if (text.StartsWith(".drop table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateDrop(text); return true; }

        if (text.StartsWith(".rename tables", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateRenameMultiple(text); return true; }

        if (text.StartsWith(".rename table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateRename(text); return true; }

        if (text.StartsWith(".alter-merge table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateAlterMerge(text); return true; }

        if (Regex.IsMatch(text, @"^\.alter\s+table\s+\w+\s+docstring\b", RegexOptions.IgnoreCase))
        { sql = TranslateTableDocstring(text); return true; }

        if (Regex.IsMatch(text, @"^\.alter\s+table\s+\w+\s+folder\b", RegexOptions.IgnoreCase))
        { sql = "SELECT 1 /* .alter table folder is a no-op — no SQL equivalent */"; return true; }

        if (Regex.IsMatch(text, @"^\.alter\s+table\s+\w+\s*\(", RegexOptions.IgnoreCase))
        { sql = TranslateAlterTableSchema(text); return true; }

        if (Regex.IsMatch(text, @"^\.alter\s+column\s+\w+\.\w+\s+docstring\b", RegexOptions.IgnoreCase))
        { sql = TranslateColumnDocstring(text); return true; }

        if (text.StartsWith(".alter column", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateAlterColumn(text); return true; }

        if (text.StartsWith(".drop column", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateDropColumn(text); return true; }

        if (text.StartsWith(".rename column", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateRenameColumn(text); return true; }

        if (text.StartsWith(".clear table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateClear(text); return true; }

        if (text.StartsWith(".show tables", StringComparison.OrdinalIgnoreCase))
        { sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"; return true; }

        if (text.StartsWith(".show table", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateShow(text); return true; }

        if (text.StartsWith(".show version", StringComparison.OrdinalIgnoreCase))
        { sql = "SELECT version()"; return true; }

        return false;
    }

    private string TranslateCreateMultiple(string text)
    {
        var match = Regex.Match(text, @"\.create\s+tables\s+(.*)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success) throw new NotSupportedException("Malformed create tables command");
        var rest = match.Groups[1].Value.Trim();
        // Split on boundaries: "T1(...), T2(...)" — split after each closing paren
        var tableMatches = Regex.Matches(rest, @"(\w+)\s*\(([^)]*)\)", RegexOptions.Singleline);
        if (tableMatches.Count == 0) throw new NotSupportedException("Malformed create tables command");
        var statements = tableMatches.Select(m =>
        {
            var columns = CommandParsingUtils.ParseColumnDefinitions(m.Groups[2].Value, _converter.Dialect);
            return $"CREATE TABLE {m.Groups[1].Value} ({string.Join(", ", columns)})";
        });
        return string.Join("; ", statements);
    }

    private string TranslateCreateMerge(string text)
    {
        var match = Regex.Match(text, @"\.create-merge\s+table\s+(\w+)\s*\(([^)]*)\)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed create-merge table command");
        var table = match.Groups[1].Value;
        var columns = CommandParsingUtils.ParseColumnDefinitions(match.Groups[2].Value, _converter.Dialect);
        var createIfNotExists = $"CREATE TABLE IF NOT EXISTS {table} ({string.Join(", ", columns)})";
        var addColumns = string.Join("; ", columns.Select(c => $"ALTER TABLE {table} ADD COLUMN {c}"));
        return $"{createIfNotExists}; {addColumns}";
    }

    private string TranslateAlterTableSchema(string text)
    {
        var match = Regex.Match(text, @"\.alter\s+table\s+(\w+)\s*\(([^)]*)\)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed alter table command");
        var table = match.Groups[1].Value;
        var columns = CommandParsingUtils.ParseColumnDefinitions(match.Groups[2].Value, _converter.Dialect);
        return $"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({string.Join(", ", columns)})";
    }

    private static string TranslateTableDocstring(string text)
    {
        var match = Regex.Match(text, @"\.alter\s+table\s+(\w+)\s+docstring\s+""([^""]*)""", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed alter table docstring command");
        return $"COMMENT ON TABLE {match.Groups[1].Value} IS '{match.Groups[2].Value}'";
    }

    private static string TranslateColumnDocstring(string text)
    {
        var match = Regex.Match(text, @"\.alter\s+column\s+(\w+)\.(\w+)\s+docstring\s+""([^""]*)""", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed alter column docstring command");
        return $"COMMENT ON COLUMN {match.Groups[1].Value}.{match.Groups[2].Value} IS '{match.Groups[3].Value}'";
    }

    private string TranslateCreate(string text)
    {
        var basedOn = Regex.Match(text, @"\.create\s+table\s+(\w+)\s+based-on\s+(\w+)", RegexOptions.IgnoreCase);
        if (basedOn.Success)
            return $"CREATE TABLE {basedOn.Groups[1].Value} AS SELECT * FROM {basedOn.Groups[2].Value} LIMIT 0";

        var match = Regex.Match(text, @"\.create\s+table\s+(\w+)\s*\(([^)]*)\)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed create table command");

        var columns = CommandParsingUtils.ParseColumnDefinitions(match.Groups[2].Value, _converter.Dialect);
        return $"CREATE TABLE {match.Groups[1].Value} ({string.Join(", ", columns)})";
    }

    private static string TranslateDrop(string text)
    {
        var match = Regex.Match(text, @"\.drop\s+table\s+(\w+)(\s+ifexists)?", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed drop table command");
        var ifExists = match.Groups[2].Success;
        return $"DROP TABLE {CommandParsingUtils.IfExistsClause(ifExists)}{match.Groups[1].Value}";
    }

    private static string TranslateDropMultiple(string text)
    {
        var match = Regex.Match(text, @"\.drop\s+tables\s*\(([^)]*)\)(\s+ifexists)?", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed drop tables command");
        var ifExists = match.Groups[2].Success;
        var tables = CommandParsingUtils.ParseTableList(match.Groups[1].Value);
        return string.Join("; ", tables.Select(t => $"DROP TABLE {CommandParsingUtils.IfExistsClause(ifExists)}{t}"));
    }

    private static string TranslateRename(string text)
    {
        var match = Regex.Match(text, @"\.rename\s+table\s+(\w+)\s+to\s+(\w+)", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed rename table command");
        return $"ALTER TABLE {match.Groups[1].Value} RENAME TO {match.Groups[2].Value}";
    }

    private static string TranslateRenameMultiple(string text)
    {
        var match = Regex.Match(text, @"\.rename\s+tables\s+(.*)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success) throw new NotSupportedException("Malformed rename tables command");
        var mappings = match.Groups[1].Value.Split(',').Select(m => m.Trim()).Where(m => m.Length > 0);
        return string.Join("; ", mappings.Select(m =>
        {
            var parts = m.Split('=');
            if (parts.Length != 2) throw new NotSupportedException("Invalid rename mapping");
            return $"ALTER TABLE {parts[1].Trim()} RENAME TO {parts[0].Trim()}";
        }));
    }

    private string TranslateAlterMerge(string text)
    {
        var match = Regex.Match(text, @"\.alter-merge\s+table\s+(\w+)\s*\(([^)]*)\)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed alter-merge table command");
        var table = match.Groups[1].Value;
        var columns = CommandParsingUtils.ParseColumnDefinitions(match.Groups[2].Value, _converter.Dialect);
        return string.Join("; ", columns.Select(c => $"ALTER TABLE {table} ADD COLUMN {c}"));
    }

    private string TranslateAlterColumn(string text)
    {
        var match = Regex.Match(text, @"\.alter\s+column\s+(\w+)\.(\w+)\s+type=(\w+)", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed alter column command");
        var sqlType = _converter.Dialect.MapType(match.Groups[3].Value);
        return $"ALTER TABLE {match.Groups[1].Value} ALTER COLUMN {match.Groups[2].Value} TYPE {sqlType}";
    }

    private static string TranslateDropColumn(string text)
    {
        var match = Regex.Match(text, @"\.drop\s+column\s+(\w+)\.(\w+)(\s+ifexists)?", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed drop column command");
        return $"ALTER TABLE {match.Groups[1].Value} DROP COLUMN {match.Groups[2].Value}";
    }

    private static string TranslateDropColumns(string text)
    {
        var match = Regex.Match(text, @"\.drop\s+table\s+(\w+)\s+columns\s*\(([^)]*)\)", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed drop table columns command");
        var table = match.Groups[1].Value;
        var cols = CommandParsingUtils.ParseTableList(match.Groups[2].Value);
        return string.Join("; ", cols.Select(c => $"ALTER TABLE {table} DROP COLUMN {c}"));
    }

    private static string TranslateRenameColumn(string text)
    {
        var match = Regex.Match(text, @"\.rename\s+column\s+(\w+)\.(\w+)\s+to\s+(\w+)", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed rename column command");
        return $"ALTER TABLE {match.Groups[1].Value} RENAME COLUMN {match.Groups[2].Value} TO {match.Groups[3].Value}";
    }

    private static string TranslateClear(string text)
    {
        var match = Regex.Match(text, @"\.clear\s+table\s+(\w+)\s+data", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed clear table command");
        return $"TRUNCATE TABLE {match.Groups[1].Value}";
    }

    private static string TranslateShow(string text)
    {
        var match = Regex.Match(text, @"\.show\s+table\s+(\w+)\s+(details|schema\s+as\s+json)", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed show table command");
        return $"DESCRIBE {match.Groups[1].Value}";
    }
}
