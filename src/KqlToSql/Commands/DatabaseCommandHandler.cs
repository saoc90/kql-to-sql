using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace KqlToSql.Commands;

internal sealed class DatabaseCommandHandler
{
    private readonly KqlToSqlConverter _converter;

    internal DatabaseCommandHandler(KqlToSqlConverter converter) => _converter = converter;

    internal bool TryTranslate(string text, out string sql)
    {
        sql = null!;

        if (text.StartsWith(".create database", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateCreate(text); return true; }

        if (text.StartsWith(".drop database", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateDrop(text); return true; }

        if (text.StartsWith(".show databases", StringComparison.OrdinalIgnoreCase))
        { sql = "SELECT schema_name FROM information_schema.schemata"; return true; }

        if (Regex.IsMatch(text, @"^\.show\s+database\s+\w+\s+schema\b", RegexOptions.IgnoreCase))
        { sql = TranslateShowSchema(text); return true; }

        if (text.StartsWith(".execute database script", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateExecuteScript(text); return true; }

        return false;
    }

    private static string TranslateCreate(string text)
    {
        var match = Regex.Match(text, @"\.create\s+database\s+(\w+)", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed create database command");
        return $"CREATE SCHEMA {match.Groups[1].Value}";
    }

    private static string TranslateDrop(string text)
    {
        var match = Regex.Match(text, @"\.drop\s+database\s+(\w+)(\s+ifexists)?", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed drop database command");
        var ifExists = match.Groups[2].Success;
        return $"DROP SCHEMA {CommandParsingUtils.IfExistsClause(ifExists)}{match.Groups[1].Value}";
    }

    private static string TranslateShowSchema(string text)
    {
        var match = Regex.Match(text, @"\.show\s+database\s+(\w+)\s+schema", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed show database schema command");
        return $"SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '{match.Groups[1].Value}'";
    }

    private string TranslateExecuteScript(string text)
    {
        var match = Regex.Match(text, @"\.execute\s+database\s+script\s*<\|\s*(.*)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed execute database script command");
        var commands = match.Groups[1].Value
            .Split(';', StringSplitOptions.RemoveEmptyEntries)
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .Select(c => _converter.Convert(c));
        return string.Join("; ", commands);
    }
}
