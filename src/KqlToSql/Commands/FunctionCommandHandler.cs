using System;
using System.Text.RegularExpressions;

namespace KqlToSql.Commands;

internal sealed class FunctionCommandHandler
{
    private readonly KqlToSqlConverter _converter;

    internal FunctionCommandHandler(KqlToSqlConverter converter) => _converter = converter;

    internal bool TryTranslate(string text, out string sql)
    {
        sql = null!;

        if (text.StartsWith(".view", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateView(text); return true; }

        if (text.StartsWith(".show functions", StringComparison.OrdinalIgnoreCase))
        { sql = "SELECT * FROM information_schema.tables WHERE table_type = 'VIEW'"; return true; }

        if (text.StartsWith(".create-or-alter function", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateCreateOrAlter(text); return true; }

        if (text.StartsWith(".create function", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateCreate(text); return true; }

        if (text.StartsWith(".drop function", StringComparison.OrdinalIgnoreCase))
        { sql = TranslateDrop(text); return true; }

        return false;
    }

    private string TranslateView(string text)
    {
        var match = Regex.Match(text, @"\.view\s+(\w+)\s+<\|\s*(.*)", RegexOptions.Singleline);
        if (!match.Success) throw new NotSupportedException("Malformed view command");
        return $"CREATE VIEW {match.Groups[1].Value} AS {_converter.Convert(match.Groups[2].Value)}";
    }

    private string TranslateCreate(string text)
    {
        var (name, query) = ParseFunctionBody(text, @"\.create\s+function(?:\s+with\s*\([^)]*\))?\s+(\w+)\s*\([^)]*\)\s*\{(.*)\}");
        return $"CREATE VIEW {name} AS {_converter.Convert(query)}";
    }

    private string TranslateCreateOrAlter(string text)
    {
        var (name, query) = ParseFunctionBody(text, @"\.create-or-alter\s+function(?:\s+with\s*\([^)]*\))?\s+(\w+)\s*\([^)]*\)\s*\{(.*)\}");
        return $"CREATE OR REPLACE VIEW {name} AS {_converter.Convert(query)}";
    }

    private static string TranslateDrop(string text)
    {
        var match = Regex.Match(text, @"\.drop\s+function\s+(\w+)(\s+ifexists)?", RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed drop function command");
        var ifExists = match.Groups[2].Success;
        return $"DROP VIEW {CommandParsingUtils.IfExistsClause(ifExists)}{match.Groups[1].Value}";
    }

    private static (string Name, string Query) ParseFunctionBody(string text, string pattern)
    {
        var match = Regex.Match(text, pattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success) throw new NotSupportedException("Malformed function command");
        return (match.Groups[1].Value, match.Groups[2].Value.Trim());
    }
}
