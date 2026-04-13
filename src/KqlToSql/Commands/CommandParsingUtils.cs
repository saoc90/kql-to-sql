using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace KqlToSql.Commands;

internal static class CommandParsingUtils
{
    internal static string[] ParseColumnDefinitions(string columnsPart, ISqlDialect dialect)
    {
        return columnsPart.Split(',')
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .Select(c =>
            {
                var parts = c.Split(':', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length != 2)
                    throw new NotSupportedException("Invalid column definition");
                return (Name: parts[0].Trim(), Type: dialect.MapType(parts[1].Trim()));
            })
            .Select(c => $"{c.Name} {c.Type}")
            .ToArray();
    }

    internal static string[] ParseTableList(string tablesPart)
    {
        return tablesPart.Split(',')
            .Select(t => t.Trim())
            .Where(t => t.Length > 0)
            .ToArray();
    }

    internal static bool HasIfExists(Match match, int groupIndex)
    {
        return match.Groups[groupIndex].Success &&
               match.Groups[groupIndex].Value.Contains("ifexists", StringComparison.OrdinalIgnoreCase);
    }

    internal static string IfExistsClause(bool ifExists) => ifExists ? "IF EXISTS " : "";
}
