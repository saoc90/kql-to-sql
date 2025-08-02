using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Xunit;

namespace KqlToSql.Tests;

public class OperatorChecklistTests
{
    [Fact]
    public async Task ChecklistContainsAllDocOperators()
    {
        using var client = new HttpClient();
        var yaml = await client.GetStringAsync("https://raw.githubusercontent.com/MicrosoftDocs/dataexplorer-docs/main/data-explorer/kusto/query/toc.yml");

        var docOps = Regex.Matches(yaml, "- name: ([^\\n]+?) operator")
            .Select(m => {
                var op = m.Groups[1].Value.Trim().Trim('"');
                var idx = op.IndexOf(" (", StringComparison.Ordinal);
                return idx >= 0 ? op[..idx] : op;
            })
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var root = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../"));
        var lines = await File.ReadAllLinesAsync(Path.Combine(root, "KqlOperatorsChecklist.md"));
        var fileOps = lines
            .Where(line => line.StartsWith("- ["))
            .Select(line => {
                var text = line[(line.IndexOf("] ") + 2)..];
                var idx = text.IndexOf(" (", StringComparison.Ordinal);
                return (idx >= 0 ? text[..idx] : text).Trim();
            })
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        docOps.ExceptWith(fileOps);
        Assert.Empty(docOps);
    }
}
