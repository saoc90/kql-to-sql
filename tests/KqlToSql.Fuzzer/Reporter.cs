using System.Text;
using System.Text.Json;

namespace KqlToSql.Fuzzer;

/// <summary>Writes the human BUGS.md report and a machine-readable summary.json.</summary>
public static class Reporter
{
    public static void Write(IReadOnlyList<VerdictRecord> verdicts, string outDir)
    {
        Directory.CreateDirectory(outDir);

        // De-dup verdicts by (Outcome, Kql) so reruns don't inflate counts.
        var distinct = verdicts
            .GroupBy(v => v.Outcome + "" + v.Kql)
            .Select(g => g.First())
            .ToList();

        WriteSummary(distinct, Path.Combine(outDir, "summary.json"));
        WriteBugs(distinct, Path.Combine(outDir, "BUGS.md"));
    }

    private static void WriteSummary(IReadOnlyList<VerdictRecord> v, string path)
    {
        var summary = new
        {
            total = v.Count,
            confirmedBugCandidates = v.Count(x => x.IsBug),
            byOutcome = v.GroupBy(x => x.Outcome).ToDictionary(g => g.Key, g => g.Count()),
            byFamily = v.Where(x => x.IsBug).GroupBy(x => x.Family).ToDictionary(g => g.Key, g => g.Count()),
            bySeverity = v.Where(x => x.IsBug).GroupBy(x => x.Severity).ToDictionary(g => g.Key, g => g.Count()),
            subVerdicts = v.SelectMany(x => x.SubVerdicts).GroupBy(s => Bucket(s)).ToDictionary(g => g.Key, g => g.Count()),
        };
        File.WriteAllText(path, JsonSerializer.Serialize(summary, new JsonSerializerOptions { WriteIndented = true }));
    }

    private static string Bucket(string sub)
    {
        var i = sub.IndexOf('[');
        return i > 0 ? sub[..i] : sub;
    }

    private static void WriteBugs(IReadOnlyList<VerdictRecord> v, string path)
    {
        var bugs = v.Where(x => x.IsBug).ToList();
        var sb = new StringBuilder();
        sb.AppendLine("# KQL→SQL Translator — Differential Fuzzing Findings");
        sb.AppendLine();
        sb.AppendLine($"Oracle: Kustainer (real Kusto). SUT: KqlToSqlConverter → DuckDB.");
        sb.AppendLine($"Total verdicts: {v.Count}. Bug candidates: {bugs.Count}.");
        sb.AppendLine();

        sb.AppendLine("## Counts by outcome");
        sb.AppendLine();
        sb.AppendLine("| Outcome | Count |");
        sb.AppendLine("|---|---|");
        foreach (var g in v.GroupBy(x => x.Outcome).OrderByDescending(g => g.Count()))
            sb.AppendLine($"| {g.Key} | {g.Count()} |");
        sb.AppendLine();

        var severityOrder = new[] { "highest", "high", "medium", "low", "none" };
        foreach (var fam in bugs.GroupBy(b => b.Family).OrderBy(g => g.Key))
        {
            sb.AppendLine($"## Family: {fam.Key} ({fam.Count()})");
            sb.AppendLine();
            foreach (var bug in fam.OrderBy(b => Array.IndexOf(severityOrder, b.Severity)))
            {
                sb.AppendLine($"### `{bug.Id}` — {bug.Outcome} ({bug.Severity})");
                sb.AppendLine();
                if (bug.SubVerdicts.Length > 0) sb.AppendLine($"*Sub-verdicts:* {string.Join(", ", bug.SubVerdicts)}  ");
                if (!string.IsNullOrEmpty(bug.Detail)) sb.AppendLine($"*Detail:* {bug.Detail}");
                sb.AppendLine();
                sb.AppendLine("**KQL**");
                sb.AppendLine("```kql");
                sb.AppendLine(bug.Kql);
                sb.AppendLine("```");
                if (!string.IsNullOrEmpty(bug.Sql))
                {
                    sb.AppendLine("**Generated SQL**");
                    sb.AppendLine("```sql");
                    sb.AppendLine(bug.Sql);
                    sb.AppendLine("```");
                }
                sb.AppendLine("**Kusto (oracle)** vs **DuckDB (translated)**");
                sb.AppendLine();
                AppendResult(sb, "Kusto", bug.Kusto);
                AppendResult(sb, "DuckDB", bug.Duck);
                sb.AppendLine();
            }
        }

        File.WriteAllText(path, sb.ToString());
    }

    private static void AppendResult(StringBuilder sb, string label, ResultSummary? r)
    {
        if (r is null) { sb.AppendLine($"- {label}: (none)"); return; }
        if (r.Error is not null) { sb.AppendLine($"- {label}: ERROR — {r.Error}"); return; }
        sb.AppendLine($"- {label}: cols=[{string.Join(", ", r.Columns)}] rows={r.RowCount}");
        foreach (var row in r.SampleRows)
            sb.AppendLine($"    - {row}");
    }
}
