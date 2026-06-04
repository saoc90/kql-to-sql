using DuckDB.NET.Data;

namespace KqlToSql.Fuzzer;

public sealed record ResultSummary(string[] Columns, int RowCount, string[] SampleRows, string? Error)
{
    public static ResultSummary From(EngineResult r, int maxRows = 8)
    {
        var sample = r.Rows.Take(maxRows).Select(Comparator.FormatRow).ToArray();
        return new ResultSummary(
            r.Columns.Select(c => $"{c.Name}:{c.Class}").ToArray(),
            r.Rows.Count,
            sample,
            r.Error);
    }
}

/// <summary>A single query's outcome, serialized to verdicts JSONL and used by the report stage.</summary>
public sealed record VerdictRecord
{
    public string Id { get; init; } = "";
    public string Family { get; init; } = "";
    public string Kql { get; init; } = "";
    public string? Sql { get; init; }
    public string Outcome { get; init; } = "";
    public string? Detail { get; init; }
    public string[] SubVerdicts { get; init; } = Array.Empty<string>();
    public string Severity { get; init; } = "none";
    public bool IsBug { get; init; }
    public ResultSummary? Kusto { get; init; }
    public ResultSummary? Duck { get; init; }

    public static VerdictRecord Build(GeneratedQuery q, DiffResult diff, Verdict v) => new()
    {
        Id = q.Id,
        Family = q.Family,
        Kql = q.Kql,
        Sql = diff.Sql,
        Outcome = v.Outcome.ToString(),
        Detail = v.Detail,
        SubVerdicts = v.SubVerdicts.ToArray(),
        Severity = v.Severity,
        IsBug = v.IsBug,
        Kusto = ResultSummary.From(diff.Kusto),
        Duck = ResultSummary.From(diff.DuckDb),
    };
}

/// <summary>Runs a query through both engines and compares.</summary>
public sealed class DifferentialRunner : IDisposable
{
    private readonly KustoOracle _oracle;
    private readonly DuckDbTarget _target = new();
    private readonly ComparisonOptions _opts;

    public DifferentialRunner(string endpoint, string db, ComparisonOptions? opts = null)
    {
        _oracle = new KustoOracle(endpoint, db);
        _opts = opts ?? ComparisonOptions.Default;
    }

    public DifferentialRunner(KustoOracle oracle, ComparisonOptions? opts = null)
    {
        _oracle = oracle;
        _opts = opts ?? ComparisonOptions.Default;
    }

    public async Task<(DiffResult Diff, Verdict Verdict)> RunAsync(
        GeneratedQuery q, Action<DuckDBConnection>? seed = null, CancellationToken ct = default)
    {
        var kusto = await _oracle.RunQueryAsync(q.Kql, ct);
        var (duck, sql) = _target.Run(q.Kql, seed);
        var verdict = Comparator.Compare(q, kusto, duck, _opts);
        return (new DiffResult(q.Kql, sql, kusto, duck), verdict);
    }

    public void Dispose() => _oracle.Dispose();
}
