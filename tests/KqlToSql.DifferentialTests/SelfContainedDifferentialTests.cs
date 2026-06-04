using KqlToSql.Fuzzer;

namespace KqlToSql.DifferentialTests;

/// <summary>
/// End-to-end smoke tests: a curated set of self-contained (datatable/range/print) KQL queries that
/// SHOULD produce identical results on Kustainer and the translator+DuckDB. These prove the harness
/// runs in xUnit and skip cleanly when the container is unavailable. (The fuzzer console app drives
/// the large adversarial campaign; these are the always-on guardrail.)
/// </summary>
[Collection("Kustainer")]
public class SelfContainedDifferentialTests
{
    private readonly KustainerFixture _fx;
    public SelfContainedDifferentialTests(KustainerFixture fx) => _fx = fx;

    public static TheoryData<string> KnownGoodQueries() => new()
    {
        "datatable(i:int, l:long, r:real)[ -5,-5,-5.5, 0,0,0.0, 7,100,3.25 ] | where r > 0",
        "datatable(i:int, l:long, r:real)[ 1,1,1.5, 2,2,2.5 ] | extend s = i + l",
        "datatable(s:string)[ \"alpha\",\"Beta\",\"café\" ] | where s contains \"a\"",
        "datatable(s:string)[ \"alpha\",\"Beta\" ] | project u = toupper(s)",
        "datatable(s:string)[ \"alpha\",\"Beta\" ] | project n = strlen(s)",
        "datatable(id:long, cat:string, score:real)[ 1,\"p\",1.5, 2,\"q\",2.5, 3,\"p\",3.5 ] | summarize sum(score) by cat",
        "datatable(id:long, cat:string)[ 1,\"p\", 2,\"q\", 3,\"p\" ] | summarize count() by cat",
        "datatable(k:long, v:long)[ 1,10, 2,20 ] | join kind=inner (datatable(k:long, w:long)[ 1,100, 2,200 ]) on k",
        "datatable(x:long)[ 3,1,2 ] | sort by x asc",
        "datatable(t:datetime)[ datetime(2007-01-01), datetime(2020-06-15 13:45:00) ] | project y = getyear(t)",
        "print x = 1, y = \"hello\", z = 1.5",
        "range x from 1 to 5 step 1",
    };

    [SkippableTheory]
    [MemberData(nameof(KnownGoodQueries))]
    public async Task KnownGood_queries_match_real_kusto(string kql)
    {
        _fx.SkipIfUnavailable();
        var q = QueryAnalyzer.Enrich(new GeneratedQuery { Id = "smoke", Kql = kql });
        var (diff, verdict) = await _fx.Runner!.RunAsync(q);

        Assert.True(verdict.Outcome == Outcome.Match,
            $"Expected Match but got {verdict.Outcome}.\nKQL: {kql}\nSQL: {diff.Sql}\nDetail: {verdict.Detail}");
    }
}
