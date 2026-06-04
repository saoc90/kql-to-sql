using KqlToSql.Fuzzer;

namespace KqlToSql.DifferentialTests.Regression;

/// <summary>
/// Regression guardrails for translator bugs found by the differential fuzzing campaign and since
/// fixed. Each case previously diverged from real Kusto (wrong rows, wrong column shape, or invalid
/// generated SQL) and now matches. Runs against Kustainer; skips cleanly when it is unavailable.
/// Grouped by the root cause that was fixed.
/// </summary>
[Collection("Kustainer")]
public class GeneratedRegressionTests
{
    private readonly KustainerFixture _fx;
    public GeneratedRegressionTests(KustainerFixture fx) => _fx = fx;

    public static TheoryData<string, string> FixedCases() => new()
    {
        // has/hasprefix/hassuffix are whole-term, not substring.
        { "has-term", "datatable(s:string)[ \"hello world\",\"helloworld\",\"hi.world\" ] | where s has \"hello\"" },
        { "has-cs", "datatable(s:string)[ \"Foo bar\",\"foobar\",\"FOO\" ] | where s has_cs \"FOO\"" },
        { "not-has", "datatable(s:string)[ \"a b\",\"ab\",\"b a\" ] | where s !has \"a\"" },
        { "hasprefix", "datatable(s:string)[ \"alpha beta\",\"alphabet\",\"x alpha\" ] | where s hasprefix \"alph\"" },
        { "hassuffix", "datatable(s:string)[ \"world hi\",\"highway\",\"the hi\" ] | where s hassuffix \"hi\"" },
        // integer division truncates; modulo is Euclidean.
        { "int-division", "datatable(i:int, l:long)[ 7,7, -7,3, 5,2 ] | extend q = l / i" },
        { "modulo-euclidean", "datatable(a:int, b:int)[ -5,3, 5,-3, -5,-3, 7,3 ] | extend m = a % b" },
        // isnotempty distinguishes '' from non-empty.
        { "isnotempty", "datatable(s:string)[ \"x\",\"\" ] | project e = isnotempty(s), m = isempty(s)" },
        // string_size = UTF-8 byte count, works on non-ASCII.
        { "string-size", "datatable(s:string)[ \"café\",\"abc\",\"😀\" ] | project ss = string_size(s)" },
        // startofweek/endofweek anchor on Sunday.
        { "startofweek", "print sow = startofweek(datetime(2020-06-15 13:45:00)), eow = endofweek(datetime(2020-06-15))" },
        // make_datetime / make_timespan partial arities.
        { "make_datetime-3", "print mk = make_datetime(2020, 6, 15)" },
        { "make_timespan-2", "print mt = make_timespan(2, 30)" },
        // sort places nulls per Kusto (asc->first, desc->last).
        { "sort-nulls-asc", "datatable(i:int)[ 3, int(null), 1, 2 ] | sort by i asc" },
        { "sort-nulls-desc", "datatable(i:int)[ 3, int(null), 1, 2 ] | sort by i desc" },
        // dynamic object/scalar literals serialize correctly (incl. nested).
        { "dynamic-nested-object", "datatable(d:dynamic)[ dynamic({\"a\":1,\"b\":[1,2,3]}), dynamic({\"nested\":{\"x\":\"y\"}}) ] | project d" },
        { "dynamic-scalar", "datatable(d:dynamic)[ dynamic(1), dynamic(\"three\"), dynamic(true) ] | project d" },
        { "dynamic-property", "print d = dynamic({\"a\":1,\"b\":[1,2,3]}) | extend x = d.a" },
        // mv-expand no longer emits a double alias.
        { "mv-expand", "datatable(d:dynamic)[ dynamic([1,2,3]), dynamic([4,5]) ] | mv-expand v = d" },
        // getschema returns Kusto's 4-column shape and correct types.
        { "getschema", "datatable(id:long, name:string, score:real, active:bool, ts:datetime, d:dynamic, sp:timespan)[ 1,\"a\",1.5,true,datetime(2020-01-01),dynamic({\"x\":1}),1d ] | getschema" },
        // long columns keep BIGINT (no INT32 overflow); real columns keep DOUBLE.
        { "long-no-overflow", "datatable(a:long, b:long)[ 2147483647,1, -2147483648,1 ] | extend sum = a + b, prod = a * 2" },
        // real special values.
        { "real-nan-inf", "print a = real(nan)+1, b = real(+inf)-1, c = real(-inf), d = isnan(real(nan))" },
        // 1tick timespan literal.
        { "timespan-tick", "print sp2 = 1d + 1tick, big = 100000tick" },
        // strcat_array scalar.
        { "strcat_array", "print j = strcat_array(dynamic([1,2,3]), \"-\")" },
    };

    [SkippableTheory]
    [MemberData(nameof(FixedCases))]
    public async Task Fixed_bugs_now_match_real_kusto(string id, string kql)
    {
        _fx.SkipIfUnavailable();
        var q = QueryAnalyzer.Enrich(new GeneratedQuery { Id = id, Kql = kql });
        var (diff, verdict) = await _fx.Runner!.RunAsync(q);

        Assert.False(verdict.IsBug,
            $"[{id}] regressed: {verdict.Outcome}.\nKQL: {kql}\nSQL: {diff.Sql}\nDetail: {verdict.Detail}");
    }
}
