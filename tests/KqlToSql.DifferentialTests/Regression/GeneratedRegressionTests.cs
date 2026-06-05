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

        // --- sub-agent cluster fixes ---
        // joins: outer-join string '' padding, key-name collision rename, null==null keys.
        { "join-collision", "let L=datatable(k:long,v:long)[1,10,2,20,3,30]; let R=datatable(k:long,v:long)[1,100,2,200]; L | join kind=leftouter R on k" },
        { "join-fullouter-strpad", "let L=datatable(k:long,lv:string)[1,\"a\",2,\"b\",3,\"c\"]; let R=datatable(k:long,rv:string)[2,\"x\",4,\"y\"]; L | join kind=fullouter R on k" },
        { "union-pad", "let A=datatable(x:long,y:string)[1,\"a\",2,\"b\"]; let B=datatable(x:long,y:string)[3,\"c\"]; A | union B" },
        // parse family.
        { "parse-simple", "datatable(s:string)[\"id:7 val:foo\",\"id:8 val:bar\"] | parse kind=simple s with \"id:\" id:long \" val:\" val:string" },
        { "parse-kv", "datatable(s:string)[\"x:10|y:20|z:30\"] | parse-kv s as (x:int, y:int, z:int) with (pair_delimiter='|', kv_delimiter=':')" },
        // scan: bare ref stays per-row (matches Kusto).
        { "scan-per-row", "datatable(t:long, v:long)[1,5, 2,0, 3,0, 4,7] | sort by t asc | scan declare (cum:long=0) with (step s: true => cum = cum + v;)" },
        // make-series / bag_unpack / top-hitters.
        { "make-series-numeric", "datatable(x:long)[1,2,3,4,5] | make-series total=sum(x) default=0 on x from 1 to 5 step 1" },
        { "make-series-datetime", "datatable(t:datetime,v:long)[datetime(2020-01-01),10,datetime(2020-01-03),30] | make-series s=sum(v) default=0 on t from datetime(2020-01-01) to datetime(2020-01-05) step 1d" },
        { "bag_unpack", "datatable(d:dynamic)[dynamic({\"a\":1,\"b\":2,\"c\":3})] | evaluate bag_unpack(d)" },
        { "top-hitters", "datatable(g:string,v:long)[\"a\",1,\"a\",2,\"b\",3,\"c\",4,\"c\",5,\"c\",6] | top-hitters 2 of g by v" },
        // dynamic property navigation, format_timespan, gettype.
        { "dynamic-nested-access", "print d = dynamic({\"a\":{\"b\":{\"c\":42}}}) | extend v = d.a.b.c" },
        { "dynamic-index-access", "print d = dynamic({\"a\":[1,2],\"b\":[3,4]}) | extend ka = d[\"a\"], k0 = d[\"a\"][0]" },
        { "bag_keys-mv-expand", "print d = dynamic({\"x\":1,\"y\":2,\"z\":3}) | extend ks = bag_keys(d) | mv-expand k = ks to typeof(string)" },
        { "format_timespan-fmt", "print ft = format_timespan(2d + 3h + 4m + 5s, \"d.hh:mm:ss\")" },
        { "gettype", "print a=gettype(1), b=gettype(1.5), c=gettype(\"x\"), d=gettype(true), e=gettype(dynamic([1,2]))" },
        // datetime(null)/timespan(null) literals, make_timespan 4-arg, make_list/make_set drop nulls.
        { "null-temporal-arith", "print a=datetime(2020-01-01)+timespan(null), b=datetime(null)+1d, c=datetime(2020-01-01)-datetime(null), d=isnull(timespan(null))" },
        { "datetime-null-col", "datatable(t:datetime)[ datetime(2020-01-01), datetime(null) ] | extend isn = isnull(t)" },
        { "make_timespan-4", "print a = make_timespan(1,2,3,4)" },
        { "make_list-drops-nulls", "datatable(x:int)[ int(null), 1, 2, int(null), 3 ] | summarize lst=make_list(x)" },
        { "make_list-by-group", "datatable(g:string,v:long)[ \"a\",1,\"a\",2,\"b\",3 ] | summarize l=make_list(v) by g | sort by g asc" },
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
