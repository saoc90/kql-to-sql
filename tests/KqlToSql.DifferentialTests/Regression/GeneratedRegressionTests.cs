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

        // --- triage follow-up fixes (themes A/G/B/D/C) ---
        // Theme A: verbatim @"..." regex + escape decoding in string literals.
        { "verbatim-regex-match", "datatable(s:string)[ \"2020-01-02\",\"nope\" ] | where s matches regex @\"^\\d{4}-\\d{2}-\\d{2}$\"" },
        { "string-escape-strlen", "datatable(s:string)[ \"a\\tb\" ] | project n = strlen(s)" },
        // Theme G: order by asc with an explicit nulls clause is no longer inverted to DESC.
        { "order-asc-nulls-first", "datatable(i:int)[ 3, int(null), 1, 2 ] | sort by i asc nulls first" },
        // Theme B: tostring of dynamic unquotes; tostring(null) -> ''; tostring(bool) -> 'True'/'False'.
        { "tostring-dynamic-unquote", "print v = tostring(dynamic({\"a\":\"red\"}).a)" },
        { "tostring-null-empty", "print n = tostring(dynamic(null)), e = isempty(tostring(dynamic(null)))" },
        { "tostring-bool-capitalized", "print b = tostring(true), f = tostring(false)" },
        // Theme D: extend redefining a column from a referenced CTE excludes the shadowed copy.
        { "extend-redefine-via-cte", "let a=datatable(k:long,v:long)[1,10,2,20]; let b=a|summarize count() by k; let c=b|extend count_=count_*2; c | project k, count_ | sort by k asc" },
        // Theme C: prev offset+default; row_number start index.
        { "prev-offset-default", "datatable(t:long,v:long)[1,10,2,20,3,30] | sort by t asc | serialize p = prev(v, 1, -1)" },
        { "row_number-start", "datatable(t:long)[1,2,3] | sort by t asc | serialize rn = row_number(5)" },

        // --- "fix all fixable" value-correctness pass (themes H/L/K/J/E/F/I) ---
        // H: stdev of a single-element group is 0.
        { "stdev-single-row", "datatable(g:string,x:long)[\"a\",5,\"b\",1,\"b\",3] | summarize sd=stdev(x) by g | sort by g asc" },
        // L: array_length of a non-array dynamic is null; bag_merge first-wins; negative index; set_union variadic.
        { "array_length-nonarray", "print d=dynamic({\"a\":1}) | extend al=array_length(d)" },
        { "bag_merge-first-wins", "print m=bag_merge(dynamic({\"a\":1,\"b\":2}), dynamic({\"b\":3,\"c\":4}))" },
        { "set_union-variadic", "print u=set_union(dynamic([1,2,3]), dynamic([3,4]), dynamic([5]))" },
        // K: tostring(timespan) day-dotted; bin 7d aligns to 0001 origin.
        { "tostring-timespan", "print s=tostring(1d+2h)" },
        { "bin-7d-origin", "print b=bin(datetime(1955-11-05), 7d)" },
        // J: substring negative; trim regex; countof regex; indexof start.
        { "substring-negative", "print s=substring(\"hello\", -2, 2)" },
        { "trim-regex", "print t=trim(\"x\", \"xxabcxx\")" },
        { "countof-regex", "print c=countof(\"hello world\", \"l+\", \"regex\")" },
        { "indexof-start", "print i=indexof(\"abcabc\", \"a\", 2)" },
        // E: has unicode boundary; empty needle; search wildcard.
        { "has-unicode", "datatable(s:string)[\"I like café au lait\"] | where s has \"café\"" },
        { "has-empty-needle", "datatable(s:string)[\"a\",\"\",\"\"] | where s has \"\" | count" },
        { "search-wildcard", "datatable(h:string)[\"web01\",\"db02\",\"web03\"] | search \"web*\" | count" },
        // F: tolong(datetime)->ticks; tostring(datetime)->ISO; real Euclidean modulo; todecimal precision.
        { "tolong-datetime-ticks", "print n=tolong(datetime(2020-01-01))" },
        { "tostring-datetime-iso", "print s=tostring(datetime(2020-01-01 12:30:00))" },
        { "real-modulo-euclidean", "print m=(-7.5)%2" },
        { "todecimal-precision", "print d=todecimal(\"3.14159\")" },
        // I: mv-apply ending in summarize collapses per source row.
        { "mvapply-summarize-collapse", "print d=dynamic([{\"v\":1},{\"v\":2},{\"v\":3},{\"v\":4}]) | mv-apply e=d on (where toint(e.v)%2==0 | summarize s=sum(toint(e.v)))" },
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

    /// <summary>
    /// Cases from the differential campaign that previously produced INVALID SQL (SqlExecError) or threw
    /// in the translator (TranslateError) — the two outcomes that must never happen for valid KQL. These
    /// now translate to executable SQL. Value-correctness here is engine-tolerant: a residual row/value
    /// difference (set order, sub-microsecond datetime precision, dynamic-vs-string JSON formatting, etc.)
    /// is acceptable, so this guardrail asserts only that the SQL translates and executes, not that every
    /// cell matches. (Full value-match cases live in <see cref="FixedCases"/>.)
    /// </summary>
    public static TheoryData<string, string> ExecutableCases() => new()
    {
        // Cluster B — dynamic/JSON mv-expand + array functions.
        { "mvexpand-mixed-dynamic", "datatable(d:dynamic)[ dynamic({\"a\":1,\"b\":[1,2,3]}), dynamic([10,20,30]), dynamic(null) ] | mv-expand d" },
        { "mvexpand-object-bag", "datatable(d:dynamic)[ dynamic({\"x\":1}), dynamic({\"y\":2}) ] | mv-expand d" },
        { "mvexpand-renamed-json", "print d = dynamic({\"x\":[1,2],\"y\":[3,4]}) | extend flat = array_concat(d.x, d.y) | mv-expand f = flat to typeof(long) | summarize sum(f)" },
        { "mvexpand-chained", "print d = dynamic([[1,2,3],[4,5,6]]) | mv-expand row = d | mv-expand cell = row to typeof(long) | summarize grand = sum(cell)" },
        { "array-funcs-on-json-col", "datatable(d:dynamic)[ dynamic([10,20,30]), dynamic({\"a\":1}), dynamic(null) ] | project v = array_sort_asc(d), c = array_concat(d, dynamic([99]))" },
        { "array-sum-empty-json", "print s = '[]' | extend e = parse_json(s) | extend al = array_length(e), s2 = array_sum(e), keys = bag_keys(e)" },
        { "mvapply-array-sum", "print d = dynamic({\"matrix\":[[1,2,3],[4,5,6]]}) | mv-apply row = d.matrix on (extend rs = array_sum(row)) | summarize total = sum(rs)" },
        // Cluster A — INTERVAL/INTERVAL division.
        { "dayofweek-over-1d", "range t from datetime(2021-12-26) to datetime(2022-01-02) step 1d | extend wd = dayofweek(t) / 1d" },
        { "timespan-over-tick", "datatable(a:datetime, b:datetime)[ datetime(2020-01-01 00:00:01),datetime(2020-01-01 00:00:00) ] | extend ticks = (a - b) / 1tick" },
        // Cluster C — dynamic object index by string key.
        { "object-index-bagkey", "print d = dynamic({\"x\":1,\"y\":2,\"z\":3}) | extend ks = bag_keys(d) | mv-expand k = ks to typeof(string) | extend val = toint(d[k]) | summarize sum(val)" },
        // Cluster D — mv-expand with_itemindex.
        { "with-itemindex", "print arr = dynamic([10,20,30,40,50]) | mv-expand with_itemindex=idx v = arr | project idx, v, prod = toint(v) * idx" },
        { "with-itemindex-objs", "print arr = dynamic([{\"k\":1},{\"k\":2}]) | mv-expand with_itemindex=i e = arr | extend tagged = bag_merge(e, pack(\"idx\", i)) | project ki = toint(tagged.k), idx = toint(tagged.idx)" },
        // Cluster E — join / as-stage / range column collision suffixing.
        { "join-collision-count1", "let a = datatable(k:long, v:long)[ 1,10, 2,20 ]; let b = a | summarize count() by k; b | join kind=inner (b) on k | project k, count_, count_1" },
        { "as-stage-collision", "datatable(k:long, v:long)[ 1,10, 2,20 ] | summarize count() by k | as Stage1 | extend count_ = count_ + 100 | join kind=inner (Stage1) on k | project k, count_, count_1" },
        { "range-selfjoin-collision", "let m1 = materialize(range x from 1 to 4 step 1 | extend y = x * x); m1 | join kind=inner (m1) on x | project x, y, y1" },
        // Cluster F — zip / array_iff list coercion.
        { "zip-json", "print d = dynamic({\"a\":[1,2,3],\"b\":[4,5,6]}) | extend zipped = zip(d.a, d.b) | mv-expand z = zipped | project lhs = toint(z[0]), rhs = toint(z[1])" },
        { "array-iff", "print d = dynamic({\"vals\":[1,2,3]}) | extend doubled = array_iff(dynamic([true,false,true]), d.vals, dynamic([0,0,0]))" },
        // Cluster G — assorted singles.
        { "union-extend-redefine", "datatable(s:string)[ \"a\", \"\" ] | union (datatable(s:string)[ \"c\" ] | extend s=tostring(dynamic(null))) | project s" },
        { "datetime_diff-nanosecond", "print d2 = datetime_diff('nanosecond', datetime(2020-01-01 00:00:00.0000002), datetime(2020-01-01 00:00:00))" },
        { "guid-literal-join", "let L = datatable(k:guid, lv:string)[ guid(11111111-1111-1111-1111-111111111111),\"a\" ]; let R = datatable(k:guid, rv:string)[ guid(11111111-1111-1111-1111-111111111111),\"x\" ]; L | join kind=leftouter R on k" },
        { "reserved-alias-both", "print outer = 1 | extend inner = 2 | extend both = outer + inner | project outer, inner, both" },
        { "series_fill_const", "datatable(t:long, v:long)[ 1,10, 2,20 ] | make-series s = sum(v) default = 0 on t from 1 to 4 step 1 | extend f = series_fill_const(s, 5)" },
        { "strcat_array-make_list", "datatable(k:long, s:string)[ 1,\"a\", 1,\"b\", 2,\"c\" ] | summarize joined = strcat_array(make_list(s), \"|\") by k | sort by k asc" },
        { "array_split-multi", "print d = dynamic([1,2,3,4,5,6,7,8]) | extend parts = array_split(d, dynamic([2,5])) | extend np = array_length(parts)" },
        { "array_sum-constructed", "print d = dynamic([[1,2],[3,4],[5,6]]) | extend cols = array_concat(pack_array(d[0][0],d[1][0]), pack_array(d[0][1],d[1][1])) | extend colsum = array_sum(cols)" },
        { "bag_unpack-sum", "datatable(d:dynamic)[ dynamic({\"type\":\"A\",\"v\":1}), dynamic({\"type\":\"B\",\"v\":2}) ] | evaluate bag_unpack(d) | summarize sum(v) by type" },
        { "partition-by-summarize", "datatable(i:int)[ 1, 2, 3, 4, 5, 6 ] | extend grp = i % 2 | partition by grp (summarize s = sum(i) | extend tag = \"p\")" },
        { "mvexpand-multi-typeof", "datatable(t:long, v:long)[ 2,20, 4,40 ] | make-series s = sum(v) default = 0 on t from 1 to 4 step 1 | mv-expand idx = range(0, array_length(s)-1, 1) to typeof(long), s to typeof(long)" },
        { "extract_all-capturegroups", "datatable(s:string)[ \"2020-01-02\" ] | project parts = extract_all(@\"(\\d)(\\d)\", dynamic([1,2]), s)" },
        // extend redefining a column shadowed by an intermediate summarize must not over-EXCLUDE.
        { "extend-redefine-after-summarize", "datatable(k:long, v:long)[ 1,10, 2,20 ] | extend v = v + 1 | summarize sum_v = sum(v) by k | extend v = sum_v | summarize sum(v) by k | project k, sum_v1 = sum_v" },
        // row_cumsum with a restart predicate (reset-group hoist) executes; value-correct, order tolerant.
        { "row_cumsum-restart", "datatable(g:string,v:long)[ \"a\",5, \"a\",3, \"b\",8, \"b\",1, \"b\",2 ] | sort by g asc, v desc | serialize cs = row_cumsum(v, g != prev(g))" },
        // gettype of JSON scalars classifies via json_type (residual parse_json scalar value diff tolerated).
        { "gettype-json-scalars", "print a = gettype(parse_json(\"123\")), b = gettype(parse_json('\"hi\"'))" },
    };

    [SkippableTheory]
    [MemberData(nameof(ExecutableCases))]
    public async Task Valid_kql_translates_to_executable_sql(string id, string kql)
    {
        _fx.SkipIfUnavailable();
        var q = QueryAnalyzer.Enrich(new GeneratedQuery { Id = id, Kql = kql });
        var (diff, verdict) = await _fx.Runner!.RunAsync(q);

        // Translation must never throw and the generated SQL must always execute. Value-level
        // differences are engine-tolerated and intentionally not asserted here.
        Assert.False(verdict.Outcome is Outcome.TranslateError or Outcome.SqlExecError,
            $"[{id}] {verdict.Outcome}.\nKQL: {kql}\nSQL: {diff.Sql}\nDetail: {verdict.Detail}");
    }
}
