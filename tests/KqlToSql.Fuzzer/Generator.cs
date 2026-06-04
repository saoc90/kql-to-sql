namespace KqlToSql.Fuzzer;

/// <summary>
/// Deterministic Tier-1 combinatorial corpus generator. Emits operator chains over the canonical
/// seed tables across several axes (binary ops, scalar functions, aggregates, joins, pipelines,
/// ordered queries). Self-contained datatable sources mean both engines see identical input.
/// </summary>
public static class Generator
{
    public static IReadOnlyList<GeneratedQuery> GenerateAll()
    {
        var list = new List<GeneratedQuery>();
        list.AddRange(BinaryStringOps());
        list.AddRange(NumericOps());
        list.AddRange(ScalarStringFunctions());
        list.AddRange(ScalarNumericFunctions());
        list.AddRange(DatetimeFunctions());
        list.AddRange(DynamicFunctions());
        list.AddRange(Aggregates());
        list.AddRange(Joins());
        list.AddRange(Pipelines());
        list.AddRange(Ordered());
        list.AddRange(SingleOperators());

        // De-dup by KQL and assign stable ids; enrich flags from the AST.
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<GeneratedQuery>();
        var counters = new Dictionary<string, int>();
        foreach (var q in list)
        {
            if (!seen.Add(q.Kql)) continue;
            counters.TryGetValue(q.Family, out var n);
            counters[q.Family] = n + 1;
            var withId = q with { Id = $"t1-{q.Family}-{n:D4}", Tier = 1 };
            result.Add(QueryAnalyzer.Enrich(withId));
        }
        return result;
    }

    private static GeneratedQuery Q(string family, string kql, string[] seeds, ComparisonMode mode = ComparisonMode.Multiset,
        bool nondet = false, string? rationale = null) =>
        new() { Family = family, Kql = kql, Seeds = seeds, ExpectedMode = mode, Nondeterministic = nondet, Rationale = rationale };

    private static IEnumerable<GeneratedQuery> BinaryStringOps()
    {
        var ops = new[] { "==", "!=", "=~", "!~", "has", "!has", "has_cs", "contains", "!contains", "contains_cs",
            "startswith", "endswith", "hasprefix", "hassuffix", "in", "!in" };
        var rhs = new[] { "\"a\"", "\"café\"", "\"\"", "\"where\"", "\"MiXeD\"" };
        foreach (var op in ops)
        {
            if (op is "in" or "!in")
            {
                yield return Q("string-ops", $"{Seeds.Str.Kql} | where s {op} (\"alpha\", \"café\", \"where\")", new[] { "S_str" });
                continue;
            }
            foreach (var r in rhs)
                yield return Q("string-ops", $"{Seeds.Str.Kql} | where s {op} {r}", new[] { "S_str" });
        }
        // matches regex
        foreach (var pat in new[] { "\"^a\"", "\"[A-Z]\"", "\"f.\"", "\".*é$\"" })
            yield return Q("string-ops", $"{Seeds.Str.Kql} | where s matches regex {pat}", new[] { "S_str" });
        // has_any / has_all
        yield return Q("string-ops", $"{Seeds.Str.Kql} | where s has_any (\"alpha\", \"beta\")", new[] { "S_str" });
        yield return Q("string-ops", $"{Seeds.Str.Kql} | where tag has_all (\"x\")", new[] { "S_str" });
    }

    private static IEnumerable<GeneratedQuery> NumericOps()
    {
        yield return Q("type-casts-coercion", $"{Seeds.Num.Kql} | extend q = l / i", new[] { "S_num" }, rationale: "integer division semantics");
        yield return Q("type-casts-coercion", $"{Seeds.Num.Kql} | extend q = r / i", new[] { "S_num" }, rationale: "real/int mixed division");
        yield return Q("type-casts-coercion", $"{Seeds.Num.Kql} | extend q = i + r", new[] { "S_num" }, rationale: "int+real coercion");
        yield return Q("type-casts-coercion", $"{Seeds.Num.Kql} | extend m = l % 3", new[] { "S_num" });
        yield return Q("type-casts-coercion", $"{Seeds.Num.Kql} | extend p = i * i", new[] { "S_num" });
        foreach (var op in new[] { ">", ">=", "<", "<=", "==", "!=" })
            yield return Q("type-casts-coercion", $"{Seeds.Num.Kql} | where r {op} 0", new[] { "S_num" });
        // null propagation
        yield return Q("nested-pipelines-let-cte", $"{Seeds.NumNull.Kql} | where i != 0", new[] { "S_numnull" }, rationale: "!= null semantics");
        yield return Q("nested-pipelines-let-cte", $"{Seeds.NumNull.Kql} | extend s = i + l", new[] { "S_numnull" }, rationale: "arithmetic with null");
        yield return Q("nested-pipelines-let-cte", $"{Seeds.NumNull.Kql} | where isnull(i)", new[] { "S_numnull" });
        yield return Q("nested-pipelines-let-cte", $"{Seeds.NumNull.Kql} | extend c = coalesce(i, -1)", new[] { "S_numnull" });
        // between
        yield return Q("type-casts-coercion", $"{Seeds.Num.Kql} | where i between (0 .. 10)", new[] { "S_num" });
        yield return Q("type-casts-coercion", $"{Seeds.Num.Kql} | where i !between (0 .. 10)", new[] { "S_num" });
    }

    private static IEnumerable<GeneratedQuery> ScalarStringFunctions()
    {
        var exprs = new[]
        {
            "tolower(s)", "toupper(s)", "strlen(s)", "substring(s, 0, 2)", "substring(s, 1)",
            "trim(\" \", s)", "trim_start(\" \", s)", "trim_end(\" \", s)", "reverse(s)",
            "strcat(s, tag)", "strcat_delim(\"-\", s, tag)", "split(s, \"a\")", "replace_string(s, \"a\", \"Z\")",
            "indexof(s, \"a\")", "countof(s, \"a\")", "extract(\"([a-z]+)\", 1, tolower(s))", "string_size(s)",
            "isempty(s)", "isnotempty(s)", "strcmp(s, tag)",
        };
        foreach (var e in exprs)
            yield return Q("string-ops", $"{Seeds.Str.Kql} | project v = {e}", new[] { "S_str" }, rationale: e);
    }

    private static IEnumerable<GeneratedQuery> ScalarNumericFunctions()
    {
        var exprs = new[]
        {
            "abs(i)", "floor(r)", "ceiling(r)", "round(r, 1)", "sqrt(abs(r))", "exp(r)", "log(abs(r)+1)",
            "log10(abs(r)+1)", "log2(abs(r)+1)", "pow(i, 2)", "sign(i)", "sin(r)", "cos(r)", "tan(r)",
            "abs(i) + abs(l)", "bin(r, 1)", "toint(r)", "tolong(r)", "todouble(i)", "tostring(i)",
            "max_of(i, 0)", "min_of(i, l)", "isnan(r/0.0)", "iif(i > 0, \"pos\", \"nonpos\")",
        };
        foreach (var e in exprs)
            yield return Q("type-casts-coercion", $"{Seeds.Num.Kql} | project v = {e}", new[] { "S_num" }, rationale: e);
    }

    private static IEnumerable<GeneratedQuery> DatetimeFunctions()
    {
        var exprs = new[]
        {
            "bin(t, 1d)", "startofday(t)", "startofweek(t)", "startofmonth(t)", "startofyear(t)",
            "endofday(t)", "endofmonth(t)", "dayofweek(t)", "dayofmonth(t)", "dayofyear(t)",
            "getyear(t)", "getmonth(t)", "monthofyear(t)", "weekofyear(t)", "hourofday(t)",
            "datetime_add(\"day\", 1, t)", "datetime_diff(\"day\", t, datetime(2000-01-01))",
            "format_datetime(t, \"yyyy-MM-dd\")", "datetime_part(\"year\", t)", "t + span", "t - 1d",
        };
        foreach (var e in exprs)
            yield return Q("datetime-timespan", $"{Seeds.Dt.Kql} | project v = {e}", new[] { "S_dt" }, rationale: e);
        // timespan arithmetic
        yield return Q("datetime-timespan", $"{Seeds.Dt.Kql} | project v = span + 1h", new[] { "S_dt" });
        yield return Q("datetime-timespan", $"{Seeds.Dt.Kql} | project v = format_timespan(span, \"hh:mm:ss\")", new[] { "S_dt" });
    }

    private static IEnumerable<GeneratedQuery> DynamicFunctions()
    {
        var exprs = new[]
        {
            "array_length(d)", "d[0]", "d.a", "d.nested", "todynamic(tostring(d))",
            "array_sort_asc(d)", "array_concat(d, dynamic([99]))", "bag_keys(d)",
        };
        foreach (var e in exprs)
            yield return Q("dynamic-json", $"{Seeds.Dyn.Kql} | project v = {e}", new[] { "S_dyn" }, rationale: e);
        yield return Q("dynamic-json", $"{Seeds.Dyn.Kql} | mv-expand d", new[] { "S_dyn" }, rationale: "mv-expand dynamic");
    }

    private static IEnumerable<GeneratedQuery> Aggregates()
    {
        var aggs = new[]
        {
            ("count()", false), ("sum(score)", false), ("avg(score)", false), ("min(score)", false),
            ("max(score)", false), ("make_list(name)", false), ("make_set(cat)", false),
            ("countif(active)", false), ("sumif(score, active)", false), ("avgif(score, active)", false),
            ("dcount(name)", true), ("stdev(score)", false), ("variance(score)", false),
            ("arg_max(score, name)", false), ("arg_min(score, name)", false), ("take_any(name)", true),
            ("percentile(score, 50)", true), ("any(name)", true),
        };
        foreach (var (agg, nondet) in aggs)
        {
            yield return Q("aggregation", $"{Seeds.Wide.Kql} | summarize {agg}", new[] { "S_wide" }, nondet: nondet, rationale: agg);
            yield return Q("aggregation", $"{Seeds.Wide.Kql} | summarize {agg} by cat", new[] { "S_wide" }, nondet: nondet, rationale: agg + " by cat");
        }
    }

    private static IEnumerable<GeneratedQuery> Joins()
    {
        var kinds = new[] { "inner", "innerunique", "leftouter", "rightouter", "fullouter",
            "leftsemi", "rightsemi", "leftanti", "rightanti" };
        foreach (var k in kinds)
        {
            yield return Q("joins-lookup-union",
                $"{Seeds.Dup.Kql} | join kind={k} ({Seeds.Dup.Kql}) on k", new[] { "S_dup" }, rationale: $"join kind={k}");
        }
        yield return Q("joins-lookup-union",
            $"{Seeds.Wide.Kql} | lookup ({Seeds.Wide.Kql} | project id, name) on id", new[] { "S_wide" });
        yield return Q("joins-lookup-union",
            $"{Seeds.Wide.Kql} | union ({Seeds.Wide.Kql})", new[] { "S_wide" });
        yield return Q("joins-lookup-union",
            $"{Seeds.Num.Kql} | join kind=inner ({Seeds.NumNull.Kql}) on i", new[] { "S_num", "S_numnull" }, rationale: "join on nullable key");
    }

    private static IEnumerable<GeneratedQuery> Pipelines()
    {
        var b = Seeds.Wide.Kql;
        var stages = new[]
        {
            "where score > 1.5", "extend bonus = score * 2", "project id, name, score",
            "where active == true", "distinct cat", "extend label = strcat(name, cat)",
            "summarize s = sum(score) by cat",
        };
        // depth-2 combinations
        for (int i = 0; i < stages.Length; i++)
            for (int j = 0; j < stages.Length; j++)
            {
                if (i == j) continue;
                yield return Q("nested-pipelines-let-cte", $"{b} | {stages[i]} | {stages[j]}", new[] { "S_wide" }, rationale: "depth-2");
            }
        // a few depth-3
        yield return Q("nested-pipelines-let-cte", $"{b} | where score > 1 | extend x = score + id | summarize sum(x) by cat", new[] { "S_wide" });
        yield return Q("nested-pipelines-let-cte", $"{b} | extend k = id % 2 | summarize count() by k | where count_ > 0", new[] { "S_wide" });
        // let / materialize
        yield return Q("nested-pipelines-let-cte", $"let T = {Seeds.Wide.Kql}; T | summarize count() by cat", new[] { "S_wide" }, rationale: "let binding");
    }

    private static IEnumerable<GeneratedQuery> Ordered()
    {
        yield return Q("aggregation", $"{Seeds.Wide.Kql} | sort by score desc", new[] { "S_wide" }, ComparisonMode.Ordered);
        yield return Q("aggregation", $"{Seeds.Wide.Kql} | sort by cat asc, score desc", new[] { "S_wide" }, ComparisonMode.Ordered);
        yield return Q("aggregation", $"{Seeds.Wide.Kql} | top 2 by score desc", new[] { "S_wide" }, ComparisonMode.Ordered);
        yield return Q("aggregation", $"{Seeds.Num.Kql} | order by r asc", new[] { "S_num" }, ComparisonMode.Ordered);
        yield return Q("aggregation", $"{Seeds.NumNull.Kql} | sort by i asc", new[] { "S_numnull" }, ComparisonMode.Ordered, rationale: "null ordering in sort");
    }

    private static IEnumerable<GeneratedQuery> SingleOperators()
    {
        yield return Q("nested-pipelines-let-cte", $"{Seeds.Wide.Kql} | count", new[] { "S_wide" });
        yield return Q("nested-pipelines-let-cte", $"{Seeds.Wide.Kql} | take 2", new[] { "S_wide" }, nondet: true, rationale: "take without sort is nondeterministic");
        yield return Q("nested-pipelines-let-cte", $"{Seeds.Wide.Kql} | distinct cat", new[] { "S_wide" });
        yield return Q("nested-pipelines-let-cte", $"{Seeds.Wide.Kql} | project-away ts, active", new[] { "S_wide" });
        yield return Q("nested-pipelines-let-cte", $"{Seeds.Wide.Kql} | project-keep id, name", new[] { "S_wide" });
        yield return Q("nested-pipelines-let-cte", $"{Seeds.Wide.Kql} | project-rename ident = id", new[] { "S_wide" });
        yield return Q("nested-pipelines-let-cte", $"{Seeds.Wide.Kql} | getschema", new[] { "S_wide" }, rationale: "schema introspection");
        yield return Q("nested-pipelines-let-cte", $"{Seeds.Empty.Kql} | count", new[] { "S_empty" }, rationale: "empty input");
        yield return Q("nested-pipelines-let-cte", $"{Seeds.Empty.Kql} | summarize count()", new[] { "S_empty" }, rationale: "aggregate over empty");
        yield return Q("nested-pipelines-let-cte", "print x = 1, y = \"hello\", z = 1.5", Array.Empty<string>(), rationale: "print");
        yield return Q("nested-pipelines-let-cte", "range x from 1 to 5 step 1", Array.Empty<string>(), rationale: "range");
    }
}
