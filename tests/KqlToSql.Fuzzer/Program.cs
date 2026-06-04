using KqlToSql.Fuzzer;

// Subcommand dispatcher for the differential fuzzing pipeline.
//   generate  --out corpus.jsonl
//   seedtest  [--endpoint http://localhost:8080] [--db NetDefaultDB]
//   validate  --in corpus.jsonl --out validated.jsonl [--endpoint] [--db]
//   run       --in corpus.jsonl --out verdicts.jsonl [--endpoint] [--db] [--strict-names] [--null-eq-empty]
//   report    --in verdicts.jsonl[,more.jsonl] --out reportDir

var opts = ArgParser.Parse(args);
string endpoint = opts.Get("endpoint", "http://localhost:8080");
string db = opts.Get("db", "NetDefaultDB");

if (args.Length == 0) { Usage(); return 1; }

switch (args[0])
{
    case "generate":
    {
        var outPath = opts.Get("out", "fuzzing/corpus/tier1.jsonl");
        var corpus = Generator.GenerateAll();
        Jsonl.WriteFile(outPath, corpus);
        Console.WriteLine($"Generated {corpus.Count} queries -> {outPath}");
        foreach (var g in corpus.GroupBy(c => c.Family).OrderBy(g => g.Key))
            Console.WriteLine($"  {g.Key,-28} {g.Count()}");
        return 0;
    }

    case "translate":
    {
        var kql = opts.Get("kql", string.Empty);
        if (kql.Length == 0 && Seeds.All.Any(s => s.Name == opts.Get("seed", "")))
            kql = Seeds.ByName(opts.Get("seed", "")).Kql;
        var target = new DuckDbTarget();
        try { Console.WriteLine(target.Translate(kql)); }
        catch (Exception ex) { Console.WriteLine($"TRANSLATE ERROR: {ex.GetType().Name}: {ex.Message}"); }
        return 0;
    }

    case "diff1":
    {
        var kql = opts.Require("kql");
        using var runner = new DifferentialRunner(endpoint, db);
        var q = QueryAnalyzer.Enrich(new GeneratedQuery { Id = "diff1", Family = "adhoc", Kql = kql });
        var (diff, v) = await runner.RunAsync(q);
        Console.WriteLine($"KQL : {kql}");
        Console.WriteLine($"SQL : {diff.Sql}");
        Console.WriteLine($"Mode: {q.ExpectedMode}  Nondet: {q.Nondeterministic}");
        Console.WriteLine($"Verdict: {v.Outcome} {(v.SubVerdicts.Count > 0 ? "[" + string.Join(",", v.SubVerdicts) + "]" : "")}");
        if (v.Detail != null) Console.WriteLine($"Detail: {v.Detail}");
        Console.WriteLine($"Kusto: {Summarize(diff.Kusto)}");
        Console.WriteLine($"Duck : {Summarize(diff.DuckDb)}");
        return 0;
    }

    case "seedtest":
    {
        using var runner = new DifferentialRunner(endpoint, db);
        int ok = 0, bad = 0;
        foreach (var seed in Seeds.All)
        {
            var q = QueryAnalyzer.Enrich(new GeneratedQuery { Id = seed.Name, Family = "seed", Kql = seed.Kql, Seeds = new[] { seed.Name } });
            var (diff, v) = await runner.RunAsync(q);
            var status = v.Outcome == Outcome.Match ? "OK " : "!! ";
            if (v.Outcome == Outcome.Match) ok++; else bad++;
            Console.WriteLine($"{status}{seed.Name,-12} {v.Outcome} {v.Detail}");
            if (v.Outcome != Outcome.Match)
            {
                Console.WriteLine($"     kusto: {Summarize(diff.Kusto)}");
                Console.WriteLine($"     duck : {Summarize(diff.DuckDb)}");
            }
        }
        Console.WriteLine($"\nSeeds: {ok} match, {bad} mismatch.");
        return bad == 0 ? 0 : 2;
    }

    case "validate":
    {
        var inPath = opts.Require("in");
        var outPath = opts.Get("out", inPath + ".validated");
        using var oracle = new KustoOracle(endpoint, db);
        var kept = new List<GeneratedQuery>();
        int dropped = 0;
        foreach (var q in Jsonl.ReadFile<GeneratedQuery>(inPath))
        {
            var r = await oracle.RunQueryAsync(q.Kql);
            if (r.IsError) { dropped++; continue; }
            kept.Add(QueryAnalyzer.Enrich(q));
        }
        Jsonl.WriteFile(outPath, kept);
        Console.WriteLine($"Validated: kept {kept.Count}, dropped {dropped} (Kusto-rejected) -> {outPath}");
        return 0;
    }

    case "run":
    {
        var inPath = opts.Require("in");
        var outPath = opts.Get("out", "fuzzing/verdicts/verdicts.jsonl");
        var cmpOpts = new ComparisonOptions
        {
            CompareColumnNames = opts.Has("strict-names"),
            NullEqualsEmpty = opts.Has("null-eq-empty"),
        };
        using var runner = new DifferentialRunner(endpoint, db, cmpOpts);
        var verdicts = new List<VerdictRecord>();
        var byOutcome = new Dictionary<string, int>();
        int i = 0;
        foreach (var raw in Jsonl.ReadFile<GeneratedQuery>(inPath))
        {
            var q = QueryAnalyzer.Enrich(raw);
            var (diff, v) = await runner.RunAsync(q);
            verdicts.Add(VerdictRecord.Build(q, diff, v));
            byOutcome[v.Outcome.ToString()] = byOutcome.GetValueOrDefault(v.Outcome.ToString()) + 1;
            if (++i % 50 == 0) Console.Error.WriteLine($"  ...{i} processed");
        }
        Jsonl.WriteFile(outPath, verdicts);
        Console.WriteLine($"Ran {verdicts.Count} queries -> {outPath}");
        foreach (var kv in byOutcome.OrderByDescending(k => k.Value))
            Console.WriteLine($"  {kv.Key,-26} {kv.Value}");
        var bugs = verdicts.Count(v => v.IsBug);
        Console.WriteLine($"  => {bugs} bug candidates");
        return 0;
    }

    case "runtext":
    {
        // Run a plain-text file of newline-separated KQL queries (the format agents emit). Lines
        // starting with '#' or '//' are comments. The family is taken from --family (default "adhoc").
        var inPath = opts.Require("in");
        var outPath = opts.Get("out", "fuzzing/verdicts/agent.jsonl");
        var family = opts.Get("family", "adhoc");
        var cmpOpts = new ComparisonOptions
        {
            CompareColumnNames = opts.Has("strict-names"),
            NullEqualsEmpty = opts.Has("null-eq-empty"),
        };
        using var runner = new DifferentialRunner(endpoint, db, cmpOpts);
        var verdicts = new List<VerdictRecord>();
        var byOutcome = new Dictionary<string, int>();
        int n = 0;
        foreach (var line in File.ReadLines(inPath))
        {
            var kql = line.Trim();
            if (kql.Length == 0 || kql.StartsWith('#') || kql.StartsWith("//")) continue;
            var q = QueryAnalyzer.Enrich(new GeneratedQuery { Id = $"agent-{family}-{n:D4}", Tier = 2, Family = family, Kql = kql });
            var (diff, v) = await runner.RunAsync(q);
            verdicts.Add(VerdictRecord.Build(q, diff, v));
            byOutcome[v.Outcome.ToString()] = byOutcome.GetValueOrDefault(v.Outcome.ToString()) + 1;
            n++;
        }
        Jsonl.WriteFile(outPath, verdicts);
        Console.WriteLine($"Ran {verdicts.Count} queries -> {outPath}");
        foreach (var kv in byOutcome.OrderByDescending(k => k.Value))
            Console.WriteLine($"  {kv.Key,-26} {kv.Value}");
        Console.WriteLine($"  => {verdicts.Count(v => v.IsBug)} bug candidates");
        return 0;
    }

    case "report":
    {
        var inPaths = opts.Require("in").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var outDir = opts.Get("out", "fuzzing/reports");
        var all = inPaths.SelectMany(Jsonl.ReadFile<VerdictRecord>).ToList();
        Reporter.Write(all, outDir);
        Console.WriteLine($"Report written to {outDir} (from {all.Count} verdicts across {inPaths.Length} files).");
        return 0;
    }

    default:
        Usage();
        return 1;
}

static string Summarize(EngineResult r) =>
    r.IsError ? $"ERROR({r.Stage}): {r.Error}" :
    $"cols=[{string.Join(",", r.Columns.Select(c => c.Name + ":" + c.Class))}] rows={r.Rows.Count} " +
    (r.Rows.Count > 0 ? Comparator.FormatRow(r.Rows[0]) : "");

static void Usage() => Console.WriteLine(
    "Usage: KqlToSql.Fuzzer <generate|seedtest|validate|run|report> [--in p] [--out p] [--endpoint url] [--db name] [--strict-names] [--null-eq-empty]");

namespace KqlToSql.Fuzzer
{
    internal sealed class ArgParser
    {
        private readonly Dictionary<string, string> _kv = new(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _flags = new(StringComparer.OrdinalIgnoreCase);

        public static ArgParser Parse(string[] args)
        {
            var p = new ArgParser();
            for (int i = 1; i < args.Length; i++)
            {
                if (!args[i].StartsWith("--")) continue;
                var key = args[i][2..];
                if (i + 1 < args.Length && !args[i + 1].StartsWith("--")) { p._kv[key] = args[++i]; }
                else p._flags.Add(key);
            }
            return p;
        }

        public string Get(string key, string fallback) => _kv.TryGetValue(key, out var v) ? v : fallback;
        public bool Has(string key) => _flags.Contains(key) || _kv.ContainsKey(key);
        public string Require(string key) => _kv.TryGetValue(key, out var v) ? v
            : throw new ArgumentException($"Missing required --{key}");
    }
}
