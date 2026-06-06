using System.Text.RegularExpressions;
using Kusto.Language;
using Kusto.Language.Syntax;

namespace KqlToSql.Fuzzer;

/// <summary>
/// Inspects a KQL string (textually + via the official parser) to decide its comparison mode,
/// whether it is nondeterministic/approximate, and whether it uses a known-unsupported operator.
/// These flags let the comparator suppress non-bug differences. Conservative by design:
/// only mark Ordered when clearly ordered (avoid false MismatchOrder), and over-mark
/// nondeterministic when in doubt (avoid false MismatchRows).
/// </summary>
public static partial class QueryAnalyzer
{
    // Functions/operators whose results are time-dependent, random, or approximate.
    private static readonly string[] NondeterministicMarkers =
    {
        "rand", "now", "ago", "new_guid", "newguid", "guid(", "datetime(now",
        "dcount", "dcountif", "hll", "hll_if", "hll_merge", "tdigest", "percentile",
        "sample", "take_any", "any(", "anyif", "current_", "rand(", "make_string(rand",
        // top-hitters is approximate AND its tie-break among equal-weight hitters is arbitrary —
        // engines legitimately disagree on which tied value fills the last slot.
        "top-hitters",
    };

    // Operators the translator deliberately does not support (per KqlOperatorsChecklist.md).
    private static readonly string[] UnsupportedOperatorMarkers =
    {
        "facet", "fork", "invoke", "macro-expand", "macro_expand", "partition", "reduce",
        "project-by-names", "find", "make-graph", "graph-match", "graph-to-table",
        "graph-shortest-paths", "graph-mark-components",
        "evaluate autocluster", "evaluate basket", "evaluate diffpatterns", "evaluate preview",
    };

    // Operator type names (Kusto.Language.Syntax) whose presence as the final stage implies a guaranteed
    // output row order. Notably absent: top-nested (a hierarchical aggregation — Kusto does NOT guarantee
    // the flattened row order, only the per-level selection) and top-hitters (approximate; order among the
    // returned hitters is arbitrary). Asserting order on those produced false, run-to-run-flaky
    // MismatchOrder verdicts; they are compared as a multiset, which still catches wrong-row-set bugs.
    private static readonly HashSet<string> OrderingOperatorTypes = new(StringComparer.Ordinal)
    {
        "SortOperator", "TopOperator", "SerializeOperator",
    };

    public static (ComparisonMode Mode, bool Nondeterministic, bool ExpectedUnsupported) Analyze(string kql)
    {
        var lower = kql.ToLowerInvariant();

        bool nondet = NondeterministicMarkers.Any(m => lower.Contains(m));
        bool unsupported = UnsupportedOperatorMarkers.Any(m => PipeStageContains(lower, m));

        var mode = ComparisonMode.Multiset;
        try
        {
            var root = KustoCode.Parse(kql).Syntax;
            var ops = root.GetDescendants<QueryOperator>();
            var last = ops.Count > 0 ? ops[ops.Count - 1] : null;
            if (last != null && OrderingOperatorTypes.Contains(last.GetType().Name))
                mode = ComparisonMode.Ordered;
        }
        catch
        {
            // Parse failure: leave as multiset; the differential run will surface a Kusto/translate error.
        }

        return (mode, nondet, unsupported);
    }

    /// <summary>Apply analysis to a query, filling in flags the generator/agent left at defaults.</summary>
    public static GeneratedQuery Enrich(GeneratedQuery q)
    {
        var (mode, nondet, unsupported) = Analyze(q.Kql);
        var lower = q.Kql.ToLowerInvariant();
        // make_set/make_bag are unordered set aggregates — Kusto leaves element order unspecified.
        bool setSemantics = lower.Contains("make_set") || lower.Contains("make_bag") || lower.Contains("makeset");
        return q with
        {
            ExpectedMode = q.ExpectedMode == ComparisonMode.Ordered ? ComparisonMode.Ordered : mode,
            Nondeterministic = q.Nondeterministic || nondet,
            ExpectedUnsupported = q.ExpectedUnsupported || unsupported,
            SetSemantics = q.SetSemantics || setSemantics,
        };
    }

    // crude "is this token a pipe-stage keyword" check to avoid matching inside identifiers/strings
    private static bool PipeStageContains(string lowerKql, string marker)
    {
        var rx = new Regex($@"(^|\|)\s*{Regex.Escape(marker)}\b");
        return rx.IsMatch(lowerKql);
    }
}
