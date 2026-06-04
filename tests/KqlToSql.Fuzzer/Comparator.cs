using System.Globalization;
using System.Text;
using System.Text.Json;

namespace KqlToSql.Fuzzer;

public enum Outcome
{
    Match,
    MismatchRows,      // values / cardinality differ
    MismatchColumns,   // column count or (strict) names differ
    MismatchOrder,     // right rows, wrong order for an ordered query
    TranslateError,    // translator threw on a query Kusto accepts
    SqlExecError,      // DuckDB rejected the generated SQL (strongest bug signal)
    KustoError,        // Kusto rejected the KQL -> malformed query, discard
    BothError,         // both engines errored in a way we can't classify -> discard
    SkippedNondeterministic,
    SkippedUnsupported,
}

public sealed record Verdict(
    Outcome Outcome,
    string? Detail,
    IReadOnlyList<string> SubVerdicts)
{
    /// <summary>Whether this verdict represents a translator bug worth reporting.</summary>
    public bool IsBug => Outcome is Outcome.MismatchRows or Outcome.MismatchColumns
        or Outcome.MismatchOrder or Outcome.SqlExecError or Outcome.TranslateError;

    public string Severity => Outcome switch
    {
        Outcome.SqlExecError => "highest",
        Outcome.MismatchRows or Outcome.MismatchColumns => "high",
        Outcome.TranslateError => "high",
        Outcome.MismatchOrder => "medium",
        _ => "none",
    };
}

public sealed class ComparisonOptions
{
    /// <summary>When true, differing column names (same count) escalate to MismatchColumns;
    /// otherwise a NAME_MISMATCH sub-verdict is recorded but row comparison still proceeds.</summary>
    public bool CompareColumnNames { get; set; }
    /// <summary>Treat SQL NULL as equal to the empty string (records NULL_VS_EMPTY).</summary>
    public bool NullEqualsEmpty { get; set; }
    public double AbsEpsilon { get; set; } = 1e-9;
    public double RelEpsilon { get; set; } = 1e-9;

    public static ComparisonOptions Default => new();
}

/// <summary>
/// Decides MATCH vs MISMATCH between the oracle (Kusto) and SUT (translator→DuckDB) results,
/// suppressing differences that are not translator bugs (row order where undefined, float noise,
/// datetime precision, dynamic/JSON key order, type widening, known nondeterminism/unsupported).
/// </summary>
public static class Comparator
{
    public static Verdict Compare(GeneratedQuery q, EngineResult kusto, EngineResult duck, ComparisonOptions? options = null)
    {
        var opts = options ?? ComparisonOptions.Default;
        var subs = new List<string>();

        // ---- error handling ------------------------------------------------
        bool kErr = kusto.IsError, dErr = duck.IsError;

        if (dErr && duck.Stage == ErrorStage.Translate)
        {
            if (q.ExpectedUnsupported) return new Verdict(Outcome.SkippedUnsupported, duck.Error, subs);
            if (kErr) return new Verdict(Outcome.KustoError, $"both failed; kusto: {kusto.Error}", subs);
            return new Verdict(Outcome.TranslateError, duck.Error, subs);
        }
        if (dErr && duck.Stage == ErrorStage.Execute)
        {
            if (kErr) return new Verdict(Outcome.KustoError, $"both failed; kusto: {kusto.Error}", subs);
            return new Verdict(Outcome.SqlExecError, duck.Error, subs);
        }
        // DuckDB succeeded from here on.
        if (kErr) return new Verdict(Outcome.KustoError, kusto.Error, subs);

        if (q.Nondeterministic) return new Verdict(Outcome.SkippedNondeterministic, null, subs);

        // ---- columns -------------------------------------------------------
        if (kusto.Columns.Count != duck.Columns.Count)
            return new Verdict(Outcome.MismatchColumns,
                $"column count: kusto={kusto.Columns.Count} duck={duck.Columns.Count} " +
                $"(kusto: {Names(kusto)}; duck: {Names(duck)})", subs);

        bool namesDiffer = false;
        for (int i = 0; i < kusto.Columns.Count; i++)
            if (!string.Equals(kusto.Columns[i].Name, duck.Columns[i].Name, StringComparison.Ordinal))
                namesDiffer = true;
        if (namesDiffer)
        {
            if (opts.CompareColumnNames)
                return new Verdict(Outcome.MismatchColumns,
                    $"names: kusto={Names(kusto)} duck={Names(duck)}", subs);
            subs.Add("NAME_MISMATCH");
        }

        for (int i = 0; i < kusto.Columns.Count; i++)
        {
            var kc = kusto.Columns[i].Class;
            var dc = duck.Columns[i].Class;
            if (kc != TypeClass.Unknown && dc != TypeClass.Unknown && kc != dc)
            {
                // int vs real is a frequent, usually-benign widening; flag softly.
                subs.Add($"TYPE_MISMATCH[{kusto.Columns[i].Name}:{kc}|{dc}]");
            }
        }

        // ---- rows ----------------------------------------------------------
        if (kusto.Rows.Count != duck.Rows.Count)
            return new Verdict(Outcome.MismatchRows,
                $"row count: kusto={kusto.Rows.Count} duck={duck.Rows.Count}", subs);

        if (q.ExpectedMode == ComparisonMode.Ordered)
        {
            if (OrderedEqual(kusto.Rows, duck.Rows, q.OrderKeys, kusto.Columns, opts, subs))
                return new Verdict(Outcome.Match, null, subs);
            if (MultisetEqual(kusto.Rows, duck.Rows, opts, subs))
                return new Verdict(Outcome.MismatchOrder, "rows match as a set but order differs", subs);
            return new Verdict(Outcome.MismatchRows, FirstRowDiff(kusto, duck, opts), subs);
        }
        else
        {
            if (MultisetEqual(kusto.Rows, duck.Rows, opts, subs))
                return new Verdict(Outcome.Match, null, subs);
            return new Verdict(Outcome.MismatchRows, FirstRowDiff(kusto, duck, opts), subs);
        }
    }

    // ---- row comparison ----------------------------------------------------

    private static bool MultisetEqual(IReadOnlyList<object?[]> a, IReadOnlyList<object?[]> b, ComparisonOptions opts, List<string> subs)
    {
        if (a.Count != b.Count) return false;
        var used = new bool[b.Count];
        foreach (var ra in a)
        {
            int match = -1;
            for (int j = 0; j < b.Count; j++)
            {
                if (used[j]) continue;
                if (RowEqual(ra, b[j], opts, null)) { match = j; break; }
            }
            if (match < 0) return false;
            used[match] = true;
        }
        // collect sub-verdicts (e.g. NULL_VS_EMPTY) on a best-effort pass
        for (int i = 0; i < a.Count; i++) RowEqual(a[i], b[i], opts, subs);
        return true;
    }

    private static bool OrderedEqual(IReadOnlyList<object?[]> a, IReadOnlyList<object?[]> b, string[]? orderKeys,
        IReadOnlyList<ColumnInfo> cols, ComparisonOptions opts, List<string> subs)
    {
        if (a.Count != b.Count) return false;

        // Resolve order-key column indices (for tie-block relaxation).
        int[] keyIdx = Array.Empty<int>();
        if (orderKeys is { Length: > 0 })
            keyIdx = orderKeys
                .Select(k => IndexOfColumn(cols, k))
                .Where(ix => ix >= 0)
                .ToArray();

        int i = 0;
        while (i < a.Count)
        {
            // Determine the extent of the current tie block on side a (rows sharing equal order keys).
            int end = i + 1;
            if (keyIdx.Length > 0)
                while (end < a.Count && KeysEqual(a[i], a[end], keyIdx, opts)) end++;

            int len = end - i;
            if (len == 1)
            {
                if (!RowEqual(a[i], b[i], opts, subs)) return false;
            }
            else
            {
                // Within a tie block, order among equal keys is undefined: compare as a multiset.
                var blockA = Slice(a, i, len);
                var blockB = Slice(b, i, len);
                if (!MultisetEqual(blockA, blockB, opts, subs)) return false;
            }
            i = end;
        }
        return true;
    }

    private static List<object?[]> Slice(IReadOnlyList<object?[]> rows, int start, int len)
    {
        var list = new List<object?[]>(len);
        for (int i = 0; i < len; i++) list.Add(rows[start + i]);
        return list;
    }

    private static bool KeysEqual(object?[] a, object?[] b, int[] keyIdx, ComparisonOptions opts)
    {
        foreach (var k in keyIdx)
            if (!CellEqual(a[k], b[k], opts, null)) return false;
        return true;
    }

    private static int IndexOfColumn(IReadOnlyList<ColumnInfo> cols, string name)
    {
        for (int i = 0; i < cols.Count; i++)
            if (string.Equals(cols[i].Name, name, StringComparison.OrdinalIgnoreCase)) return i;
        return -1;
    }

    private static bool RowEqual(object?[] a, object?[] b, ComparisonOptions opts, List<string>? subs)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
            if (!CellEqual(a[i], b[i], opts, subs)) return false;
        return true;
    }

    // ---- cell comparison ---------------------------------------------------

    public static bool CellEqual(object? a, object? b, ComparisonOptions opts, List<string>? subs)
    {
        if (a is null && b is null) return true;
        if (a is null || b is null)
        {
            var other = a ?? b;
            if (opts.NullEqualsEmpty && other is string s && s.Length == 0) { subs?.AddOnce("NULL_VS_EMPTY"); return true; }
            return false;
        }

        // dynamic / JSON
        if (a is DynamicJson || b is DynamicJson)
            return JsonEqual(ToJsonText(a), ToJsonText(b));

        // numeric (covers int/long/double/decimal/float widening across engines)
        if (IsNumeric(a) && IsNumeric(b))
            return NumericEqual(a, b, opts);

        // datetime
        if (a is DateTime || b is DateTime)
        {
            if (TryDate(a, out var da) && TryDate(b, out var db)) return DateEqual(da, db);
            return false;
        }

        // timespan
        if (a is TimeSpan || b is TimeSpan)
        {
            if (TryTimeSpan(a, out var ta) && TryTimeSpan(b, out var tb))
                return Math.Abs((ta - tb).Ticks) <= 10; // 1 microsecond
            return false;
        }

        if (a is bool ba && b is bool bb) return ba == bb;

        // strings / guids / fallback
        var sa = NormalizeString(a);
        var sb = NormalizeString(b);
        return string.Equals(sa, sb, StringComparison.Ordinal);
    }

    private static bool IsNumeric(object o) =>
        o is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal or System.Numerics.BigInteger;

    private static bool NumericEqual(object a, object b, ComparisonOptions opts)
    {
        // Exact integer comparison when both are integral.
        if (IsIntegral(a) && IsIntegral(b))
        {
            try { return ToDecimal(a) == ToDecimal(b); } catch { /* overflow -> fall through */ }
        }

        double da = ToDouble(a), db = ToDouble(b);
        if (double.IsNaN(da) || double.IsNaN(db)) return double.IsNaN(da) && double.IsNaN(db);
        if (double.IsInfinity(da) || double.IsInfinity(db)) return da == db;
        double diff = Math.Abs(da - db);
        double tol = opts.AbsEpsilon + opts.RelEpsilon * Math.Max(Math.Abs(da), Math.Abs(db));
        return diff <= tol;
    }

    private static bool IsIntegral(object o) =>
        o is sbyte or byte or short or ushort or int or uint or long or ulong or System.Numerics.BigInteger;

    private static decimal ToDecimal(object o) => o switch
    {
        System.Numerics.BigInteger bi => (decimal)bi,
        _ => Convert.ToDecimal(o, CultureInfo.InvariantCulture),
    };

    private static double ToDouble(object o) => o switch
    {
        System.Numerics.BigInteger bi => (double)bi,
        _ => Convert.ToDouble(o, CultureInfo.InvariantCulture),
    };

    private static bool TryDate(object? o, out DateTime dt)
    {
        switch (o)
        {
            // Both engines store these literals as timezone-naive UTC. DuckDB.NET returns Kind=Unspecified;
            // treat that as UTC (do NOT ToUniversalTime, which would shift by the host's local offset).
            case DateTime d:
                dt = d.Kind == DateTimeKind.Utc ? d
                    : d.Kind == DateTimeKind.Local ? d.ToUniversalTime()
                    : DateTime.SpecifyKind(d, DateTimeKind.Utc);
                return true;
            case DateTimeOffset dto: dt = dto.UtcDateTime; return true;
            case string s when DateTime.TryParse(s, CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var p): dt = p; return true;
            default: dt = default; return false;
        }
    }

    private static bool DateEqual(DateTime a, DateTime b)
    {
        // Compare at microsecond resolution (DuckDB's limit), tolerating sub-µs jitter.
        long ka = a.Ticks / 10, kb = b.Ticks / 10;
        return Math.Abs(ka - kb) <= 1;
    }

    private static bool TryTimeSpan(object? o, out TimeSpan ts)
    {
        switch (o)
        {
            case TimeSpan t: ts = t; return true;
            case string s when TimeSpan.TryParse(s, CultureInfo.InvariantCulture, out var p): ts = p; return true;
            case long l: ts = TimeSpan.FromTicks(l); return true;
            default: ts = default; return false;
        }
    }

    private static string NormalizeString(object o)
    {
        var s = o switch
        {
            string str => str,
            Guid g => g.ToString("D"),
            bool b => b ? "true" : "false",
            DateTime d => (TryDate(d, out var u) ? u : d).ToString("o", CultureInfo.InvariantCulture),
            _ => Convert.ToString(o, CultureInfo.InvariantCulture) ?? "",
        };
        return s.Normalize(NormalizationForm.FormC);
    }

    // ---- JSON / dynamic ----------------------------------------------------

    private static string ToJsonText(object? o) => o switch
    {
        null => "null",
        DynamicJson dj => dj.RawJson,
        string s => s,
        _ => JsonSerializer.Serialize(o),
    };

    public static bool JsonEqual(string a, string b)
    {
        var ca = TryCanonicalizeJson(a, out var oka);
        var cb = TryCanonicalizeJson(b, out var okb);
        if (oka && okb) return string.Equals(ca, cb, StringComparison.Ordinal);
        // One or both aren't valid JSON: compare trimmed raw text.
        return string.Equals(a.Trim(), b.Trim(), StringComparison.Ordinal);
    }

    private static string TryCanonicalizeJson(string s, out bool ok)
    {
        try
        {
            using var doc = JsonDocument.Parse(s);
            var sb = new StringBuilder();
            WriteCanonical(doc.RootElement, sb);
            ok = true;
            return sb.ToString();
        }
        catch
        {
            ok = false;
            return s;
        }
    }

    private static void WriteCanonical(JsonElement el, StringBuilder sb)
    {
        switch (el.ValueKind)
        {
            case JsonValueKind.Object:
                sb.Append('{');
                bool first = true;
                foreach (var p in el.EnumerateObject().OrderBy(p => p.Name, StringComparer.Ordinal))
                {
                    if (!first) sb.Append(',');
                    first = false;
                    sb.Append(JsonSerializer.Serialize(p.Name)).Append(':');
                    WriteCanonical(p.Value, sb);
                }
                sb.Append('}');
                break;
            case JsonValueKind.Array:
                sb.Append('[');
                bool f2 = true;
                foreach (var item in el.EnumerateArray())
                {
                    if (!f2) sb.Append(',');
                    f2 = false;
                    WriteCanonical(item, sb);
                }
                sb.Append(']');
                break;
            case JsonValueKind.Number:
                // Normalize number formatting (1.0 -> 1, 1e2 -> 100).
                sb.Append(el.TryGetDouble(out var d) ? d.ToString("R", CultureInfo.InvariantCulture) : el.GetRawText());
                break;
            case JsonValueKind.String:
                sb.Append(JsonSerializer.Serialize(el.GetString()));
                break;
            case JsonValueKind.True: sb.Append("true"); break;
            case JsonValueKind.False: sb.Append("false"); break;
            case JsonValueKind.Null: sb.Append("null"); break;
        }
    }

    // ---- diagnostics -------------------------------------------------------

    private static string Names(EngineResult r) => "[" + string.Join(", ", r.Columns.Select(c => c.Name)) + "]";

    private static string FirstRowDiff(EngineResult kusto, EngineResult duck, ComparisonOptions opts)
    {
        int n = Math.Min(kusto.Rows.Count, duck.Rows.Count);
        for (int i = 0; i < n; i++)
        {
            if (!RowEqual(kusto.Rows[i], duck.Rows[i], opts, null))
                return $"first differing row[{i}]: kusto={FormatRow(kusto.Rows[i])} duck={FormatRow(duck.Rows[i])}";
        }
        return $"rows differ (kusto={kusto.Rows.Count}, duck={duck.Rows.Count})";
    }

    public static string FormatRow(object?[] row) =>
        "(" + string.Join(", ", row.Select(FormatCell)) + ")";

    private static string FormatCell(object? c) => c switch
    {
        null => "null",
        DynamicJson dj => dj.RawJson,
        string s => $"'{s}'",
        // Render naive datetimes as-is (UTC) — do NOT ToUniversalTime, which would shift by host offset.
        DateTime d => (TryDate(d, out var u) ? u : d).ToString("o", CultureInfo.InvariantCulture),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => c.ToString() ?? "null",
    };
}

internal static class ListExtensions
{
    public static void AddOnce(this List<string> list, string item)
    {
        if (!list.Contains(item)) list.Add(item);
    }
}
