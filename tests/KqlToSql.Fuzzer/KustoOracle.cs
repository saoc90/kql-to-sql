using System.Globalization;
using System.Text;
using System.Text.Json;

namespace KqlToSql.Fuzzer;

/// <summary>
/// Talks to a Kustainer (Kusto emulator) instance over the v1 REST API. We use raw HTTP
/// rather than the Kusto .NET SDK because Kustainer is auth-free HTTP and the REST response
/// hands us exact Kusto <c>ColumnType</c> strings (long/real/datetime/dynamic/...) which makes
/// type normalization precise. The primary result is always table index 0 ("Table_0").
/// </summary>
public sealed class KustoOracle : IDisposable
{
    private readonly HttpClient _http;
    private readonly string _db;

    public KustoOracle(string endpoint, string db, TimeSpan? timeout = null)
    {
        _http = new HttpClient { BaseAddress = new Uri(endpoint), Timeout = timeout ?? TimeSpan.FromSeconds(30) };
        _db = db;
    }

    public Task<EngineResult> RunQueryAsync(string kql, CancellationToken ct = default)
        => PostAsync("/v1/rest/query", kql, ct);

    public Task<EngineResult> RunMgmtAsync(string command, CancellationToken ct = default)
        => PostAsync("/v1/rest/mgmt", command, ct);

    private async Task<EngineResult> PostAsync(string path, string csl, CancellationToken ct)
    {
        try
        {
            var payload = JsonSerializer.Serialize(new { db = _db, csl });
            using var content = new StringContent(payload, Encoding.UTF8, "application/json");
            using var resp = await _http.PostAsync(path, content, ct);
            var body = await resp.Content.ReadAsStringAsync(ct);

            if (!resp.IsSuccessStatusCode)
                return EngineResult.Failure(ExtractError(body, (int)resp.StatusCode), ErrorStage.Execute);

            return ParsePrimaryTable(body);
        }
        catch (Exception ex)
        {
            return EngineResult.Failure(ex.Message, ErrorStage.Execute);
        }
    }

    private static string ExtractError(string body, int status)
    {
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.TryGetProperty("error", out var err))
            {
                if (err.TryGetProperty("@message", out var m) && m.ValueKind == JsonValueKind.String)
                    return $"[{status}] {m.GetString()}";
                if (err.TryGetProperty("message", out var m2) && m2.ValueKind == JsonValueKind.String)
                    return $"[{status}] {m2.GetString()}";
            }
        }
        catch { /* fall through */ }
        var trimmed = body.Length > 400 ? body[..400] : body;
        return $"[{status}] {trimmed}";
    }

    private static EngineResult ParsePrimaryTable(string body)
    {
        using var doc = JsonDocument.Parse(body);
        if (!doc.RootElement.TryGetProperty("Tables", out var tables) || tables.GetArrayLength() == 0)
            return EngineResult.Failure("Kusto response had no Tables", ErrorStage.Execute);

        var primary = tables[0];
        var colsEl = primary.GetProperty("Columns");
        var columns = new List<ColumnInfo>(colsEl.GetArrayLength());
        var classes = new TypeClass[colsEl.GetArrayLength()];
        int i = 0;
        foreach (var c in colsEl.EnumerateArray())
        {
            var name = c.GetProperty("ColumnName").GetString() ?? $"Column{i}";
            var ctype = c.TryGetProperty("ColumnType", out var ct) ? ct.GetString() : null;
            var cls = TypeNormalizer.FromKustoColumnType(ctype);
            classes[i] = cls;
            columns.Add(new ColumnInfo(name, cls, ctype ?? "?"));
            i++;
        }

        var rows = new List<object?[]>();
        foreach (var r in primary.GetProperty("Rows").EnumerateArray())
        {
            // QueryStatus / out-of-band rows are sometimes objects, not arrays — skip them.
            if (r.ValueKind != JsonValueKind.Array) continue;
            var cells = new object?[columns.Count];
            int j = 0;
            foreach (var cell in r.EnumerateArray())
            {
                if (j >= cells.Length) break;
                cells[j] = ConvertCell(cell, classes[j]);
                j++;
            }
            rows.Add(cells);
        }

        return new EngineResult(columns, rows, null, ErrorStage.None);
    }

    /// <summary>Convert a Kusto REST JSON cell into a normalized CLR object the comparator understands.</summary>
    private static object? ConvertCell(JsonElement cell, TypeClass cls)
    {
        if (cell.ValueKind == JsonValueKind.Null) return null;

        switch (cls)
        {
            case TypeClass.Dynamic:
                // Preserve the structured value as canonical-able raw JSON text.
                return new DynamicJson(cell.GetRawText());

            case TypeClass.Bool:
                if (cell.ValueKind is JsonValueKind.True or JsonValueKind.False) return cell.GetBoolean();
                if (cell.ValueKind == JsonValueKind.String) return bool.TryParse(cell.GetString(), out var b) && b;
                return cell.ValueKind == JsonValueKind.Number && cell.GetDouble() != 0;

            case TypeClass.Int:
                if (cell.ValueKind == JsonValueKind.Number && cell.TryGetInt64(out var l)) return l;
                if (cell.ValueKind == JsonValueKind.String && long.TryParse(cell.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var ls)) return ls;
                return cell.ValueKind == JsonValueKind.Number ? (object)cell.GetDouble() : cell.GetRawText();

            case TypeClass.Real:
                if (cell.ValueKind == JsonValueKind.Number) return cell.GetDouble();
                if (cell.ValueKind == JsonValueKind.String)
                {
                    var s = cell.GetString();
                    if (string.Equals(s, "NaN", StringComparison.OrdinalIgnoreCase)) return double.NaN;
                    if (string.Equals(s, "Infinity", StringComparison.OrdinalIgnoreCase)) return double.PositiveInfinity;
                    if (string.Equals(s, "-Infinity", StringComparison.OrdinalIgnoreCase)) return double.NegativeInfinity;
                    if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var d)) return d;
                }
                return cell.GetRawText();

            case TypeClass.DateTime:
                if (cell.ValueKind == JsonValueKind.String &&
                    DateTime.TryParse(cell.GetString(), CultureInfo.InvariantCulture,
                        DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var dt))
                    return dt;
                return cell.GetString();

            case TypeClass.TimeSpan:
                if (cell.ValueKind == JsonValueKind.String && TimeSpan.TryParse(cell.GetString(), CultureInfo.InvariantCulture, out var ts))
                    return ts;
                return cell.GetString();

            case TypeClass.Guid:
                return cell.ValueKind == JsonValueKind.String ? cell.GetString()?.ToLowerInvariant() : cell.GetRawText();

            default:
                return cell.ValueKind == JsonValueKind.String ? cell.GetString() : cell.GetRawText();
        }
    }

    public void Dispose() => _http.Dispose();
}

/// <summary>Marker wrapper for a dynamic/JSON cell so the comparator can canonicalize it.</summary>
public sealed record DynamicJson(string RawJson);
