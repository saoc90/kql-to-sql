using System.Text.Json;
using System.Text.Json.Serialization;

namespace KqlToSql.Fuzzer;

public enum ComparisonMode { Multiset, Ordered }

/// <summary>One generated KQL test case. Serialized as a line in the JSONL corpus.</summary>
public sealed record GeneratedQuery
{
    public string Id { get; init; } = "";
    public int Tier { get; init; }
    public string Family { get; init; } = "";
    public string Kql { get; init; } = "";
    public string[] Seeds { get; init; } = Array.Empty<string>();
    public int Depth { get; init; }
    public ComparisonMode ExpectedMode { get; init; } = ComparisonMode.Multiset;
    public string[]? OrderKeys { get; init; }
    public bool Nondeterministic { get; init; }
    public bool ExpectedUnsupported { get; init; }
    public bool NeedsStormEvents { get; init; }
    public string? Rationale { get; init; }
    public Dictionary<string, string>? GeneratorAxes { get; init; }
}

public static class Jsonl
{
    public static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
        WriteIndented = false,
    };

    public static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Options);

    public static T Deserialize<T>(string line) => JsonSerializer.Deserialize<T>(line, Options)!;

    public static IEnumerable<T> ReadFile<T>(string path)
    {
        foreach (var line in File.ReadLines(path))
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            yield return Deserialize<T>(line);
        }
    }

    public static void WriteFile<T>(string path, IEnumerable<T> items)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        using var w = new StreamWriter(path, append: false);
        foreach (var item in items) w.WriteLine(Serialize(item));
    }
}
