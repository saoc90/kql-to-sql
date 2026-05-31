namespace KqlToSql.Render;

/// <summary>
/// Render metadata extracted from a KQL <c>| render</c> operator. The field set mirrors the 19-key
/// "Visualization" annotation Kusto emits in the <c>@ExtendedProperties</c> frame of a query response,
/// so a single instance can populate both the WASM bridge result and the KustoApi response verbatim.
/// The render operator never changes the data/SQL; this is purely the chart-drawing instruction.
/// </summary>
public sealed record RenderInfo(
    string? Visualization,
    string? Title,
    string? XColumn,
    string[]? Series,
    string[]? YColumns,
    string[]? AnomalyColumns,
    string? XTitle,
    string? YTitle,
    string? XAxis,
    string? YAxis,
    string? Legend,
    string? YSplit,
    bool Accumulate,
    bool IsQuerySorted,
    string? Kind,
    string? Ymin,
    string? Ymax,
    string? Xmin,
    string? Xmax);
