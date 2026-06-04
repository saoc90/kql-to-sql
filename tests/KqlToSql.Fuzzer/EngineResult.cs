namespace KqlToSql.Fuzzer;

/// <summary>Which stage failed when an engine could not produce a result.</summary>
public enum ErrorStage
{
    None,
    Translate, // KqlToSqlConverter.Convert threw
    Execute,   // the engine rejected/failed the query
}

/// <summary>A coarse type bucket both engines map onto so values are comparable.</summary>
public enum TypeClass
{
    Bool,
    Int,      // int + long
    Real,     // real/double + decimal
    String,
    DateTime,
    TimeSpan,
    Guid,
    Dynamic,  // dynamic / JSON
    Unknown,
}

public sealed record ColumnInfo(string Name, TypeClass Class, string RawType);

/// <summary>The result of running one query on one engine.</summary>
public sealed record EngineResult(
    IReadOnlyList<ColumnInfo> Columns,
    IReadOnlyList<object?[]> Rows,
    string? Error,
    ErrorStage Stage)
{
    public bool IsError => Error is not null;

    public static EngineResult Failure(string error, ErrorStage stage) =>
        new(Array.Empty<ColumnInfo>(), Array.Empty<object?[]>(), error, stage);
}

/// <summary>Both engines' results for one KQL query, plus the SQL the translator emitted.</summary>
public sealed record DiffResult(string Kql, string? Sql, EngineResult Kusto, EngineResult DuckDb);
