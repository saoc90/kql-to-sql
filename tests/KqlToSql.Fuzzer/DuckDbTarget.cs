using System.Data.Common;
using DuckDB.NET.Data;
using DuckDB.NET.Native;
using KqlToSql.Dialects;

namespace KqlToSql.Fuzzer;

/// <summary>
/// The system-under-test channel: translate KQL → SQL with KqlToSqlConverter, then execute the
/// SQL on an in-memory DuckDB. Translate-time and execute-time failures are captured separately
/// so a divergence can be attributed to the translator vs invalid generated SQL vs a true mismatch.
/// </summary>
public sealed class DuckDbTarget
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    /// <summary>Translate only (no execution). Throws if the translator throws.</summary>
    public string Translate(string kql) => _converter.Convert(kql);

    /// <summary>
    /// Translate and execute. <paramref name="seed"/> optionally creates/populates tables on the
    /// connection before the query runs (used by the realistic StormEvents tier).
    /// </summary>
    public (EngineResult Result, string? Sql) Run(string kql, Action<DuckDBConnection>? seed = null)
    {
        string sql;
        try
        {
            sql = _converter.Convert(kql);
        }
        catch (Exception ex)
        {
            return (EngineResult.Failure(ex.Message, ErrorStage.Translate), null);
        }

        try
        {
            DuckDbNative.Ensure();
            using var conn = new DuckDBConnection("DataSource=:memory:");
            conn.Open();
            seed?.Invoke(conn);

            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            using var reader = cmd.ExecuteReader();
            return (Read(reader), sql);
        }
        catch (Exception ex)
        {
            return (EngineResult.Failure(ex.Message, ErrorStage.Execute), sql);
        }
    }

    private static EngineResult Read(DbDataReader reader)
    {
        var columns = new List<ColumnInfo>(reader.FieldCount);
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var t = reader.GetFieldType(i);
            columns.Add(new ColumnInfo(reader.GetName(i), TypeNormalizer.FromClrType(t), t?.Name ?? "?"));
        }

        var rows = new List<object?[]>();
        while (reader.Read())
        {
            var cells = new object?[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
                cells[i] = ReadCell(reader, i);
            rows.Add(cells);
        }

        return new EngineResult(columns, rows, null, ErrorStage.None);
    }

    private static object? ReadCell(DbDataReader reader, int i)
    {
        if (reader.IsDBNull(i)) return null;
        try
        {
            var v = reader.GetValue(i);
            if (v is DBNull) return null;
            // DuckDB LIST/STRUCT columns come back as List<>/arrays; Kusto returns the same data as a
            // dynamic JSON array/object. Canonicalize to JSON so the comparator's dynamic path applies.
            if (v is not string && v is not byte[] && v is System.Collections.IEnumerable)
                return new DynamicJson(System.Text.Json.JsonSerializer.Serialize(v));
            return v;
        }
        catch
        {
            // DuckDB.NET throws converting negative/large INTERVAL → TimeSpan because Micros is a
            // UInt64 (negative intervals wrap). Read the raw interval and reinterpret the bits as
            // signed. Months are treated as 0 (KQL timespans have no month component).
            try
            {
                var iv = reader.GetFieldValue<DuckDBInterval>(i);
                long signedMicros = unchecked((long)iv.Micros);
                long ticks = (long)iv.Days * TimeSpan.TicksPerDay + signedMicros * 10;
                return new TimeSpan(ticks);
            }
            catch (Exception ex)
            {
                return $"<unreadable:{ex.GetType().Name}>";
            }
        }
    }
}
