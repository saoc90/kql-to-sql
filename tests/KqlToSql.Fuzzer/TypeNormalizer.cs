namespace KqlToSql.Fuzzer;

/// <summary>
/// Maps Kusto column-type strings and DuckDB CLR types onto a shared <see cref="TypeClass"/>
/// so values from the two engines can be compared. Extends the spirit of the CLR→Kusto
/// MapType in src/KustoApi/Program.cs.
/// </summary>
public static class TypeNormalizer
{
    /// <summary>Classify from a Kusto REST <c>ColumnType</c> string (e.g. "long", "real", "dynamic").</summary>
    public static TypeClass FromKustoColumnType(string? columnType) => (columnType ?? "").ToLowerInvariant() switch
    {
        "bool" or "boolean" => TypeClass.Bool,
        "int" or "long" => TypeClass.Int,
        "real" or "double" or "decimal" => TypeClass.Real,
        "string" => TypeClass.String,
        "datetime" or "date" => TypeClass.DateTime,
        "timespan" or "time" => TypeClass.TimeSpan,
        "guid" or "uuid" => TypeClass.Guid,
        "dynamic" or "object" => TypeClass.Dynamic,
        _ => TypeClass.Unknown,
    };

    /// <summary>Classify from a DuckDB CLR field type.</summary>
    public static TypeClass FromClrType(Type? t)
    {
        if (t is null) return TypeClass.Unknown;
        t = Nullable.GetUnderlyingType(t) ?? t;

        if (t == typeof(bool)) return TypeClass.Bool;
        if (t == typeof(sbyte) || t == typeof(byte) || t == typeof(short) || t == typeof(ushort)
            || t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong)
            || t == typeof(System.Numerics.BigInteger))
            return TypeClass.Int;
        if (t == typeof(float) || t == typeof(double) || t == typeof(decimal)) return TypeClass.Real;
        if (t == typeof(string)) return TypeClass.String;
        if (t == typeof(DateTime) || t == typeof(DateTimeOffset)) return TypeClass.DateTime;
        if (t == typeof(TimeSpan)) return TypeClass.TimeSpan;
        if (t == typeof(Guid)) return TypeClass.Guid;
        return TypeClass.Unknown;
    }
}
