using System;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class TypeCastFunctionTests
{
    [Fact]
    public void Converts_ToInt()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| take 1
| extend year_str=tostring(EpisodeId)
| project toint(year_str)";
        var sql = converter.Convert(kql);
        // toint truncates toward zero (Kusto), unlike DuckDB's rounding CAST — route through TRUNC(double).
        // A dynamic JSON boolean coerces true→1 / false→0, so a failed numeric parse falls back to a boolean cast.
        Assert.Equal("SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(year_str AS DOUBLE), TRY_CAST(TRY_CAST(year_str AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS year_str FROM (SELECT *, TRY_CAST(EpisodeId AS TEXT) AS year_str FROM StormEvents LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        // Single row, single query (unordered LIMIT 1 is non-deterministic across executions):
        // toint(tostring(EpisodeId)) must round-trip the integer for whatever row is read.
        cmd.CommandText = "SELECT EpisodeId, TRY_CAST(TRUNC(TRY_CAST(CAST(EpisodeId AS TEXT) AS DOUBLE)) AS INTEGER) FROM StormEvents LIMIT 1";
        using var rdr = cmd.ExecuteReader();
        Assert.True(rdr.Read());
        // EpisodeId is a fractional DOUBLE here; KQL toint truncates toward zero (e.g. 1.585 -> 1).
        Assert.Equal((long)Math.Truncate(rdr.GetDouble(0)), rdr.GetInt64(1));
    }

    [Fact]
    public void Converts_ToLong()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| take 1
| extend year_str=tostring(EpisodeId)
| project tolong(year_str)";
        var sql = converter.Convert(kql);
        // tolong truncates toward zero (Kusto), unlike DuckDB's rounding CAST — route through TRUNC(double).
        // A dynamic JSON boolean coerces true→1 / false→0, so a failed numeric parse falls back to a boolean cast.
        Assert.Equal("SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(year_str AS DOUBLE), TRY_CAST(TRY_CAST(year_str AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS year_str FROM (SELECT *, TRY_CAST(EpisodeId AS TEXT) AS year_str FROM StormEvents LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        // Single row, single query (unordered LIMIT 1 is non-deterministic across executions):
        // tolong(tostring(EpisodeId)) must round-trip the integer for whatever row is read.
        cmd.CommandText = "SELECT EpisodeId, TRY_CAST(TRUNC(TRY_CAST(CAST(EpisodeId AS TEXT) AS DOUBLE)) AS BIGINT) FROM StormEvents LIMIT 1";
        using var rdr = cmd.ExecuteReader();
        Assert.True(rdr.Read());
        // EpisodeId is a fractional DOUBLE here; KQL tolong truncates toward zero (e.g. 1.585 -> 1).
        Assert.Equal((long)Math.Truncate(rdr.GetDouble(0)), rdr.GetInt64(1));
    }

    [Fact]
    public void Converts_ToDouble()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| take 1
| extend year_str=tostring(EpisodeId)
| project todouble(year_str)";
        var sql = converter.Convert(kql);
        // A dynamic JSON boolean coerces true→1 / false→0, so a failed numeric parse falls back to a boolean cast.
        Assert.Equal("SELECT COALESCE(TRY_CAST(year_str AS DOUBLE), TRY_CAST(TRY_CAST(year_str AS BOOLEAN) AS DOUBLE)) AS year_str FROM (SELECT *, TRY_CAST(EpisodeId AS TEXT) AS year_str FROM StormEvents LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT EpisodeId FROM (SELECT * FROM StormEvents LIMIT 1)";
        var expected = Convert.ToDouble(cmd.ExecuteScalar()!);
        cmd.CommandText = sql;
        var result = Convert.ToDouble(cmd.ExecuteScalar()!);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Converts_ToBool()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| take 1
| project tobool('1')";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT TRY_CAST('1' AS BOOLEAN) AS Column1 FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (bool)cmd.ExecuteScalar()!;
        Assert.True(result);
    }

    [Fact]
    public void Converts_ToDateTime()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| take 1
| project todatetime('1950-01-03 00:00:00')";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COALESCE(TRY_CAST('1950-01-03 00:00:00' AS TIMESTAMP), TRY_STRPTIME(CAST('1950-01-03 00:00:00' AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS Column1 FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (DateTime)cmd.ExecuteScalar()!;
        Assert.Equal(new DateTime(1950, 1, 3, 0, 0, 0), result);
    }

    [Theory]
    // KQL todatetime() is lenient: non-ISO formats DuckDB's bare cast rejects must still parse,
    // instead of becoming NULL (which silently collapses `summarize by bin(todatetime(col),1d)`).
    [InlineData("1.1.2007, 00:00:00")]   // German locale (D.M.Y)
    [InlineData("27.1.2007, 14:00:00")]
    [InlineData("1/1/2007 12:00:00 AM")] // US locale
    [InlineData("2007-01-01T00:00:00.0000000Z")] // Kusto ISO export
    [InlineData("2007-01-01 00:00:00")]  // ISO
    public void ToDateTime_Parses_NonIso_Formats(string value)
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert($"StormEvents | take 1 | project d = todatetime('{value}')");

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.IsType<DateTime>(result);            // parsed, not NULL
        Assert.Equal(2007, ((DateTime)result!).Year);
    }

    [Fact]
    public void ToDateTime_Unparseable_Yields_Null()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | take 1 | project d = todatetime('not a date')");

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        Assert.True(cmd.ExecuteScalar() is null or DBNull);
    }

    [Fact]
    public void Converts_ToString()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| take 1
| project tostring(EpisodeId)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT TRY_CAST(EpisodeId AS TEXT) AS EpisodeId FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT CAST(EpisodeId AS VARCHAR) FROM StormEvents LIMIT 1";
        var expected = (string)cmd.ExecuteScalar()!;
        cmd.CommandText = sql;
        var result = (string)cmd.ExecuteScalar()!;
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Project_Toreal_AutoNames_Column()
    {
        // KQL: | project toreal(Value) → auto-names output column "Value" (inner identifier)
        var sql = new KqlToSqlConverter().Convert("T | project toreal(Value)");
        Assert.Equal("SELECT COALESCE(TRY_CAST(Value AS DOUBLE), TRY_CAST(TRY_CAST(Value AS BOOLEAN) AS DOUBLE)) AS Value FROM T", sql);
    }

    [Theory]
    [InlineData("print bool(true)", "TRUE")]
    [InlineData("print bool(false)", "FALSE")]
    public void BoolLiteral_EmitsTrueOrFalse(string kql, string expected)
    {
        var sql = new KqlToSqlConverter().Convert(kql);
        Assert.Contains(expected, sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("print bool(0)", "FALSE")]
    [InlineData("print bool(1)", "TRUE")]
    public void BoolNumericLiteral_EmitsTrueOrFalse(string kql, string expected)
    {
        var sql = new KqlToSqlConverter().Convert(kql);
        Assert.Contains(expected, sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("bool(", sql, StringComparison.OrdinalIgnoreCase);
    }
}
