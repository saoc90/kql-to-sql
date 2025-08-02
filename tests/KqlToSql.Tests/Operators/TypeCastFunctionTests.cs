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
| extend year_str=tostring(Year)
| project toint(year_str)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT CAST(year_str AS INTEGER) FROM (SELECT *, CAST(Year AS TEXT) AS year_str FROM StormEvents LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Year FROM StormEvents LIMIT 1";
        var expected = Convert.ToInt64(cmd.ExecuteScalar()!);
        cmd.CommandText = sql;
        var result = Convert.ToInt64(cmd.ExecuteScalar()!);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Converts_ToLong()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| take 1
| extend year_str=tostring(Year)
| project tolong(year_str)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT CAST(year_str AS BIGINT) FROM (SELECT *, CAST(Year AS TEXT) AS year_str FROM StormEvents LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Year FROM StormEvents LIMIT 1";
        var expected = Convert.ToInt64(cmd.ExecuteScalar()!);
        cmd.CommandText = sql;
        var result = Convert.ToInt64(cmd.ExecuteScalar()!);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Converts_ToDouble()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| take 1
| extend year_str=tostring(Year)
| project todouble(year_str)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT CAST(year_str AS DOUBLE) FROM (SELECT *, CAST(Year AS TEXT) AS year_str FROM StormEvents LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Year FROM StormEvents LIMIT 1";
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
        Assert.Equal("SELECT CAST('1' AS BOOLEAN) FROM StormEvents LIMIT 1", sql);

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
        Assert.Equal("SELECT CAST('1950-01-03 00:00:00' AS TIMESTAMP) FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (DateTime)cmd.ExecuteScalar()!;
        Assert.Equal(new DateTime(1950, 1, 3, 0, 0, 0), result);
    }

    [Fact]
    public void Converts_ToString()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| take 1
| project tostring(Year)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT CAST(Year AS TEXT) FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT CAST(Year AS VARCHAR) FROM StormEvents LIMIT 1";
        var expected = (string)cmd.ExecuteScalar()!;
        cmd.CommandText = sql;
        var result = (string)cmd.ExecuteScalar()!;
        Assert.Equal(expected, result);
    }
}
