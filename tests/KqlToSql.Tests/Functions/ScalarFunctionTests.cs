using System;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Functions;

public class ScalarFunctionTests
{
    [Fact]
    public void String_Functions()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | take 1 | project original=State, lower=tolower(State), upper=toupper(State), len=strlen(State), sub=substring(State,0,3)";
        var sql = converter.Convert(kql);
        Assert.Contains("LOWER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("UPPER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LENGTH", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SUBSTR", sql, StringComparison.OrdinalIgnoreCase);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var original = reader.GetString(0);
        var lower = reader.GetString(1);
        var upper = reader.GetString(2);
        var len = (int)reader.GetInt64(3);
        var sub = reader.GetString(4);
        Assert.Equal(original.ToLowerInvariant(), lower);
        Assert.Equal(original.ToUpperInvariant(), upper);
        Assert.Equal(original.Length, len);
        Assert.Equal(original.Substring(0, 3), sub);
    }

    [Fact]
    public void Now_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project t=now() | take 1";
        var sql = converter.Convert(kql);
        Assert.Contains("NOW()", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (DateTime)cmd.ExecuteScalar();
        Assert.InRange((DateTime.UtcNow - result).TotalMinutes, 0, 5);
    }

    [Fact]
    public void Ago_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project t=ago(1d) | take 1";
        var sql = converter.Convert(kql);
        Assert.Contains("INTERVAL '1 millisecond'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (DateTime)cmd.ExecuteScalar();
        var diff = DateTime.UtcNow - result;
        Assert.InRange(diff.TotalHours, 24 - 0.5, 24 + 0.5);
    }

    [Fact]
    public void Iif_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project r=iif(1==1, 'yes', 'no') | take 1";
        var sql = converter.Convert(kql);
        Assert.Contains("CASE", sql, StringComparison.OrdinalIgnoreCase);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (string)cmd.ExecuteScalar();
        Assert.Equal("yes", result);
    }

    [Fact]
    public void Switch_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project r=switch(2, 1, 'a', 2, 'b', 'c') | take 1";
        var sql = converter.Convert(kql);
        Assert.Contains("CASE", sql, StringComparison.OrdinalIgnoreCase);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (string)cmd.ExecuteScalar();
        Assert.Equal("b", result);
    }
}
