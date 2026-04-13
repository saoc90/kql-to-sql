using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class EndsWithOperatorTests
{
    [Fact]
    public void Converts_EndsWith()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize Events=count() by State
| where State endswith ""sas""
| where Events > 10
| project State, Events";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, Events FROM (SELECT State, COUNT(*) AS Events FROM StormEvents GROUP BY ALL) WHERE State ILIKE '%sas' AND Events > 10", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string State, long Count)>();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetInt64(1)));
        }
        Assert.True(results.Count > 0);
    }

    [Fact]
    public void Converts_EndsWith_CaseSensitive()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize Events = count() by State
| where State endswith_cs ""NA""";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT State, COUNT(*) AS Events FROM StormEvents GROUP BY ALL) WHERE State LIKE '%NA'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var results = new List<(string State, long Count)>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetInt64(1)));
        }
        Assert.True(results.Count > 0);
    }

    [Fact]
    public void Converts_NotEndsWith()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State !endswith ""A""
| project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY ALL) WHERE State NOT ILIKE '%A'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Assert.False(reader.GetString(0).EndsWith("A"));
        }
    }

    [Fact]
    public void Converts_NotEndsWith_CaseSensitive()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State !endswith_cs ""a""
| project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY ALL) WHERE State NOT LIKE '%a'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var states = new List<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(0));
        }
        Assert.Contains("ALABAMA", states);
    }
}
