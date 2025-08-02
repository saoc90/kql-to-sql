using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class StartsWithOperatorTests
{
    [Fact]
    public void Converts_StartsWith()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State startswith ""Lo""
| where event_count > 10
| project State, event_count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, event_count FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE State ILIKE 'Lo%' AND event_count > 10", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("LOUISIANA", reader.GetString(0));
        Assert.Equal(28L, reader.GetInt64(1));
        Assert.False(reader.Read());
    }

    [Fact]
    public void Converts_StartsWith_CaseSensitive()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State startswith_cs ""I""
| where event_count > 3
| project State, event_count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, event_count FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE State LIKE 'I%' AND event_count > 3", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var results = new List<(string State, long Count)>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetInt64(1)));
        }
        results.Sort();
        Assert.Equal(new List<(string, long)> { ("ILLINOIS", 11), ("IOWA", 4) }, results);
    }

    [Fact]
    public void Converts_NotStartsWith()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State !startswith ""AL""
| project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE State NOT ILIKE 'AL%'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Assert.False(reader.GetString(0).StartsWith("AL"));
        }
    }

    [Fact]
    public void Converts_NotStartsWith_CaseSensitive()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State !startswith_cs ""al""
| project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE State NOT LIKE 'al%'", sql);

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
