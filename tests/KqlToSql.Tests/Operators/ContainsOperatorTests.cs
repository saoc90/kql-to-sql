using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ContainsOperatorTests
{
    [Fact]
    public void Converts_Contains()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State contains ""enn""
| where event_count > 1
| project State, event_count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, event_count FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE State ILIKE '%enn%' AND event_count > 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string State, long Count)>();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetInt64(1)));
        }
        results.Sort();
        Assert.Equal(new List<(string, long)> { ("PENNSYLVANIA", 6), ("TENNESSEE", 3) }, results);
    }

    [Fact]
    public void Converts_Contains_CaseSensitive_Count()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State contains_cs ""AS""
| count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS Count FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE State LIKE '%AS%'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(4L, (long)result!);
    }

    [Fact]
    public void Converts_NotContains()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State !contains ""ALABAMA""
| project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE State NOT ILIKE '%ALABAMA%'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var states = new List<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(0));
        }
        Assert.DoesNotContain("ALABAMA", states);
    }

    [Fact]
    public void Converts_NotContains_CaseSensitive()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State !contains_cs ""AS""
| project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE State NOT LIKE '%AS%'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Assert.DoesNotContain("AS", reader.GetString(0));
        }
    }
}
