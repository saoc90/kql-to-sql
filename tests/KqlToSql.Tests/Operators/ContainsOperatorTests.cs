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
        Assert.Equal("SELECT State, event_count FROM (SELECT State, COUNT(*) AS event_count FROM (SELECT * FROM StormEvents) GROUP BY State) WHERE State ILIKE '%enn%' AND event_count > 1", sql);

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
        Assert.Equal("SELECT COUNT(*) AS Count FROM (SELECT * FROM (SELECT State, COUNT(*) AS event_count FROM (SELECT * FROM StormEvents) GROUP BY State) WHERE State LIKE '%AS%')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(4L, (long)result!);
    }
}
