using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class HasOperatorTests
{
    [Fact]
    public void Converts_Has()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State has ""New""
| where event_count > 1
| project State, event_count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, event_count FROM (SELECT State, COUNT(*) AS event_count FROM (SELECT * FROM StormEvents) GROUP BY State) WHERE State ILIKE '%New%' AND event_count > 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string State, long Count)>();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetInt64(1)));
        }
        Assert.Equal(new List<(string, long)> { ("NEW MEXICO", 2) }, results);
    }

    [Fact]
    public void Converts_Has_CaseSensitive_NoMatch()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State has_cs 'new'";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM StormEvents WHERE State LIKE '%new%'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Null(result);
    }
}
