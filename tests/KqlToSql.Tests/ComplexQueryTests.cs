using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests;

public class ComplexQueryTests
{
    [Fact]
    public void Converts_Complex_Pipeline_Without_SelectStar()
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
}
