using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class HasAllOperatorTests
{
    [Fact]
    public void Converts_HasAll()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State has_all ('NEW', 'MEXICO')
| project State, event_count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, event_count FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE State ILIKE '%NEW%' AND State ILIKE '%MEXICO%'", sql);

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
}
