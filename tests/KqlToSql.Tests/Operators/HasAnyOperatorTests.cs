using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class HasAnyOperatorTests
{
    [Fact]
    public void Converts_HasAny()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State has_any ('NEW', 'TEXAS')
| project State, event_count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, event_count FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY ALL) WHERE State ILIKE '%NEW%' OR State ILIKE '%TEXAS%'", sql);

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
        Assert.True(results.Count > 0);
    }

    // Additional case-sensitive or negated forms can be added once supported by the parser
}
