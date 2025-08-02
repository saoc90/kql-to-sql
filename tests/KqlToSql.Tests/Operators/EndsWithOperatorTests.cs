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
        Assert.Equal("SELECT State, Events FROM (SELECT State, COUNT(*) AS Events FROM StormEvents GROUP BY State) WHERE State ILIKE '%sas' AND Events > 10", sql);

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
        var expectedPair = new List<(string, long)> { ("ARKANSAS", 13), ("KANSAS", 33) };
        expectedPair.Sort();
        Assert.Equal(expectedPair, results);
    }

    [Fact]
    public void Converts_EndsWith_CaseSensitive()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize Events = count() by State
| where State endswith_cs ""NA""";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT State, COUNT(*) AS Events FROM StormEvents GROUP BY State) WHERE State LIKE '%NA'", sql);

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
        var expected = new List<(string, long)>
        {
            ("INDIANA", 3),
            ("NORTH CAROLINA", 9),
            ("LOUISIANA", 28),
            ("SOUTH CAROLINA", 1)
        };
        expected.Sort();
        Assert.Equal(expected, results);
    }
}
