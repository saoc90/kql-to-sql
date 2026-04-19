using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ArgMinOperatorTests
{
    [Fact]
    public void Converts_ArgMin_By_State()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize arg_min(EndTime, EventType) by State | sort by State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT State, MIN(EndTime) AS EndTime, ARG_MIN(EventType, EndTime) AS EventType FROM StormEvents GROUP BY ALL) ORDER BY State DESC", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
        Assert.False(string.IsNullOrWhiteSpace(reader.GetValue(1).ToString()));
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(2)));
    }

    [Fact]
    public void Converts_ArgMin_With_Wildcard()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize arg_min(EndTime, *) by State | project State, EventType, EndTime | where State == 'ALABAMA'";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT State, EventType, EndTime FROM StormEvents QUALIFY ROW_NUMBER() OVER (PARTITION BY State ORDER BY EndTime ASC) = 1) WHERE State = 'ALABAMA'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(1)));
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(2)));
    }
}

