using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ArgMaxOperatorTests
{
    [Fact]
    public void Converts_ArgMax_By_State()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize arg_max(EndTime, EventType) by State | sort by State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, arg_max(EventType, EndTime) AS EventType FROM StormEvents GROUP BY ALL ORDER BY State DESC", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(1)));
    }

    [Fact]
    public void Converts_ArgMax_With_Wildcard()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize arg_max(EndTime, *) by State | project State, EventType, EndTime | where State == 'ALABAMA'";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT State, EventType, EndTime FROM StormEvents QUALIFY ROW_NUMBER() OVER (PARTITION BY State ORDER BY EndTime DESC) = 1) WHERE State = 'ALABAMA'", sql);

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
