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
        Assert.Equal("SELECT State, MAX(EndTime) AS EndTime, ARG_MAX(EventType, EndTime) AS EventType FROM StormEvents GROUP BY ALL ORDER BY State DESC", sql);

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
    public void Converts_ArgMax_With_Aliased_Argument()
    {
        // KQL: arg_max(key, alias = value) renames the value output to alias (must emit AS inside function — that's invalid SQL).
        // Must emit MAX(key) AS key, ARG_MAX(value, key) AS alias — NOT arg_max(value AS alias, key).
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize arg_max(EndTime, TopEvent = EventType) by State";
        var sql = converter.Convert(kql);
        Assert.Contains("MAX(EndTime) AS EndTime", sql);
        Assert.Contains("ARG_MAX(EventType, EndTime) AS TopEvent", sql);
        Assert.DoesNotContain("EventType AS TopEvent", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }

    [Fact]
    public void Converts_ArgMax_With_Outer_Alias_On_Key()
    {
        // KQL: MaxT = arg_max(Timestamp, Value) — outer alias MaxT renames the key output.
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize MaxT = arg_max(EndTime, EventType) by State";
        var sql = converter.Convert(kql);
        Assert.Contains("MAX(EndTime) AS MaxT", sql);
        Assert.Contains("ARG_MAX(EventType, EndTime) AS EventType", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
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
