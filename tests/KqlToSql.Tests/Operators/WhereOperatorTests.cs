using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class WhereOperatorTests
{
    [Fact]
    public void Converts_Where_And_Project_With_StormEvents()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where STATE == 'TEXAS' | project EVENT_TYPE";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT EVENT_TYPE FROM StormEvents WHERE STATE = 'TEXAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Tornado", reader.GetString(0));
    }

    [Fact]
    public void Converts_Multiple_Where_Conditions()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where STATE == 'KANSAS' and INJURIES_DIRECT > 0 | project EVENT_TYPE";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT EVENT_TYPE FROM StormEvents WHERE STATE = 'KANSAS' AND INJURIES_DIRECT > 0", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Tornado", reader.GetString(0));
    }

    [Fact]
    public void Converts_NotEqual_Condition()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where STATE != 'TEXAS' | project STATE";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STATE FROM StormEvents WHERE STATE <> 'TEXAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.NotEqual("TEXAS", reader.GetString(0));
    }

    [Fact]
    public void Converts_CaseInsensitive_Equal()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where STATE =~ 'texas' | project STATE";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STATE FROM StormEvents WHERE UPPER(STATE) = UPPER('texas')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("TEXAS", reader.GetString(0));
    }

    [Fact]
    public void Converts_CaseInsensitive_NotEqual()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where STATE !~ 'texas' | project STATE";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STATE FROM StormEvents WHERE UPPER(STATE) <> UPPER('texas')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.NotEqual("TEXAS", reader.GetString(0));
    }

    [Fact]
    public void Converts_GreaterThanOrEqual_Condition()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where INJURIES_DIRECT >= 1 | project INJURIES_DIRECT";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT INJURIES_DIRECT FROM StormEvents WHERE INJURIES_DIRECT >= 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt32(0) >= 1);
    }

    [Fact]
    public void Converts_LessThanOrEqual_Condition()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where INJURIES_DIRECT <= 0 | project INJURIES_DIRECT";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT INJURIES_DIRECT FROM StormEvents WHERE INJURIES_DIRECT <= 0", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt32(0) <= 0);
    }

    [Fact]
    public void Converts_LessThan_Condition()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where INJURIES_DIRECT < 1 | project INJURIES_DIRECT";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT INJURIES_DIRECT FROM StormEvents WHERE INJURIES_DIRECT < 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt32(0) < 1);
    }
}
