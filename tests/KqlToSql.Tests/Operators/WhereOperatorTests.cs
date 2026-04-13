using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class WhereOperatorTests
{
    [Fact]
    public void Converts_Where_And_Project_With_StormEvents()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State == 'TEXAS' | project EventType";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT EventType FROM StormEvents WHERE State = 'TEXAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
    }

    [Fact]
    public void Converts_Multiple_Where_Conditions()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State == 'KANSAS' and InjuriesDirect > 0 | project EventType";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT EventType FROM StormEvents WHERE State = 'KANSAS' AND InjuriesDirect > 0", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
    }

    [Fact]
    public void Converts_NotEqual_Condition()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State != 'TEXAS' | project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM StormEvents WHERE State <> 'TEXAS'", sql);

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
        var kql = "StormEvents | where State =~ 'texas' | project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM StormEvents WHERE UPPER(State) = UPPER('texas')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
    }

    [Fact]
    public void Converts_CaseInsensitive_NotEqual()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State !~ 'texas' | project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM StormEvents WHERE UPPER(State) <> UPPER('texas')", sql);

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
        var kql = "StormEvents | where InjuriesDirect >= 1 | project InjuriesDirect";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT InjuriesDirect FROM StormEvents WHERE InjuriesDirect >= 1", sql);

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
        var kql = "StormEvents | where InjuriesDirect <= 0 | project InjuriesDirect";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT InjuriesDirect FROM StormEvents WHERE InjuriesDirect <= 0", sql);

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
        var kql = "StormEvents | where InjuriesDirect < 1 | project InjuriesDirect";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT InjuriesDirect FROM StormEvents WHERE InjuriesDirect < 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt32(0) < 1);
    }
}
