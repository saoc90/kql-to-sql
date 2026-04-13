using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class BetweenOperatorTests
{
    [Fact]
    public void Converts_Between_Operator()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where InjuriesDirect between (5 .. 50) | summarize cnt=count()";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS cnt FROM StormEvents WHERE InjuriesDirect BETWEEN 5 AND 50", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (long)cmd.ExecuteScalar();
        Assert.True(result > 0);
    }

    [Fact]
    public void Converts_NotBetween_Operator()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where InjuriesDirect !between (5 .. 50) | summarize cnt=count()";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS cnt FROM StormEvents WHERE NOT (InjuriesDirect BETWEEN 5 AND 50)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (long)cmd.ExecuteScalar();
        Assert.True(result > 0);
    }
}
