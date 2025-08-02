using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class BetweenOperatorTests
{
    [Fact]
    public void Converts_Between_Operator()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where INJURIES_DIRECT between (5 .. 50) | summarize cnt=count()";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS cnt FROM StormEvents WHERE INJURIES_DIRECT BETWEEN 5 AND 50", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (long)cmd.ExecuteScalar();
        Assert.Equal(35L, result);
    }

    [Fact]
    public void Converts_NotBetween_Operator()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where INJURIES_DIRECT !between (5 .. 50) | summarize cnt=count()";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS cnt FROM StormEvents WHERE NOT (INJURIES_DIRECT BETWEEN 5 AND 50)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (long)cmd.ExecuteScalar();
        Assert.Equal(188L, result);
    }
}
