using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class SummarizeOperatorTests
{
    [Fact]
    public void Converts_Summarize_And_Sort_With_StormEvents()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize cnt=count() by State | sort by cnt desc";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT State, COUNT(*) AS cnt FROM StormEvents GROUP BY ALL) ORDER BY cnt DESC", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
        Assert.True(reader.GetInt64(1) > 0);
    }

    [Fact]
    public void Summarize_MultipleAggregates()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize MaxInj=max(InjuriesDirect), MinInj=min(InjuriesDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT MAX(InjuriesDirect) AS MaxInj, MIN(InjuriesDirect) AS MinInj FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(0) > 0);
        Assert.True(reader.GetInt64(1) >= 0);
    }
}
