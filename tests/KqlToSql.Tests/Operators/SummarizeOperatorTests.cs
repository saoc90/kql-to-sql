using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class SummarizeOperatorTests
{
    [Fact]
    public void Converts_Summarize_And_Sort_With_StormEvents()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize cnt=count() by STATE | sort by cnt desc";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STATE, COUNT(*) AS cnt FROM (SELECT * FROM StormEvents) GROUP BY STATE ORDER BY cnt DESC", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("KANSAS", reader.GetString(0));
        Assert.Equal(33L, reader.GetInt64(1));
    }

    [Fact]
    public void Summarize_MultipleAggregates()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize MaxInj=max(INJURIES_DIRECT), MinInj=min(INJURIES_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT MAX(INJURIES_DIRECT) AS MaxInj, MIN(INJURIES_DIRECT) AS MinInj FROM (SELECT * FROM StormEvents)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(101L, reader.GetInt64(0));
        Assert.Equal(0L, reader.GetInt64(1));
    }
}
