using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class DistinctOperatorTests
{
    [Fact]
    public void Converts_Distinct()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | distinct STATE";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT DISTINCT STATE FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var count = 0;
        while (reader.Read())
        {
            count++;
        }
        Assert.Equal(30, count);
    }

    [Fact]
    public void Distinct_MultipleColumns()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | distinct STATE, EVENT_TYPE";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT DISTINCT STATE, EVENT_TYPE FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM ({sql})";
        var result = cmd.ExecuteScalar();
        Assert.Equal(30L, (long)result!);
    }
}
