using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class DistinctOperatorTests
{
    [Fact]
    public void Converts_Distinct()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | distinct State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT DISTINCT State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var count = 0;
        while (reader.Read())
        {
            count++;
        }
        Assert.True(count > 0);
    }

    [Fact]
    public void Distinct_MultipleColumns()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | distinct State, EventType";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT DISTINCT State, EventType FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM ({sql})";
        var result = cmd.ExecuteScalar();
        Assert.True((long)result! > 0);
    }
}
