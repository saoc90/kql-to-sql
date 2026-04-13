using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class TopOperatorTests
{
    [Fact]
    public void Converts_Top()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | top 1 by InjuriesDirect";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM StormEvents ORDER BY InjuriesDirect DESC LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(reader.GetOrdinal("State"))));
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(reader.GetOrdinal("EventType"))));
        Assert.True(reader.GetInt64(reader.GetOrdinal("InjuriesDirect")) > 0);
    }

    [Fact]
    public void Converts_Top_Ascending()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | top 1 by EventId asc";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM StormEvents ORDER BY EventId ASC LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(reader.GetOrdinal("State"))));
        Assert.True(reader.GetInt64(reader.GetOrdinal("EventId")) > 0);
    }
}
