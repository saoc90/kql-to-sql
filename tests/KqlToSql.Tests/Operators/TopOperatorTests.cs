using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class TopOperatorTests
{
    [Fact]
    public void Converts_Top()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | top 1 by INJURIES_DIRECT";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM StormEvents ORDER BY INJURIES_DIRECT DESC LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("NEBRASKA", reader.GetString(reader.GetOrdinal("STATE")));
        Assert.Equal("Tornado", reader.GetString(reader.GetOrdinal("EVENT_TYPE")));
        Assert.Equal(101L, reader.GetInt64(reader.GetOrdinal("INJURIES_DIRECT")));
    }

    [Fact]
    public void Converts_Top_Ascending()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | top 1 by EVENT_ID asc";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM StormEvents ORDER BY EVENT_ID ASC LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("ALABAMA", reader.GetString(reader.GetOrdinal("STATE")));
        Assert.Equal(9979207L, reader.GetInt64(reader.GetOrdinal("EVENT_ID")));
    }
}
