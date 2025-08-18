using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class PrintOperatorTests
{
    [Fact]
    public void Converts_Print()
    {
        var converter = new KqlToSqlConverter();
        var kql = "print x=1+1, y=2";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT 1 + 1 AS x, 2 AS y", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal(2L, reader.GetInt64(1));
        Assert.False(reader.Read());
    }
}
