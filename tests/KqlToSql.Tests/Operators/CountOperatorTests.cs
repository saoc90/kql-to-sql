using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class CountOperatorTests
{
    [Fact]
    public void Converts_Count()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where STATE == 'TEXAS' | count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS Count FROM StormEvents WHERE STATE = 'TEXAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(20L, (long)result!);
    }

    [Fact]
    public void Converts_Count_NoRows()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where STATE == 'NOTASTATE' | count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS Count FROM StormEvents WHERE STATE = 'NOTASTATE'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(0L, (long)result!);
    }
}
