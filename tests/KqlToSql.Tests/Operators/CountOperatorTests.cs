using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class CountOperatorTests
{
    [Fact]
    public void Converts_Count()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State == 'TEXAS' | count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS Count FROM StormEvents WHERE State = 'TEXAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.True((long)result! > 0);
    }

    [Fact]
    public void Converts_Count_NoRows()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State == 'NOTAState' | count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS Count FROM StormEvents WHERE State = 'NOTAState'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(0L, (long)result!);
    }
}
