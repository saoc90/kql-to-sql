using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class InOperatorTests
{
    [Fact]
    public void Converts_In()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State in~ ('FLORIDA','georgia','NEW YORK') | count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS Count FROM StormEvents WHERE UPPER(State) IN ('FLORIDA', 'GEORGIA', 'NEW YORK')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(10L, (long)result!);
    }

    [Fact]
    public void Converts_Case_Sensitive_In()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State in ('texas') | count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS Count FROM StormEvents WHERE State IN ('texas')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(0L, (long)result!);
    }
}
