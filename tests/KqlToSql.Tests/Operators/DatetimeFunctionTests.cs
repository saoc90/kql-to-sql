using System;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class DatetimeFunctionTests
{
    [Fact]
    public void Converts_Datetime_Function_To_Timestamp()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | extend d=datetime('2024-09-12 20:23:44') | take 1 | project d";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT d FROM (SELECT *, TIMESTAMP '2024-09-12 20:23:44' AS d FROM StormEvents LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(new DateTime(2024, 9, 12, 20, 23, 44), reader.GetDateTime(0));
    }
}
