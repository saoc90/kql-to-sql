using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class BinOperatorTests
{
    [Fact]
    public void Converts_Bin()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize cnt=count() by bin(InjuriesDirect, 10) | sort by InjuriesDirect asc | take 5";
        var sql = converter.Convert(kql);
        Assert.Contains("FLOOR((InjuriesDirect)/(10))*(10) AS InjuriesDirect", sql);
        Assert.Contains("GROUP BY ALL", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    [Fact]
    public void Converts_Bin_With_Smaller_Size()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize cnt=count() by bin(InjuriesDirect, 5) | sort by InjuriesDirect asc | take 5";
        var sql = converter.Convert(kql);
        Assert.Contains("FLOOR((InjuriesDirect)/(5))*(5) AS InjuriesDirect", sql);
        Assert.Contains("GROUP BY ALL", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }
}
