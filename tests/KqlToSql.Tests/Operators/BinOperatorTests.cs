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
        var kql = "StormEvents | summarize cnt=count() by bin(INJURIES_DIRECT, 10) | sort by INJURIES_DIRECT asc | take 5";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT FLOOR((INJURIES_DIRECT)/(10))*(10) AS INJURIES_DIRECT, COUNT(*) AS cnt FROM StormEvents GROUP BY FLOOR((INJURIES_DIRECT)/(10))*(10) ORDER BY INJURIES_DIRECT ASC LIMIT 5", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(long Bin, long Count)>();
        while (reader.Read())
        {
            results.Add(((long)reader.GetDouble(0), reader.GetInt64(1)));
        }
        Assert.Equal(new List<(long, long)> { (0,205), (10,8), (20,3), (30,5), (40,1) }, results);
    }

    [Fact]
    public void Converts_Bin_With_Smaller_Size()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize cnt=count() by bin(INJURIES_DIRECT, 5) | sort by INJURIES_DIRECT asc | take 5";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT FLOOR((INJURIES_DIRECT)/(5))*(5) AS INJURIES_DIRECT, COUNT(*) AS cnt FROM StormEvents GROUP BY FLOOR((INJURIES_DIRECT)/(5))*(5) ORDER BY INJURIES_DIRECT ASC LIMIT 5", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(long Bin, long Count)>();
        while (reader.Read())
        {
            results.Add(((long)reader.GetDouble(0), reader.GetInt64(1)));
        }
        Assert.Equal(new List<(long, long)> { (0,187), (5,18), (10,6), (15,2), (20,1) }, results);
    }
}
