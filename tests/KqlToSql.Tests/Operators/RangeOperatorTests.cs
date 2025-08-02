using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class RangeOperatorTests
{
    [Fact]
    public void Range_GeneratesNumbers()
    {
        var converter = new KqlToSqlConverter();
        var kql = "range x from 1 to 5 step 1";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT generate_series AS x FROM generate_series(1, 5, 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<int>();
        while (reader.Read())
        {
            results.Add(reader.GetInt32(0));
        }
        Assert.Equal(new[] {1, 2, 3, 4, 5}, results);
    }

    [Fact]
    public void Range_In_Pipeline()
    {
        var converter = new KqlToSqlConverter();
        var kql = "range x from 1 to 5 step 1 | where x > 3";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT generate_series AS x FROM generate_series(1, 5, 1)) WHERE x > 3", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<int>();
        while (reader.Read())
        {
            results.Add(reader.GetInt32(0));
        }
        Assert.Equal(new[] {4, 5}, results);
    }
}
