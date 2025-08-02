using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class RangeOperatorTests
{
    [Fact]
    public void Converts_Range()
    {
        var converter = new KqlToSqlConverter();
        var kql = "range Steps from 1 to 8 step 3";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT range AS Steps FROM range(1, 8 + 1, 3)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<long>();
        while (reader.Read())
        {
            results.Add(reader.GetInt64(0));
        }
        Assert.Equal(new List<long> { 1, 4, 7 }, results);
    }
}
