using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class MvExpandOperatorTests
{
    [Fact]
    public void MvExpand_ExpandsList()
    {
        var converter = new KqlToSqlConverter();
        var kql = "range x from 1 to 1 step 1 | extend arr=pack_array(1,2,3) | mv-expand arr";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT t.* EXCLUDE (arr), u.value AS arr FROM (SELECT *, LIST_VALUE(1, 2, 3) AS arr FROM (SELECT generate_series AS x FROM generate_series(1, 1, 1))) AS t CROSS JOIN UNNEST(t.arr) AS u(value)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<int>();
        while (reader.Read())
        {
            results.Add(reader.GetInt32(1));
        }
        Assert.Equal(new[] {1, 2, 3}, results);
    }
}
