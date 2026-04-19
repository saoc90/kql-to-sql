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
        Assert.Equal("SELECT t.* EXCLUDE (arr), u.value AS arr FROM (SELECT *, LIST_VALUE(1, 2, 3) AS arr FROM (SELECT generate_series AS x FROM generate_series(CAST(1 AS BIGINT), CAST(1 AS BIGINT), CAST(1 AS BIGINT)))) AS t CROSS JOIN UNNEST(t.arr) AS u(value)", sql);

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

    [Fact]
    public void MvExpand_ParseJsonTostring_ExcludesInnerIdentifier()
    {
        // Kusto auto-names mv-expand parse_json(tostring(Value)) output as 'Value'
        // (innermost identifier). The EXCLUDE clause must use 'Value', not the raw expression.
        var converter = new KqlToSqlConverter();
        var kql = "datatable(Value:string) [ \"[1,2,3]\" ] | mv-expand parse_json(tostring(Value))";
        var sql = converter.Convert(kql);
        Assert.Contains("EXCLUDE (Value)", sql);
        Assert.DoesNotContain("EXCLUDE (parse_json", sql);
        Assert.Contains("u.value AS Value", sql);
    }

    [Fact]
    public void MvExpand_ParseJsonOfStringColumn_OutputsInnerIdentifier()
    {
        // mv-expand parse_json(X) where X is a string column: Kusto names the output 'X'
        // (innermost identifier). Verifies the EXCLUDE/output-alias wiring, not execution —
        // our parse_json translates to CAST AS JSON which isn't directly UNNEST-able.
        var converter = new KqlToSqlConverter();
        var kql = "datatable(X:string) [ \"[1,2,3]\" ] | mv-expand parse_json(X)";
        var sql = converter.Convert(kql);
        Assert.Contains("EXCLUDE (X)", sql);
        Assert.Contains("u.value AS X", sql);
    }
}
