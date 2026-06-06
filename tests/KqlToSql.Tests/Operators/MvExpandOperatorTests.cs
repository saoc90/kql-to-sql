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
        // mv-expand replaces the source column in place (REPLACE), keeping its original position
        // so downstream column order matches Kusto's (arr stays where extend put it).
        Assert.Equal("SELECT t.* REPLACE (u.value AS arr) FROM (SELECT *, LIST_VALUE(1, 2, 3) AS arr FROM (SELECT generate_series AS x FROM generate_series(CAST(1 AS BIGINT), CAST(1 AS BIGINT), CAST(1 AS BIGINT)))) AS t CROSS JOIN UNNEST(t.arr) AS u(value)", sql);

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
        // (innermost identifier). The in-place REPLACE must use 'Value', not the raw expression.
        var converter = new KqlToSqlConverter();
        var kql = "datatable(Value:string) [ \"[1,2,3]\" ] | mv-expand parse_json(tostring(Value))";
        var sql = converter.Convert(kql);
        Assert.Contains("REPLACE (u.value AS Value)", sql);
        Assert.DoesNotContain("REPLACE (parse_json", sql);
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
        Assert.Contains("REPLACE (u.value AS X)", sql);
        Assert.Contains("u.value AS X", sql);
    }

    [Fact]
    public void MvExpand_JsonObjectColumn_EmitsCaseWhenJsonType()
    {
        // mv-expand on a JSON-cast expression (e.g. output of make_bag / parse_json) must emit a CASE
        // that coerces every dynamic shape to a JSON[] so DuckDB's UNNEST never sees a bare JSON value:
        //   null → one null row, array → elements, object → one single-key bag per property, scalar → one row.
        var converter = new KqlToSqlConverter();
        var kql = "datatable(bag:string) [ '{\"a\":1}' ] | mv-expand bag=parse_json(bag)";
        var sql = converter.Convert(kql);
        Assert.Contains("CASE WHEN", sql);
        Assert.Contains("json_type(", sql);
        Assert.Contains("= 'ARRAY'", sql);
        Assert.Contains("= 'OBJECT'", sql);
        // object → single-key bags {"k":v} (not raw values), matching Kusto's default bag expansion.
        Assert.Contains("list_transform(json_keys(", sql);
        Assert.Contains("json_object(", sql);
    }
}
