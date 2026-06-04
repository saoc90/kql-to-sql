using System.Collections.Generic;
using DuckDB.NET.Data;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class LookupOperatorTests
{
    [Fact]
    public void Converts_Lookup_Default_LeftOuter()
    {
        var converter = new KqlToSqlConverter();
        var kql = "Facts | lookup Dims on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* EXCLUDE (\"Key\") FROM Facts AS L LEFT OUTER JOIN Dims AS R ON L.\"Key\" IS NOT DISTINCT FROM R.\"Key\"", sql);
    }

    [Fact]
    public void Converts_Lookup_Inner()
    {
        var converter = new KqlToSqlConverter();
        var kql = "Facts | lookup kind=inner Dims on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* EXCLUDE (\"Key\") FROM Facts AS L INNER JOIN Dims AS R ON L.\"Key\" IS NOT DISTINCT FROM R.\"Key\"", sql);
    }

    [Fact]
    public void Converts_Lookup_MultipleKeys()
    {
        var converter = new KqlToSqlConverter();
        var kql = "Facts | lookup Dims on Key1, $left.Col1 == $right.Col2";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* EXCLUDE (Key1, Col2) FROM Facts AS L LEFT OUTER JOIN Dims AS R ON L.Key1 IS NOT DISTINCT FROM R.Key1 AND L.Col1 IS NOT DISTINCT FROM R.Col2", sql);
    }

    [Fact]
    public void Lookup_LeftOuter_UnmatchedRightStringColumns_PaddedWith_EmptyString()
    {
        // Bug: lookup is a LEFT OUTER JOIN; unmatched right rows pad STRING columns with '' (not
        // NULL), numerics stay NULL — same rule as join, verified against the oracle.
        DuckDbSetup.EnsureDuckDb();
        var converter = new KqlToSqlConverter();
        var kql = "let L=datatable(k:long,lv:string)[1,\"a\",2,\"b\"]; let R=datatable(k:long,rv:string,rn:long)[1,\"x\",100]; L | lookup R on k";
        var sql = converter.Convert(kql);
        Assert.Contains("COALESCE(R.rv, '')", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var rows = new List<(long k, string lv, string rv, long? rn)>();
        while (reader.Read())
            rows.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2),
                reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3)));
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r.k == 1 && r.lv == "a" && r.rv == "x" && r.rn == 100);
        // unmatched: rv padded to '' but the long rn stays NULL
        Assert.Contains(rows, r => r.k == 2 && r.lv == "b" && r.rv == "" && r.rn == null);
    }
}
