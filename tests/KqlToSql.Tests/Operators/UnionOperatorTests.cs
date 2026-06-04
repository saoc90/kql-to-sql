using System.Collections.Generic;
using DuckDB.NET.Data;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class UnionOperatorTests
{
    [Fact]
    public void Union_CombineTwoQueries()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State == \"ALABAMA\" | take 1 | union (StormEvents | where State == \"TEXAS\" | take 1)";
        var sql = converter.Convert(kql);
        Assert.Equal("(SELECT * FROM StormEvents WHERE State = 'ALABAMA' LIMIT 1) UNION ALL BY NAME (SELECT * FROM StormEvents WHERE State = 'TEXAS' LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var states = new List<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(reader.GetOrdinal("State")));
        }
        Assert.Equal(new[] { "ALABAMA", "TEXAS" }, states);
    }

    [Fact]
    public void Union_WithSource_LabelsRows_PositionallyAs_UnionArgN()
    {
        // Bug: withsource labels each operand positionally (union_arg0, union_arg1, ...) — verified
        // against the oracle for let-bound datatables, inline datatables, and subquery operands.
        DuckDbSetup.EnsureDuckDb();
        var converter = new KqlToSqlConverter();
        var kql = "let A=datatable(x:long,y:string)[1,\"a\",2,\"b\"]; let B=datatable(x:long,y:string)[3,\"c\"]; A | union withsource=Src B";
        var sql = converter.Convert(kql);
        Assert.Contains("'union_arg0' AS Src", sql);
        Assert.Contains("'union_arg1' AS Src", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var rows = new List<(long x, string y, string src)>();
        while (reader.Read())
            rows.Add((
                reader.GetInt64(reader.GetOrdinal("x")),
                reader.GetString(reader.GetOrdinal("y")),
                reader.GetString(reader.GetOrdinal("Src"))));
        Assert.Equal(3, rows.Count);
        Assert.Contains(rows, r => r.x == 1 && r.y == "a" && r.src == "union_arg0");
        Assert.Contains(rows, r => r.x == 2 && r.y == "b" && r.src == "union_arg0");
        Assert.Contains(rows, r => r.x == 3 && r.y == "c" && r.src == "union_arg1");
    }

    [Fact]
    public void Union_DifferentShapes_MissingStringColumns_PaddedWith_EmptyString()
    {
        // Bug: union of differently-shaped tables pads non-overlapping STRING columns with ''
        // (not NULL); numeric/missing columns stay NULL. Verified against the oracle.
        DuckDbSetup.EnsureDuckDb();
        var converter = new KqlToSqlConverter();
        var kql = "let A=datatable(x:long,y:string)[1,\"a\",2,\"b\"]; let B=datatable(x:long,z:string)[3,\"c\"]; A | union B";
        var sql = converter.Convert(kql);
        // Each operand explicitly fills the string column it lacks.
        Assert.Contains("'' AS z", sql);
        Assert.Contains("'' AS y", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        int xi = reader.GetOrdinal("x"), yi = reader.GetOrdinal("y"), zi = reader.GetOrdinal("z");
        var rows = new List<(long x, string y, string z)>();
        while (reader.Read())
            rows.Add((reader.GetInt64(xi), reader.GetString(yi), reader.GetString(zi)));
        Assert.Equal(3, rows.Count);
        Assert.Contains(rows, r => r.x == 1 && r.y == "a" && r.z == "");
        Assert.Contains(rows, r => r.x == 2 && r.y == "b" && r.z == "");
        Assert.Contains(rows, r => r.x == 3 && r.y == "" && r.z == "c");
    }
}
