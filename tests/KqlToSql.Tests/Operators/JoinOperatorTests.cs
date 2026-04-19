using System.Collections.Generic;
using DuckDB.NET.Data;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class JoinOperatorTests
{
    [Fact]
    public void Converts_Join_Default()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* EXCLUDE (\"Key\") FROM (SELECT * FROM X QUALIFY ROW_NUMBER() OVER (PARTITION BY \"Key\") = 1) AS L INNER JOIN Y AS R ON L.\"Key\" = R.\"Key\"", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string Key, long Value1, long Value2)>();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)));
        }
        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 2 && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Value2 == 20);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Value2 == 30);
    }

    [Fact]
    public void Converts_LeftOuterJoin()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join kind=leftouter Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* EXCLUDE (\"Key\") FROM X AS L LEFT OUTER JOIN Y AS R ON L.\"Key\" = R.\"Key\"", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string? Key, long? Value1, long? Value2)>();
        while (reader.Read())
        {
            results.Add((
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1),
                reader.IsDBNull(2) ? (long?)null : reader.GetInt64(2)
            ));
        }
        Assert.Equal(5, results.Count);
        Assert.Contains(results, r => r.Key == "a" && r.Value1 == 1 && r.Value2 == null);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 2 && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 3 && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Value2 == 20);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Value2 == 30);
    }

    [Fact]
    public void Converts_RightOuterJoin()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join kind=rightouter Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* EXCLUDE (\"Key\") FROM X AS L RIGHT OUTER JOIN Y AS R ON L.\"Key\" = R.\"Key\"", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string? Key, long? Value1, long Value2)>();
        while (reader.Read())
        {
            results.Add((
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1),
                reader.GetInt64(2)
            ));
        }
        Assert.Equal(5, results.Count);
        Assert.Contains(results, r => r.Value2 == 10 && r.Key == "b" && r.Value1 == 2);
        Assert.Contains(results, r => r.Value2 == 10 && r.Key == "b" && r.Value1 == 3);
        Assert.Contains(results, r => r.Value2 == 20 && r.Key == "c" && r.Value1 == 4);
        Assert.Contains(results, r => r.Value2 == 30 && r.Key == "c" && r.Value1 == 4);
        Assert.Contains(results, r => r.Value2 == 40 && r.Key == null && r.Value1 == null);
    }

    [Fact]
    public void Converts_FullOuterJoin()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join kind=fullouter Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* EXCLUDE (\"Key\") FROM X AS L FULL OUTER JOIN Y AS R ON L.\"Key\" = R.\"Key\"", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string? Key, long? Value1, long? Value2)>();
        while (reader.Read())
        {
            results.Add((
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1),
                reader.IsDBNull(2) ? (long?)null : reader.GetInt64(2)
            ));
        }
        Assert.Equal(6, results.Count);
        Assert.Contains(results, r => r.Key == "a" && r.Value1 == 1 && r.Value2 == null);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 2 && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 3 && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Value2 == 20);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Value2 == 30);
        Assert.Contains(results, r => r.Key == null && r.Value1 == null && r.Value2 == 40);
    }

    [Fact]
    public void Converts_Join_DuplicateCols_Get_Suffixed_With_1()
    {
        // KQL: any R column that also exists on L keeps L's name and is re-emitted as <name>1.
        // Uses CTEs so the converter can enumerate both sides' output columns via AST.
        DuckDbSetup.EnsureDuckDb();
        var converter = new KqlToSqlConverter();
        var kql = "let A = X | project Key, Value; let B = Y | project Key, Value; A | join B on Key";
        var sql = converter.Convert(kql);

        // Expect R.Value aliased to Value1 (because L also has Value).
        Assert.Contains("R.Value AS Value1", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE X (Key VARCHAR, Value BIGINT); INSERT INTO X VALUES ('b', 2), ('c', 4);";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE TABLE Y (Key VARCHAR, Value BIGINT); INSERT INTO Y VALUES ('b', 10), ('c', 20);";
            cmd.ExecuteNonQuery();
        }
        using var runCmd = conn.CreateCommand();
        runCmd.CommandText = sql;
        using var reader = runCmd.ExecuteReader();
        var rows = new List<(string Key, long Value, long Value1)>();
        while (reader.Read())
        {
            rows.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)));
        }
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r.Key == "b" && r.Value == 2 && r.Value1 == 10);
        Assert.Contains(rows, r => r.Key == "c" && r.Value == 4 && r.Value1 == 20);
    }

    [Fact]
    public void Converts_Join_NonColliding_Cols_Not_Suffixed()
    {
        // If the R column name doesn't exist on L, no suffix is applied.
        var converter = new KqlToSqlConverter();
        var kql = "let A = X | project Key, LeftOnly; let B = Y | project Key, RightOnly; A | join B on Key";
        var sql = converter.Convert(kql);
        Assert.Contains("R.RightOnly", sql);
        Assert.DoesNotContain("AS RightOnly1", sql);
    }

    private static void CreateJoinTables(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE X (Key VARCHAR, Value1 BIGINT);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO X VALUES ('a',1),('b',2),('b',3),('c',4);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE TABLE Y (Key VARCHAR, Value2 BIGINT);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Y VALUES ('b',10),('c',20),('c',30),('d',40);";
        cmd.ExecuteNonQuery();
    }
}
