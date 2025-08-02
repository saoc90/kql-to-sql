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
        Assert.Equal("SELECT * FROM (SELECT * FROM (SELECT * FROM X) QUALIFY ROW_NUMBER() OVER (PARTITION BY Key) = 1) AS L INNER JOIN (SELECT * FROM Y) AS R ON L.Key = R.Key", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string Key, long Value1, string Key1, long Value2)>();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetInt64(1), reader.GetString(2), reader.GetInt64(3)));
        }
        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 2 && r.Key1 == "b" && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 20);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 30);
    }

    [Fact]
    public void Converts_LeftOuterJoin()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join kind=leftouter Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT * FROM X) AS L LEFT OUTER JOIN (SELECT * FROM Y) AS R ON L.Key = R.Key", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string? Key, long? Value1, string? Key1, long? Value2)>();
        while (reader.Read())
        {
            results.Add((
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3)
            ));
        }
        Assert.Equal(5, results.Count);
        Assert.Contains(results, r => r.Key == "a" && r.Value1 == 1 && r.Key1 == null && r.Value2 == null);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 2 && r.Key1 == "b" && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 3 && r.Key1 == "b" && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 20);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 30);
    }

    [Fact]
    public void Converts_RightOuterJoin()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join kind=rightouter Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT * FROM X) AS L RIGHT OUTER JOIN (SELECT * FROM Y) AS R ON L.Key = R.Key", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string? Key, long? Value1, string Key1, long Value2)>();
        while (reader.Read())
        {
            results.Add((
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1),
                reader.GetString(2),
                reader.GetInt64(3)
            ));
        }
        Assert.Equal(5, results.Count);
        Assert.Contains(results, r => r.Key1 == "b" && r.Value2 == 10 && r.Key == "b" && r.Value1 == 2);
        Assert.Contains(results, r => r.Key1 == "b" && r.Value2 == 10 && r.Key == "b" && r.Value1 == 3);
        Assert.Contains(results, r => r.Key1 == "c" && r.Value2 == 20 && r.Key == "c" && r.Value1 == 4);
        Assert.Contains(results, r => r.Key1 == "c" && r.Value2 == 30 && r.Key == "c" && r.Value1 == 4);
        Assert.Contains(results, r => r.Key1 == "d" && r.Value2 == 40 && r.Key == null && r.Value1 == null);
    }

    [Fact]
    public void Converts_FullOuterJoin()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join kind=fullouter Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT * FROM X) AS L FULL OUTER JOIN (SELECT * FROM Y) AS R ON L.Key = R.Key", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string? Key, long? Value1, string? Key1, long? Value2)>();
        while (reader.Read())
        {
            results.Add((
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3)
            ));
        }
        Assert.Equal(6, results.Count);
        Assert.Contains(results, r => r.Key == "a" && r.Value1 == 1 && r.Key1 == null && r.Value2 == null);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 2 && r.Key1 == "b" && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 3 && r.Key1 == "b" && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 20);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 30);
        Assert.Contains(results, r => r.Key == null && r.Value1 == null && r.Key1 == "d" && r.Value2 == 40);
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
