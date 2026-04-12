using System;
using System.Collections.Generic;
using DuckDB.NET.Data;
using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.DuckDbExtension.Tests;

/// <summary>
/// Integration tests for round 3: make-series, parse-kv, search, externaldata,
/// getschema, bitwise functions, and mv-apply.
/// </summary>
public class Round3IntegrationTests
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    // ── parse-kv ──────────────────────────────────────────────────────────

    [Fact]
    public void ParseKv_ExtractsKeyValuePairs()
    {
        var kql = @"
datatable(Text: string)['name=alice,age=30', 'name=bob,age=25']
| parse-kv Text as (name:string, age:long)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var results = new List<(string Name, long Age)>();
        while (reader.Read())
        {
            results.Add((
                reader.GetString(reader.GetOrdinal("name")),
                reader.GetInt64(reader.GetOrdinal("age"))
            ));
        }
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Name == "alice" && r.Age == 30);
        Assert.Contains(results, r => r.Name == "bob" && r.Age == 25);
    }

    // ── search ────────────────────────────────────────────────────────────

    [Fact]
    public void Search_ColumnEqual()
    {
        var kql = "StormEvents | search State == 'TEXAS' | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    // ── getschema ─────────────────────────────────────────────────────────

    [Fact]
    public void GetSchema_DescribesTable()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE T (Id BIGINT, Name VARCHAR, Score DOUBLE);";
        setup.ExecuteNonQuery();

        var kql = "T | getschema";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var columns = new List<string>();
        while (reader.Read())
        {
            columns.Add(reader.GetString(0)); // column_name
        }
        Assert.Contains("Id", columns);
        Assert.Contains("Name", columns);
        Assert.Contains("Score", columns);
    }

    // ── Bitwise functions ─────────────────────────────────────────────────

    [Fact]
    public void BinaryAnd_Computes()
    {
        var kql = "print result = binary_and(12, 10)";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(8L, reader.GetInt64(0)); // 1100 & 1010 = 1000 = 8
    }

    [Fact]
    public void BinaryOr_Computes()
    {
        var kql = "print result = binary_or(12, 10)";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(14L, reader.GetInt64(0)); // 1100 | 1010 = 1110 = 14
    }

    [Fact]
    public void BinaryXor_Computes()
    {
        var kql = "print result = binary_xor(12, 10)";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(6L, reader.GetInt64(0)); // 1100 ^ 1010 = 0110 = 6
    }

    [Fact]
    public void BinaryShiftLeft_Computes()
    {
        var kql = "print result = binary_shift_left(1, 4)";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(16L, reader.GetInt64(0)); // 1 << 4 = 16
    }

    [Fact]
    public void BinaryShiftRight_Computes()
    {
        var kql = "print result = binary_shift_right(16, 2)";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(4L, reader.GetInt64(0)); // 16 >> 2 = 4
    }

    // ── externaldata ──────────────────────────────────────────────────────

    [Fact]
    public void ExternalData_GeneratesReadCsvAuto()
    {
        var kql = "externaldata(Name:string, Age:int)['https://example.com/data.csv']";
        var sql = _converter.Convert(kql);
        Assert.Contains("read_csv_auto", sql);
        Assert.Contains("https://example.com/data.csv", sql);
    }

    [Fact]
    public void ExternalData_GeneratesReadParquet()
    {
        var kql = "externaldata(Name:string, Age:int)['https://example.com/data.parquet']";
        var sql = _converter.Convert(kql);
        Assert.Contains("read_parquet", sql);
    }

    // ── Complex pipelines with new operators ──────────────────────────────

    [Fact]
    public void ParseKv_ThenSummarize()
    {
        var kql = @"
datatable(Log: string)['action=click,user=alice', 'action=view,user=bob', 'action=click,user=charlie']
| parse-kv Log as (action:string, user:string)
| summarize cnt = count() by action
| sort by cnt desc";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var results = new Dictionary<string, long>();
        while (reader.Read())
        {
            results[reader.GetString(0)] = reader.GetInt64(1);
        }
        Assert.Equal(2L, results["click"]);
        Assert.Equal(1L, results["view"]);
    }

    [Fact]
    public void Search_ThenSummarize()
    {
        var kql = "StormEvents | search EventType == 'Tornado' | summarize cnt = count() by State | top 3 by cnt desc";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.True(reader.GetInt64(reader.GetOrdinal("cnt")) > 0);
        }
        Assert.True(rows > 0 && rows <= 3);
    }

    [Fact]
    public void BitwiseFunctions_InExtend()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE Flags (Id BIGINT, Mask BIGINT); INSERT INTO Flags VALUES (1, 7), (2, 12), (3, 15);";
        setup.ExecuteNonQuery();

        var kql = "Flags | extend HasBit2 = binary_and(Mask, 4) | project Id, Mask, HasBit2";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var results = new List<(long Id, long HasBit2)>();
        while (reader.Read())
        {
            results.Add((reader.GetInt64(0), reader.GetInt64(2)));
        }
        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Id == 1 && r.HasBit2 == 4); // 7 & 4 = 4
        Assert.Contains(results, r => r.Id == 2 && r.HasBit2 == 4); // 12 & 4 = 4
        Assert.Contains(results, r => r.Id == 3 && r.HasBit2 == 4); // 15 & 4 = 4
    }
}
