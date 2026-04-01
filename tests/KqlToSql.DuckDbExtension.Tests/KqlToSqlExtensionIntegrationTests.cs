using System;
using System.Collections.Generic;
using DuckDB.NET.Data;
using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.DuckDbExtension.Tests;

/// <summary>
/// Integration tests that verify the KQL-to-SQL conversion pipeline works
/// end-to-end with real DuckDB execution against the StormEvents dataset.
/// These tests simulate the behavior of the DuckDB extension's kql_to_sql function.
/// </summary>
public class KqlToSqlExtensionIntegrationTests
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    [Fact]
    public void KqlToSql_SimpleCount_ReturnsCorrectResult()
    {
        var kql = "StormEvents | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();

        Assert.IsType<long>(result);
        Assert.True((long)result! > 0);
    }

    [Fact]
    public void KqlToSql_WhereFilter_ExecutesSuccessfully()
    {
        var kql = "StormEvents | where State == 'TEXAS' | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = (long)cmd.ExecuteScalar()!;

        Assert.True(result > 0);
    }

    [Fact]
    public void KqlToSql_ProjectColumns_ReturnsSelectedColumns()
    {
        var kql = "StormEvents | project State, EventType | take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        Assert.Equal(2, reader.FieldCount);

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.False(reader.IsDBNull(0));
        }
        Assert.Equal(5, rows);
    }

    [Fact]
    public void KqlToSql_SortByAndTake_ReturnsOrderedResults()
    {
        var kql = "StormEvents | where State == 'TEXAS' | sort by DamageProperty desc | take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        var rows = 0;
        while (reader.Read())
        {
            rows++;
        }
        Assert.Equal(3, rows);
    }

    [Fact]
    public void KqlToSql_Summarize_ReturnsAggregatedResults()
    {
        var kql = "StormEvents | summarize EventCount = count() by State | sort by EventCount desc | take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        var states = new List<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(reader.GetOrdinal("State")));
        }

        Assert.Equal(5, states.Count);
        Assert.All(states, s => Assert.False(string.IsNullOrWhiteSpace(s)));
    }

    [Fact]
    public void KqlToSql_Extend_AddsComputedColumn()
    {
        var kql = "StormEvents | where State == 'TEXAS' | extend UpperState = toupper(State) | take 1";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var upperStateOrdinal = reader.GetOrdinal("UpperState");
        Assert.Equal("TEXAS", reader.GetString(upperStateOrdinal));
    }

    [Fact]
    public void KqlToSql_Distinct_ReturnsUniqueValues()
    {
        var kql = "StormEvents | distinct State | sort by State asc | take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        var states = new List<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(0));
        }

        Assert.Equal(5, states.Count);
        var distinctStates = new HashSet<string>(states);
        Assert.Equal(states.Count, distinctStates.Count);
    }

    [Fact]
    public void KqlToSql_Top_ReturnsTopResults()
    {
        var kql = "StormEvents | top 3 by DamageProperty desc";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        var rows = 0;
        while (reader.Read())
        {
            rows++;
        }
        Assert.Equal(3, rows);
    }

    [Fact]
    public void KqlToSql_Contains_FiltersCorrectly()
    {
        var kql = "StormEvents | where State contains 'TEX' | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var result = (long)cmd.ExecuteScalar()!;
        Assert.True(result > 0);
    }

    [Fact]
    public void KqlToSql_ConverterReturnsValidSql_ForVariousOperators()
    {
        var testCases = new[]
        {
            "StormEvents | take 1",
            "StormEvents | where State == 'TEXAS'",
            "StormEvents | project State, EventType",
            "StormEvents | count",
            "StormEvents | distinct State",
            "StormEvents | summarize count() by State",
            "StormEvents | sort by State asc",
        };

        using var conn = StormEventsDatabase.GetConnection();

        foreach (var kql in testCases)
        {
            var sql = _converter.Convert(kql);

            Assert.False(string.IsNullOrWhiteSpace(sql), $"Conversion of '{kql}' returned empty SQL");

            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;

            using var reader = cmd.ExecuteReader();
            Assert.True(reader.FieldCount > 0, $"SQL '{sql}' from KQL '{kql}' returned no columns");
        }
    }

    [Fact]
    public void KqlToSql_DialectSelection_ProducesValidSql()
    {
        var kql = "StormEvents | take 5";

        var duckDbDialect = new DuckDbDialect();
        var duckDbConverter = new KqlToSqlConverter(duckDbDialect);
        var duckDbSql = duckDbConverter.Convert(kql);

        Assert.False(string.IsNullOrWhiteSpace(duckDbSql));
        Assert.Contains("LIMIT 5", duckDbSql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = duckDbSql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    [Fact]
    public void KqlToSql_ComplexPipeline_ExecutesSuccessfully()
    {
        var kql = "StormEvents | where State == 'TEXAS' | project State, EventType, DamageProperty | sort by DamageProperty desc | take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        Assert.Equal(3, reader.FieldCount);

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.Equal("TEXAS", reader.GetString(reader.GetOrdinal("State")));
        }
        Assert.Equal(5, rows);
    }

    [Fact]
    public void KqlToSql_LetStatements_WorkWithCtes()
    {
        var kql = @"
let texasEvents = StormEvents | where State == 'TEXAS';
texasEvents | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var result = (long)cmd.ExecuteScalar()!;
        Assert.True(result > 0);
    }

    [Fact]
    public void KqlToSql_SummarizeWithMultipleAggregates_ExecutesSuccessfully()
    {
        var kql = "StormEvents | where State == 'TEXAS' | summarize EventCount = count() by EventType | sort by EventCount desc | take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.True(reader.GetInt64(reader.GetOrdinal("EventCount")) > 0);
        }
        Assert.True(rows > 0 && rows <= 3);
    }

    [Fact]
    public void KqlToSql_DataTableExpression_CreatesInlineData()
    {
        var kql = "datatable(Name: string, Age: int)['Alice', 30, 'Bob', 25]";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        var names = new List<string>();
        while (reader.Read())
        {
            names.Add(reader.GetString(reader.GetOrdinal("Name")));
        }

        Assert.Equal(2, names.Count);
        Assert.Contains("Alice", names);
        Assert.Contains("Bob", names);
    }

    [Fact]
    public void KqlToSql_PrintOperator_ReturnsValues()
    {
        var kql = "print x = 1, y = 'hello'";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("x")));
        Assert.Equal("hello", reader.GetString(reader.GetOrdinal("y")));
    }

    [Fact]
    public void KqlToSql_UnionOperator_CombinesResults()
    {
        var kql = @"
let t1 = StormEvents | where State == 'TEXAS' | take 2;
let t2 = StormEvents | where State == 'FLORIDA' | take 2;
union t1, t2 | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var result = (long)cmd.ExecuteScalar()!;
        Assert.Equal(4L, result);
    }
}
