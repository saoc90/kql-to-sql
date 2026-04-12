using System;
using System.Collections.Generic;
using DuckDB.NET.Data;
using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.DuckDbExtension.Tests;

/// <summary>
/// Integration tests for newly added functions: extract_all, replace_regex,
/// tohex, datetime_local_to_utc, and combined binary operations.
/// </summary>
public class NewFunctionIntegrationTests
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    // ── extract_all ──────────────────────────────────────────────────────

    [Fact]
    public void ExtractAll_ReturnsAllMatches()
    {
        var kql = "print result = extract_all('a]b]c', '[a-c]')";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // Should return a list containing 'a', 'b', 'c'
        var value = reader.GetValue(0);
        Assert.NotNull(value);
    }

    // ── replace_regex ────────────────────────────────────────────────────

    [Fact]
    public void ReplaceRegex_ReplacesPattern()
    {
        var kql = "print result = replace_regex('hello-world', '-', '_')";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("hello_world", reader.GetString(0));
    }

    // ── tohex ────────────────────────────────────────────────────────────

    [Fact]
    public void ToHex_ConvertsIntegerToHex()
    {
        var kql = "print result = tohex(255)";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("ff", reader.GetString(0));
    }

    // ── datetime_local_to_utc ────────────────────────────────────────────

    [Fact]
    public void DatetimeLocalToUtc_ConvertsTimezone()
    {
        var kql = "print result = datetime_local_to_utc(datetime(2024-01-15 12:00:00), 'US/Eastern')";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var dt = reader.GetDateTime(0);
        // US/Eastern is UTC-5 in January, so 12:00 local -> 17:00 UTC
        Assert.Equal(17, dt.Hour);
    }

    // ── Combined binary operations ───────────────────────────────────────

    [Fact]
    public void BinaryOperations_Combined()
    {
        var kql = "print result = binary_and(binary_or(3, 5), 6)";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // binary_or(3, 5) = 0011 | 0101 = 0111 = 7
        // binary_and(7, 6) = 0111 & 0110 = 0110 = 6
        Assert.Equal(6L, reader.GetInt64(0));
    }

    // ── StormEvents: extract_all ─────────────────────────────────────────

    [Fact]
    public void ExtractAll_WithStormEvents()
    {
        var kql = "StormEvents | extend EventPrefix = extract_all(EventType, '[A-Z][a-z]+') | take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            var prefixOrdinal = reader.GetOrdinal("EventPrefix");
            Assert.NotNull(reader.GetValue(prefixOrdinal));
        }
        Assert.Equal(3, rows);
    }

    // ── StormEvents: replace_regex ───────────────────────────────────────

    [Fact]
    public void ReplaceRegex_WithStormEvents()
    {
        var kql = "StormEvents | extend UpperEvent = replace_regex(EventType, ' ', '_') | take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            var val = reader.GetString(reader.GetOrdinal("UpperEvent"));
            Assert.DoesNotContain(" ", val);
        }
        Assert.Equal(3, rows);
    }
}
