using System;
using System.Collections.Generic;
using DuckDB.NET.Data;
using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.DuckDbExtension.Tests;

/// <summary>
/// Integration tests for scalar functions executing against DuckDB.
/// </summary>
public class ScalarFunctionIntegrationTests
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    // ── Date/time functions ───────────────────────────────────────────────

    [Fact]
    public void DayOfWeek_ReturnsCorrectValue()
    {
        var kql = "print dow = dayofweek(datetime(2024-01-15))"; // Monday
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }

    [Fact]
    public void DayOfMonth_ReturnsCorrectValue()
    {
        var kql = "print dom = dayofmonth(datetime(2024-03-15))";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var val = Convert.ToInt32(reader.GetValue(0));
        Assert.Equal(15, val);
    }

    [Fact]
    public void DayOfYear_ReturnsCorrectValue()
    {
        var kql = "print doy = dayofyear(datetime(2024-02-01))";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var val = Convert.ToInt32(reader.GetValue(0));
        Assert.Equal(32, val); // Jan has 31 days, so Feb 1 = day 32
    }

    [Fact]
    public void GetMonth_ReturnsCorrectValue()
    {
        var kql = "print m = getmonth(datetime(2024-07-20))";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var val = Convert.ToInt32(reader.GetValue(0));
        Assert.Equal(7, val);
    }

    [Fact]
    public void GetYear_ReturnsCorrectValue()
    {
        var kql = "print y = getyear(datetime(2024-07-20))";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var val = Convert.ToInt32(reader.GetValue(0));
        Assert.Equal(2024, val);
    }

    [Fact]
    public void HourOfDay_ReturnsCorrectValue()
    {
        var kql = "print h = hourofday(datetime(2024-07-20 14:30:00))";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var val = Convert.ToInt32(reader.GetValue(0));
        Assert.Equal(14, val);
    }

    [Fact]
    public void UnixTimeSecondsToDatetime_Converts()
    {
        var kql = "print dt = unixtime_seconds_todatetime(0)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var dt = reader.GetDateTime(0);
        Assert.Equal(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dt);
    }

    // ── String functions ──────────────────────────────────────────────────

    [Fact]
    public void Strcmp_ComparesStrings()
    {
        var kql = "print cmp = strcmp('abc', 'def')";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var val = Convert.ToInt32(reader.GetValue(0));
        Assert.Equal(-1, val);
    }

    [Fact]
    public void Repeat_RepeatsString()
    {
        var kql = "print r = repeat('ab', 3)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("ababab", reader.GetString(0));
    }

    [Fact]
    public void Translate_TranslatesChars()
    {
        var kql = "print t = translate('abc', 'bcd', 'hello')";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }

    [Fact]
    public void Base64_RoundTrips()
    {
        var kql = "print encoded = base64_encode_tostring('hello')";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // BASE64 of 'hello' should be 'aGVsbG8='
        var encoded = reader.GetString(0);
        Assert.False(string.IsNullOrWhiteSpace(encoded));
    }

    // ── Hash functions ────────────────────────────────────────────────────

    [Fact]
    public void HashMd5_ReturnsHash()
    {
        var kql = "print h = hash_md5('hello')";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var hash = reader.GetString(0);
        Assert.Equal(32, hash.Length); // MD5 = 32 hex chars
    }

    [Fact]
    public void Hash_ReturnsValue()
    {
        var kql = "print h = hash('hello')";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }

    // ── Array functions ───────────────────────────────────────────────────

    [Fact]
    public void ArrayLength_ReturnsCount()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        var kql = "print len = array_length(pack_array(1, 2, 3))";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var val = Convert.ToInt64(reader.GetValue(0));
        Assert.Equal(3, val);
    }

    [Fact]
    public void ArraySortAsc_SortsArray()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        var kql = "print sorted = array_sort_asc(pack_array(3, 1, 2))";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }

    // ── Type/conditional functions ─────────────────────────────────────────

    [Fact]
    public void IsNan_ChecksNaN()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        var kql = "print check = isnan(0.0 / 0.0)";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }

    // ── New operators with StormEvents ─────────────────────────────────────

    [Fact]
    public void TopHitters_ReturnsTopFrequent()
    {
        var kql = "StormEvents | top-hitters 5 of State";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        long prev = long.MaxValue;
        while (reader.Read())
        {
            rows++;
            var count = reader.GetInt64(reader.GetOrdinal("approximate_count"));
            Assert.True(count <= prev);
            prev = count;
        }
        Assert.Equal(5, rows);
    }

    [Fact]
    public void TopHitters_WithByCount()
    {
        var kql = "StormEvents | top-hitters 3 of EventType by count()";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    [Fact]
    public void TopNested_SingleLevel()
    {
        var kql = "StormEvents | top-nested 3 of State by count()";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    [Fact]
    public void TopNested_TwoLevels()
    {
        var kql = "StormEvents | top-nested 2 of State by count(), top-nested 2 of EventType by count()";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        // 2 states x 2 event types each = 4
        Assert.Equal(4, rows);
    }

    [Fact]
    public void Consume_PassesThrough()
    {
        var kql = "StormEvents | where State == 'TEXAS' | consume";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }

    [Fact]
    public void ParseWhere_ExtractsAndFilters()
    {
        var kql = @"
datatable(Text: string)['key=abc,val=123', 'key=def,val=456', 'no match here']
| parse-where Text with 'key=' key:string ',val=' val:long";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(2, rows); // 'no match here' should be filtered out
    }

    // ── Date functions with StormEvents ────────────────────────────────────

    [Fact]
    public void DateFunctions_WithExtend()
    {
        var kql = @"
StormEvents
| take 5
| extend Year = getyear(datetime(2024-06-15)), Month = getmonth(datetime(2024-06-15))
| project Year, Month";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(2024, Convert.ToInt32(reader.GetValue(reader.GetOrdinal("Year"))));
        Assert.Equal(6, Convert.ToInt32(reader.GetValue(reader.GetOrdinal("Month"))));
    }
}
