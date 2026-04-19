using System;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class DatetimeFunctionTests
{
    [Fact]
    public void Converts_Datetime_Function_To_Timestamp()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | extend d=datetime('2024-09-12 20:23:44') | take 1 | project d";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT d FROM (SELECT *, TIMESTAMP '2024-09-12 20:23:44' AS d FROM StormEvents LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(new DateTime(2024, 9, 12, 20, 23, 44), reader.GetDateTime(0));
    }

    [Fact]
    public void Converts_Year_Only_Datetime_Literal()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend d=datetime(0001) | take 1 | project d");
        Assert.Contains("TIMESTAMP '0001-01-01 00:00:00'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(new DateTime(1, 1, 1), reader.GetDateTime(0));
    }

    [Fact]
    public void Converts_Year_Month_Datetime_Literal()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend d=datetime(2026-04) | take 1 | project d");
        Assert.Contains("TIMESTAMP '2026-04-01 00:00:00'", sql);
    }

    [Fact]
    public void Converts_UnixtimeMs_To_DuckDb_EpochMs()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend d=unixtime_milliseconds_todatetime(1700000000000) | take 1 | project d");
        Assert.Contains("EPOCH_MS(CAST(1700000000000 AS BIGINT))", sql);
        Assert.DoesNotContain("TO_TIMESTAMP_MS", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(new DateTime(2023, 11, 14, 22, 13, 20), reader.GetDateTime(0));
    }

    [Fact]
    public void Converts_UnixtimeUs_And_Ns_To_MakeTimestamp()
    {
        var converter = new KqlToSqlConverter();
        var sqlUs = converter.Convert("StormEvents | extend d=unixtime_microseconds_todatetime(1700000000000000) | take 1 | project d");
        Assert.Contains("MAKE_TIMESTAMP(CAST(1700000000000000 AS BIGINT))", sqlUs);
        Assert.DoesNotContain("TO_TIMESTAMP_US", sqlUs);

        var sqlNs = converter.Convert("StormEvents | extend d=unixtime_nanoseconds_todatetime(1700000000000000000) | take 1 | project d");
        Assert.Contains("MAKE_TIMESTAMP(CAST(1700000000000000000 AS BIGINT) / 1000)", sqlNs);
        Assert.DoesNotContain("TO_TIMESTAMP_NS", sqlNs);
    }

    [Fact]
    public void Bin_Timestamp_Uses_EpochMs()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("datatable(T:datetime)[datetime('2024-09-12 20:23:44')] | extend b = bin(T, 1h) | project b");
        Assert.Contains("EPOCH_MS(CAST(", sql);
        Assert.DoesNotContain("TO_TIMESTAMP_MS", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(new DateTime(2024, 9, 12, 20, 0, 0), reader.GetDateTime(0));
    }
}
