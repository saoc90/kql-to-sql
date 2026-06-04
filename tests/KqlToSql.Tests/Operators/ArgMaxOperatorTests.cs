using System;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ArgMaxOperatorTests
{
    [Fact]
    public void ArgMax_With_Duplicate_Keys_Returns_A_Tied_Row_Not_A_Phantom()
    {
        // Two rows share the IDENTICAL max key (Ts). KQL arg_max(Ts, *) returns one of them; the tie-break is
        // arbitrary per the KQL spec (Kusto uses internal storage order, which is not portable to SQL — see
        // "Known semantic differences" in KqlOperatorsChecklist.md). The translation must return a row whose
        // key is the max AND whose value is one of the genuine tied candidates — never an interpolated/phantom value.
        var converter = new KqlToSqlConverter();
        var kql = "datatable(Name:string, Ts:datetime, Value:long)" +
                  "['s', datetime(2026-01-01 00:00:01), 10, 's', datetime(2026-01-01 00:00:02), 20, 's', datetime(2026-01-01 00:00:02), 30]" +
                  " | summarize arg_max(Ts, *) by Name | project Name, Ts, Value";
        var sql = converter.Convert(kql);
        Assert.Contains("ROW_NUMBER() OVER (PARTITION BY Name ORDER BY Ts DESC)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(new DateTime(2026, 1, 1, 0, 0, 2), reader.GetDateTime(1)); // key is the max timestamp
        var val = Convert.ToInt64(reader.GetValue(2));
        Assert.True(val == 20 || val == 30, $"expected a tied candidate (20 or 30), got {val}"); // a real reading, not a phantom
    }

    [Fact]
    public void Converts_ArgMax_By_State()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize arg_max(EndTime, EventType) by State | sort by State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT State, MAX(EndTime) AS EndTime, ARG_MAX(EventType, EndTime) AS EventType FROM StormEvents GROUP BY ALL) ORDER BY State DESC NULLS LAST", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
        Assert.False(string.IsNullOrWhiteSpace(reader.GetValue(1).ToString()));
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(2)));
    }

    [Fact]
    public void Converts_ArgMax_With_Aliased_Argument()
    {
        // KQL: arg_max(key, alias = value) renames the value output to alias (must emit AS inside function — that's invalid SQL).
        // Must emit MAX(key) AS key, ARG_MAX(value, key) AS alias — NOT arg_max(value AS alias, key).
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize arg_max(EndTime, TopEvent = EventType) by State";
        var sql = converter.Convert(kql);
        Assert.Contains("MAX(EndTime) AS EndTime", sql);
        Assert.Contains("ARG_MAX(EventType, EndTime) AS TopEvent", sql);
        Assert.DoesNotContain("EventType AS TopEvent", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }

    [Fact]
    public void Converts_ArgMax_With_Outer_Alias_On_Key()
    {
        // KQL: MaxT = arg_max(Timestamp, Value) — outer alias MaxT renames the key output.
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize MaxT = arg_max(EndTime, EventType) by State";
        var sql = converter.Convert(kql);
        Assert.Contains("MAX(EndTime) AS MaxT", sql);
        Assert.Contains("ARG_MAX(EventType, EndTime) AS EventType", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }

    [Fact]
    public void Converts_ArgMax_With_Wildcard()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize arg_max(EndTime, *) by State | project State, EventType, EndTime | where State == 'ALABAMA'";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT State, EventType, EndTime FROM StormEvents QUALIFY ROW_NUMBER() OVER (PARTITION BY State ORDER BY EndTime DESC) = 1) WHERE State = 'ALABAMA'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(1)));
        // EndTime is a real TIMESTAMP in the dataset, not a string — assert it's present.
        Assert.False(reader.IsDBNull(2));
    }

    [Fact]
    public void ArgMax_Wildcard_After_Union_Wraps_Qualify_In_Subquery()
    {
        // QUALIFY cannot attach to a set-op result — must be wrapped in SELECT * FROM (...)
        var converter = new KqlToSqlConverter();
        var kql = "(StormEvents | where State == 'ALABAMA' | take 5) | union (StormEvents | where State == 'TEXAS' | take 5) | summarize arg_max(EndTime, *) by State";
        var sql = converter.Convert(kql);
        // Correctly wrapped: SELECT * FROM (...UNION ALL...) QUALIFY ROW_NUMBER() ...
        Assert.Contains("SELECT * FROM (", sql);
        Assert.Contains(") QUALIFY ROW_NUMBER() OVER", sql);
        // Must NOT end with QUALIFY directly on the UNION result (i.e. no bare UNION … QUALIFY at top level)
        Assert.DoesNotContain("UNION ALL BY NAME (SELECT * FROM StormEvents WHERE State = 'TEXAS' LIMIT 5) QUALIFY", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }
}
