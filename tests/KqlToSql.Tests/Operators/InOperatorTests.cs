using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class InOperatorTests
{
    [Fact]
    public void Converts_In()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State in~ ('FLORIDA','georgia','NEW YORK') | count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS Count FROM StormEvents WHERE UPPER(State) IN ('FLORIDA', 'GEORGIA', 'NEW YORK')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(10L, (long)result!);
    }

    [Fact]
    public void Converts_Case_Sensitive_In()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State in ('texas') | count"; 
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS Count FROM StormEvents WHERE State IN ('texas')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(0L, (long)result!);
    }

    [Fact]
    public void Converts_NotIn()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State !in ('TEXAS','KANSAS')
| project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE State NOT IN ('TEXAS', 'KANSAS')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var states = new List<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(0));
        }
        Assert.DoesNotContain("TEXAS", states);
        Assert.DoesNotContain("KANSAS", states);
    }

    [Fact]
    public void Converts_NotIn_CaseInsensitive()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| summarize event_count=count() by State
| where State !in~ ('texas','kansas')
| project State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM (SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY State) WHERE UPPER(State) NOT IN ('TEXAS', 'KANSAS')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var states = new List<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(0));
        }
        Assert.DoesNotContain("TEXAS", states);
        Assert.DoesNotContain("KANSAS", states);
    }
}
