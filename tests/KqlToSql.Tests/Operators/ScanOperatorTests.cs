using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ScanOperatorTests
{
    private readonly KqlToSqlConverter _converter = new();

    [Fact]
    public void Cumulative_Sum_Pattern()
    {
        // col = s.col + expr → SUM(expr) OVER (...)
        var kql = @"range x from 1 to 5 step 1
            | scan declare (cum:long=0) with (
                step s1: true => cum = x + s1.cum;
            )";
        var sql = _converter.Convert(kql);

        Assert.Contains("SUM(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", sql);
        Assert.Contains("AS cum", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<long>();
        while (reader.Read()) results.Add(reader.GetInt64(1));
        Assert.Equal(new long[] { 1, 3, 6, 10, 15 }, results);
    }

    [Fact]
    public void Forward_Fill_Pattern_IsEmpty()
    {
        // col = iff(isempty(X), s.col, X) → LAST_VALUE ... IGNORE NULLS
        var kql = @"datatable (Ts: long, Event: string) [
                0, 'A', 1, '', 2, 'B', 3, '', 4, '', 6, 'C'
            ]
            | sort by Ts asc
            | scan declare (Event_filled: string='') with (
                step s1: true => Event_filled = iff(isempty(Event), s1.Event_filled, Event);
            )";
        var sql = _converter.Convert(kql);

        Assert.Contains("LAST_VALUE", sql);
        Assert.Contains("IGNORE NULLS", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var filled = new List<string>();
        while (reader.Read()) filled.Add(reader.GetString(2));
        Assert.Equal(new[] { "A", "A", "B", "B", "B", "C" }, filled);
    }

    [Fact]
    public void Forward_Fill_Pattern_IsNull_With_Cast()
    {
        // col = iff(isnull(X), s.col, toint(X)) → casted forward fill
        var kql = @"datatable (Ts: long, V: string) [
                0, '1', 1, '', 2, '2', 3, ''
            ]
            | sort by Ts asc
            | scan declare (V_filled: int=int(null)) with (
                step s1: true => V_filled = iff(isempty(V), s1.V_filled, toint(V));
            )";
        var sql = _converter.Convert(kql);

        Assert.Contains("CAST", sql);
        Assert.Contains("AS INTEGER", sql);
    }

    [Fact]
    public void Cumulative_Sum_With_Reset_Pattern()
    {
        // col = iff(s.col >= threshold, reset, s.col + delta) — approximate
        var kql = @"range x from 1 to 5 step 1
            | scan declare (cum:long=0) with (
                step s1: true => cum = iff(s1.cum >= 10, x, x + s1.cum);
            )";
        var sql = _converter.Convert(kql);

        // Should produce CASE WHEN ... THEN reset ELSE prior + delta
        Assert.Contains("CASE WHEN", sql);
        Assert.Contains(">= 10", sql);
    }

    [Fact]
    public void Multiple_Assignments_In_Single_Step()
    {
        var kql = @"range x from 1 to 3 step 1
            | scan declare (cum_x:long=0, cum_sq:long=0) with (
                step s1: true => cum_x = x + s1.cum_x, cum_sq = (x * x) + s1.cum_sq;
            )";
        var sql = _converter.Convert(kql);

        Assert.Contains("AS cum_x", sql);
        Assert.Contains("AS cum_sq", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var cumX = new List<long>();
        var cumSq = new List<long>();
        while (reader.Read()) { cumX.Add(reader.GetInt64(1)); cumSq.Add(reader.GetInt64(2)); }
        Assert.Equal(new long[] { 1, 3, 6 }, cumX);
        Assert.Equal(new long[] { 1, 5, 14 }, cumSq); // 1, 1+4, 1+4+9
    }

    [Fact]
    public void Multiple_Steps_Not_Supported()
    {
        // Multi-step scan (pattern matching) — not supported
        var kql = @"range x from 1 to 5 step 1
            | scan with (
                step s1: x == 1;
                step s2: x > s1.x;
            )";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }
}
