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

    // -----------------------------------------------------------------------------------------
    // Bare (non-step-prefixed) declared references read the value WITHIN the current row only:
    // they reset to the declared default each row and do NOT carry across rows. These previously
    // emitted SQL that referenced the column before it was defined.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void Bare_Reference_Resolves_To_Default_Not_Carried_State()
    {
        // `cum = cum + v` with a BARE `cum` is per-row: cum = default(0) + v = v (NOT a running sum).
        var kql = @"datatable(t:long, v:long)[1,5, 2,0, 3,0, 4,7, 5,0]
            | sort by t asc
            | scan declare (cum:long=0) with (
                step s: true => cum = cum + v;
            )";
        var sql = _converter.Convert(kql);

        // The bare state reference must resolve to the declared default, not an unbound column.
        Assert.Equal("SELECT *, (0) + v AS cum FROM (VALUES (CAST(1 AS BIGINT), CAST(5 AS BIGINT)), (CAST(2 AS BIGINT), CAST(0 AS BIGINT)), (CAST(3 AS BIGINT), CAST(0 AS BIGINT)), (CAST(4 AS BIGINT), CAST(7 AS BIGINT)), (CAST(5 AS BIGINT), CAST(0 AS BIGINT))) AS t(t, v) ORDER BY t ASC NULLS FIRST", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<long>();
        while (reader.Read()) results.Add(reader.GetInt64(2));
        Assert.Equal(new long[] { 5, 0, 0, 7, 0 }, results); // == v, per-row
    }

    [Fact]
    public void Bare_Reference_Running_Max_Idiom_Is_Per_Row_Clamp()
    {
        // `mx = iff(v > mx, v, mx)` with a BARE `mx` clamps each row at the default: max(v, 0).
        var kql = @"datatable(t:long, v:long)[1,5, 2,3, 3,99, 4,7]
            | sort by t asc
            | scan declare (mx:long=0) with (
                step s: true => mx = iff(v > mx, v, mx);
            )";
        var sql = _converter.Convert(kql);

        Assert.Equal("SELECT *, CASE WHEN v > (0) THEN v ELSE (0) END AS mx FROM (VALUES (CAST(1 AS BIGINT), CAST(5 AS BIGINT)), (CAST(2 AS BIGINT), CAST(3 AS BIGINT)), (CAST(3 AS BIGINT), CAST(99 AS BIGINT)), (CAST(4 AS BIGINT), CAST(7 AS BIGINT))) AS t(t, v) ORDER BY t ASC NULLS FIRST", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<long>();
        while (reader.Read()) results.Add(reader.GetInt64(2));
        Assert.Equal(new long[] { 5, 3, 99, 7 }, results); // max(v, 0) == v here
    }

    [Fact]
    public void Bare_Reference_Ema_Idiom_Is_Per_Row()
    {
        // `ema = 0.5*v + 0.5*ema` with BARE `ema` is per-row: 0.5*v + 0.5*0 = 0.5*v.
        var kql = @"datatable(t:long, v:real)[1,1.0, 2,2.0, 3,3.0, 4,4.0]
            | sort by t asc
            | scan declare (ema:real=0.0) with (
                step s: true => ema = 0.5*v + 0.5*ema;
            )";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<double>();
        while (reader.Read()) results.Add(Convert.ToDouble(reader.GetValue(2)));
        Assert.Equal(new[] { 0.5, 1.0, 1.5, 2.0 }, results);
    }

    // -----------------------------------------------------------------------------------------
    // Step-prefixed references (`s.col`) read the PREVIOUS row's value — true carried state.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void Prefixed_Reference_Running_Max_Uses_Window()
    {
        // `mx = iff(v > s.mx, v, s.mx)` is a running max → MAX(...) OVER (...) clamped by the seed.
        var kql = @"datatable(t:long, v:long)[1,5, 2,3, 3,99, 4,7]
            | sort by t asc
            | scan declare (mx:long=0) with (
                step s: true => mx = iff(v > s.mx, v, s.mx);
            )";
        var sql = _converter.Convert(kql);

        Assert.Contains("GREATEST(MAX(v) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<long>();
        while (reader.Read()) results.Add(reader.GetInt64(2));
        Assert.Equal(new long[] { 5, 5, 99, 99 }, results); // running max
    }

    [Fact]
    public void Prefixed_Reference_Recurrence_Uses_Recursive_Cte()
    {
        // A genuine recurrence `ema = 0.5*v + 0.5*s.ema` is not a single window aggregate →
        // recursive CTE carrying the declared (DOUBLE) state row by row.
        var kql = @"datatable(t:long, v:real)[1,1.0, 2,2.0, 3,3.0, 4,4.0]
            | sort by t asc
            | scan declare (ema:real=0.0) with (
                step s: true => ema = 0.5*v + 0.5*s.ema;
            )";
        var sql = _converter.Convert(kql);

        Assert.Contains("WITH RECURSIVE", sql);
        Assert.Contains("CAST(0.5 * v + 0.5 * p.ema AS DOUBLE)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        // Column order after the recursive join: t, v, ema.
        var results = new List<double>();
        while (reader.Read()) results.Add(reader.GetDouble(2));
        Assert.Equal(new[] { 0.5, 1.25, 2.125, 3.0625 }, results); // exact recurrence, no DECIMAL drift
    }

    [Fact]
    public void Prefixed_Reference_Cumulative_Sum_Uses_Window()
    {
        // `cum = s.cum + v` (prefixed) is the true running sum.
        var kql = @"datatable(t:long, v:long)[1,5, 2,0, 3,0, 4,7, 5,0]
            | sort by t asc
            | scan declare (cum:long=0) with (
                step s: true => cum = s.cum + v;
            )";
        var sql = _converter.Convert(kql);

        Assert.Contains("SUM(v) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<long>();
        while (reader.Read()) results.Add(reader.GetInt64(2));
        Assert.Equal(new long[] { 5, 5, 5, 12, 12 }, results); // running sum
    }
}
