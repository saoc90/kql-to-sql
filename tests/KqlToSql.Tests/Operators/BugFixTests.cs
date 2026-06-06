using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class BugFixTests
{
    [Fact]
    public void Sort_AfterSummarize_WrapsInSubquery_AndExecutes()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize n = count() by State | sort by n desc";
        var sql = converter.Convert(kql);

        // Summarize result must be wrapped so ORDER BY doesn't reference ungrouped columns.
        Assert.StartsWith("SELECT * FROM (", sql);
        Assert.EndsWith(") ORDER BY n DESC NULLS LAST", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
        Assert.True(reader.GetInt64(1) > 0);
    }


    [Fact]
    public void Sort_DefaultDirection_IsDescending()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | sort by State");
        Assert.Equal("SELECT * FROM StormEvents ORDER BY State DESC NULLS LAST", sql);
    }

    [Fact]
    public void Sort_ExplicitAsc_IsAscending()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | sort by State asc");
        Assert.Equal("SELECT * FROM StormEvents ORDER BY State ASC NULLS FIRST", sql);
    }

    [Fact]
    public void Sort_ExplicitDesc_IsDescending()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | sort by State desc");
        Assert.Equal("SELECT * FROM StormEvents ORDER BY State DESC NULLS LAST", sql);
    }

    [Fact]
    public void Sort_NamedExpression_DoesNotEmitAliasInOrderBy()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize count() by EventType | order by c = count_ | render columnchart";
        var sql = converter.Convert(kql);

        Assert.Equal("SELECT * FROM (SELECT EventType, COUNT(*) AS count_ FROM StormEvents GROUP BY ALL) ORDER BY count_ DESC NULLS LAST", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
        Assert.True(reader.GetInt64(1) > 0);
    }

    [Fact]
    public void Extend_SubtractExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend diff = InjuriesDirect - InjuriesIndirect");
        Assert.Equal("SELECT *, InjuriesDirect - InjuriesIndirect AS diff FROM StormEvents", sql);
    }

    [Fact]
    public void Extend_MultiplyExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend doubled = InjuriesDirect * 2");
        Assert.Equal("SELECT *, InjuriesDirect * 2 AS doubled FROM StormEvents", sql);
    }

    [Fact]
    public void Extend_DivideExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend halved = InjuriesDirect / 2");
        Assert.Equal("SELECT *, InjuriesDirect / 2 AS halved FROM StormEvents", sql);
    }

    [Fact]
    public void Extend_ModuloExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend remainder = InjuriesDirect % 2");
        // KQL modulo is Euclidean (result in [0,|b|)) for all numeric types.
        Assert.Equal("SELECT *, (((InjuriesDirect) % NULLIF(2, 0)) + ABS(2)) % NULLIF(2, 0) AS remainder FROM StormEvents", sql);
    }

    [Fact]
    public void Iff_Function_MapsToCase()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend result = iff(InjuriesDirect > 0, 'Yes', 'No')");
        Assert.Equal("SELECT *, CASE WHEN InjuriesDirect > 0 THEN 'Yes' ELSE 'No' END AS result FROM StormEvents", sql);
    }

    [Fact]
    public void Iif_Function_MapsToCase()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend result = iif(InjuriesDirect > 0, 'Yes', 'No')");
        Assert.Equal("SELECT *, CASE WHEN InjuriesDirect > 0 THEN 'Yes' ELSE 'No' END AS result FROM StormEvents", sql);
    }

    [Fact]
    public void BoolTypedLiteral_EmitsTrueOrFalse_NotFunctionCall()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | where bool(true)");
        Assert.Contains("TRUE", sql);
        Assert.DoesNotContain("bool(", sql);
    }

    // ---- Differential-fuzzing regressions (value mismatches found vs Kustainer) ------------------

    [Fact]
    public void DataTable_BoolNullLiteral_BecomesSqlNull_NotFalse()
    {
        // bool(null) datatable cells were rendered as FALSE, corrupting countif(isnull(b))/countif(not(b)).
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("datatable(b:bool)[ bool(null), true, false ] | where isnull(b)");
        Assert.Contains("CAST(NULL AS BOOLEAN)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FILTER (WHERE b IS NULL) FROM (VALUES (CAST(NULL AS BOOLEAN)), (TRUE), (FALSE)) AS t(b)";
        Assert.Equal(1L, (long)cmd.ExecuteScalar()!);
    }

    [Fact]
    public void Extend_RedefineColumnArrivingViaStar_ReplacesInPlace_NoDuplicate()
    {
        // `extend x` redefining a column that flows in through `*` must replace it (REPLACE), not append a
        // second `x` (which shadows the new value). Resolved via the semantic model when a top-level * exists.
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("print x = 1 | extend y = x + 1 | extend x = y * 10 | extend z = x + y | project x, y, z");

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Equal(20L, r.GetInt64(0)); // x = y*10 = 20
        Assert.Equal(2L, r.GetInt64(1));  // y = x+1 = 2
        Assert.Equal(22L, r.GetInt64(2)); // z = x+y = 22
    }

    [Fact]
    public void ToDateTime_OfNumeric_InterpretsAsTicks()
    {
        // Kusto todatetime(numeric) reads the number as .NET ticks (inverse of tolong(datetime)); a bare
        // CAST to TIMESTAMP yields NULL. 0 ticks → 0001-01-01.
        var sql = new KqlToSqlConverter().Convert("print d = todatetime(0)");
        Assert.Contains("TIMESTAMP '0001-01-01 00:00:00'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        Assert.Equal(new System.DateTime(1, 1, 1), (System.DateTime)cmd.ExecuteScalar()!);
    }

    [Fact]
    public void MakeTimespan_OutOfRangeComponent_YieldsNull()
    {
        // Kusto make_timespan(25, 70) → null (minutes/hours out of range); we previously computed a value.
        var sql = new KqlToSqlConverter().Convert("print t = make_timespan(25, 70)");
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        Assert.True(cmd.ExecuteScalar() is null or System.DBNull);

        cmd.CommandText = new KqlToSqlConverter().Convert("print t = make_timespan(2, 30)");
        Assert.False(cmd.ExecuteScalar() is null or System.DBNull); // valid → not null
    }

    [Fact]
    public void ArrayIndexOf_NotFound_ReturnsMinusOne()
    {
        // Kusto array_index_of returns -1 when absent; DuckDB list_position returns NULL.
        var sql = new KqlToSqlConverter().Convert("print i = array_index_of(dynamic([1,2,3]), 99)");
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        Assert.Equal(-1L, System.Convert.ToInt64(cmd.ExecuteScalar()));
    }

    [Fact]
    public void DynamicArray_NegativeIndex_ReturnsFromEnd()
    {
        // d[-1] is the last element (Kusto). A native LIST shifted +1 turned d[-1] into d[0] = NULL.
        var sql = new KqlToSqlConverter().Convert("print last = dynamic([10,20,30])[-1]");
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        Assert.Equal(30L, System.Convert.ToInt64(cmd.ExecuteScalar()));
    }

    [Fact]
    public void Split_WithIndex_ReturnsSingleElementArrayOfThatPart()
    {
        // split(s, delim, index) returns a one-element array of the indexed part (Kusto), not the whole split.
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = new KqlToSqlConverter().Convert("print p = split('key=val', '=', 1)");
        var single = (System.Collections.Generic.List<string>)cmd.ExecuteScalar()!;
        Assert.Equal(new[] { "val" }, single);

        cmd.CommandText = new KqlToSqlConverter().Convert("print p = split('key=val', '=', 5)"); // out of range → []
        var empty = (System.Collections.Generic.List<string>)cmd.ExecuteScalar()!;
        Assert.Empty(empty);
    }

    [Fact]
    public void TopNested_AggNameCollidingWithGroupColumnCase_StillComputesAggregate()
    {
        // `top-nested of sub by Sub=sum(v)` — DuckDB folds `sub`/`Sub` to one column. Internal agg aliases
        // keep the ranking/output bound to the aggregate, not the group key.
        var sql = new KqlToSqlConverter().Convert(
            "datatable(sub:string, v:long)[ 'x',10, 'y',20 ] | top-nested of sub by Sub=sum(v)");
        Assert.Contains("_tnagg0", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        long total = 0;
        while (r.Read()) total += r.GetInt64(1); // the Sub aggregate column, not the sub group key
        Assert.Equal(30L, total);
    }
}
