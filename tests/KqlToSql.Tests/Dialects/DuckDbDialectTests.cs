using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.Tests.Dialects;

/// <summary>
/// Engine-specific tests that verify DuckDB dialect produces correct SQL syntax.
/// These tests ensure the strategy pattern produces the expected DuckDB-specific output.
/// </summary>
public class DuckDbDialectTests
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    [Fact]
    public void Dialect_Name_Is_DuckDB()
    {
        Assert.Equal("DuckDB", _converter.Dialect.Name);
    }

    [Fact]
    public void Default_Constructor_Uses_DuckDb_Dialect()
    {
        var converter = new KqlToSqlConverter();
        Assert.IsType<DuckDbDialect>(converter.Dialect);
    }

    // --- Scalar function tests ---

    [Fact]
    public void DuckDb_ParseJson_Uses_CastAsJson()
    {
        var sql = _converter.Convert("StormEvents | extend data = parse_json(STATE)");
        Assert.Contains("CAST(STATE AS JSON)", sql);
    }

    [Fact]
    public void DuckDb_ToDynamic_Uses_CastAsJson()
    {
        var sql = _converter.Convert("StormEvents | extend data = todynamic(STATE)");
        Assert.Contains("CAST(STATE AS JSON)", sql);
    }

    [Fact]
    public void DuckDb_Split_Uses_StringSplit()
    {
        var sql = _converter.Convert("StormEvents | extend parts = split(STATE, ',')");
        Assert.Contains("STRING_SPLIT(STATE, ',')", sql);
    }

    [Fact]
    public void DuckDb_PackArray_Uses_ListValue()
    {
        var sql = _converter.Convert("print x = pack_array(1, 2, 3)");
        Assert.Contains("LIST_VALUE(1, 2, 3)", sql);
    }

    [Fact]
    public void DuckDb_BagPack_Uses_JsonObject()
    {
        var sql = _converter.Convert("print x = bag_pack('key', 'value')");
        Assert.Contains("json_object('key', 'value')", sql);
    }

    [Fact]
    public void DuckDb_Rand_Uses_Random()
    {
        var sql = _converter.Convert("print x = rand()");
        Assert.Contains("RANDOM()", sql);
    }

    [Fact]
    public void DuckDb_FormatDatetime_Uses_Strftime()
    {
        var sql = _converter.Convert("StormEvents | extend d = format_datetime(StartTime, 'yyyy-MM-dd')");
        Assert.Contains("STRFTIME(StartTime, 'yyyy-MM-dd')", sql);
    }

    [Fact]
    public void DuckDb_StartOfDay_Uses_DateTrunc()
    {
        var sql = _converter.Convert("StormEvents | extend d = startofday(StartTime)");
        Assert.Contains("DATE_TRUNC('day', StartTime)", sql);
    }

    [Fact]
    public void DuckDb_MinOf_Uses_Least()
    {
        var sql = _converter.Convert("print x = min_of(1, 2, 3)");
        Assert.Contains("LEAST(1, 2, 3)", sql);
    }

    [Fact]
    public void DuckDb_MaxOf_Uses_Greatest()
    {
        var sql = _converter.Convert("print x = max_of(1, 2, 3)");
        Assert.Contains("GREATEST(1, 2, 3)", sql);
    }

    // --- Aggregate function tests ---

    [Fact]
    public void DuckDb_Dcount_Uses_ApproxCountDistinct()
    {
        var sql = _converter.Convert("StormEvents | summarize dcount(STATE)");
        Assert.Contains("APPROX_COUNT_DISTINCT(STATE)", sql);
    }

    [Fact]
    public void DuckDb_MakeList_Uses_List()
    {
        var sql = _converter.Convert("StormEvents | summarize make_list(STATE)");
        Assert.Contains("LIST(STATE)", sql);
    }

    [Fact]
    public void DuckDb_MakeSet_Uses_ListDistinct()
    {
        var sql = _converter.Convert("StormEvents | summarize make_set(STATE)");
        Assert.Contains("LIST(DISTINCT STATE)", sql);
    }

    [Fact]
    public void DuckDb_TakeAny_Uses_AnyValue()
    {
        var sql = _converter.Convert("StormEvents | summarize take_any(STATE)");
        Assert.Contains("ANY_VALUE(STATE)", sql);
    }

    [Fact]
    public void DuckDb_Percentile_Uses_QuantileCont()
    {
        var sql = _converter.Convert("StormEvents | summarize percentile(DAMAGE_PROPERTY, 50)");
        Assert.Contains("quantile_cont(DAMAGE_PROPERTY, 50 / 100.0)", sql);
    }

    // --- SQL syntax feature tests ---

    [Fact]
    public void DuckDb_ProjectAway_Uses_Exclude()
    {
        var sql = _converter.Convert("StormEvents | project-away STATE");
        Assert.Contains("* EXCLUDE (STATE)", sql);
    }

    [Fact]
    public void DuckDb_ProjectRename_Uses_Rename()
    {
        var sql = _converter.Convert("StormEvents | project-rename s = STATE");
        Assert.Contains("* RENAME (STATE AS s)", sql);
    }

    [Fact]
    public void DuckDb_Join_InnerUnique_Uses_Qualify()
    {
        var sql = _converter.Convert("StormEvents | join (StormEvents) on STATE");
        Assert.Contains("QUALIFY ROW_NUMBER()", sql);
    }

    [Fact]
    public void DuckDb_Range_Uses_GenerateSeries()
    {
        var sql = _converter.Convert("range x from 1 to 5 step 1");
        Assert.Contains("generate_series", sql);
    }

    [Fact]
    public void DuckDb_Has_Uses_Ilike()
    {
        var sql = _converter.Convert("StormEvents | where STATE has 'tex'");
        Assert.Contains("ILIKE", sql);
    }

    [Fact]
    public void DuckDb_Contains_Uses_Ilike()
    {
        var sql = _converter.Convert("StormEvents | where STATE contains 'tex'");
        Assert.Contains("ILIKE", sql);
    }

    // --- Type mapping tests ---

    [Fact]
    public void DuckDb_MapType_Dynamic_Is_Json()
    {
        var sql = _converter.Convert(".create table T (c1: dynamic)");
        Assert.Contains("JSON", sql);
    }

    [Fact]
    public void DuckDb_MapType_Guid_Is_Uuid()
    {
        var sql = _converter.Convert(".create table T (c1: guid)");
        Assert.Contains("UUID", sql);
    }

    [Fact]
    public void DuckDb_MapType_DateTime_Is_Timestamp()
    {
        var sql = _converter.Convert(".create table T (c1: datetime)");
        Assert.Contains("TIMESTAMP", sql);
    }

    [Fact]
    public void DuckDb_MapType_String_Is_Varchar()
    {
        var sql = _converter.Convert(".create table T (c1: string)");
        Assert.Contains("VARCHAR", sql);
    }

    // --- Dialect interface contract tests ---

    [Fact]
    public void DuckDb_SelectExclude_Returns_Correct_Syntax()
    {
        var dialect = new DuckDbDialect();
        Assert.Equal("* EXCLUDE (a, b)", dialect.SelectExclude(new[] { "a", "b" }));
    }

    [Fact]
    public void DuckDb_SelectRename_Returns_Correct_Syntax()
    {
        var dialect = new DuckDbDialect();
        Assert.Equal("* RENAME (old AS new)", dialect.SelectRename(new[] { "old AS new" }));
    }

    [Fact]
    public void DuckDb_Qualify_Returns_Correct_Syntax()
    {
        var dialect = new DuckDbDialect();
        Assert.Equal("SELECT * FROM (SELECT 1) QUALIFY x = 1", dialect.Qualify("SELECT 1", "x = 1"));
    }

    [Fact]
    public void DuckDb_JsonAccess_Returns_Correct_Syntax()
    {
        var dialect = new DuckDbDialect();
        Assert.Equal("trim(both '\"' from json_extract(col, '$.path'))", dialect.JsonAccess("col", "path"));
    }

    [Fact]
    public void DuckDb_GenerateSeries_Returns_Correct_Syntax()
    {
        var dialect = new DuckDbDialect();
        Assert.Equal("SELECT generate_series AS x FROM generate_series(1, 10, 1)", dialect.GenerateSeries("x", "1", "10", "1"));
    }

    [Fact]
    public void DuckDb_Unnest_Returns_Correct_Syntax()
    {
        var dialect = new DuckDbDialect();
        Assert.Equal("CROSS JOIN UNNEST(t.col) AS u(value)", dialect.Unnest("t", "col", "u"));
    }

    [Fact]
    public void DuckDb_TryTranslateFunction_Returns_Null_For_Unknown()
    {
        var dialect = new DuckDbDialect();
        Assert.Null(dialect.TryTranslateFunction("unknown_function", new[] { "arg1" }));
    }

    [Fact]
    public void DuckDb_TryTranslateAggregate_Returns_Null_For_Unknown()
    {
        var dialect = new DuckDbDialect();
        Assert.Null(dialect.TryTranslateAggregate("unknown_agg", new[] { "arg1" }));
    }
}
