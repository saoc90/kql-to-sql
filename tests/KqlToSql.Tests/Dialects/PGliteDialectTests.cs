using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.Tests.Dialects;

/// <summary>
/// Engine-specific tests that verify PGlite (PostgreSQL) dialect produces correct SQL syntax.
/// These tests ensure the strategy pattern produces the expected PostgreSQL-specific output.
/// </summary>
public class PGliteDialectTests
{
    private readonly KqlToSqlConverter _converter = new(new PGliteDialect());

    [Fact]
    public void Dialect_Name_Is_PGlite()
    {
        Assert.Equal("PGlite", _converter.Dialect.Name);
    }

    [Fact]
    public void Constructor_Accepts_PGlite_Dialect()
    {
        var converter = new KqlToSqlConverter(new PGliteDialect());
        Assert.IsType<PGliteDialect>(converter.Dialect);
    }

    // --- Scalar function tests (PGlite-specific mappings) ---

    [Fact]
    public void PGlite_ParseJson_Uses_CastAsJsonb()
    {
        var sql = _converter.Convert("StormEvents | extend data = parse_json(STATE)");
        Assert.Contains("CAST(STATE AS JSONB)", sql);
    }

    [Fact]
    public void PGlite_ToDynamic_Uses_CastAsJsonb()
    {
        var sql = _converter.Convert("StormEvents | extend data = todynamic(STATE)");
        Assert.Contains("CAST(STATE AS JSONB)", sql);
    }

    [Fact]
    public void PGlite_Split_Uses_StringToArray()
    {
        var sql = _converter.Convert("StormEvents | extend parts = split(STATE, ',')");
        Assert.Contains("string_to_array(STATE, ',')", sql);
    }

    [Fact]
    public void PGlite_PackArray_Uses_ArrayLiteral()
    {
        var sql = _converter.Convert("print x = pack_array(1, 2, 3)");
        Assert.Contains("ARRAY[1, 2, 3]", sql);
    }

    [Fact]
    public void PGlite_BagPack_Uses_JsonbBuildObject()
    {
        var sql = _converter.Convert("print x = bag_pack('key', 'value')");
        Assert.Contains("jsonb_build_object('key', 'value')", sql);
    }

    [Fact]
    public void PGlite_Rand_Uses_Random()
    {
        var sql = _converter.Convert("print x = rand()");
        Assert.Contains("RANDOM()", sql);
    }

    [Fact]
    public void PGlite_FormatDatetime_Uses_ToChar()
    {
        var sql = _converter.Convert("StormEvents | extend d = format_datetime(StartTime, 'yyyy-MM-dd')");
        Assert.Contains("TO_CHAR(StartTime, 'yyyy-MM-dd')", sql);
    }

    [Fact]
    public void PGlite_StartOfDay_Uses_DateTrunc()
    {
        var sql = _converter.Convert("StormEvents | extend d = startofday(StartTime)");
        Assert.Contains("DATE_TRUNC('day', StartTime)", sql);
    }

    [Fact]
    public void PGlite_MinOf_Uses_Least()
    {
        var sql = _converter.Convert("print x = min_of(1, 2, 3)");
        Assert.Contains("LEAST(1, 2, 3)", sql);
    }

    [Fact]
    public void PGlite_MaxOf_Uses_Greatest()
    {
        var sql = _converter.Convert("print x = max_of(1, 2, 3)");
        Assert.Contains("GREATEST(1, 2, 3)", sql);
    }

    [Fact]
    public void PGlite_Log10_Uses_LogFunction()
    {
        var sql = _converter.Convert("print x = log10(100)");
        Assert.Contains("LOG(10, 100)", sql);
    }

    [Fact]
    public void PGlite_IndexOf_Uses_Position()
    {
        var sql = _converter.Convert("StormEvents | extend pos = indexof(STATE, 'X')");
        Assert.Contains("POSITION('X' IN STATE)", sql);
    }

    // --- Aggregate function tests (PGlite-specific mappings) ---

    [Fact]
    public void PGlite_Dcount_Uses_CountDistinct()
    {
        var sql = _converter.Convert("StormEvents | summarize dcount(STATE)");
        Assert.Contains("COUNT(DISTINCT STATE)", sql);
    }

    [Fact]
    public void PGlite_MakeList_Uses_ArrayAgg()
    {
        var sql = _converter.Convert("StormEvents | summarize make_list(STATE)");
        Assert.Contains("array_agg(STATE)", sql);
    }

    [Fact]
    public void PGlite_MakeSet_Uses_ArrayAggDistinct()
    {
        var sql = _converter.Convert("StormEvents | summarize make_set(STATE)");
        Assert.Contains("array_agg(DISTINCT STATE)", sql);
    }

    [Fact]
    public void PGlite_TakeAny_Uses_ArrayAggIndexed()
    {
        var sql = _converter.Convert("StormEvents | summarize take_any(STATE)");
        Assert.Contains("(array_agg(STATE))[1]", sql);
    }

    [Fact]
    public void PGlite_Percentile_Uses_PercentileCont()
    {
        var sql = _converter.Convert("StormEvents | summarize percentile(DAMAGE_PROPERTY, 50)");
        Assert.Contains("PERCENTILE_CONT(50 / 100.0) WITHIN GROUP (ORDER BY DAMAGE_PROPERTY)", sql);
    }

    [Fact]
    public void PGlite_CountDistinct_Uses_CountDistinct()
    {
        var sql = _converter.Convert("StormEvents | summarize count_distinct(STATE)");
        Assert.Contains("COUNT(DISTINCT STATE)", sql);
    }

    [Fact]
    public void PGlite_BinaryAllAnd_Uses_BitAndWithCast()
    {
        var sql = _converter.Convert("StormEvents | summarize binary_all_and(INJURIES_DIRECT)");
        Assert.Contains("BIT_AND(INJURIES_DIRECT::int)", sql);
    }

    // --- SQL syntax feature tests ---

    [Fact]
    public void PGlite_Range_Uses_GenerateSeries()
    {
        var sql = _converter.Convert("range x from 1 to 5 step 1");
        Assert.Contains("generate_series", sql);
    }

    [Fact]
    public void PGlite_Has_Uses_Ilike()
    {
        var sql = _converter.Convert("StormEvents | where STATE has 'tex'");
        Assert.Contains("ILIKE", sql);
    }

    [Fact]
    public void PGlite_Contains_Uses_Ilike()
    {
        var sql = _converter.Convert("StormEvents | where STATE contains 'tex'");
        Assert.Contains("ILIKE", sql);
    }

    [Fact]
    public void PGlite_Where_And_Project()
    {
        var sql = _converter.Convert("StormEvents | where STATE == 'TEXAS' | project EVENT_TYPE");
        Assert.Equal("SELECT EVENT_TYPE FROM StormEvents WHERE STATE = 'TEXAS'", sql);
    }

    [Fact]
    public void PGlite_Summarize_Count_ByState()
    {
        var sql = _converter.Convert("StormEvents | summarize count() by STATE");
        Assert.Equal("SELECT STATE, COUNT(*) AS count FROM StormEvents GROUP BY STATE", sql);
    }

    // --- Type mapping tests ---

    [Fact]
    public void PGlite_MapType_Dynamic_Is_Jsonb()
    {
        var sql = _converter.Convert(".create table T (c1: dynamic)");
        Assert.Contains("JSONB", sql);
    }

    [Fact]
    public void PGlite_MapType_Guid_Is_Uuid()
    {
        var sql = _converter.Convert(".create table T (c1: guid)");
        Assert.Contains("UUID", sql);
    }

    [Fact]
    public void PGlite_MapType_DateTime_Is_Timestamp()
    {
        var sql = _converter.Convert(".create table T (c1: datetime)");
        Assert.Contains("TIMESTAMP", sql);
    }

    [Fact]
    public void PGlite_MapType_String_Is_Text()
    {
        var sql = _converter.Convert(".create table T (c1: string)");
        Assert.Contains("TEXT", sql);
    }

    [Fact]
    public void PGlite_MapType_Real_Is_DoublePrecision()
    {
        var sql = _converter.Convert(".create table T (c1: real)");
        Assert.Contains("DOUBLE PRECISION", sql);
    }

    [Fact]
    public void PGlite_MapType_Decimal_Is_Numeric()
    {
        var sql = _converter.Convert(".create table T (c1: decimal)");
        Assert.Contains("NUMERIC", sql);
    }

    [Fact]
    public void PGlite_MapType_Int_Is_Integer()
    {
        var sql = _converter.Convert(".create table T (c1: int)");
        Assert.Contains("INTEGER", sql);
    }

    // --- Dialect interface contract tests ---

    [Fact]
    public void PGlite_JsonAccess_Single_Key()
    {
        var dialect = new PGliteDialect();
        Assert.Equal("(col::jsonb->>'name')", dialect.JsonAccess("col", "name"));
    }

    [Fact]
    public void PGlite_JsonAccess_Nested_Path()
    {
        var dialect = new PGliteDialect();
        Assert.Equal("(col::jsonb->'address'->>'city')", dialect.JsonAccess("col", "address.city"));
    }

    [Fact]
    public void PGlite_Extend_DotNotation_JsonAccess_CastsToJsonb()
    {
        var sql = _converter.Convert("StormEvents | take 10 | extend test = StormSummary.TotalDamages");
        Assert.Equal("SELECT *, (StormSummary::jsonb->>'TotalDamages') AS test FROM StormEvents LIMIT 10", sql);
    }

    [Fact]
    public void PGlite_Extend_BracketNotation_JsonAccess_CastsToJsonb()
    {
        var sql = _converter.Convert("StormEvents | take 10 | extend test = StormSummary['TotalDamages']");
        Assert.Equal("SELECT *, (StormSummary::jsonb->>'TotalDamages') AS test FROM StormEvents LIMIT 10", sql);
    }

    [Fact]
    public void PGlite_ToInt_Of_DotNotation_JsonProperty_CastsToJsonb()
    {
        var sql = _converter.Convert("StormEvents | take 10 | extend test = toint(StormSummary.TotalDamages)");
        Assert.Equal("SELECT *, CAST((StormSummary::jsonb->>'TotalDamages') AS INTEGER) AS test FROM StormEvents LIMIT 10", sql);
    }

    [Fact]
    public void PGlite_ToInt_Of_BracketNotation_JsonProperty_CastsToJsonb()
    {
        var sql = _converter.Convert("StormEvents | take 10 | extend test = toint(StormSummary['TotalDamages'])");
        Assert.Equal("SELECT *, CAST((StormSummary::jsonb->>'TotalDamages') AS INTEGER) AS test FROM StormEvents LIMIT 10", sql);
    }

    [Fact]
    public void PGlite_GenerateSeries_Returns_Correct_Syntax()
    {
        var dialect = new PGliteDialect();
        Assert.Equal("SELECT generate_series AS x FROM generate_series(1, 10, 1)", dialect.GenerateSeries("x", "1", "10", "1"));
    }

    [Fact]
    public void PGlite_Unnest_Uses_CrossJoinLateral()
    {
        var dialect = new PGliteDialect();
        Assert.Equal("CROSS JOIN LATERAL UNNEST(t.col) AS u(value)", dialect.Unnest("t", "col", "u"));
    }

    [Fact]
    public void PGlite_Qualify_Uses_Subquery_Workaround()
    {
        var dialect = new PGliteDialect();
        var result = dialect.Qualify("SELECT * FROM my_table", "ROW_NUMBER() OVER (PARTITION BY state) = 1");
        Assert.Contains("ROW_NUMBER() OVER (PARTITION BY state) AS _rn", result);
        Assert.Contains("WHERE _rn = 1", result);
        Assert.Contains("my_table", result);
    }

    [Fact]
    public void PGlite_TryTranslateFunction_Returns_Null_For_Unknown()
    {
        var dialect = new PGliteDialect();
        Assert.Null(dialect.TryTranslateFunction("unknown_function", new[] { "arg1" }));
    }

    [Fact]
    public void PGlite_TryTranslateAggregate_Returns_Null_For_Unknown()
    {
        var dialect = new PGliteDialect();
        Assert.Null(dialect.TryTranslateAggregate("unknown_agg", new[] { "arg1" }));
    }

    // --- Verify different output from DuckDB for key functions ---

    [Fact]
    public void PGlite_ParseJson_Differs_From_DuckDb()
    {
        var pgConverter = new KqlToSqlConverter(new PGliteDialect());
        var duckConverter = new KqlToSqlConverter(new DuckDbDialect());

        var kql = "StormEvents | extend data = parse_json(STATE)";
        var pgSql = pgConverter.Convert(kql);
        var duckSql = duckConverter.Convert(kql);

        Assert.Contains("JSONB", pgSql);
        Assert.Contains("JSON", duckSql);
        Assert.DoesNotContain("JSONB", duckSql);
    }

    [Fact]
    public void PGlite_Split_Differs_From_DuckDb()
    {
        var pgConverter = new KqlToSqlConverter(new PGliteDialect());
        var duckConverter = new KqlToSqlConverter(new DuckDbDialect());

        var kql = "StormEvents | extend parts = split(STATE, ',')";
        var pgSql = pgConverter.Convert(kql);
        var duckSql = duckConverter.Convert(kql);

        Assert.Contains("string_to_array", pgSql);
        Assert.Contains("STRING_SPLIT", duckSql);
    }

    [Fact]
    public void PGlite_MakeList_Differs_From_DuckDb()
    {
        var pgConverter = new KqlToSqlConverter(new PGliteDialect());
        var duckConverter = new KqlToSqlConverter(new DuckDbDialect());

        var kql = "StormEvents | summarize make_list(STATE)";
        var pgSql = pgConverter.Convert(kql);
        var duckSql = duckConverter.Convert(kql);

        Assert.Contains("array_agg(STATE)", pgSql);
        Assert.Contains("LIST(STATE)", duckSql);
    }
}
