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

    // --- Window function tests ---

    [Fact]
    public void PGlite_RowNumber_Uses_RowNumberOver()
    {
        var sql = _converter.Convert("StormEvents | extend rn = row_number()");
        Assert.Contains("ROW_NUMBER() OVER ()", sql);
    }

    [Fact]
    public void PGlite_Prev_Uses_Lag()
    {
        var sql = _converter.Convert("StormEvents | extend p = prev(STATE)");
        Assert.Contains("LAG(STATE) OVER ()", sql);
    }

    [Fact]
    public void PGlite_Next_Uses_Lead()
    {
        var sql = _converter.Convert("StormEvents | extend n = next(STATE)");
        Assert.Contains("LEAD(STATE) OVER ()", sql);
    }

    // --- Date/time extraction function tests ---

    [Fact]
    public void PGlite_DayOfWeek_Uses_ExtractDow()
    {
        var sql = _converter.Convert("StormEvents | extend d = dayofweek(StartTime)");
        Assert.Contains("EXTRACT(DOW FROM StartTime)", sql);
    }

    [Fact]
    public void PGlite_DayOfMonth_Uses_ExtractDay()
    {
        var sql = _converter.Convert("StormEvents | extend d = dayofmonth(StartTime)");
        Assert.Contains("EXTRACT(DAY FROM StartTime)", sql);
    }

    [Fact]
    public void PGlite_DayOfYear_Uses_ExtractDoy()
    {
        var sql = _converter.Convert("StormEvents | extend d = dayofyear(StartTime)");
        Assert.Contains("EXTRACT(DOY FROM StartTime)", sql);
    }

    [Fact]
    public void PGlite_GetMonth_Uses_ExtractMonth()
    {
        var sql = _converter.Convert("StormEvents | extend m = getmonth(StartTime)");
        Assert.Contains("EXTRACT(MONTH FROM StartTime)", sql);
    }

    [Fact]
    public void PGlite_GetYear_Uses_ExtractYear()
    {
        var sql = _converter.Convert("StormEvents | extend y = getyear(StartTime)");
        Assert.Contains("EXTRACT(YEAR FROM StartTime)", sql);
    }

    [Fact]
    public void PGlite_MonthOfYear_Uses_ExtractMonth()
    {
        var sql = _converter.Convert("StormEvents | extend m = monthofyear(StartTime)");
        Assert.Contains("EXTRACT(MONTH FROM StartTime)", sql);
    }

    [Fact]
    public void PGlite_WeekOfYear_Uses_ExtractWeek()
    {
        var sql = _converter.Convert("StormEvents | extend w = weekofyear(StartTime)");
        Assert.Contains("EXTRACT(WEEK FROM StartTime)", sql);
    }

    [Fact]
    public void PGlite_HourOfDay_Uses_ExtractHour()
    {
        var sql = _converter.Convert("StormEvents | extend h = hourofday(StartTime)");
        Assert.Contains("EXTRACT(HOUR FROM StartTime)", sql);
    }

    [Fact]
    public void PGlite_MinuteOfHour_Uses_ExtractMinute()
    {
        var sql = _converter.Convert("StormEvents | extend m = minuteofhour(StartTime)");
        Assert.Contains("EXTRACT(MINUTE FROM StartTime)", sql);
    }

    [Fact]
    public void PGlite_SecondOfMinute_Uses_ExtractSecond()
    {
        var sql = _converter.Convert("StormEvents | extend s = secondofminute(StartTime)");
        Assert.Contains("EXTRACT(SECOND FROM StartTime)", sql);
    }

    // --- Date/time construction function tests ---

    [Fact]
    public void PGlite_MakeDatetime_6Args_Uses_MakeTimestamp()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("make_datetime", new[] { "2024", "1", "15", "10", "30", "0" });
        Assert.Equal("MAKE_TIMESTAMP(2024, 1, 15, 10, 30, 0)", result);
    }

    [Fact]
    public void PGlite_MakeDatetime_1Arg_Uses_CastAsTimestamp()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("make_datetime", new[] { "'2024-01-15'" });
        Assert.Equal("CAST('2024-01-15' AS TIMESTAMP)", result);
    }

    [Fact]
    public void PGlite_MakeTimespan_Uses_IntervalArithmetic()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("make_timespan", new[] { "1", "30", "0" });
        Assert.Contains("INTERVAL '1 hour'", result);
        Assert.Contains("INTERVAL '1 minute'", result);
        Assert.Contains("INTERVAL '1 second'", result);
    }

    [Fact]
    public void PGlite_UnixtimeSecondsTodatetime_Uses_ToTimestamp()
    {
        var sql = _converter.Convert("print x = unixtime_seconds_todatetime(1672531200)");
        Assert.Contains("TO_TIMESTAMP(1672531200)", sql);
    }

    [Fact]
    public void PGlite_UnixtimeMillisecondsTodatetime_Uses_ToTimestampDivided()
    {
        var sql = _converter.Convert("print x = unixtime_milliseconds_todatetime(1672531200000)");
        Assert.Contains("TO_TIMESTAMP(1672531200000::double precision / 1000)", sql);
    }

    // --- Date/time formatting function tests ---

    [Fact]
    public void PGlite_DatetimePart_Uses_Extract()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("datetime_part", new[] { "'year'", "StartTime" });
        Assert.Equal("EXTRACT(year FROM StartTime)", result);
    }

    [Fact]
    public void PGlite_FormatTimespan_Uses_CastAsText()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("format_timespan", new[] { "duration", "'hh:mm'" });
        Assert.Equal("CAST(duration AS TEXT)", result);
    }

    // --- String function tests ---

    [Fact]
    public void PGlite_Base64EncodeTostring_Uses_Encode()
    {
        var sql = _converter.Convert("StormEvents | extend b = base64_encode_tostring(STATE)");
        Assert.Contains("ENCODE(CAST(STATE AS BYTEA), 'base64')", sql);
    }

    [Fact]
    public void PGlite_Base64DecodeTostring_Uses_ConvertFromDecode()
    {
        var sql = _converter.Convert("StormEvents | extend b = base64_decode_tostring(STATE)");
        Assert.Contains("CONVERT_FROM(DECODE(STATE, 'base64'), 'UTF8')", sql);
    }

    [Fact]
    public void PGlite_Translate_Uses_Translate()
    {
        var sql = _converter.Convert("print x = translate('hello', 'aeiou', 'AEIOU')");
        Assert.Contains("TRANSLATE('hello', 'aeiou', 'AEIOU')", sql);
    }

    [Fact]
    public void PGlite_Strcmp_Uses_CaseExpression()
    {
        var sql = _converter.Convert("print x = strcmp('abc', 'def')");
        Assert.Contains("CASE WHEN 'abc' < 'def' THEN -1 WHEN 'abc' = 'def' THEN 0 ELSE 1 END", sql);
    }

    [Fact]
    public void PGlite_StringSize_Uses_OctetLength()
    {
        var sql = _converter.Convert("StormEvents | extend sz = string_size(STATE)");
        Assert.Contains("OCTET_LENGTH(STATE)", sql);
    }

    [Fact]
    public void PGlite_Repeat_Uses_Repeat()
    {
        var sql = _converter.Convert("print x = repeat('abc', 3)");
        Assert.Contains("REPEAT('abc', 3)", sql);
    }

    [Fact]
    public void PGlite_Unicode_Uses_Ascii()
    {
        var sql = _converter.Convert("print x = unicode('A')");
        Assert.Contains("ASCII('A')", sql);
    }

    [Fact]
    public void PGlite_MakeString_Uses_Chr()
    {
        var sql = _converter.Convert("print x = make_string(65)");
        Assert.Contains("CHR(65)", sql);
    }

    [Fact]
    public void PGlite_ParsePath_Uses_SubstringRegex()
    {
        var sql = _converter.Convert("StormEvents | extend f = parse_path(STATE)");
        Assert.Contains("SUBSTRING(STATE FROM '[^/\\\\]+$')", sql);
    }

    [Fact]
    public void PGlite_ToUtf8_Uses_CastAsBytea()
    {
        var sql = _converter.Convert("StormEvents | extend b = to_utf8(STATE)");
        Assert.Contains("CAST(STATE AS BYTEA)", sql);
    }

    // --- Hash function tests ---

    [Fact]
    public void PGlite_HashMd5_Uses_Md5()
    {
        var sql = _converter.Convert("StormEvents | extend h = hash_md5(STATE)");
        Assert.Contains("MD5(STATE)", sql);
    }

    [Fact]
    public void PGlite_HashSha256_Uses_EncodeSha256()
    {
        var sql = _converter.Convert("StormEvents | extend h = hash_sha256(STATE)");
        Assert.Contains("ENCODE(SHA256(CAST(STATE AS BYTEA)), 'hex')", sql);
    }

    [Fact]
    public void PGlite_HashSha1_Uses_EncodeDigest()
    {
        var sql = _converter.Convert("StormEvents | extend h = hash_sha1(STATE)");
        Assert.Contains("ENCODE(DIGEST(CAST(STATE AS BYTEA), 'sha1'), 'hex')", sql);
    }

    // --- Array/dynamic function tests ---

    [Fact]
    public void PGlite_ArrayLength_Uses_ArrayLengthDim1()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("array_length", new[] { "myarray" });
        Assert.Equal("ARRAY_LENGTH(myarray, 1)", result);
    }

    [Fact]
    public void PGlite_ArrayIndexOf_Uses_ArrayPositionMinusOne()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("array_index_of", new[] { "myarray", "'val'" });
        Assert.Equal("(ARRAY_POSITION(myarray, 'val') - 1)", result);
    }

    [Fact]
    public void PGlite_ArraySortAsc_Uses_ArrayAggOrderBy()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("array_sort_asc", new[] { "myarray" });
        Assert.Contains("ARRAY_AGG(x ORDER BY x)", result);
        Assert.Contains("UNNEST(myarray)", result);
    }

    [Fact]
    public void PGlite_ArraySortDesc_Uses_ArrayAggOrderByDesc()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("array_sort_desc", new[] { "myarray" });
        Assert.Contains("ARRAY_AGG(x ORDER BY x DESC)", result);
        Assert.Contains("UNNEST(myarray)", result);
    }

    [Fact]
    public void PGlite_ArrayConcat_Uses_PipePipeOperator()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("array_concat", new[] { "arr1", "arr2" });
        Assert.Equal("(arr1 || arr2)", result);
    }

    [Fact]
    public void PGlite_ArraySlice_Uses_BracketNotation()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("array_slice", new[] { "myarray", "1", "3" });
        Assert.Equal("myarray[1+1:3+1]", result);
    }

    // --- Bag/set function tests ---

    [Fact]
    public void PGlite_BagKeys_Uses_JsonbObjectKeys()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("bag_keys", new[] { "mybag" });
        Assert.Contains("JSONB_OBJECT_KEYS(mybag)", result);
        Assert.Contains("ARRAY_AGG", result);
    }

    [Fact]
    public void PGlite_BagHasKey_Uses_JsonbQuestionMark()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("bag_has_key", new[] { "mybag", "'key'" });
        Assert.Equal("(mybag::jsonb ? 'key')", result);
    }

    [Fact]
    public void PGlite_BagMerge_Uses_JsonbConcat()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("bag_merge", new[] { "bag1", "bag2" });
        Assert.Equal("(bag1::jsonb || bag2::jsonb)", result);
    }

    [Fact]
    public void PGlite_SetDifference_Uses_UnnestWithNotAll()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("set_difference", new[] { "arr1", "arr2" });
        Assert.Contains("UNNEST(arr1)", result);
        Assert.Contains("<> ALL(arr2)", result);
    }

    [Fact]
    public void PGlite_SetIntersect_Uses_UnnestWithAny()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("set_intersect", new[] { "arr1", "arr2" });
        Assert.Contains("UNNEST(arr1)", result);
        Assert.Contains("= ANY(arr2)", result);
    }

    [Fact]
    public void PGlite_SetUnion_Uses_UnnestDistinct()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("set_union", new[] { "arr1", "arr2" });
        Assert.Contains("ARRAY_AGG(DISTINCT x)", result);
        Assert.Contains("UNNEST(arr1 || arr2)", result);
    }

    // --- Bitwise function tests ---

    [Fact]
    public void PGlite_BinaryAnd_Uses_AmpersandOperator()
    {
        var sql = _converter.Convert("print x = binary_and(5, 3)");
        Assert.Contains("(5 & 3)", sql);
    }

    [Fact]
    public void PGlite_BinaryOr_Uses_PipeOperator()
    {
        var sql = _converter.Convert("print x = binary_or(5, 3)");
        Assert.Contains("(5 | 3)", sql);
    }

    [Fact]
    public void PGlite_BinaryXor_Uses_HashOperator()
    {
        var sql = _converter.Convert("print x = binary_xor(5, 3)");
        Assert.Contains("(5 # 3)", sql);
    }

    [Fact]
    public void PGlite_BinaryNot_Uses_TildeOperator()
    {
        var sql = _converter.Convert("print x = binary_not(5)");
        Assert.Contains("(~5)", sql);
    }

    [Fact]
    public void PGlite_BinaryShiftLeft_Uses_ShiftLeftOperator()
    {
        var sql = _converter.Convert("print x = binary_shift_left(1, 4)");
        Assert.Contains("(1 << 4)", sql);
    }

    [Fact]
    public void PGlite_BinaryShiftRight_Uses_ShiftRightOperator()
    {
        var sql = _converter.Convert("print x = binary_shift_right(16, 2)");
        Assert.Contains("(16 >> 2)", sql);
    }

    // --- Type/conditional function tests ---

    [Fact]
    public void PGlite_Gettype_Uses_PgTypeof()
    {
        var sql = _converter.Convert("StormEvents | extend t = gettype(STATE)");
        Assert.Contains("PG_TYPEOF(STATE)::text", sql);
    }

    [Fact]
    public void PGlite_Isnan_Uses_NanComparison()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("isnan", new[] { "val" });
        Assert.Equal("(val = 'NaN'::double precision)", result);
    }

    [Fact]
    public void PGlite_Isinf_Uses_InfinityComparison()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("isinf", new[] { "val" });
        Assert.Contains("'Infinity'::double precision", result);
        Assert.Contains("'-Infinity'::double precision", result);
    }

    // --- Additional string/regex function tests ---

    [Fact]
    public void PGlite_ExtractAll_Uses_RegexpMatches()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("extract_all", new[] { "text_col", "'\\d+'" });
        Assert.Contains("REGEXP_MATCHES(text_col, '\\d+', 'g')", result);
        Assert.Contains("ARRAY_AGG", result);
    }

    [Fact]
    public void PGlite_ReplaceRegex_Uses_RegexpReplace()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("replace_regex", new[] { "text_col", "'\\d+'", "'NUM'" });
        Assert.Equal("REGEXP_REPLACE(text_col, '\\d+', 'NUM', 'g')", result);
    }

    [Fact]
    public void PGlite_ParseCsv_Uses_StringToArray()
    {
        var sql = _converter.Convert("StormEvents | extend parts = parse_csv(STATE)");
        Assert.Contains("STRING_TO_ARRAY(STATE, ',')", sql);
    }

    [Fact]
    public void PGlite_DynamicToJson_Uses_ToJson()
    {
        var sql = _converter.Convert("StormEvents | extend j = dynamic_to_json(STATE)");
        Assert.Contains("TO_JSON(STATE)", sql);
    }

    [Fact]
    public void PGlite_Tohex_Uses_ToHex()
    {
        var sql = _converter.Convert("print x = tohex(255)");
        Assert.Contains("TO_HEX(255::bigint)", sql);
    }

    [Fact]
    public void PGlite_BagRemoveKeys_Uses_JsonbMinusArray()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("bag_remove_keys", new[] { "mybag", "'key1'", "'key2'" });
        Assert.Contains("mybag::jsonb", result);
        Assert.Contains("ARRAY[", result);
        Assert.Contains("::text[]", result);
    }

    // --- Timezone function tests ---

    [Fact]
    public void PGlite_DatetimeLocalToUtc_Uses_AtTimeZone()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("datetime_local_to_utc", new[] { "dt", "'US/Pacific'" });
        Assert.Contains("AT TIME ZONE 'US/Pacific'", result);
        Assert.Contains("AT TIME ZONE 'UTC'", result);
    }

    [Fact]
    public void PGlite_DatetimeLocalToUtc_SingleArg_Uses_UtcTimeZone()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("datetime_local_to_utc", new[] { "dt" });
        Assert.Equal("(dt AT TIME ZONE 'UTC')", result);
    }

    [Fact]
    public void PGlite_DatetimeUtcToLocal_Uses_AtTimeZone()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("datetime_utc_to_local", new[] { "dt", "'Europe/Berlin'" });
        Assert.Contains("AT TIME ZONE 'UTC'", result);
        Assert.Contains("AT TIME ZONE 'Europe/Berlin'", result);
    }

    [Fact]
    public void PGlite_DatetimeUtcToLocal_SingleArg_Returns_Passthrough()
    {
        var dialect = new PGliteDialect();
        var result = dialect.TryTranslateFunction("datetime_utc_to_local", new[] { "dt" });
        Assert.Equal("dt", result);
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
