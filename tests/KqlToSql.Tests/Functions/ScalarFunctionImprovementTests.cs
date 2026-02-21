using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Functions;

public class ScalarFunctionImprovementTests
{
    [Fact]
    public void IsEmpty_MapsToNullOrEmpty()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | where isempty(State)");
        Assert.Equal("SELECT * FROM T WHERE (State IS NULL OR CAST(State AS VARCHAR) = '')", sql);
    }

    [Fact]
    public void IsNotEmpty_MapsToIsNotNull()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | where isnotempty(State)");
        Assert.Equal("SELECT * FROM T WHERE (State IS NOT NULL)", sql);
    }

    [Fact]
    public void IsNull_MapsToIsNull()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | where isnull(State)");
        Assert.Equal("SELECT * FROM T WHERE (State IS NULL)", sql);
    }

    [Fact]
    public void IsNotNull_MapsToIsNotNull()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | where isnotnull(State)");
        Assert.Equal("SELECT * FROM T WHERE (State IS NOT NULL)", sql);
    }

    [Fact]
    public void Not_MapsToNot()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | where not(State == 'TX')");
        Assert.Equal("SELECT * FROM T WHERE NOT (State = 'TX')", sql);
    }

    [Fact]
    public void Strcat_MapsToConcat()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend combined = strcat(State, '-', City)");
        Assert.Equal("SELECT *, CONCAT(State, '-', City) AS combined FROM T", sql);
    }

    [Fact]
    public void ReplaceString_MapsToReplace()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend r = replace_string(State, 'TX', 'Texas')");
        Assert.Equal("SELECT *, REPLACE(State, 'TX', 'Texas') AS r FROM T", sql);
    }

    [Fact]
    public void Trim_MapsToTrim()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend t = trim(' ', State)");
        Assert.Equal("SELECT *, TRIM(State, ' ') AS t FROM T", sql);
    }

    [Fact]
    public void TrimStart_MapsToLtrim()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend t = trim_start(' ', State)");
        Assert.Equal("SELECT *, LTRIM(State, ' ') AS t FROM T", sql);
    }

    [Fact]
    public void TrimEnd_MapsToRtrim()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend t = trim_end(' ', State)");
        Assert.Equal("SELECT *, RTRIM(State, ' ') AS t FROM T", sql);
    }

    [Fact]
    public void Indexof_MapsToInstr()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend idx = indexof(State, 'TX')");
        Assert.Equal("SELECT *, (INSTR(State, 'TX') - 1) AS idx FROM T", sql);
    }

    [Fact]
    public void Coalesce_MapsToCoalesce()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend c = coalesce(State, 'Unknown')");
        Assert.Equal("SELECT *, COALESCE(State, 'Unknown') AS c FROM T", sql);
    }

    [Fact]
    public void Countof_MapsToLengthDifference()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend c = countof(State, 'a')");
        Assert.Equal("SELECT *, (LENGTH(State) - LENGTH(REPLACE(State, 'a', ''))) / LENGTH('a') AS c FROM T", sql);
    }

    [Fact]
    public void Reverse_MapsToReverse()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend r = reverse(State)");
        Assert.Equal("SELECT *, REVERSE(State) AS r FROM T", sql);
    }

    [Fact]
    public void Split_MapsToStringSplit()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend s = split(State, ',')");
        Assert.Equal("SELECT *, STRING_SPLIT(State, ',') AS s FROM T", sql);
    }

    [Fact]
    public void StrcatDelim_MapsToConcatWs()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend s = strcat_delim('-', State, City)");
        Assert.Equal("SELECT *, CONCAT_WS('-', State, City) AS s FROM T", sql);
    }

    [Fact]
    public void Floor_MapsToFloor()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend f = floor(Score)");
        Assert.Equal("SELECT *, FLOOR(Score) AS f FROM T", sql);
    }

    [Fact]
    public void Ceiling_MapsToCeiling()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend c = ceiling(Score)");
        Assert.Equal("SELECT *, CEILING(Score) AS c FROM T", sql);
    }

    [Fact]
    public void Abs_MapsToAbs()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend a = abs(Score)");
        Assert.Equal("SELECT *, ABS(Score) AS a FROM T", sql);
    }

    [Fact]
    public void Round_WithPrecision()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend r = round(Score, 2)");
        Assert.Equal("SELECT *, ROUND(Score, 2) AS r FROM T", sql);
    }

    [Fact]
    public void Round_WithoutPrecision()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend r = round(Score)");
        Assert.Equal("SELECT *, ROUND(Score) AS r FROM T", sql);
    }

    [Fact]
    public void Sqrt_MapsToSqrt()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend s = sqrt(Score)");
        Assert.Equal("SELECT *, SQRT(Score) AS s FROM T", sql);
    }

    [Fact]
    public void Log_MapsToLn()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend l = log(Score)");
        Assert.Equal("SELECT *, LN(Score) AS l FROM T", sql);
    }

    [Fact]
    public void Log10_MapsToLog10()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend l = log10(Score)");
        Assert.Equal("SELECT *, LOG10(Score) AS l FROM T", sql);
    }

    [Fact]
    public void Exp_MapsToExp()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend e = exp(Score)");
        Assert.Equal("SELECT *, EXP(Score) AS e FROM T", sql);
    }

    [Fact]
    public void Pow_MapsToPower()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend p = pow(Score, 2)");
        Assert.Equal("SELECT *, POWER(Score, 2) AS p FROM T", sql);
    }

    [Fact]
    public void Cos_MapsToCos()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend c = cos(Score)");
        Assert.Equal("SELECT *, COS(Score) AS c FROM T", sql);
    }

    [Fact]
    public void Sin_MapsToSin()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend s = sin(Score)");
        Assert.Equal("SELECT *, SIN(Score) AS s FROM T", sql);
    }

    [Fact]
    public void Atan2_MapsToAtan2()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend a = atan2(X, Y)");
        Assert.Equal("SELECT *, ATAN2(X, Y) AS a FROM T", sql);
    }

    [Fact]
    public void Rand_MapsToRandom()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend r = rand()");
        Assert.Equal("SELECT *, RANDOM() AS r FROM T", sql);
    }

    [Fact]
    public void ParseJson_MapsToCast()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend j = parse_json(Data)");
        Assert.Equal("SELECT *, CAST(Data AS JSON) AS j FROM T", sql);
    }

    [Fact]
    public void StartOfDay_MapsToDateTrunc()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend d = startofday(Ts)");
        Assert.Equal("SELECT *, DATE_TRUNC('day', Ts) AS d FROM T", sql);
    }

    [Fact]
    public void StartOfMonth_MapsToDateTrunc()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend d = startofmonth(Ts)");
        Assert.Equal("SELECT *, DATE_TRUNC('month', Ts) AS d FROM T", sql);
    }

    [Fact]
    public void EndOfDay_MapsToDateTruncPlusInterval()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend d = endofday(Ts)");
        Assert.Equal("SELECT *, DATE_TRUNC('day', Ts) + INTERVAL '1 day' - INTERVAL '1 microsecond' AS d FROM T", sql);
    }

    [Fact]
    public void EndOfMonth_MapsToDateTruncPlusInterval()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend d = endofmonth(Ts)");
        Assert.Equal("SELECT *, DATE_TRUNC('month', Ts) + INTERVAL '1 month' - INTERVAL '1 microsecond' AS d FROM T", sql);
    }

    [Fact]
    public void DatetimeAdd_MapsToIntervalAdd()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend d = datetime_add('day', 3, Ts)");
        Assert.Equal("SELECT *, Ts + 3 * INTERVAL '1 day' AS d FROM T", sql);
    }

    [Fact]
    public void DatetimeDiff_MapsToDateDiff()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend d = datetime_diff('day', Ts1, Ts2)");
        Assert.Equal("SELECT *, DATE_DIFF('day', Ts2, Ts1) AS d FROM T", sql);
    }

    [Fact]
    public void MinOf_MapsToLeast()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend m = min_of(A, B, C)");
        Assert.Equal("SELECT *, LEAST(A, B, C) AS m FROM T", sql);
    }

    [Fact]
    public void MaxOf_MapsToGreatest()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend m = max_of(A, B, C)");
        Assert.Equal("SELECT *, GREATEST(A, B, C) AS m FROM T", sql);
    }

    [Fact]
    public void Extract_MapsToRegexpExtract()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend e = extract('[0-9]+', 1, State)");
        Assert.Equal("SELECT *, REGEXP_EXTRACT(State, '[0-9]+', 1) AS e FROM T", sql);
    }

    [Fact]
    public void FormatDatetime_MapsToStrftime()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend f = format_datetime(Ts, 'yyyy-MM-dd')");
        Assert.Equal("SELECT *, STRFTIME(Ts, 'yyyy-MM-dd') AS f FROM T", sql);
    }

    [Fact]
    public void Sign_MapsToSign()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend s = sign(Score)");
        Assert.Equal("SELECT *, SIGN(Score) AS s FROM T", sql);
    }

    [Fact]
    public void Pi_MapsToPi()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("T | extend p = pi()");
        Assert.Equal("SELECT *, PI() AS p FROM T", sql);
    }
}
