using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class DivideVarcharTests
{
    [Fact]
    public void Trim_Divided_By_Integer_Casts_To_Double()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | extend ms = trim('\"', tostring(Payload.timestamp)) / 1000";
        var sql = converter.Convert(kql);
        Assert.Contains("TRY_CAST(", sql);
        Assert.Contains("AS DOUBLE) / 1000", sql);
    }

    [Fact]
    public void JsonExtract_Divided_By_Integer_Casts_To_Double()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | extend sec = Payload.epoch / 1000";
        var sql = converter.Convert(kql);
        // json_extract path should trigger the cast
        Assert.Contains("/ 1000", sql);
    }

    [Fact]
    public void Summarize_IntervalColumn_Divided_By_Interval_Uses_EpochExtract()
    {
        // countif(...) * 1h produces INTERVAL in summarize; dividing two intervals must use EXTRACT not bare /
        var converter = new KqlToSqlConverter();
        var kql = "let Step = 1h; let Window = 2h; T | summarize OnTime = countif(v > 0) * Step by x | extend TotalTime = Window | extend Ratio = OnTime / TotalTime";
        var sql = converter.Convert(kql);
        Assert.DoesNotContain("OnTime / TotalTime", sql);
        Assert.Contains("EXTRACT(EPOCH FROM", sql);
    }
}
