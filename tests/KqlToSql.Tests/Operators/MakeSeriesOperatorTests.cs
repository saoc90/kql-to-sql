using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class MakeSeriesOperatorTests
{
    [Fact]
    public void Converts_MakeSeries_Basic()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | make-series avg(val) on ts from datetime(2020-01-01) to datetime(2020-12-31) step 1d";
        var sql = converter.Convert(kql);
        Assert.Contains("generate_series", sql);
        Assert.Contains("LEFT JOIN", sql);
        Assert.Contains("AVG", sql);
    }

    [Fact]
    public void Converts_MakeSeries_WithBy()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | make-series avg_val = avg(val) on ts from datetime(2020-01-01) to datetime(2020-12-31) step 1d by State";
        var sql = converter.Convert(kql);
        Assert.Contains("generate_series", sql);
        Assert.Contains("State", sql);
        Assert.Contains("CROSS JOIN", sql);
    }
}
