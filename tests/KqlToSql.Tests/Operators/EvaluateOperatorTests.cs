using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class EvaluateOperatorTests
{
    [Fact]
    public void Converts_EvaluatePivot_Basic()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | evaluate pivot(State, count())";
        var sql = converter.Convert(kql);
        Assert.Contains("PIVOT", sql);
        Assert.Contains("ON State", sql);
        Assert.Contains("COUNT(*)", sql);
    }

    [Fact]
    public void Converts_EvaluatePivot_WithGroupBy()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | evaluate pivot(State, sum(Value), Name)";
        var sql = converter.Convert(kql);
        Assert.Contains("PIVOT", sql);
        Assert.Contains("ON State", sql);
        Assert.Contains("SUM(Value)", sql);
        Assert.Contains("GROUP BY Name", sql);
    }

    [Fact]
    public void Converts_EvaluateNarrow()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | evaluate narrow()";
        var sql = converter.Convert(kql);
        Assert.Contains("UNPIVOT", sql);
    }

    [Fact]
    public void Converts_EvaluateBagUnpack()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | evaluate bag_unpack(DynCol)";
        var sql = converter.Convert(kql);
        Assert.Contains("UNNEST", sql);
        Assert.Contains("DynCol", sql);
    }

    [Fact]
    public void Throws_ForUnsupportedPlugin()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | evaluate autocluster()";
        Assert.Throws<System.NotSupportedException>(() => converter.Convert(kql));
    }
}
