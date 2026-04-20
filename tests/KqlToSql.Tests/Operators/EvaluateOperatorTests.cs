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

    [Fact]
    public void Pivot_With_Downstream_column_ifexists_Emits_IN_List()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"datatable(D:string, N:string, V:long)[]
| evaluate pivot(N, sum(V), D)
| extend Cleaning = column_ifexists('Cleaning', long(0)), Production = column_ifexists('Production', long(0))";
        var sql = converter.Convert(kql);
        Assert.Contains("ON N IN (", sql);
        Assert.Contains("'Cleaning'", sql);
        Assert.Contains("'Production'", sql);
    }

    [Fact]
    public void Pivot_Without_Downstream_References_Emits_No_IN_List()
    {
        var converter = new KqlToSqlConverter();
        var kql = "datatable(D:string, N:string, V:long)[] | evaluate pivot(N, sum(V), D)";
        var sql = converter.Convert(kql);
        Assert.Contains("PIVOT", sql);
        Assert.DoesNotContain(" IN (", sql);
    }

    [Fact]
    public void Pivot_With_Self_Referencing_Extend_Emits_IN_List()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"datatable(D:string, N:string, V:real)[]
| evaluate pivot(N, take_any(V))
| extend Foo = toreal(Foo), Bar = toreal(Bar)";
        var sql = converter.Convert(kql);
        Assert.Contains("ON N IN (", sql);
        Assert.Contains("'Foo'", sql);
        Assert.Contains("'Bar'", sql);
    }
}
