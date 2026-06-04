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
        // bag_unpack infers the bag's keys from the dynamic object literals upstream and projects one
        // column per key (alphabetically ordered, as Kusto does), dropping the original bag column.
        var kql = "datatable(d:dynamic)[dynamic({\"a\":1,\"b\":2})] | evaluate bag_unpack(d)";
        var sql = converter.Convert(kql);
        Assert.Contains("EXCLUDE (d)", sql);
        Assert.Contains("d->>'$.a' AS a", sql);
        Assert.Contains("d->>'$.b' AS b", sql);
        Assert.DoesNotContain("'{}'", sql); // no longer emits an empty from_json schema
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
