using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ParseOperatorTests
{
    [Fact]
    public void Converts_Parse_Simple()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | parse Name with * 'hello' rest";
        var sql = converter.Convert(kql);
        Assert.Contains("REGEXP_EXTRACT(Name,", sql);
        Assert.Contains("AS rest", sql);
    }

    [Fact]
    public void Converts_Parse_WithTypedCaptures()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | parse Text with * 'key=' val:long ',' rest:string";
        var sql = converter.Convert(kql);
        Assert.Contains("REGEXP_EXTRACT(Text,", sql);
        Assert.Contains("AS val", sql);
        Assert.Contains("AS rest", sql);
        Assert.Contains("CAST(", sql);
        Assert.Contains("BIGINT", sql);
    }

    [Fact]
    public void Converts_Parse_MultipleCaptures()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | parse Text with 'name=' name '&age=' age:long";
        var sql = converter.Convert(kql);
        Assert.Contains("AS name", sql);
        Assert.Contains("AS age", sql);
    }
}
