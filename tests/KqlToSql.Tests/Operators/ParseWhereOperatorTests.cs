using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ParseWhereOperatorTests
{
    [Fact]
    public void Converts_ParseWhere_Simple()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | parse-where Name with * 'key=' val:long";
        var sql = converter.Convert(kql);
        Assert.Contains("REGEXP_EXTRACT(Name,", sql);
        Assert.Contains("AS val", sql);
        Assert.Contains("WHERE", sql);
        Assert.Contains("IS NOT NULL", sql);
    }

    [Fact]
    public void Converts_ParseWhere_MultipleCapturesAllFiltered()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | parse-where Text with 'a=' x:long ',b=' y:string";
        var sql = converter.Convert(kql);
        Assert.Contains("AS x", sql);
        Assert.Contains("AS y", sql);
        // Should have IS NOT NULL for each capture
        Assert.Contains("IS NOT NULL AND", sql);
    }
}
