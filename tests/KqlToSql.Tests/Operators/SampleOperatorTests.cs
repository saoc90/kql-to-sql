using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class SampleOperatorTests
{
    [Fact]
    public void Converts_Sample()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | sample 5";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM T ORDER BY RANDOM() LIMIT 5", sql);
    }

    [Fact]
    public void Converts_SampleDistinct()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | sample-distinct 3 of State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT DISTINCT State FROM (SELECT * FROM T) ORDER BY RANDOM() LIMIT 3", sql);
    }

    [Fact]
    public void Converts_Sample_AfterFilter()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | where Status == 'active' | sample 10";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM T WHERE Status = 'active' ORDER BY RANDOM() LIMIT 10", sql);
    }
}
