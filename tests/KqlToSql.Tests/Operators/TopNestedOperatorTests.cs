using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class TopNestedOperatorTests
{
    [Fact]
    public void Converts_TopNested_SingleLevel()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | top-nested 3 of State by count()";
        var sql = converter.Convert(kql);
        Assert.Contains("COUNT(*)", sql);
        Assert.Contains("ROW_NUMBER()", sql);
        Assert.Contains("State", sql);
    }

    [Fact]
    public void Converts_TopNested_TwoLevels()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | top-nested 3 of State by count(), top-nested 2 of EventType by count()";
        var sql = converter.Convert(kql);
        Assert.Contains("State", sql);
        Assert.Contains("EventType", sql);
        Assert.Contains("ROW_NUMBER()", sql);
    }
}
