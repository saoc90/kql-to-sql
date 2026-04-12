using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class SerializeOperatorTests
{
    [Fact]
    public void Converts_Serialize_NoOp()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | serialize";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM T", sql);
    }

    [Fact]
    public void Converts_Serialize_WithRowNumber()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | serialize rn = row_number()";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT *, ROW_NUMBER() OVER () AS rn FROM T", sql);
    }

    [Fact]
    public void Converts_Serialize_AfterSortAndTake()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | sort by Name asc | take 100 | serialize rn = row_number()";
        var sql = converter.Convert(kql);
        // When leftSql has LIMIT, serialize wraps in subquery so ROW_NUMBER runs on limited result
        Assert.Equal("SELECT *, ROW_NUMBER() OVER () AS rn FROM (SELECT * FROM T ORDER BY Name ASC LIMIT 100)", sql);
    }
}
