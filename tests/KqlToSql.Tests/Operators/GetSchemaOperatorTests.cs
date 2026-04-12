using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class GetSchemaOperatorTests
{
    [Fact]
    public void Converts_GetSchema()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | getschema";
        var sql = converter.Convert(kql);
        Assert.Contains("DESCRIBE", sql);
    }
}
