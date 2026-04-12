using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ParseKvOperatorTests
{
    [Fact]
    public void Converts_ParseKv_Basic()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | parse-kv Text as (key1:string, key2:long)";
        var sql = converter.Convert(kql);
        Assert.Contains("REGEXP_EXTRACT(Text,", sql);
        Assert.Contains("AS key1", sql);
        Assert.Contains("AS key2", sql);
    }
}
