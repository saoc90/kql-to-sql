using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class MatchesRegexTests
{
    [Fact]
    public void Converts_MatchesRegex()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | where Name matches regex 'pat.*'";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM T WHERE REGEXP_MATCHES(Name, 'pat.*')", sql);
    }

    [Fact]
    public void Converts_MatchesRegex_SimplePrefixRewritesToLike()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | where Name matches regex '^Error' and Status == 'active'";
        var sql = converter.Convert(kql);
        // Simple ^prefix pattern is rewritten to LIKE for scan pushdown
        Assert.Equal("SELECT * FROM T WHERE Name LIKE 'Error%' AND Status = 'active'", sql);
    }
}
