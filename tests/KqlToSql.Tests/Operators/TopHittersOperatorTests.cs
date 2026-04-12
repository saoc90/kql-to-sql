using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class TopHittersOperatorTests
{
    [Fact]
    public void Converts_TopHitters_Simple()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | top-hitters 5 of State";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, COUNT(*) AS approximate_count FROM T GROUP BY State ORDER BY approximate_count DESC LIMIT 5", sql);
    }

    [Fact]
    public void Converts_TopHitters_WithByClause()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | top-hitters 5 of State by count()";
        var sql = converter.Convert(kql);
        Assert.Contains("COUNT(*)", sql);
        Assert.Contains("GROUP BY State", sql);
        Assert.Contains("LIMIT 5", sql);
    }
}
