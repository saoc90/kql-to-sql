using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class SearchOperatorTests
{
    [Fact]
    public void Converts_Search_ColumnSpecific()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | search State == 'TEXAS'";
        var sql = converter.Convert(kql);
        Assert.Contains("WHERE", sql);
        Assert.Contains("State = 'TEXAS'", sql);
    }
}
