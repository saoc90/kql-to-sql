using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class RenderOperatorTests
{
    [Fact]
    public void Converts_Render_PassThrough()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | summarize count() by State | render piechart";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, COUNT(*) AS count FROM T GROUP BY ALL", sql);
    }
}
