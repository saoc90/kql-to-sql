using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ConsumeOperatorTests
{
    [Fact]
    public void Converts_Consume_PassThrough()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | where State == 'TEXAS' | consume";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM T WHERE State = 'TEXAS'", sql);
    }
}
