using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class AsOperatorTests
{
    [Fact]
    public void Converts_As_PassThrough()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | where State == 'TEXAS' | as myTable";
        var sql = converter.Convert(kql);
        // `| as name` now registers the pipeline as a CTE so later references work.
        Assert.Contains("myTable AS NOT MATERIALIZED (SELECT * FROM T WHERE State = 'TEXAS')", sql);
    }
}
