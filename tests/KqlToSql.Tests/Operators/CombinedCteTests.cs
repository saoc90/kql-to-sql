using Xunit;

namespace KqlToSql.Tests.Operators;

public class CombinedCteTests
{
    [Fact]
    public void Translates_Mixed_Materialized_And_NonMaterialized_CTEs()
    {
        // Test combining both materialized and non-materialized CTEs
        var kql = @"
let ExpensiveData = materialize(StormEvents | where State == ""TEXAS"" | summarize count() by EventType);
let SimpleView = StormEvents | project State, EventType;
ExpensiveData | where count_ > 5
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        // Should have both materialized and non-materialized CTEs
        Assert.Contains("WITH ExpensiveData AS MATERIALIZED", sql);
        Assert.Contains("SimpleView AS NOT MATERIALIZED", sql);
        Assert.Contains("WHERE count_ > 5", sql);
    }

    [Fact]
    public void Translates_Multiple_Regular_Let_Statements()
    {
        // Test multiple regular let statements
        var kql = @"
let TexasEvents = StormEvents | where State == ""TEXAS"";
let TexasCount = TexasEvents | summarize count() by EventType;
TexasCount | where count_ > 10
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        // Should create multiple non-materialized CTEs
        Assert.Contains("WITH TexasEvents AS NOT MATERIALIZED", sql);
        Assert.Contains("TexasCount AS NOT MATERIALIZED", sql);
        Assert.Contains("WHERE count_ > 10", sql);
    }
}
