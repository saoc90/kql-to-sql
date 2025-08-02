using Xunit;

namespace KqlToSql.Tests.Operators;

public class ViewOperatorTests
{
    [Fact]
    public void Translates_Regular_Let_Statement()
    {
        // Test basic let statement (regular view-like behavior) - should create a non-materialized CTE
        var kql = @"
let ViewData = StormEvents | where State == ""TEXAS"";
ViewData | summarize count() by EventType
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        // Expected: WITH ViewData AS NOT MATERIALIZED (SELECT * FROM StormEvents WHERE State = 'TEXAS') SELECT EventType, COUNT(*) FROM ViewData GROUP BY EventType
        Assert.Contains("AS NOT MATERIALIZED", sql);
        Assert.Contains("WITH ViewData", sql);
    }

    [Fact]
    public void Translates_Regular_Let_With_Multiple_References()
    {
        // Test regular let statement being referenced multiple times - should inline each time
        var kql = @"
let ViewData = StormEvents | project State, EventType;
ViewData | where State == ""FLORIDA""
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        // Should create a non-materialized CTE that gets inlined
        Assert.Contains("AS NOT MATERIALIZED", sql);
        Assert.Contains("WITH ViewData", sql);
    }
}
