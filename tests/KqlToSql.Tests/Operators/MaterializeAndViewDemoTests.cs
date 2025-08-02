using System;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class MaterializeAndViewDemoTests
{
    [Fact]
    public void Demonstrates_Materialize_Translation_To_DuckDB()
    {
        // This test demonstrates how KQL materialize() translates to DuckDB MATERIALIZED CTE
        var kql = @"
let ExpensiveComputation = materialize(
    StormEvents 
    | where State == ""TEXAS""
    | summarize TotalEvents=count(), AvgDamage=avg(DamageProperty) by EventType
);
ExpensiveComputation | where TotalEvents > 100
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        Console.WriteLine("KQL materialize() -> DuckDB MATERIALIZED CTE:");
        Console.WriteLine("Generated SQL: " + sql);
        
        // Verify the key components are present
        Assert.Contains("WITH ExpensiveComputation AS MATERIALIZED", sql);
        Assert.Contains("WHERE State = 'TEXAS'", sql);
        Assert.Contains("GROUP BY EventType", sql);
        Assert.Contains("WHERE TotalEvents > 100", sql);
    }

    [Fact] 
    public void Demonstrates_Regular_Let_Translation_To_DuckDB()
    {
        // This test demonstrates how regular KQL let statements translate to DuckDB NOT MATERIALIZED CTE
        var kql = @"
let TexasEvents = StormEvents | where State == ""TEXAS"" | project EventType, DamageProperty;
let HighDamageEvents = TexasEvents | where DamageProperty > 1000000;
HighDamageEvents | summarize count() by EventType
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        Console.WriteLine("KQL let statements -> DuckDB NOT MATERIALIZED CTE:");
        Console.WriteLine("Generated SQL: " + sql);
        
        // Verify the key components are present  
        Assert.Contains("WITH TexasEvents AS NOT MATERIALIZED", sql);
        Assert.Contains("HighDamageEvents AS NOT MATERIALIZED", sql);
        Assert.Contains("WHERE State = 'TEXAS'", sql);
        Assert.Contains("WHERE DamageProperty > 1000000", sql);
        Assert.Contains("GROUP BY EventType", sql);
    }
}
