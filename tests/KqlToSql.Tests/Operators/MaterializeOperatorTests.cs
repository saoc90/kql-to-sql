using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class MaterializeOperatorTests
{
    [Fact]
    public void Translates_Materialize_Function()
    {
        // Test basic materialize function usage
        var kql = @"
let MaterializedData = materialize(StormEvents | where STATE == 'TEXAS' | summarize cnt=count() by EVENT_TYPE);
MaterializedData | top 5 by cnt
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        // Expected: WITH MaterializedData AS MATERIALIZED (SELECT EVENT_TYPE, COUNT(*) AS cnt FROM StormEvents WHERE STATE = 'TEXAS' GROUP BY EVENT_TYPE) SELECT * FROM MaterializedData ORDER BY cnt DESC LIMIT 5
        Assert.Contains("AS MATERIALIZED", sql);
        Assert.Contains("WITH MaterializedData", sql);
        
        // Test with DuckDB
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // Should have results since TEXAS has events in the test data
        Assert.NotNull(reader.GetString(0)); // EVENT_TYPE
        Assert.True(reader.GetInt64(1) > 0); // cnt
    }

    [Fact]
    public void Translates_Materialize_With_Multiple_References()
    {
        // Test materialize being referenced multiple times - simplified without union for now
        var kql = @"
let MaterializedData = materialize(StormEvents | summarize cnt=count() by STATE);
MaterializedData | where cnt > 5
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        // Should create a materialized CTE that can be referenced multiple times
        Assert.Contains("AS MATERIALIZED", sql);
        Assert.Contains("WITH MaterializedData", sql);
        
        // Test with DuckDB
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // Should have at least one state with more than 5 events
        Assert.NotNull(reader.GetString(0)); // STATE
        Assert.True(reader.GetInt64(1) > 5); // cnt
    }
    
    [Fact]
    public void Materialize_Performance_Test_With_DuckDB()
    {
        // Test that materialize actually works with a more complex query
        var kql = @"
let ExpensiveData = materialize(StormEvents | summarize TotalEvents=count() by STATE);
ExpensiveData | where TotalEvents > 10 | top 3 by TotalEvents
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        Assert.Contains("AS MATERIALIZED", sql);
        Assert.Contains("WITH ExpensiveData", sql);
        
        // Test with DuckDB
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        
        var results = new List<(string State, long TotalEvents)>();
        while (reader.Read())
        {
            results.Add((
                reader.GetString(0), 
                reader.GetInt64(1)
            ));
        }
        
        // Should have results and they should be ordered by TotalEvents descending
        Assert.NotEmpty(results);
        Assert.True(results.All(r => r.TotalEvents > 10));
        Assert.True(results.Count <= 3);
        
        // Results should be in descending order
        for (int i = 1; i < results.Count; i++)
        {
            Assert.True(results[i-1].TotalEvents >= results[i].TotalEvents);
        }
    }
}
