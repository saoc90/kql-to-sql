using DuckDB.NET.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace KqlToSql.Tests.Operators;

public class ViewFunctionDeclarationTests
{
    private readonly ITestOutputHelper _output;

    public ViewFunctionDeclarationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void Translates_View_Function_Declaration()
    {
        // Test view function declaration syntax: let T_view = view () { ... };
        var kql = @"
let T_view = view () { StormEvents | where State == 'TEXAS' };
T_view | top 5 by EventType
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        _output.WriteLine($"KQL: {kql}");
        _output.WriteLine($"SQL: {sql}");
        
        // Should create a non-materialized CTE
        Assert.Contains("WITH T_view AS NOT MATERIALIZED", sql);
        Assert.Contains("SELECT * FROM StormEvents WHERE State = 'TEXAS'", sql);
        Assert.Contains("SELECT * FROM T_view ORDER BY EventType DESC LIMIT 5", sql);
    }

    [Fact]
    public void View_Function_Declaration_With_DuckDB_Test()
    {
        // Test view function declaration with actual DuckDB execution
        var kql = @"
let MaterializedData = materialize(StormEvents | where State == 'TEXAS' | summarize cnt=count() by EventType);
let ViewData = view () { StormEvents | where State == 'CALIFORNIA' };
MaterializedData | top 5 by cnt
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        _output.WriteLine($"Generated SQL:\n{sql}");
        
        // Test with DuckDB
        using var connection = StormEventsDatabase.GetConnection();
        
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        
        var results = new List<dynamic>();
        using var reader = command.ExecuteReader();
        
        while (reader.Read())
        {
            results.Add(new
            {
                EventType = reader["EventType"]?.ToString(),
                cnt = reader["cnt"] != DBNull.Value ? (long)reader["cnt"] : 0
            });
        }
        
        // Verify we get results
        Assert.NotEmpty(results);
        _output.WriteLine($"Retrieved {results.Count} rows");
        
        foreach (var result in results.Take(3))
        {
            _output.WriteLine($"EventType: {result.EventType}, cnt: {result.cnt}");
        }
        
        // Should have both CTEs in the SQL
        Assert.Contains("WITH MaterializedData AS MATERIALIZED", sql);
        Assert.Contains("ViewData AS NOT MATERIALIZED", sql);
    }

    [Fact]
    public void Simple_View_Function_Declaration_Test()
    {
        // Test simple view function declaration
        var kql = @"
let SimpleView = view () { StormEvents };
SimpleView | top 3 by EventType
";
        
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(kql);
        
        _output.WriteLine($"KQL: {kql}");
        _output.WriteLine($"SQL: {sql}");
        
        // Should create a non-materialized CTE with simple table reference
        Assert.Contains("WITH SimpleView AS NOT MATERIALIZED", sql);
        Assert.Contains("SELECT * FROM StormEvents", sql);
        Assert.Contains("SELECT * FROM SimpleView ORDER BY EventType DESC LIMIT 3", sql);
    }
}
