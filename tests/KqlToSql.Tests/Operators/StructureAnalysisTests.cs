using System;
using System.Linq;
using Kusto.Language;
using Kusto.Language.Syntax;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class StructureAnalysisTests
{
    [Fact]
    public void Analyze_Materialize_Structure()
    {
        var kql = @"let MaterializedData = materialize(StormEvents | where State == ""TEXAS"");
MaterializedData | top 5 by count_";
        
        var code = KustoCode.Parse(kql);
        var root = code.Syntax;
        
        var statements = root.GetDescendants<Statement>().ToList();
        var letStatement = statements.OfType<LetStatement>().FirstOrDefault();
        
        var exprType = letStatement?.Expression?.GetType().Name;
        
        // Find all statements and their types
        var allTypes = statements.Select(s => s.GetType().Name).ToList();
        
        Assert.Fail($"Root: {root.GetType().Name}, Statements: [{string.Join(", ", allTypes)}], Let expression type: {exprType}");
    }
    
    [Fact]
    public void Analyze_View_Structure()
    {
        var kql = @"let ViewData = view(StormEvents | where State == ""TEXAS"");
ViewData | summarize count() by EventType";
        
        var code = KustoCode.Parse(kql);
        var root = code.Syntax;
        
        var statements = root.GetDescendants<Statement>().ToList();
        var letStatement = statements.OfType<LetStatement>().FirstOrDefault();
        
        var exprType = letStatement?.Expression?.GetType().Name;
        
        // Find all statements and their types
        var allTypes = statements.Select(s => s.GetType().Name).ToList();
        
        Assert.Fail($"Root: {root.GetType().Name}, Statements: [{string.Join(", ", allTypes)}], Let expression type: {exprType}");
    }
}
