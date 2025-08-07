using System;
using Kusto.Language;
using Kusto.Language.Syntax;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class SyntaxAnalysisTests
{
    [Fact(Skip = "Debug helper test")]
    public void Analyze_Let_Statement_Structure()
    {
        var kql = @"let ViewData = StormEvents | where State == ""TEXAS"";
ViewData | summarize count() by EventType";
        
        var code = KustoCode.Parse(kql);
        var root = code.Syntax;
        
        // Let's see what structure we get
        var nodeType = root.GetType().Name;
        var children = root.GetDescendants<SyntaxNode>();
        
        // This will help us understand the structure
        Assert.Fail($"Root type: {nodeType}, Children: {string.Join(", ", children.Take(10).Select(c => c.GetType().Name))}");
    }
}
