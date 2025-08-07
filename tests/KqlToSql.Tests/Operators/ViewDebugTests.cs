using System;
using System.Linq;
using Kusto.Language;
using Kusto.Language.Syntax;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ViewDebugTests
{
    [Fact(Skip = "Debug helper test")]
    public void Debug_View_Statement_Types()
    {
        // Test the view case to see why it's different
        var kql = @"let ViewData = view(StormEvents | where State == ""TEXAS"");
ViewData | summarize count() by EventType";
        
        var code = KustoCode.Parse(kql);
        var root = code.Syntax;
        
        var statements = root.GetDescendants<Statement>().ToList();
        var statementTypes = statements.Select(s => $"{s.GetType().Name}: {s.ToString().Trim().Replace('\n', ' ').Replace('\r', ' ')}").ToList();
        
        // Also check if there are any parsing errors
        var diagnostics = code.GetDiagnostics().Select(d => d.Message).ToList();
        
        Assert.Fail($"Found {statements.Count} statements: [{string.Join("; ", statementTypes)}]. Diagnostics: [{string.Join("; ", diagnostics)}]");
    }
}
