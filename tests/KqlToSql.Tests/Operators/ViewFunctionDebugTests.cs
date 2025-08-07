using System;
using System.Linq;
using Kusto.Language;
using Kusto.Language.Syntax;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ViewFunctionDebugTests
{
    [Fact(Skip = "Debug helper test")]
    public void Debug_View_Function_Syntax()
    {
        var kql = @"let T_view = view () { StormEvents | where STATE == 'TEXAS' };
T_view";
        
        var code = KustoCode.Parse(kql);
        var root = code.Syntax;
        
        var letStatement = root.GetDescendants<LetStatement>().First();
        var expression = letStatement.Expression;
        
        Assert.Fail($"Let expression type: {expression.GetType().Name}, Expression: {expression.ToString().Trim()}");
    }
}
