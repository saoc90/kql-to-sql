using System;
using System.Linq;
using System.Reflection;
using Kusto.Language;
using Kusto.Language.Syntax;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class FunctionDeclarationDebugTests
{
    [Fact(Skip = "Debug helper test")] 
    public void Debug_FunctionDeclaration_Properties()
    {
        var kql = @"let T_view = view () { StormEvents | where STATE == 'TEXAS' };";

        var code = KustoCode.Parse(kql);
        var letStatement = code.Syntax.GetDescendants<LetStatement>().First();
        var funcDecl = letStatement.Expression as FunctionDeclaration;

        Assert.NotNull(funcDecl);

        var viewKeyword = funcDecl.ViewKeyword.ToString();
        var bodyType = funcDecl.Body?.GetType().Name;
        var bodyContent = funcDecl.Body?.ToString();

        // Force the test to fail to see properties
        throw new Exception($"ViewKeyword: '{viewKeyword}', Body type: {bodyType}, Body: '{bodyContent}'");
    }
}
