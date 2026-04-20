using System;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Functions;

public class TabularUserFunctionTests
{
    [Fact]
    public void Invoke_Simple_Tabular_Function_Inlines_Body()
    {
        var converter = new KqlToSqlConverter();
        var kql = "let f = (t:(*)) { t | project Value=1 }; datatable(X:int)[1,2] | invoke f()";
        var sql = converter.Convert(kql);

        // The body's projection should appear in the SQL — not a bare SELECT * FROM f
        Assert.Contains("Value", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SELECT * FROM f", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Invoke_With_Scalar_Args_Substitutes_Param_References()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"
let f = (t:(*), col:string) { t | extend _c = column_ifexists(col, '') | where _c != '' };
datatable(X:int)[1]
| invoke f('X')";
        var sql = converter.Convert(kql);

        // col must be substituted with the string literal 'X'
        Assert.Contains("'X'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT * FROM f", sql, StringComparison.Ordinal);
    }
}
