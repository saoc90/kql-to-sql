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

        // col='X' substitutes at the column_ifexists call; the dialect unwraps the string
        // literal to emit COALESCE(X, '') so DuckDB treats X as a column reference.
        Assert.Contains("COALESCE(X", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT * FROM f", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Nested_Let_Inside_Function_Body_Does_Not_Leak_Into_Outer_CTE_Chain()
    {
        // Repro for the "tbl does not exist" bug: the inner let (tbl_ex) must not be hoisted
        // to query scope before the tabular param (tbl) is bound by the invoke inliner.
        var converter = new KqlToSqlConverter();
        var kql = "let f = (tbl:(*)) { let ex = tbl | project X; ex | count }; datatable(X:int)[1,2,3] | invoke f()";
        var sql = converter.Convert(kql);

        // tbl must appear in the WITH chain before ex (ex depends on tbl)
        var tblIdx  = sql.IndexOf("tbl ", StringComparison.OrdinalIgnoreCase);
        var exIdx   = sql.IndexOf(" ex ", StringComparison.OrdinalIgnoreCase);
        Assert.True(tblIdx >= 0 && exIdx >= 0 && tblIdx < exIdx,
            $"Expected 'tbl' before 'ex' in WITH chain but got: {sql}");
    }
}
