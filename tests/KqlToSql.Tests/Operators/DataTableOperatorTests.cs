using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class DataTableOperatorTests
{
    [Fact]
    public void Converts_Simple_DataTable()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("datatable(Name:string, Age:int) ['Alice', 25, 'Bob', 30]");
        Assert.Equal("SELECT * FROM (VALUES ('Alice', 25), ('Bob', 30)) AS t(Name, Age)", sql);
    }

    [Fact]
    public void Converts_DataTable_With_Pipeline()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("datatable(Name:string, Age:int) ['Alice', 25, 'Bob', 30] | where Age > 25");
        Assert.Equal("SELECT * FROM (VALUES ('Alice', 25), ('Bob', 30)) AS t(Name, Age) WHERE Age > 25", sql);
    }

    [Fact]
    public void Converts_DataTable_With_Bool()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("datatable(Name:string, Active:bool) ['Alice', true, 'Bob', false]");
        Assert.Equal("SELECT * FROM (VALUES ('Alice', TRUE), ('Bob', FALSE)) AS t(Name, Active)", sql);
    }

    [Fact]
    public void Converts_DataTable_SingleColumn()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("datatable(x:long) [1, 2, 3]");
        Assert.Equal("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)", sql);
    }

    [Fact]
    public void Converts_DataTable_With_Real()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("datatable(Score:real) [1.5, 2.7, 3.14]");
        Assert.Equal("SELECT * FROM (VALUES (1.5), (2.7), (3.14)) AS t(Score)", sql);
    }

    [Fact]
    public void Converts_DataTable_With_Project()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("datatable(Name:string, Age:int) ['Alice', 25, 'Bob', 30] | project Name");
        Assert.Equal("SELECT Name FROM (VALUES ('Alice', 25), ('Bob', 30)) AS t(Name, Age)", sql);
    }

    [Fact]
    public void Converts_DataTable_With_Count()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("datatable(Name:string, Age:int) ['Alice', 25, 'Bob', 30] | count");
        Assert.Equal("SELECT COUNT(*) AS Count FROM (VALUES ('Alice', 25), ('Bob', 30)) AS t(Name, Age)", sql);
    }
}
