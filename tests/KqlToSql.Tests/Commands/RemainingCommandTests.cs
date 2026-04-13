using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class RemainingCommandTests
{
    // --- .create tables ---

    [Fact]
    public void Translates_Create_Tables()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".create tables T1(Id:int, Name:string), T2(Score:real)");
        Assert.Equal("CREATE TABLE T1 (Id INT, Name VARCHAR); CREATE TABLE T2 (Score DOUBLE)", sql);
    }

    // --- .create-merge table ---

    [Fact]
    public void Translates_Create_Merge_Table()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".create-merge table Events (Id:int, Name:string)");
        Assert.Equal("CREATE TABLE IF NOT EXISTS Events (Id INT, Name VARCHAR); ALTER TABLE Events ADD COLUMN Id INT; ALTER TABLE Events ADD COLUMN Name VARCHAR", sql);
    }

    // --- .alter table T (schema) ---

    [Fact]
    public void Translates_Alter_Table_Schema()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".alter table MyTable (Col1:int, Col2:string)");
        Assert.Equal("DROP TABLE IF EXISTS MyTable; CREATE TABLE MyTable (Col1 INT, Col2 VARCHAR)", sql);
    }

    // --- .alter table T docstring ---

    [Fact]
    public void Translates_Alter_Table_Docstring()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".alter table MyTable docstring \"My description\"");
        Assert.Equal("COMMENT ON TABLE MyTable IS 'My description'", sql);
    }

    // --- .alter column T.C docstring ---

    [Fact]
    public void Translates_Alter_Column_Docstring()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".alter column MyTable.MyCol docstring \"Column description\"");
        Assert.Equal("COMMENT ON COLUMN MyTable.MyCol IS 'Column description'", sql);
    }

    // --- .show version ---

    [Fact]
    public void Translates_Show_Version()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show version");
        Assert.Equal("SELECT version()", sql);
    }

    // --- .alter function ---

    [Fact]
    public void Translates_Alter_Function()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".alter function MyView() { StormEvents | where State == 'KANSAS' }");
        Assert.Equal("CREATE OR REPLACE VIEW MyView AS SELECT * FROM StormEvents WHERE State = 'KANSAS'", sql);
    }

    // --- .show function Name ---

    [Fact]
    public void Translates_Show_Function_Specific()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show function MyView");
        Assert.Equal("DESCRIBE MyView", sql);
    }

    [Fact]
    public void Translates_Show_Functions_List()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show functions");
        Assert.Equal("SELECT * FROM information_schema.tables WHERE table_type = 'VIEW'", sql);
    }

    // --- .alter function F docstring ---

    [Fact]
    public void Translates_Alter_Function_Docstring()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".alter function MyView docstring \"View description\"");
        Assert.Equal("COMMENT ON VIEW MyView IS 'View description'", sql);
    }

    // --- .set stored_query_result ---

    [Fact]
    public void Translates_Set_StoredQueryResult()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".set stored_query_result MyResult <| StormEvents | where State == 'KANSAS'");
        Assert.Equal("CREATE TEMP TABLE MyResult AS (SELECT * FROM StormEvents WHERE State = 'KANSAS')", sql);
    }

    // --- .show stored_query_result ---

    [Fact]
    public void Translates_Show_StoredQueryResult()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show stored_query_result MyResult");
        Assert.Equal("SELECT * FROM MyResult", sql);
    }

    // --- .drop stored_query_result ---

    [Fact]
    public void Translates_Drop_StoredQueryResult()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop stored_query_result MyResult");
        Assert.Equal("DROP TABLE IF EXISTS MyResult", sql);
    }

    // --- .set does not match .set stored_query_result ---

    [Fact]
    public void Set_StoredQueryResult_Does_Not_Interfere_With_Set()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".set MyTable <| StormEvents | take 5");
        Assert.Equal("CREATE TABLE MyTable AS (SELECT * FROM StormEvents LIMIT 5)", sql);
    }
}
