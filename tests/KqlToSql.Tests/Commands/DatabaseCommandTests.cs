using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class DatabaseCommandTests
{
    // --- .create database ---

    [Fact]
    public void Translates_Create_Database()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".create database MyDb");
        Assert.Equal("CREATE SCHEMA MyDb", sql);
    }

    // --- .drop database ---

    [Fact]
    public void Translates_Drop_Database()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop database MyDb");
        Assert.Equal("DROP SCHEMA MyDb", sql);
    }

    [Fact]
    public void Translates_Drop_Database_IfExists()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop database MyDb ifexists");
        Assert.Equal("DROP SCHEMA IF EXISTS MyDb", sql);
    }

    // --- .show databases ---

    [Fact]
    public void Translates_Show_Databases()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show databases");
        Assert.Equal("SELECT schema_name FROM information_schema.schemata", sql);
    }

    // --- .show database Db schema ---

    [Fact]
    public void Translates_Show_Database_Schema()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show database MyDb schema");
        Assert.Equal("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'MyDb'", sql);
    }

    // --- .execute database script ---

    [Fact]
    public void Translates_Execute_Database_Script()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".execute database script <| StormEvents | take 5");
        Assert.Equal("SELECT * FROM StormEvents LIMIT 5", sql);
    }

    [Fact]
    public void Translates_Execute_Database_Script_Multiple_Commands()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".execute database script <| StormEvents | take 5; StormEvents | take 3");
        Assert.Equal("SELECT * FROM StormEvents LIMIT 5; SELECT * FROM StormEvents LIMIT 3", sql);
    }
}
