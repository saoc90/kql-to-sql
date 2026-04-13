using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class ColumnManagementCommandTests
{
    [Fact]
    public void Translates_Alter_Column_Type()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".alter column MyTable.Score type=real";
        var sql = converter.Convert(kql);
        Assert.Equal("ALTER TABLE MyTable ALTER COLUMN Score TYPE DOUBLE", sql);
    }

    [Fact]
    public void Translates_Alter_Column_Type_And_Executes()
    {
        var converter = new KqlToSqlConverter();

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS AlterColTest; CREATE TABLE AlterColTest (Id INT, Score INT);";
        cmd.ExecuteNonQuery();

        var kql = ".alter column AlterColTest.Score type=real";
        var sql = converter.Convert(kql);
        Assert.Equal("ALTER TABLE AlterColTest ALTER COLUMN Score TYPE DOUBLE", sql);
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO AlterColTest VALUES (1, 3.14);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT Score FROM AlterColTest WHERE Id = 1;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetDouble(0) > 3.0);
    }

    [Fact]
    public void Translates_Drop_Column()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".drop column MyTable.OldCol";
        var sql = converter.Convert(kql);
        Assert.Equal("ALTER TABLE MyTable DROP COLUMN OldCol", sql);
    }

    [Fact]
    public void Translates_Drop_Column_Ifexists()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".drop column MyTable.OldCol ifexists";
        var sql = converter.Convert(kql);
        Assert.Equal("ALTER TABLE MyTable DROP COLUMN OldCol", sql);
    }

    [Fact]
    public void Translates_Drop_Column_And_Executes()
    {
        var converter = new KqlToSqlConverter();

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS DropColTest; CREATE TABLE DropColTest (Id INT, Name VARCHAR, Extra VARCHAR);";
        cmd.ExecuteNonQuery();

        var kql = ".drop column DropColTest.Extra";
        var sql = converter.Convert(kql);
        Assert.Equal("ALTER TABLE DropColTest DROP COLUMN Extra", sql);
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO DropColTest VALUES (1, 'foo');";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT Name FROM DropColTest WHERE Id = 1;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("foo", reader.GetString(0));
    }

    [Fact]
    public void Translates_Drop_Table_Columns()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".drop table MyTable columns (C1, C2, C3)";
        var sql = converter.Convert(kql);
        Assert.Equal("ALTER TABLE MyTable DROP COLUMN C1; ALTER TABLE MyTable DROP COLUMN C2; ALTER TABLE MyTable DROP COLUMN C3", sql);
    }

    [Fact]
    public void Translates_Drop_Table_Columns_And_Executes()
    {
        var converter = new KqlToSqlConverter();

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS DropColsTest; CREATE TABLE DropColsTest (Id INT, A VARCHAR, B VARCHAR, C VARCHAR);";
        cmd.ExecuteNonQuery();

        var kql = ".drop table DropColsTest columns (A, B)";
        var sql = converter.Convert(kql);
        Assert.Equal("ALTER TABLE DropColsTest DROP COLUMN A; ALTER TABLE DropColsTest DROP COLUMN B", sql);

        foreach (var stmt in sql.Split(';'))
        {
            cmd.CommandText = stmt.Trim();
            if (cmd.CommandText.Length > 0)
                cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "INSERT INTO DropColsTest VALUES (1, 'x');";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT C FROM DropColsTest WHERE Id = 1;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("x", reader.GetString(0));
    }

    [Fact]
    public void Translates_Rename_Column()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".rename column MyTable.OldName to NewName";
        var sql = converter.Convert(kql);
        Assert.Equal("ALTER TABLE MyTable RENAME COLUMN OldName TO NewName", sql);
    }

    [Fact]
    public void Translates_Rename_Column_And_Executes()
    {
        var converter = new KqlToSqlConverter();

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS RenameColTest; CREATE TABLE RenameColTest (Id INT, OldName VARCHAR);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO RenameColTest VALUES (1, 'hello');";
        cmd.ExecuteNonQuery();

        var kql = ".rename column RenameColTest.OldName to NewName";
        var sql = converter.Convert(kql);
        Assert.Equal("ALTER TABLE RenameColTest RENAME COLUMN OldName TO NewName", sql);
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT NewName FROM RenameColTest WHERE Id = 1;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("hello", reader.GetString(0));
    }
}
