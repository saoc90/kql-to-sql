using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class TableManagementCommandTests
{
    // --- .drop table ---

    [Fact]
    public void Translates_Drop_Table()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop table MyTable");
        Assert.Equal("DROP TABLE MyTable", sql);
    }

    [Fact]
    public void Translates_Drop_Table_IfExists()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop table MyTable ifexists");
        Assert.Equal("DROP TABLE IF EXISTS MyTable", sql);
    }

    [Fact]
    public void Executes_Drop_Table()
    {
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS DropMe (Id INT);";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop table DropMe");
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'DropMe' AND table_schema = 'main';";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0, reader.GetInt32(0));
    }

    // --- .drop tables ---

    [Fact]
    public void Translates_Drop_Tables()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop tables (T1, T2)");
        Assert.Equal("DROP TABLE T1; DROP TABLE T2", sql);
    }

    [Fact]
    public void Translates_Drop_Tables_IfExists()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop tables (T1, T2) ifexists");
        Assert.Equal("DROP TABLE IF EXISTS T1; DROP TABLE IF EXISTS T2", sql);
    }

    [Fact]
    public void Executes_Drop_Tables()
    {
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS DropMultiA (Id INT); CREATE TABLE IF NOT EXISTS DropMultiB (Id INT);";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop tables (DropMultiA, DropMultiB) ifexists");
        foreach (var statement in sql.Split("; "))
        {
            cmd.CommandText = statement;
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('DropMultiA', 'DropMultiB') AND table_schema = 'main';";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0, reader.GetInt32(0));
    }

    // --- .rename table ---

    [Fact]
    public void Translates_Rename_Table()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".rename table OldName to NewName");
        Assert.Equal("ALTER TABLE OldName RENAME TO NewName", sql);
    }

    [Fact]
    public void Executes_Rename_Table()
    {
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS RenameSource; DROP TABLE IF EXISTS RenameTarget; CREATE TABLE RenameSource (Id INT);";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".rename table RenameSource to RenameTarget");
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'RenameTarget' AND table_schema = 'main';";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(0));
    }

    // --- .rename tables ---

    [Fact]
    public void Translates_Rename_Tables()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".rename tables NewA=OldA, NewB=OldB");
        Assert.Equal("ALTER TABLE OldA RENAME TO NewA; ALTER TABLE OldB RENAME TO NewB", sql);
    }

    [Fact]
    public void Executes_Rename_Tables()
    {
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS RenameMultiSrcA; DROP TABLE IF EXISTS RenameMultiSrcB; DROP TABLE IF EXISTS RenameMultiDstA; DROP TABLE IF EXISTS RenameMultiDstB; CREATE TABLE RenameMultiSrcA (Id INT); CREATE TABLE RenameMultiSrcB (Id INT);";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".rename tables RenameMultiDstA=RenameMultiSrcA, RenameMultiDstB=RenameMultiSrcB");
        foreach (var statement in sql.Split("; "))
        {
            cmd.CommandText = statement;
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('RenameMultiDstA', 'RenameMultiDstB') AND table_schema = 'main';";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(0));
    }

    // --- .alter-merge table ---

    [Fact]
    public void Translates_AlterMerge_Table()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".alter-merge table Events (NewCol:string, Score:real)");
        Assert.Equal("ALTER TABLE Events ADD COLUMN NewCol VARCHAR; ALTER TABLE Events ADD COLUMN Score DOUBLE", sql);
    }

    [Fact]
    public void Executes_AlterMerge_Table()
    {
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS AlterMergeTest; CREATE TABLE AlterMergeTest (Id INT);";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".alter-merge table AlterMergeTest (Label:string, Value:int)");
        foreach (var statement in sql.Split("; "))
        {
            cmd.CommandText = statement;
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT column_name FROM information_schema.columns WHERE table_name = 'AlterMergeTest' ORDER BY column_name;";
        using var reader = cmd.ExecuteReader();
        var cols = new System.Collections.Generic.List<string>();
        while (reader.Read()) cols.Add(reader.GetString(0));
        Assert.Contains("Id", cols);
        Assert.Contains("Label", cols);
        Assert.Contains("Value", cols);
    }

    // --- .clear table data ---

    [Fact]
    public void Translates_Clear_Table_Data()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".clear table MyTable data");
        Assert.Equal("TRUNCATE TABLE MyTable", sql);
    }

    [Fact]
    public void Executes_Clear_Table_Data()
    {
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS ClearTest; CREATE TABLE ClearTest (Id INT); INSERT INTO ClearTest VALUES (1), (2);";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".clear table ClearTest data");
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT COUNT(*) FROM ClearTest;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0, reader.GetInt32(0));
    }

    // --- .show tables ---

    [Fact]
    public void Translates_Show_Tables()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show tables");
        Assert.Equal("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'", sql);
    }

    [Fact]
    public void Executes_Show_Tables()
    {
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        var converter = new KqlToSqlConverter();
        cmd.CommandText = converter.Convert(".show tables");
        using var reader = cmd.ExecuteReader();
        var tables = new System.Collections.Generic.List<string>();
        while (reader.Read()) tables.Add(reader.GetString(0));
        Assert.Contains("StormEvents", tables);
    }

    // --- .show table T details ---

    [Fact]
    public void Translates_Show_Table_Details()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show table StormEvents details");
        Assert.Equal("DESCRIBE StormEvents", sql);
    }

    [Fact]
    public void Executes_Show_Table_Details()
    {
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        var converter = new KqlToSqlConverter();
        cmd.CommandText = converter.Convert(".show table StormEvents details");
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }

    // --- .show table T schema as json ---

    [Fact]
    public void Translates_Show_Table_Schema_As_Json()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show table StormEvents schema as json");
        Assert.Equal("DESCRIBE StormEvents", sql);
    }

    // --- .create table T based-on Other ---

    [Fact]
    public void Translates_Create_Table_BasedOn()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".create table NewTable based-on OtherTable");
        Assert.Equal("CREATE TABLE NewTable AS SELECT * FROM OtherTable LIMIT 0", sql);
    }

    [Fact]
    public void Executes_Create_Table_BasedOn()
    {
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS StormEventsClone;";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".create table StormEventsClone based-on StormEvents");
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT COUNT(*) FROM StormEventsClone;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0, reader.GetInt32(0));
    }
}
