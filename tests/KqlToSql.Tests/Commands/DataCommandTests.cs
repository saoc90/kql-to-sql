using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class DataCommandTests
{
    [Fact]
    public void Set_Creates_Table_From_Query()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".set KansasEvents <| StormEvents | where State == 'KANSAS'";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE TABLE KansasEvents AS (SELECT * FROM StormEvents WHERE State = 'KANSAS')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS KansasEvents;";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM KansasEvents;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(0) > 0);
    }

    [Fact]
    public void Set_With_Async_Prefix()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".set async MyTable <| StormEvents | where State == 'TEXAS'");
        Assert.Equal("CREATE TABLE MyTable AS (SELECT * FROM StormEvents WHERE State = 'TEXAS')", sql);
    }

    [Fact]
    public void Append_Inserts_Query_Results()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".append KansasEvents <| StormEvents | where State == 'KANSAS'";
        var sql = converter.Convert(kql);
        Assert.Equal("INSERT INTO KansasEvents SELECT * FROM StormEvents WHERE State = 'KANSAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS KansasEvents; CREATE TABLE KansasEvents AS SELECT * FROM StormEvents WHERE State = 'KANSAS';";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM KansasEvents;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // Should be double since we created from query then appended again
        Assert.True(reader.GetInt64(0) > 0);
    }

    [Fact]
    public void Set_Or_Append_Creates_Table_If_Not_Exists()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".set-or-append NewEvents <| StormEvents | where State == 'KANSAS'";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE TABLE IF NOT EXISTS NewEvents AS (SELECT * FROM StormEvents WHERE State = 'KANSAS')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS NewEvents;";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM NewEvents;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(0) > 0);
    }

    [Fact]
    public void Set_Or_Replace_Drops_Then_Creates_Table()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".set-or-replace ReplaceTest <| StormEvents | where State == 'KANSAS'";
        var sql = converter.Convert(kql);
        Assert.Equal("DROP TABLE IF EXISTS ReplaceTest; CREATE TABLE ReplaceTest AS (SELECT * FROM StormEvents WHERE State = 'KANSAS')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS ReplaceTest;";
        cmd.ExecuteNonQuery();
        // Execute both statements
        cmd.CommandText = "CREATE TABLE ReplaceTest AS SELECT * FROM StormEvents WHERE State = 'KANSAS';";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql.Split(';')[0].Trim() + ";";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql.Split(';')[1].Trim() + ";";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM ReplaceTest;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(0) > 0);
    }

    [Fact]
    public void Export_To_Csv()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".export to csv ('/tmp/output.csv') <| StormEvents | where State == 'KANSAS'";
        var sql = converter.Convert(kql);
        Assert.Equal("COPY (SELECT * FROM StormEvents WHERE State = 'KANSAS') TO '/tmp/output.csv' (FORMAT csv)", sql);
    }

    [Fact]
    public void Export_To_Parquet()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".export to parquet ('/tmp/output.parquet') <| StormEvents | where State == 'KANSAS'";
        var sql = converter.Convert(kql);
        Assert.Equal("COPY (SELECT * FROM StormEvents WHERE State = 'KANSAS') TO '/tmp/output.parquet' (FORMAT parquet)", sql);
    }

    [Fact]
    public void Export_To_Json()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".export to json ('/tmp/output.json') <| StormEvents | where State == 'KANSAS'";
        var sql = converter.Convert(kql);
        Assert.Equal("COPY (SELECT * FROM StormEvents WHERE State = 'KANSAS') TO '/tmp/output.json' (FORMAT json)", sql);
    }

    [Fact]
    public void Purge_Table_Records_With_Predicate()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".purge table StormEvents records <| where State == 'KANSAS'";
        var sql = converter.Convert(kql);
        Assert.Equal("DELETE FROM StormEvents WHERE State = 'KANSAS'", sql);
    }

    [Fact]
    public void Delete_Table_Records_With_Predicate()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".delete table StormEvents records <| where State == 'KANSAS'";
        var sql = converter.Convert(kql);
        Assert.Equal("DELETE FROM StormEvents WHERE State = 'KANSAS'", sql);
    }

    [Fact]
    public void Delete_Table_Records_With_Compound_Predicate()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".delete table StormEvents records <| where State == 'KANSAS' and InjuriesDirect > 0";
        var sql = converter.Convert(kql);
        Assert.Equal("DELETE FROM StormEvents WHERE State = 'KANSAS' AND InjuriesDirect > 0", sql);
    }
}
