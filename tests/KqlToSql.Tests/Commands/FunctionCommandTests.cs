using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class FunctionCommandTests
{
    [Fact]
    public void Show_Functions()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show functions");
        Assert.Equal("SELECT * FROM information_schema.tables WHERE table_type = 'VIEW'", sql);
    }

    [Fact]
    public void Create_Function_Translates_To_Create_View()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".create function KansasStorms() { StormEvents | where State == 'KANSAS' }";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE VIEW KansasStorms AS SELECT * FROM StormEvents WHERE State = 'KANSAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP VIEW IF EXISTS KansasStorms;";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM KansasStorms;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(0) > 0);
    }

    [Fact]
    public void Create_Function_With_Properties()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".create function with (view=true) TexasStorms() { StormEvents | where State == 'TEXAS' }";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE VIEW TexasStorms AS SELECT * FROM StormEvents WHERE State = 'TEXAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP VIEW IF EXISTS TexasStorms;";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM TexasStorms;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(0) > 0);
    }

    [Fact]
    public void Create_Or_Alter_Function_Translates_To_Create_Or_Replace_View()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".create-or-alter function StormCount() { StormEvents | summarize cnt=count() by State }";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE OR REPLACE VIEW StormCount AS SELECT State, COUNT(*) AS cnt FROM StormEvents GROUP BY ALL", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP VIEW IF EXISTS StormCount;";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM StormCount;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(0) > 0);
    }

    [Fact]
    public void Drop_Function_Translates_To_Drop_View()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop function MyView");
        Assert.Equal("DROP VIEW MyView", sql);
    }

    [Fact]
    public void Drop_Function_If_Exists()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop function MyView ifexists");
        Assert.Equal("DROP VIEW IF EXISTS MyView", sql);
    }
}
