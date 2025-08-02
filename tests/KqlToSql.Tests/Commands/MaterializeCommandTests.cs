using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class MaterializeCommandTests
{
    [Fact]
    public void Translates_Materialize_Command()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".materialize TexasStorms <| StormEvents | where STATE == 'TEXAS'";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE TABLE TexasStorms AS SELECT * FROM StormEvents WHERE STATE = 'TEXAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS TexasStorms;";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM TexasStorms;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(20L, reader.GetInt64(0));
    }
}
