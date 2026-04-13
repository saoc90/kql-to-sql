using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class ViewCommandTests
{
    [Fact]
    public void Translates_View_Command()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".view StormCount <| StormEvents | summarize event_count=count() by State";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE VIEW StormCount AS SELECT State, COUNT(*) AS event_count FROM StormEvents GROUP BY ALL", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP VIEW IF EXISTS StormCount;";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT event_count FROM StormCount WHERE State = 'KANSAS';";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(0) > 0);
    }
}
