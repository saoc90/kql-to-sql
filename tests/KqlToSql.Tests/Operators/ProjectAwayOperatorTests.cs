using System.Linq;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ProjectAwayOperatorTests
{
    [Fact]
    public void Converts_ProjectAway()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project-away EPISODE_ID, EVENT_ID | take 1";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * EXCLUDE (EPISODE_ID, EVENT_ID) FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        Assert.DoesNotContain("EPISODE_ID", columns);
        Assert.DoesNotContain("EVENT_ID", columns);
    }

    [Fact]
    public void ProjectAway_SingleColumn()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project-away STATE | take 1";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * EXCLUDE (STATE) FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        Assert.DoesNotContain("STATE", columns);
    }
}
