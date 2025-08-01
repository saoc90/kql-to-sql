using System.Linq;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ProjectRenameOperatorTests
{
    [Fact]
    public void Converts_ProjectRename()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project-rename StateName = STATE | take 1";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * RENAME (STATE AS StateName) FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        Assert.Contains("StateName", columns);
        Assert.DoesNotContain("STATE", columns);
    }

    [Fact]
    public void Renames_Multiple_Columns()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project-rename S=STATE, E=EVENT_TYPE | take 1";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * RENAME (STATE AS S, EVENT_TYPE AS E) FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        Assert.Contains("S", columns);
        Assert.Contains("E", columns);
        Assert.DoesNotContain("STATE", columns);
        Assert.DoesNotContain("EVENT_TYPE", columns);
    }
}
