using System.Linq;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ProjectReorderOperatorTests
{
    [Fact]
    public void Converts_ProjectReorder()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project-reorder EVENT_TYPE, STATE | take 1";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT EVENT_TYPE, STATE, * EXCLUDE (EVENT_TYPE, STATE) FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        Assert.Equal("EVENT_TYPE", columns[0]);
        Assert.Equal("STATE", columns[1]);
    }
}

