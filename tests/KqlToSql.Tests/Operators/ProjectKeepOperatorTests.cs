using System.Linq;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ProjectKeepOperatorTests
{
    [Fact]
    public void Converts_ProjectKeep()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project-keep State, EventType | take 1";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, EventType FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        Assert.Equal(new[] { "State", "EventType" }, columns);
    }

    [Fact]
    public void ProjectKeep_SingleColumn()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | project-keep State | take 1";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State FROM StormEvents LIMIT 1", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        Assert.Equal(new[] { "State" }, columns);
    }
}

