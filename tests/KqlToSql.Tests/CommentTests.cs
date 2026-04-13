using KqlToSql;
using Xunit;

namespace KqlToSql.Tests;

public class CommentTests
{
    [Fact]
    public void Converts_Query_With_SingleLine_Comments()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents // table comment
| where State == 'TEXAS' // filter comment
| project EventType";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT EventType FROM StormEvents WHERE State = 'TEXAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
    }

    [Fact]
    public void Converts_Query_With_MultiLine_Comments()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents /* table comment */
| where State == 'TEXAS' /* filter comment */
| project EventType";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT EventType FROM StormEvents WHERE State = 'TEXAS'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
    }

    [Fact]
    public void Converts_Query_With_Leading_Comments()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"//commenthere
StormEvents
| where State != """"
//also here
| count";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) AS Count FROM StormEvents WHERE State <> ''", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.True((long)result! > 0);
    }
}
