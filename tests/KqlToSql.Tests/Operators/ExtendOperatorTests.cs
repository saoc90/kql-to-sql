using System.Linq;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ExtendOperatorTests
{
    [Fact]
    public void Converts_Extend_Sort_Take_And_Project()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | extend TotalInjuries = InjuriesDirect + InjuriesIndirect | sort by TotalInjuries desc | take 1 | project State, EventType, TotalInjuries";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT State, EventType, TotalInjuries FROM (SELECT *, InjuriesDirect + InjuriesIndirect AS TotalInjuries FROM StormEvents ORDER BY TotalInjuries DESC LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(1)));
        Assert.True(reader.GetInt64(2) > 0);
    }

    [Fact]
    public void Handles_Multiple_Extends()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | extend Injuries = InjuriesDirect + InjuriesIndirect | extend InjuriesPlusOne = Injuries + 1 | take 1 | project Injuries, InjuriesPlusOne";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT Injuries, InjuriesPlusOne FROM (SELECT *, Injuries + 1 AS InjuriesPlusOne FROM (SELECT *, InjuriesDirect + InjuriesIndirect AS Injuries FROM StormEvents) LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0L, reader.GetInt64(0));
        Assert.Equal(1L, reader.GetInt64(1));
    }
}
