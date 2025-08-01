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
        var kql = "StormEvents | extend TotalInjuries = INJURIES_DIRECT + INJURIES_INDIRECT | sort by TotalInjuries desc | take 1 | project STATE, EVENT_TYPE, TotalInjuries";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STATE, EVENT_TYPE, TotalInjuries FROM (SELECT *, INJURIES_DIRECT + INJURIES_INDIRECT AS TotalInjuries FROM (SELECT * FROM StormEvents) ORDER BY TotalInjuries DESC LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("NEBRASKA", reader.GetString(0));
        Assert.Equal("Tornado", reader.GetString(1));
        Assert.Equal(101L, reader.GetInt64(2));
    }

    [Fact]
    public void Handles_Multiple_Extends()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | extend Injuries = INJURIES_DIRECT + INJURIES_INDIRECT | extend InjuriesPlusOne = Injuries + 1 | take 1 | project Injuries, InjuriesPlusOne";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT Injuries, InjuriesPlusOne FROM (SELECT *, Injuries + 1 AS InjuriesPlusOne FROM (SELECT *, INJURIES_DIRECT + INJURIES_INDIRECT AS Injuries FROM (SELECT * FROM StormEvents)) LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0L, reader.GetInt64(0));
        Assert.Equal(1L, reader.GetInt64(1));
    }
}
