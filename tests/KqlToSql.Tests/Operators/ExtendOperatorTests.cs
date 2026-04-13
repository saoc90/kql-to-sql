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

    [Fact]
    public void Extend_Same_Name_Replaces_Column()
    {
        // KQL extend with the same column name should replace, not create a duplicate
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | extend X = InjuriesDirect | extend X = X + 100 | project State, X | take 3";
        var sql = converter.Convert(kql);

        // Should use EXCLUDE to drop old X before re-adding
        Assert.Contains("EXCLUDE (X)", sql);
        Assert.Contains("X + 100 AS X", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // X should be InjuriesDirect + 100, not the original InjuriesDirect
        Assert.True(reader.GetInt64(1) >= 100);
    }

    [Fact]
    public void Extend_Same_Name_Triple_Chain()
    {
        // Three chained extends overwriting the same column
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | extend Val = toint(InjuriesDirect) | extend Val = binary_and(Val, 255) | extend Val = Val + 1 | project State, Val | take 3";
        var sql = converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // Val should be (InjuriesDirect & 255) + 1, minimum value is 1
        Assert.True(reader.GetInt64(1) >= 1);
    }

    [Fact]
    public void Extend_Different_Names_Appends()
    {
        // Different names should append normally, no EXCLUDE
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | extend A = 1 | extend B = A + 1 | project A, B | take 1";
        var sql = converter.Convert(kql);

        // Should NOT use EXCLUDE
        Assert.DoesNotContain("EXCLUDE", sql);
        Assert.Contains("1 AS A", sql);
        Assert.Contains("A + 1 AS B", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal(2L, reader.GetInt64(1));
    }
}
