using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ArgMaxOperatorTests
{
    [Fact]
    public void Converts_ArgMax_By_State()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize arg_max(END_DATE_TIME, EVENT_TYPE) by STATE | sort by STATE";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STATE, arg_max(EVENT_TYPE, END_DATE_TIME) AS EVENT_TYPE FROM StormEvents GROUP BY STATE ORDER BY STATE ASC", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("ALABAMA", reader.GetString(0));
        Assert.Equal("Tornado", reader.GetString(1));
    }

    [Fact]
    public void Converts_ArgMax_With_Wildcard()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize arg_max(END_DATE_TIME, *) by STATE | project STATE, EVENT_TYPE, END_DATE_TIME | where STATE == 'ALABAMA'";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT * FROM (SELECT STATE, EVENT_TYPE, END_DATE_TIME FROM StormEvents QUALIFY ROW_NUMBER() OVER (PARTITION BY STATE ORDER BY END_DATE_TIME DESC) = 1) WHERE STATE = 'ALABAMA'", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("ALABAMA", reader.GetString(0));
        Assert.Equal("Tornado", reader.GetString(1));
        Assert.Equal("18-APR-50 01:45:00", reader.GetString(2));
    }
}
