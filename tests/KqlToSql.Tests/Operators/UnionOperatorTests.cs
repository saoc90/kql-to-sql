using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class UnionOperatorTests
{
    [Fact]
    public void Union_CombineTwoQueries()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | where State == \"ALABAMA\" | take 1 | union (StormEvents | where State == \"TEXAS\" | take 1)";
        var sql = converter.Convert(kql);
        Assert.Equal("(SELECT * FROM StormEvents WHERE State = 'ALABAMA' LIMIT 1) UNION ALL (SELECT * FROM StormEvents WHERE State = 'TEXAS' LIMIT 1)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var states = new List<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(reader.GetOrdinal("STATE")));
        }
        Assert.Equal(new[] { "ALABAMA", "TEXAS" }, states);
    }
}
