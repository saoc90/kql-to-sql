using System.Collections.Generic;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class DynamicAccessTests
{
    [Fact]
    public void Extracts_Keys_From_Dynamic_Object()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"StormEvents
| extend Metadata = bag_pack('state', State, 'injuries', Injuries_Direct)
| extend StateFromMetadata = Metadata['state'], InjuriesFromMetadata = Metadata.injuries
| project StateFromMetadata, InjuriesFromMetadata
| take 3";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT StateFromMetadata, InjuriesFromMetadata FROM (SELECT *, trim(both '\"' from json_extract(Metadata, '$.state')) AS StateFromMetadata, trim(both '\"' from json_extract(Metadata, '$.injuries')) AS InjuriesFromMetadata FROM (SELECT *, json_object('state', State, 'injuries', Injuries_Direct) AS Metadata FROM StormEvents)) LIMIT 3", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string State, long Injuries)>();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), long.Parse(reader.GetString(1))));
        }

        Assert.Equal(new List<(string, long)>
        {
            ("OKLAHOMA", 0),
            ("TEXAS", 0),
            ("PENNSYLVANIA", 2)
        }, results);
    }
}

