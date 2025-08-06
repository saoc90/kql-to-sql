using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class CreateTableCommandTests
{
    [Fact]
    public void Translates_Create_Table_Command()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".create table TempTable (Id:int, Name:string, EventDate:datetime, Data:dynamic)";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE TABLE TempTable (Id INT, Name VARCHAR, EventDate TIMESTAMP, Data JSON)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS TempTable;";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO TempTable VALUES (1, 'foo', '2020-01-01 00:00:00', '{\"a\":1}');";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT Name FROM TempTable WHERE Id = 1;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("foo", reader.GetString(0));
    }
}
