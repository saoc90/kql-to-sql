using System.IO;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class IngestCommandTests
{
    [Fact]
    public void Translates_Inline_Ingest()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".ingest inline into table InlineData <| 1,foo\n2,bar";
        var sql = converter.Convert(kql);
        Assert.Equal("INSERT INTO InlineData VALUES (1, 'foo'), (2, 'bar')", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS InlineData; CREATE TABLE InlineData (Id INT, Name VARCHAR);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM InlineData";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(0));
    }

    [Fact]
    public void Translates_Csv_Ingest()
    {
        var path = Path.GetTempFileName();
        File.WriteAllText(path, "Id,Name\n1,foo\n2,bar\n");
        var converter = new KqlToSqlConverter();
        var kql = $".ingest into table CsvData '{path}'";
        var sql = converter.Convert(kql);
        Assert.Equal($"COPY CsvData FROM '{path}' (HEADER, AUTO_DETECT TRUE)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS CsvData; CREATE TABLE CsvData (Id INT, Name VARCHAR);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM CsvData";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(0));
    }

    [Fact]
    public void Translates_Csv_Ingest_With_Spaces()
    {
        var dir = Path.GetTempPath();
        var path = Path.Combine(dir, "ingest test.csv");
        File.WriteAllText(path, "Id,Name\n1,foo\n2,bar\n");
        var converter = new KqlToSqlConverter();
        var kql = $".ingest into table CsvSpace '{path}'";
        var sql = converter.Convert(kql);
        Assert.Equal($"COPY CsvSpace FROM '{path}' (HEADER, AUTO_DETECT TRUE)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS CsvSpace; CREATE TABLE CsvSpace (Id INT, Name VARCHAR);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM CsvSpace";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(0));
    }
}
