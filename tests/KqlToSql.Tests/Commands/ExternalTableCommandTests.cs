using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class ExternalTableCommandTests
{
    // --- .create external table ---

    [Fact]
    public void Translates_Create_External_Table_Parquet()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".create external table MyData (Id:int, Name:string) kind=storage dataformat=parquet ('https://example.com/data.parquet')";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE VIEW MyData AS SELECT * FROM read_parquet('https://example.com/data.parquet')", sql);
    }

    [Fact]
    public void Translates_Create_External_Table_Csv()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".create external table CsvTable (A:string, B:int) kind=storage dataformat=csv ('https://example.com/data.csv')";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE VIEW CsvTable AS SELECT * FROM read_csv_auto('https://example.com/data.csv')", sql);
    }

    [Fact]
    public void Translates_Create_External_Table_Json()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".create external table JsonTable (A:string) kind=storage dataformat=json ('https://example.com/data.json')";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE VIEW JsonTable AS SELECT * FROM read_json_auto('https://example.com/data.json')", sql);
    }

    [Fact]
    public void Translates_Create_External_Table_Jsonl()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".create external table JsonlTable (A:string) kind=storage dataformat=jsonl ('https://example.com/data.jsonl')";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE VIEW JsonlTable AS SELECT * FROM read_json_auto('https://example.com/data.jsonl')", sql);
    }

    // --- .create-or-alter external table ---

    [Fact]
    public void Translates_CreateOrAlter_External_Table_Parquet()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".create-or-alter external table MyData (Id:int, Name:string) kind=storage dataformat=parquet ('https://example.com/data.parquet')";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE OR REPLACE VIEW MyData AS SELECT * FROM read_parquet('https://example.com/data.parquet')", sql);
    }

    [Fact]
    public void Translates_CreateOrAlter_External_Table_Csv()
    {
        var converter = new KqlToSqlConverter();
        var kql = ".create-or-alter external table CsvTable (A:string) kind=storage dataformat=csv ('/tmp/data.csv')";
        var sql = converter.Convert(kql);
        Assert.Equal("CREATE OR REPLACE VIEW CsvTable AS SELECT * FROM read_csv_auto('/tmp/data.csv')", sql);
    }

    // --- .drop external table ---

    [Fact]
    public void Translates_Drop_External_Table()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop external table MyData");
        Assert.Equal("DROP VIEW MyData", sql);
    }

    [Fact]
    public void Translates_Drop_External_Table_IfExists()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop external table MyData ifexists");
        Assert.Equal("DROP VIEW IF EXISTS MyData", sql);
    }

    // --- .show external tables ---

    [Fact]
    public void Translates_Show_External_Tables()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show external tables");
        Assert.Equal("SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'", sql);
    }

    // --- .show external table T ---

    [Fact]
    public void Translates_Show_External_Table()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".show external table MyData");
        Assert.Equal("DESCRIBE MyData", sql);
    }

    // --- execution tests ---

    [Fact]
    public void Executes_Create_External_Table_Csv()
    {
        var path = System.IO.Path.GetTempFileName() + ".csv";
        System.IO.File.WriteAllText(path, "Id,Name\n1,Alice\n2,Bob\n");

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP VIEW IF EXISTS ExtCsvView;";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var sql = converter.Convert($".create external table ExtCsvView (Id:int, Name:string) kind=storage dataformat=csv ('{path}')");
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT COUNT(*) FROM ExtCsvView;";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(0));
    }

    [Fact]
    public void Executes_Drop_External_Table_IfExists()
    {
        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP VIEW IF EXISTS ExtDropMe;";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var sql = converter.Convert(".drop external table ExtDropMe ifexists");
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery(); // should not throw
    }

    [Fact]
    public void Executes_Show_External_Tables()
    {
        var path = System.IO.Path.GetTempFileName() + ".csv";
        System.IO.File.WriteAllText(path, "Id\n1\n");

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP VIEW IF EXISTS ExtShowView;";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var createSql = converter.Convert($".create external table ExtShowView (Id:int) kind=storage dataformat=csv ('{path}')");
        cmd.CommandText = createSql;
        cmd.ExecuteNonQuery();

        cmd.CommandText = converter.Convert(".show external tables");
        using var reader = cmd.ExecuteReader();
        var views = new System.Collections.Generic.List<string>();
        while (reader.Read()) views.Add(reader.GetString(0));
        Assert.Contains("ExtShowView", views);
    }

    [Fact]
    public void Executes_Show_External_Table()
    {
        var path = System.IO.Path.GetTempFileName() + ".csv";
        System.IO.File.WriteAllText(path, "Id\n1\n");

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP VIEW IF EXISTS ExtDescribeView;";
        cmd.ExecuteNonQuery();

        var converter = new KqlToSqlConverter();
        var createSql = converter.Convert($".create external table ExtDescribeView (Id:int) kind=storage dataformat=csv ('{path}')");
        cmd.CommandText = createSql;
        cmd.ExecuteNonQuery();

        cmd.CommandText = converter.Convert(".show external table ExtDescribeView");
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }
}
