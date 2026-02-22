using System.Text.Json;
using KqlToSql.Dialects;

namespace KqlToSql.IntegrationTests;

/// <summary>
/// Integration tests that convert KQL to SQL using the DuckDB dialect
/// and execute the generated SQL against DuckDB WASM running in Node.js.
/// </summary>
[Collection("NodeJS")]
public class DuckDbWasmIntegrationTests : IAsyncLifetime
{
    private readonly NodeJSFixture _fixture;
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());
    private string _scriptPath = null!;

    public DuckDbWasmIntegrationTests(NodeJSFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        _scriptPath = Path.Combine(_fixture.ScriptsPath, "duckdbWasmRunner.js");

        // Ensure clean test data
        await Exec("DROP TABLE IF EXISTS StormEvents");

        await Exec(@"
            CREATE TABLE StormEvents (
                STATE VARCHAR,
                EVENT_TYPE VARCHAR,
                INJURIES_DIRECT INTEGER,
                DAMAGE_PROPERTY VARCHAR,
                BEGIN_DATE_TIME VARCHAR,
                YEAR INTEGER
            )");

        await Exec(@"
            INSERT INTO StormEvents VALUES
                ('TEXAS', 'Tornado', 5, '10000', '1950-01-03 11:00:00', 1950),
                ('TEXAS', 'Hail', 0, '500', '1950-02-15 14:30:00', 1950),
                ('KANSAS', 'Tornado', 3, '25000', '1950-03-20 09:00:00', 1950),
                ('OKLAHOMA', 'Thunderstorm Wind', 1, '2000', '1950-04-10 16:45:00', 1950),
                ('OKLAHOMA', 'Tornado', 2, '15000', '1950-05-05 12:00:00', 1950)
        ");
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task DuckDb_Wasm_SimpleQuery()
    {
        var rows = await Query("SELECT 1 AS val");
        Assert.Single(rows);
        Assert.Equal(1, rows[0].GetProperty("val").GetInt32());
    }

    [Fact]
    public async Task DuckDb_Wasm_Where_And_Project()
    {
        var kql = "StormEvents | where STATE == 'TEXAS' | project EVENT_TYPE";
        var sql = _converter.Convert(kql);

        Assert.Equal("SELECT EVENT_TYPE FROM StormEvents WHERE STATE = 'TEXAS'", sql);

        var rows = await Query(sql);
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r.GetProperty("EVENT_TYPE").GetString() == "Tornado");
        Assert.Contains(rows, r => r.GetProperty("EVENT_TYPE").GetString() == "Hail");
    }

    [Fact]
    public async Task DuckDb_Wasm_Summarize_Count()
    {
        var kql = "StormEvents | summarize count() by STATE";
        var sql = _converter.Convert(kql);

        Assert.Contains("COUNT(*)", sql);
        Assert.Contains("GROUP BY STATE", sql);

        var rows = await Query(sql);
        Assert.Equal(3, rows.Count);

        var texas = rows.First(r => r.GetProperty("STATE").GetString() == "TEXAS");
        Assert.Equal(2, texas.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task DuckDb_Wasm_Take_Operator()
    {
        var kql = "StormEvents | take 3";
        var sql = _converter.Convert(kql);

        Assert.Contains("LIMIT 3", sql);

        var rows = await Query(sql);
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task DuckDb_Wasm_Sort_Operator()
    {
        var kql = "StormEvents | sort by INJURIES_DIRECT desc | project STATE, INJURIES_DIRECT";
        var sql = _converter.Convert(kql);

        Assert.Contains("ORDER BY INJURIES_DIRECT DESC", sql);

        var rows = await Query(sql);
        Assert.True(rows[0].GetProperty("INJURIES_DIRECT").GetInt32() >=
                     rows[1].GetProperty("INJURIES_DIRECT").GetInt32());
    }

    [Fact]
    public async Task DuckDb_Wasm_Distinct_Operator()
    {
        var kql = "StormEvents | distinct STATE";
        var sql = _converter.Convert(kql);

        var rows = await Query(sql);
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task DuckDb_Wasm_Where_With_Multiple_Conditions()
    {
        var kql = "StormEvents | where STATE == 'TEXAS' and INJURIES_DIRECT > 0 | project EVENT_TYPE";
        var sql = _converter.Convert(kql);

        var rows = await Query(sql);
        Assert.Single(rows);
        Assert.Equal("Tornado", rows[0].GetProperty("EVENT_TYPE").GetString());
    }

    [Fact]
    public async Task DuckDb_Wasm_Extend_Operator()
    {
        var kql = "StormEvents | extend InjuredFlag = INJURIES_DIRECT > 0 | project STATE, InjuredFlag | take 1";
        var sql = _converter.Convert(kql);

        Assert.Contains("INJURIES_DIRECT > 0 AS InjuredFlag", sql);

        var rows = await Query(sql);
        Assert.Single(rows);
    }

    [Fact]
    public async Task DuckDb_Wasm_Count_Operator()
    {
        var kql = "StormEvents | count";
        var sql = _converter.Convert(kql);

        var rows = await Query(sql);
        Assert.Single(rows);
        Assert.Equal(5, rows[0].GetProperty("Count").GetInt32());
    }

    private async Task<List<JsonElement>> Query(string sql)
    {
        var json = await _fixture.NodeJS.InvokeFromFileAsync<string>(
            _scriptPath,
            args: new object[] { "query", _fixture.NodeModulesPath, sql });

        return JsonSerializer.Deserialize<List<JsonElement>>(json!)!;
    }

    private async Task Exec(string sql)
    {
        await _fixture.NodeJS.InvokeFromFileAsync<string>(
            _scriptPath,
            args: new object[] { "exec", _fixture.NodeModulesPath, sql });
    }
}
