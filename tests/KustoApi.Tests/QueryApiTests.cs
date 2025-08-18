using System.Net.Http.Json;
using System.Text.Json;
using System.Linq;
using Microsoft.AspNetCore.Mvc.Testing;

namespace KustoApi.Tests;

public class QueryApiTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly HttpClient _client;

    public QueryApiTests(WebApplicationFactory<Program> factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task QueryPrintReturnsResult()
    {
        var request = new { db = "StormEvents", csl = "print Test=\"Hello, World!\"" };
        var response = await _client.PostAsJsonAsync("/v2/rest/query", request);
        response.EnsureSuccessStatusCode();

        using var json = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());
        var rows = json.RootElement[1].GetProperty("Rows");
        Assert.Equal("Hello, World!", rows[0][0].GetString());
    }

    [Fact]
    public async Task MetadataReturnsTables()
    {
        var response = await _client.GetAsync("/v1/rest/metadata/StormEvents");
        response.EnsureSuccessStatusCode();

        using var json = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());
        var columns = json.RootElement[1].GetProperty("Columns");
        Assert.Equal("TableName", columns[0].GetProperty("ColumnName").GetString());
    }

    [Fact]
    public async Task MgmtShowTablesReturnsTableList()
    {
        var request = new { db = "StormEvents", csl = ".show tables" };
        var response = await _client.PostAsJsonAsync("/v1/rest/mgmt", request);
        response.EnsureSuccessStatusCode();

        using var json = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());
        var rows = json.RootElement.GetProperty("Tables")[0].GetProperty("Rows");
        var tableNames = rows.EnumerateArray().Select(r => r[0].GetString()).ToList();
        Assert.Contains("StormEvents", tableNames);
    }

    [Fact]
    public async Task MgmtShowTableSchemaReturnsColumns()
    {
        var request = new { db = "StormEvents", csl = ".show table StormEvents schema" };
        var response = await _client.PostAsJsonAsync("/v1/rest/mgmt", request);
        response.EnsureSuccessStatusCode();

        using var json = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());
        var rows = json.RootElement.GetProperty("Tables")[0].GetProperty("Rows");
        Assert.Contains(rows.EnumerateArray(), r => r[0].GetString() == "BEGIN_YEARMONTH");
    }
}

