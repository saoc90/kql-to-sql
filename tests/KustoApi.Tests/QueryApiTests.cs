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

    [Fact]
    public async Task AuthMetadataShapeMatchesExpected()
    {
        var response = await _client.GetAsync("/v1/rest/auth/metadata");
        response.EnsureSuccessStatusCode();

        using var json = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());
        var root = json.RootElement;

        var azureAd = root.GetProperty("AzureAD");
        Assert.Equal("https://login.microsoftonline.com", azureAd.GetProperty("LoginEndpoint").GetString());
        Assert.False(azureAd.GetProperty("LoginMfaRequired").GetBoolean());
        Assert.Equal("db662dc1-0cfe-4e1c-a843-19a68e65be58", azureAd.GetProperty("KustoClientAppId").GetString());
        Assert.Equal("http://localhost", azureAd.GetProperty("KustoClientRedirectUri").GetString());
        Assert.Equal("https://kusto.kusto.windows.net", azureAd.GetProperty("KustoServiceResourceId").GetString());
        Assert.Equal("https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a", azureAd.GetProperty("FirstPartyAuthorityUrl").GetString());

        var dsts = root.GetProperty("dSTS");
        Assert.Equal("windows.net", dsts.GetProperty("CloudEndpointSuffix").GetString());
        Assert.Equal("realm://dsts.core.windows.net", dsts.GetProperty("DstsRealm").GetString());
        Assert.Equal("prod-dsts.dsts.core.windows.net", dsts.GetProperty("DstsInstance").GetString());
        Assert.Equal("kusto.windows.net", dsts.GetProperty("KustoDnsHostName").GetString());
        Assert.Equal("kusto", dsts.GetProperty("ServiceName").GetString());
        Assert.Equal("4d248be5-f7bb-4cb0-95b6-36fb9e4f97a8", dsts.GetProperty("KustoDstsServiceId").GetString());
        Assert.Equal("https://prod-passive-dsts.dsts.core.windows.net/dstsv2/7a433bfc-2514-4697-b467-e0933190487f", dsts.GetProperty("DstsJWTAuthorityAddress").GetString());

        var azureSettings = root.GetProperty("AzureSettings");
        Assert.Equal("PublicCloud", azureSettings.GetProperty("CloudName").GetString());
        Assert.Equal("West Europe", azureSettings.GetProperty("AzureRegion").GetString());
        Assert.Equal("External", azureSettings.GetProperty("Classification").GetString());
    }

    [Fact]
    public async Task MgmtShowDatabasesAsJson_ReturnsSchema()
    {
        var request = new { db = "StormEvents", csl = ".show databases as json" };
        var response = await _client.PostAsJsonAsync("/v1/rest/mgmt", request);
        response.EnsureSuccessStatusCode();

        using var json = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());
        var databases = json.RootElement.GetProperty("Databases");
        Assert.True(databases.TryGetProperty("StormEvents", out var db));
        Assert.Equal("StormEvents", db.GetProperty("Name").GetString());
        var tables = db.GetProperty("Tables");
        Assert.True(tables.TryGetProperty("StormEvents", out var table));
        Assert.Equal("StormEvents", table.GetProperty("Name").GetString());
        var orderedColumns = table.GetProperty("OrderedColumns");
        Assert.True(orderedColumns.GetArrayLength() > 0);
        var firstCol = orderedColumns[0];
        Assert.True(firstCol.TryGetProperty("Name", out _));
        Assert.True(firstCol.TryGetProperty("Type", out _));
        Assert.True(firstCol.TryGetProperty("CslType", out _));
    }
}
