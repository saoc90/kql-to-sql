using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace KustoApi.Tests;

public class KustoSdkIntegrationTests : IClassFixture<KustoApiServer>
{
    private readonly Uri _baseUri;

    public KustoSdkIntegrationTests(KustoApiServer server)
    {
        _baseUri = server.BaseUri;
    }

    [Fact]
    public async Task Query_and_control_command_work_with_official_sdk()
    {
        var kcsb = new KustoConnectionStringBuilder(_baseUri.ToString())
            .WithAadUserTokenAuthentication("fake");
        using var queryClient = KustoClientFactory.CreateCslQueryProvider(kcsb);
        using var adminClient = KustoClientFactory.CreateCslAdminProvider(kcsb);

        using var queryReader = await queryClient.ExecuteQueryAsync("StormEvents", "print Test='Hello'", new ClientRequestProperties());
        Assert.True(queryReader.Read());
        Assert.Equal("Hello", queryReader.GetString(0));

        using var mgmtReader = await adminClient.ExecuteControlCommandAsync("StormEvents", ".show tables", new ClientRequestProperties());
        var tables = new List<string>();
        while (mgmtReader.Read())
        {
            tables.Add(mgmtReader.GetString(0));
        }
        Assert.Contains("StormEvents", tables);
    }

    [Fact]
    public async Task Create_table_and_ingest_inline_with_official_sdk()
    {
        var kcsb = new KustoConnectionStringBuilder(_baseUri.ToString())
            .WithAadUserTokenAuthentication("fake");
        using var adminClient = KustoClientFactory.CreateCslAdminProvider(kcsb);
        using var queryClient = KustoClientFactory.CreateCslQueryProvider(kcsb);

        var table = $"IngestTest_{Guid.NewGuid():N}";
        using (await adminClient.ExecuteControlCommandAsync("StormEvents", $".create table {table} (Name:string, Age:int)")) { }

        var ingest = $".ingest inline into table {table} <| John,1\nJane,2";
        using (await adminClient.ExecuteControlCommandAsync("StormEvents", ingest)) { }

        using var reader = await queryClient.ExecuteQueryAsync("StormEvents", table, new ClientRequestProperties());
        var rows = new List<(string Name, int Age)>();
        while (reader.Read())
        {
            rows.Add((reader.GetString(0), Convert.ToInt32(reader.GetValue(1))));
        }

        Assert.Contains(("John", 1), rows);
        Assert.Contains(("Jane", 2), rows);
    }
}
