using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using DuckDB.NET.Data;

namespace KqlToSql.Tests;

internal static class StormEventsDatabase
{
    private static readonly string DbPath = Path.Combine(AppContext.BaseDirectory, "StormEvents.duckdb");
    private static readonly object InitLock = new();

    internal static DuckDBConnection GetConnection()
    {
        DuckDbSetup.EnsureDuckDb();
        EnsureDatabase();
        var conn = new DuckDBConnection($"DataSource={DbPath}");
        conn.Open();
        return conn;
    }

    private static void EnsureDatabase()
    {
        lock (InitLock)
        {
            if (File.Exists(DbPath))
            {
                return;
            }

            const string csvUrl = "https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d1950_c20250520.csv.gz";
            var csvPath = Path.Combine(AppContext.BaseDirectory, "StormEvents1950.csv");
            if (!File.Exists(csvPath))
            {
                using var client = new HttpClient();
                using var stream = client.GetStreamAsync(csvUrl).Result;
                using var gzip = new GZipStream(stream, CompressionMode.Decompress);
                using var file = File.Create(csvPath);
                gzip.CopyTo(file);
            }

            var path = csvPath.Replace("\\", "/");
            using var conn = new DuckDBConnection($"DataSource={DbPath}");
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"CREATE TABLE StormEvents AS SELECT * FROM read_csv_auto('{path}');";
            cmd.ExecuteNonQuery();
        }
    }
}
