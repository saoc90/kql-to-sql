using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace KqlToSql.Tests;

internal static class DuckDbSetup
{
    [ModuleInitializer]
    public static void Init()
    {
        try
        {
            EnsureDuckDb();
        }
        catch
        {
            // allow tests that don't require DuckDB to run even if download fails
        }
    }

    internal static void EnsureDuckDb()
    {
        const string url = "https://github.com/duckdb/duckdb/releases/download/v1.3.2/libduckdb-linux-amd64.zip";
        var libPath = Path.Combine(AppContext.BaseDirectory, "libduckdb.so");
        if (!File.Exists(libPath))
        {
            using var client = new HttpClient();
            using var stream = client.GetStreamAsync(url).Result;
            using var archive = new ZipArchive(stream);
            archive.GetEntry("libduckdb.so")!.ExtractToFile(libPath);
        }
        NativeLibrary.Load(libPath);
    }
}
