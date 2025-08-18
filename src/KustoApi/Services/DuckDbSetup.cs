using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace KustoApi.Services;

/// <summary>
/// Downloads and loads the native DuckDB library at runtime.
/// </summary>
public static class DuckDbSetup
{
    [ModuleInitializer]
    public static void Init()
    {
        EnsureDuckDb();
    }

    public static void EnsureDuckDb()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            EnsureDuckDbWindows();
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            EnsureDuckDbLinux();
        }
        else
        {
            throw new PlatformNotSupportedException("DuckDB is not supported on this platform.");
        }
    }

    private static void EnsureDuckDbWindows()
    {
        const string url = "https://github.com/duckdb/duckdb/releases/download/v1.3.2/libduckdb-windows-amd64.zip";
        LoadNativeLibrary(url, "duckdb.dll");
    }

    private static void EnsureDuckDbLinux()
    {
        const string url = "https://github.com/duckdb/duckdb/releases/download/v1.3.2/libduckdb-linux-amd64.zip";
        LoadNativeLibrary(url, "libduckdb.so");
    }

    private static void LoadNativeLibrary(string url, string filename)
    {
        var libPath = Path.Combine(AppContext.BaseDirectory, filename);
        if (!File.Exists(libPath))
        {
            using var client = new HttpClient();
            using var stream = client.GetStreamAsync(url).Result;
            using var archive = new ZipArchive(stream);
            archive.GetEntry(filename)!.ExtractToFile(libPath);
        }
        NativeLibrary.Load(libPath);
    }
}

