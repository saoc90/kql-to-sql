using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace KqlToSql.Fuzzer;

/// <summary>
/// Loads the native DuckDB library at startup. Copied from the pattern in
/// tests/KqlToSql.Tests/DuckDbSetup.cs (can't project-reference a test project).
/// The module initializer runs whenever this assembly is loaded, so both the
/// console app and the DifferentialTests project get a loaded engine for free.
/// </summary>
public static class DuckDbNative
{
    private static readonly object Gate = new();
    private static bool _loaded;

    [ModuleInitializer]
    public static void Init()
    {
        try { Ensure(); }
        catch { /* allow callers that don't need DuckDB to proceed */ }
    }

    public static void Ensure()
    {
        lock (Gate)
        {
            if (_loaded) return;

            (string url, string filename) = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? ("https://github.com/duckdb/duckdb/releases/download/v1.5.0/libduckdb-windows-amd64.zip", "duckdb.dll")
                : RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                    ? ("https://github.com/duckdb/duckdb/releases/download/v1.5.0/libduckdb-linux-amd64.zip", "libduckdb.so")
                    : RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                        ? ("https://github.com/duckdb/duckdb/releases/download/v1.5.0/libduckdb-osx-universal.zip", "libduckdb.dylib")
                        : throw new PlatformNotSupportedException("DuckDB is not supported on this platform.");

            var libPath = Path.Combine(AppContext.BaseDirectory, filename);
            if (!File.Exists(libPath))
            {
                using var client = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };
                using var stream = client.GetStreamAsync(url).Result;
                using var archive = new ZipArchive(stream);
                archive.GetEntry(filename)!.ExtractToFile(libPath);
            }

            NativeLibrary.Load(libPath);
            _loaded = true;
        }
    }
}
