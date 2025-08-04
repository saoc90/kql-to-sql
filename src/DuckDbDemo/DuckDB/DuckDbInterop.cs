using System.Runtime.InteropServices.JavaScript;
using System.Runtime.Versioning;
using System.Text.Json;

namespace DuckDbDemo.DuckDB
{
    /// <remarks>Runs only on WASM, so guard with [SupportedOSPlatform].</remarks>
    [SupportedOSPlatform("browser")]
    internal static partial class DuckDbInterop
    {
        // Marshals the returned JS string straight into C#
        [JSImport("queryJson", "DuckDbInterop")]
        internal static partial Task<string> QueryJsonAsync(string sql);

        // Load StormEvents data from CSV URL - returns JSON string with result
        [JSImport("loadStormEventsFromUrl", "DuckDbInterop")]
        private static partial Task<string> LoadStormEventsFromUrlRawAsync(string csvUrl);

        // Create sample StormEvents data as fallback - returns JSON string with result
        [JSImport("createSampleStormEventsData", "DuckDbInterop")]
        private static partial Task<string> CreateSampleStormEventsDataRawAsync();

        // Upload file to database - returns JSON string with result
        [JSImport("uploadFileToDatabase", "DuckDbInterop")]
        private static partial Task<string> UploadFileToDatabaseRawAsync(string fileName, string fileContent, string fileType);

        // Kusto Monaco Editor support methods will be handled directly in C# using JSRuntime

        [JSImport("getAvailableTables", "DuckDbInterop")]
        private static partial Task<string> GetAvailableTablesRawAsync();

        [JSImport("getDatabaseSchema", "DuckDbInterop")]
        private static partial Task<string> GetDatabaseSchemaRawAsync();

        // Public wrapper methods that handle JSON parsing
        internal static async Task<Dictionary<string, object>?> LoadStormEventsFromUrlAsync(string csvUrl)
        {
            try
            {
                var resultJson = await LoadStormEventsFromUrlRawAsync(csvUrl);
                return JsonSerializer.Deserialize<Dictionary<string, object>>(resultJson);
            }
            catch (Exception ex)
            {
                return new Dictionary<string, object>
                {
                    ["success"] = false,
                    ["error"] = ex.Message,
                    ["message"] = $"Failed to load data: {ex.Message}"
                };
            }
        }

        internal static async Task<Dictionary<string, object>?> CreateSampleStormEventsDataAsync()
        {
            try
            {
                var resultJson = await CreateSampleStormEventsDataRawAsync();
                return JsonSerializer.Deserialize<Dictionary<string, object>>(resultJson);
            }
            catch (Exception ex)
            {
                return new Dictionary<string, object>
                {
                    ["success"] = false,
                    ["error"] = ex.Message,
                    ["message"] = $"Failed to create sample data: {ex.Message}"
                };
            }
        }

        internal static async Task<Dictionary<string, object>?> UploadFileToDatabaseAsync(string fileName, string fileContent, string fileType)
        {
            try
            {
                var resultJson = await UploadFileToDatabaseRawAsync(fileName, fileContent, fileType);
                return JsonSerializer.Deserialize<Dictionary<string, object>>(resultJson);
            }
            catch (Exception ex)
            {
                return new Dictionary<string, object>
                {
                    ["success"] = false,
                    ["error"] = ex.Message,
                    ["message"] = $"Failed to upload file: {ex.Message}"
                };
            }
        }

        internal static async Task<List<string>> GetAvailableTablesAsync()
        {
            try
            {
                var resultJson = await GetAvailableTablesRawAsync();
                var tables = JsonSerializer.Deserialize<List<string>>(resultJson);
                return tables ?? new List<string>();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to get tables: {ex.Message}");
                return new List<string>();
            }
        }

        internal static async Task<Dictionary<string, object>?> GetDatabaseSchemaAsync()
        {
            try
            {
                var resultJson = await GetDatabaseSchemaRawAsync();
                return JsonSerializer.Deserialize<Dictionary<string, object>>(resultJson);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to get database schema: {ex.Message}");
                return null;
            }
        }

        // OPTIONAL – JS can call this to push progress updates
        [JSExport]
        internal static void ReportProgress(int percent)
            => QueryProgressChanged?.Invoke(percent);

        internal static event Action<int>? QueryProgressChanged;
    }
}
