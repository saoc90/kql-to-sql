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

        // OPTIONAL – JS can call this to push progress updates
        [JSExport]
        internal static void ReportProgress(int percent)
            => QueryProgressChanged?.Invoke(percent);

        internal static event Action<int>? QueryProgressChanged;
    }
}
