using System.Text.Json;
using Microsoft.JSInterop;

namespace DuckDbDemo.Services
{
    public interface IFileManagerService
    {
        List<UploadedFileInfo> UploadedFiles { get; }
        event Action? StateChanged;
        Task InitializeAsync(DotNetObjectReference<object> dotnetRef);
        Task<object> LoadFileIntoDatabaseAsync(string fileId);
        Task RemoveFileAsync(string fileId);
        Task ClearFilesAsync();
        Task RefreshMetadataAsync();
    }

    public class FileManagerService : IFileManagerService
    {
        private readonly IJSRuntime _jsRuntime;
        private readonly List<UploadedFileInfo> _uploadedFiles = new();

        public FileManagerService(IJSRuntime jsRuntime)
        {
            _jsRuntime = jsRuntime;
        }

        public List<UploadedFileInfo> UploadedFiles => _uploadedFiles;

        public event Action? StateChanged;

        public async Task InitializeAsync(DotNetObjectReference<object> dotnetRef)
        {
            try
            {
                // Initialize the JavaScript file manager
                await _jsRuntime.InvokeVoidAsync("FileManagerInterop.initialize", dotnetRef);
                await RefreshMetadataAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to initialize FileManagerService: {ex.Message}");
            }
        }

        public async Task<object> LoadFileIntoDatabaseAsync(string fileId)
        {
            try
            {
                var result = await _jsRuntime.InvokeAsync<JsonElement>("FileManagerInterop.loadFileIntoDatabase", fileId);
                
                // Update local metadata
                await RefreshMetadataAsync();
                
                return result;
            }
            catch (Exception ex)
            {
                return new { success = false, error = ex.Message };
            }
        }

        public async Task RemoveFileAsync(string fileId)
        {
            try
            {
                await _jsRuntime.InvokeVoidAsync("FileManagerInterop.removeFile", fileId);
                await RefreshMetadataAsync();
                StateChanged?.Invoke();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to remove file: {ex.Message}");
            }
        }

        public async Task ClearFilesAsync()
        {
            try
            {
                await _jsRuntime.InvokeVoidAsync("FileManagerInterop.clearAllFiles");
                _uploadedFiles.Clear();
                StateChanged?.Invoke();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to clear files: {ex.Message}");
            }
        }

        public async Task RefreshMetadataAsync()
        {
            try
            {
                var metadata = await _jsRuntime.InvokeAsync<JsonElement>("FileManagerInterop.getFileMetadata");
                
                _uploadedFiles.Clear();
                
                if (metadata.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in metadata.EnumerateArray())
                    {
                        var fileInfo = new UploadedFileInfo
                        {
                            Id = GetStringProperty(item, "id"),
                            Name = GetStringProperty(item, "name"),
                            Size = GetLongProperty(item, "size"),
                            Type = GetStringProperty(item, "type"),
                            UploadDate = GetDateTimeProperty(item, "uploadDate"),
                            IsLoaded = GetBoolProperty(item, "isLoaded"),
                            HasError = GetBoolProperty(item, "hasError"),
                            TableName = GetStringProperty(item, "tableName"),
                            RowCount = GetIntProperty(item, "rowCount")
                        };
                        
                        _uploadedFiles.Add(fileInfo);
                    }
                }
                
                StateChanged?.Invoke();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to refresh metadata: {ex.Message}");
            }
        }

        private static string GetStringProperty(JsonElement element, string propertyName)
        {
            return element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String 
                ? prop.GetString() ?? string.Empty 
                : string.Empty;
        }

        private static long GetLongProperty(JsonElement element, string propertyName)
        {
            return element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.Number 
                ? prop.GetInt64() 
                : 0;
        }

        private static int GetIntProperty(JsonElement element, string propertyName)
        {
            return element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.Number 
                ? prop.GetInt32() 
                : 0;
        }

        private static bool GetBoolProperty(JsonElement element, string propertyName)
        {
            return element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.True;
        }

        private static DateTime GetDateTimeProperty(JsonElement element, string propertyName)
        {
            if (element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String)
            {
                var dateString = prop.GetString();
                if (DateTime.TryParse(dateString, out var date))
                {
                    return date;
                }
            }
            return DateTime.Now;
        }
    }

    public class UploadedFileInfo
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public long Size { get; set; }
        public string Type { get; set; } = string.Empty;
        public DateTime UploadDate { get; set; }
        public bool IsLoaded { get; set; }
        public bool HasError { get; set; }
        public string? TableName { get; set; }
        public int RowCount { get; set; }
    }
}