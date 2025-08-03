using System.Text.Json;
using Microsoft.JSInterop;

namespace DuckDbDemo.Services
{
    public interface IFileManagerService
    {
        List<UploadedFileInfo> UploadedFiles { get; }
        event Action? StateChanged;
        Task InitializeAsync();
        Task AddFileAsync(UploadedFileInfo fileInfo);
        Task RemoveFileAsync(UploadedFileInfo fileInfo);
        Task UpdateFileAsync(UploadedFileInfo fileInfo);
        Task ClearFilesAsync();

        // Legacy synchronous methods for backward compatibility
        void AddFile(UploadedFileInfo fileInfo);
        void RemoveFile(UploadedFileInfo fileInfo);
        void UpdateFile(UploadedFileInfo fileInfo);
        void ClearFiles();
    }

    public class FileManagerService : IFileManagerService
    {
        private readonly IJSRuntime _jsRuntime;
        private readonly List<UploadedFileInfo> _uploadedFiles = new();
        private const string StorageKey = "fileManagerState";

        public FileManagerService(IJSRuntime jsRuntime)
        {
            _jsRuntime = jsRuntime;
        }

        public List<UploadedFileInfo> UploadedFiles => _uploadedFiles;

        public event Action? StateChanged;

        public async Task InitializeAsync()
        {
            try
            {
                // Try to load from browser storage
                var json = await _jsRuntime.InvokeAsync<string>("localStorage.getItem", StorageKey);
                if (!string.IsNullOrEmpty(json))
                {
                    var files = JsonSerializer.Deserialize<List<UploadedFileInfo>>(json);
                    if (files != null)
                    {
                        _uploadedFiles.Clear();
                        _uploadedFiles.AddRange(files);
                        StateChanged?.Invoke();
                    }
                }
            }
            catch
            {
                // If localStorage fails, just continue with empty state
            }
        }

        public async Task AddFileAsync(UploadedFileInfo fileInfo)
        {
            _uploadedFiles.Add(fileInfo);
            await SaveToStorageAsync();
            StateChanged?.Invoke();
        }

        public async Task RemoveFileAsync(UploadedFileInfo fileInfo)
        {
            _uploadedFiles.Remove(fileInfo);
            await SaveToStorageAsync();
            StateChanged?.Invoke();
        }

        public async Task UpdateFileAsync(UploadedFileInfo fileInfo)
        {
            // The file object is already updated by reference, just save and notify
            await SaveToStorageAsync();
            StateChanged?.Invoke();
        }

        public async Task ClearFilesAsync()
        {
            _uploadedFiles.Clear();
            await SaveToStorageAsync();
            StateChanged?.Invoke();
        }

        private async Task SaveToStorageAsync()
        {
            try
            {
                // Create a copy without the large Content arrays for storage
                var filesToStore = _uploadedFiles.Select(f => new UploadedFileInfo
                {
                    Name = f.Name,
                    Size = f.Size,
                    Type = f.Type,
                    UploadDate = f.UploadDate,
                    IsLoaded = f.IsLoaded,
                    HasError = f.HasError,
                    TableName = f.TableName,
                    RowCount = f.RowCount,
                    Content = Array.Empty<byte>() // Don't store large file content
                }).ToList();

                var json = JsonSerializer.Serialize(filesToStore);
                await _jsRuntime.InvokeVoidAsync("localStorage.setItem", StorageKey, json);
            }
            catch
            {
                // If localStorage fails, just continue
            }
        }

        // Legacy synchronous methods for backward compatibility
        public void AddFile(UploadedFileInfo fileInfo) => _ = AddFileAsync(fileInfo);
        public void RemoveFile(UploadedFileInfo fileInfo) => _ = RemoveFileAsync(fileInfo);
        public void UpdateFile(UploadedFileInfo fileInfo) => _ = UpdateFileAsync(fileInfo);
        public void ClearFiles() => _ = ClearFilesAsync();
    }

    public class UploadedFileInfo
    {
        public string Name { get; set; } = string.Empty;
        public long Size { get; set; }
        public string Type { get; set; } = string.Empty;
        public DateTime UploadDate { get; set; }
        public byte[] Content { get; set; } = Array.Empty<byte>();
        public bool IsLoaded { get; set; }
        public bool HasError { get; set; }
        public string? TableName { get; set; }
        public int RowCount { get; set; }
    }
}