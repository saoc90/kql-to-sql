using Microsoft.JSInterop;
using System.Runtime.Versioning;
using System.Text.Json;
using DuckDbDemo.DuckDB;

namespace DuckDbDemo.Services
{
    /// <summary>
    /// Service to handle Monaco Kusto initialization and schema management
    /// </summary>
    [SupportedOSPlatform("browser")]
    public class MonacoKustoService
    {
        private readonly IJSRuntime _jsRuntime;
        private bool _isInitialized = false;
        private string? _currentEditorId = null;

        public MonacoKustoService(IJSRuntime jsRuntime)
        {
            _jsRuntime = jsRuntime;
        }

        /// <summary>
        /// Initialize Monaco Kusto support by configuring the worker environment
        /// </summary>
        public async Task<bool> InitializeKustoSupportAsync()
        {
            if (_isInitialized)
                return true;

            try
            {
                // Check if Monaco is available
                var monacoAvailable = await _jsRuntime.InvokeAsync<bool>("monacoKustoInterop.isMonacoAvailable");
                if (!monacoAvailable)
                {
                    Console.WriteLine("Monaco Editor is not available yet");
                    return false;
                }

                // Initialize the Monaco Kusto environment
                var environmentInitialized = await _jsRuntime.InvokeAsync<bool>("monacoKustoInterop.initializeEnvironment");
                if (!environmentInitialized)
                {
                    Console.WriteLine("Failed to initialize Monaco Kusto environment");
                    return false;
                }

                // Wait for Kusto language to be available
                var kustoInitialized = await _jsRuntime.InvokeAsync<bool>("monacoKustoInterop.waitForKustoLanguage");

                if (kustoInitialized)
                {
                    Console.WriteLine("Kusto language successfully initialized");
                    _isInitialized = true;
                    return true;
                }
                else
                {
                    Console.WriteLine("Kusto language support not available, continuing with basic editor");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to initialize Kusto support: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Set the schema for a Monaco editor instance using Kusto language service
        /// </summary>
        public async Task SetSchemaForEditorAsync(string editorId)
        {
            try
            {
                if (!_isInitialized)
                {
                    var initialized = await InitializeKustoSupportAsync();
                    if (!initialized)
                        return;
                }

                _currentEditorId = editorId;

                // Get the database schema from DuckDB
                var schema = await DuckDbInterop.GetDatabaseSchemaAsync();
                if (schema == null)
                {
                    Console.WriteLine("Failed to get database schema");
                    return;
                }

                var schemaJson = JsonSerializer.Serialize(schema);

                // Set the schema using the dedicated JavaScript function
                var success = await _jsRuntime.InvokeAsync<bool>("monacoKustoInterop.setSchemaForEditor", editorId, schemaJson);
                
                if (success)
                {
                    Console.WriteLine($"Kusto schema updated successfully for editor: {editorId}");
                }
                else
                {
                    Console.WriteLine($"Failed to set Kusto schema for editor: {editorId}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to set Kusto schema for editor {editorId}: {ex.Message}");
            }
        }

        /// <summary>
        /// Update the schema for the currently active editor
        /// </summary>
        public async Task UpdateSchemaForCurrentEditorAsync()
        {
            if (!string.IsNullOrEmpty(_currentEditorId))
            {
                await SetSchemaForEditorAsync(_currentEditorId);
            }
        }

        /// <summary>
        /// Check if Kusto language support is available
        /// </summary>
        public async Task<bool> IsKustoSupportAvailableAsync()
        {
            try
            {
                return await _jsRuntime.InvokeAsync<bool>("monacoKustoInterop.isKustoAvailable");
            }
            catch
            {
                return false;
            }
        }
    }
}
