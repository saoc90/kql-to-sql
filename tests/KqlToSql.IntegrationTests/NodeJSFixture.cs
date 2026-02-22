using Jering.Javascript.NodeJS;
using Microsoft.Extensions.DependencyInjection;

namespace KqlToSql.IntegrationTests;

/// <summary>
/// Shared fixture that provides a configured INodeJSService for invoking
/// DuckDB WASM and PGlite JavaScript modules from .NET integration tests.
/// </summary>
public class NodeJSFixture : IAsyncLifetime
{
    public INodeJSService NodeJS { get; private set; } = null!;
    public string NodeModulesPath { get; private set; } = null!;
    public string ScriptsPath { get; private set; } = null!;
    private ServiceProvider? _serviceProvider;

    public async Task InitializeAsync()
    {
        // Locate node_modules relative to the project source (not bin output)
        var projectDir = FindProjectDirectory();
        NodeModulesPath = Path.Combine(projectDir, "node_modules");
        ScriptsPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "Scripts"));

        if (!Directory.Exists(NodeModulesPath))
        {
            throw new InvalidOperationException(
                $"node_modules not found at {NodeModulesPath}. Run 'npm install' in the integration test project directory.");
        }

        var services = new ServiceCollection();
        services.AddNodeJS();
        _serviceProvider = services.BuildServiceProvider();
        NodeJS = _serviceProvider.GetRequiredService<INodeJSService>();

        await Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_serviceProvider is IAsyncDisposable asyncDisposable)
            await asyncDisposable.DisposeAsync();
        else
            _serviceProvider?.Dispose();
    }

    private static string FindProjectDirectory()
    {
        // Walk up from the bin output directory to find the project root with node_modules
        var dir = AppContext.BaseDirectory;
        while (dir != null)
        {
            if (Directory.Exists(Path.Combine(dir, "node_modules")))
                return dir;
            dir = Path.GetDirectoryName(dir);
        }

        throw new InvalidOperationException(
            "Could not find project directory containing node_modules. Run 'npm install' in the integration test project directory.");
    }
}

[CollectionDefinition("NodeJS")]
public class NodeJSCollection : ICollectionFixture<NodeJSFixture>
{
}
