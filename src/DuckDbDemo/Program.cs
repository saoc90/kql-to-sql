using DuckDbDemo;
using DuckDbDemo.DuckDB;
using DuckDbDemo.Services;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using System.Runtime.InteropServices.JavaScript;
using MudBlazor.Services;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

builder.Services.AddScoped(sp => new HttpClient { BaseAddress = new Uri(builder.HostEnvironment.BaseAddress) });

// Register MudBlazor services first
builder.Services.AddMudServices();

// Register the file manager service as singleton to persist state across navigation
builder.Services.AddSingleton<IFileManagerService, FileManagerService>();

if (OperatingSystem.IsBrowser())
{
    await JSHost.ImportAsync("DuckDbInterop", "/duckdbInterop.js");
    
    // Register the Monaco Kusto service for handling Kusto language support
    builder.Services.AddScoped<MonacoKustoService>();
}

await builder.Build().RunAsync();
