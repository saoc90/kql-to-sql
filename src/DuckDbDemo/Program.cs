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
builder.Services.AddMudServices();

// Register the file manager service as singleton to persist state across navigation
builder.Services.AddSingleton<IFileManagerService, FileManagerService>();

if (OperatingSystem.IsBrowser())
{
    await JSHost.ImportAsync("DuckDbInterop", "/duckdbInterop.js");
}

await builder.Build().RunAsync();
