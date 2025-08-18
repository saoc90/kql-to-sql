using Microsoft.AspNetCore.Builder;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Linq;
using System.Net;

namespace KustoApi.Tests;

public class KustoApiServer : IAsyncLifetime
{
    private WebApplication? _app;
    private X509Certificate2? _cert;
    public Uri BaseUri { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        using var rsa = RSA.Create(2048);
        var certRequest = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        certRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        certRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(certRequest.PublicKey, false));
        var san = new SubjectAlternativeNameBuilder();
        san.AddIpAddress(IPAddress.Loopback);
        san.AddDnsName("localhost");
        certRequest.CertificateExtensions.Add(san.Build());
        _cert = certRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));

        using var store = new X509Store(StoreName.Root, StoreLocation.CurrentUser);
        store.Open(OpenFlags.ReadWrite);
        store.Add(_cert);
        store.Close();

        _app = Program.BuildApp(new[] { "--urls", "https://127.0.0.1:0" }, _cert);
        await _app.StartAsync();
        BaseUri = new Uri(_app.Urls.First(u => u.StartsWith("https://")));
    }

    public async Task DisposeAsync()
    {
        if (_app != null)
        {
            await _app.StopAsync();
        }
        if (_cert != null)
        {
            using var store = new X509Store(StoreName.Root, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadWrite);
            store.Remove(_cert);
            store.Close();
            _cert.Dispose();
        }
    }
}
