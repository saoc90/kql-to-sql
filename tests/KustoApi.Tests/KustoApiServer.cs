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
        certRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, false));
        var eku = new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }; // Server Authentication
        certRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(eku, false));
        var san = new SubjectAlternativeNameBuilder();
        san.AddIpAddress(IPAddress.Loopback);
        san.AddIpAddress(IPAddress.IPv6Loopback);
        san.AddDnsName("localhost");
        certRequest.CertificateExtensions.Add(san.Build());

        // Create ephemeral self-signed, then persist by re-importing as PFX with a persisted key
        using var tempCert = certRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
        var pfxBytes = tempCert.Export(X509ContentType.Pfx, "pwd");
        _cert = new X509Certificate2(pfxBytes, "pwd", X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.Exportable);

        using var store = new X509Store(StoreName.Root, StoreLocation.CurrentUser);
        store.Open(OpenFlags.ReadWrite);
        store.Add(_cert);
        store.Close();

        // Start server on HTTPS so Kusto SDK accepts the channel
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
