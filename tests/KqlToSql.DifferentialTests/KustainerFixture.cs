using KqlToSql.Fuzzer;

namespace KqlToSql.DifferentialTests;

/// <summary>
/// Collection fixture that starts/reuses a Kustainer container once for all differential tests.
/// Never throws on missing infrastructure: tests call <see cref="SkipIfUnavailable"/> so they SKIP
/// (rather than fail) when podman/Kustainer is absent, keeping normal CI green.
/// </summary>
public sealed class KustainerFixture : IAsyncLifetime
{
    public KustainerHost Host { get; } = new();
    public bool Available => Host.Available;
    public string SkipReason => Host.SkipReason ?? "Kustainer unavailable";
    public DifferentialRunner? Runner { get; private set; }

    public async Task InitializeAsync()
    {
        await Host.EnsureStartedAsync();
        if (Host.Available)
            Runner = new DifferentialRunner(Host.Endpoint, KustainerHost.DatabaseName);
    }

    public Task DisposeAsync()
    {
        Runner?.Dispose();
        Host.Teardown();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Skip the calling test (not fail) when the container is unavailable. Requires the test to be
    /// a [SkippableFact]/[SkippableTheory] (Xunit.SkippableFact), which the runner honors as a skip.
    /// </summary>
    public void SkipIfUnavailable()
    {
        Skip.IfNot(Available, SkipReason);
    }
}

[CollectionDefinition("Kustainer")]
public class KustainerCollection : ICollectionFixture<KustainerFixture> { }
