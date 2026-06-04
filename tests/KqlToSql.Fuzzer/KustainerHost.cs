namespace KqlToSql.Fuzzer;

/// <summary>
/// Starts (or reuses) a Kustainer container and reports whether it is usable. Designed to NEVER
/// throw: if anything is missing (no engine, machine down, image absent, slow start) it sets
/// <see cref="Available"/> = false and a human-readable <see cref="SkipReason"/> so tests skip
/// rather than fail. The Kusto emulator is HTTP-only, auth-free, default DB "NetDefaultDB".
/// </summary>
public sealed class KustainerHost
{
    public const string Image = "mcr.microsoft.com/azuredataexplorer/kustainer-linux:latest";
    public const string ContainerName = "kql-kustainer";
    public const string DatabaseName = "NetDefaultDB";

    public bool Available { get; private set; }
    public string? SkipReason { get; private set; }
    public string Endpoint { get; private set; } = "http://localhost:8080";

    private bool _startedByUs;
    private ContainerCli? _cli;

    public int Port { get; }

    public KustainerHost()
    {
        Port = int.TryParse(Environment.GetEnvironmentVariable("KQLTOSQL_KUSTAINER_PORT"), out var p) ? p : 8080;
        Endpoint = $"http://localhost:{Port}";
    }

    public async Task EnsureStartedAsync(CancellationToken ct = default)
    {
        // 1. Already answering? Reuse it.
        if (await ProbeAsync(TimeSpan.FromSeconds(3), ct))
        {
            Available = true;
            return;
        }

        // 2. Need a container engine.
        _cli = ContainerCli.Detect();
        if (_cli is null)
        {
            SkipReason = "No container engine (podman/docker) found on PATH.";
            return;
        }

        // 3. On Windows, the podman machine must be running.
        if (_cli.Engine == "podman" && OperatingSystem.IsWindows())
        {
            var machines = _cli.Run("machine list --format {{.Running}}", TimeSpan.FromSeconds(15));
            if (machines.Ok && !machines.StdOut.Contains("true", StringComparison.OrdinalIgnoreCase))
            {
                SkipReason = "podman machine is not running (run 'podman machine start').";
                return;
            }
        }

        // 4. Ensure image present.
        if (!_cli.Run($"image exists {Image}", TimeSpan.FromSeconds(15)).Ok)
        {
            var pull = _cli.Run($"pull --platform linux/amd64 {Image}", TimeSpan.FromMinutes(10));
            if (!pull.Ok)
            {
                SkipReason = $"Failed to pull {Image}: {pull.StdErr.Trim()}";
                return;
            }
        }

        // 5. Start: reuse stopped container, else run fresh.
        var existing = _cli.Run($"ps -a --filter name={ContainerName} --format {{{{.Names}}}}", TimeSpan.FromSeconds(15));
        if (existing.Ok && existing.StdOut.Contains(ContainerName))
        {
            _cli.Run($"start {ContainerName}", TimeSpan.FromSeconds(30));
        }
        else
        {
            var run = _cli.Run($"run --name {ContainerName} -e ACCEPT_EULA=Y -m 4G -d -p {Port}:8080 {Image}", TimeSpan.FromSeconds(60));
            if (!run.Ok)
            {
                SkipReason = $"Failed to start Kustainer: {run.StdErr.Trim()}";
                return;
            }
        }
        _startedByUs = true;

        // 6. Wait for readiness.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(120);
        while (DateTime.UtcNow < deadline)
        {
            if (await ProbeAsync(TimeSpan.FromSeconds(5), ct)) { Available = true; return; }
            await Task.Delay(2000, ct);
        }

        SkipReason = "Kustainer did not become ready within 120s.";
        if (_startedByUs) _cli.Run($"stop {ContainerName}", TimeSpan.FromSeconds(30));
    }

    private async Task<bool> ProbeAsync(TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            using var oracle = new KustoOracle(Endpoint, DatabaseName, timeout);
            var r = await oracle.RunQueryAsync("print probe=1", ct);
            return !r.IsError && r.Rows.Count == 1;
        }
        catch
        {
            return false;
        }
    }

    public void Teardown()
    {
        if (!_startedByUs || _cli is null) return;
        if (Environment.GetEnvironmentVariable("KQLTOSQL_KEEP_KUSTAINER") is "1" or "true") return;
        _cli.Run($"stop {ContainerName}", TimeSpan.FromSeconds(30));
    }

    public KustoOracle CreateOracle() => new(Endpoint, DatabaseName, TimeSpan.FromSeconds(30));
}
