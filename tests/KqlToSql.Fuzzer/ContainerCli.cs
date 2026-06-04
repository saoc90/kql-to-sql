using System.Diagnostics;
using System.Text;

namespace KqlToSql.Fuzzer;

public sealed record CliResult(int ExitCode, string StdOut, string StdErr)
{
    public bool Ok => ExitCode == 0;
}

/// <summary>Minimal wrapper around a container CLI (podman, falling back to docker).</summary>
public sealed class ContainerCli
{
    public string Engine { get; }

    private ContainerCli(string engine) => Engine = engine;

    /// <summary>Returns a usable CLI, or null if neither podman nor docker responds.</summary>
    public static ContainerCli? Detect()
    {
        foreach (var engine in new[] { "podman", "docker" })
        {
            try
            {
                var r = RunRaw(engine, "version --format {{.Client.Version}}", TimeSpan.FromSeconds(15));
                if (r.Ok) return new ContainerCli(engine);
            }
            catch { /* try next */ }
        }
        return null;
    }

    public CliResult Run(string args, TimeSpan timeout) => RunRaw(Engine, args, timeout);

    private static CliResult RunRaw(string exe, string args, TimeSpan timeout)
    {
        var psi = new ProcessStartInfo
        {
            FileName = exe,
            Arguments = args,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        using var proc = new Process { StartInfo = psi };
        var outSb = new StringBuilder();
        var errSb = new StringBuilder();
        proc.OutputDataReceived += (_, e) => { if (e.Data != null) outSb.AppendLine(e.Data); };
        proc.ErrorDataReceived += (_, e) => { if (e.Data != null) errSb.AppendLine(e.Data); };

        proc.Start();
        proc.BeginOutputReadLine();
        proc.BeginErrorReadLine();

        if (!proc.WaitForExit((int)timeout.TotalMilliseconds))
        {
            try { proc.Kill(entireProcessTree: true); } catch { }
            return new CliResult(-1, outSb.ToString(), $"timed out after {timeout.TotalSeconds:0}s");
        }
        proc.WaitForExit(); // flush async readers
        return new CliResult(proc.ExitCode, outSb.ToString(), errSb.ToString());
    }
}
