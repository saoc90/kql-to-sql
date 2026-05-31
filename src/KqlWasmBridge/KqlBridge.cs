using System.Runtime.InteropServices.JavaScript;
using System.Runtime.Versioning;
using System.Text.Json;
using System.Text.Json.Serialization;
using KqlToSql;
using KqlToSql.Dialects;
using KqlToSql.Render;
using Kusto.Language;

namespace KqlWasmBridge;

// Source-generated JSON serialization (required for WASM — reflection is disabled).
// RenderInfo MUST be registered or serializing the nested render metadata throws in the browser.
[JsonSerializable(typeof(TranslateResult))]
[JsonSerializable(typeof(ValidateResult))]
[JsonSerializable(typeof(DiagnosticError[]))]
[JsonSerializable(typeof(RenderInfo))]
internal partial class BridgeJsonContext : JsonSerializerContext;

public record TranslateResult(bool success, string? sql, string? error, RenderInfo? render);

public record DiagnosticError(string message, int start, int length);

public record ValidateResult(bool success, bool valid, DiagnosticError[] errors);

[SupportedOSPlatform("browser")]
public static partial class KqlBridge
{
    [JSExport]
    public static string TranslateKqlToSql(string kql, string dialect)
    {
        try
        {
            ISqlDialect sqlDialect = dialect?.ToLowerInvariant() switch
            {
                "pglite" => new PGliteDialect(),
                _ => new DuckDbDialect()
            };

            var converter = new KqlToSqlConverter(sqlDialect);
            var (sql, render) = converter.ConvertWithRender(kql);
            return JsonSerializer.Serialize(new TranslateResult(true, sql, null, render), BridgeJsonContext.Default.TranslateResult);
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new TranslateResult(false, null, ex.Message, null), BridgeJsonContext.Default.TranslateResult);
        }
    }

    [JSExport]
    public static string ValidateKql(string kql)
    {
        try
        {
            var code = KustoCode.Parse(kql);
            var diagnostics = code.GetDiagnostics();
            var errors = diagnostics
                .Where(d => d.Severity == "Error")
                .Select(d => new DiagnosticError(d.Message, d.Start, d.Length))
                .ToArray();

            return JsonSerializer.Serialize(
                new ValidateResult(true, errors.Length == 0, errors),
                BridgeJsonContext.Default.ValidateResult);
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(
                new ValidateResult(false, false, [new DiagnosticError(ex.Message, 0, 0)]),
                BridgeJsonContext.Default.ValidateResult);
        }
    }
}
