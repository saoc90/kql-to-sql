// KQL-to-SQL WASM bridge via .NET [JSExport]
// Bootstraps the Mono WASM runtime and exposes translation functions.

let exports = null;
let initPromise = null;

export async function initialize() {
    if (exports) return;
    if (initPromise) return initPromise;

    initPromise = (async () => {
        try {
            // dotnet.js is produced by `dotnet publish` and placed in _framework/
            const { dotnet } = await import('../_framework/dotnet.js');
            const runtime = await dotnet.create();
            exports = await runtime.getAssemblyExports('KqlWasmBridge');
            console.log('[KqlBridge] .NET WASM runtime ready');
        } catch (err) {
            console.error('[KqlBridge] Failed to initialize .NET WASM:', err);
            throw err;
        }
    })();

    return initPromise;
}

export function translateKqlToSql(kql, dialect) {
    if (!exports) throw new Error('KqlBridge not initialized');
    const raw = exports.KqlWasmBridge.KqlBridge.TranslateKqlToSql(kql, dialect);
    return JSON.parse(raw);
}

export function validateKql(kql) {
    if (!exports) throw new Error('KqlBridge not initialized');
    const raw = exports.KqlWasmBridge.KqlBridge.ValidateKql(kql);
    return JSON.parse(raw);
}

export function isReady() {
    return exports !== null;
}

// Expose on globalThis for non-module scripts
globalThis.KqlBridge = { initialize, translateKqlToSql, validateKql, isReady };
