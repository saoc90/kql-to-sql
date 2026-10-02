// KQL-to-SQL bridge backed by the Rust translator compiled to WebAssembly (crates/kql-wasm).
// The translator is type-directed, so every call passes the current table schemas of the
// selected backend (read from information_schema by the backend interop module).

import init, { translate, validate } from '../kql-wasm/kql_wasm.js';

let ready = false;
let initPromise = null;

export async function initialize() {
    if (ready) return;
    if (initPromise) return initPromise;
    initPromise = (async () => {
        try {
            await init();
            ready = true;
            console.log('[KqlBridge] Rust WASM translator ready');
        } catch (err) {
            console.error('[KqlBridge] Failed to initialize the WASM translator:', err);
            initPromise = null;
            throw err;
        }
    })();
    return initPromise;
}

// { Table: [{ name, type }] } where type is the engine's SQL type (preferred) or a Kusto type.
async function schemaFor(dialect) {
    const interop = dialect === 'pglite' ? globalThis.PGliteInterop : globalThis.DuckDbInterop;
    if (!interop?.getDatabaseSchema) return {};
    const schema = await interop.getDatabaseSchema();
    const out = {};
    for (const t of schema?.database?.tables ?? []) {
        out[t.name] = t.columns.map(c => ({ name: c.name, type: c.sqlType || c.type }));
    }
    return out;
}

// The translator reports `| render` properties in camelCase (visualization, xColumn, ...); the chart
// renderer uses Kusto's property names (Visualization, XColumn, ...), as the C# bridge returned them.
const RENDER_KEYS = {
    visualization: 'Visualization', title: 'Title', xColumn: 'XColumn', series: 'Series', yColumns: 'YColumns',
    anomalyColumns: 'AnomalyColumns', xTitle: 'XTitle', yTitle: 'YTitle', xAxis: 'XAxis', yAxis: 'YAxis',
    legend: 'Legend', ySplit: 'YSplit', accumulate: 'Accumulate', kind: 'Kind',
    ymin: 'Ymin', ymax: 'Ymax', xmin: 'Xmin', xmax: 'Xmax',
};

function toRenderInfo(render) {
    if (!render) return null;
    const out = {};
    for (const [k, v] of Object.entries(render)) out[RENDER_KEYS[k] ?? k] = v;
    return out;
}

/** Translates KQL to SQL: { success, sql, error, columns, render }. */
export async function translateKqlToSql(kql, dialect) {
    if (!ready) throw new Error('KqlBridge not initialized');
    const schema = await schemaFor(dialect);
    const result = JSON.parse(translate(kql, dialect, JSON.stringify(schema)));
    result.render = toRenderInfo(result.render);
    return result;
}

/** Checks KQL syntax: { success, valid, errors: [{ message, start, length }] }. */
export function validateKql(kql) {
    if (!ready) throw new Error('KqlBridge not initialized');
    return JSON.parse(validate(kql));
}

export function isReady() {
    return ready;
}

// Expose on globalThis for non-module scripts
globalThis.KqlBridge = { initialize, translateKqlToSql, validateKql, isReady };
