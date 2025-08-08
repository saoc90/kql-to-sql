// PGlite (Postgres WASM) interop for the Blazor demo
// Lightweight implementation to mirror the DuckDB functions used in C# / Razor.
// Uses CDN import; persisted to IndexedDB so data survives page reloads.

import { PGlite } from 'https://cdn.jsdelivr.net/npm/@electric-sql/pglite/dist/index.js'

let pg; // singleton

// Expose for debugging
window.pg = null;

async function init() {
    if (pg) return;
    console.log('🚀 Initializing PGlite (Postgres WASM)...');
    // Persist to IndexedDB (creates db if missing)
    pg = new PGlite('idb://kql-to-sql');
    window.pg = pg;
    console.log('✅ PGlite ready');
    await ensureStormEventsLoaded();
}

// Simple type mapping to Kusto types
function mapPgTypeToKusto(t) {
    if (!t) return 'string';
    const base = t.toLowerCase();
    const map = {
        'text': 'string',
        'varchar': 'string',
        'char': 'string',
        'uuid': 'guid',
        'int2': 'int',
        'int4': 'int',
        'int8': 'long',
        'serial': 'int',
        'bigserial': 'long',
        'float4': 'real',
        'float8': 'real',
        'numeric': 'decimal',
        'bool': 'bool',
        'boolean': 'bool',
        'date': 'datetime',
        'timestamp': 'datetime',
        'timestamptz': 'datetime',
        'time': 'timespan',
        'json': 'dynamic',
        'jsonb': 'dynamic'
    };
    return map[base] || 'string';
}

// Gzip decompression (mirrors approach used in DuckDB interop)
async function decompressGzip(compressedStream) {
    if (typeof DecompressionStream !== 'undefined') {
        const decompressed = compressedStream.pipeThrough(new DecompressionStream('gzip'));
        const resp = new Response(decompressed);
        const buf = await resp.arrayBuffer();
        return new Uint8Array(buf);
    }
    // Fallback: assume already plain
    const resp = new Response(compressedStream);
    const buf = await resp.arrayBuffer();
    return new Uint8Array(buf);
}

// Simple CSV header splitter that respects quotes
function splitCsvHeader(line) {
    const cols = [];
    let cur = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"') {
            // toggle unless escaped
            if (inQuotes && line[i + 1] === '"') { // escaped quote
                cur += '"';
                i++; // skip next
            } else {
                inQuotes = !inQuotes;
            }
        } else if (ch === ',' && !inQuotes) {
            cols.push(cur);
            cur = '';
        } else {
            cur += ch;
        }
    }
    cols.push(cur);
    return cols.map(c => c.trim());
}

function sanitizeIdentifier(name) {
    return name
        .replace(/"/g, '')
        .replace(/[^A-Za-z0-9_]/g, '_')
        .replace(/^([0-9])/, '_$1')
        .toLowerCase();
}

async function ensureStormEventsLoaded() {
    try {
        console.log('🔍 Checking for StormEvents table (PGlite)...');
        const existsRes = await pg.query("SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='stormevents';");
        if (existsRes.rows.length > 0) {
            console.log('✅ StormEvents already present in PGlite');
            return;
        }

        console.log('📥 Fetching StormEvents.csv.gz...');
        const res = await fetch('./StormEvents.csv.gz');
        if (!res.ok) {
            console.warn('⚠️ StormEvents.csv.gz not found, skipping load for PGlite');
            return;
        }
        const compressedArrayBuffer = await res.arrayBuffer();
        const compressedStream = new ReadableStream({
            start(controller) {
                controller.enqueue(new Uint8Array(compressedArrayBuffer));
                controller.close();
            }
        });
        const decompressed = await decompressGzip(compressedStream);
        const text = new TextDecoder('utf-8').decode(decompressed);
        const firstNewline = text.indexOf('\n');
        if (firstNewline === -1) {
            console.warn('⚠️ StormEvents file appears to have no newline, aborting');
            return;
        }
        const headerLine = text.substring(0, firstNewline).replace(/\r$/, '');
        const headers = splitCsvHeader(headerLine);
        if (!headers.length) {
            console.warn('⚠️ Could not parse headers for StormEvents');
            return;
        }
        const sanitized = headers.map(sanitizeIdentifier);
        // Build create table with text columns so queries still work (user can cast later)
        const colsDef = sanitized.map(c => `"${c}" text`).join(', ');
        const createSql = `CREATE TABLE stormevents (${colsDef});`;
        console.log('🛠️ Creating StormEvents table (all columns as text)');
        await pg.exec(createSql);

        // Use COPY with blob
        const blob = new Blob([decompressed], { type: 'text/csv' });
        console.log('📤 Copying CSV into StormEvents via /dev/blob ...');
        await pg.query("COPY stormevents FROM '/dev/blob' WITH (FORMAT csv, HEADER true);", [], { blob });
        const count = await pg.query('SELECT COUNT(*) AS cnt FROM stormevents;');
        console.log(`✅ Loaded StormEvents into PGlite (${count.rows[0].cnt} rows)`);
    } catch (e) {
        console.error('❌ Failed to load StormEvents into PGlite:', e);
    }
}

export async function queryJson(sql) {
    if (!pg) await init();
    try {
        const res = await pg.query(sql);
        return JSON.stringify(res.rows || []);
    } catch (e) {
        console.error('❌ PGlite query failed:', e);
        throw e;
    }
}

export async function getAvailableTables() {
    if (!pg) await init();
    try {
        const res = await pg.query("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename;");
        return JSON.stringify(res.rows.map(r => r.tablename));
    } catch (e) {
        console.warn('⚠️ Failed to list tables (PGlite):', e.message);
        return JSON.stringify([]);
    }
}

export async function getDatabaseSchema() {
    if (!pg) await init();
    try {
        const tablesRes = await pg.query("SELECT tablename FROM pg_tables WHERE schemaname='public'");
        const schemaTables = [];
        for (const row of tablesRes.rows) {
            const tn = row.tablename;
            const colsRes = await pg.query(`SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='${tn}' ORDER BY ordinal_position;`);
            const cols = colsRes.rows.map(c => ({ name: c.column_name, type: mapPgTypeToKusto(c.data_type) }));
            schemaTables.push({ name: tn, entityType: 'Table', columns: cols });
        }
        const schema = {
            clusterType: 'Engine',
            cluster: { connectionString: 'PGlite://idb', databases: [{ database: { name: 'public', majorVersion: 1, minorVersion: 0, tables: schemaTables } }] },
            database: { name: 'public', majorVersion: 1, minorVersion: 0, tables: schemaTables }
        };
        return schema;
    } catch (e) {
        console.warn('⚠️ Failed to build schema (PGlite):', e.message);
        return { clusterType: 'Engine', cluster: { connectionString: 'PGlite://idb', databases: [{ database: { name: 'public', majorVersion: 1, minorVersion: 0, tables: [] } }] }, database: { name: 'public', majorVersion: 1, minorVersion: 0, tables: [] } };
    }
}

// Placeholder for future file upload support (mirroring DuckDB) - kept for API parity
export async function uploadFileToDatabase() {
    throw new Error('File upload not yet implemented for PGlite backend');
}

globalThis.PGliteInterop = {
    queryJson,
    getAvailableTables,
    getDatabaseSchema,
    uploadFileToDatabase
};
