// PGlite (Postgres WASM) interop for the Blazor demo
// Lightweight implementation to mirror the DuckDB functions used in C# / Razor.
// Uses CDN import; persisted to IndexedDB so data survives page reloads.

// Pinned to 0.2.17. The import was previously unpinned, so it floated to the latest release
// (0.5.x), whose `idb://` IndexedDB VFS changed and could no longer open the store created by
// the older build — failing at startup with "PGlite failed to initialize properly".
import { PGlite } from 'https://cdn.jsdelivr.net/npm/@electric-sql/pglite@0.2.17/dist/index.js'

const IDB_DATA_DIR = 'idb://kql-to-sql';

let pg; // singleton

// Expose for debugging
window.pg = null;

// Create a PGlite instance and wait for the Postgres boot to actually complete, so init
// failures surface here (where we can recover) rather than mid-query.
async function createPg(dataDir) {
    const inst = new PGlite(dataDir);
    await inst.waitReady;
    return inst;
}

let initPromise = null;

// Initializes once, even when called concurrently (schema lookup and the first query race).
function init() {
    if (!initPromise) initPromise = initOnce().catch(e => { initPromise = null; throw e; });
    return initPromise;
}

async function initOnce() {
    console.log('🚀 Initializing PGlite (Postgres WASM)...');
    try {
        // Persist to IndexedDB (creates db if missing)
        pg = await createPg(IDB_DATA_DIR);
    } catch (e) {
        // A persisted store left behind by an incompatible PGlite build can fail to open. Fall
        // back to an in-memory store so the demo still works (StormEvents is reloaded on demand).
        console.warn('⚠️ PGlite IndexedDB store failed to open, falling back to in-memory:', e);
        pg = await createPg('memory://');
    }
    await ensureStormEventsLoaded();
    window.pg = pg;
    console.log('✅ PGlite ready');
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
async function ensureStormEventsLoaded() {
    try {
        console.log('🔍 Checking for StormEvents table (PGlite)...');
        const existsRes = await pg.query("SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='StormEvents';");
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
        // Kusto's StormEvents schema; names are quoted so PostgreSQL keeps their case
        // (the translator quotes mixed-case identifiers for PostgreSQL).
        const columns = [
            ['StartTime', 'timestamp'], ['EndTime', 'timestamp'], ['EpisodeId', 'integer'], ['EventId', 'integer'],
            ['State', 'text'], ['EventType', 'text'], ['InjuriesDirect', 'integer'], ['InjuriesIndirect', 'integer'],
            ['DeathsDirect', 'integer'], ['DeathsIndirect', 'integer'], ['DamageProperty', 'integer'], ['DamageCrops', 'integer'],
            ['Source', 'text'], ['BeginLocation', 'text'], ['EndLocation', 'text'], ['BeginLat', 'double precision'],
            ['BeginLon', 'double precision'], ['EndLat', 'double precision'], ['EndLon', 'double precision'],
            ['EpisodeNarrative', 'text'], ['EventNarrative', 'text'], ['StormSummary', 'jsonb']];
        await pg.exec(`CREATE TABLE "StormEvents" (${columns.map(([n, t]) => `"${n}" ${t}`).join(', ')});`);

        // Use COPY with blob
        const blob = new Blob([decompressed], { type: 'text/csv' });
        console.log('📤 Copying CSV into StormEvents via /dev/blob ...');
        await pg.query(`COPY "StormEvents" FROM '/dev/blob' WITH (FORMAT csv, HEADER true);`, [], { blob });
        const count = await pg.query('SELECT COUNT(*) AS cnt FROM "StormEvents";');
        console.log(`✅ Loaded StormEvents into PGlite (${count.rows[0].cnt} rows)`);
    } catch (e) {
        console.error('❌ Failed to load StormEvents into PGlite:', e);
    }
}

// Classify a Postgres type OID into the chart-relevant type set. (StormEvents loads as text in
// PGlite, so most columns classify as 'string'; the renderer coerces y-values with Number().)
function classifyPgOid(oid) {
    switch (oid) {
        case 20: case 21: case 23: case 700: case 701: case 1700: return 'number'; // int8/int2/int4/float4/float8/numeric
        case 16: return 'bool';
        case 1082: case 1114: case 1184: return 'datetime'; // date/timestamp/timestamptz
        default: return 'string';
    }
}

export async function queryJson(sql) {
    await init();
    try {
        // Use rowMode:'array' to preserve duplicate column values from joins.
        // Keep date/timestamp/timestamptz as their raw Postgres text (identity parsers): the default
        // client parser turns "timestamp without time zone" into a *local-time* JS Date, which then
        // renders shifted by the browser's UTC offset. KQL datetimes are UTC wall-clock, so the raw
        // text is what we want to display. (OIDs: 1082 date, 1114 timestamp, 1184 timestamptz.)
        const keepText = v => v;
        const res = await pg.query(sql, [], {
            rowMode: 'array',
            parsers: { 1082: keepText, 1114: keepText, 1184: keepText },
        });
        const fields = res.fields || [];

        // Deduplicate column names (KQL style: col, col1, col2, ...)
        const names = [];
        const counts = {};
        for (const f of fields) {
            const name = f.name;
            counts[name] = (counts[name] || 0) + 1;
            names.push(counts[name] > 1 ? `${name}${counts[name] - 1}` : name);
        }

        const columns = names.map((name, i) => ({ name, type: classifyPgOid(fields[i] && fields[i].dataTypeID) }));

        // Build row objects with deduplicated names
        const rows = (res.rows || []).map(row => {
            const obj = {};
            for (let i = 0; i < names.length; i++) {
                obj[names[i]] = row[i];
            }
            return obj;
        });

        return JSON.stringify({ columns, rows });   // typed envelope for table + chart
    } catch (e) {
        console.error('❌ PGlite query failed:', e);
        throw e;
    }
}

export async function getAvailableTables() {
    await init();
    try {
        const res = await pg.query("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename;");
        return JSON.stringify(res.rows.map(r => r.tablename));
    } catch (e) {
        console.warn('⚠️ Failed to list tables (PGlite):', e.message);
        return JSON.stringify([]);
    }
}

export async function getDatabaseSchema() {
    await init();
    try {
        // All columns of all tables in one query (no string-built SQL)
        const colsRes = await pg.query(
            "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position");
        const byTable = new Map();
        for (const c of colsRes.rows) {
            if (!byTable.has(c.table_name)) byTable.set(c.table_name, []);
            byTable.get(c.table_name).push({ name: c.column_name, type: mapPgTypeToKusto(c.data_type), sqlType: c.data_type });
        }
        const schemaTables = [...byTable].map(([name, columns]) => ({ name, entityType: 'Table', columns }));
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
