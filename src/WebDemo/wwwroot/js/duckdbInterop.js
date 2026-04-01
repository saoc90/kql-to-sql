import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29/+esm"

let db;                              // singleton per tab
let initPromise = null;              // BUG-004: guard against concurrent init()

// Make db available globally for file manager
window.db = null;

// Track whether OPFS persistence is active
window.duckdbUsesOpfs = false;

// Helper function to safely convert BigInt values to numbers for JSON serialization
function convertBigIntToNumber(obj) {
    if (obj === null || obj === undefined) return obj;
    if (typeof obj === 'bigint') return Number(obj);
    if (Array.isArray(obj)) return obj.map(convertBigIntToNumber);
    if (typeof obj === 'object') {
        const converted = {};
        for (const [key, value] of Object.entries(obj)) {
            converted[key] = convertBigIntToNumber(value);
        }
        return converted;
    }
    return obj;
}

// Helper function to decompress gzip data using browser's native DecompressionStream
async function decompressGzip(compressedData) {
    try {
        if (typeof DecompressionStream !== 'undefined') {
            const decompressedStream = compressedData.pipeThrough(
                new DecompressionStream('gzip')
            );
            const response = new Response(decompressedStream);
            const decompressedArrayBuffer = await response.arrayBuffer();
            return new Uint8Array(decompressedArrayBuffer);
        } else {
            console.warn('[DuckDB] DecompressionStream not available');
            const response = new Response(compressedData);
            const arrayBuffer = await response.arrayBuffer();
            return new Uint8Array(arrayBuffer);
        }
    } catch (error) {
        throw new Error(`Gzip decompression failed: ${error.message}`);
    }
}

// Check if OPFS is available in this browser
async function isOpfsAvailable() {
    try {
        if (!navigator.storage || !navigator.storage.getDirectory) {
            return false;
        }
        await navigator.storage.getDirectory();
        return true;
    } catch {
        return false;
    }
}

// Escape a SQL string value (single quotes)
function escapeSqlString(value) {
    return value.replace(/'/g, "''");
}

// Quote a SQL identifier (double quotes)
function quoteIdentifier(name) {
    return '"' + name.replace(/"/g, '""') + '"';
}

// BUG-004: Ensure init is only called once, even with concurrent callers
async function ensureInit() {
    if (!db) {
        if (!initPromise) {
            initPromise = init().catch(err => {
                initPromise = null; // Allow retry on failure
                throw err;
            });
        }
        await initPromise;
    }
}

export async function queryJson(sql) {
    await ensureInit();
    const c = await db.connect();
    try {
        const res = await c.query(sql);    // ArrowTable

        // Handle duplicate column names (e.g. from self-joins)
        const schema = res.schema;
        const names = schema.fields.map(f => f.name);
        const seen = {};
        let hasDuplicates = false;
        for (const name of names) {
            seen[name] = (seen[name] || 0) + 1;
            if (seen[name] > 1) hasDuplicates = true;
        }

        let data;
        if (hasDuplicates) {
            const uniqueNames = [];
            const counts = {};
            for (const name of names) {
                counts[name] = (counts[name] || 0) + 1;
                uniqueNames.push(counts[name] > 1 ? `${name}${counts[name] - 1}` : name);
            }
            const numRows = res.numRows;
            data = [];
            for (let r = 0; r < numRows; r++) {
                const row = {};
                for (let col = 0; col < uniqueNames.length; col++) {
                    row[uniqueNames[col]] = res.getChildAt(col)?.get(r);
                }
                data.push(row);
            }
        } else {
            data = res.toArray();
        }

        const convertedData = convertBigIntToNumber(data);
        return JSON.stringify(convertedData);
    } finally {
        c.close();
    }
}

export async function uploadFileToDatabase(fileName, fileContent, fileType) {
    await ensureInit();

    try {
        // Create a Uint8Array from the base64 content
        const binaryString = atob(fileContent);
        const bytes = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) {
            bytes[i] = binaryString.charCodeAt(i);
        }

        // Check if the file is gzipped and decompress if needed
        let finalBytes = bytes;
        if (fileName.toLowerCase().endsWith('.gz')) {
            try {
                const stream = new ReadableStream({
                    start(controller) {
                        controller.enqueue(bytes);
                        controller.close();
                    }
                });
                finalBytes = await decompressGzip(stream);
                fileName = fileName.replace(/\.gz$/, '');
            } catch (decompressionError) {
                console.warn('[DuckDB] Decompression failed, treating as uncompressed:', decompressionError.message);
                finalBytes = bytes;
            }
        }

        await db.registerFileBuffer(fileName, finalBytes);

        let tableName = fileName.replace(/\.[^/.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_');
        if (tableName.match(/^\d/)) {
            tableName = 'table_' + tableName;
        }

        const c = await db.connect();
        try {
            await c.query(`DROP TABLE IF EXISTS ${quoteIdentifier(tableName)}`);

            let sql = '';
            if (fileType === 'text/csv' || fileName.toLowerCase().endsWith('.csv')) {
                sql = `CREATE TABLE ${quoteIdentifier(tableName)} AS SELECT * FROM read_csv('${escapeSqlString(fileName)}', header=true, ignore_errors=true, null_padding=true)`;
            } else if (fileType === 'application/json' || fileName.toLowerCase().endsWith('.json')) {
                sql = `CREATE TABLE ${quoteIdentifier(tableName)} AS SELECT * FROM read_json('${escapeSqlString(fileName)}')`;
            } else if (fileName.toLowerCase().endsWith('.parquet')) {
                sql = `CREATE TABLE ${quoteIdentifier(tableName)} AS SELECT * FROM read_parquet('${escapeSqlString(fileName)}')`;
            } else if (fileType === 'text/plain' || fileName.toLowerCase().endsWith('.txt')) {
                sql = `CREATE TABLE ${quoteIdentifier(tableName)} AS SELECT * FROM read_csv('${escapeSqlString(fileName)}', header=false, ignore_errors=true, columns={'line': 'VARCHAR'})`;
            } else {
                throw new Error('Unsupported file type: ' + fileType);
            }

            await c.query(sql);

            const countResult = await c.query(`SELECT COUNT(*) as count FROM ${quoteIdentifier(tableName)}`);
            const countData = countResult.toArray();
            const rowCount = Number(countData[0].count);

            return JSON.stringify({
                success: true,
                tableName: tableName,
                rowCount: rowCount,
                message: `Successfully loaded ${rowCount} rows into table '${tableName}'`
            });
        } finally {
            c.close();
        }
    } catch (error) {
        console.error('[DuckDB] Failed to upload file:', error);
        return JSON.stringify({
            success: false,
            error: error.message,
            message: `Failed to load file: ${error.message}`
        });
    }
}

// Function to get database schema from DuckDB
export async function getDatabaseSchema() {
    await ensureInit();

    const c = await db.connect();
    try {
        const tablesResult = await c.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'");
        const tables = tablesResult.toArray();

        const schemaTables = [];

        for (const table of tables) {
            const tableName = table.table_name;
            const columnsResult = await c.query(
                `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = '${escapeSqlString(tableName)}' ORDER BY ordinal_position`
            );
            const columns = columnsResult.toArray();

            const schemaColumns = columns.map(col => ({
                name: col.column_name,
                type: mapDuckDbTypeToKustoType(col.data_type)
            }));

            schemaTables.push({
                name: tableName,
                entityType: "Table",
                columns: schemaColumns
            });
        }

        const schema = {
            clusterType: "Engine",
            cluster: {
                connectionString: "DuckDB://memory",
                databases: [{
                    database: {
                        name: "main",
                        majorVersion: 1,
                        minorVersion: 0,
                        tables: schemaTables
                    }
                }]
            },
            database: {
                name: "main",
                majorVersion: 1,
                minorVersion: 0,
                tables: schemaTables
            }
        };

        return schema;
    } catch (error) {
        console.error('[DuckDB] Failed to get database schema:', error);
        return {
            clusterType: "Engine",
            cluster: {
                connectionString: "DuckDB://memory",
                databases: [{
                    database: { name: "main", majorVersion: 1, minorVersion: 0, tables: [] }
                }]
            },
            database: { name: "main", majorVersion: 1, minorVersion: 0, tables: [] }
        };
    } finally {
        c.close();
    }
}

// Helper function to map DuckDB types to Kusto types
function mapDuckDbTypeToKustoType(duckDbType) {
    const typeMapping = {
        'VARCHAR': 'string',
        'INTEGER': 'int',
        'BIGINT': 'long',
        'DOUBLE': 'real',
        'BOOLEAN': 'bool',
        'DATE': 'datetime',
        'TIMESTAMP': 'datetime',
        'TIME': 'timespan',
        'DECIMAL': 'decimal',
        'FLOAT': 'real',
        'SMALLINT': 'int',
        'TINYINT': 'int',
        'UBIGINT': 'long',
        'UINTEGER': 'int',
        'USMALLINT': 'int',
        'UTINYINT': 'int',
        'JSON': 'dynamic',
        'BLOB': 'string',
        'UUID': 'guid'
    };

    if (duckDbType.includes('[]')) return 'dynamic';
    const baseType = duckDbType.split('(')[0].toUpperCase();
    return typeMapping[baseType] || 'string';
}

// Function to get list of available tables
export async function getAvailableTables() {
    await ensureInit();

    const c = await db.connect();
    try {
        const tablesResult = await c.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name");
        const tables = tablesResult.toArray();
        return JSON.stringify(tables.map(t => t.table_name));
    } catch (error) {
        console.error('[DuckDB] Failed to get table list:', error);
        return JSON.stringify([]);
    } finally {
        c.close();
    }
}

// Get the list of tables that already exist (for persistence checking)
async function getExistingTableNames() {
    const c = await db.connect();
    try {
        const result = await c.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'");
        const tables = result.toArray();
        return tables.map(t => t.table_name);
    } catch {
        return [];
    } finally {
        c.close();
    }
}

async function init() {
    console.log('[DuckDB] Initializing...');

    const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
    const workerUrl = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}")`], { type: "text/javascript" }));
    const worker = new Worker(workerUrl);

    db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);

    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    // BUG-010: Do NOT revoke workerUrl — pthread workers may still need it

    // Try to use OPFS for persistent storage, fall back to in-memory
    const opfsSupported = await isOpfsAvailable();
    if (opfsSupported) {
        try {
            await db.open({
                path: 'opfs://kql-to-sql.db',
                accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
            });
            window.duckdbUsesOpfs = true;
            console.log('[DuckDB] Opened with OPFS persistence (opfs://kql-to-sql.db)');
        } catch (opfsErr) {
            console.warn('[DuckDB] OPFS open failed, falling back to in-memory:', opfsErr.message);
            await db.open({ path: ':memory:' });
            window.duckdbUsesOpfs = false;
        }
    } else {
        await db.open({ path: ':memory:' });
        window.duckdbUsesOpfs = false;
        console.log('[DuckDB] OPFS not available, using in-memory database');
    }

    // Check if StormEvents already exists (persisted from a previous session)
    const existingTables = await getExistingTableNames();
    if (existingTables.includes('StormEvents')) {
        console.log('[DuckDB] StormEvents table already exists from previous session, skipping reload');
    } else {
        console.log('[DuckDB] Loading StormEvents sample data...');
        const res = await fetch('./StormEvents.csv.gz');

        if (!res.ok) {
            console.warn('[DuckDB] StormEvents.csv.gz not found, skipping sample data load');
        } else {
            try {
                const compressedArrayBuffer = await res.arrayBuffer();
                const compressedStream = new ReadableStream({
                    start(controller) {
                        controller.enqueue(new Uint8Array(compressedArrayBuffer));
                        controller.close();
                    }
                });
                const decompressedData = await decompressGzip(compressedStream);

                await db.registerFileBuffer('StormEvents.csv', decompressedData);
                const conn = await db.connect();
                try {
                    await conn.query("CREATE TABLE IF NOT EXISTS StormEvents AS SELECT * FROM read_csv_auto('StormEvents.csv')");
                } finally {
                    conn.close();
                }
                console.log('[DuckDB] StormEvents sample data loaded successfully');
            } catch (error) {
                console.error('[DuckDB] Failed to load StormEvents sample data:', error);
            }
        }
    }

    // Make db and interop available globally BEFORE dispatching the event (BUG-005)
    window.db = db;

    // Set up the global interop object before dispatching the ready event
    globalThis.DuckDbInterop = {
        queryJson,
        uploadFileToDatabase,
        getDatabaseSchema,
        getAvailableTables,
        resetDatabase
    };

    // Notify that persistence status is available
    window.dispatchEvent(new CustomEvent('duckdb-ready', { detail: { opfs: window.duckdbUsesOpfs } }));

    console.log(`[DuckDB] Initialization complete (persistence: ${window.duckdbUsesOpfs ? 'OPFS' : 'in-memory'})`);
}

// Export a function to clear the persisted database
export async function resetDatabase() {
    await ensureInit();
    const tables = await getExistingTableNames();
    const c = await db.connect();
    try {
        for (const table of tables) {
            await c.query(`DROP TABLE IF EXISTS ${quoteIdentifier(table)}`);
        }
    } finally {
        c.close();
    }
    console.log('[DuckDB] All tables dropped');
}

// Eagerly start init so it's ready when the app needs it (BUG-003)
ensureInit().catch(err => {
    console.error('[DuckDB] Eager init failed:', err);
    // Dispatch ready event even on failure so listeners don't hang
    window.dispatchEvent(new CustomEvent('duckdb-ready', { detail: { opfs: false, error: err.message } }));
});
