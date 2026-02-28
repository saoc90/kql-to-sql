import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29/+esm"

let db;                              // singleton per tab

// Make db available globally for file manager
window.db = null;

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
        console.log('🗜️ Decompressing gzip data...');
        
        // Check if DecompressionStream is available (modern browsers)
        if (typeof DecompressionStream !== 'undefined') {
            const decompressedStream = compressedData.pipeThrough(
                new DecompressionStream('gzip')
            );
            
            const response = new Response(decompressedStream);
            const decompressedArrayBuffer = await response.arrayBuffer();
            console.log('✅ Gzip decompression successful using native DecompressionStream');
            return new Uint8Array(decompressedArrayBuffer);
        } else {
            console.warn('⚠️ DecompressionStream not available, attempting manual decompression');
            // Fallback: try to process as uncompressed data
            // This is a limitation - for full gzip support in older browsers, 
            // you would need to include a library like pako
            const response = new Response(compressedData);
            const arrayBuffer = await response.arrayBuffer();
            return new Uint8Array(arrayBuffer);
        }
    } catch (error) {
        console.error('❌ Failed to decompress gzip data:', error);
        throw new Error(`Gzip decompression failed: ${error.message}`);
    }
}

export async function queryJson(sql) {
    if (!db) await init();
    const c = await db.connect();
    const res = await c.query(sql);    // ArrowTable
    c.close();

    // Handle duplicate column names (e.g. from self-joins) by renaming them
    // before calling toArray(), which fails on duplicate keys in JS proxies.
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
        // Build rows manually with deduplicated column names
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

    // Convert any BigInt values to numbers before serialization
    const convertedData = convertBigIntToNumber(data);
    return JSON.stringify(convertedData);   // plain JSON for easy marshalling
}

export async function uploadFileToDatabase(fileName, fileContent, fileType) {
    if (!db) await init();
    
    try {
        console.log(`🚀 Uploading file: ${fileName} (${fileType})`);
        
        // Create a Uint8Array from the base64 content
        const binaryString = atob(fileContent);
        const bytes = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) {
            bytes[i] = binaryString.charCodeAt(i);
        }
        
        // Check if the file is gzipped and decompress if needed
        let finalBytes = bytes;
        if (fileName.toLowerCase().endsWith('.gz')) {
            console.log('🗜️ Detected gzipped file, decompressing...');
            try {
                // Create a ReadableStream from the bytes
                const stream = new ReadableStream({
                    start(controller) {
                        controller.enqueue(bytes);
                        controller.close();
                    }
                });
                
                finalBytes = await decompressGzip(stream);
                
                // Update fileName to remove .gz extension for DuckDB registration
                fileName = fileName.replace(/\.gz$/, '');
                console.log(`✅ File decompressed, new name: ${fileName}`);
            } catch (decompressionError) {
                console.warn('⚠️ Decompression failed, treating as uncompressed:', decompressionError.message);
                // Continue with original bytes if decompression fails
                finalBytes = bytes;
            }
        }
        
        // Register the file with DuckDB
        await db.registerFileBuffer(fileName, finalBytes);
        console.log(`✅ Registered file: ${fileName}`);
        
        // Create table based on file type
        let tableName = fileName.replace(/\.[^/.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_');
        if (tableName.match(/^\d/)) {
            tableName = 'table_' + tableName;
        }
        
        let sql = '';
        const c = await db.connect();
        
        // Drop table if it exists
        await c.query(`DROP TABLE IF EXISTS ${tableName}`);
        
        if (fileType === 'text/csv' || fileName.toLowerCase().endsWith('.csv')) {
            sql = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${fileName}', header=true, ignore_errors=true, null_padding=true)`;
        } else if (fileType === 'application/json' || fileName.toLowerCase().endsWith('.json')) {
            sql = `CREATE TABLE ${tableName} AS SELECT * FROM read_json('${fileName}')`;
        } else if (fileName.toLowerCase().endsWith('.parquet')) {
            sql = `CREATE TABLE ${tableName} AS SELECT * FROM read_parquet('${fileName}')`;
        } else if (fileType === 'text/plain' || fileName.toLowerCase().endsWith('.txt')) {
            // For text files, create a simple table with line content
            sql = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${fileName}', header=false, ignore_errors=true, columns={'line': 'VARCHAR'})`;
        } else {
            throw new Error('Unsupported file type: ' + fileType);
        }
        
        console.log('🔧 Executing SQL:', sql);
        await c.query(sql);
        
        // Get row count
        const countResult = await c.query(`SELECT COUNT(*) as count FROM ${tableName}`);
        const countData = countResult.toArray();
        c.close();
        
        // Convert BigInt to number for JSON serialization
        const rowCount = Number(countData[0].count);
        console.log(`✅ Successfully loaded ${rowCount} rows into table '${tableName}'`);
        
        return JSON.stringify({
            success: true,
            tableName: tableName,
            rowCount: rowCount,
            message: `Successfully loaded ${rowCount} rows into table '${tableName}'`
        });
        
    } catch (error) {
        console.error('❌ Failed to upload file:', error);
        return JSON.stringify({
            success: false,
            error: error.message,
            message: `Failed to load file: ${error.message}`
        });
    }
}

// Enhanced drag and drop support
export function setupFileDropZone(dropZoneId, dotnetRef) {
    const dropZone = document.getElementById(dropZoneId);
    if (!dropZone) {
        console.warn(`❌ Drop zone element with id '${dropZoneId}' not found`);
        return;
    }

    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dotnetRef.invokeMethodAsync('OnDragOver');
    });

    dropZone.addEventListener('dragenter', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dotnetRef.invokeMethodAsync('OnDragEnter');
    });

    dropZone.addEventListener('dragleave', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dotnetRef.invokeMethodAsync('OnDragLeave');
    });

    dropZone.addEventListener('drop', async (e) => {
        e.preventDefault();
        e.stopPropagation();
        
        const files = Array.from(e.dataTransfer.files);
        if (files.length > 0) {
            const fileData = [];
            
            for (const file of files) {
                // Read file as base64
                const reader = new FileReader();
                const fileContent = await new Promise((resolve) => {
                    reader.onload = (event) => resolve(event.target.result);
                    reader.readAsDataURL(file);
                });
                
                // Remove data URL prefix to get pure base64
                const base64Content = fileContent.split(',')[1];
                
                fileData.push({
                    name: file.name,
                    size: file.size,
                    type: file.type,
                    content: base64Content
                });
            }
            
            await dotnetRef.invokeMethodAsync('OnFilesDropped', fileData);
        }
    });
}

// Store reference to current Monaco editor for schema updates
let currentMonacoEditor = null;

// Function to get database schema from DuckDB
export async function getDatabaseSchema() {
    if (!db) await init();
    
    try {
        const c = await db.connect();
        
        // Get list of all tables
        const tablesResult = await c.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'");
        const tables = tablesResult.toArray();
        
        const schemaTables = [];
        
        // For each table, get its columns
        for (const table of tables) {
            const tableName = table.table_name;
            const columnsResult = await c.query(`
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'main' AND table_name = '${tableName}'
                ORDER BY ordinal_position
            `);
            const columns = columnsResult.toArray();
            
            // Map DuckDB types to Kusto types
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
        
        c.close();
        
        // Create the schema object in the format expected by Monaco Kusto
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
        console.error('❌ Failed to get database schema:', error);
        return {
            clusterType: "Engine",
            cluster: {
                connectionString: "DuckDB://memory",
                databases: [{
                    database: {
                        name: "main",
                        majorVersion: 1,
                        minorVersion: 0,
                        tables: []
                    }
                }]
            },
            database: {
                name: "main",
                majorVersion: 1,
                minorVersion: 0,
                tables: []
            }
        };
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
    
    // Handle array types
    if (duckDbType.includes('[]')) {
        return 'dynamic';
    }
    
    // Get the base type (remove any size specifications)
    const baseType = duckDbType.split('(')[0].toUpperCase();
    
    return typeMapping[baseType] || 'string';
}

// Function to get list of available tables
export async function getAvailableTables() {
    if (!db) await init();
    
    try {
        const c = await db.connect();
        const tablesResult = await c.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name");
        const tables = tablesResult.toArray();
        c.close();
        
        return JSON.stringify(tables.map(t => t.table_name));
    } catch (error) {
        console.error('❌ Failed to get table list:', error);
        return JSON.stringify([]);
    }
}

async function init() {
    console.log('🚀 Initializing DuckDB...');
    
    const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
    const workerUrl = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}")`], { type: "text/javascript" }));
    const worker = new Worker(workerUrl);

    db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    await db.open({ path: ":memory:" });
    
    console.log('📂 Loading StormEvents sample data...');
    const res = await fetch('./StormEvents.csv.gz');
    
    if (!res.ok) {
        console.warn('⚠️ StormEvents.csv.gz not found, skipping sample data load');
    } else {
        try {
            // Get the compressed data as a stream
            const compressedArrayBuffer = await res.arrayBuffer();
            console.log(`📦 Downloaded ${compressedArrayBuffer.byteLength} bytes of compressed data`);
            
            // Create a ReadableStream for decompression
            const compressedStream = new ReadableStream({
                start(controller) {
                    controller.enqueue(new Uint8Array(compressedArrayBuffer));
                    controller.close();
                }
            });
            
            // Decompress the data
            const decompressedData = await decompressGzip(compressedStream);
            console.log(`📊 Decompressed to ${decompressedData.byteLength} bytes`);
            
            // Register the decompressed CSV data with DuckDB
            await db.registerFileBuffer('StormEvents.csv', decompressedData);
            const conn = await db.connect();
            await conn.query("CREATE OR REPLACE TABLE StormEvents AS SELECT * FROM read_csv_auto('StormEvents.csv'); ");
            await conn.close();
            console.log('✅ StormEvents sample data loaded successfully');
        } catch (error) {
            console.error('❌ Failed to load StormEvents sample data:', error);
            console.warn('⚠️ Continuing without sample data...');
        }
    }
    
    URL.revokeObjectURL(workerUrl);
    
    // Make db available globally for file manager
    window.db = db;
    console.log('✅ DuckDB initialization complete');
}

// Make functions available globally for Blazor JSImport
globalThis.DuckDbInterop = {
    queryJson,
    uploadFileToDatabase,
    setupFileDropZone,
    getDatabaseSchema,
    getAvailableTables
};