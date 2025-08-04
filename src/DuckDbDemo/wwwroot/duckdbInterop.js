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

export async function queryJson(sql) {
    if (!db) await init();
    const c = await db.connect();
    const res = await c.query(sql);    // ArrowTable
    c.close();
    const data = res.toArray();
    // Convert any BigInt values to numbers before serialization
    const convertedData = convertBigIntToNumber(data);
    return JSON.stringify(convertedData);   // plain JSON for easy marshalling
}

export async function uploadFileToDatabase(fileName, fileContent, fileType) {
    if (!db) await init();
    
    try {
        console.log(`Uploading file: ${fileName} (${fileType})`);
        
        // Create a Uint8Array from the base64 content
        const binaryString = atob(fileContent);
        const bytes = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) {
            bytes[i] = binaryString.charCodeAt(i);
        }
        
        // Register the file with DuckDB
        await db.registerFileBuffer(fileName, bytes);
        console.log(`Registered file: ${fileName}`);
        
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
        
        console.log('Executing SQL:', sql);
        await c.query(sql);
        
        // Get row count
        const countResult = await c.query(`SELECT COUNT(*) as count FROM ${tableName}`);
        const countData = countResult.toArray();
        c.close();
        
        // Convert BigInt to number for JSON serialization
        const rowCount = Number(countData[0].count);
        console.log(`Successfully loaded ${rowCount} rows into table '${tableName}'`);
        
        return JSON.stringify({
            success: true,
            tableName: tableName,
            rowCount: rowCount,
            message: `Successfully loaded ${rowCount} rows into table '${tableName}'`
        });
        
    } catch (error) {
        console.error('Failed to upload file:', error);
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
        console.warn(`Drop zone element with id '${dropZoneId}' not found`);
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

export async function loadStormEventsFromUrl(csvUrl) {
    if (!db) await init();
    
    try {
        console.log(`Attempting to load StormEvents data from: ${csvUrl}`);
        
        // Strategy 1: Try to fetch and register the file first
        const response = await fetch(csvUrl);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        // Get the compressed data
        const compressedData = new Uint8Array(await response.arrayBuffer());
        console.log(`Downloaded ${compressedData.length} bytes of compressed data`);
        
        // Register the file with DuckDB
        const filename = 'stormevents.csv.gz';
        await db.registerFileBuffer(filename, compressedData);
        console.log(`Registered file: ${filename}`);
        
        // Create table with explicit CSV parameters
        const c = await db.connect();
        const createTableSql = `
            CREATE TABLE StormEvents AS 
            SELECT * FROM read_csv(
                '${filename}',
                delim=',',
                quote='"',
                escape='"',
                header=true,
                ignore_errors=true,
                null_padding=true,
                compression='gzip',
                sample_size=100000
            )
            WHERE STATE IS NOT NULL AND STATE != ''
            LIMIT 2000
        `;
        
        console.log('Executing CREATE TABLE statement...');
        await c.query(createTableSql);
        
        // Verify the data was loaded
        const countResult = await c.query("SELECT COUNT(*) as count FROM StormEvents");
        const countData = countResult.toArray();
        c.close();
        
        // Convert BigInt to number for JSON serialization
        const rowCount = Number(countData[0].count);
        console.log(`Successfully loaded ${rowCount} rows`);
        
        return {
            success: true,
            rowCount: rowCount,
            message: `Successfully loaded ${rowCount} rows of NOAA StormEvents data`
        };
        
    } catch (error) {
        console.error('Failed to load StormEvents data:', error);
        
        // Try to clean up any partial table
        try {
            const c = await db.connect();
            await c.query("DROP TABLE IF EXISTS StormEvents");
            c.close();
        } catch (cleanupError) {
            console.warn('Failed to cleanup partial table:', cleanupError);
        }
        
        return {
            success: false,
            error: error.message,
            message: `Failed to load data: ${error.message}`
        };
    }
}

export async function createSampleStormEventsData() {
    if (!db) await init();
    
    try {
        console.log('Creating sample StormEvents data...');
        
        const c = await db.connect();
        
        // Drop existing table if any
        await c.query("DROP TABLE IF EXISTS StormEvents");
        
        // Create table with NOAA-like structure
        const createTableSql = `
            CREATE TABLE StormEvents (
                EVENT_ID INTEGER,
                STATE VARCHAR,
                YEAR INTEGER,
                MONTH_NAME VARCHAR,
                EVENT_TYPE VARCHAR,
                CZ_TYPE VARCHAR,
                CZ_NAME VARCHAR,
                BEGIN_DATE_TIME TIMESTAMP,
                END_DATE_TIME TIMESTAMP,
                INJURIES_DIRECT INTEGER,
                INJURIES_INDIRECT INTEGER,
                DEATHS_DIRECT INTEGER,
                DEATHS_INDIRECT INTEGER,
                DAMAGE_PROPERTY_NUM BIGINT,
                DAMAGE_CROPS_NUM BIGINT,
                DAMAGE_PROPERTY VARCHAR,
                DAMAGE_CROPS VARCHAR,
                SOURCE VARCHAR,
                MAGNITUDE DOUBLE,
                MAGNITUDE_TYPE VARCHAR,
                BEGIN_LAT DOUBLE,
                BEGIN_LON DOUBLE,
                END_LAT DOUBLE,
                END_LON DOUBLE,
                EPISODE_NARRATIVE TEXT,
                EVENT_NARRATIVE TEXT,
                DATA_SOURCE VARCHAR
            )
        `;
        
        await c.query(createTableSql);
        
        // Insert sample data
        const insertSql = `
            INSERT INTO StormEvents VALUES
            (10001, 'TEXAS', 2023, 'May', 'Tornado', 'C', 'DALLAS', '2023-05-01 14:30:00', '2023-05-01 15:15:00', 5, 2, 1, 0, 2500000, 100000, '2.50M', '100K', 'Emergency Manager', 2.5, 'EF Scale', 32.7767, -96.7970, 33.0198, -96.6989, 'A strong tornado outbreak affected North Texas.', 'EF2 tornado touched down in Dallas suburbs.', 'NWS'),
            (10002, 'CALIFORNIA', 2023, 'August', 'Wildfire', 'Z', 'LOS ANGELES', '2023-08-15 10:00:00', '2023-08-20 18:00:00', 12, 8, 3, 1, 15000000, 500000, '15.0M', '500K', 'Fire Department', NULL, NULL, 34.0522, -118.2437, 34.0522, -118.2437, 'Major wildfire season in Southern California.', 'Large wildfire burned through residential areas.', 'CAL FIRE'),
            (10003, 'FLORIDA', 2023, 'September', 'Hurricane', 'Z', 'MIAMI-DADE', '2023-09-10 06:00:00', '2023-09-12 12:00:00', 25, 15, 8, 3, 50000000, 2000000, '50.0M', '2.0M', 'Emergency Manager', 125.0, 'Sustained Wind', 25.7617, -80.1918, 25.7617, -80.1918, 'Major hurricane made landfall in South Florida.', 'Category 3 hurricane caused widespread damage.', 'NHC'),
            (10004, 'NEW YORK', 2023, 'January', 'Blizzard', 'Z', 'NEW YORK CITY', '2023-01-28 18:00:00', '2023-01-30 06:00:00', 3, 12, 0, 2, 8000000, 0, '8.0M', '0', 'NWS', 24.0, 'Snow Depth', 40.7128, -74.0060, 40.7128, -74.0060, 'Historic blizzard paralyzed the Northeast.', 'Blizzard dropped over 2 feet of snow in NYC.', 'NWS'),
            (10005, 'ILLINOIS', 2023, 'April', 'Tornado', 'C', 'COOK', '2023-04-12 20:45:00', '2023-04-12 21:30:00', 15, 8, 4, 1, 12000000, 300000, '12.0M', '300K', 'Emergency Manager', 3.2, 'EF Scale', 41.8781, -87.6298, 42.0451, -87.6877, 'Severe weather outbreak in the Midwest.', 'Strong tornado caused major damage in Chicago area.', 'NWS'),
            (10006, 'TEXAS', 2023, 'June', 'Flash Flood', 'C', 'HARRIS', '2023-06-18 16:00:00', '2023-06-18 22:00:00', 2, 5, 0, 1, 5000000, 150000, '5.0M', '150K', 'Trained Spotter', NULL, NULL, 29.7604, -95.3698, 29.7604, -95.3698, 'Flash flooding from heavy rainfall in Houston.', 'Rapid flooding trapped motorists on highways.', 'NWS'),
            (10007, 'CALIFORNIA', 2023, 'February', 'High Wind', 'Z', 'SAN FRANCISCO', '2023-02-22 12:00:00', '2023-02-23 06:00:00', 1, 3, 0, 0, 3000000, 50000, '3.0M', '50K', 'Mesonet', 85.0, 'Sustained Wind', 37.7749, -122.4194, 37.7749, -122.4194, 'Powerful wind storm hit Northern California.', 'High winds caused widespread power outages.', 'NWS'),
            (10008, 'FLORIDA', 2023, 'July', 'Thunderstorm Wind', 'C', 'ORANGE', '2023-07-04 15:30:00', '2023-07-04 16:15:00', 4, 1, 0, 0, 800000, 25000, '800K', '25K', 'Law Enforcement', 75.0, 'Sustained Wind', 28.5383, -81.3792, 28.5383, -81.3792, 'Severe thunderstorms during Independence Day.', 'Damaging winds from severe thunderstorm.', 'NWS'),
            (10009, 'NEW YORK', 2023, 'March', 'Heavy Snow', 'Z', 'ALBANY', '2023-03-14 22:00:00', '2023-03-15 14:00:00', 0, 8, 0, 1, 2500000, 0, '2.5M', '0', 'NWS', 18.0, 'Snow Depth', 42.6526, -73.7562, 42.6526, -73.7562, 'Late season snowstorm in the Northeast.', 'Heavy snow caused travel disruptions.', 'NWS'),
            (10010, 'ILLINOIS', 2023, 'August', 'Excessive Heat', 'Z', 'COOK', '2023-08-10 12:00:00', '2023-08-13 20:00:00', 8, 15, 2, 4, 0, 1000000, '0', '1.0M', 'Emergency Manager', 108.0, 'Temperature', 41.8781, -87.6298, 41.8781, -87.6298, 'Dangerous heat wave gripped the Midwest.', 'Record-breaking temperatures for multiple days.', 'NWS')
        `;
        
        await c.query(insertSql);
        
        // Get row count
        const countResult = await c.query("SELECT COUNT(*) as count FROM StormEvents");
        const countData = countResult.toArray();
        c.close();
        
        // Convert BigInt to number for JSON serialization
        const rowCount = Number(countData[0].count);
        console.log(`Created sample data with ${rowCount} rows`);
        
        return {
            success: true,
            rowCount: rowCount,
            message: `Created sample StormEvents data with ${rowCount} rows for demonstration`
        };
        
    } catch (error) {
        console.error('Failed to create sample data:', error);
        return {
            success: false,
            error: error.message,
            message: `Failed to create sample data: ${error.message}`
        };
    }
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
        console.error('Failed to get database schema:', error);
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
        
        return tables.map(t => t.table_name);
    } catch (error) {
        console.error('Failed to get table list:', error);
        return [];
    }
}

async function init() {
    const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
    const workerUrl = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}")`], { type: "text/javascript" }));
    const worker = new Worker(workerUrl);

    db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    await db.open({ path: ":memory:" });
    URL.revokeObjectURL(workerUrl);
    
    // Make db available globally for file manager
    window.db = db;
}

// Make functions available globally for Blazor JSImport
globalThis.DuckDbInterop = {
    queryJson,
    uploadFileToDatabase,
    setupFileDropZone,
    loadStormEventsFromUrl,
    createSampleStormEventsData,
    getDatabaseSchema,
    getAvailableTables
};