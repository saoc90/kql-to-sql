const path = require('path');

let db = null;
let conn = null;

async function ensureDb(nodeModulesPath) {
    if (conn) return;
    const duckdb = require(path.join(nodeModulesPath, '@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs'));
    const DUCKDB_DIST = path.join(nodeModulesPath, '@duckdb/duckdb-wasm/dist');

    const bundles = {
        mvp: {
            mainModule: path.join(DUCKDB_DIST, 'duckdb-mvp.wasm'),
            mainWorker: path.join(DUCKDB_DIST, 'duckdb-node-mvp.worker.cjs'),
        },
        eh: {
            mainModule: path.join(DUCKDB_DIST, 'duckdb-eh.wasm'),
            mainWorker: path.join(DUCKDB_DIST, 'duckdb-node-eh.worker.cjs'),
        }
    };

    const logger = new duckdb.VoidLogger();
    db = await duckdb.createDuckDB(bundles, logger, duckdb.NODE_RUNTIME);
    await db.instantiate();
    conn = db.connect();
}

module.exports = async (action, nodeModulesPath, sql) => {
    await ensureDb(nodeModulesPath);

    if (action === 'query') {
        const result = conn.query(sql);
        const rows = result.toArray().map(row => {
            const obj = {};
            for (const field of result.schema.fields) {
                const val = row[field.name];
                obj[field.name] = typeof val === 'bigint' ? Number(val) : val;
            }
            return obj;
        });
        return JSON.stringify(rows);
    } else if (action === 'exec') {
        conn.query(sql);
        return 'OK';
    } else {
        throw new Error('Unknown action: ' + action);
    }
};
