const path = require('path');

let db = null;

async function ensureDb(nodeModulesPath) {
    if (db) return;
    const { PGlite } = require(path.join(nodeModulesPath, '@electric-sql/pglite/dist/index.cjs'));
    db = new PGlite();
    await db.waitReady;
}

module.exports = async (action, nodeModulesPath, sql) => {
    await ensureDb(nodeModulesPath);

    if (action === 'query') {
        const result = await db.query(sql);
        return JSON.stringify(result.rows);
    } else if (action === 'exec') {
        await db.exec(sql);
        return 'OK';
    } else {
        throw new Error('Unknown action: ' + action);
    }
};
