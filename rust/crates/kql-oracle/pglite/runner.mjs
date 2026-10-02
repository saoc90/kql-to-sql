// PGlite runner for the kql-oracle differential harness.
//
//   node runner.mjs <batch.jsonl> <out.jsonl> [--setup <setup.sql>] [--copy <table> <file.csv[.gz]>]... [--fresh]
//
// Reads one {"id","sql"} object per line, executes each statement and writes one
// {"id","columns":[{"name","type_oid"}],"rows":[[...]],"error"} object per line.
//
// Every cell is returned as PostgreSQL's own text output (all type parsers are replaced by the
// identity), so nothing is lossy: int8 beyond 2^53, NaN/Infinity, numeric, timestamps with
// microseconds, intervals, jsonb, uuid. NULL stays JSON null. The Rust side interprets the text
// using the translator's declared column types and the type OID.
//
// By default one PGlite instance is reused; each statement runs inside BEGIN ... ROLLBACK (as a
// cursor, fetched in chunks) so
// stray side effects (temp tables, settings) do not leak into the next query. `--fresh` creates
// a new instance per statement instead (much slower). `--setup` runs a SQL script once per
// instance before the batch (e.g. CREATE TABLE for StormEvents); each `--copy` then loads a CSV
// file with a header line (gunzipped when it ends in .gz) via COPY ... FROM '/dev/blob'.

import { readFileSync, createWriteStream } from 'node:fs';
import { gunzipSync } from 'node:zlib';
import { PGlite, types } from '@electric-sql/pglite';

const args = process.argv.slice(2);
let fresh = false;
let setupFile = null;
const copies = [];
const positional = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--fresh') fresh = true;
  else if (args[i] === '--setup') setupFile = args[++i];
  else if (args[i] === '--copy') {
    copies.push({ table: args[i + 1], file: args[i + 2] });
    i += 2;
  }
  else positional.push(args[i]);
}
if (positional.length !== 2) {
  console.error('usage: node runner.mjs <batch.jsonl> <out.jsonl> [--setup <setup.sql>] [--copy <table> <csv>]... [--fresh]');
  process.exit(2);
}
const [batchPath, outPath] = positional;
const setupSql = setupFile ? readFileSync(setupFile, 'utf8') : null;
const copyBlobs = copies.map(({ table, file }) => {
  let bytes = readFileSync(file);
  if (file.endsWith('.gz')) bytes = gunzipSync(bytes);
  return { table, blob: new Blob([bytes]) };
});

const identity = (x) => x;

/** Parsers that keep every value as its PostgreSQL text representation. */
function textParsers(db) {
  const p = {};
  for (const k of Object.keys(types.parsers)) p[k] = identity;
  for (const k of Object.keys(db.parsers ?? {})) p[k] = identity;
  // Built-in and array OIDs PGlite might know parsers for (array types live below 10000).
  for (let oid = 16; oid < 10000; oid++) p[oid] = identity;
  return p;
}

async function newInstance() {
  const db = await PGlite.create();
  await db.exec("SET TIME ZONE 'UTC'; SET intervalstyle = 'postgres'; SET extra_float_digits = 1;");
  if (setupSql) await db.exec(setupSql);
  for (const { table, blob } of copyBlobs) {
    await db.query(`COPY ${table} FROM '/dev/blob' WITH (FORMAT csv, HEADER true)`, [], { blob });
  }
  return { db, parsers: textParsers(db) };
}

// Results are fetched through a cursor in chunks: a single huge result (tens of MB, e.g. a
// full StormEvents projection) overflows PGlite's protocol buffer ("received invalid response").
const CHUNK = 2000;

async function runQuery(cur, sql) {
  const opts = { rowMode: 'array', parsers: cur.parsers };
  await cur.db.query(`DECLARE kql_oracle_cursor NO SCROLL CURSOR FOR ${sql}`);
  let columns = null;
  const rows = [];
  for (;;) {
    const res = await cur.db.query(`FETCH FORWARD ${CHUNK} FROM kql_oracle_cursor`, [], opts);
    columns ??= res.fields.map((f) => ({ name: f.name, type_oid: f.dataTypeID }));
    for (const r of res.rows) rows.push(r);
    if (res.rows.length < CHUNK) break;
  }
  return { columns, rows };
}

let done = 0;
const lines = readFileSync(batchPath, 'utf8').split('\n').filter((l) => l.trim() !== '');
const out = createWriteStream(outPath);
const started = Date.now();

let inst = fresh ? null : await newInstance();
for (const line of lines) {
  const { id, sql } = JSON.parse(line);
  let rec;
  let cur = inst;
  try {
    if (fresh) cur = await newInstance();
    await cur.db.exec('BEGIN');
    try {
      rec = { id, ...(await runQuery(cur, sql)), error: null };
    } finally {
      try {
        await cur.db.exec('ROLLBACK');
      } catch {
        /* a failed statement aborts the transaction; ROLLBACK may still be needed */
      }
    }
  } catch (e) {
    rec = { id, columns: [], rows: [], error: String(e?.message ?? e) };
    // An error inside PGlite can (rarely) leave the instance unusable; replace it.
    if (!fresh) {
      try {
        await inst.db.query('SELECT 1');
      } catch {
        inst = await newInstance();
      }
    }
  } finally {
    if (fresh && cur) await cur.db.close().catch(() => {});
  }
  out.write(JSON.stringify(rec) + '\n');
  if (++done % 250 === 0) console.error(`pglite runner: ${done}/${lines.length}`);
}
await new Promise((resolve) => out.end(resolve));
if (inst) await inst.db.close().catch(() => {});
console.error(`pglite runner: ${lines.length} statements in ${((Date.now() - started) / 1000).toFixed(1)}s`);
