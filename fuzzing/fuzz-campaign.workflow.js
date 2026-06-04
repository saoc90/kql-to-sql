export const meta = {
  name: 'fuzz-kql-translator',
  description: 'Fan out family agents to invent weird KQL, differentially test vs Kustainer, loop until dry',
  phases: [
    { title: 'Round 1' },
    { title: 'Round 2' },
    { title: 'Round 3' },
  ],
}

// Each family agent: invent weird self-contained KQL, run the differential harness against the live
// Kustainer, then return the confirmed translator bugs. Verdict files on disk are the source of truth;
// the returned summary drives the loop-until-dry controller.

const DLL = 'C:/Git/kql-to-sql/tests/KqlToSql.Fuzzer/bin/Debug/net10.0/KqlToSql.Fuzzer.dll'

const SEEDS = `Self-contained seed templates you may copy/adapt (data lives in the query, so both engines see identical input):
  numbers : datatable(i:int, l:long, r:real)[ -5,-5,-5.5, 0,0,0.0, 3,2147483648,0.1, 7,100,3.25 ]
  strings : datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", "O'Brien","x" ]
  dates   : datatable(t:datetime, span:timespan)[ datetime(2007-01-01),1d, datetime(2020-06-15 13:45:00),2h ]
  dynamic : datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]), dynamic({"x":{"y":1}}) ]
  wide    : datatable(id:long, name:string, score:real, active:bool, ts:datetime, cat:string)[ 1,"a",1.5,true,datetime(2020-01-01),"p", 2,"b",2.5,false,datetime(2021-06-01),"q" ]
  dup     : datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30 ]`

const RULES = `HARD RULES:
- Every query MUST be SELF-CONTAINED: it must start with a datatable(...)[...] literal, or 'print', or 'range'. NEVER reference an external table name (the emulator's default DB has no tables).
- Each query MUST be a SINGLE LINE (pipelines on one line are fine).
- Queries MUST be valid KQL that real Kusto accepts (invalid ones are auto-discarded, wasting effort).
- Do NOT use unsupported operators: facet, fork, invoke, partition, reduce, find, project-by-names, make-graph/graph-*, multi-step scan, evaluate autocluster/basket/diffpatterns. These are known-unsupported and uninteresting.
- Avoid pure nondeterminism as the POINT of a query (rand, now, sample, take without sort, dcount/percentile) — they are auto-skipped. You may use them incidentally.`

const FAMILIES = [
  { name: 'string-ops', focus: 'String/term operators and functions: has / !has / has_cs / hasprefix / hassuffix vs contains/startswith/endswith (term-vs-substring semantics!), case sensitivity (=~, _cs variants), matches regex, in/in~, unicode (café, emoji, combining marks), embedded quotes and escapes, strcat/strcat_delim/split/extract/extract_all/replace_string/replace_regex/trim/substring with negative or out-of-range indices, indexof/countof, isempty/isnotempty on "" vs null, strlen vs string_size (bytes vs chars).' },
  { name: 'datetime-timespan', focus: 'Datetime/timespan: bin/bin_at across boundaries, startofweek/startofmonth/startofyear/endof* (week start day!), dayofweek/dayofmonth/getyear/monthofyear/weekofyear/hourofday, datetime_add/datetime_diff with all units, format_datetime/format_timespan with varied format strings, datetime arithmetic (t+span, t-t -> timespan), fractional-second datetimes, negative timespans, far-future/pre-1970 dates, timespan literals (1d,2h,30m,1tick,1.5h).' },
  { name: 'aggregation', focus: 'summarize: many aggregates (sum/avg/min/max/count/countif/sumif/avgif/make_list/make_set/make_bag/stdev/variance/arg_max/arg_min/take_any), by 0/1/2 keys, aggregates over empty groups, make_list/make_set element ordering, countif/sumif with bool predicates, nested iif inside aggregates, having-style filters after summarize, bin() in by-clause.' },
  { name: 'joins-lookup-union', focus: 'join all kinds (inner/innerunique/leftouter/rightouter/fullouter/leftsemi/rightsemi/leftanti/rightanti), single and multi-key joins, key-name collisions across sides, joins on nullable keys, no-match and all-match cases, duplicate keys, lookup, union (with/without withsource), union of differently-shaped tables.' },
  { name: 'dynamic-json', focus: 'dynamic literals (nested objects, arrays, mixed-type arrays, deeply nested, empty {}/[], dynamic(null)), parse_json, property access d.a.b and d[0] and d["k"], array_length/array_slice/array_concat/array_sort_asc/bag_keys/bag_merge/set_union, mv-expand and mv-apply over arrays, bag_unpack via evaluate, todynamic(tostring(d)) round-trips, dynamic_to_json.' },
  { name: 'type-casts-coercion', focus: 'toint/tolong/todouble/toreal/tostring/tobool/todecimal/todatetime/totimespan, integer division (long/int, int/int -> truncation), modulo with negative operands (sign!), int overflow into long, real vs int mixed arithmetic, casting strings to numbers (valid and invalid -> null), gettype, isnan/isinf/isfinite, NaN/Infinity literals.' },
  { name: 'nested-pipelines-let-cte', focus: 'Deep multi-stage pipelines (5+ operators), let chains referencing prior lets, materialize(), as <name>, column names that are SQL reserved words (select/from/order/group/table/value/count), column names that collide with auto-generated names (count_, Column1, print_0), extend that shadows a column, project reordering, repeated/contradictory where filters.' },
  { name: 'parse-search', focus: 'parse with simple/regex kind and varied patterns, parse-where, parse-kv with pair delimiters, search across columns, search with terms and predicates, where with combinations of and/or/not and parentheses, between/!between on numbers and datetimes.' },
  { name: 'window-series-scan', focus: 'serialize then row_number()/prev()/next()/row_cumsum()/row_rank_dense/row_rank_min, single-step scan (cumulative sum, forward fill, cumulative-with-reset), make-series with various ranges and aggregations and default values, top-nested, top-hitters, sample-distinct.' },
  { name: 'null-and-edge', focus: 'Null propagation everywhere: arithmetic/comparison/string-funcs/aggregates over nulls, isnull/isnotnull/isempty/isnotempty/coalesce/iif/case around nulls, null ordering in sort (asc/desc), empty datatable through every operator, contradictory filters (where x>5 and x<3), where true / where false, extreme literals (very long strings, int.MaxValue, year-9999, 1tick, 0-row results).' },
]

function genPrompt(family, focus, round, priorCount) {
  const novelty = round === 1
    ? 'Generate 45 diverse, weird, adversarial KQL queries.'
    : `This is round ${round}. ${priorCount} bug candidates already found across the campaign. Generate 45 NEW, DEEPER, MORE UNUSUAL queries than typical round-1 cases — combine multiple tricky features per query, push edge cases harder, and avoid simple/obvious patterns already likely tested.`
  return `You are an adversarial test generator trying to BREAK a KQL-to-SQL translator (KQL is run on a real Kusto engine and on the translator->DuckDB; results are compared).

Your family: **${family}**.
Focus on: ${focus}

${novelty}

${RULES}

${SEEDS}

STEPS (do all of them):
1. Invent the queries. Make them genuinely weird and combinatorial within your family. Aim to expose translation bugs (wrong values, wrong row counts, wrong column names/types, invalid generated SQL, semantic mismatches).
2. Use the Write tool to write them to: C:/Git/kql-to-sql/fuzzing/agent/r${round}-${family}.kql  — one query per line, no blank lines, no commentary.
3. Run the differential harness against the live Kustainer + DuckDB with the Bash tool:
   dotnet "${DLL}" runtext --in "C:/Git/kql-to-sql/fuzzing/agent/r${round}-${family}.kql" --family ${family} --out "C:/Git/kql-to-sql/fuzzing/verdicts/r${round}-${family}.jsonl"
   The command prints a per-outcome summary and "=> N bug candidates".
4. Read the verdicts JSONL file. Each line is a verdict with fields: Outcome, Kql, Sql, Detail, Severity, IsBug, SubVerdicts, Kusto{Columns,RowCount,SampleRows,Error}, Duck{...}.
5. Return ONLY the structured summary. For the bugs array, include entries where IsBug is true (Outcomes: MismatchRows, MismatchColumns, MismatchOrder, SqlExecError, TranslateError). For each, give the kql, outcome, severity, a one-line detail, and a short rootCause hypothesis (which KQL feature is mistranslated). Cap the bugs array at the 15 most distinct/severe; set bugCount to the true total.

Notes:
- KUSTO_ERROR means your query was invalid KQL (Kusto rejected it) — those are NOT bugs; reduce them next time.
- SkippedNondeterministic / SkippedUnsupported are not bugs.
- The DLL is already built; do not rebuild. If dotnet prints a download line for DuckDB on first run that is fine.`
}

const BUG_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    family: { type: 'string' },
    generated: { type: 'integer' },
    kustoErrors: { type: 'integer' },
    bugCount: { type: 'integer' },
    bugs: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          kql: { type: 'string' },
          outcome: { type: 'string' },
          severity: { type: 'string' },
          detail: { type: 'string' },
          rootCause: { type: 'string' },
        },
        required: ['kql', 'outcome', 'rootCause'],
      },
    },
  },
  required: ['family', 'generated', 'bugCount', 'bugs'],
}

const seen = new Set()
const allBugs = []
let dryRounds = 0
const MAX_ROUNDS = 3

for (let round = 1; round <= MAX_ROUNDS; round++) {
  phase(`Round ${round}`)
  log(`Round ${round}: dispatching ${FAMILIES.length} family agents`)

  const results = await parallel(FAMILIES.map(f => () =>
    agent(genPrompt(f.name, f.focus, round, allBugs.length), {
      schema: BUG_SCHEMA,
      label: `${f.name} r${round}`,
      phase: `Round ${round}`,
    })
  ))

  let newThisRound = 0
  for (const r of results.filter(Boolean)) {
    for (const b of (r.bugs || [])) {
      const key = b.outcome + '::' + (b.kql || '').replace(/\s+/g, ' ').trim()
      if (seen.has(key)) continue
      seen.add(key)
      allBugs.push({ round, ...b })
      newThisRound++
    }
  }

  const totalGenerated = results.filter(Boolean).reduce((a, r) => a + (r.generated || 0), 0)
  log(`Round ${round}: ${totalGenerated} queries generated, ${newThisRound} new distinct bug candidates (total ${allBugs.length})`)

  if (newThisRound === 0) {
    dryRounds++
    if (dryRounds >= 2) { log('Two dry rounds — stopping early.'); break }
  } else {
    dryRounds = 0
  }
}

// Group for a quick view; full data is in fuzzing/verdicts/*.jsonl on disk.
const byRootCause = {}
for (const b of allBugs) {
  const k = (b.rootCause || 'unknown').slice(0, 80)
  byRootCause[k] = (byRootCause[k] || 0) + 1
}

return {
  totalDistinctBugs: allBugs.length,
  byRootCause,
  bugs: allBugs,
}
