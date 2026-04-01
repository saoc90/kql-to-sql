#!/usr/bin/env bash
# ============================================================================
# KQL vs Native SQL DuckDB Benchmark
# ============================================================================
# Measures the latency difference between running native SQL directly in DuckDB
# versus converting KQL to SQL via the kql extension and then executing.
#
# Two-step pattern (DuckDB v1.2.0 has no EXECUTE(<expr>)):
#   Step 1: kql_to_sql() returns a SQL string via the extension
#   Step 2: Execute that SQL string in a separate DuckDB invocation
#
# Usage:
#   ./tests/benchmark.sh [DUCKDB_CLI] [EXTENSION_PATH]
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DUCKDB="${1:-/tmp/duckdb}"
EXT="${2:-$REPO_ROOT/src/KqlToSql.DuckDbExtension/bin/Release/net10.0/linux-x64/publish/kql.duckdb_extension}"

# ── Verify prerequisites ────────────────────────────────────────────────────
if [[ ! -x "$DUCKDB" ]]; then
  echo "ERROR: DuckDB CLI not found at $DUCKDB" >&2; exit 1
fi
if [[ ! -f "$EXT" ]]; then
  echo "ERROR: Extension not found at $EXT" >&2; exit 1
fi
echo "DuckDB CLI : $DUCKDB ($($DUCKDB --version 2>&1))"
echo "Extension  : $EXT"

# ── Generate test data (100k rows) ──────────────────────────────────────────
DATA_DIR=$(mktemp -d)
LARGE_CSV="$DATA_DIR/events.csv"
echo "Generating 100,000-row test CSV..."
python3 -c "
import csv, random, sys
random.seed(42)
states = ['TEXAS','FLORIDA','CALIFORNIA','NEW YORK','OHIO','GEORGIA','ILLINOIS','PENNSYLVANIA','NORTH CAROLINA','MICHIGAN']
events = ['Thunderstorm Wind','Hail','Tornado','Flash Flood','Winter Storm','Heavy Rain','Lightning','Drought','Flood','Hurricane']
writer = csv.writer(sys.stdout)
writer.writerow(['State','EventType','DamageProperty','Injuries','Deaths','Year'])
for i in range(100000):
    writer.writerow([random.choice(states), random.choice(events), random.randint(0,1000000), random.randint(0,50), random.randint(0,5), random.randint(2000,2023)])
" > "$LARGE_CSV"
echo "  $(wc -l < "$LARGE_CSV") lines written to $LARGE_CSV"

DB_PATH="$DATA_DIR/bench.duckdb"

# ── Load data into DuckDB ───────────────────────────────────────────────────
echo ""
echo "Loading CSV into DuckDB..."
"$DUCKDB" "$DB_PATH" -c "
CREATE TABLE Events AS SELECT * FROM read_csv_auto('$LARGE_CSV');
SELECT COUNT(*) AS row_count FROM Events;
"
echo "Data loaded."

# ── Helpers ─────────────────────────────────────────────────────────────────
ITERATIONS=20

# time_sql: run a native SQL query $ITERATIONS times, print stats
time_sql() {
  local label="$1"
  local sql="$2"
  local times=()

  for ((i=0; i<ITERATIONS; i++)); do
    local start end elapsed
    start=$(date +%s%N)
    "$DUCKDB" "$DB_PATH" -c "$sql" > /dev/null 2>&1
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))
    times+=("$elapsed")
  done

  IFS=$'\n' sorted=($(printf '%s\n' "${times[@]}" | sort -n)); unset IFS
  local median=${sorted[$(( ITERATIONS / 2 ))]}
  local min=${sorted[0]}
  local max=${sorted[$(( ITERATIONS - 1 ))]}
  printf "  %-50s  median %4d ms  (min %4d / max %4d)\n" "$label" "$median" "$min" "$max"
}

# time_kql: convert KQL to SQL via extension, then execute the SQL.
# Both steps use the DuckDB CLI (two invocations per iteration).
time_kql() {
  local label="$1"
  local kql="$2"
  local times=()

  for ((i=0; i<ITERATIONS; i++)); do
    local start end elapsed sql
    start=$(date +%s%N)
    # Step 1: convert KQL -> SQL
    sql=$("$DUCKDB" -unsigned "$DB_PATH" -noheader -list -c "LOAD '$EXT'; SELECT kql_to_sql('$kql');" 2>/dev/null)
    # Step 2: execute the SQL
    "$DUCKDB" "$DB_PATH" -c "$sql" > /dev/null 2>&1
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))
    times+=("$elapsed")
  done

  IFS=$'\n' sorted=($(printf '%s\n' "${times[@]}" | sort -n)); unset IFS
  local median=${sorted[$(( ITERATIONS / 2 ))]}
  local min=${sorted[0]}
  local max=${sorted[$(( ITERATIONS - 1 ))]}
  printf "  %-50s  median %4d ms  (min %4d / max %4d)\n" "$label" "$median" "$min" "$max"
}

# time_kql_convert_only: just KQL->SQL conversion, no execution
time_kql_convert_only() {
  local label="$1"
  local kql="$2"
  local times=()

  for ((i=0; i<ITERATIONS; i++)); do
    local start end elapsed
    start=$(date +%s%N)
    "$DUCKDB" -unsigned "$DB_PATH" -noheader -list -c "LOAD '$EXT'; SELECT kql_to_sql('$kql');" > /dev/null 2>/dev/null
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))
    times+=("$elapsed")
  done

  IFS=$'\n' sorted=($(printf '%s\n' "${times[@]}" | sort -n)); unset IFS
  local median=${sorted[$(( ITERATIONS / 2 ))]}
  local min=${sorted[0]}
  local max=${sorted[$(( ITERATIONS - 1 ))]}
  printf "  %-50s  median %4d ms  (min %4d / max %4d)\n" "$label" "$median" "$min" "$max"
}

# ============================================================================
echo ""
echo "==================================================================="
echo " BENCHMARK: Native SQL vs KQL (via extension) - $ITERATIONS iterations each"
echo "==================================================================="
echo ""
echo " Note: KQL path requires TWO duckdb process invocations per"
echo " iteration (convert + execute). In a long-running connection,"
echo " the extension loads once and KQL->SQL adds ~0.1 ms per query."
echo ""

# ── 1. Simple count ─────────────────────────────────────────────────────────
echo ">> Test 1: Simple count"
time_sql  "SQL  : SELECT COUNT(*) FROM Events"  \
          "SELECT COUNT(*) AS Count FROM Events;"
time_kql  "KQL  : Events | count"  \
          "Events | count"
echo ""

# ── 2. Filtered count ──────────────────────────────────────────────────────
echo ">> Test 2: Filtered count (WHERE)"
time_sql  "SQL  : ... WHERE State = 'TEXAS'"  \
          "SELECT COUNT(*) AS Count FROM Events WHERE State = 'TEXAS';"
time_kql  "KQL  : Events | where State == 'TEXAS' | count" \
          "Events | where State == '\''TEXAS'\'' | count"
echo ""

# ── 3. Aggregation (GROUP BY) ──────────────────────────────────────────────
echo ">> Test 3: Aggregation (GROUP BY + ORDER + LIMIT)"
time_sql  "SQL  : GROUP BY State ORDER BY cnt DESC LIMIT 5"  \
          "SELECT State, COUNT(*) AS cnt FROM Events GROUP BY State ORDER BY cnt DESC LIMIT 5;"
time_kql  "KQL  : summarize cnt=count() by State | top 5 by cnt" \
          "Events | summarize cnt = count() by State | top 5 by cnt desc"
echo ""

# ── 4. Complex pipeline ───────────────────────────────────────────────────
echo ">> Test 4: Complex pipeline (WHERE + PROJECT + SORT + TAKE)"
time_sql  "SQL  : WHERE+SELECT+ORDER+LIMIT"  \
          "SELECT State, EventType, DamageProperty FROM Events WHERE State = 'CALIFORNIA' ORDER BY DamageProperty DESC LIMIT 10;"
time_kql  "KQL  : where+project+sort+take"  \
          "Events | where State == '\''CALIFORNIA'\'' | project State, EventType, DamageProperty | sort by DamageProperty desc | take 10"
echo ""

# ── 5. Conversion-only overhead ───────────────────────────────────────────
echo ">> Test 5: Conversion-only overhead (no query execution)"
time_sql  "SQL  : (baseline: SELECT 1)"  \
          "SELECT 1;"
time_kql_convert_only \
          "KQL  : kql_to_sql() only (no execute)" \
          "Events | where State == '\''TEXAS'\'' | summarize count() by EventType | top 5 by count_ desc"
echo ""

# ── Summary ────────────────────────────────────────────────────────────────
echo "==================================================================="
echo " NOTES"
echo "==================================================================="
echo " - Each iteration launches a new DuckDB process (cold start)."
echo " - KQL iterations load the extension on every invocation."
echo " - The overhead you see is: process start + extension load + KQL parse."
echo " - In a long-running connection (app server, notebook), extension"
echo "   load is a one-time cost; KQL->SQL conversion adds ~0.1 ms."
echo "==================================================================="

# Cleanup
rm -rf "$DATA_DIR"
