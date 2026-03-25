#!/usr/bin/env bash
# ============================================================================
# KQL Extension – CSV Ingestion & End-to-End Test
# ============================================================================
# Loads the real kql.duckdb_extension into DuckDB CLI, creates tables,
# ingests CSV data, and runs KQL queries against it – all from shell.
#
# The extension's kql_to_sql() returns a SQL string. DuckDB v1.2.0 does not
# support EXECUTE(<expr>), so end-to-end tests use a two-step approach:
#   1. Convert KQL to SQL via the extension (duckdb -noheader -list)
#   2. Execute the resulting SQL in a second duckdb invocation
#
# Usage:
#   ./tests/test-extension.sh [DUCKDB_CLI] [EXTENSION_PATH]
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DUCKDB="${1:-/tmp/duckdb}"
EXT="${2:-$REPO_ROOT/src/KqlToSql.DuckDbExtension/bin/Release/net10.0/linux-x64/publish/kql.duckdb_extension}"

PASS=0
FAIL=0
ERRORS=""

# ── Verify prerequisites ────────────────────────────────────────────────────
if [[ ! -x "$DUCKDB" ]]; then echo "ERROR: DuckDB CLI not found at $DUCKDB" >&2; exit 1; fi
if [[ ! -f "$EXT" ]]; then echo "ERROR: Extension not found at $EXT" >&2; exit 1; fi
echo "DuckDB CLI : $DUCKDB ($($DUCKDB --version 2>&1))"
echo "Extension  : $EXT"
echo ""

# ── Setup temp dir ──────────────────────────────────────────────────────────
WORK=$(mktemp -d)

# ── Helpers ─────────────────────────────────────────────────────────────────
# run_test: run a SQL statement, check output contains expected substring
run_test() {
  local name="$1"
  local db="$2"
  local sql="$3"
  local expect="$4"

  local output
  output=$("$DUCKDB" -unsigned "$db" -c "$sql" 2>&1) || true

  if echo "$output" | grep -qF "$expect"; then
    printf "  PASS  %-55s\n" "$name"
    PASS=$((PASS + 1))
  else
    printf "  FAIL  %-55s\n" "$name"
    ERRORS+="--- $name ---\nExpected substring: $expect\nGot:\n$output\n\n"
    FAIL=$((FAIL + 1))
  fi
}

# kql_convert: use the extension to convert KQL to SQL, print the SQL string
kql_convert() {
  local db="$1"
  local kql="$2"
  "$DUCKDB" -unsigned "$db" -noheader -list -c "LOAD '$EXT'; SELECT kql_to_sql('$kql');" 2>/dev/null
}

# run_kql_e2e: convert KQL via extension, execute result, check output
#   $1 = test name, $2 = db path, $3 = setup SQL (run first), $4 = KQL query, $5 = expected substring
run_kql_e2e() {
  local name="$1"
  local db="$2"
  local setup_sql="$3"
  local kql="$4"
  local expect="$5"

  # Run setup SQL (create table, load data, etc.)
  if [[ -n "$setup_sql" ]]; then
    "$DUCKDB" "$db" -c "$setup_sql" > /dev/null 2>&1 || true
  fi

  # Convert KQL to SQL via extension
  local sql
  sql=$(kql_convert "$db" "$kql") || true

  if [[ -z "$sql" ]]; then
    printf "  FAIL  %-55s\n" "$name"
    ERRORS+="--- $name ---\nKQL conversion returned empty. KQL: $kql\n\n"
    FAIL=$((FAIL + 1))
    return
  fi

  # Execute the converted SQL
  local output
  output=$("$DUCKDB" "$db" -c "$sql" 2>&1) || true

  if echo "$output" | grep -qF "$expect"; then
    printf "  PASS  %-55s\n" "$name"
    PASS=$((PASS + 1))
  else
    printf "  FAIL  %-55s\n" "$name"
    ERRORS+="--- $name ---\nKQL: $kql\nConverted SQL: $sql\nExpected substring: $expect\nGot:\n$output\n\n"
    FAIL=$((FAIL + 1))
  fi
}

# ============================================================================
echo "==================================================================="
echo " TEST SUITE: KQL DuckDB Extension (via CLI)"
echo "==================================================================="
echo ""

# ── 1. Extension loading ───────────────────────────────────────────────────
echo ">> Section 1: Extension Loading"

DB1="$WORK/s1.duckdb"
run_test "Load extension" "$DB1" \
  "LOAD '$EXT'; SELECT 'ext_loaded' AS status;" \
  "ext_loaded"

# ── 2. kql_to_sql() scalar function ────────────────────────────────────────
echo ""
echo ">> Section 2: kql_to_sql() - Conversion Tests"

DB2="$WORK/s2.duckdb"
run_test "Simple count" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('T | count');" \
  "SELECT COUNT(*) AS Count FROM T"

run_test "Where filter" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('T | where State == ''TEXAS''');" \
  "WHERE State = 'TEXAS'"

run_test "Project columns" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('T | project A, B');" \
  "SELECT A, B FROM T"

run_test "Sort + take" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('T | sort by Score desc | take 5');" \
  "ORDER BY Score DESC LIMIT 5"

run_test "Top operator" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('T | top 3 by Score desc');" \
  "ORDER BY Score DESC LIMIT 3"

run_test "Distinct" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('T | distinct State');" \
  "SELECT DISTINCT State FROM T"

run_test "Summarize count() by" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('T | summarize cnt=count() by State');" \
  "GROUP BY State"

run_test "Contains filter" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('T | where Name contains ''ali''');" \
  "ILIKE"

run_test "Extend with function" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('T | extend Upper = toupper(Name)');" \
  "UPPER"

run_test "Let / CTE" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('let x = T | where A > 1; x | count');" \
  "WITH"

run_test "Datatable expression" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('datatable(Name: string, Age: int)[''Alice'', 30, ''Bob'', 25]');" \
  "Alice"

run_test "Print expression" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('print x = 1, y = ''hello''');" \
  "hello"

# ── 3. kql_to_sql_dialect() ────────────────────────────────────────────────
echo ""
echo ">> Section 3: kql_to_sql_dialect() - Dialect Selection"

run_test "DuckDB dialect" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql_dialect('T | take 5', 'duckdb');" \
  "LIMIT 5"

run_test "PGlite dialect" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql_dialect('T | take 5', 'pglite');" \
  "LIMIT 5"

# ── 4. kql_explain() table function ────────────────────────────────────────
echo ""
echo ">> Section 4: kql_explain() - Table Function"

run_test "Explain returns kql_input column" "$DB2" \
  "LOAD '$EXT'; SELECT kql_input FROM kql_explain('T | take 5');" \
  "T | take 5"

run_test "Explain returns sql_output column" "$DB2" \
  "LOAD '$EXT'; SELECT sql_output FROM kql_explain('T | take 5');" \
  "LIMIT 5"

run_test "Explain returns dialect column" "$DB2" \
  "LOAD '$EXT'; SELECT dialect FROM kql_explain('T | count');" \
  "duckdb"

# ── 5. CSV Ingestion → KQL query end-to-end ────────────────────────────────
echo ""
echo ">> Section 5: CSV Ingestion + KQL Query (end-to-end)"

TEST_CSV="$WORK/scores.csv"
cat > "$TEST_CSV" << 'CSVEOF'
Id,Name,City,Score,Category
1,Alice,New York,95,A
2,Bob,Los Angeles,82,B
3,Charlie,Chicago,91,A
4,Diana,Houston,78,C
5,Eve,Phoenix,88,B
6,Frank,Philadelphia,73,C
7,Grace,San Antonio,96,A
8,Hank,San Diego,85,B
9,Ivy,Dallas,70,D
10,Jack,San Jose,92,A
CSVEOF

DB5="$WORK/s5.duckdb"
"$DUCKDB" "$DB5" -c "CREATE TABLE Scores AS SELECT * FROM read_csv_auto('$TEST_CSV');" > /dev/null 2>&1

run_kql_e2e "CSV: kql count" "$DB5" "" \
  "Scores | count" "10"

run_kql_e2e "CSV: kql where filter (Score > 90)" "$DB5" "" \
  "Scores | where Score > 90 | count" "4"

run_kql_e2e "CSV: kql project + take" "$DB5" "" \
  "Scores | project Name, Score | take 3" "Alice"

run_kql_e2e "CSV: kql summarize avg by Category" "$DB5" "" \
  "Scores | summarize avg_score = avg(Score) by Category | sort by avg_score desc" "Category"

run_kql_e2e "CSV: kql top 2 by Score" "$DB5" "" \
  "Scores | top 2 by Score desc" "Grace"

run_kql_e2e "CSV: kql distinct Category" "$DB5" "" \
  "Scores | distinct Category | sort by Category asc" "A"

run_kql_e2e "CSV: kql extend toupper" "$DB5" "" \
  "Scores | extend NameUpper = toupper(Name) | project NameUpper | take 1" "ALICE"

# ── 6. .ingest command conversion ──────────────────────────────────────────
echo ""
echo ">> Section 6: .ingest Command via kql_to_sql()"

run_test "Ingest inline -> INSERT conversion" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('.ingest inline into table T <| 1,foo
2,bar');" \
  "INSERT INTO T VALUES"

run_test "Ingest file -> COPY conversion" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('.ingest into table T ''$TEST_CSV''');" \
  "COPY T FROM"

# End-to-end: convert .ingest command, then execute
DB6="$WORK/s6.duckdb"
"$DUCKDB" "$DB6" -c "CREATE TABLE InlineTest (Id BIGINT, Name VARCHAR);" > /dev/null 2>&1
INGEST_SQL=$(kql_convert "$DB6" ".ingest inline into table InlineTest <| 1,Alice
2,Bob")
"$DUCKDB" "$DB6" -c "$INGEST_SQL" > /dev/null 2>&1
run_test "Ingest inline -> execute end-to-end" "$DB6" \
  "SELECT COUNT(*) AS cnt FROM InlineTest;" \
  "2"

# ── 7. .create table conversion ───────────────────────────────────────────
echo ""
echo ">> Section 7: .create table Command"

run_test "Create table -> DDL conversion" "$DB2" \
  "LOAD '$EXT'; SELECT kql_to_sql('.create table MyTable(Name: string, Age: int, Score: real)');" \
  "CREATE TABLE MyTable"

# End-to-end: .create table + .ingest + query
DB7="$WORK/s7.duckdb"
CREATE_SQL=$(kql_convert "$DB7" ".create table People(Name: string, Age: int)")
"$DUCKDB" "$DB7" -c "$CREATE_SQL" > /dev/null 2>&1
INGEST_SQL=$(kql_convert "$DB7" ".ingest inline into table People <| Alice,30
Bob,25")
"$DUCKDB" "$DB7" -c "$INGEST_SQL" > /dev/null 2>&1
run_kql_e2e "Create table + ingest + kql count" "$DB7" "" \
  "People | count" "2"

# ── 8. Large dataset end-to-end ────────────────────────────────────────────
echo ""
echo ">> Section 8: Large Dataset (100k rows)"

LARGE_CSV="$WORK/large.csv"
python3 -c "
import csv, random, sys
random.seed(42)
states = ['TEXAS','FLORIDA','CALIFORNIA','NEW YORK','OHIO']
events = ['Thunderstorm','Hail','Tornado','Flood','Storm']
w = csv.writer(sys.stdout)
w.writerow(['State','EventType','Damage','Year'])
for i in range(100000):
    w.writerow([random.choice(states), random.choice(events), random.randint(0,1000000), random.randint(2000,2023)])
" > "$LARGE_CSV"

DB8="$WORK/s8.duckdb"
"$DUCKDB" "$DB8" -c "CREATE TABLE BigEvents AS SELECT * FROM read_csv_auto('$LARGE_CSV');" > /dev/null 2>&1

run_kql_e2e "100k rows: kql count" "$DB8" "" \
  "BigEvents | count" "100000"

run_kql_e2e "100k rows: kql where TEXAS + count" "$DB8" "" \
  "BigEvents | where State == ''TEXAS'' | count" "200"

run_kql_e2e "100k rows: kql summarize by State" "$DB8" "" \
  "BigEvents | summarize cnt = count() by State | sort by cnt desc" "State"

# ============================================================================
echo ""
echo "==================================================================="
if [[ $FAIL -eq 0 ]]; then
  echo " ALL $PASS TESTS PASSED"
else
  echo " RESULTS: $PASS passed, $FAIL failed"
  echo ""
  echo " FAILURES:"
  echo -e "$ERRORS"
fi
echo "==================================================================="

# Cleanup
rm -rf "$WORK"

exit "$FAIL"
