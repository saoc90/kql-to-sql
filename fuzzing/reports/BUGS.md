# KQL→SQL Translator — Differential Fuzzing Findings

Oracle: Kustainer (real Kusto). SUT: KqlToSqlConverter → DuckDB.
Total verdicts: 1662. Bug candidates: 212.

## Counts by outcome

| Outcome | Count |
|---|---|
| Match | 1261 |
| MismatchRows | 202 |
| KustoError | 127 |
| SkippedNondeterministic | 34 |
| SkippedEngineError | 28 |
| MismatchOrder | 7 |
| MismatchColumns | 3 |

## Family: aggregation (24)

### `agent-aggregation-0034` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  {
    "a": 1
  },
  {
    "b": 2
  },
  1,
  2,
  3
]) duck=(["{\u0022a\u0022:1}","{\u0022b\u0022:2}","[1,2,3]"])

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1}), dynamic({"b":2}), dynamic([1,2,3]) ] | summarize make_list(d)
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(d) FILTER (WHERE d IS NOT NULL), []) AS list_d FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST('{"a":1}'::JSON AS JSON)), (CAST('{"b":2}'::JSON AS JSON)), (CAST(LIST_VALUE(1, 2, 3) AS JSON))) AS t(d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[list_d:Dynamic] rows=1
    - ([
  {
    "a": 1
  },
  {
    "b": 2
  },
  1,
  2,
  3
])
- DuckDB: cols=[list_d:Unknown] rows=1
    - (["{\u0022a\u0022:1}","{\u0022b\u0022:2}","[1,2,3]"])

### `agent-aggregation-0035` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, {
  "a": 1,
  "b": 2
}) duck=(1, {"{\u0022a\u0022:1}":1,"{\u0022b\u0022:2}":1})

**KQL**
```kql
datatable(k:long, v:dynamic)[ 1,dynamic({"a":1}), 1,dynamic({"b":2}), 2,dynamic({"c":3}) ] | summarize make_bag(v) by k
```
**Generated SQL**
```sql
SELECT k, histogram(v) AS bag_v FROM (SELECT k, CAST(v AS JSON) AS v FROM (VALUES (CAST(1 AS BIGINT), CAST('{"a":1}'::JSON AS JSON)), (CAST(1 AS BIGINT), CAST('{"b":2}'::JSON AS JSON)), (CAST(2 AS BIGINT), CAST('{"c":3}'::JSON AS JSON))) AS t(k, v)) GROUP BY ALL
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, bag_v:Dynamic] rows=2
    - (1, {
  "a": 1,
  "b": 2
})
    - (2, {
  "c": 3
})
- DuckDB: cols=[k:Int, bag_v:Unknown] rows=2
    - (1, {"{\u0022a\u0022:1}":1,"{\u0022b\u0022:2}":1})
    - (2, {"{\u0022c\u0022:3}":1})

### `agent-aggregation-0001` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 20, 'b', 20, 'b') duck=(2, null, null, null, null)

**KQL**
```kql
datatable(k:long, v:long, t:string)[ 1,long(null),"a", 1,20,"b", 2,long(null),"c", 2,long(null),"d" ] | summarize arg_max(v, t), arg_min(v, t) by k
```
**Generated SQL**
```sql
SELECT k, MAX(v) AS v, ARG_MAX(t, v) AS t, MIN(v) AS v1, ARG_MIN(t, v) AS t1 FROM (VALUES (CAST(1 AS BIGINT), CAST(CAST(NULL AS BIGINT) AS BIGINT), 'a'), (CAST(1 AS BIGINT), CAST(20 AS BIGINT), 'b'), (CAST(2 AS BIGINT), CAST(CAST(NULL AS BIGINT) AS BIGINT), 'c'), (CAST(2 AS BIGINT), CAST(CAST(NULL AS BIGINT) AS BIGINT), 'd')) AS t(k, v, t) GROUP BY ALL
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, v:Int, t:String, v1:Int, t1:String] rows=2
    - (1, 20, 'b', 20, 'b')
    - (2, null, 'c', null, 'c')
- DuckDB: cols=[k:Int, v:Int, t:String, v1:Int, t1:String] rows=2
    - (2, null, null, null, null)
    - (1, 20, 'b', 20, 'b')

### `agent-aggregation-0002` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH  
*Detail:* first differing row[0]: kusto=(1, NaN, NaN, NaN, 1, NaN) duck=(2, NaN, NaN, NaN, 2, NaN)

**KQL**
```kql
datatable(k:long, v:real)[ 1,1.5, 1,real(nan), 1,2.5, 2,real(nan) ] | summarize mx=max(v), mn=min(v), amx=arg_max(v, k), s=sum(v) by k
```
**Generated SQL**
```sql
SELECT k, MAX(v) AS mx, MIN(v) AS mn, MAX(v) AS amx, ARG_MAX(k, v) AS k, COALESCE(SUM(v), 0) AS s FROM (VALUES (CAST(1 AS BIGINT), CAST(1.5 AS DOUBLE)), (CAST(1 AS BIGINT), CAST(CAST('nan' AS DOUBLE) AS DOUBLE)), (CAST(1 AS BIGINT), CAST(2.5 AS DOUBLE)), (CAST(2 AS BIGINT), CAST(CAST('nan' AS DOUBLE) AS DOUBLE))) AS t(k, v) GROUP BY ALL
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, mx:Real, mn:Real, amx:Real, k1:Int, s:Real] rows=2
    - (1, NaN, NaN, NaN, 1, NaN)
    - (2, NaN, NaN, NaN, 2, NaN)
- DuckDB: cols=[k:Int, mx:Real, mn:Real, amx:Real, k:Int, s:Real] rows=2
    - (2, NaN, NaN, NaN, 2, NaN)
    - (1, NaN, 1.5, NaN, 1, NaN)

### `agent-aggregation-0004` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, {
  "a": 1,
  "b": 3,
  "c": 4
}) duck=(2, {"{\u0022a\u0022:9}":1})

**KQL**
```kql
datatable(k:long, v:dynamic)[ 1,dynamic({"a":1}), 1,dynamic({"a":2,"b":3}), 1,dynamic({"c":4}), 2,dynamic({"a":9}) ] | summarize bag=make_bag(v) by k
```
**Generated SQL**
```sql
SELECT k, histogram(v) AS bag FROM (SELECT k, CAST(v AS JSON) AS v FROM (VALUES (CAST(1 AS BIGINT), CAST('{"a":1}'::JSON AS JSON)), (CAST(1 AS BIGINT), CAST('{"a":2,"b":3}'::JSON AS JSON)), (CAST(1 AS BIGINT), CAST('{"c":4}'::JSON AS JSON)), (CAST(2 AS BIGINT), CAST('{"a":9}'::JSON AS JSON))) AS t(k, v)) GROUP BY ALL
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, bag:Dynamic] rows=2
    - (1, {
  "a": 1,
  "b": 3,
  "c": 4
})
    - (2, {
  "a": 9
})
- DuckDB: cols=[k:Int, bag:Unknown] rows=2
    - (2, {"{\u0022a\u0022:9}":1})
    - (1, {"{\u0022a\u0022:1}":1,"{\u0022a\u0022:2,\u0022b\u0022:3}":1,"{\u0022c\u0022:4}":1})

### `agent-aggregation-0005` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH  
*Detail:* first differing row[0]: kusto=(1, {
  "x": 1,
  "y": 2
}, [
  {
    "x": 1
  },
  null,
  {
    "y": 2
  }
], [
  {
    "x": 1
  },
  null,
  {
    "y": 2
  }
]) duck=(2, null, [], [null])

**KQL**
```kql
datatable(k:long, v:dynamic)[ 1,dynamic({"x":1}), 1,dynamic(null), 1,dynamic({"y":2}), 2,dynamic(null) ] | summarize make_bag(v), make_list(v), make_list_with_nulls(v) by k
```
**Generated SQL**
```sql
SELECT k, histogram(v) AS bag_v, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS list_v, COALESCE(LIST(v), []) AS list_v FROM (SELECT k, CAST(v AS JSON) AS v FROM (VALUES (CAST(1 AS BIGINT), CAST('{"x":1}'::JSON AS JSON)), (CAST(1 AS BIGINT), CAST(NULL AS JSON)), (CAST(1 AS BIGINT), CAST('{"y":2}'::JSON AS JSON)), (CAST(2 AS BIGINT), CAST(NULL AS JSON))) AS t(k, v)) GROUP BY ALL
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, bag_v:Dynamic, list_v:Dynamic, list_v1:Dynamic] rows=2
    - (1, {
  "x": 1,
  "y": 2
}, [
  {
    "x": 1
  },
  null,
  {
    "y": 2
  }
], [
  {
    "x": 1
  },
  null,
  {
    "y": 2
  }
])
    - (2, {}, [
  null
], [
  null
])
- DuckDB: cols=[k:Int, bag_v:Unknown, list_v:Unknown, list_v:Unknown] rows=2
    - (2, null, [], [null])
    - (1, {"{\u0022x\u0022:1}":1,"{\u0022y\u0022:2}":1}, ["{\u0022x\u0022:1}","{\u0022y\u0022:2}"], ["{\u0022x\u0022:1}",null,"{\u0022y\u0022:2}"])

### `agent-aggregation-0025` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(-9223372036854775806, 9223372036854775807, 1, -3.0744573456182584E+18) duck=(9223372036854775810, 9223372036854775807, 1, 3.0744573456182584E+18)

**KQL**
```kql
datatable(v:long)[ 9223372036854775807, 1, 2 ] | summarize sum(v), max(v), min(v), avg(v)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(v), 0) AS sum_v, MAX(v) AS max_v, MIN(v) AS min_v, COALESCE(AVG(v), 'nan'::DOUBLE) AS avg_v FROM (VALUES (CAST(9223372036854775807 AS BIGINT)), (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT))) AS t(v)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[sum_v:Int, max_v:Int, min_v:Int, avg_v:Real] rows=1
    - (-9223372036854775806, 9223372036854775807, 1, -3.0744573456182584E+18)
- DuckDB: cols=[sum_v:Int, max_v:Int, min_v:Int, avg_v:Real] rows=1
    - (9223372036854775810, 9223372036854775807, 1, 3.0744573456182584E+18)

### `agent-aggregation-0027` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH  
*Detail:* first differing row[0]: kusto=(1, 1, 2, 2, 4) duck=(1, 3, 0, 2, 4)

**KQL**
```kql
datatable(b:bool, v:long)[ bool(null),1, true,2, false,3, bool(null),4 ] | summarize countif(b), countif(not(b)), countif(isnull(b)), sumif(v, b), count()
```
**Generated SQL**
```sql
SELECT COUNT(*) FILTER (WHERE b) AS countif_, COUNT(*) FILTER (WHERE NOT (b)) AS countif_, COUNT(*) FILTER (WHERE (b IS NULL)) AS countif_, COALESCE(SUM(v) FILTER (WHERE b), 0) AS sumif_v, COUNT(*) AS count_ FROM (VALUES (FALSE, CAST(1 AS BIGINT)), (TRUE, CAST(2 AS BIGINT)), (FALSE, CAST(3 AS BIGINT)), (FALSE, CAST(4 AS BIGINT))) AS t(b, v)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[countif_b:Int, countif_:Int, countif_1:Int, sumif_v:Int, count_:Int] rows=1
    - (1, 1, 2, 2, 4)
- DuckDB: cols=[countif_:Int, countif_:Int, countif_:Int, sumif_v:Int, count_:Int] rows=1
    - (1, 3, 0, 2, 4)

### `agent-aggregation-0030` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, [
  2,
  1
], 2) duck=(2, [1], 1)

**KQL**
```kql
datatable(x:long, y:long)[ 1,1, 1,1, 1,2, 2,1 ] | summarize c=count() by x, y | summarize subtotals=make_list(c), n=count() by x
```
**Generated SQL**
```sql
SELECT x, COALESCE(LIST(c) FILTER (WHERE c IS NOT NULL), []) AS subtotals, COUNT(*) AS n FROM (SELECT x, y, COUNT(*) AS c FROM (VALUES (CAST(1 AS BIGINT), CAST(1 AS BIGINT)), (CAST(1 AS BIGINT), CAST(1 AS BIGINT)), (CAST(1 AS BIGINT), CAST(2 AS BIGINT)), (CAST(2 AS BIGINT), CAST(1 AS BIGINT))) AS t(x, y) GROUP BY ALL) GROUP BY ALL
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, subtotals:Dynamic, n:Int] rows=2
    - (1, [
  2,
  1
], 2)
    - (2, [
  1
], 1)
- DuckDB: cols=[x:Int, subtotals:Unknown, n:Int] rows=2
    - (2, [1], 1)
    - (1, [1,2], 2)

### `agent-aggregation-0043` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=({
  "1": 2,
  "2": 3,
  "3": 1
}) duck=({"{\u00221\u0022:2}":1,"{\u00222\u0022:3}":1,"{\u00223\u0022:1}":1})

**KQL**
```kql
datatable(k:long)[ 1,1,2,2,2,3 ] | summarize c=count() by k | summarize histo=make_bag(pack(tostring(k), c))
```
**Generated SQL**
```sql
SELECT histogram(json_object(COALESCE(TRY_CAST(k AS TEXT), ''), c)) AS histo FROM (SELECT k, COUNT(*) AS c FROM (VALUES (CAST(1 AS BIGINT)), (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT))) AS t(k) GROUP BY ALL)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[histo:Dynamic] rows=1
    - ({
  "1": 2,
  "2": 3,
  "3": 1
})
- DuckDB: cols=[histo:Unknown] rows=1
    - ({"{\u00221\u0022:2}":1,"{\u00222\u0022:3}":1,"{\u00223\u0022:1}":1})

### `agent-aggregation-0001` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, NaN, 10, NaN, 10, 20, 2) duck=(1, NaN, 10, 2, 20, 20, 2)

**KQL**
```kql
datatable(k:long, v:real, w:real)[ 1,real(nan),10.0, 1,2.0,20.0, 2,real(inf),5.0, 2,3.0,real(nan) ] | summarize amx=arg_max(v, w), amn=arg_min(v, w), amxw=arg_max(w, v) by k | order by k asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT k, MAX(v) AS amx, ARG_MAX(w, v) AS w, MIN(v) AS amn, ARG_MIN(w, v) AS w1, MAX(w) AS amxw, ARG_MAX(v, w) AS v FROM (VALUES (CAST(1 AS BIGINT), CAST(CAST('nan' AS DOUBLE) AS DOUBLE), CAST(10.0 AS DOUBLE)), (CAST(1 AS BIGINT), CAST(2.0 AS DOUBLE), CAST(20.0 AS DOUBLE)), (CAST(2 AS BIGINT), CAST(CAST('inf' AS DOUBLE) AS DOUBLE), CAST(5.0 AS DOUBLE)), (CAST(2 AS BIGINT), CAST(3.0 AS DOUBLE), CAST(CAST('nan' AS DOUBLE) AS DOUBLE))) AS t(k, v, w) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, amx:Real, w:Real, amn:Real, w1:Real, amxw:Real, v:Real] rows=2
    - (1, NaN, 10, NaN, 10, 20, 2)
    - (2, Infinity, 5, 3, NaN, NaN, 3)
- DuckDB: cols=[k:Int, amx:Real, w:Real, amn:Real, w1:Real, amxw:Real, v:Real] rows=2
    - (1, NaN, 10, 2, 20, 20, 2)
    - (2, Infinity, 5, 3, NaN, NaN, 3)

### `agent-aggregation-0006` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, {
  "a": 1,
  "b": 2,
  "c": 3
}, [
  {
    "a": 1,
    "b": 2
  },
  {
    "a": 10,
    "c": 3
  },
  {
    "b": 99
  }
]) duck=(1, {"{\u0022a\u0022:1,\u0022b\u0022:2}":1,"{\u0022a\u0022:10,\u0022c\u0022:3}":1,"{\u0022b\u0022:99}":1}, ["{\u0022a\u0022:1,\u0022b\u0022:2}","{\u0022a\u0022:10,\u0022c\u0022:3}","{\u0022b\u0022:99}"])

**KQL**
```kql
datatable(g:long, v:dynamic)[ 1,dynamic({"a":1,"b":2}), 1,dynamic({"a":10,"c":3}), 1,dynamic({"b":99}), 2,dynamic({"a":5}) ] | summarize bag=make_bag(v), lst=make_list(v) by g | order by g asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT g, histogram(v) AS bag, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS lst FROM (SELECT g, CAST(v AS JSON) AS v FROM (VALUES (CAST(1 AS BIGINT), CAST('{"a":1,"b":2}'::JSON AS JSON)), (CAST(1 AS BIGINT), CAST('{"a":10,"c":3}'::JSON AS JSON)), (CAST(1 AS BIGINT), CAST('{"b":99}'::JSON AS JSON)), (CAST(2 AS BIGINT), CAST('{"a":5}'::JSON AS JSON))) AS t(g, v)) GROUP BY ALL) ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:Int, bag:Dynamic, lst:Dynamic] rows=2
    - (1, {
  "a": 1,
  "b": 2,
  "c": 3
}, [
  {
    "a": 1,
    "b": 2
  },
  {
    "a": 10,
    "c": 3
  },
  {
    "b": 99
  }
])
    - (2, {
  "a": 5
}, [
  {
    "a": 5
  }
])
- DuckDB: cols=[g:Int, bag:Unknown, lst:Unknown] rows=2
    - (1, {"{\u0022a\u0022:1,\u0022b\u0022:2}":1,"{\u0022a\u0022:10,\u0022c\u0022:3}":1,"{\u0022b\u0022:99}":1}, ["{\u0022a\u0022:1,\u0022b\u0022:2}","{\u0022a\u0022:10,\u0022c\u0022:3}","{\u0022b\u0022:99}"])
    - (2, {"{\u0022a\u0022:5}":1}, ["{\u0022a\u0022:5}"])

### `agent-aggregation-0007` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  2,
  4,
  6,
  8,
  10
], [
  null,
  2,
  null,
  4,
  null,
  6,
  null,
  8,
  null,
  10
]) duck=([2,4,6,8,10], '<unreadable:IndexOutOfRangeException>')

**KQL**
```kql
datatable(x:long)[ 1,2,3,4,5,6,7,8,9,10 ] | summarize lo=make_list(iif(x%2==0, x, long(null))), lon=make_list_with_nulls(iif(x%2==0, x, long(null)))
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(CASE WHEN (((x) % NULLIF(2, 0)) + ABS(2)) % NULLIF(2, 0) = 0 THEN x ELSE CAST(NULL AS BIGINT) END) FILTER (WHERE CASE WHEN (((x) % NULLIF(2, 0)) + ABS(2)) % NULLIF(2, 0) = 0 THEN x ELSE CAST(NULL AS BIGINT) END IS NOT NULL), []) AS lo, COALESCE(LIST(CASE WHEN (((x) % NULLIF(2, 0)) + ABS(2)) % NULLIF(2, 0) = 0 THEN x ELSE CAST(NULL AS BIGINT) END), []) AS lon FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT)), (CAST(4 AS BIGINT)), (CAST(5 AS BIGINT)), (CAST(6 AS BIGINT)), (CAST(7 AS BIGINT)), (CAST(8 AS BIGINT)), (CAST(9 AS BIGINT)), (CAST(10 AS BIGINT))) AS t(x)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[lo:Dynamic, lon:Dynamic] rows=1
    - ([
  2,
  4,
  6,
  8,
  10
], [
  null,
  2,
  null,
  4,
  null,
  6,
  null,
  8,
  null,
  10
])
- DuckDB: cols=[lo:Unknown, lon:Unknown] rows=1
    - ([2,4,6,8,10], '<unreadable:IndexOutOfRangeException>')

### `agent-aggregation-0017` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(0, 0, [], [], {}, NaN, 0) duck=(0, 0, [], [], null, NaN, 0)

**KQL**
```kql
datatable(x:long)[ 100,200,300 ] | where x < 0 | summarize s=sum(x), c=count(), ml=make_list(x), ms=make_set(x), mb=make_bag(pack("k", x)), av=avg(x), st=stdev(x)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(x), 0) AS s, COUNT(*) AS c, COALESCE(LIST(x) FILTER (WHERE x IS NOT NULL), []) AS ml, COALESCE(LIST(DISTINCT x) FILTER (WHERE x IS NOT NULL), []) AS ms, histogram(json_object('k', x)) AS mb, COALESCE(AVG(x), 'nan'::DOUBLE) AS av, COALESCE(STDDEV_SAMP(x), 0) AS st FROM (SELECT * FROM (VALUES (CAST(100 AS BIGINT)), (CAST(200 AS BIGINT)), (CAST(300 AS BIGINT))) AS t(x) WHERE x < 0)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, c:Int, ml:Dynamic, ms:Dynamic, mb:Dynamic, av:Real, st:Real] rows=1
    - (0, 0, [], [], {}, NaN, 0)
- DuckDB: cols=[s:Int, c:Int, ml:Unknown, ms:Unknown, mb:Unknown, av:Real, st:Real] rows=1
    - (0, 0, [], [], null, NaN, 0)

### `agent-aggregation-0021` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 5, 5, 3, 2, [
  5
], [
  null,
  5,
  null
], 5) duck=(1, 5, 5, 3, 2, [5], '<unreadable:IndexOutOfRangeException>', 5)

**KQL**
```kql
datatable(k:long, v:int)[ 1,int(null), 1,5, 1,int(null), 2,int(null) ] | summarize s=sum(v), av=avg(v), c=count(), cif=countif(isnull(v)), ml=make_list(v), mln=make_list_with_nulls(v), mx=max(v) by k | order by k asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT k, COALESCE(SUM(v), 0) AS s, COALESCE(AVG(v), 'nan'::DOUBLE) AS av, COUNT(*) AS c, COUNT(*) FILTER (WHERE (v IS NULL)) AS cif, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS ml, COALESCE(LIST(v), []) AS mln, MAX(v) AS mx FROM (VALUES (CAST(1 AS BIGINT), CAST(NULL AS INTEGER)), (CAST(1 AS BIGINT), 5), (CAST(1 AS BIGINT), CAST(NULL AS INTEGER)), (CAST(2 AS BIGINT), CAST(NULL AS INTEGER))) AS t(k, v) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, s:Int, av:Real, c:Int, cif:Int, ml:Dynamic, mln:Dynamic, mx:Int] rows=2
    - (1, 5, 5, 3, 2, [
  5
], [
  null,
  5,
  null
], 5)
    - (2, 0, NaN, 1, 1, [], [
  null
], null)
- DuckDB: cols=[k:Int, s:Int, av:Real, c:Int, cif:Int, ml:Unknown, mln:Unknown, mx:Int] rows=2
    - (1, 5, 5, 3, 2, [5], '<unreadable:IndexOutOfRangeException>', 5)
    - (2, 0, NaN, 1, 1, [], '<unreadable:IndexOutOfRangeException>', null)

### `agent-aggregation-0023` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=('b', NaN, NaN, NaN, NaN, [
  3.3,
  "NaN"
]) duck=('b', NaN, NaN, 3.3, NaN, '<unreadable:IndexOutOfRangeException>')

**KQL**
```kql
datatable(g:string, v:real)[ "a",1.1, "a",2.2, "b",3.3, "b",real(nan), "c",real(inf) ] | summarize s=sum(v), mx=max(v), mn=min(v), av=avg(v), ml=make_list(v) by g | order by g asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT g, COALESCE(SUM(v), 0) AS s, MAX(v) AS mx, MIN(v) AS mn, COALESCE(AVG(v), 'nan'::DOUBLE) AS av, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS ml FROM (VALUES ('a', CAST(1.1 AS DOUBLE)), ('a', CAST(2.2 AS DOUBLE)), ('b', CAST(3.3 AS DOUBLE)), ('b', CAST(CAST('nan' AS DOUBLE) AS DOUBLE)), ('c', CAST(CAST('inf' AS DOUBLE) AS DOUBLE))) AS t(g, v) GROUP BY ALL) ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, s:Real, mx:Real, mn:Real, av:Real, ml:Dynamic] rows=3
    - ('a', 3.3000000000000003, 2.2, 1.1, 1.6500000000000001, [
  1.1,
  2.2
])
    - ('b', NaN, NaN, NaN, NaN, [
  3.3,
  "NaN"
])
    - ('c', Infinity, Infinity, Infinity, Infinity, [
  "Infinity"
])
- DuckDB: cols=[g:String, s:Real, mx:Real, mn:Real, av:Real, ml:Unknown] rows=3
    - ('a', 3.3000000000000003, 2.2, 1.1, 1.6500000000000001, [1.1,2.2])
    - ('b', NaN, NaN, 3.3, NaN, '<unreadable:IndexOutOfRangeException>')
    - ('c', Infinity, Infinity, Infinity, Infinity, '<unreadable:IndexOutOfRangeException>')

### `agent-aggregation-0024` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-01-01T00:00:00.0000000Z, 2020-01-10T00:00:00.0000000Z, 3, 9.00:00:00, [
  "2020-01-01T00:00:00.0000000Z",
  "2020-01-05T00:00:00.0000000Z",
  "2020-01-10T00:00:00.0000000Z"
]) duck=(2020-01-01T00:00:00.0000000Z, 2020-01-10T00:00:00.0000000Z, 3, 9.00:00:00, ["2020-01-01T00:00:00","2020-01-05T00:00:00","2020-01-10T00:00:00"])

**KQL**
```kql
datatable(d:datetime)[ datetime(2020-01-01), datetime(2020-01-05), datetime(2020-01-10) ] | summarize mn=min(d), mx=max(d), cnt=count(), span=max(d)-min(d), ml=make_list(d)
```
**Generated SQL**
```sql
SELECT MIN(d) AS mn, MAX(d) AS mx, COUNT(*) AS cnt, max(d) - min(d) AS span, COALESCE(LIST(d) FILTER (WHERE d IS NOT NULL), []) AS ml FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00'), (TIMESTAMP '2020-01-05 00:00:00'), (TIMESTAMP '2020-01-10 00:00:00')) AS t(d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[mn:DateTime, mx:DateTime, cnt:Int, span:TimeSpan, ml:Dynamic] rows=1
    - (2020-01-01T00:00:00.0000000Z, 2020-01-10T00:00:00.0000000Z, 3, 9.00:00:00, [
  "2020-01-01T00:00:00.0000000Z",
  "2020-01-05T00:00:00.0000000Z",
  "2020-01-10T00:00:00.0000000Z"
])
- DuckDB: cols=[mn:DateTime, mx:DateTime, cnt:Int, span:TimeSpan, ml:Unknown] rows=1
    - (2020-01-01T00:00:00.0000000Z, 2020-01-10T00:00:00.0000000Z, 3, 9.00:00:00, ["2020-01-01T00:00:00","2020-01-05T00:00:00","2020-01-10T00:00:00"])

### `agent-aggregation-0027` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1000, 700, 2, [
  200,
  400
]) duck=(1000, 700, 2, [400,200])

**KQL**
```kql
datatable(g:long, v:long)[ 1,100, 1,200, 2,300, 2,400 ] | summarize tot=sum(v), amx=arg_max(v, g) by g | summarize gtot=sum(tot), maxgrp=arg_max(tot, g), allamx=make_list(amx)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(tot), 0) AS gtot, MAX(tot) AS maxgrp, ARG_MAX(g, tot) AS g, COALESCE(LIST(amx) FILTER (WHERE amx IS NOT NULL), []) AS allamx FROM (SELECT g, COALESCE(SUM(v), 0) AS tot, MAX(v) AS amx, ARG_MAX(g, v) AS g FROM (VALUES (CAST(1 AS BIGINT), CAST(100 AS BIGINT)), (CAST(1 AS BIGINT), CAST(200 AS BIGINT)), (CAST(2 AS BIGINT), CAST(300 AS BIGINT)), (CAST(2 AS BIGINT), CAST(400 AS BIGINT))) AS t(g, v) GROUP BY ALL)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[gtot:Int, maxgrp:Int, g:Int, allamx:Dynamic] rows=1
    - (1000, 700, 2, [
  200,
  400
])
- DuckDB: cols=[gtot:Int, maxgrp:Int, g:Int, allamx:Unknown] rows=1
    - (1000, 700, 2, [400,200])

### `agent-aggregation-0029` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3,
  1,
  2,
  1
], 6, 3) duck=([[1],[1,2],[1,2,3]], 6, 3)

**KQL**
```kql
datatable(x:long, y:long)[ 1,1, 1,2, 1,3, 2,1, 2,2, 3,1 ] | summarize inner=make_list(y), cnt=count() by x | summarize outer_lists=make_list(inner), totcnt=sum(cnt), grps=count()
```
**Generated SQL**
```sql
SELECT COALESCE(LIST("inner") FILTER (WHERE "inner" IS NOT NULL), []) AS outer_lists, COALESCE(SUM(cnt), 0) AS totcnt, COUNT(*) AS grps FROM (SELECT x, COALESCE(LIST(y) FILTER (WHERE y IS NOT NULL), []) AS "inner", COUNT(*) AS cnt FROM (VALUES (CAST(1 AS BIGINT), CAST(1 AS BIGINT)), (CAST(1 AS BIGINT), CAST(2 AS BIGINT)), (CAST(1 AS BIGINT), CAST(3 AS BIGINT)), (CAST(2 AS BIGINT), CAST(1 AS BIGINT)), (CAST(2 AS BIGINT), CAST(2 AS BIGINT)), (CAST(3 AS BIGINT), CAST(1 AS BIGINT))) AS t(x, y) GROUP BY ALL)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[outer_lists:Dynamic, totcnt:Int, grps:Int] rows=1
    - ([
  1,
  2,
  3,
  1,
  2,
  1
], 6, 3)
- DuckDB: cols=[outer_lists:Unknown, totcnt:Int, grps:Int] rows=1
    - ([[1],[1,2],[1,2,3]], 6, 3)

### `agent-aggregation-0032` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 4, 2, 2.5, 1.5, [
  "1.5",
  "2.5"
]) duck=(1, 4, 2, 2.5, 1.5, [1.5,2.5])

**KQL**
```kql
datatable(k:long, v:decimal)[ 1,decimal(1.5), 1,decimal(2.5), 2,decimal(3.5) ] | summarize s=sum(v), av=avg(v), mx=max(v), mn=min(v), ml=make_list(v) by k | order by k asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT k, COALESCE(SUM(v), 0) AS s, COALESCE(AVG(v), 'nan'::DOUBLE) AS av, MAX(v) AS mx, MIN(v) AS mn, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS ml FROM (VALUES (CAST(1 AS BIGINT), 1.5), (CAST(1 AS BIGINT), 2.5), (CAST(2 AS BIGINT), 3.5)) AS t(k, v) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, s:Real, av:Real, mx:Real, mn:Real, ml:Dynamic] rows=2
    - (1, 4, 2, 2.5, 1.5, [
  "1.5",
  "2.5"
])
    - (2, 3.5, 3.5, 3.5, 3.5, [
  "3.5"
])
- DuckDB: cols=[k:Int, s:Real, av:Real, mx:Real, mn:Real, ml:Unknown] rows=2
    - (1, 4, 2, 2.5, 1.5, [1.5,2.5])
    - (2, 3.5, 3.5, 3.5, 3.5, [3.5])

### `agent-aggregation-0033` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(9223372036854775807, 1, -0.6666666666666666, [
  9223372036854775806,
  1,
  9223372036854775807
]) duck=(9223372036854775807, 1, 6.148914691236517E+18, [9223372036854775806,1,9223372036854775807])

**KQL**
```kql
datatable(x:long)[ 9223372036854775806, 1, 9223372036854775807 ] | summarize mx=max(x), mn=min(x), av=avg(x), ml=make_list(x)
```
**Generated SQL**
```sql
SELECT MAX(x) AS mx, MIN(x) AS mn, COALESCE(AVG(x), 'nan'::DOUBLE) AS av, COALESCE(LIST(x) FILTER (WHERE x IS NOT NULL), []) AS ml FROM (VALUES (CAST(9223372036854775806 AS BIGINT)), (CAST(1 AS BIGINT)), (CAST(9223372036854775807 AS BIGINT))) AS t(x)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[mx:Int, mn:Int, av:Real, ml:Dynamic] rows=1
    - (9223372036854775807, 1, -0.6666666666666666, [
  9223372036854775806,
  1,
  9223372036854775807
])
- DuckDB: cols=[mx:Int, mn:Int, av:Real, ml:Unknown] rows=1
    - (9223372036854775807, 1, 6.148914691236517E+18, [9223372036854775806,1,9223372036854775807])

### `agent-aggregation-0037` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3
], [
  1,
  2
]) duck=([1,2,3,4,5,6], [1,4,3,6,2,5])

**KQL**
```kql
datatable(v:long)[ 1,2,3,4,5,6 ] | summarize a=make_list(v, 3), b=make_set(v, 2)
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS a, COALESCE(LIST(DISTINCT v) FILTER (WHERE v IS NOT NULL), []) AS b FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT)), (CAST(4 AS BIGINT)), (CAST(5 AS BIGINT)), (CAST(6 AS BIGINT))) AS t(v)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Dynamic, b:Dynamic] rows=1
    - ([
  1,
  2,
  3
], [
  1,
  2
])
- DuckDB: cols=[a:Unknown, b:Unknown] rows=1
    - ([1,2,3,4,5,6], [1,4,3,6,2,5])

### `agent-aggregation-0039` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('p', [
  1,
  2,
  3,
  4
], [
  1,
  2,
  3,
  4
]) duck=('p', ["[1,2]","[3,4]"], ["[1,2]","[3,4]"])

**KQL**
```kql
datatable(g:string, v:dynamic)[ "p",dynamic([1,2]), "p",dynamic([3,4]), "q",dynamic(null), "q",dynamic([5]) ] | summarize ml=make_list(v), mln=make_list_with_nulls(v) by g | order by g asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT g, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS ml, COALESCE(LIST(v), []) AS mln FROM (SELECT g, CAST(v AS JSON) AS v FROM (VALUES ('p', CAST(LIST_VALUE(1, 2) AS JSON)), ('p', CAST(LIST_VALUE(3, 4) AS JSON)), ('q', CAST(NULL AS JSON)), ('q', CAST(LIST_VALUE(5) AS JSON))) AS t(g, v)) GROUP BY ALL) ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, ml:Dynamic, mln:Dynamic] rows=2
    - ('p', [
  1,
  2,
  3,
  4
], [
  1,
  2,
  3,
  4
])
    - ('q', [
  null,
  5
], [
  null,
  5
])
- DuckDB: cols=[g:String, ml:Unknown, mln:Unknown] rows=2
    - ('p', ["[1,2]","[3,4]"], ["[1,2]","[3,4]"])
    - ('q', ["[5]"], [null,"[5]"])

### `agent-aggregation-0042` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[histlist:Dynamic|String]  
*Detail:* first differing row[0]: kusto=({
  "1": 2,
  "2": 1,
  "3": 2
}, [
  {
    "g": 1,
    "c": 2
  },
  {
    "g": 2,
    "c": 1
  },
  {
    "g": 3,
    "c": 2
  }
]) duck=({"{\u00221\u0022:2}":1,"{\u00222\u0022:1}":1,"{\u00223\u0022:2}":1}, '[{"g":2,"c":1},{"g":3,"c":2},{"g":1,"c":2}]')

**KQL**
```kql
datatable(g:long, v:long)[ 1,10, 1,20, 2,30, 3,40, 3,50 ] | summarize cnt=count() by g | summarize bag=make_bag(pack(tostring(g), cnt)), histlist=make_list(pack("g", g, "c", cnt))
```
**Generated SQL**
```sql
SELECT histogram(json_object(COALESCE(TRY_CAST(g AS TEXT), ''), cnt)) AS bag, TO_JSON(COALESCE(LIST(json_object('g', g, 'c', cnt)) FILTER (WHERE json_object('g', g, 'c', cnt) IS NOT NULL), [])) AS histlist FROM (SELECT g, COUNT(*) AS cnt FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(3 AS BIGINT), CAST(40 AS BIGINT)), (CAST(3 AS BIGINT), CAST(50 AS BIGINT))) AS t(g, v) GROUP BY ALL)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[bag:Dynamic, histlist:Dynamic] rows=1
    - ({
  "1": 2,
  "2": 1,
  "3": 2
}, [
  {
    "g": 1,
    "c": 2
  },
  {
    "g": 2,
    "c": 1
  },
  {
    "g": 3,
    "c": 2
  }
])
- DuckDB: cols=[bag:Unknown, histlist:String] rows=1
    - ({"{\u00221\u0022:2}":1,"{\u00222\u0022:1}":1,"{\u00223\u0022:2}":1}, '[{"g":2,"c":1},{"g":3,"c":2},{"g":1,"c":2}]')

## Family: datetime-timespan (30)

### `agent-datetime-timespan-0043` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-01-01T00:00:00.0000001Z, 2020, '0000001', 2020-12-31T23:59:59.9999999Z) duck=(2020-01-01T00:00:00.0000000Z, 2020, '000000', 2020-12-31T23:59:59.9999990Z)

**KQL**
```kql
datatable(t:datetime)[ datetime(2020-01-01 00:00:00.0000001), datetime(2020-12-31 23:59:59.9999999) ] | extend yr = getyear(t), tick = format_datetime(t, 'fffffff'), eoy = endofyear(t)
```
**Generated SQL**
```sql
SELECT *, EXTRACT(YEAR FROM t) AS yr, STRFTIME(t, '%f') AS tick, DATE_TRUNC('year', t) + INTERVAL '1 year' - INTERVAL '1 microsecond' AS eoy FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00.000000'), (TIMESTAMP '2020-12-31 23:59:59.999999')) AS t(t)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, yr:Int, tick:String, eoy:DateTime] rows=2
    - (2020-01-01T00:00:00.0000001Z, 2020, '0000001', 2020-12-31T23:59:59.9999999Z)
    - (2020-12-31T23:59:59.9999999Z, 2020, '9999999', 2020-12-31T23:59:59.9999999Z)
- DuckDB: cols=[t:DateTime, yr:Int, tick:String, eoy:DateTime] rows=2
    - (2020-01-01T00:00:00.0000000Z, 2020, '000000', 2020-12-31T23:59:59.9999990Z)
    - (2020-12-31T23:59:59.9999990Z, 2020, '999999', 2020-12-31T23:59:59.9999990Z)

### `agent-datetime-timespan-0014` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('2020-06-15 13:45:09.1234567', '1234567') duck=('2020-06-15 13:45:09.123456', '123456')

**KQL**
```kql
print f1 = format_datetime(datetime(2020-06-15 13:45:09.1234567), 'yyyy-MM-dd HH:mm:ss.fffffff'), f2 = format_datetime(datetime(2020-06-15 13:45:09.1234567), 'FFFFFFF')
```
**Generated SQL**
```sql
SELECT STRFTIME(TIMESTAMP '2020-06-15 13:45:09.123456', '%Y-%m-%d %H:%M:%S.%f') AS f1, STRFTIME(TIMESTAMP '2020-06-15 13:45:09.123456', '%f') AS f2
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[f1:String, f2:String] rows=1
    - ('2020-06-15 13:45:09.1234567', '1234567')
- DuckDB: cols=[f1:String, f2:String] rows=1
    - ('2020-06-15 13:45:09.123456', '123456')

### `agent-datetime-timespan-0015` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('0909, 0303 9, 2007', '099 033') duck=('09, 03 9, 2007', '09 03')

**KQL**
```kql
print f1 = format_datetime(datetime(2007-03-09 05:04:03), 'dddd, MMMM d, yyyy'), f2 = format_datetime(datetime(2007-03-09 05:04:03), 'ddd MMM')
```
**Generated SQL**
```sql
SELECT STRFTIME(TIMESTAMP '2007-03-09 05:04:03', '%d, %m %-d, %Y') AS f1, STRFTIME(TIMESTAMP '2007-03-09 05:04:03', '%d %m') AS f2
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[f1:String, f2:String] rows=1
    - ('0909, 0303 9, 2007', '099 033')
- DuckDB: cols=[f1:String, f2:String] rows=1
    - ('09, 03 9, 2007', '09 03')

### `agent-datetime-timespan-0017` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('09.5', '09.05', '2020/06/15') duck=('09.500', '09.050', '2020/06/15')

**KQL**
```kql
print f1 = format_datetime(datetime(2020-06-15 13:45:09.5), 'ss.f'), f2 = format_datetime(datetime(2020-06-15 13:45:09.05), 'ss.ff'), f3 = format_datetime(datetime(2020-06-15 13:45:09), 'yyyy/MM/dd')
```
**Generated SQL**
```sql
SELECT STRFTIME(TIMESTAMP '2020-06-15 13:45:09.500000', '%S.%g') AS f1, STRFTIME(TIMESTAMP '2020-06-15 13:45:09.050000', '%S.%g') AS f2, STRFTIME(TIMESTAMP '2020-06-15 13:45:09', '%Y/%m/%d') AS f3
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[f1:String, f2:String, f3:String] rows=1
    - ('09.5', '09.05', '2020/06/15')
- DuckDB: cols=[f1:String, f2:String, f3:String] rows=1
    - ('09.500', '09.050', '2020/06/15')

### `agent-datetime-timespan-0018` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('1:02:03:04.005', '0000001') duck=('1:02:03:04.005', '0000000')

**KQL**
```kql
print ft = format_timespan(1d + 2h + 3m + 4s + 5ms, 'd:hh:mm:ss.fff'), ft2 = format_timespan(time(0.00:00:00.0000001), 'fffffff')
```
**Generated SQL**
```sql
SELECT (CASE WHEN (EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000) < 0 THEN '-' ELSE '' END || CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR) || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000)), 86400000000) / 3600000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000)), 3600000000) / 60000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000)), 60000000) / 1000000) AS BIGINT) AS VARCHAR), 2, '0') || '.' || LPAD(CAST(CAST(FLOOR(CAST(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000)), 1000000) AS BIGINT) / 1000) AS BIGINT) AS VARCHAR), 3, '0')) AS ft, (CASE WHEN (EXTRACT(EPOCH FROM ((CAST(0 AS BIGINT) * INTERVAL '1 microsecond'))) * 1000000) < 0 THEN '-' ELSE '' END || (LPAD(CAST(CAST(FLOOR(CAST(MOD(ABS((EXTRACT(EPOCH FROM ((CAST(0 AS BIGINT) * INTERVAL '1 microsecond'))) * 1000000)), 1000000) AS BIGINT) / 1) AS BIGINT) AS VARCHAR), 6, '0') || '0')) AS ft2
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ft:String, ft2:String] rows=1
    - ('1:02:03:04.005', '0000001')
- DuckDB: cols=[ft:String, ft2:String] rows=1
    - ('1:02:03:04.005', '0000000')

### `agent-datetime-timespan-0023` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(24, 2.12:00:00, 00:45:00, 54000000000) duck=(24, 2.12:00:00, 00:45:00, Infinity)

**KQL**
```kql
print arith = 1d / 1h, arith2 = 1d * 2.5, arith3 = (1h + 30m) / 2, arith4 = 90m / 1tick
```
**Generated SQL**
```sql
SELECT (EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond'))) / EXTRACT(EPOCH FROM ((3600000 * INTERVAL '1 millisecond')))) AS arith, (86400000 * INTERVAL '1 millisecond') * 2.5 AS arith2, ((3600000 * INTERVAL '1 millisecond') + (1800000 * INTERVAL '1 millisecond')) / 2 AS arith3, (EXTRACT(EPOCH FROM ((5400000 * INTERVAL '1 millisecond'))) / EXTRACT(EPOCH FROM (((1 / 10.0) * INTERVAL '1 microsecond')))) AS arith4
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[arith:Real, arith2:TimeSpan, arith3:TimeSpan, arith4:Real] rows=1
    - (24, 2.12:00:00, 00:45:00, 54000000000)
- DuckDB: cols=[arith:Real, arith2:TimeSpan, arith3:TimeSpan, arith4:Real] rows=1
    - (24, 2.12:00:00, 00:45:00, Infinity)

### `agent-datetime-timespan-0027` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-06-15T00:00:00.0000000Z, null, 02:30:00) duck=(2020-06-15T00:00:00.0000000Z, 1.02:10:00, 02:30:00)

**KQL**
```kql
print mk = make_datetime(2020, 6, 15), mk2 = make_timespan(25, 70), mk3 = make_timespan(2, 30)
```
**Generated SQL**
```sql
SELECT MAKE_TIMESTAMP(2020, 6, 15, 0, 0, 0) AS mk, (25 * INTERVAL '1 hour' + 70 * INTERVAL '1 minute') AS mk2, (2 * INTERVAL '1 hour' + 30 * INTERVAL '1 minute') AS mk3
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[mk:DateTime, mk2:TimeSpan, mk3:TimeSpan] rows=1
    - (2020-06-15T00:00:00.0000000Z, null, 02:30:00)
- DuckDB: cols=[mk:DateTime, mk2:TimeSpan, mk3:TimeSpan] rows=1
    - (2020-06-15T00:00:00.0000000Z, 1.02:10:00, 02:30:00)

### `agent-datetime-timespan-0028` — MismatchRows (high)

*Detail:* first differing row[2]: kusto=(9999-12-31T23:59:59.9999999Z, 9999, 52, null, 9999-12-31T23:59:59.9999999Z) duck=(9999-12-31T23:59:59.9999990Z, 9999, 52, 5, 9999-12-31T23:59:59.9999990Z)

**KQL**
```kql
datatable(t:datetime)[ datetime(1601-01-01 00:00:00), datetime(0001-01-01 00:00:00.0000001), datetime(9999-12-31 23:59:59.9999999) ] | extend y = getyear(t), woy = week_of_year(t), dow = dayofweek(t) / 1d, eom = endofmonth(t)
```
**Generated SQL**
```sql
SELECT *, EXTRACT(YEAR FROM t) AS y, EXTRACT(WEEK FROM t) AS woy, (EXTRACT(EPOCH FROM ((EXTRACT(DOW FROM t) * INTERVAL '1 day'))) / EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond')))) AS dow, DATE_TRUNC('month', t) + INTERVAL '1 month' - INTERVAL '1 microsecond' AS eom FROM (VALUES (TIMESTAMP '1601-01-01 00:00:00'), (TIMESTAMP '0001-01-01 00:00:00.000000'), (TIMESTAMP '9999-12-31 23:59:59.999999')) AS t(t)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, y:Int, woy:Int, dow:Real, eom:DateTime] rows=3
    - (1601-01-01T00:00:00.0000000Z, 1601, 1, 1, 1601-01-31T23:59:59.9999999Z)
    - (0001-01-01T00:00:00.0000001Z, 1, 1, 1, 0001-01-31T23:59:59.9999999Z)
    - (9999-12-31T23:59:59.9999999Z, 9999, 52, null, 9999-12-31T23:59:59.9999999Z)
- DuckDB: cols=[t:DateTime, y:Int, woy:Int, dow:Real, eom:DateTime] rows=3
    - (1601-01-01T00:00:00.0000000Z, 1601, 1, 1, 1601-01-31T23:59:59.9999990Z)
    - (0001-01-01T00:00:00.0000000Z, 1, 1, 1, 0001-01-31T23:59:59.9999990Z)
    - (9999-12-31T23:59:59.9999990Z, 9999, 52, 5, 9999-12-31T23:59:59.9999990Z)

### `agent-datetime-timespan-0041` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-06-15T13:45:09.1234567Z, 1.02:03:04.5000000) duck=(2020-06-15T13:45:09.1234560Z, null)

**KQL**
```kql
print roundtrip = todatetime(tostring(datetime(2020-06-15 13:45:09.1234567))), spround = totimespan(tostring(1d + 2h + 3m + 4.5s))
```
**Generated SQL**
```sql
SELECT COALESCE(TRY_CAST(COALESCE(STRFTIME(TIMESTAMP '2020-06-15 13:45:09.123456', '%Y-%m-%dT%H:%M:%S.%f') || '0Z', '') AS TIMESTAMP), TRY_STRPTIME(CAST(COALESCE(STRFTIME(TIMESTAMP '2020-06-15 13:45:09.123456', '%Y-%m-%dT%H:%M:%S.%f') || '0Z', '') AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS roundtrip, TRY_CAST(COALESCE(((CASE WHEN (EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + INTERVAL '4.5s')) * 1000000) < 0 THEN '-' ELSE '' END) || (CASE WHEN CAST(FLOOR(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + INTERVAL '4.5s')) * 1000000)) / 86400000000) AS BIGINT) > 0 THEN CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + INTERVAL '4.5s')) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR) || '.' ELSE '' END) || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + INTERVAL '4.5s')) * 1000000)), 86400000000) / 3600000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + INTERVAL '4.5s')) * 1000000)), 3600000000) / 60000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + INTERVAL '4.5s')) * 1000000)), 60000000) / 1000000) AS BIGINT) AS VARCHAR), 2, '0') || (CASE WHEN CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + INTERVAL '4.5s')) * 1000000)), 1000000)) AS BIGINT) > 0 THEN '.' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + INTERVAL '4.5s')) * 1000000)), 1000000)) AS BIGINT) * 10 AS VARCHAR), 7, '0') ELSE '' END)), '') AS INTERVAL) AS spround
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[roundtrip:DateTime, spround:TimeSpan] rows=1
    - (2020-06-15T13:45:09.1234567Z, 1.02:03:04.5000000)
- DuckDB: cols=[roundtrip:DateTime, spround:TimeSpan] rows=1
    - (2020-06-15T13:45:09.1234560Z, null)

### `agent-datetime-timespan-0043` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-05-01T00:00:00.0000000Z, 2020-08-01T00:00:00.0000000Z, 2021-01-01T00:00:00.0000000Z) duck=(2020-06-01T00:00:00.0000000Z, 2020-06-01T00:00:00.0000000Z, 2020-01-01T00:00:00.0000000Z)

**KQL**
```kql
print startm = startofmonth(datetime(2020-06-15), -1), startm2 = startofmonth(datetime(2020-06-15), 2), starty = startofyear(datetime(2020-06-15), 1)
```
**Generated SQL**
```sql
SELECT DATE_TRUNC('month', TIMESTAMP '2020-06-15 00:00:00') AS startm, DATE_TRUNC('month', TIMESTAMP '2020-06-15 00:00:00') AS startm2, DATE_TRUNC('year', TIMESTAMP '2020-06-15 00:00:00') AS starty
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[startm:DateTime, startm2:DateTime, starty:DateTime] rows=1
    - (2020-05-01T00:00:00.0000000Z, 2020-08-01T00:00:00.0000000Z, 2021-01-01T00:00:00.0000000Z)
- DuckDB: cols=[startm:DateTime, startm2:DateTime, starty:DateTime] rows=1
    - (2020-06-01T00:00:00.0000000Z, 2020-06-01T00:00:00.0000000Z, 2020-01-01T00:00:00.0000000Z)

### `agent-datetime-timespan-0044` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-05-31T00:00:00.0000000Z, 2020-06-18T00:00:00.0000000Z, 2020-06-15T23:59:59.9999999Z) duck=(2020-06-14T00:00:00.0000000Z, 2020-06-15T00:00:00.0000000Z, 2020-06-15T23:59:59.9999990Z)

**KQL**
```kql
print startw = startofweek(datetime(2020-06-15), -2), startd = startofday(datetime(2020-06-15 13:45:00), 3), endh = endofday(datetime(2020-06-15 13:45:00))
```
**Generated SQL**
```sql
SELECT (DATE_TRUNC('week', TIMESTAMP '2020-06-15 00:00:00' + INTERVAL '1 day') - INTERVAL '1 day') AS startw, DATE_TRUNC('day', TIMESTAMP '2020-06-15 13:45:00') AS startd, DATE_TRUNC('day', TIMESTAMP '2020-06-15 13:45:00') + INTERVAL '1 day' - INTERVAL '1 microsecond' AS endh
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[startw:DateTime, startd:DateTime, endh:DateTime] rows=1
    - (2020-05-31T00:00:00.0000000Z, 2020-06-18T00:00:00.0000000Z, 2020-06-15T23:59:59.9999999Z)
- DuckDB: cols=[startw:DateTime, startd:DateTime, endh:DateTime] rows=1
    - (2020-06-14T00:00:00.0000000Z, 2020-06-15T00:00:00.0000000Z, 2020-06-15T23:59:59.9999990Z)

### `agent-datetime-timespan-0001` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('7.00:00:00.0000000', 6047999999999) duck=('7.00:00:00.0000000', Infinity)

**KQL**
```kql
print x = format_timespan(datetime(2024-03-03 00:00:00) - startofweek(datetime(2024-02-29 23:59:59.9999999)), 'd.hh:mm:ss.fffffff'), y = (endofweek(datetime(2024-02-29)) - startofweek(datetime(2024-02-29))) / 1tick
```
**Generated SQL**
```sql
SELECT (CASE WHEN (EXTRACT(EPOCH FROM (TIMESTAMP '2024-03-03 00:00:00' - (DATE_TRUNC('week', TIMESTAMP '2024-02-29 23:59:59.999999' + INTERVAL '1 day') - INTERVAL '1 day'))) * 1000000) < 0 THEN '-' ELSE '' END || CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM (TIMESTAMP '2024-03-03 00:00:00' - (DATE_TRUNC('week', TIMESTAMP '2024-02-29 23:59:59.999999' + INTERVAL '1 day') - INTERVAL '1 day'))) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR) || '.' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (TIMESTAMP '2024-03-03 00:00:00' - (DATE_TRUNC('week', TIMESTAMP '2024-02-29 23:59:59.999999' + INTERVAL '1 day') - INTERVAL '1 day'))) * 1000000)), 86400000000) / 3600000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (TIMESTAMP '2024-03-03 00:00:00' - (DATE_TRUNC('week', TIMESTAMP '2024-02-29 23:59:59.999999' + INTERVAL '1 day') - INTERVAL '1 day'))) * 1000000)), 3600000000) / 60000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (TIMESTAMP '2024-03-03 00:00:00' - (DATE_TRUNC('week', TIMESTAMP '2024-02-29 23:59:59.999999' + INTERVAL '1 day') - INTERVAL '1 day'))) * 1000000)), 60000000) / 1000000) AS BIGINT) AS VARCHAR), 2, '0') || '.' || (LPAD(CAST(CAST(FLOOR(CAST(MOD(ABS((EXTRACT(EPOCH FROM (TIMESTAMP '2024-03-03 00:00:00' - (DATE_TRUNC('week', TIMESTAMP '2024-02-29 23:59:59.999999' + INTERVAL '1 day') - INTERVAL '1 day'))) * 1000000)), 1000000) AS BIGINT) / 1) AS BIGINT) AS VARCHAR), 6, '0') || '0')) AS x, (EXTRACT(EPOCH FROM (((DATE_TRUNC('week', TIMESTAMP '2024-02-29 00:00:00' + INTERVAL '1 day') - INTERVAL '1 day') + INTERVAL '7 days' - INTERVAL '1 microsecond' - (DATE_TRUNC('week', TIMESTAMP '2024-02-29 00:00:00' + INTERVAL '1 day') - INTERVAL '1 day')))) / EXTRACT(EPOCH FROM (((1 / 10.0) * INTERVAL '1 microsecond')))) AS y
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:String, y:Real] rows=1
    - ('7.00:00:00.0000000', 6047999999999)
- DuckDB: cols=[x:String, y:Real] rows=1
    - ('7.00:00:00.0000000', Infinity)

### `agent-datetime-timespan-0004` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2, 200) duck=(2, 0)

**KQL**
```kql
print d1 = datetime_diff('microsecond', datetime(2020-01-01 00:00:00.0000020), datetime(2020-01-01 00:00:00.0000005)), d2 = datetime_diff('nanosecond', datetime(2020-01-01 00:00:00.0000002), datetime(2020-01-01 00:00:00))
```
**Generated SQL**
```sql
SELECT DATE_DIFF('microsecond', TIMESTAMP '2020-01-01 00:00:00.000000', TIMESTAMP '2020-01-01 00:00:00.000002') AS d1, (DATE_DIFF('microsecond', TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:00.000000') * 1000) AS d2
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d1:Int, d2:Int] rows=1
    - (2, 200)
- DuckDB: cols=[d1:Int, d2:Int] rows=1
    - (2, 0)

### `agent-datetime-timespan-0005` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('123456', '0000007', '') duck=('123456', '000000', '000000')

**KQL**
```kql
print f1 = format_datetime(datetime(2020-06-15 13:45:09.1234567), 'ffffff'), f2 = format_datetime(datetime(2020-06-15 13:45:09.0000007), 'FFFFFFF'), f3 = format_datetime(datetime(2020-06-15 13:45:09.0000000), 'FFFFFFF')
```
**Generated SQL**
```sql
SELECT STRFTIME(TIMESTAMP '2020-06-15 13:45:09.123456', '%f') AS f1, STRFTIME(TIMESTAMP '2020-06-15 13:45:09.000000', '%f') AS f2, STRFTIME(TIMESTAMP '2020-06-15 13:45:09', '%f') AS f3
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[f1:String, f2:String, f3:String] rows=1
    - ('123456', '0000007', '')
- DuckDB: cols=[f1:String, f2:String, f3:String] rows=1
    - ('123456', '000000', '000000')

### `agent-datetime-timespan-0007` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('3.5', '5 AM', '5 PM', '12:30 AM') duck=('3.500', '5 AM', '5 PM', '12:30 AM')

**KQL**
```kql
print f1 = format_datetime(datetime(2007-03-09 05:04:03.5), 's.f'), f2 = format_datetime(datetime(2007-03-09 05:04:03), 'h tt'), f3 = format_datetime(datetime(2007-03-09 17:04:03), 'h tt'), f4 = format_datetime(datetime(2007-03-09 00:30:00), 'hh:mm tt')
```
**Generated SQL**
```sql
SELECT STRFTIME(TIMESTAMP '2007-03-09 05:04:03.500000', '%-S.%g') AS f1, STRFTIME(TIMESTAMP '2007-03-09 05:04:03', '%-I %p') AS f2, STRFTIME(TIMESTAMP '2007-03-09 17:04:03', '%-I %p') AS f3, STRFTIME(TIMESTAMP '2007-03-09 00:30:00', '%I:%M %p') AS f4
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[f1:String, f2:String, f3:String, f4:String] rows=1
    - ('3.5', '5 AM', '5 PM', '12:30 AM')
- DuckDB: cols=[f1:String, f2:String, f3:String, f4:String] rows=1
    - ('3.500', '5 AM', '5 PM', '12:30 AM')

### `agent-datetime-timespan-0012` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(null, null, 01:02:03, 00:00:00.0000001) duck=(1.02:02:01.5000000, -01:02:00, 01:02:03, 00:00:00)

**KQL**
```kql
print a = make_timespan(0, 25, 61, 61.5), b = make_timespan(-1, -2), c = make_timespan(1, 2, 3), d = make_timespan(0, 0, 0, 0.0000001)
```
**Generated SQL**
```sql
SELECT (0 * INTERVAL '1 day' + 25 * INTERVAL '1 hour' + 61 * INTERVAL '1 minute' + 61.5 * INTERVAL '1 second') AS a, ((-1) * INTERVAL '1 hour' + (-2) * INTERVAL '1 minute') AS b, (1 * INTERVAL '1 hour' + 2 * INTERVAL '1 minute' + 3 * INTERVAL '1 second') AS c, (0 * INTERVAL '1 day' + 0 * INTERVAL '1 hour' + 0 * INTERVAL '1 minute' + 1E-07 * INTERVAL '1 second') AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:TimeSpan, b:TimeSpan, c:TimeSpan, d:TimeSpan] rows=1
    - (null, null, 01:02:03, 00:00:00.0000001)
- DuckDB: cols=[a:TimeSpan, b:TimeSpan, c:TimeSpan, d:TimeSpan] rows=1
    - (1.02:02:01.5000000, -01:02:00, 01:02:03, 00:00:00)

### `agent-datetime-timespan-0018` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-05-31T00:00:00.0000000Z, 2020-07-05T00:00:00.0000000Z, 2019-05-01T00:00:00.0000000Z, 2015-01-01T00:00:00.0000000Z) duck=(2020-06-14T00:00:00.0000000Z, 2020-06-14T00:00:00.0000000Z, 2020-06-01T00:00:00.0000000Z, 2020-01-01T00:00:00.0000000Z)

**KQL**
```kql
print a = startofweek(datetime(2020-06-15 13:45:00), -2), b = startofweek(datetime(2020-06-15 13:45:00), 3), c = startofmonth(datetime(2020-06-15), -13), d = startofyear(datetime(2020-06-15), -5)
```
**Generated SQL**
```sql
SELECT (DATE_TRUNC('week', TIMESTAMP '2020-06-15 13:45:00' + INTERVAL '1 day') - INTERVAL '1 day') AS a, (DATE_TRUNC('week', TIMESTAMP '2020-06-15 13:45:00' + INTERVAL '1 day') - INTERVAL '1 day') AS b, DATE_TRUNC('month', TIMESTAMP '2020-06-15 00:00:00') AS c, DATE_TRUNC('year', TIMESTAMP '2020-06-15 00:00:00') AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:DateTime, b:DateTime, c:DateTime, d:DateTime] rows=1
    - (2020-05-31T00:00:00.0000000Z, 2020-07-05T00:00:00.0000000Z, 2019-05-01T00:00:00.0000000Z, 2015-01-01T00:00:00.0000000Z)
- DuckDB: cols=[a:DateTime, b:DateTime, c:DateTime, d:DateTime] rows=1
    - (2020-06-14T00:00:00.0000000Z, 2020-06-14T00:00:00.0000000Z, 2020-06-01T00:00:00.0000000Z, 2020-01-01T00:00:00.0000000Z)

### `agent-datetime-timespan-0019` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-07-04T23:59:59.9999999Z, 2020-02-29T23:59:59.9999999Z, 2021-02-28T23:59:59.9999999Z, 2019-12-31T23:59:59.9999999Z) duck=(2020-06-20T23:59:59.9999990Z, 2020-01-31T23:59:59.9999990Z, 2020-01-31T23:59:59.9999990Z, 2020-12-31T23:59:59.9999990Z)

**KQL**
```kql
print a = endofweek(datetime(2020-06-15 13:45:00), 2), b = endofmonth(datetime(2020-01-31), 1), c = endofmonth(datetime(2020-01-31), 13), d = endofyear(datetime(2020-06-15), -1)
```
**Generated SQL**
```sql
SELECT (DATE_TRUNC('week', TIMESTAMP '2020-06-15 13:45:00' + INTERVAL '1 day') - INTERVAL '1 day') + INTERVAL '7 days' - INTERVAL '1 microsecond' AS a, DATE_TRUNC('month', TIMESTAMP '2020-01-31 00:00:00') + INTERVAL '1 month' - INTERVAL '1 microsecond' AS b, DATE_TRUNC('month', TIMESTAMP '2020-01-31 00:00:00') + INTERVAL '1 month' - INTERVAL '1 microsecond' AS c, DATE_TRUNC('year', TIMESTAMP '2020-06-15 00:00:00') + INTERVAL '1 year' - INTERVAL '1 microsecond' AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:DateTime, b:DateTime, c:DateTime, d:DateTime] rows=1
    - (2020-07-04T23:59:59.9999999Z, 2020-02-29T23:59:59.9999999Z, 2021-02-28T23:59:59.9999999Z, 2019-12-31T23:59:59.9999999Z)
- DuckDB: cols=[a:DateTime, b:DateTime, c:DateTime, d:DateTime] rows=1
    - (2020-06-20T23:59:59.9999990Z, 2020-01-31T23:59:59.9999990Z, 2020-01-31T23:59:59.9999990Z, 2020-12-31T23:59:59.9999990Z)

### `agent-datetime-timespan-0020` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-06-12T00:00:00.0000000Z, 2020-06-17T23:59:59.9999999Z, 0001-01-01T00:00:00.0000000Z) duck=(2020-06-15T00:00:00.0000000Z, 2020-06-15T23:59:59.9999990Z, 0001-01-02T00:00:00.0000000Z)

**KQL**
```kql
print a = startofday(datetime(2020-06-15 13:45:00.5), -3), b = endofday(datetime(2020-06-15 13:45:00), 2), c = startofday(datetime(0001-01-02 00:00:00), -1)
```
**Generated SQL**
```sql
SELECT DATE_TRUNC('day', TIMESTAMP '2020-06-15 13:45:00.500000') AS a, DATE_TRUNC('day', TIMESTAMP '2020-06-15 13:45:00') + INTERVAL '1 day' - INTERVAL '1 microsecond' AS b, DATE_TRUNC('day', TIMESTAMP '0001-01-02 00:00:00') AS c
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:DateTime, b:DateTime, c:DateTime] rows=1
    - (2020-06-12T00:00:00.0000000Z, 2020-06-17T23:59:59.9999999Z, 0001-01-01T00:00:00.0000000Z)
- DuckDB: cols=[a:DateTime, b:DateTime, c:DateTime] rows=1
    - (2020-06-15T00:00:00.0000000Z, 2020-06-15T23:59:59.9999990Z, 0001-01-02T00:00:00.0000000Z)

### `agent-datetime-timespan-0023` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(6543210, 3652059) duck=(Infinity, 3652059)

**KQL**
```kql
print a = (datetime(2020-06-15 13:45:30.7654321) - datetime(2020-06-15 13:45:30.1111111)) / 1tick, b = (datetime(9999-12-31 23:59:59.9999999) - datetime(0001-01-01 00:00:00)) / 1d
```
**Generated SQL**
```sql
SELECT (EXTRACT(EPOCH FROM ((TIMESTAMP '2020-06-15 13:45:30.765432' - TIMESTAMP '2020-06-15 13:45:30.111111'))) / EXTRACT(EPOCH FROM (((1 / 10.0) * INTERVAL '1 microsecond')))) AS a, (EXTRACT(EPOCH FROM ((TIMESTAMP '9999-12-31 23:59:59.999999' - TIMESTAMP '0001-01-01 00:00:00'))) / EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond')))) AS b
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Real, b:Real] rows=1
    - (6543210, 3652059)
- DuckDB: cols=[a:Real, b:Real] rows=1
    - (Infinity, 3652059)

### `agent-datetime-timespan-0024` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-02-29T12:00:00.0000001Z, null, 9999-12-31T23:59:59.9999999Z, 2020-06-14T00:00:00.0000000Z) duck=(2020-02-29T12:00:00.0000000Z, 0001-01-01T00:00:00.0000000Z, 9999-12-31T23:59:59.9999990Z, 2020-06-14T00:00:00.0000000Z)

**KQL**
```kql
print a = datetime(2020-02-29 12:00:00) + 1tick, b = datetime(0001-01-01 00:00:00) - 1tick, c = datetime(9999-12-31 23:59:59.9999999) + 0tick, d = datetime(2020-06-15) + (-1d)
```
**Generated SQL**
```sql
SELECT TIMESTAMP '2020-02-29 12:00:00' + ((1 / 10.0) * INTERVAL '1 microsecond') AS a, TIMESTAMP '0001-01-01 00:00:00' - ((1 / 10.0) * INTERVAL '1 microsecond') AS b, TIMESTAMP '9999-12-31 23:59:59.999999' + ((0 / 10.0) * INTERVAL '1 microsecond') AS c, TIMESTAMP '2020-06-15 00:00:00' + ((-(86400000 * INTERVAL '1 millisecond'))) AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:DateTime, b:DateTime, c:DateTime, d:DateTime] rows=1
    - (2020-02-29T12:00:00.0000001Z, null, 9999-12-31T23:59:59.9999999Z, 2020-06-14T00:00:00.0000000Z)
- DuckDB: cols=[a:DateTime, b:DateTime, c:DateTime, d:DateTime] rows=1
    - (2020-02-29T12:00:00.0000000Z, 0001-01-01T00:00:00.0000000Z, 9999-12-31T23:59:59.9999990Z, 2020-06-14T00:00:00.0000000Z)

### `agent-datetime-timespan-0028` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[dw:Real|Int]  
*Detail:* first differing row[0]: kusto=(2024-02-29T00:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 48, 16, 4, 209) duck=(2024-02-29T00:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 48, 16, 4, 208)

**KQL**
```kql
datatable(a:datetime, b:datetime)[ datetime(2024-02-29),datetime(2020-02-29), datetime(2020-01-31),datetime(2019-12-31), datetime(2021-03-01),datetime(2020-12-31) ] | extend dm = datetime_diff('month', a, b), dq = datetime_diff('quarter', a, b), dy = datetime_diff('year', a, b), dw = datetime_diff('week', a, b)
```
**Generated SQL**
```sql
SELECT *, DATE_DIFF('month', b, a) AS dm, DATE_DIFF('quarter', b, a) AS dq, DATE_DIFF('year', b, a) AS dy, DATE_DIFF('week', b, a) AS dw FROM (VALUES (TIMESTAMP '2024-02-29 00:00:00', TIMESTAMP '2020-02-29 00:00:00'), (TIMESTAMP '2020-01-31 00:00:00', TIMESTAMP '2019-12-31 00:00:00'), (TIMESTAMP '2021-03-01 00:00:00', TIMESTAMP '2020-12-31 00:00:00')) AS t(a, b)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:DateTime, b:DateTime, dm:Int, dq:Int, dy:Int, dw:Real] rows=3
    - (2024-02-29T00:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 48, 16, 4, 209)
    - (2020-01-31T00:00:00.0000000Z, 2019-12-31T00:00:00.0000000Z, 1, 1, 1, 4)
    - (2021-03-01T00:00:00.0000000Z, 2020-12-31T00:00:00.0000000Z, 3, 1, 1, 9)
- DuckDB: cols=[a:DateTime, b:DateTime, dm:Int, dq:Int, dy:Int, dw:Int] rows=3
    - (2024-02-29T00:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 48, 16, 4, 208)
    - (2020-01-31T00:00:00.0000000Z, 2019-12-31T00:00:00.0000000Z, 1, 1, 1, 4)
    - (2021-03-01T00:00:00.0000000Z, 2020-12-31T00:00:00.0000000Z, 3, 1, 1, 8)

### `agent-datetime-timespan-0029` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-01-01T00:00:01.9999999Z, 2020-01-01T00:00:00.0000001Z, 1, 1999, 00:00:01.9999998, 19999998) duck=(2020-01-01T00:00:01.9999990Z, 2020-01-01T00:00:00.0000000Z, 1, 1999, 00:00:01.9999990, Infinity)

**KQL**
```kql
datatable(a:datetime, b:datetime)[ datetime(2020-01-01 00:00:01.9999999),datetime(2020-01-01 00:00:00.0000001), datetime(2020-03-01 00:00:00),datetime(2020-02-29 23:59:59.9999999) ] | extend ds = datetime_diff('second', a, b), dms = datetime_diff('millisecond', a, b), span = a - b, spanticks = (a - b) / 1tick
```
**Generated SQL**
```sql
SELECT *, DATE_DIFF('second', b, a) AS ds, DATE_DIFF('millisecond', b, a) AS dms, a - b AS span, (EXTRACT(EPOCH FROM ((a - b))) / EXTRACT(EPOCH FROM (((1 / 10.0) * INTERVAL '1 microsecond')))) AS spanticks FROM (VALUES (TIMESTAMP '2020-01-01 00:00:01.999999', TIMESTAMP '2020-01-01 00:00:00.000000'), (TIMESTAMP '2020-03-01 00:00:00', TIMESTAMP '2020-02-29 23:59:59.999999')) AS t(a, b)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:DateTime, b:DateTime, ds:Int, dms:Int, span:TimeSpan, spanticks:Real] rows=2
    - (2020-01-01T00:00:01.9999999Z, 2020-01-01T00:00:00.0000001Z, 1, 1999, 00:00:01.9999998, 19999998)
    - (2020-03-01T00:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 1, 1, 00:00:00.0000001, 1)
- DuckDB: cols=[a:DateTime, b:DateTime, ds:Int, dms:Int, span:TimeSpan, spanticks:Real] rows=2
    - (2020-01-01T00:00:01.9999990Z, 2020-01-01T00:00:00.0000000Z, 1, 1999, 00:00:01.9999990, Infinity)
    - (2020-03-01T00:00:00.0000000Z, 2020-02-29T23:59:59.9999990Z, 1, 1, 00:00:00.0000010, Infinity)

### `agent-datetime-timespan-0031` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('2020-06-15T13:45:30.0000000Z', '2020-06-15T13:45:30.7654321Z', '2020-06-15T00:00:00.0000000Z', '2020-06-15T13:45:00.0000000Z') duck=('2020-06-15 13:45:30', '2020-06-15T13:45:30.7654320Z', '2020-06-15T00:00:00.0000000Z', '2020-06-15T13:45:00.0000000Z')

**KQL**
```kql
print a = tostring(bin(datetime(2020-06-15 13:45:30.7654321), 1s)), b = tostring(datetime(2020-06-15 13:45:30.7654321)), c = tostring(datetime(2020-06-15 00:00:00)), d = tostring(datetime(2020-06-15 13:45:00))
```
**Generated SQL**
```sql
SELECT COALESCE(TRY_CAST(EPOCH_MS(CAST(FLOOR(((EPOCH_MS(CAST(TIMESTAMP '2020-06-15 13:45:30.765432' AS TIMESTAMP))) - (EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00') * 1000))/1000)*1000 + (EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00') * 1000) AS BIGINT)) AS TEXT), '') AS a, COALESCE(STRFTIME(TIMESTAMP '2020-06-15 13:45:30.765432', '%Y-%m-%dT%H:%M:%S.%f') || '0Z', '') AS b, COALESCE(STRFTIME(TIMESTAMP '2020-06-15 00:00:00', '%Y-%m-%dT%H:%M:%S.%f') || '0Z', '') AS c, COALESCE(STRFTIME(TIMESTAMP '2020-06-15 13:45:00', '%Y-%m-%dT%H:%M:%S.%f') || '0Z', '') AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String, b:String, c:String, d:String] rows=1
    - ('2020-06-15T13:45:30.0000000Z', '2020-06-15T13:45:30.7654321Z', '2020-06-15T00:00:00.0000000Z', '2020-06-15T13:45:00.0000000Z')
- DuckDB: cols=[a:String, b:String, c:String, d:String] rows=1
    - ('2020-06-15 13:45:30', '2020-06-15T13:45:30.7654320Z', '2020-06-15T00:00:00.0000000Z', '2020-06-15T13:45:00.0000000Z')

### `agent-datetime-timespan-0033` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-03-09T12:00:00.0000000Z, null, 2031-05-29T16:00:00.0000000Z, 2019-12-30T22:58:59.0000000Z) duck=(2020-03-09T12:00:00.0000000Z, '<unreadable:InvalidCastException>', 2031-05-29T16:00:00.0000000Z, 2019-12-30T22:58:59.0000000Z)

**KQL**
```kql
print a = datetime_add('week', -53, datetime(2021-03-15 12:00:00)), b = datetime_add('day', -1, datetime(0001-01-01 12:00:00)), c = datetime_add('hour', 100000, datetime(2020-01-01)), d = datetime_add('second', -90061, datetime(2020-01-01 00:00:00))
```
**Generated SQL**
```sql
SELECT TIMESTAMP '2021-03-15 12:00:00' + (-53) * INTERVAL '1 week' AS a, TIMESTAMP '0001-01-01 12:00:00' + (-1) * INTERVAL '1 day' AS b, TIMESTAMP '2020-01-01 00:00:00' + 100000 * INTERVAL '1 hour' AS c, TIMESTAMP '2020-01-01 00:00:00' + (-90061) * INTERVAL '1 second' AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:DateTime, b:DateTime, c:DateTime, d:DateTime] rows=1
    - (2020-03-09T12:00:00.0000000Z, null, 2031-05-29T16:00:00.0000000Z, 2019-12-30T22:58:59.0000000Z)
- DuckDB: cols=[a:DateTime, b:DateTime, c:DateTime, d:DateTime] rows=1
    - (2020-03-09T12:00:00.0000000Z, '<unreadable:InvalidCastException>', 2031-05-29T16:00:00.0000000Z, 2019-12-30T22:58:59.0000000Z)

### `agent-datetime-timespan-0035` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(24, 864000000000, 5400, 00:45:00, 2.12:00:00, 24, 7) duck=(24, Infinity, 5400, 00:45:00, 2.12:00:00, 24, 7)

**KQL**
```kql
print a = 1d / 1h, b = 1d / 1tick, c = 90m / 1s, d = (1h + 30m) / 2, e = 1d * 2.5, f = -1d / -1h, g = 7d / 1d
```
**Generated SQL**
```sql
SELECT (EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond'))) / EXTRACT(EPOCH FROM ((3600000 * INTERVAL '1 millisecond')))) AS a, (EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond'))) / EXTRACT(EPOCH FROM (((1 / 10.0) * INTERVAL '1 microsecond')))) AS b, (EXTRACT(EPOCH FROM ((5400000 * INTERVAL '1 millisecond'))) / EXTRACT(EPOCH FROM ((1000 * INTERVAL '1 millisecond')))) AS c, ((3600000 * INTERVAL '1 millisecond') + (1800000 * INTERVAL '1 millisecond')) / 2 AS d, (86400000 * INTERVAL '1 millisecond') * 2.5 AS e, (EXTRACT(EPOCH FROM ((-(86400000 * INTERVAL '1 millisecond')))) / EXTRACT(EPOCH FROM ((-(3600000 * INTERVAL '1 millisecond'))))) AS f, (EXTRACT(EPOCH FROM ((604800000 * INTERVAL '1 millisecond'))) / EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond')))) AS g
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Real, b:Real, c:Real, d:TimeSpan, e:TimeSpan, f:Real, g:Real] rows=1
    - (24, 864000000000, 5400, 00:45:00, 2.12:00:00, 24, 7)
- DuckDB: cols=[a:Real, b:Real, c:Real, d:TimeSpan, e:TimeSpan, f:Real, g:Real] rows=1
    - (24, Infinity, 5400, 00:45:00, 2.12:00:00, 24, 7)

### `agent-datetime-timespan-0036` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2.03:00:00, 19:00:00, 2.06:00:00, 00:00:01, 00:00:00.0010000) duck=(2.03:00:00, 19:00:00, 2.06:00:00, 00:00:00, 00:00:00.0010000)

**KQL**
```kql
print a = 2 * 1d + 3 * 1h, b = 1d - 1h * 5, c = (1d + 12h) * 1.5, d = 1tick * 10000000, e = 1s / 1000
```
**Generated SQL**
```sql
SELECT 2 * (86400000 * INTERVAL '1 millisecond') + 3 * (3600000 * INTERVAL '1 millisecond') AS a, (86400000 * INTERVAL '1 millisecond') - (3600000 * INTERVAL '1 millisecond') * 5 AS b, ((86400000 * INTERVAL '1 millisecond') + (43200000 * INTERVAL '1 millisecond')) * 1.5 AS c, ((1 / 10.0) * INTERVAL '1 microsecond') * 10000000 AS d, (1000 * INTERVAL '1 millisecond') / 1000 AS e
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:TimeSpan, b:TimeSpan, c:TimeSpan, d:TimeSpan, e:TimeSpan] rows=1
    - (2.03:00:00, 19:00:00, 2.06:00:00, 00:00:01, 00:00:00.0010000)
- DuckDB: cols=[a:TimeSpan, b:TimeSpan, c:TimeSpan, d:TimeSpan, e:TimeSpan] rows=1
    - (2.03:00:00, 19:00:00, 2.06:00:00, 00:00:00, 00:00:00.0010000)

### `agent-datetime-timespan-0037` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(2020-10-25T02:30:00.0000000Z, -01:00:00, 2020-10-25T01:30:00.0000000Z, 2020-10-25T03:30:00.0000000Z, null, 0) duck=(2020-10-25T02:30:00.0000000Z, -01:00:00, 2020-10-25T01:30:00.0000000Z, 2020-10-25T03:30:00.0000000Z, 2020-10-25T03:00:00.0000000Z, 0)

**KQL**
```kql
datatable(t:datetime, s:timespan)[ datetime(2020-03-29 01:30:00),1h, datetime(2020-10-25 02:30:00),-1h, datetime(2024-02-29 23:00:00),2h ] | extend plus = t + s, minus = t - s, bin = bin(t, s), dow = dayofweek(t) / 1d
```
**Generated SQL**
```sql
SELECT *, t + s AS plus, t - s AS minus, EPOCH_MS(CAST(FLOOR(((EPOCH_MS(CAST(t AS TIMESTAMP))) - (EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00') * 1000))/EPOCH_MS(CAST(TIMESTAMP 'epoch' + (s) AS TIMESTAMP)))*EPOCH_MS(CAST(TIMESTAMP 'epoch' + (s) AS TIMESTAMP)) + (EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00') * 1000) AS BIGINT)) AS bin, (EXTRACT(EPOCH FROM ((EXTRACT(DOW FROM t) * INTERVAL '1 day'))) / EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond')))) AS dow FROM (VALUES (TIMESTAMP '2020-03-29 01:30:00', (3600000 * INTERVAL '1 millisecond')), (TIMESTAMP '2020-10-25 02:30:00', (-3600000 * INTERVAL '1 millisecond')), (TIMESTAMP '2024-02-29 23:00:00', (7200000 * INTERVAL '1 millisecond'))) AS t(t, s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, s:TimeSpan, plus:DateTime, minus:DateTime, bin:DateTime, dow:Real] rows=3
    - (2020-03-29T01:30:00.0000000Z, 01:00:00, 2020-03-29T02:30:00.0000000Z, 2020-03-29T00:30:00.0000000Z, 2020-03-29T01:00:00.0000000Z, 0)
    - (2020-10-25T02:30:00.0000000Z, -01:00:00, 2020-10-25T01:30:00.0000000Z, 2020-10-25T03:30:00.0000000Z, null, 0)
    - (2024-02-29T23:00:00.0000000Z, 02:00:00, 2024-03-01T01:00:00.0000000Z, 2024-02-29T21:00:00.0000000Z, 2024-02-29T22:00:00.0000000Z, 4)
- DuckDB: cols=[t:DateTime, s:TimeSpan, plus:DateTime, minus:DateTime, bin:DateTime, dow:Real] rows=3
    - (2020-03-29T01:30:00.0000000Z, 01:00:00, 2020-03-29T02:30:00.0000000Z, 2020-03-29T00:30:00.0000000Z, 2020-03-29T01:00:00.0000000Z, 0)
    - (2020-10-25T02:30:00.0000000Z, -01:00:00, 2020-10-25T01:30:00.0000000Z, 2020-10-25T03:30:00.0000000Z, 2020-10-25T03:00:00.0000000Z, 0)
    - (2024-02-29T23:00:00.0000000Z, 02:00:00, 2024-03-01T01:00:00.0000000Z, 2024-02-29T21:00:00.0000000Z, 2024-02-29T22:00:00.0000000Z, 4)

### `agent-datetime-timespan-0039` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, null, 366, 365, 23) duck=(1, 5, 366, 365, 23)

**KQL**
```kql
print a = dayofweek(datetime(0001-01-01)) / 1d, b = dayofweek(datetime(9999-12-31)) / 1d, c = dayofyear(datetime(2020-12-31)), d = dayofyear(datetime(2021-12-31)), e = hourofday(datetime(2020-06-15 23:59:59.9999999))
```
**Generated SQL**
```sql
SELECT (EXTRACT(EPOCH FROM ((EXTRACT(DOW FROM TIMESTAMP '0001-01-01 00:00:00') * INTERVAL '1 day'))) / EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond')))) AS a, (EXTRACT(EPOCH FROM ((EXTRACT(DOW FROM TIMESTAMP '9999-12-31 00:00:00') * INTERVAL '1 day'))) / EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond')))) AS b, EXTRACT(DOY FROM TIMESTAMP '2020-12-31 00:00:00') AS c, EXTRACT(DOY FROM TIMESTAMP '2021-12-31 00:00:00') AS d, EXTRACT(HOUR FROM TIMESTAMP '2020-06-15 23:59:59.999999') AS e
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Real, b:Real, c:Int, d:Int, e:Int] rows=1
    - (1, null, 366, 365, 23)
- DuckDB: cols=[a:Real, b:Real, c:Int, d:Int, e:Int] rows=1
    - (1, 5, 366, 365, 23)

### `agent-datetime-timespan-0041` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('0505', '055', '0101', '011') duck=('05', '05', '01', '01')

**KQL**
```kql
print a = format_datetime(datetime(2020-01-05 00:00:00), 'dddd'), b = format_datetime(datetime(2020-01-05 00:00:00), 'ddd'), c = format_datetime(datetime(2020-01-05 00:00:00), 'MMMM'), d = format_datetime(datetime(2020-01-05 00:00:00), 'MMM')
```
**Generated SQL**
```sql
SELECT STRFTIME(TIMESTAMP '2020-01-05 00:00:00', '%d') AS a, STRFTIME(TIMESTAMP '2020-01-05 00:00:00', '%d') AS b, STRFTIME(TIMESTAMP '2020-01-05 00:00:00', '%m') AS c, STRFTIME(TIMESTAMP '2020-01-05 00:00:00', '%m') AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String, b:String, c:String, d:String] rows=1
    - ('0505', '055', '0101', '011')
- DuckDB: cols=[a:String, b:String, c:String, d:String] rows=1
    - ('05', '05', '01', '01')

## Family: dynamic-json (24)

### `agent-dynamic-json-0018` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=({
  "b": 2,
  "a": 1
}, '{"a":1,"b":2}') duck=('{"b":2,"a":1}', '{"b":2,"a":1}')

**KQL**
```kql
print d = dynamic({"b":2,"a":1}) | extend j = dynamic_to_json(d)
```
**Generated SQL**
```sql
SELECT *, CAST(CAST(d AS JSON) AS JSON) AS j FROM (SELECT '{"b":2,"a":1}'::JSON AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, j:String] rows=1
    - ({
  "b": 2,
  "a": 1
}, '{"a":1,"b":2}')
- DuckDB: cols=[d:String, j:String] rows=1
    - ('{"b":2,"a":1}', '{"b":2,"a":1}')

### `agent-dynamic-json-0019` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String], TYPE_MISMATCH[k:Dynamic|String], TYPE_MISMATCH[kk:Dynamic|String]  
*Detail:* first differing row[0]: kusto=({
  "x": 1
}, "x", "x") duck=('{"x":1}', 'x', 'x')

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"x":1}), dynamic({"y":2}) ] | mv-apply k = bag_keys(d) on (extend kk = k)
```
**Generated SQL**
```sql
SELECT t.*, _sub.* FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST('{"x":1}'::JSON AS JSON)), (CAST('{"y":2}'::JSON AS JSON))) AS t(d)) AS t, LATERAL (SELECT *, k AS kk FROM (SELECT u.value AS k FROM UNNEST(JSON_KEYS(CAST(d AS JSON))) AS u(value))) AS _sub
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, k:Dynamic, kk:Dynamic] rows=2
    - ({
  "x": 1
}, "x", "x")
    - ({
  "y": 2
}, "y", "y")
- DuckDB: cols=[d:String, k:String, kk:String] rows=2
    - ('{"x":1}', 'x', 'x')
    - ('{"y":2}', 'y', 'y')

### `agent-dynamic-json-0021` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[x:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(null, 'null', True) duck=('null', 'null', False)

**KQL**
```kql
print x = parse_json("null") | extend t = gettype(x), isn = isnull(x)
```
**Generated SQL**
```sql
SELECT *, CASE WHEN TYPEOF(CAST(x AS JSON)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(CAST(x AS JSON)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(CAST(x AS JSON)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(CAST(x AS JSON)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(CAST(x AS JSON)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(CAST(x AS JSON)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(CAST(x AS JSON)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(CAST(x AS JSON)) = 'UUID' THEN 'guid' WHEN TYPEOF(CAST(x AS JSON)) LIKE '%[]' OR TYPEOF(CAST(x AS JSON)) LIKE 'STRUCT%' OR TYPEOF(CAST(x AS JSON)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(CAST(x AS JSON)) = 'JSON' THEN (CASE json_type(CAST(CAST(x AS JSON) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(CAST(x AS JSON))) END AS t, (CAST(x AS JSON) IS NULL) AS isn FROM (SELECT TRY_CAST('null' AS JSON) AS x)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Dynamic, t:String, isn:Bool] rows=1
    - (null, 'null', True)
- DuckDB: cols=[x:String, t:String, isn:Bool] rows=1
    - ('null', 'null', False)

### `agent-dynamic-json-0040` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[oob:Dynamic|Int], TYPE_MISMATCH[neg:Dynamic|Int]  
*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3
], null, 3) duck=([1,2,3], null, null)

**KQL**
```kql
print d = dynamic([1,2,3]) | extend oob = d[10], neg = d[-1]
```
**Generated SQL**
```sql
SELECT *, d[10 + 1] AS oob, d[(-1) + 1] AS neg FROM (SELECT LIST_VALUE(1, 2, 3) AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, oob:Dynamic, neg:Dynamic] rows=1
    - ([
  1,
  2,
  3
], null, 3)
- DuckDB: cols=[d:Unknown, oob:Int, neg:Int] rows=1
    - ([1,2,3], null, null)

### `agent-dynamic-json-0045` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String], TYPE_MISMATCH[keys:Dynamic|String], TYPE_MISMATCH[kv:Dynamic|String]  
*Detail:* first differing row[0]: kusto=({
  "a": 1,
  "b": 2
}, "a", 1) duck=('{"a":1,"b":2}', 'a', '1')

**KQL**
```kql
print d = dynamic({"a":1,"b":2}) | extend keys = bag_keys(d) | mv-expand keys | extend kv = d[tostring(keys)]
```
**Generated SQL**
```sql
SELECT *, json_extract(d, '$.' || COALESCE(TRY_CAST(keys AS TEXT), '')) AS kv FROM (SELECT t.* EXCLUDE (keys), u.value AS keys FROM (SELECT *, JSON_KEYS(CAST(d AS JSON)) AS keys FROM (SELECT '{"a":1,"b":2}'::JSON AS d)) AS t CROSS JOIN UNNEST(t.keys) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, keys:Dynamic, kv:Dynamic] rows=2
    - ({
  "a": 1,
  "b": 2
}, "a", 1)
    - ({
  "a": 1,
  "b": 2
}, "b", 2)
- DuckDB: cols=[d:String, keys:String, kv:String] rows=2
    - ('{"a":1,"b":2}', 'a', '1')
    - ('{"a":1,"b":2}', 'b', '2')

### `agent-dynamic-json-0002` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String], TYPE_MISMATCH[deep:Dynamic|String], TYPE_MISMATCH[rt:Dynamic|String]  
*Detail:* first differing row[0]: kusto=({
  "a": {
    "b": {
      "c": [
        10,
        20,
        30
      ]
    }
  }
}, 30, 3, 20) duck=('{"a":{"b":{"c":[10,20,30]}}}', '30', 3, '30')

**KQL**
```kql
print d = dynamic({"a":{"b":{"c":[10,20,30]}}}) | extend deep = d.a.b.c[2], len = array_length(d.a.b.c), rt = todynamic(tostring(d.a.b.c))[1]
```
**Generated SQL**
```sql
SELECT *, json_extract(json_extract(d, '$.a.b.c'), '$[' || (CASE WHEN (2) < 0 THEN '#' ELSE '' END) || (2) || ']') AS deep, CASE WHEN json_type(json_extract(d, '$.a.b.c')) = 'ARRAY' THEN LEN(json_extract(json_extract(d, '$.a.b.c'), '$[*]')) ELSE NULL END AS len, TRY_CAST(COALESCE(json_extract_string(json_extract(d, '$.a.b.c'), '$'), '') AS JSON)[1 + 1] AS rt FROM (SELECT '{"a":{"b":{"c":[10,20,30]}}}'::JSON AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, deep:Dynamic, len:Int, rt:Dynamic] rows=1
    - ({
  "a": {
    "b": {
      "c": [
        10,
        20,
        30
      ]
    }
  }
}, 30, 3, 20)
- DuckDB: cols=[d:String, deep:String, len:Int, rt:String] rows=1
    - ('{"a":{"b":{"c":[10,20,30]}}}', '30', 3, '30')

### `agent-dynamic-json-0005` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=({
  "a": 1,
  "b": 2,
  "c": 4
}, [
  "a",
  "b",
  "c"
]) duck=({"{\u0022a\u0022:1}":1,"{\u0022a\u0022:3,\u0022c\u0022:4}":1,"{\u0022b\u0022:2}":1}, ["{\u0022a\u0022:1}","{\u0022a\u0022:3,\u0022c\u0022:4}","{\u0022b\u0022:2}"])

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1}), dynamic({"b":2}), dynamic({"a":3,"c":4}) ] | summarize merged = make_bag(d) | extend keys = bag_keys(merged)
```
**Generated SQL**
```sql
SELECT *, JSON_KEYS(merged) AS keys FROM (SELECT histogram(d) AS merged FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST('{"a":1}'::JSON AS JSON)), (CAST('{"b":2}'::JSON AS JSON)), (CAST('{"a":3,"c":4}'::JSON AS JSON))) AS t(d)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[merged:Dynamic, keys:Dynamic] rows=1
    - ({
  "a": 1,
  "b": 2,
  "c": 4
}, [
  "a",
  "b",
  "c"
])
- DuckDB: cols=[merged:Unknown, keys:Unknown] rows=1
    - ({"{\u0022a\u0022:1}":1,"{\u0022a\u0022:3,\u0022c\u0022:4}":1,"{\u0022b\u0022:2}":1}, ["{\u0022a\u0022:1}","{\u0022a\u0022:3,\u0022c\u0022:4}","{\u0022b\u0022:2}"])

### `agent-dynamic-json-0010` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String], TYPE_MISMATCH[m1:Dynamic|String], TYPE_MISMATCH[bval:Dynamic|String]  
*Detail:* first differing row[0]: kusto=({
  "a": 1,
  "b": 2
}, {
  "a": 1,
  "b": 2,
  "c": 3
}, [
  "a",
  "b",
  "c"
], null) duck=('{"a":1,"b":2}', '{"c":3,"a":1,"b":2}', ["c","a","b"], null)

**KQL**
```kql
print d = dynamic({"a":1,"b":2}) | extend m1 = bag_merge(d, dynamic({"b":{"nested":true},"c":3})) | extend bk = bag_keys(m1), bval = m1.b.nested
```
**Generated SQL**
```sql
SELECT *, JSON_KEYS(CAST(m1 AS JSON)) AS bk, json_extract(m1, '$.b.nested') AS bval FROM (SELECT *, JSON_MERGE_PATCH(COALESCE('{"b":{"nested":true},"c":3}'::JSON, '{}'::JSON), COALESCE(CAST(d AS JSON), '{}'::JSON)) AS m1 FROM (SELECT '{"a":1,"b":2}'::JSON AS d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, m1:Dynamic, bk:Dynamic, bval:Dynamic] rows=1
    - ({
  "a": 1,
  "b": 2
}, {
  "a": 1,
  "b": 2,
  "c": 3
}, [
  "a",
  "b",
  "c"
], null)
- DuckDB: cols=[d:String, m1:String, bk:Unknown, bval:String] rows=1
    - ('{"a":1,"b":2}', '{"c":3,"a":1,"b":2}', ["c","a","b"], null)

### `agent-dynamic-json-0021` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  null,
  1,
  null,
  2,
  null
], 5, 2) duck=('<unreadable:IndexOutOfRangeException>', 5, 2)

**KQL**
```kql
print d = dynamic([null, 1, null, 2, null]) | extend al = array_length(d), nonnull = array_length(set_difference(d, dynamic([null])))
```
**Generated SQL**
```sql
SELECT *, LEN(d) AS al, LEN(LIST_FILTER(d, x -> NOT LIST_CONTAINS(LIST_VALUE(NULL), x))) AS nonnull FROM (SELECT LIST_VALUE(NULL, 1, NULL, 2, NULL) AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, al:Int, nonnull:Int] rows=1
    - ([
  null,
  1,
  null,
  2,
  null
], 5, 2)
- DuckDB: cols=[d:Unknown, al:Int, nonnull:Int] rows=1
    - ('<unreadable:IndexOutOfRangeException>', 5, 2)

### `agent-dynamic-json-0026` — MismatchColumns (high)

*Detail:* column count: kusto=4 duck=5 (kusto: [d, a, b, c]; duck: [a, b, c, d, e])

**KQL**
```kql
print d = dynamic({"a":1,"b":"two","c":[3],"d":{"e":4}}) | evaluate bag_unpack(d)
```
**Generated SQL**
```sql
SELECT * EXCLUDE (d), d->>'$.a' AS a, d->>'$.b' AS b, d->>'$.c' AS c, d->>'$.d' AS d, d->>'$.e' AS e FROM (SELECT '{"a":1,"b":"two","c":[3],"d":{"e":4}}'::JSON AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, a:Int, b:String, c:Dynamic] rows=1
    - ({
  "e": 4
}, 1, 'two', [
  3
])
- DuckDB: cols=[a:String, b:String, c:String, d:String, e:String] rows=1
    - ('1', 'two', '[3]', '{"e":4}', null)

### `agent-dynamic-json-0029` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  10,
  20,
  30
], [
  20,
  30,
  10
], [
  null,
  10,
  20
]) duck=([10,20,30], [20,30,10], '<unreadable:IndexOutOfRangeException>')

**KQL**
```kql
print d = dynamic([10,20,30]) | extend rotated = array_rotate_left(d, 1), shifted = array_shift_right(d, 1)
```
**Generated SQL**
```sql
SELECT *, LIST_CONCAT(LIST_SLICE(d, 1 + 1, LEN(d)), LIST_SLICE(d, 1, 1)) AS rotated, LIST_CONCAT(LIST_TRANSFORM(GENERATE_SERIES(1, 1), x -> NULL), LIST_SLICE(d, 1, LEN(d) - 1)) AS shifted FROM (SELECT LIST_VALUE(10, 20, 30) AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, rotated:Dynamic, shifted:Dynamic] rows=1
    - ([
  10,
  20,
  30
], [
  20,
  30,
  10
], [
  null,
  10,
  20
])
- DuckDB: cols=[d:Unknown, rotated:Unknown, shifted:Unknown] rows=1
    - ([10,20,30], [20,30,10], '<unreadable:IndexOutOfRangeException>')

### `agent-dynamic-json-0030` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=({
  "a": [
    1,
    2,
    3
  ]
}, 1, -1) duck=('{"a":[1,2,3]}', 1, null)

**KQL**
```kql
print d = dynamic({"a":[1,2,3]}) | extend exists = array_index_of(d.a, 2), notfound = array_index_of(d.a, 99)
```
**Generated SQL**
```sql
SELECT *, (LIST_POSITION(json_extract(json_extract(d, '$.a'), '$[*]'), 2) - 1) AS "exists", (LIST_POSITION(json_extract(json_extract(d, '$.a'), '$[*]'), 99) - 1) AS notfound FROM (SELECT '{"a":[1,2,3]}'::JSON AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, exists:Int, notfound:Int] rows=1
    - ({
  "a": [
    1,
    2,
    3
  ]
}, 1, -1)
- DuckDB: cols=[d:String, exists:Int, notfound:Int] rows=1
    - ('{"a":[1,2,3]}', 1, null)

### `agent-dynamic-json-0037` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  2,
  4
]) duck=([4,2])

**KQL**
```kql
print d = dynamic({"nums":[1,2,3,4,5]}) | extend evens = array_length(d.nums) | mv-expand n = d.nums to typeof(long) | where n % 2 == 0 | summarize make_list(n)
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(n) FILTER (WHERE n IS NOT NULL), []) AS list_n FROM (SELECT * FROM (SELECT t.*, CAST(u.value AS BIGINT) AS n FROM (SELECT *, CASE WHEN json_type(json_extract(d, '$.nums')) = 'ARRAY' THEN LEN(json_extract(json_extract(d, '$.nums'), '$[*]')) ELSE NULL END AS evens FROM (SELECT '{"nums":[1,2,3,4,5]}'::JSON AS d)) AS t CROSS JOIN UNNEST(CASE WHEN json_extract(d, '$.nums') IS NULL THEN CAST([NULL] AS JSON[]) WHEN json_type(json_extract(d, '$.nums')) = 'ARRAY' THEN CAST(json_extract(d, '$.nums') AS JSON[]) WHEN json_type(json_extract(d, '$.nums')) = 'OBJECT' THEN list_transform(json_keys(json_extract(d, '$.nums')), lambda k: json_object(k, json_extract(json_extract(d, '$.nums'), '$."' || k || '"'))) ELSE CAST([json_extract(d, '$.nums')] AS JSON[]) END) AS u(value)) WHERE (((n) % NULLIF(2, 0)) + ABS(2)) % NULLIF(2, 0) = 0)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[list_n:Dynamic] rows=1
    - ([
  2,
  4
])
- DuckDB: cols=[list_n:Unknown] rows=1
    - ([4,2])

### `agent-dynamic-json-0040` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  true,
  false,
  true,
  null
], 4, True, null) duck=('<unreadable:IndexOutOfRangeException>', 4, True, null)

**KQL**
```kql
print d = dynamic([true, false, true, null]) | extend al = array_length(d), t0 = tobool(d[0]), tn = tobool(d[3])
```
**Generated SQL**
```sql
SELECT *, LEN(d) AS al, TRY_CAST(d[0 + 1] AS BOOLEAN) AS t0, TRY_CAST(d[3 + 1] AS BOOLEAN) AS tn FROM (SELECT LIST_VALUE(TRUE, FALSE, TRUE, NULL) AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, al:Int, t0:Bool, tn:Bool] rows=1
    - ([
  true,
  false,
  true,
  null
], 4, True, null)
- DuckDB: cols=[d:Unknown, al:Int, t0:Bool, tn:Bool] rows=1
    - ('<unreadable:IndexOutOfRangeException>', 4, True, null)

### `agent-dynamic-json-0002` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  "a=1",
  "b=2",
  "c=3"
]) duck=(["c=3","b=2","a=1"])

**KQL**
```kql
print d = dynamic({"a":1,"b":2,"c":3}) | extend ks = bag_keys(d) | mv-apply k = ks on (extend pair = strcat(tostring(k), "=", tostring(d[tostring(k)]))) | summarize joined = make_list(pair)
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(pair) FILTER (WHERE pair IS NOT NULL), []) AS joined FROM (SELECT t.*, _sub.* FROM (SELECT *, JSON_KEYS(CAST(d AS JSON)) AS ks FROM (SELECT '{"a":1,"b":2,"c":3}'::JSON AS d)) AS t, LATERAL (SELECT *, CONCAT(COALESCE(TRY_CAST(k AS TEXT), ''), '=', COALESCE(json_extract_string(json_extract(d, '$.' || COALESCE(TRY_CAST(k AS TEXT), '')), '$'), '')) AS pair FROM (SELECT u.value AS k FROM UNNEST(t.ks) AS u(value))) AS _sub)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[joined:Dynamic] rows=1
    - ([
  "a=1",
  "b=2",
  "c=3"
])
- DuckDB: cols=[joined:Unknown] rows=1
    - (["c=3","b=2","a=1"])

### `agent-dynamic-json-0006` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  "['a']",
  "['a']['b']",
  "['a']['b']['c']",
  "['x']",
  "['x']['y']"
]) duck=(["a","x"])

**KQL**
```kql
print d = dynamic({"a":{"b":{"c":1}},"x":{"y":2}}) | extend paths = treepath(d) | mv-expand p = paths to typeof(string) | sort by p asc | summarize make_list(p)
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(p) FILTER (WHERE p IS NOT NULL), []) AS list_p FROM (SELECT t.*, CAST(u.value AS VARCHAR) AS p FROM (SELECT *, JSON_KEYS(CAST(d AS JSON)) AS paths FROM (SELECT '{"a":{"b":{"c":1}},"x":{"y":2}}'::JSON AS d)) AS t CROSS JOIN UNNEST(t.paths) AS u(value) ORDER BY p ASC NULLS FIRST)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[list_p:Dynamic] rows=1
    - ([
  "['a']",
  "['a']['b']",
  "['a']['b']['c']",
  "['x']",
  "['x']['y']"
])
- DuckDB: cols=[list_p:Unknown] rows=1
    - (["a","x"])

### `agent-dynamic-json-0013` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, [
  "a",
  "b"
]) duck=(1, ["\u0022b\u0022","\u0022a\u0022"])

**KQL**
```kql
print d = dynamic([{"id":1,"t":["a","b"]},{"id":2,"t":[]},{"id":3,"t":["c"]}]) | mv-expand o = d | mv-expand tag = o.t | summarize tags = make_list(tag) by id = toint(o.id) | sort by id asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(o, '$.id') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(o, '$.id') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS id, COALESCE(LIST(tag) FILTER (WHERE tag IS NOT NULL), []) AS tags FROM (SELECT t.*, u.value AS tag FROM (SELECT t.*, u.value AS o FROM (SELECT '[{"id":1,"t":["a","b"]},{"id":2,"t":[]},{"id":3,"t":["c"]}]'::JSON AS d) AS t CROSS JOIN UNNEST(CASE WHEN t.d IS NULL THEN CAST([NULL] AS JSON[]) WHEN json_type(t.d) = 'ARRAY' THEN CAST(t.d AS JSON[]) WHEN json_type(t.d) = 'OBJECT' THEN list_transform(json_keys(t.d), lambda k: json_object(k, json_extract(t.d, '$."' || k || '"'))) ELSE CAST([t.d] AS JSON[]) END) AS u(value)) AS t CROSS JOIN UNNEST(CASE WHEN json_extract(o, '$.t') IS NULL THEN CAST([NULL] AS JSON[]) WHEN json_type(json_extract(o, '$.t')) = 'ARRAY' THEN CAST(json_extract(o, '$.t') AS JSON[]) WHEN json_type(json_extract(o, '$.t')) = 'OBJECT' THEN list_transform(json_keys(json_extract(o, '$.t')), lambda k: json_object(k, json_extract(json_extract(o, '$.t'), '$."' || k || '"'))) ELSE CAST([json_extract(o, '$.t')] AS JSON[]) END) AS u(value)) GROUP BY ALL) ORDER BY id ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[id:Int, tags:Dynamic] rows=2
    - (1, [
  "a",
  "b"
])
    - (3, [
  "c"
])
- DuckDB: cols=[id:Int, tags:Unknown] rows=2
    - (1, ["\u0022b\u0022","\u0022a\u0022"])
    - (3, ["\u0022c\u0022"])

### `agent-dynamic-json-0016` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=([
  [
    1,
    2
  ],
  [
    3,
    4
  ],
  [
    5,
    6
  ]
], [
  1,
  3,
  5,
  2,
  4,
  6
], 21) duck=('[[1,2],[3,4],[5,6]]', ["1","3","5","2","4","6"], 21)

**KQL**
```kql
print d = dynamic([[1,2],[3,4],[5,6]]) | extend cols = array_concat(pack_array(d[0][0],d[1][0],d[2][0]), pack_array(d[0][1],d[1][1],d[2][1])) | extend colsum = array_sum(cols)
```
**Generated SQL**
```sql
SELECT *, LIST_SUM(LIST_TRANSFORM(cols, x -> TRY_CAST(x AS DOUBLE))) AS colsum FROM (SELECT *, LIST_CONCAT(LIST_VALUE(json_extract(json_extract(d, '$[' || (CASE WHEN (0) < 0 THEN '#' ELSE '' END) || (0) || ']'), '$[' || (CASE WHEN (0) < 0 THEN '#' ELSE '' END) || (0) || ']'), json_extract(json_extract(d, '$[' || (CASE WHEN (1) < 0 THEN '#' ELSE '' END) || (1) || ']'), '$[' || (CASE WHEN (0) < 0 THEN '#' ELSE '' END) || (0) || ']'), json_extract(json_extract(d, '$[' || (CASE WHEN (2) < 0 THEN '#' ELSE '' END) || (2) || ']'), '$[' || (CASE WHEN (0) < 0 THEN '#' ELSE '' END) || (0) || ']')), LIST_VALUE(json_extract(json_extract(d, '$[' || (CASE WHEN (0) < 0 THEN '#' ELSE '' END) || (0) || ']'), '$[' || (CASE WHEN (1) < 0 THEN '#' ELSE '' END) || (1) || ']'), json_extract(json_extract(d, '$[' || (CASE WHEN (1) < 0 THEN '#' ELSE '' END) || (1) || ']'), '$[' || (CASE WHEN (1) < 0 THEN '#' ELSE '' END) || (1) || ']'), json_extract(json_extract(d, '$[' || (CASE WHEN (2) < 0 THEN '#' ELSE '' END) || (2) || ']'), '$[' || (CASE WHEN (1) < 0 THEN '#' ELSE '' END) || (1) || ']'))) AS cols FROM (SELECT '[[1,2],[3,4],[5,6]]'::JSON AS d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, cols:Dynamic, colsum:Real] rows=1
    - ([
  [
    1,
    2
  ],
  [
    3,
    4
  ],
  [
    5,
    6
  ]
], [
  1,
  3,
  5,
  2,
  4,
  6
], 21)
- DuckDB: cols=[d:String, cols:Unknown, colsum:Real] rows=1
    - ('[[1,2],[3,4],[5,6]]', ["1","3","5","2","4","6"], 21)

### `agent-dynamic-json-0020` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3
], True, False, False) duck=([1,2,3], True, False, True)

**KQL**
```kql
print a = dynamic([1,2,3]) | extend has2 = set_has_element(a, 2), has9 = set_has_element(a, 9), hasS = set_has_element(a, "2")
```
**Generated SQL**
```sql
SELECT *, LIST_CONTAINS(a, 2) AS has2, LIST_CONTAINS(a, 9) AS has9, LIST_CONTAINS(a, '2') AS hasS FROM (SELECT LIST_VALUE(1, 2, 3) AS a)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Dynamic, has2:Bool, has9:Bool, hasS:Bool] rows=1
    - ([
  1,
  2,
  3
], True, False, False)
- DuckDB: cols=[a:Unknown, has2:Bool, has9:Bool, hasS:Bool] rows=1
    - ([1,2,3], True, False, True)

### `agent-dynamic-json-0027` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[e:Dynamic|String], TYPE_MISMATCH[srt:Dynamic|String]  
*Detail:* first differing row[0]: kusto=('[]', [], 0, null, [], null) duck=('[]', '[]', 0, null, '[]', [])

**KQL**
```kql
print s = '[]' | extend e = parse_json(s) | extend al = array_length(e), s2 = array_sum(e), srt = array_sort_asc(e), keys = bag_keys(e)
```
**Generated SQL**
```sql
SELECT *, CASE WHEN json_type(CAST(e AS JSON)) = 'ARRAY' THEN LEN(json_extract(CAST(e AS JSON), '$[*]')) ELSE NULL END AS al, LIST_SUM(LIST_TRANSFORM(json_extract(CAST(e AS JSON), '$[*]'), x -> TRY_CAST(x AS DOUBLE))) AS s2, TO_JSON(LIST_SORT(json_extract(CAST(e AS JSON), '$[*]'))) AS srt, JSON_KEYS(CAST(e AS JSON)) AS keys FROM (SELECT *, TRY_CAST(s AS JSON) AS e FROM (SELECT '[]' AS s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, e:Dynamic, al:Int, s2:Real, srt:Dynamic, keys:Dynamic] rows=1
    - ('[]', [], 0, null, [], null)
- DuckDB: cols=[s:String, e:String, al:Int, s2:Real, srt:String, keys:Unknown] rows=1
    - ('[]', '[]', 0, null, '[]', [])

### `agent-dynamic-json-0034` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[b1:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(True, True, 1, True, 3) duck=(False, False, '1', False, 3)

**KQL**
```kql
print s = '{"a":null,"b":[null,1,null],"c":{"d":null}}' | extend d = parse_json(s) | project an = isnull(d.a), b0 = isnull(d.b[0]), b1 = d.b[1], cd = isnull(d.c.d), blen = array_length(d.b)
```
**Generated SQL**
```sql
SELECT (json_extract(d, '$.a') IS NULL) AS an, (json_extract(json_extract(d, '$.b'), '$[' || (CASE WHEN (0) < 0 THEN '#' ELSE '' END) || (0) || ']') IS NULL) AS b0, json_extract(json_extract(d, '$.b'), '$[' || (CASE WHEN (1) < 0 THEN '#' ELSE '' END) || (1) || ']') AS b1, (json_extract(d, '$.c.d') IS NULL) AS cd, CASE WHEN json_type(json_extract(d, '$.b')) = 'ARRAY' THEN LEN(json_extract(json_extract(d, '$.b'), '$[*]')) ELSE NULL END AS blen FROM (SELECT *, TRY_CAST(s AS JSON) AS d FROM (SELECT '{"a":null,"b":[null,1,null],"c":{"d":null}}' AS s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[an:Bool, b0:Bool, b1:Dynamic, cd:Bool, blen:Int] rows=1
    - (True, True, 1, True, 3)
- DuckDB: cols=[an:Bool, b0:Bool, b1:String, cd:Bool, blen:Int] rows=1
    - (False, False, '1', False, 3)

### `agent-dynamic-json-0043` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3
], [
  "a",
  "b",
  "c"
]) duck=([3,2,1], ["c","b","a"])

**KQL**
```kql
print x = dynamic([[1,"a"],[2,"b"],[3,"c"]]) | mv-expand pair = x | project num = toint(pair[0]), letter = tostring(pair[1]) | summarize make_list(num), make_list(letter)
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(num) FILTER (WHERE num IS NOT NULL), []) AS list_num, COALESCE(LIST(letter) FILTER (WHERE letter IS NOT NULL), []) AS list_letter FROM (SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(pair, '$[' || (CASE WHEN (0) < 0 THEN '#' ELSE '' END) || (0) || ']') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(pair, '$[' || (CASE WHEN (0) < 0 THEN '#' ELSE '' END) || (0) || ']') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS num, COALESCE(json_extract_string(json_extract(pair, '$[' || (CASE WHEN (1) < 0 THEN '#' ELSE '' END) || (1) || ']'), '$'), '') AS letter FROM (SELECT t.*, u.value AS pair FROM (SELECT '[[1,"a"],[2,"b"],[3,"c"]]'::JSON AS x) AS t CROSS JOIN UNNEST(CASE WHEN t.x IS NULL THEN CAST([NULL] AS JSON[]) WHEN json_type(t.x) = 'ARRAY' THEN CAST(t.x AS JSON[]) WHEN json_type(t.x) = 'OBJECT' THEN list_transform(json_keys(t.x), lambda k: json_object(k, json_extract(t.x, '$."' || k || '"'))) ELSE CAST([t.x] AS JSON[]) END) AS u(value)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[list_num:Dynamic, list_letter:Dynamic] rows=1
    - ([
  1,
  2,
  3
], [
  "a",
  "b",
  "c"
])
- DuckDB: cols=[list_num:Unknown, list_letter:Unknown] rows=1
    - ([3,2,1], ["c","b","a"])

### `t1-dynamic-json-0005` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[v:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(null) duck=('[]')

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]), dynamic({"nested":{"x":"y"}}), dynamic(null) ] | project v = array_sort_asc(d)
```
**Generated SQL**
```sql
SELECT TO_JSON(LIST_SORT(json_extract(CAST(d AS JSON), '$[*]'))) AS v FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST('{"a":1,"b":[1,2,3]}'::JSON AS JSON)), (CAST(LIST_VALUE(10, 20, 30) AS JSON)), (CAST('{"nested":{"x":"y"}}'::JSON AS JSON)), (CAST(NULL AS JSON))) AS t(d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[v:Dynamic] rows=4
    - (null)
    - ([
  10,
  20,
  30
])
    - (null)
    - (null)
- DuckDB: cols=[v:String] rows=4
    - ('[]')
    - ('[10,20,30]')
    - ('[]')
    - (null)

### `t1-dynamic-json-0007` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(null) duck=([])

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]), dynamic({"nested":{"x":"y"}}), dynamic(null) ] | project v = bag_keys(d)
```
**Generated SQL**
```sql
SELECT JSON_KEYS(CAST(d AS JSON)) AS v FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST('{"a":1,"b":[1,2,3]}'::JSON AS JSON)), (CAST(LIST_VALUE(10, 20, 30) AS JSON)), (CAST('{"nested":{"x":"y"}}'::JSON AS JSON)), (CAST(NULL AS JSON))) AS t(d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[v:Dynamic] rows=4
    - ([
  "a",
  "b"
])
    - (null)
    - ([
  "nested"
])
    - (null)
- DuckDB: cols=[v:Unknown] rows=4
    - (["a","b"])
    - ([])
    - (["nested"])
    - (null)

## Family: joins-lookup-union (2)

### `agent-joins-lookup-union-0033` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[z:Int|String], TYPE_MISMATCH[w:String|Int]  
*Detail:* first differing row[0]: kusto=(1, 'a', null, '') duck=(1, 'a', '', null)

**KQL**
```kql
let A = datatable(x:long, y:string)[ 1,"a" ]; let B = datatable(z:long, w:string)[ 9,"q" ]; A | union B
```
**Generated SQL**
```sql
WITH A AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT), 'a')) AS t(x, y)), B AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(9 AS BIGINT), 'q')) AS t(z, w)) (SELECT *, '' AS w FROM (SELECT * FROM A)) UNION ALL BY NAME (SELECT *, '' AS y FROM (SELECT * FROM B))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:String, z:Int, w:String] rows=2
    - (1, 'a', null, '')
    - (null, '', 9, 'q')
- DuckDB: cols=[x:Int, y:String, w:String, z:Int] rows=2
    - (1, 'a', '', null)
    - (null, '', 'q', 9)

### `agent-joins-lookup-union-0034` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 'a', '') duck=(1, 'a', null)

**KQL**
```kql
let L = datatable(k:long, lv:string)[ 1,"a", 2,"b", 3,"c" ]; let R = datatable(k:long, rv:string)[ 2,"x", 3,"y", 4,"z" ]; L | join kind=leftanti R on k | union (L | join kind=rightanti R on k) | project k, lv, rv | order by k asc
```
**Generated SQL**
```sql
WITH L AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT), 'a'), (CAST(2 AS BIGINT), 'b'), (CAST(3 AS BIGINT), 'c')) AS t(k, lv)), R AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(2 AS BIGINT), 'x'), (CAST(3 AS BIGINT), 'y'), (CAST(4 AS BIGINT), 'z')) AS t(k, rv)) SELECT k, lv, rv FROM ((SELECT L.* FROM L AS L WHERE NOT EXISTS (SELECT 1 FROM R AS R WHERE L.k = R.k)) UNION ALL BY NAME (SELECT L.* FROM R AS L WHERE NOT EXISTS (SELECT 1 FROM L AS R WHERE R.k = L.k))) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, lv:String, rv:String] rows=2
    - (1, 'a', '')
    - (4, '', 'z')
- DuckDB: cols=[k:Int, lv:String, rv:String] rows=2
    - (1, 'a', null)
    - (4, null, 'z')

## Family: nested-pipelines-let-cte (13)

### `agent-nested-pipelines-let-cte-0004` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(20, 2, 22) duck=(1, 2, 3)

**KQL**
```kql
print x = 1 | extend y = x + 1 | extend x = y * 10 | extend z = x + y | project x, y, z
```
**Generated SQL**
```sql
SELECT x, y, z FROM (SELECT *, x + y AS z FROM (SELECT *, y * 10 AS x FROM (SELECT *, x + 1 AS y FROM (SELECT 1 AS x))))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:Int, z:Int] rows=1
    - (20, 2, 22)
- DuckDB: cols=[x:Int, y:Int, z:Int] rows=1
    - (1, 2, 3)

### `agent-nested-pipelines-let-cte-0018` — MismatchColumns (high)

*Detail:* column count: kusto=5 duck=6 (kusto: [id, label, score, active, cat]; duck: [id, label, score, active, cat, label])

**KQL**
```kql
datatable(id:long, name:string, score:real, active:bool, ts:datetime, cat:string)[ 1,"a",1.5,true,datetime(2020-01-01),"p", 2,"b",2.5,false,datetime(2021-06-01),"q" ] | project-away ts | project-rename label = name | extend label = strcat(label, "_x") | sort by id asc
```
**Generated SQL**
```sql
SELECT *, CONCAT(label, '_x') AS label FROM (SELECT * RENAME (name AS label) FROM (SELECT * EXCLUDE (ts) FROM (VALUES (CAST(1 AS BIGINT), 'a', CAST(1.5 AS DOUBLE), TRUE, TIMESTAMP '2020-01-01 00:00:00', 'p'), (CAST(2 AS BIGINT), 'b', CAST(2.5 AS DOUBLE), FALSE, TIMESTAMP '2021-06-01 00:00:00', 'q')) AS t(id, name, score, active, ts, cat))) ORDER BY id ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[id:Int, label:String, score:Real, active:Bool, cat:String] rows=2
    - (1, 'a_x', 1.5, True, 'p')
    - (2, 'b_x', 2.5, False, 'q')
- DuckDB: cols=[id:Int, label:String, score:Real, active:Bool, cat:String, label:String] rows=2
    - (1, 'a', 1.5, True, 'p', 'a_x')
    - (2, 'b', 2.5, False, 'q', 'b_x')

### `agent-nested-pipelines-let-cte-0020` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[c:Dynamic|String]  
*Detail:* first differing row[0]: kusto=([
  "x",
  1
]) duck=('{"x":1}')

**KQL**
```kql
print a = 1, b = 2 | extend c = pack("x", a, "y", b) | mv-expand kind=array c | project c | sort by tostring(c) asc
```
**Generated SQL**
```sql
SELECT c FROM (SELECT t.* EXCLUDE (c), u.value AS c FROM (SELECT *, json_object('x', a, 'y', b) AS c FROM (SELECT 1 AS a, 2 AS b)) AS t CROSS JOIN UNNEST(CASE WHEN t.c IS NULL THEN CAST([NULL] AS JSON[]) WHEN json_type(t.c) = 'ARRAY' THEN CAST(t.c AS JSON[]) WHEN json_type(t.c) = 'OBJECT' THEN list_transform(json_keys(t.c), lambda k: json_object(k, json_extract(t.c, '$."' || k || '"'))) ELSE CAST([t.c] AS JSON[]) END) AS u(value)) ORDER BY COALESCE(json_extract_string(c, '$'), '') ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[c:Dynamic] rows=2
    - ([
  "x",
  1
])
    - ([
  "y",
  2
])
- DuckDB: cols=[c:String] rows=2
    - ('{"x":1}')
    - ('{"y":2}')

### `agent-nested-pipelines-let-cte-0024` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[frac:Int|Real]  
*Detail:* first differing row[0]: kusto=(5, 16) duck=(5, 16.666666666666668)

**KQL**
```kql
let x = datatable(n:long)[ 5, 10, 15 ]; let y = x | summarize total = sum(n); let z = toscalar(y); x | extend frac = n * 100 / z | project n, frac | sort by n asc
```
**Generated SQL**
```sql
WITH x AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(5 AS BIGINT)), (CAST(10 AS BIGINT)), (CAST(15 AS BIGINT))) AS t(n)), y AS NOT MATERIALIZED (SELECT COALESCE(SUM(n), 0) AS total FROM x) SELECT n, frac FROM (SELECT *, n * 100 / (SELECT * FROM y LIMIT 1) AS frac FROM x) ORDER BY n ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[n:Int, frac:Int] rows=3
    - (5, 16)
    - (10, 33)
    - (15, 50)
- DuckDB: cols=[n:Int, frac:Real] rows=3
    - (5, 16.666666666666668)
    - (10, 33.333333333333336)
    - (15, 50)

### `agent-nested-pipelines-let-cte-0029` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=1

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30 ] | partition by k (top 1 by v desc) | sort by k asc, v desc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v) ORDER BY v DESC LIMIT 1) ORDER BY k ASC NULLS FIRST, v DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, v:Int] rows=2
    - (1, 20)
    - (2, 30)
- DuckDB: cols=[k:Int, v:Int] rows=1
    - (2, 30)

### `agent-nested-pipelines-let-cte-0003` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(16, 4) duck=(4, 4)

**KQL**
```kql
print x = 1 | extend x = x + 1 | extend x = x + 1 | extend x = x + 1 | extend y = x | extend x = y * x | project x, y
```
**Generated SQL**
```sql
SELECT x, y FROM (SELECT *, y * x AS x FROM (SELECT *, x AS y FROM (SELECT * EXCLUDE (x), x + 1 AS x FROM (SELECT * EXCLUDE (x), x + 1 AS x FROM (SELECT * EXCLUDE (x), x + 1 AS x FROM (SELECT 1 AS x))))))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:Int] rows=1
    - (16, 4)
- DuckDB: cols=[x:Int, y:Int] rows=1
    - (4, 4)

### `agent-nested-pipelines-let-cte-0012` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[Column2:Dynamic|String]  
*Detail:* first differing row[0]: kusto=('x', 3, "Beta") duck=('x', 3, 'Beta')

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", "O'Brien","x" ] | summarize Column1 = count(), Column2 = make_list(s) by tag | mv-expand Column2 | project tag, Column1, Column2 | sort by tag asc, tostring(Column2) asc
```
**Generated SQL**
```sql
SELECT tag, Column1, Column2 FROM (SELECT t.* EXCLUDE (Column2), u.value AS Column2 FROM (SELECT tag, COUNT(*) AS Column1, COALESCE(LIST(s) FILTER (WHERE s IS NOT NULL), []) AS Column2 FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), ('O''Brien', 'x')) AS t(s, tag) GROUP BY ALL) AS t CROSS JOIN UNNEST(t.Column2) AS u(value)) ORDER BY tag ASC NULLS FIRST, COALESCE(TRY_CAST(Column2 AS TEXT), '') ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[tag:String, Column1:Int, Column2:Dynamic] rows=5
    - ('x', 3, "Beta")
    - ('x', 3, "O'Brien")
    - ('x', 3, "alpha")
    - ('y', 1, "café")
    - ('z', 1, "")
- DuckDB: cols=[tag:String, Column1:Int, Column2:String] rows=5
    - ('x', 3, 'Beta')
    - ('x', 3, 'O'Brien')
    - ('x', 3, 'alpha')
    - ('y', 1, 'café')
    - ('z', 1, '')

### `agent-nested-pipelines-let-cte-0013` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[frac:Int|Real], TYPE_MISMATCH[norm:Int|Real]  
*Detail:* first differing row[0]: kusto=(1, 6, 20) duck=(1, 6.666666666666667, 20)

**KQL**
```kql
let z = datatable(n:long)[ 1, 2, 3, 4, 5 ]; let s = toscalar(z | summarize sum(n)); let m = toscalar(z | summarize max(n)); z | extend frac = n * 100 / s | extend norm = n * 100 / m | project n, frac, norm | sort by n asc
```
**Generated SQL**
```sql
WITH z AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT)), (CAST(4 AS BIGINT)), (CAST(5 AS BIGINT))) AS t(n)) SELECT n, frac, norm FROM (SELECT *, n * 100 / (SELECT MAX(n) AS max_n FROM z LIMIT 1) AS norm FROM (SELECT *, n * 100 / (SELECT COALESCE(SUM(n), 0) AS sum_n FROM z LIMIT 1) AS frac FROM z)) ORDER BY n ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[n:Int, frac:Int, norm:Int] rows=5
    - (1, 6, 20)
    - (2, 13, 40)
    - (3, 20, 60)
    - (4, 26, 80)
    - (5, 33, 100)
- DuckDB: cols=[n:Int, frac:Real, norm:Real] rows=5
    - (1, 6.666666666666667, 20)
    - (2, 13.333333333333334, 40)
    - (3, 20, 60)
    - (4, 26.666666666666668, 80)
    - (5, 33.333333333333336, 100)

### `agent-nested-pipelines-let-cte-0014` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=1

**KQL**
```kql
datatable(i:int)[ 1, 2, 3, 4, 5, 6 ] | extend grp = i % 2 | partition by grp (summarize s = sum(i) | extend tag = "p") | sort by s asc
```
**Generated SQL**
```sql
SELECT *, 'p' AS tag FROM (SELECT COALESCE(SUM(i), 0) AS s FROM (SELECT *, (((i) % NULLIF(2, 0)) + ABS(2)) % NULLIF(2, 0) AS grp FROM (VALUES (1), (2), (3), (4), (5), (6)) AS t(i))) ORDER BY s ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, tag:String] rows=2
    - (9, 'p')
    - (12, 'p')
- DuckDB: cols=[s:Int, tag:String] rows=1
    - (21, 'p')

### `agent-nested-pipelines-let-cte-0021` — MismatchColumns (high)

*Detail:* column count: kusto=3 duck=4 (kusto: [c, b, a]; duck: [c, b, a, b_1])

**KQL**
```kql
datatable(a:long, b:long, c:long)[ 1,2,3, 4,5,6, 7,8,9 ] | extend a = c, c = a | project a, b, c | extend b = a + c | project-reorder c, b, a | sort by a asc
```
**Generated SQL**
```sql
SELECT c, b, a, * EXCLUDE (c, b, a) FROM (SELECT *, a + c AS b FROM (SELECT a, b, c FROM (SELECT * EXCLUDE (a, c), c AS a, a AS c FROM (VALUES (CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT)), (CAST(4 AS BIGINT), CAST(5 AS BIGINT), CAST(6 AS BIGINT)), (CAST(7 AS BIGINT), CAST(8 AS BIGINT), CAST(9 AS BIGINT))) AS t(a, b, c)))) ORDER BY a ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[c:Int, b:Int, a:Int] rows=3
    - (1, 4, 3)
    - (4, 10, 6)
    - (7, 16, 9)
- DuckDB: cols=[c:Int, b:Int, a:Int, b_1:Int] rows=3
    - (1, 2, 3, 4)
    - (4, 5, 6, 10)
    - (7, 8, 9, 16)

### `agent-nested-pipelines-let-cte-0022` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(2, 'neg', 1, -5.5) duck=(2, 'zero', 1, 0)

**KQL**
```kql
let nums = datatable(i:int, l:long, r:real)[ -5,-5,-5.5, 0,0,0.0, 3,2147483648,0.1, 7,100,3.25 ]; let s1 = nums | extend cat = case(i < 0, "neg", i == 0, "zero", "pos"); let s2 = s1 | summarize cnt = count(), avgr = avg(r) by cat; let s3 = s2 | sort by cnt desc; s3 | extend rn = row_number() | project rn, cat, cnt, avgr
```
**Generated SQL**
```sql
WITH nums AS NOT MATERIALIZED (SELECT * FROM (VALUES (-5, CAST(-5 AS BIGINT), CAST(-5.5 AS DOUBLE)), (0, CAST(0 AS BIGINT), CAST(0.0 AS DOUBLE)), (3, CAST(2147483648 AS BIGINT), CAST(0.1 AS DOUBLE)), (7, CAST(100 AS BIGINT), CAST(3.25 AS DOUBLE))) AS t(i, l, r)), s1 AS NOT MATERIALIZED (SELECT *, CASE WHEN i < 0 THEN 'neg' WHEN i = 0 THEN 'zero' ELSE 'pos' END AS cat FROM nums), s2 AS NOT MATERIALIZED (SELECT cat, COUNT(*) AS cnt, COALESCE(AVG(r), 'nan'::DOUBLE) AS avgr FROM s1 GROUP BY ALL), s3 AS NOT MATERIALIZED (SELECT * FROM s2 ORDER BY cnt DESC NULLS LAST) SELECT rn, cat, cnt, avgr FROM (SELECT *, ROW_NUMBER() OVER () AS rn FROM s3)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[rn:Int, cat:String, cnt:Int, avgr:Real] rows=3
    - (1, 'pos', 2, 1.675)
    - (2, 'neg', 1, -5.5)
    - (3, 'zero', 1, 0)
- DuckDB: cols=[rn:Int, cat:String, cnt:Int, avgr:Real] rows=3
    - (1, 'pos', 2, 1.675)
    - (2, 'zero', 1, 0)
    - (3, 'neg', 1, -5.5)

### `agent-nested-pipelines-let-cte-0038` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(10, 1, 9) duck=(10, 10, 0)

**KQL**
```kql
datatable(x:long, y:long)[ 1,10, 2,20, 3,30 ] | extend tmp = x | extend x = y | extend y = tmp | project-away tmp | extend z = x - y | project x, y, z | sort by z asc
```
**Generated SQL**
```sql
SELECT x, y, z FROM (SELECT *, x - y AS z FROM (SELECT * EXCLUDE (tmp) FROM (SELECT *, tmp AS y FROM (SELECT * EXCLUDE (x), y AS x FROM (SELECT *, x AS tmp FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(2 AS BIGINT), CAST(20 AS BIGINT)), (CAST(3 AS BIGINT), CAST(30 AS BIGINT))) AS t(x, y)))))) ORDER BY z ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:Int, z:Int] rows=3
    - (10, 1, 9)
    - (20, 2, 18)
    - (30, 3, 27)
- DuckDB: cols=[x:Int, y:Int, z:Int] rows=3
    - (10, 10, 0)
    - (20, 20, 0)
    - (30, 30, 0)

### `agent-nested-pipelines-let-cte-0031` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(x:long, g:string)[ 1,"a", 2,"a", 3,"b" ] | summarize s = sum(x) by g | where s > 2 | extend label = strcat("g_", g) | project label, s | order by s asc
```
**Generated SQL**
```sql
SELECT label, s FROM (SELECT *, CONCAT('g_', g) AS label FROM (SELECT g, COALESCE(SUM(x), 0) AS s FROM (VALUES (CAST(1 AS BIGINT), 'a'), (CAST(2 AS BIGINT), 'a'), (CAST(3 AS BIGINT), 'b')) AS t(x, g) GROUP BY ALL) WHERE s > 2) ORDER BY s ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[label:String, s:Int] rows=2
    - ('g_a', 3)
    - ('g_b', 3)
- DuckDB: cols=[label:String, s:Int] rows=2
    - ('g_b', 3)
    - ('g_a', 3)

## Family: null-and-edge (20)

### `agent-null-and-edge-0044` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('null', True, null) duck=('dictionary', True, null)

**KQL**
```kql
datatable(d:dynamic)[ dynamic(null), dynamic({"a":1}), dynamic([]) ] | extend t=gettype(d), n=isnull(d), an=array_length(d) | project t, n, an
```
**Generated SQL**
```sql
SELECT t, n, an FROM (SELECT *, CASE WHEN TYPEOF(CAST(d AS JSON)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(CAST(d AS JSON)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(CAST(d AS JSON)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(CAST(d AS JSON)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(CAST(d AS JSON)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(CAST(d AS JSON)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(CAST(d AS JSON)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(CAST(d AS JSON)) = 'UUID' THEN 'guid' WHEN TYPEOF(CAST(d AS JSON)) LIKE '%[]' OR TYPEOF(CAST(d AS JSON)) LIKE 'STRUCT%' OR TYPEOF(CAST(d AS JSON)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(CAST(d AS JSON)) = 'JSON' THEN (CASE json_type(CAST(CAST(d AS JSON) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(CAST(d AS JSON))) END AS t, (CAST(d AS JSON) IS NULL) AS n, CASE WHEN json_type(CAST(d AS JSON)) = 'ARRAY' THEN LEN(json_extract(CAST(d AS JSON), '$[*]')) ELSE NULL END AS an FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST(NULL AS JSON)), (CAST('{"a":1}'::JSON AS JSON)), (CAST(LIST_VALUE() AS JSON))) AS t(d)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:String, n:Bool, an:Int] rows=3
    - ('null', True, null)
    - ('dictionary', False, null)
    - ('array', False, 0)
- DuckDB: cols=[t:String, n:Bool, an:Int] rows=3
    - ('dictionary', True, null)
    - ('dictionary', False, null)
    - ('array', False, 0)

### `agent-null-and-edge-0003` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH  
*Detail:* first differing row[0]: kusto=('a', null, null, null, 'a', null, 'a') duck=('a', null, null, null, null, null, null)

**KQL**
```kql
datatable(g:string, x:long)[ "a",long(null), "b",long(null) ] | summarize mn=min(x), mx=max(x), arg=arg_max(x, g), argn=arg_min(x, g) by g | order by g asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT g, MIN(x) AS mn, MAX(x) AS mx, MAX(x) AS arg, ARG_MAX(g, x) AS g, MIN(x) AS argn, ARG_MIN(g, x) AS g1 FROM (VALUES ('a', CAST(CAST(NULL AS BIGINT) AS BIGINT)), ('b', CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(g, x) GROUP BY ALL) ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, mn:Int, mx:Int, arg:Int, g1:String, argn:Int, g2:String] rows=2
    - ('a', null, null, null, 'a', null, 'a')
    - ('b', null, null, null, 'b', null, 'b')
- DuckDB: cols=[g:String, mn:Int, mx:Int, arg:Int, g_1:String, argn:Int, g1:String] rows=2
    - ('a', null, null, null, null, null, null)
    - ('b', null, null, null, null, null, null)

### `agent-null-and-edge-0009` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('both', 'c1', 'fallback') duck=('both', 'c1', '')

**KQL**
```kql
print a=iif(isnull(int(null)), iif(isempty(""), "both", "n"), "no"), b=case(isnull(real(null)), "c1", isempty("x"), "c2", "def"), c=coalesce(tostring(int(null)), tostring(dynamic(null)), "fallback")
```
**Generated SQL**
```sql
SELECT CASE WHEN (CAST(NULL AS INTEGER) IS NULL) THEN CASE WHEN ('' IS NULL OR CAST('' AS VARCHAR) = '') THEN 'both' ELSE 'n' END ELSE 'no' END AS a, CASE WHEN (CAST(NULL AS DOUBLE) IS NULL) THEN 'c1' WHEN ('x' IS NULL OR CAST('x' AS VARCHAR) = '') THEN 'c2' ELSE 'def' END AS b, COALESCE(COALESCE(TRY_CAST(CAST(NULL AS INTEGER) AS TEXT), ''), COALESCE(TRY_CAST(NULL AS TEXT), ''), 'fallback') AS c
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String, b:String, c:String] rows=1
    - ('both', 'c1', 'fallback')
- DuckDB: cols=[a:String, b:String, c:String] rows=1
    - ('both', 'c1', '')

### `agent-null-and-edge-0013` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(NaN, NaN, NaN, False, True) duck=(NaN, NaN, NaN, True, True)

**KQL**
```kql
print a=todouble("nan")+1.0, b=todouble("inf")-todouble("inf"), c=todouble("inf")*0.0, d=todouble("nan")==todouble("nan"), e=todouble("inf")>1e308
```
**Generated SQL**
```sql
SELECT COALESCE(TRY_CAST('nan' AS DOUBLE), TRY_CAST(TRY_CAST('nan' AS BOOLEAN) AS DOUBLE)) + 1.0 AS a, COALESCE(TRY_CAST('inf' AS DOUBLE), TRY_CAST(TRY_CAST('inf' AS BOOLEAN) AS DOUBLE)) - COALESCE(TRY_CAST('inf' AS DOUBLE), TRY_CAST(TRY_CAST('inf' AS BOOLEAN) AS DOUBLE)) AS b, COALESCE(TRY_CAST('inf' AS DOUBLE), TRY_CAST(TRY_CAST('inf' AS BOOLEAN) AS DOUBLE)) * 0.0 AS c, COALESCE(TRY_CAST('nan' AS DOUBLE), TRY_CAST(TRY_CAST('nan' AS BOOLEAN) AS DOUBLE)) = COALESCE(TRY_CAST('nan' AS DOUBLE), TRY_CAST(TRY_CAST('nan' AS BOOLEAN) AS DOUBLE)) AS d, COALESCE(TRY_CAST('inf' AS DOUBLE), TRY_CAST(TRY_CAST('inf' AS BOOLEAN) AS DOUBLE)) > 1E+308 AS e
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Real, b:Real, c:Real, d:Bool, e:Bool] rows=1
    - (NaN, NaN, NaN, False, True)
- DuckDB: cols=[a:Real, b:Real, c:Real, d:Bool, e:Bool] rows=1
    - (NaN, NaN, NaN, True, True)

### `agent-null-and-edge-0016` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(9999-12-31T23:59:59.9999999Z, null, 3652058.00:00:00) duck=(9999-12-31T23:59:59.9999990Z, 9999-12-31T23:59:59.9999990Z, 3652058.00:00:00)

**KQL**
```kql
print d1=datetime(9999-12-31 23:59:59.9999999), d2=datetime(9999-12-31 23:59:59.9999999)+1tick, diff=datetime(9999-12-31)-datetime(0001-01-01)
```
**Generated SQL**
```sql
SELECT TIMESTAMP '9999-12-31 23:59:59.999999' AS d1, TIMESTAMP '9999-12-31 23:59:59.999999' + ((1 / 10.0) * INTERVAL '1 microsecond') AS d2, TIMESTAMP '9999-12-31 00:00:00' - TIMESTAMP '0001-01-01 00:00:00' AS diff
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d1:DateTime, d2:DateTime, diff:TimeSpan] rows=1
    - (9999-12-31T23:59:59.9999999Z, null, 3652058.00:00:00)
- DuckDB: cols=[d1:DateTime, d2:DateTime, diff:TimeSpan] rows=1
    - (9999-12-31T23:59:59.9999990Z, 9999-12-31T23:59:59.9999990Z, 3652058.00:00:00)

### `agent-null-and-edge-0024` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[va:Dynamic|String], TYPE_MISMATCH[vab:Dynamic|String], TYPE_MISMATCH[idx0:Dynamic|String]  
*Detail:* first differing row[0]: kusto=('null', null, null, null, null) duck=('dictionary', null, null, null, null)

**KQL**
```kql
datatable(d:dynamic)[ dynamic(null), dynamic({"a":null}), dynamic({"a":{"b":null}}), dynamic([null,1,null]) ] | extend va=d.a, vab=d.a.b, idx0=d[0], al=array_length(d), t=gettype(d) | project t, va, vab, idx0, al
```
**Generated SQL**
```sql
SELECT t, va, vab, idx0, al FROM (SELECT *, json_extract(d, '$.a') AS va, json_extract(d, '$.a.b') AS vab, json_extract(d, '$[' || (CASE WHEN (0) < 0 THEN '#' ELSE '' END) || (0) || ']') AS idx0, CASE WHEN json_type(CAST(d AS JSON)) = 'ARRAY' THEN LEN(json_extract(CAST(d AS JSON), '$[*]')) ELSE NULL END AS al, CASE WHEN TYPEOF(CAST(d AS JSON)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(CAST(d AS JSON)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(CAST(d AS JSON)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(CAST(d AS JSON)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(CAST(d AS JSON)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(CAST(d AS JSON)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(CAST(d AS JSON)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(CAST(d AS JSON)) = 'UUID' THEN 'guid' WHEN TYPEOF(CAST(d AS JSON)) LIKE '%[]' OR TYPEOF(CAST(d AS JSON)) LIKE 'STRUCT%' OR TYPEOF(CAST(d AS JSON)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(CAST(d AS JSON)) = 'JSON' THEN (CASE json_type(CAST(CAST(d AS JSON) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(CAST(d AS JSON))) END AS t FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST(NULL AS JSON)), (CAST('{"a":null}'::JSON AS JSON)), (CAST('{"a":{"b":null}}'::JSON AS JSON)), (CAST(LIST_VALUE(NULL, 1, NULL) AS JSON))) AS t(d)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:String, va:Dynamic, vab:Dynamic, idx0:Dynamic, al:Int] rows=4
    - ('null', null, null, null, null)
    - ('dictionary', null, null, null, null)
    - ('dictionary', {
  "b": null
}, null, null, null)
    - ('array', null, null, null, 3)
- DuckDB: cols=[t:String, va:String, vab:String, idx0:String, al:Int] rows=4
    - ('dictionary', null, null, null, null)
    - ('dictionary', 'null', null, null, null)
    - ('dictionary', '{"b":null}', 'null', null, null)
    - ('array', null, null, 'null', 3)

### `agent-null-and-edge-0028` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[a:Dynamic|String], TYPE_MISMATCH[b:Dynamic|String], TYPE_MISMATCH[c:Dynamic|String], TYPE_MISMATCH[d:Dynamic|String], TYPE_MISMATCH[e:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(null, [], {}, null, "not json", True) duck=('null', '[]', '{}', null, null, False)

**KQL**
```kql
print a=parse_json("null"), b=parse_json("[]"), c=parse_json("{}"), d=parse_json(""), e=parse_json("not json"), f=isnull(parse_json("null"))
```
**Generated SQL**
```sql
SELECT TRY_CAST('null' AS JSON) AS a, TRY_CAST('[]' AS JSON) AS b, TRY_CAST('{}' AS JSON) AS c, TRY_CAST('' AS JSON) AS d, TRY_CAST('not json' AS JSON) AS e, (TRY_CAST('null' AS JSON) IS NULL) AS f
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Dynamic, b:Dynamic, c:Dynamic, d:Dynamic, e:Dynamic, f:Bool] rows=1
    - (null, [], {}, null, "not json", True)
- DuckDB: cols=[a:String, b:String, c:String, d:String, e:String, f:Bool] rows=1
    - ('null', '[]', '{}', null, null, False)

### `agent-null-and-edge-0036` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('abc', 'c', 'bc', null, 'abc') duck=('abc', 'c', 'bc', 0, 'abc')

**KQL**
```kql
datatable(s:string)[ "abc", "", "x" ] | extend sub1=substring(s, 2, 10), sub2=substring(s, -2, 5), idx=indexof(s, ""), rep=replace_string(s, "", "_") | project s, sub1, sub2, idx, rep
```
**Generated SQL**
```sql
SELECT s, sub1, sub2, idx, rep FROM (SELECT *, (CASE WHEN (10) < 0 THEN '' ELSE SUBSTR(CAST(s AS VARCHAR), (CASE WHEN (2) < 0 THEN (2) ELSE (2) + 1 END), 10) END) AS sub1, (CASE WHEN (5) < 0 THEN '' ELSE SUBSTR(CAST(s AS VARCHAR), (CASE WHEN ((-2)) < 0 THEN ((-2)) ELSE ((-2)) + 1 END), 5) END) AS sub2, (INSTR(CAST(s AS VARCHAR), '') - 1) AS idx, REPLACE(s, '', '_') AS rep FROM (VALUES ('abc'), (''), ('x')) AS t(s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, sub1:String, sub2:String, idx:Int, rep:String] rows=3
    - ('abc', 'c', 'bc', null, 'abc')
    - ('', '', '', null, '')
    - ('x', '', '', null, 'x')
- DuckDB: cols=[s:String, sub1:String, sub2:String, idx:Int, rep:String] rows=3
    - ('abc', 'c', 'bc', 0, 'abc')
    - ('', '', '', 0, '')
    - ('x', '', 'x', 0, 'x')

### `agent-null-and-edge-0037` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('1--3', [
  "a",
  "",
  "b"
], [
  ""
], 1, '') duck=('1-3', ["a","","b"], [""], 1, '')

**KQL**
```kql
print a=strcat_array(dynamic([1,null,3]), "-"), b=split("a,,b", ","), c=split("", ","), d=array_length(split("", ",")), e=strcat_array(dynamic([]), ",")
```
**Generated SQL**
```sql
SELECT ARRAY_TO_STRING(LIST_VALUE(1, NULL, 3), '-') AS a, STRING_SPLIT(CAST('a,,b' AS VARCHAR), ',') AS b, STRING_SPLIT(CAST('' AS VARCHAR), ',') AS c, LEN(STRING_SPLIT(CAST('' AS VARCHAR), ',')) AS d, ARRAY_TO_STRING(LIST_VALUE(), ',') AS e
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String, b:Dynamic, c:Dynamic, d:Int, e:String] rows=1
    - ('1--3', [
  "a",
  "",
  "b"
], [
  ""
], 1, '')
- DuckDB: cols=[a:String, b:Unknown, c:Unknown, d:Int, e:String] rows=1
    - ('1-3', ["a","","b"], [""], 1, '')

### `agent-null-and-edge-0043` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=({
  "1": 10,
  "2": null
}) duck=({"{\u00221\u0022:10}":1,"{\u00221\u0022:null}":1,"{\u00222\u0022:null}":1})

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,long(null), 2,long(null) ] | summarize m=make_bag(pack(tostring(k), v)) | project m
```
**Generated SQL**
```sql
SELECT m FROM (SELECT histogram(json_object(COALESCE(TRY_CAST(k AS TEXT), ''), v)) AS m FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(CAST(NULL AS BIGINT) AS BIGINT)), (CAST(2 AS BIGINT), CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(k, v))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[m:Dynamic] rows=1
    - ({
  "1": 10,
  "2": null
})
- DuckDB: cols=[m:Unknown] rows=1
    - ({"{\u00221\u0022:10}":1,"{\u00221\u0022:null}":1,"{\u00222\u0022:null}":1})

### `agent-null-and-edge-0006` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('pq', 'fb') duck=('pq', '')

**KQL**
```kql
print a=iif(isnull(int(null)) and isempty(""), strcat("p",tostring(int(null)),"q"), "x"), b=coalesce(strcat(tostring(real(null))), "fb")
```
**Generated SQL**
```sql
SELECT CASE WHEN (CAST(NULL AS INTEGER) IS NULL) AND ('' IS NULL OR CAST('' AS VARCHAR) = '') THEN CONCAT('p', COALESCE(TRY_CAST(CAST(NULL AS INTEGER) AS TEXT), ''), 'q') ELSE 'x' END AS a, COALESCE(CONCAT(COALESCE(TRY_CAST(CAST(NULL AS DOUBLE) AS TEXT), '')), 'fb') AS b
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String, b:String] rows=1
    - ('pq', 'fb')
- DuckDB: cols=[a:String, b:String] rows=1
    - ('pq', '')

### `agent-null-and-edge-0008` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2) duck=(1)

**KQL**
```kql
datatable(x:long)[ 1, 2, long(null) ] | where not(x == 1) | count
```
**Generated SQL**
```sql
SELECT COUNT(*) AS Count FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(x) WHERE NOT (x = 1)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[Count:Int] rows=1
    - (2)
- DuckDB: cols=[Count:Int] rows=1
    - (1)

### `agent-null-and-edge-0010` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1) duck=(0)

**KQL**
```kql
datatable(x:long)[ 1, 2, long(null) ] | where x !in (1, 2) | count
```
**Generated SQL**
```sql
SELECT COUNT(*) AS Count FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(x) WHERE x NOT IN (1, 2)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[Count:Int] rows=1
    - (1)
- DuckDB: cols=[Count:Int] rows=1
    - (0)

### `agent-null-and-edge-0013` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1.5, 1, 1.5, 2) duck=(1.5, 1, 1, 2)

**KQL**
```kql
datatable(x:real)[ 1.5, real(null), 2.5 ] | extend b=bin(x, 1.0), f=floor(x, 0.5), c=ceiling(x) | project x, b, f, c | order by x asc nulls last
```
**Generated SQL**
```sql
SELECT x, b, f, c FROM (SELECT *, FLOOR((x)/(1.0))*(1.0) AS b, FLOOR(x) AS f, CEILING(x) AS c FROM (VALUES (CAST(1.5 AS DOUBLE)), (CAST(CAST(NULL AS DOUBLE) AS DOUBLE)), (CAST(2.5 AS DOUBLE))) AS t(x)) ORDER BY x ASC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Real, b:Real, f:Real, c:Real] rows=3
    - (1.5, 1, 1.5, 2)
    - (2.5, 2, 2.5, 3)
    - (null, null, null, null)
- DuckDB: cols=[x:Real, b:Real, f:Real, c:Real] rows=3
    - (1.5, 1, 1, 2)
    - (2.5, 2, 2, 3)
    - (null, null, null, null)

### `agent-null-and-edge-0019` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([], {}) duck=([], null)

**KQL**
```kql
datatable(x:int)[ ] | extend y=int(null) | summarize m=make_list(y), mb=make_bag(pack("k", y))
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(y) FILTER (WHERE y IS NOT NULL), []) AS m, histogram(json_object('k', y)) AS mb FROM (SELECT *, CAST(NULL AS INTEGER) AS y FROM (SELECT NULL AS x WHERE 1 = 0))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[m:Dynamic, mb:Dynamic] rows=1
    - ([], {})
- DuckDB: cols=[m:Unknown, mb:Unknown] rows=1
    - ([], null)

### `agent-null-and-edge-0023` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a|||b', ',', 2) duck=('a||b', null, 2)

**KQL**
```kql
print s=strcat_array(dynamic(["a", null, "", "b"]), "|"), j=strcat_array(dynamic([null, null]), ","), l=array_length(dynamic([null,null]))
```
**Generated SQL**
```sql
SELECT ARRAY_TO_STRING(LIST_VALUE('a', NULL, '', 'b'), '|') AS s, ARRAY_TO_STRING(LIST_VALUE(NULL, NULL), ',') AS j, LEN(LIST_VALUE(NULL, NULL)) AS l
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, j:String, l:Int] rows=1
    - ('a|||b', ',', 2)
- DuckDB: cols=[s:String, j:String, l:Int] rows=1
    - ('a||b', null, 2)

### `agent-null-and-edge-0027` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(9999-12-31T23:59:59.9999999Z, 0001-01-01T00:00:00.0000001Z, 3652058.00:00:00, null) duck=(9999-12-31T23:59:59.9999990Z, 0001-01-01T00:00:00.0000000Z, 3652058.00:00:00, 0001-01-01T00:00:00.0000000Z)

**KQL**
```kql
print t1=datetime(9999-12-31 23:59:59.9999999), t2=datetime(0001-01-01 00:00:00)+1tick, diff=datetime(9999-12-31)-datetime(0001-01-01), neg=datetime(0001-01-01)-1tick
```
**Generated SQL**
```sql
SELECT TIMESTAMP '9999-12-31 23:59:59.999999' AS t1, TIMESTAMP '0001-01-01 00:00:00' + ((1 / 10.0) * INTERVAL '1 microsecond') AS t2, TIMESTAMP '9999-12-31 00:00:00' - TIMESTAMP '0001-01-01 00:00:00' AS diff, TIMESTAMP '0001-01-01 00:00:00' - ((1 / 10.0) * INTERVAL '1 microsecond') AS neg
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t1:DateTime, t2:DateTime, diff:TimeSpan, neg:DateTime] rows=1
    - (9999-12-31T23:59:59.9999999Z, 0001-01-01T00:00:00.0000001Z, 3652058.00:00:00, null)
- DuckDB: cols=[t1:DateTime, t2:DateTime, diff:TimeSpan, neg:DateTime] rows=1
    - (9999-12-31T23:59:59.9999990Z, 0001-01-01T00:00:00.0000000Z, 3652058.00:00:00, 0001-01-01T00:00:00.0000000Z)

### `agent-null-and-edge-0032` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(True, 1, 'True', False, True, True) duck=(False, 0, 'False', True, False, False)

**KQL**
```kql
datatable(b:bool)[ true, false, bool(null) ] | extend i=toint(b), s=tostring(b), n=not(b), a=b and true, o=b or false | project b, i, s, n, a, o | order by b asc nulls last
```
**Generated SQL**
```sql
SELECT b, i, s, n, a, o FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(b AS DOUBLE), TRY_CAST(TRY_CAST(b AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS i, COALESCE(CASE WHEN b THEN 'True' WHEN NOT b THEN 'False' END, '') AS s, NOT (b) AS n, b AND TRUE AS a, b OR FALSE AS o FROM (VALUES (TRUE), (FALSE), (FALSE)) AS t(b)) ORDER BY b ASC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[b:Bool, i:Int, s:String, n:Bool, a:Bool, o:Bool] rows=3
    - (False, 0, 'False', True, False, False)
    - (True, 1, 'True', False, True, True)
    - (null, null, '', null, null, null)
- DuckDB: cols=[b:Bool, i:Int, s:String, n:Bool, a:Bool, o:Bool] rows=3
    - (False, 0, 'False', True, False, False)
    - (False, 0, 'False', True, False, False)
    - (True, 1, 'True', False, True, True)

### `agent-null-and-edge-0034` — MismatchRows (high)

*Detail:* first differing row[2]: kusto=('b', null, 0) duck=('b', null, 2)

**KQL**
```kql
datatable(g:string, x:long)[ "a",long(null), "a",2, "b",long(null), "b",long(null) ] | sort by g asc, x desc nulls first | extend rn=row_number(0, g!=prev(g)) | project g, x, rn
```
**Generated SQL**
```sql
SELECT g, x, rn FROM (SELECT *, (ROW_NUMBER() OVER (ORDER BY g ASC NULLS FIRST, x DESC NULLS FIRST) + (0 - 1)) AS rn FROM (VALUES ('a', CAST(CAST(NULL AS BIGINT) AS BIGINT)), ('a', CAST(2 AS BIGINT)), ('b', CAST(CAST(NULL AS BIGINT) AS BIGINT)), ('b', CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(g, x) ORDER BY g ASC NULLS FIRST, x DESC NULLS FIRST)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, x:Int, rn:Int] rows=4
    - ('a', null, 0)
    - ('a', 2, 1)
    - ('b', null, 0)
    - ('b', null, 1)
- DuckDB: cols=[g:String, x:Int, rn:Int] rows=4
    - ('a', null, 0)
    - ('a', 2, 1)
    - ('b', null, 2)
    - ('b', null, 3)

### `agent-null-and-edge-0044` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(-2147483649, 2147483647, 2147483647) duck=(-2147483649, null, null)

**KQL**
```kql
datatable(x:long)[ 2147483648, -2147483649, long(null) ] | extend i=toint(x), back=tolong(toint(x)) | project x, i, back | order by x asc nulls last
```
**Generated SQL**
```sql
SELECT x, i, back FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS i, TRY_CAST(TRUNC(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS back FROM (VALUES (CAST(2147483648 AS BIGINT)), (CAST(-2147483649 AS BIGINT)), (CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(x)) ORDER BY x ASC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, i:Int, back:Int] rows=3
    - (-2147483649, 2147483647, 2147483647)
    - (2147483648, -2147483648, -2147483648)
    - (null, null, null)
- DuckDB: cols=[x:Int, i:Int, back:Int] rows=3
    - (-2147483649, null, null)
    - (2147483648, null, null)
    - (null, null, null)

## Family: parse-search (4)

### `agent-parse-search-0007` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('[k1=v1][k2=v2]', '', 'v2]', '') duck=('[k1=v1][k2=v2]', '', 'v2', '')

**KQL**
```kql
datatable(s:string)[ "[k1=v1][k2=v2]", "[k3=v3]" ] | parse-kv s as (k1:string, k2:string, k3:string) with (pair_delimiter='][', kv_delimiter='=')
```
**Generated SQL**
```sql
SELECT *, TRIM(REGEXP_EXTRACT(s, '(^|]\[)\s*k1\s*=\s*([^\][]*)', 2)) AS k1, TRIM(REGEXP_EXTRACT(s, '(^|]\[)\s*k2\s*=\s*([^\][]*)', 2)) AS k2, TRIM(REGEXP_EXTRACT(s, '(^|]\[)\s*k3\s*=\s*([^\][]*)', 2)) AS k3 FROM (VALUES ('[k1=v1][k2=v2]'), ('[k3=v3]')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, k1:String, k2:String, k3:String] rows=2
    - ('[k1=v1][k2=v2]', '', 'v2]', '')
    - ('[k3=v3]', '', '', '')
- DuckDB: cols=[s:String, k1:String, k2:String, k3:String] rows=2
    - ('[k1=v1][k2=v2]', '', 'v2', '')
    - ('[k3=v3]', '', '', '')

### `agent-parse-search-0008` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('name="John Smith";city="New York"', 'John Smith', 'New York') duck=('name="John Smith";city="New York"', '"John Smith"', '"New York"')

**KQL**
```kql
datatable(s:string)[ "name=\"John Smith\";city=\"New York\"" ] | parse-kv s as (name:string, city:string) with (pair_delimiter=';', kv_delimiter='=', quote='"')
```
**Generated SQL**
```sql
SELECT *, TRIM(REGEXP_EXTRACT(s, '(^|;)\s*name\s*=\s*([^;]*)', 2)) AS name, TRIM(REGEXP_EXTRACT(s, '(^|;)\s*city\s*=\s*([^;]*)', 2)) AS city FROM (VALUES ('name="John Smith";city="New York"')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, name:String, city:String] rows=1
    - ('name="John Smith";city="New York"', 'John Smith', 'New York')
- DuckDB: cols=[s:String, name:String, city:String] rows=1
    - ('name="John Smith";city="New York"', '"John Smith"', '"New York"')

### `agent-parse-search-0040` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=2

**KQL**
```kql
datatable(a:string, b:long, c:real)[ "needle",42,3.14, "haystack",7,2.71 ] | search "needle" or 42 or 2.71 | project a
```
**Generated SQL**
```sql
SELECT a FROM (SELECT 'search_arg0' AS "$table", * FROM (VALUES ('needle', CAST(42 AS BIGINT), CAST(3.14 AS DOUBLE)), ('haystack', CAST(7 AS BIGINT), CAST(2.71 AS DOUBLE))) AS t(a, b, c) WHERE (((REGEXP_MATCHES(LOWER(CAST(a AS VARCHAR)), '(^|[^A-Za-z0-9])needle([^A-Za-z0-9]|$)') OR REGEXP_MATCHES(LOWER(CAST(b AS VARCHAR)), '(^|[^A-Za-z0-9])needle([^A-Za-z0-9]|$)') OR REGEXP_MATCHES(LOWER(CAST(c AS VARCHAR)), '(^|[^A-Za-z0-9])needle([^A-Za-z0-9]|$)')) OR 42) OR 2.71))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String] rows=1
    - ('needle')
- DuckDB: cols=[a:String] rows=2
    - ('needle')
    - ('haystack')

### `agent-parse-search-0037` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('quoted value', 'nested ;semicolon') duck=(''quoted value'', ''nested')

**KQL**
```kql
datatable(s:string)[ "key='quoted value';other='nested ;semicolon'" ] | parse-kv s as (key:string, other:string) with (pair_delimiter=';', kv_delimiter='=', quote="'") | project key, other
```
**Generated SQL**
```sql
SELECT "key", other FROM (SELECT *, TRIM(REGEXP_EXTRACT(s, '(^|;)\s*key\s*=\s*([^;]*)', 2)) AS key, TRIM(REGEXP_EXTRACT(s, '(^|;)\s*other\s*=\s*([^;]*)', 2)) AS other FROM (VALUES ('key=''quoted value'';other=''nested ;semicolon''')) AS t(s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[key:String, other:String] rows=1
    - ('quoted value', 'nested ;semicolon')
- DuckDB: cols=[key:String, other:String] rows=1
    - (''quoted value'', ''nested')

## Family: string-ops (22)

### `agent-string-ops-0015` — MismatchRows (high)

*Detail:* row count: kusto=3 duck=4

**KQL**
```kql
datatable(s:string)[ "O'Brien","D'Angelo","normal","it's" ] | where s contains "'" | project s, q=strcat(s,"!")
```
**Generated SQL**
```sql
SELECT s, CONCAT(s, '!') AS q FROM (VALUES ('O''Brien'), ('D''Angelo'), ('normal'), ('it''s')) AS t(s) WHERE s ILIKE '%%'
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, q:String] rows=3
    - ('O'Brien', 'O'Brien!')
    - ('D'Angelo', 'D'Angelo!')
    - ('it's', 'it's!')
- DuckDB: cols=[s:String, q:String] rows=4
    - ('O'Brien', 'O'Brien!')
    - ('D'Angelo', 'D'Angelo!')
    - ('normal', 'normal!')
    - ('it's', 'it's!')

### `agent-string-ops-0020` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[p0:Dynamic|String], TYPE_MISMATCH[p2:Dynamic|String], TYPE_MISMATCH[p5:Dynamic|String]  
*Detail:* first differing row[0]: kusto=("a", "c", null) duck=('a', 'c', null)

**KQL**
```kql
datatable(s:string)[ "a,b,c,d","one" ] | project p0=split(s,",")[0], p2=split(s,",")[2], p5=split(s,",")[5]
```
**Generated SQL**
```sql
SELECT STRING_SPLIT(CAST(s AS VARCHAR), ',')[0 + 1] AS p0, STRING_SPLIT(CAST(s AS VARCHAR), ',')[2 + 1] AS p2, STRING_SPLIT(CAST(s AS VARCHAR), ',')[5 + 1] AS p5 FROM (VALUES ('a,b,c,d'), ('one')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[p0:Dynamic, p2:Dynamic, p5:Dynamic] rows=2
    - ("a", "c", null)
    - ("one", null, null)
- DuckDB: cols=[p0:String, p2:String, p5:String] rows=2
    - ('a', 'c', null)
    - ('one', null, null)

### `agent-string-ops-0021` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('key=val', [
  "key"
], [
  "val"
]) duck=('key=val', ["key","val"], ["key","val"])

**KQL**
```kql
datatable(s:string)[ "key=val","a=b=c","noequals","=leading","trailing=" ] | project s, kv=split(s,"=",0), kv2=split(s,"=",1)
```
**Generated SQL**
```sql
SELECT s, STRING_SPLIT(CAST(s AS VARCHAR), '=') AS kv, STRING_SPLIT(CAST(s AS VARCHAR), '=') AS kv2 FROM (VALUES ('key=val'), ('a=b=c'), ('noequals'), ('=leading'), ('trailing=')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, kv:Dynamic, kv2:Dynamic] rows=5
    - ('key=val', [
  "key"
], [
  "val"
])
    - ('a=b=c', [
  "a"
], [
  "b"
])
    - ('noequals', [
  "noequals"
], null)
    - ('=leading', [
  ""
], [
  "leading"
])
    - ('trailing=', [
  "trailing"
], [
  ""
])
- DuckDB: cols=[s:String, kv:Unknown, kv2:Unknown] rows=5
    - ('key=val', ["key","val"], ["key","val"])
    - ('a=b=c', ["a","b","c"], ["a","b","c"])
    - ('noequals', ["noequals"], ["noequals"])
    - ('=leading', ["","leading"], ["","leading"])
    - ('trailing=', ["trailing",""], ["trailing",""])

### `agent-string-ops-0028` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a-a-1-True', 'a|a|end') duck=('a-a-1-true', 'a|a|end')

**KQL**
```kql
datatable(s:string)[ "a","b","c" ] | project cat=strcat(s,"-",s,"-",1,"-",true), catd=strcat_delim("|",s,s,"end")
```
**Generated SQL**
```sql
SELECT CONCAT(s, '-', s, '-', 1, '-', TRUE) AS cat, CONCAT_WS('|', s, s, 'end') AS catd FROM (VALUES ('a'), ('b'), ('c')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[cat:String, catd:String] rows=3
    - ('a-a-1-True', 'a|a|end')
    - ('b-b-1-True', 'b|b|end')
    - ('c-c-1-True', 'c|c|end')
- DuckDB: cols=[cat:String, catd:String] rows=3
    - ('a-a-1-true', 'a|a|end')
    - ('b-b-1-true', 'b|b|end')
    - ('c-c-1-true', 'c|c|end')

### `agent-string-ops-0029` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('one two three', 'one_two_three', '[one] [two] [three]') duck=('one two three', 'one_two_three', '[one] two three')

**KQL**
```kql
datatable(s:string)[ "one two three","a-b-c","x" ] | project s, repl=replace_string(s," ","_"), reg=replace_regex(s,@"(\w+)",@"[\1]")
```
**Generated SQL**
```sql
SELECT s, REPLACE(s, ' ', '_') AS repl, REGEXP_REPLACE(s, '(\w+)', '[\1]') AS reg FROM (VALUES ('one two three'), ('a-b-c'), ('x')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, repl:String, reg:String] rows=3
    - ('one two three', 'one_two_three', '[one] [two] [three]')
    - ('a-b-c', 'a-b-c', '[a]-[b]-[c]')
    - ('x', 'x', '[x]')
- DuckDB: cols=[s:String, repl:String, reg:String] rows=3
    - ('one two three', 'one_two_three', '[one] two three')
    - ('a-b-c', 'a-b-c', '[a]-b-c')
    - ('x', 'x', '[x]')

### `agent-string-ops-0032` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a1 b2 c3', [
  [
    "a",
    "1"
  ],
  [
    "b",
    "2"
  ],
  [
    "c",
    "3"
  ]
]) duck=('a1 b2 c3', ["a1","b2","c3"])

**KQL**
```kql
datatable(s:string)[ "a1 b2 c3","no digits","x9" ] | project s, all=extract_all(@"([a-z])(\d)",s)
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT_ALL(s, '([a-z])(\d)') AS "all" FROM (VALUES ('a1 b2 c3'), ('no digits'), ('x9')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, all:Dynamic] rows=3
    - ('a1 b2 c3', [
  [
    "a",
    "1"
  ],
  [
    "b",
    "2"
  ],
  [
    "c",
    "3"
  ]
])
    - ('no digits', null)
    - ('x9', [
  [
    "x",
    "9"
  ]
])
- DuckDB: cols=[s:String, all:Unknown] rows=3
    - ('a1 b2 c3', ["a1","b2","c3"])
    - ('no digits', [])
    - ('x9', ["x9"])

### `agent-string-ops-0018` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[p_last:Dynamic|String]  
*Detail:* first differing row[0]: kusto=('a,b,,c,', 5, "c") duck=('a,b,,c,', 5, 'c')

**KQL**
```kql
datatable(s:string)[ "a,b,,c,","",",",",," ] | project s, n=array_length(split(s,",")), p_last=split(s,",")[3]
```
**Generated SQL**
```sql
SELECT s, LEN(STRING_SPLIT(CAST(s AS VARCHAR), ',')) AS n, STRING_SPLIT(CAST(s AS VARCHAR), ',')[3 + 1] AS p_last FROM (VALUES ('a,b,,c,'), (''), (','), (',,')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, n:Int, p_last:Dynamic] rows=4
    - ('a,b,,c,', 5, "c")
    - ('', 1, null)
    - (',', 2, null)
    - (',,', 3, null)
- DuckDB: cols=[s:String, n:Int, p_last:String] rows=4
    - ('a,b,,c,', 5, 'c')
    - ('', 1, null)
    - (',', 2, null)
    - (',,', 3, null)

### `agent-string-ops-0020` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('one two three', 'one-two-three', 'noe wto htere') duck=('one two three', 'one-two three', 'noe two three')

**KQL**
```kql
datatable(s:string)[ "one two three","wordword","   " ] | project s, rr=replace_regex(s,@"\s+","-"), rr2=replace_regex(s,@"(\w)(\w)",@"\2\1")
```
**Generated SQL**
```sql
SELECT s, REGEXP_REPLACE(s, '\s+', '-') AS rr, REGEXP_REPLACE(s, '(\w)(\w)', '\2\1') AS rr2 FROM (VALUES ('one two three'), ('wordword'), ('   ')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, rr:String, rr2:String] rows=3
    - ('one two three', 'one-two-three', 'noe wto htere')
    - ('wordword', 'wordword', 'owdrowdr')
    - ('   ', '-', '   ')
- DuckDB: cols=[s:String, rr:String, rr2:String] rows=3
    - ('one two three', 'one-two three', 'noe two three')
    - ('wordword', 'wordword', 'owrdword')
    - ('   ', '-', '   ')

### `agent-string-ops-0021` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=('a1b2c3d4', [
  "1",
  "2",
  "3",
  "4"
], null) duck=('a1b2c3d4', ["1","2","3","4"], [])

**KQL**
```kql
datatable(s:string)[ "2020-01-02 03:04","a1b2c3d4","nope" ] | project s, all=extract_all(@"(\d+)",s), all2=extract_all(@"(\d)(\d)",dynamic([1,2]),s)
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT_ALL(s, '(\d+)') AS "all", LIST_TRANSFORM(range(1, GREATEST(LEN(REGEXP_EXTRACT_ALL(s, '(\d)(\d)', 1)), LEN(REGEXP_EXTRACT_ALL(s, '(\d)(\d)', 2))) + 1), lambda i: [REGEXP_EXTRACT_ALL(s, '(\d)(\d)', 1)[i], REGEXP_EXTRACT_ALL(s, '(\d)(\d)', 2)[i]]) AS all2 FROM (VALUES ('2020-01-02 03:04'), ('a1b2c3d4'), ('nope')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, all:Dynamic, all2:Dynamic] rows=3
    - ('2020-01-02 03:04', [
  "2020",
  "01",
  "02",
  "03",
  "04"
], [
  [
    "2",
    "0"
  ],
  [
    "2",
    "0"
  ],
  [
    "0",
    "1"
  ],
  [
    "0",
    "2"
  ],
  [
    "0",
    "3"
  ],
  [
    "0",
    "4"
  ]
])
    - ('a1b2c3d4', [
  "1",
  "2",
  "3",
  "4"
], null)
    - ('nope', null, null)
- DuckDB: cols=[s:String, all:Unknown, all2:Unknown] rows=3
    - ('2020-01-02 03:04', ["2020","01","02","03","04"], [["2","0"],["2","0"],["0","1"],["0","2"],["0","3"],["0","4"]])
    - ('a1b2c3d4', ["1","2","3","4"], [])
    - ('nope', [], [])

### `agent-string-ops-0035` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=4

**KQL**
```kql
datatable(s:string)[ "a_b","a%b","a\\b","a[b]" ] | where s contains "%" or s contains "_" | project s
```
**Generated SQL**
```sql
SELECT s FROM (VALUES ('a_b'), ('a%b'), ('a\b'), ('a[b]')) AS t(s) WHERE s ILIKE '%%%' OR s ILIKE '%_%'
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String] rows=2
    - ('a_b')
    - ('a%b')
- DuckDB: cols=[s:String] rows=4
    - ('a_b')
    - ('a%b')
    - ('a\b')
    - ('a[b]')

### `agent-string-ops-0007` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(' nbsp ', 'nbsp', 4, True) duck=(' nbsp ', ' nbsp ', 6, False)

**KQL**
```kql
datatable(s:string)[ "  spaced  "," nbsp ","tab\ttab","plain" ] | project s, t=trim(@"\s",s), tlen=strlen(trim(@"\s",s)), hasws=(s matches regex @"\s")
```
**Generated SQL**
```sql
SELECT s, REGEXP_REPLACE(REGEXP_REPLACE(s, '^(\s)', ''), '(\s)$', '') AS t, LENGTH(CAST(REGEXP_REPLACE(REGEXP_REPLACE(s, '^(\s)', ''), '(\s)$', '') AS VARCHAR)) AS tlen, (REGEXP_MATCHES(s, '\s')) AS hasws FROM (VALUES ('  spaced  '), (' nbsp '), ('tab	tab'), ('plain')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, t:String, tlen:Int, hasws:Bool] rows=4
    - ('  spaced  ', ' spaced ', 8, True)
    - (' nbsp ', 'nbsp', 4, True)
    - ('tab	tab', 'tab	tab', 7, True)
    - ('plain', 'plain', 5, False)
- DuckDB: cols=[s:String, t:String, tlen:Int, hasws:Bool] rows=4
    - ('  spaced  ', ' spaced ', 8, True)
    - (' nbsp ', ' nbsp ', 6, False)
    - ('tab	tab', 'tab	tab', 7, True)
    - ('plain', 'plain', 5, False)

### `agent-string-ops-0008` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('abcABCabc', '###', 'abc_ABCabc', 6) duck=('abcABCabc', '#ABCabc', 'abc_ABCabc', 6)

**KQL**
```kql
datatable(s:string)[ "abcABCabc","XYZxyz","123" ] | project s, ri=replace_regex(s,@"(?i)abc","#"), rg=replace_regex(s,@"([a-z])([A-Z])",@"\1_\2"), cnt=countof(s,@"[a-z]","regex")
```
**Generated SQL**
```sql
SELECT s, REGEXP_REPLACE(s, '(?i)abc', '#') AS ri, REGEXP_REPLACE(s, '([a-z])([A-Z])', '\1_\2') AS rg, LEN(REGEXP_EXTRACT_ALL(CAST(s AS VARCHAR), '[a-z]')) AS cnt FROM (VALUES ('abcABCabc'), ('XYZxyz'), ('123')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, ri:String, rg:String, cnt:Int] rows=3
    - ('abcABCabc', '###', 'abc_ABCabc', 6)
    - ('XYZxyz', 'XYZxyz', 'XYZxyz', 3)
    - ('123', '123', '123', 0)
- DuckDB: cols=[s:String, ri:String, rg:String, cnt:Int] rows=3
    - ('abcABCabc', '#ABCabc', 'abc_ABCabc', 6)
    - ('XYZxyz', 'XYZxyz', 'XYZxyz', 3)
    - ('123', '123', '123', 0)

### `agent-string-ops-0009` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a1b2c3', [
  [
    "a",
    "1"
  ],
  [
    "b",
    "2"
  ],
  [
    "c",
    "3"
  ]
], 3) duck=('a1b2c3', ["a1","b2","c3"], 3)

**KQL**
```kql
datatable(s:string)[ "a1b2c3","aXbYcZ","abc" ] | project s, ea=extract_all(@"([a-z])(\d)?",s), n=array_length(extract_all(@"([a-z])(\d)?",s))
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT_ALL(s, '([a-z])(\d)?') AS ea, LEN(REGEXP_EXTRACT_ALL(s, '([a-z])(\d)?')) AS n FROM (VALUES ('a1b2c3'), ('aXbYcZ'), ('abc')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, ea:Dynamic, n:Int] rows=3
    - ('a1b2c3', [
  [
    "a",
    "1"
  ],
  [
    "b",
    "2"
  ],
  [
    "c",
    "3"
  ]
], 3)
    - ('aXbYcZ', [
  [
    "a",
    ""
  ],
  [
    "b",
    ""
  ],
  [
    "c",
    ""
  ]
], 3)
    - ('abc', [
  [
    "a",
    ""
  ],
  [
    "b",
    ""
  ],
  [
    "c",
    ""
  ]
], 3)
- DuckDB: cols=[s:String, ea:Unknown, n:Int] rows=3
    - ('a1b2c3', ["a1","b2","c3"], 3)
    - ('aXbYcZ', ["a","b","c"], 3)
    - ('abc', ["a","b","c"], 3)

### `agent-string-ops-0010` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[p0:Dynamic|String], TYPE_MISMATCH[pneg:Dynamic|String]  
*Detail:* first differing row[0]: kusto=('first.second.third', "first", "third", 3, 'first/second') duck=('first.second.third', 'first', null, 3, 'first/second')

**KQL**
```kql
datatable(s:string)[ "first.second.third","onlyone","..","a..b" ] | project s, p0=split(s,".")[0], pneg=split(s,".")[-1], cnt=array_length(split(s,".")), joined=strcat_delim("/",tostring(split(s,".")[0]),tostring(split(s,".")[1]))
```
**Generated SQL**
```sql
SELECT s, STRING_SPLIT(CAST(s AS VARCHAR), '.')[0 + 1] AS p0, STRING_SPLIT(CAST(s AS VARCHAR), '.')[(-1) + 1] AS pneg, LEN(STRING_SPLIT(CAST(s AS VARCHAR), '.')) AS cnt, CONCAT_WS('/', COALESCE(TRY_CAST(STRING_SPLIT(CAST(s AS VARCHAR), '.')[0 + 1] AS TEXT), ''), COALESCE(TRY_CAST(STRING_SPLIT(CAST(s AS VARCHAR), '.')[1 + 1] AS TEXT), '')) AS joined FROM (VALUES ('first.second.third'), ('onlyone'), ('..'), ('a..b')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, p0:Dynamic, pneg:Dynamic, cnt:Int, joined:String] rows=4
    - ('first.second.third', "first", "third", 3, 'first/second')
    - ('onlyone', "onlyone", "onlyone", 1, 'onlyone/')
    - ('..', "", "", 3, '/')
    - ('a..b', "a", "b", 3, 'a/')
- DuckDB: cols=[s:String, p0:String, pneg:String, cnt:Int, joined:String] rows=4
    - ('first.second.third', 'first', null, 3, 'first/second')
    - ('onlyone', 'onlyone', null, 1, 'onlyone/')
    - ('..', '', null, 3, '/')
    - ('a..b', 'a', null, 3, 'a/')

### `agent-string-ops-0015` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=('xx', 'yy', 'x', 'xx') duck=('xx', 'yx', 'x', 'xx')

**KQL**
```kql
datatable(s:string)[ "x","xx","xxx","xxxx" ] | project s, rep=replace_regex(s,@"x",strcat("y")), trimmed=trim_end("x",s), subneg=substring(s,strlen(s)-2,2)
```
**Generated SQL**
```sql
SELECT s, REGEXP_REPLACE(s, 'x', CONCAT('y')) AS rep, REGEXP_REPLACE(s, '(x)$', '') AS trimmed, (CASE WHEN (2) < 0 THEN '' ELSE SUBSTR(CAST(s AS VARCHAR), (CASE WHEN (LENGTH(CAST(s AS VARCHAR)) - 2) < 0 THEN (LENGTH(CAST(s AS VARCHAR)) - 2) ELSE (LENGTH(CAST(s AS VARCHAR)) - 2) + 1 END), 2) END) AS subneg FROM (VALUES ('x'), ('xx'), ('xxx'), ('xxxx')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, rep:String, trimmed:String, subneg:String] rows=4
    - ('x', 'y', '', 'x')
    - ('xx', 'yy', 'x', 'xx')
    - ('xxx', 'yyy', 'xx', 'xx')
    - ('xxxx', 'yyyy', 'xxx', 'xx')
- DuckDB: cols=[s:String, rep:String, trimmed:String, subneg:String] rows=4
    - ('x', 'y', '', 'x')
    - ('xx', 'yx', 'x', 'xx')
    - ('xxx', 'yxx', 'xx', 'xx')
    - ('xxxx', 'yxxx', 'xxx', 'xx')

### `agent-string-ops-0017` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[cnt:Int|Real]  
*Detail:* first differing row[0]: kusto=('Hello\nWorld', 12, False, True, 1) duck=('Hello\nWorld', 12, True, False, 1)

**KQL**
```kql
datatable(s:string)[ "Hello\\nWorld","Hello\nWorld","a\\tb","a\tb" ] | project s, l=strlen(s), hasnl=(s contains "\n"), hasbs=(s contains "\\"), cnt=countof(s,"\\")
```
**Generated SQL**
```sql
SELECT s, LENGTH(CAST(s AS VARCHAR)) AS l, (s ILIKE '%\n%') AS hasnl, (s ILIKE '%\\%') AS hasbs, (LENGTH(s) - LENGTH(REPLACE(s, '\', ''))) / LENGTH('\') AS cnt FROM (VALUES ('Hello\nWorld'), ('Hello
World'), ('a\tb'), ('a	b')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, l:Int, hasnl:Bool, hasbs:Bool, cnt:Int] rows=4
    - ('Hello\nWorld', 12, False, True, 1)
    - ('Hello
World', 11, True, False, 0)
    - ('a\tb', 4, False, True, 1)
    - ('a	b', 3, False, False, 0)
- DuckDB: cols=[s:String, l:Int, hasnl:Bool, hasbs:Bool, cnt:Real] rows=4
    - ('Hello\nWorld', 12, True, False, 1)
    - ('Hello
World', 11, False, False, 0)
    - ('a\tb', 4, False, False, 1)
    - ('a	b', 3, False, False, 0)

### `agent-string-ops-0021` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[cnt:Int|Real]  
*Detail:* first differing row[0]: kusto=('ababab', 'X', [
  [
    "a",
    "b"
  ],
  [
    "a",
    "b"
  ],
  [
    "a",
    "b"
  ]
], 3) duck=('ababab', 'X', ["ab","ab","ab"], 3)

**KQL**
```kql
datatable(s:string)[ "ababab","aaa","abc","" ] | project s, ra=replace_regex(s,@"(ab)+","X"), ea=extract_all(@"(a)(b)?",s), cnt=countof(s,"ab")
```
**Generated SQL**
```sql
SELECT s, REGEXP_REPLACE(s, '(ab)+', 'X') AS ra, REGEXP_EXTRACT_ALL(s, '(a)(b)?') AS ea, (LENGTH(s) - LENGTH(REPLACE(s, 'ab', ''))) / LENGTH('ab') AS cnt FROM (VALUES ('ababab'), ('aaa'), ('abc'), ('')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, ra:String, ea:Dynamic, cnt:Int] rows=4
    - ('ababab', 'X', [
  [
    "a",
    "b"
  ],
  [
    "a",
    "b"
  ],
  [
    "a",
    "b"
  ]
], 3)
    - ('aaa', 'aaa', [
  [
    "a",
    ""
  ],
  [
    "a",
    ""
  ],
  [
    "a",
    ""
  ]
], 0)
    - ('abc', 'Xc', [
  [
    "a",
    "b"
  ]
], 1)
    - ('', '', null, 0)
- DuckDB: cols=[s:String, ra:String, ea:Unknown, cnt:Real] rows=4
    - ('ababab', 'X', ["ab","ab","ab"], 3)
    - ('aaa', 'aaa', ["a","a","a"], 0)
    - ('abc', 'Xc', ["ab"], 1)
    - ('', '', [], 0)

### `agent-string-ops-0026` — MismatchRows (high)

*Detail:* row count: kusto=3 duck=2

**KQL**
```kql
datatable(s:string)[ "ＡＢ１２","AB12","ＡＢ12" ] | where s matches regex @"\d" | project s, dig=extract_all(@"(\d)",s), l=strlen(s)
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT_ALL(s, '(\d)') AS dig, LENGTH(CAST(s AS VARCHAR)) AS l FROM (VALUES ('ＡＢ１２'), ('AB12'), ('ＡＢ12')) AS t(s) WHERE REGEXP_MATCHES(s, '\d')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, dig:Dynamic, l:Int] rows=3
    - ('ＡＢ１２', [
  "１",
  "２"
], 4)
    - ('AB12', [
  "1",
  "2"
], 4)
    - ('ＡＢ12', [
  "1",
  "2"
], 4)
- DuckDB: cols=[s:String, dig:Unknown, l:Int] rows=2
    - ('AB12', ["1","2"], 4)
    - ('ＡＢ12', ["1","2"], 4)

### `agent-string-ops-0030` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('x;y,z|w', [
  ";",
  ",",
  "|"
], 3, 'x_y_z_w') duck=('x;y,z|w', [";",",","|"], 3, 'x_y,z|w')

**KQL**
```kql
datatable(s:string)[ "x;y,z|w","a-b","single" ] | project s, sp=extract_all(@"([;,|-])",s), n=array_length(extract_all(@"([;,|-])",s)), repl=replace_regex(s,@"[;,|-]","_")
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT_ALL(s, '([;,|-])') AS sp, LEN(REGEXP_EXTRACT_ALL(s, '([;,|-])')) AS n, REGEXP_REPLACE(s, '[;,|-]', '_') AS repl FROM (VALUES ('x;y,z|w'), ('a-b'), ('single')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, sp:Dynamic, n:Int, repl:String] rows=3
    - ('x;y,z|w', [
  ";",
  ",",
  "|"
], 3, 'x_y_z_w')
    - ('a-b', [
  "-"
], 1, 'a_b')
    - ('single', null, null, 'single')
- DuckDB: cols=[s:String, sp:Unknown, n:Int, repl:String] rows=3
    - ('x;y,z|w', [";",",","|"], 3, 'x_y,z|w')
    - ('a-b', ["-"], 1, 'a_b')
    - ('single', [], 0, 'single')

### `agent-string-ops-0032` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[cnt:Int|Real]  
*Detail:* first differing row[0]: kusto=('<tag>text</tag>', 'tag', [
  "tag",
  "/tag"
], 2) duck=('<tag>text</tag>', 'tag', ["\u003Ctag","\u003C/tag"], 2)

**KQL**
```kql
datatable(s:string)[ "<tag>text</tag>","<br/>","plain text","<a href=\"x\">" ] | project s, inner=extract(@"<([a-z]+)",1,s), all=extract_all(@"<(/?[a-z]+)",s), cnt=countof(s,"<")
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT(s, '<([a-z]+)', 1) AS "inner", REGEXP_EXTRACT_ALL(s, '<(/?[a-z]+)') AS "all", (LENGTH(s) - LENGTH(REPLACE(s, '<', ''))) / LENGTH('<') AS cnt FROM (VALUES ('<tag>text</tag>'), ('<br/>'), ('plain text'), ('<a href="x">')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, inner:String, all:Dynamic, cnt:Int] rows=4
    - ('<tag>text</tag>', 'tag', [
  "tag",
  "/tag"
], 2)
    - ('<br/>', 'br', [
  "br"
], 1)
    - ('plain text', '', null, 0)
    - ('<a href="x">', 'a', [
  "a"
], 1)
- DuckDB: cols=[s:String, inner:String, all:Unknown, cnt:Real] rows=4
    - ('<tag>text</tag>', 'tag', ["\u003Ctag","\u003C/tag"], 2)
    - ('<br/>', 'br', ["\u003Cbr"], 1)
    - ('plain text', '', [], 0)
    - ('<a href="x">', 'a', ["\u003Ca"], 1)

### `agent-string-ops-0036` — MismatchRows (high)

*Detail:* first differing row[2]: kusto=('a b', 3, 3, 'ab', True) duck=('a b', 3, 3, 'ab', False)

**KQL**
```kql
datatable(s:string)[ "aaa​bbb","a\tb","a b","ab" ] | project s, l=strlen(s), b=string_size(s), nospace=replace_string(replace_string(s," ",""),"\t",""), hasspace=(s has " ")
```
**Generated SQL**
```sql
SELECT s, LENGTH(CAST(s AS VARCHAR)) AS l, OCTET_LENGTH(ENCODE(CAST(s AS VARCHAR))) AS b, REPLACE(REPLACE(s, ' ', ''), '	', '') AS nospace, (regexp_matches(CAST(s AS VARCHAR), '(?i)(?:^|[^\p{L}\p{N}]) (?:[^\p{L}\p{N}]|$)')) AS hasspace FROM (VALUES ('aaa​bbb'), ('a	b'), ('a b'), ('ab')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, l:Int, b:Int, nospace:String, hasspace:Bool] rows=4
    - ('aaa​bbb', 7, 9, 'aaa​bbb', False)
    - ('a	b', 3, 3, 'ab', False)
    - ('a b', 3, 3, 'ab', True)
    - ('ab', 2, 2, 'ab', False)
- DuckDB: cols=[s:String, l:Int, b:Int, nospace:String, hasspace:Bool] rows=4
    - ('aaa​bbb', 7, 9, 'aaa​bbb', False)
    - ('a	b', 3, 3, 'ab', False)
    - ('a b', 3, 3, 'ab', False)
    - ('ab', 2, 2, 'ab', False)

### `agent-string-ops-0039` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('abcXYZabc', False, '#xyz#', True) duck=('abcXYZabc', False, '#xyzabc', True)

**KQL**
```kql
datatable(s:string)[ "abcXYZabc","ABCxyzABC","mix" ] | project s, lo_has=(tolower(s) has "abc"), rep=replace_regex(tolower(s),@"abc","#"), eqi=(s =~ "abcxyzabc")
```
**Generated SQL**
```sql
SELECT s, (regexp_matches(CAST(LOWER(s) AS VARCHAR), '(?i)(?:^|[^\p{L}\p{N}])abc(?:[^\p{L}\p{N}]|$)')) AS lo_has, REGEXP_REPLACE(LOWER(s), 'abc', '#') AS rep, (UPPER(s) = UPPER('abcxyzabc')) AS eqi FROM (VALUES ('abcXYZabc'), ('ABCxyzABC'), ('mix')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, lo_has:Bool, rep:String, eqi:Bool] rows=3
    - ('abcXYZabc', False, '#xyz#', True)
    - ('ABCxyzABC', False, '#xyz#', True)
    - ('mix', False, 'mix', False)
- DuckDB: cols=[s:String, lo_has:Bool, rep:String, eqi:Bool] rows=3
    - ('abcXYZabc', False, '#xyzabc', True)
    - ('ABCxyzABC', False, '#xyzabc', True)
    - ('mix', False, 'mix', False)

## Family: type-casts-coercion (41)

### `agent-type-casts-coercion-0006` — MismatchRows (high)

*Detail:* first differing row[3]: kusto=('3.14', null, null, 3.14) duck=('3.14', 3, 3, 3.14)

**KQL**
```kql
datatable(s:string)[ "123", "  45  ", "-9", "3.14", "abc", "", "0x1F", "1e3", "+7", "  " ] | extend ti = toint(s), tl = tolong(s), td = todouble(s)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ti, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tl, COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE)) AS td FROM (VALUES ('123'), ('  45  '), ('-9'), ('3.14'), ('abc'), (''), ('0x1F'), ('1e3'), ('+7'), ('  ')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, ti:Int, tl:Int, td:Real] rows=10
    - ('123', 123, 123, 123)
    - ('  45  ', 45, 45, 45)
    - ('-9', -9, -9, -9)
    - ('3.14', null, null, 3.14)
    - ('abc', null, null, null)
    - ('', null, null, null)
    - ('0x1F', 31, 31, null)
    - ('1e3', null, null, 1000)
- DuckDB: cols=[s:String, ti:Int, tl:Int, td:Real] rows=10
    - ('123', 123, 123, 123)
    - ('  45  ', 45, 45, 45)
    - ('-9', -9, -9, -9)
    - ('3.14', 3, 3, 3.14)
    - ('abc', null, null, null)
    - ('', null, null, null)
    - ('0x1F', null, null, null)
    - ('1e3', 1000, 1000, 1000)

### `agent-type-casts-coercion-0008` — MismatchRows (high)

*Detail:* first differing row[5]: kusto=('yes', null) duck=('yes', True)

**KQL**
```kql
datatable(s:string)[ "true", "True", "FALSE", "1", "0", "yes", "", "tRuE" ] | extend b = tobool(s)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(s AS BOOLEAN) AS b FROM (VALUES ('true'), ('True'), ('FALSE'), ('1'), ('0'), ('yes'), (''), ('tRuE')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, b:Bool] rows=8
    - ('true', True)
    - ('True', True)
    - ('FALSE', False)
    - ('1', True)
    - ('0', False)
    - ('yes', null)
    - ('', null)
    - ('tRuE', True)
- DuckDB: cols=[s:String, b:Bool] rows=8
    - ('true', True)
    - ('True', True)
    - ('FALSE', False)
    - ('1', True)
    - ('0', False)
    - ('yes', True)
    - ('', null)
    - ('tRuE', True)

### `agent-type-casts-coercion-0015` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('dictionary', 'array', 'timespan', 'decimal') duck=('dictionary', 'array', 'timespan', 'real')

**KQL**
```kql
print g7 = gettype(dynamic({"a":1})), g8 = gettype(dynamic([1,2])), g9 = gettype(1s), g10 = gettype(todecimal(1))
```
**Generated SQL**
```sql
SELECT CASE WHEN TYPEOF('{"a":1}'::JSON) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF('{"a":1}'::JSON) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF('{"a":1}'::JSON) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF('{"a":1}'::JSON) = 'VARCHAR' THEN 'string' WHEN TYPEOF('{"a":1}'::JSON) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF('{"a":1}'::JSON) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF('{"a":1}'::JSON) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF('{"a":1}'::JSON) = 'UUID' THEN 'guid' WHEN TYPEOF('{"a":1}'::JSON) LIKE '%[]' OR TYPEOF('{"a":1}'::JSON) LIKE 'STRUCT%' OR TYPEOF('{"a":1}'::JSON) LIKE 'MAP%' THEN 'array' WHEN TYPEOF('{"a":1}'::JSON) = 'JSON' THEN (CASE json_type(CAST('{"a":1}'::JSON AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF('{"a":1}'::JSON)) END AS g7, CASE WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(LIST_VALUE(1, 2)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'UUID' THEN 'guid' WHEN TYPEOF(LIST_VALUE(1, 2)) LIKE '%[]' OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'STRUCT%' OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'JSON' THEN (CASE json_type(CAST(LIST_VALUE(1, 2) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(LIST_VALUE(1, 2))) END AS g8, CASE WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF((1000 * INTERVAL '1 millisecond')) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) = 'VARCHAR' THEN 'string' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) = 'UUID' THEN 'guid' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) LIKE '%[]' OR TYPEOF((1000 * INTERVAL '1 millisecond')) LIKE 'STRUCT%' OR TYPEOF((1000 * INTERVAL '1 millisecond')) LIKE 'MAP%' THEN 'array' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) = 'JSON' THEN (CASE json_type(CAST((1000 * INTERVAL '1 millisecond') AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF((1000 * INTERVAL '1 millisecond'))) END AS g9, CASE WHEN TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) LIKE '%[]' OR TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18))) = 'JSON' THEN (CASE json_type(CAST(TRY_CAST(1 AS DECIMAL(38, 18)) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(1 AS DECIMAL(38, 18)))) END AS g10
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g7:String, g8:String, g9:String, g10:String] rows=1
    - ('dictionary', 'array', 'timespan', 'decimal')
- DuckDB: cols=[g7:String, g8:String, g9:String, g10:String] rows=1
    - ('dictionary', 'array', 'timespan', 'real')

### `agent-type-casts-coercion-0028` — MismatchRows (high)

*Detail:* first differing row[3]: kusto=('1_000', null) duck=('1_000', 1000)

**KQL**
```kql
datatable(s:string)[ "1.5e10", "0.0001", "-0", "1_000", "Inf", "NaN", "0b101" ] | extend d = todouble(s)
```
**Generated SQL**
```sql
SELECT *, COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE)) AS d FROM (VALUES ('1.5e10'), ('0.0001'), ('-0'), ('1_000'), ('Inf'), ('NaN'), ('0b101')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, d:Real] rows=7
    - ('1.5e10', 15000000000)
    - ('0.0001', 0.0001)
    - ('-0', -0)
    - ('1_000', null)
    - ('Inf', Infinity)
    - ('NaN', NaN)
    - ('0b101', null)
- DuckDB: cols=[s:String, d:Real] rows=7
    - ('1.5e10', 15000000000)
    - ('0.0001', 0.0001)
    - ('-0', -0)
    - ('1_000', 1000)
    - ('Inf', Infinity)
    - ('NaN', NaN)
    - ('0b101', null)

### `agent-type-casts-coercion-0033` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(10, 3.3333333333333335, 'decimal') duck=(10.0, 3.3333333333333335, 'real')

**KQL**
```kql
datatable(d:decimal)[ decimal(10), decimal(3), decimal(0) ] | extend div = d / decimal(3), gt = gettype(d)
```
**Generated SQL**
```sql
SELECT *, d / 3.0 AS div, CASE WHEN TYPEOF(d) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(d) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(d) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(d) = 'VARCHAR' THEN 'string' WHEN TYPEOF(d) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(d) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(d) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(d) = 'UUID' THEN 'guid' WHEN TYPEOF(d) LIKE '%[]' OR TYPEOF(d) LIKE 'STRUCT%' OR TYPEOF(d) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(d) = 'JSON' THEN (CASE json_type(CAST(d AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(d)) END AS gt FROM (VALUES (10.0), (3.0), (0.0)) AS t(d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Real, div:Real, gt:String] rows=3
    - (10, 3.3333333333333335, 'decimal')
    - (3, 1, 'decimal')
    - (0, 0, 'decimal')
- DuckDB: cols=[d:Real, div:Real, gt:String] rows=3
    - (10.0, 3.3333333333333335, 'real')
    - (3.0, 1, 'real')
    - (0.0, 0, 'real')

### `agent-type-casts-coercion-0034` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(255, 16) duck=(null, null)

**KQL**
```kql
print h2i = toint("0xFF"), h2l = tolong("0x10")
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST('0xFF' AS DOUBLE), TRY_CAST(TRY_CAST('0xFF' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS h2i, TRY_CAST(TRUNC(COALESCE(TRY_CAST('0x10' AS DOUBLE), TRY_CAST(TRY_CAST('0x10' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS h2l
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[h2i:Int, h2l:Int] rows=1
    - (255, 16)
- DuckDB: cols=[h2i:Int, h2l:Int] rows=1
    - (null, null)

### `agent-type-casts-coercion-0040` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('9223372036854775807', 9223372036854775807) duck=('9223372036854775807', null)

**KQL**
```kql
datatable(s:string)[ "9223372036854775807", "9223372036854775808", "-9223372036854775808" ] | extend l = tolong(s)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS l FROM (VALUES ('9223372036854775807'), ('9223372036854775808'), ('-9223372036854775808')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, l:Int] rows=3
    - ('9223372036854775807', 9223372036854775807)
    - ('9223372036854775808', null)
    - ('-9223372036854775808', -9223372036854775808)
- DuckDB: cols=[s:String, l:Int] rows=3
    - ('9223372036854775807', null)
    - ('9223372036854775808', null)
    - ('-9223372036854775808', -9223372036854775808)

### `agent-type-casts-coercion-0042` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(False, True, True) duck=(True, False, True)

**KQL**
```kql
print comp1 = real(nan) == real(nan), comp2 = real(nan) != real(nan), comp3 = real(+inf) > 1e308
```
**Generated SQL**
```sql
SELECT CAST('nan' AS DOUBLE) = CAST('nan' AS DOUBLE) AS comp1, CAST('nan' AS DOUBLE) IS DISTINCT FROM CAST('nan' AS DOUBLE) AS comp2, CAST('inf' AS DOUBLE) > 1E+308 AS comp3
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[comp1:Bool, comp2:Bool, comp3:Bool] rows=1
    - (False, True, True)
- DuckDB: cols=[comp1:Bool, comp2:Bool, comp3:Bool] rows=1
    - (True, False, True)

### `agent-type-casts-coercion-0001` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(-9223372036854775807, 0, -1317624576693539401, -9223372036854775807, True) duck=(-9223372036854775807, 0, -1317624576693539328, -9223372036854775296, False)

**KQL**
```kql
datatable(l:long)[ -9223372036854775807, -100, 0, 100, 9223372036854775807 ] | extend m = l % 7, d = l / 7, recon = (l / 7) * 7 + (l % 7), eq = ((l / 7) * 7 + (l % 7)) == l
```
**Generated SQL**
```sql
SELECT *, (((l) % NULLIF(7, 0)) + ABS(7)) % NULLIF(7, 0) AS m, CAST(TRUNC(CAST(l AS DOUBLE) / NULLIF(7, 0)) AS BIGINT) AS d, (CAST(TRUNC(CAST(l AS DOUBLE) / NULLIF(7, 0)) AS BIGINT)) * 7 + ((((l) % NULLIF(7, 0)) + ABS(7)) % NULLIF(7, 0)) AS recon, ((CAST(TRUNC(CAST(l AS DOUBLE) / NULLIF(7, 0)) AS BIGINT)) * 7 + ((((l) % NULLIF(7, 0)) + ABS(7)) % NULLIF(7, 0))) = l AS eq FROM (VALUES (CAST(-9223372036854775807 AS BIGINT)), (CAST(-100 AS BIGINT)), (CAST(0 AS BIGINT)), (CAST(100 AS BIGINT)), (CAST(9223372036854775807 AS BIGINT))) AS t(l)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[l:Int, m:Int, d:Int, recon:Int, eq:Bool] rows=5
    - (-9223372036854775807, 0, -1317624576693539401, -9223372036854775807, True)
    - (-100, 5, -14, -93, False)
    - (0, 0, 0, 0, True)
    - (100, 2, 14, 100, True)
    - (9223372036854775807, 0, 1317624576693539401, 9223372036854775807, True)
- DuckDB: cols=[l:Int, m:Int, d:Int, recon:Int, eq:Bool] rows=5
    - (-9223372036854775807, 0, -1317624576693539328, -9223372036854775296, False)
    - (-100, 5, -14, -93, False)
    - (0, 0, 0, 0, True)
    - (100, 2, 14, 100, True)
    - (9223372036854775807, 0, 1317624576693539328, 9223372036854775296, False)

### `agent-type-casts-coercion-0008` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2147483647, 9223372036854775807, 31, -1, null) duck=(null, null, null, null, null)

**KQL**
```kql
print h1 = toint("0x7FFFFFFF"), h2 = tolong("0x7FFFFFFFFFFFFFFF"), h3 = toint("0X1f"), h4 = tolong("0xFFFFFFFFFFFFFFFF"), h5 = toint("-0x10")
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST('0x7FFFFFFF' AS DOUBLE), TRY_CAST(TRY_CAST('0x7FFFFFFF' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS h1, TRY_CAST(TRUNC(COALESCE(TRY_CAST('0x7FFFFFFFFFFFFFFF' AS DOUBLE), TRY_CAST(TRY_CAST('0x7FFFFFFFFFFFFFFF' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS h2, TRY_CAST(TRUNC(COALESCE(TRY_CAST('0X1f' AS DOUBLE), TRY_CAST(TRY_CAST('0X1f' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS h3, TRY_CAST(TRUNC(COALESCE(TRY_CAST('0xFFFFFFFFFFFFFFFF' AS DOUBLE), TRY_CAST(TRY_CAST('0xFFFFFFFFFFFFFFFF' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS h4, TRY_CAST(TRUNC(COALESCE(TRY_CAST('-0x10' AS DOUBLE), TRY_CAST(TRY_CAST('-0x10' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS h5
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[h1:Int, h2:Int, h3:Int, h4:Int, h5:Int] rows=1
    - (2147483647, 9223372036854775807, 31, -1, null)
- DuckDB: cols=[h1:Int, h2:Int, h3:Int, h4:Int, h5:Int] rows=1
    - (null, null, null, null, null)

### `agent-type-casts-coercion-0009` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('1.5e3', 1500, null, null) duck=('1.5e3', 1500, 1500, 1500)

**KQL**
```kql
datatable(s:string)[ "1.5e3", "1.5E3", "1.5e+3", "1.5e-3", ".5", "5.", "+.5", "1_000", "0b11", "1d" ] | extend td = todouble(s), ti = toint(s), tl = tolong(s)
```
**Generated SQL**
```sql
SELECT *, COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE)) AS td, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ti, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tl FROM (VALUES ('1.5e3'), ('1.5E3'), ('1.5e+3'), ('1.5e-3'), ('.5'), ('5.'), ('+.5'), ('1_000'), ('0b11'), ('1d')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, td:Real, ti:Int, tl:Int] rows=10
    - ('1.5e3', 1500, null, null)
    - ('1.5E3', 1500, null, null)
    - ('1.5e+3', 1500, null, null)
    - ('1.5e-3', 0.0015, null, null)
    - ('.5', 0.5, null, null)
    - ('5.', 5, null, null)
    - ('+.5', 0.5, null, null)
    - ('1_000', null, null, null)
- DuckDB: cols=[s:String, td:Real, ti:Int, tl:Int] rows=10
    - ('1.5e3', 1500, 1500, 1500)
    - ('1.5E3', 1500, 1500, 1500)
    - ('1.5e+3', 1500, 1500, 1500)
    - ('1.5e-3', 0.0015, 0, 0)
    - ('.5', 0.5, 0, 0)
    - ('5.', 5, 5, 5)
    - ('+.5', 0.5, 0, 0)
    - ('1_000', 1000, 1000, 1000)

### `agent-type-casts-coercion-0010` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(NaN, NaN, null, -Infinity, Infinity, Infinity) duck=(NaN, NaN, Infinity, -Infinity, Infinity, Infinity)

**KQL**
```kql
print sp1 = todouble("  NaN  "), sp2 = todouble("nan"), sp3 = todouble("INF"), sp4 = todouble("-inf"), sp5 = todouble("Infinity"), sp6 = todouble("+Infinity")
```
**Generated SQL**
```sql
SELECT COALESCE(TRY_CAST('  NaN  ' AS DOUBLE), TRY_CAST(TRY_CAST('  NaN  ' AS BOOLEAN) AS DOUBLE)) AS sp1, COALESCE(TRY_CAST('nan' AS DOUBLE), TRY_CAST(TRY_CAST('nan' AS BOOLEAN) AS DOUBLE)) AS sp2, COALESCE(TRY_CAST('INF' AS DOUBLE), TRY_CAST(TRY_CAST('INF' AS BOOLEAN) AS DOUBLE)) AS sp3, COALESCE(TRY_CAST('-inf' AS DOUBLE), TRY_CAST(TRY_CAST('-inf' AS BOOLEAN) AS DOUBLE)) AS sp4, COALESCE(TRY_CAST('Infinity' AS DOUBLE), TRY_CAST(TRY_CAST('Infinity' AS BOOLEAN) AS DOUBLE)) AS sp5, COALESCE(TRY_CAST('+Infinity' AS DOUBLE), TRY_CAST(TRY_CAST('+Infinity' AS BOOLEAN) AS DOUBLE)) AS sp6
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[sp1:Real, sp2:Real, sp3:Real, sp4:Real, sp5:Real, sp6:Real] rows=1
    - (NaN, NaN, null, -Infinity, Infinity, Infinity)
- DuckDB: cols=[sp1:Real, sp2:Real, sp3:Real, sp4:Real, sp5:Real, sp6:Real] rows=1
    - (NaN, NaN, Infinity, -Infinity, Infinity, Infinity)

### `agent-type-casts-coercion-0011` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(False, True, False, False, NaN, NaN) duck=(True, False, False, True, 1, NaN)

**KQL**
```kql
print nan_eq = real(nan) == real(nan), nan_ne = real(nan) != real(nan), nan_lt = real(nan) < 1.0, nan_gt = real(nan) > 1.0, nan_min = min_of(real(nan), 1.0), nan_max = max_of(real(nan), 1.0)
```
**Generated SQL**
```sql
SELECT CAST('nan' AS DOUBLE) = CAST('nan' AS DOUBLE) AS nan_eq, CAST('nan' AS DOUBLE) IS DISTINCT FROM CAST('nan' AS DOUBLE) AS nan_ne, CAST('nan' AS DOUBLE) < 1.0 AS nan_lt, CAST('nan' AS DOUBLE) > 1.0 AS nan_gt, LEAST(CAST('nan' AS DOUBLE), 1.0) AS nan_min, GREATEST(CAST('nan' AS DOUBLE), 1.0) AS nan_max
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[nan_eq:Bool, nan_ne:Bool, nan_lt:Bool, nan_gt:Bool, nan_min:Real, nan_max:Real] rows=1
    - (False, True, False, False, NaN, NaN)
- DuckDB: cols=[nan_eq:Bool, nan_ne:Bool, nan_lt:Bool, nan_gt:Bool, nan_min:Real, nan_max:Real] rows=1
    - (True, False, False, True, 1, NaN)

### `agent-type-casts-coercion-0014` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(2147483648, -2147483648, 2147483648, 2147483648, True) duck=('2147483648', null, 2147483648, 2147483648, True)

**KQL**
```kql
datatable(d:dynamic)[ dynamic(2147483648), dynamic(1.9), dynamic("42"), dynamic("3.14"), dynamic("0x10"), dynamic(true), dynamic(null), dynamic([1]) ] | extend ti = toint(d), tl = tolong(d), td = todouble(d), tb = tobool(d)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ti, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tl, COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE)) AS td, TRY_CAST(d AS BOOLEAN) AS tb FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST('2147483648'::JSON AS JSON)), (CAST('1.9'::JSON AS JSON)), (CAST('"42"'::JSON AS JSON)), (CAST('"3.14"'::JSON AS JSON)), (CAST('"0x10"'::JSON AS JSON)), (CAST('true'::JSON AS JSON)), (CAST(NULL AS JSON)), (CAST(LIST_VALUE(1) AS JSON))) AS t(d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, ti:Int, tl:Int, td:Real, tb:Bool] rows=8
    - (2147483648, -2147483648, 2147483648, 2147483648, True)
    - (1.9, 1, 1, 1.9, True)
    - ("42", 42, 42, 42, True)
    - ("3.14", null, null, 3.14, null)
    - ("0x10", 16, 16, null, True)
    - (true, 1, 1, 1, True)
    - (null, null, null, null, null)
    - ([
  1
], null, null, null, null)
- DuckDB: cols=[d:String, ti:Int, tl:Int, td:Real, tb:Bool] rows=8
    - ('2147483648', null, 2147483648, 2147483648, True)
    - ('1.9', 1, 1, 1.9, True)
    - ('"42"', 42, 42, 42, null)
    - ('"3.14"', 3, 3, 3.14, null)
    - ('"0x10"', null, null, null, null)
    - ('true', 1, 1, 1, True)
    - (null, null, null, null, null)
    - ('[1]', null, null, null, null)

### `agent-type-casts-coercion-0015` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(9, 9, null, null, 9223372036854775807) duck=(9, 9, null, null, null)

**KQL**
```kql
print dyn_int = toint(dynamic(9.99)), dyn_long = tolong(dynamic(9.99)), dyn_arr = toint(dynamic([1,2,3])), dyn_obj = todouble(dynamic({"a":1})), dyn_str = tolong(dynamic("9223372036854775807"))
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST('9.99'::JSON AS DOUBLE), TRY_CAST(TRY_CAST('9.99'::JSON AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS dyn_int, TRY_CAST(TRUNC(COALESCE(TRY_CAST('9.99'::JSON AS DOUBLE), TRY_CAST(TRY_CAST('9.99'::JSON AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS dyn_long, TRY_CAST(TRUNC(COALESCE(TRY_CAST(LIST_VALUE(1, 2, 3) AS DOUBLE), TRY_CAST(TRY_CAST(LIST_VALUE(1, 2, 3) AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS dyn_arr, COALESCE(TRY_CAST('{"a":1}'::JSON AS DOUBLE), TRY_CAST(TRY_CAST('{"a":1}'::JSON AS BOOLEAN) AS DOUBLE)) AS dyn_obj, TRY_CAST(TRUNC(COALESCE(TRY_CAST('"9223372036854775807"'::JSON AS DOUBLE), TRY_CAST(TRY_CAST('"9223372036854775807"'::JSON AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS dyn_str
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[dyn_int:Int, dyn_long:Int, dyn_arr:Int, dyn_obj:Real, dyn_str:Int] rows=1
    - (9, 9, null, null, 9223372036854775807)
- DuckDB: cols=[dyn_int:Int, dyn_long:Int, dyn_arr:Int, dyn_obj:Real, dyn_str:Int] rows=1
    - (9, 9, null, null, null)

### `agent-type-casts-coercion-0016` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1970-01-01T00:00:00.0000000Z, 621355968000000000, 6.21355968E+17, 1970-01-01T00:00:00.0000000Z) duck=(1970-01-01T00:00:00.0000000Z, 621355968000000000, 6.21355968E+17, null)

**KQL**
```kql
datatable(t:datetime)[ datetime(1970-01-01), datetime(0001-01-01), datetime(9999-12-31 23:59:59.9999999), datetime(2020-02-29) ] | extend asl = tolong(t), asr = todouble(t), roundtrip = todatetime(tolong(t))
```
**Generated SQL**
```sql
SELECT *, CAST(ROUND((EXTRACT(EPOCH FROM (t)) - EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00')) * 10000000) AS BIGINT) AS asl, CAST(ROUND((EXTRACT(EPOCH FROM (t)) - EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00')) * 10000000) AS DOUBLE) AS asr, COALESCE(TRY_CAST(CAST(ROUND((EXTRACT(EPOCH FROM (t)) - EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00')) * 10000000) AS BIGINT) AS TIMESTAMP), TRY_STRPTIME(CAST(CAST(ROUND((EXTRACT(EPOCH FROM (t)) - EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00')) * 10000000) AS BIGINT) AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS roundtrip FROM (VALUES (TIMESTAMP '1970-01-01 00:00:00'), (TIMESTAMP '0001-01-01 00:00:00'), (TIMESTAMP '9999-12-31 23:59:59.999999'), (TIMESTAMP '2020-02-29 00:00:00')) AS t(t)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, asl:Int, asr:Real, roundtrip:DateTime] rows=4
    - (1970-01-01T00:00:00.0000000Z, 621355968000000000, 6.21355968E+17, 1970-01-01T00:00:00.0000000Z)
    - (0001-01-01T00:00:00.0000000Z, 0, 0, 0001-01-01T00:00:00.0000000Z)
    - (9999-12-31T23:59:59.9999999Z, 3155378975999999999, 3.155378976E+18, 9999-12-31T23:59:59.9999999Z)
    - (2020-02-29T00:00:00.0000000Z, 637185312000000000, 6.37185312E+17, 2020-02-29T00:00:00.0000000Z)
- DuckDB: cols=[t:DateTime, asl:Int, asr:Real, roundtrip:DateTime] rows=4
    - (1970-01-01T00:00:00.0000000Z, 621355968000000000, 6.21355968E+17, null)
    - (0001-01-01T00:00:00.0000000Z, 0, 0, null)
    - (9999-12-31T23:59:59.9999990Z, 3155378976000000000, 3.155378976E+18, null)
    - (2020-02-29T00:00:00.0000000Z, 637185312000000000, 6.37185312E+17, null)

### `agent-type-casts-coercion-0021` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('9223372036854775807', 9223372036854775807, 9.223372036854776E+18, null) duck=('9223372036854775807', null, 9.223372036854776E+18, null)

**KQL**
```kql
datatable(s:string)[ "9223372036854775807", "9223372036854775808", "-9223372036854775809", "92233720368547758070", "1e19" ] | extend tl = tolong(s), td = todouble(s), ti = toint(s)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tl, COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE)) AS td, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ti FROM (VALUES ('9223372036854775807'), ('9223372036854775808'), ('-9223372036854775809'), ('92233720368547758070'), ('1e19')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tl:Int, td:Real, ti:Int] rows=5
    - ('9223372036854775807', 9223372036854775807, 9.223372036854776E+18, null)
    - ('9223372036854775808', null, 9.223372036854776E+18, null)
    - ('-9223372036854775809', null, -9.223372036854776E+18, null)
    - ('92233720368547758070', null, 9.223372036854776E+19, null)
    - ('1e19', null, 1E+19, null)
- DuckDB: cols=[s:String, tl:Int, td:Real, ti:Int] rows=5
    - ('9223372036854775807', null, 9.223372036854776E+18, null)
    - ('9223372036854775808', null, 9.223372036854776E+18, null)
    - ('-9223372036854775809', -9223372036854775808, -9.223372036854776E+18, null)
    - ('92233720368547758070', null, 9.223372036854776E+19, null)
    - ('1e19', null, 1E+19, null)

### `agent-type-casts-coercion-0022` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(null, 3, 100, True) duck=(42, 3, 100, True)

**KQL**
```kql
print chain1 = toint(tostring(todouble(tolong("42")))), chain2 = todouble(tostring(toint(3.99))), chain3 = tolong(tostring(todecimal("100"))), chain4 = tostring(toint(true)) == tostring(1)
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('42' AS DOUBLE), TRY_CAST(TRY_CAST('42' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('42' AS DOUBLE), TRY_CAST(TRY_CAST('42' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS BOOLEAN) AS DOUBLE)) AS TEXT), '') AS DOUBLE), TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('42' AS DOUBLE), TRY_CAST(TRY_CAST('42' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('42' AS DOUBLE), TRY_CAST(TRY_CAST('42' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS BOOLEAN) AS DOUBLE)) AS TEXT), '') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS chain1, COALESCE(TRY_CAST(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(3.99 AS DOUBLE), TRY_CAST(TRY_CAST(3.99 AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT), '') AS DOUBLE), TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(3.99 AS DOUBLE), TRY_CAST(TRY_CAST(3.99 AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT), '') AS BOOLEAN) AS DOUBLE)) AS chain2, TRY_CAST(TRUNC(COALESCE(TRY_CAST(COALESCE(TRY_CAST(TRY_CAST('100' AS DECIMAL(38, 18)) AS TEXT), '') AS DOUBLE), TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(TRY_CAST('100' AS DECIMAL(38, 18)) AS TEXT), '') AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS chain3, COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(TRUE AS DOUBLE), TRY_CAST(TRY_CAST(TRUE AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT), '') = COALESCE(TRY_CAST(1 AS TEXT), '') AS chain4
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[chain1:Int, chain2:Real, chain3:Int, chain4:Bool] rows=1
    - (null, 3, 100, True)
- DuckDB: cols=[chain1:Int, chain2:Real, chain3:Int, chain4:Bool] rows=1
    - (42, 3, 100, True)

### `agent-type-casts-coercion-0023` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('0.30000000000000007', '0.3333333333333333', '9223372036854775807', '-0.0', 'inf', 'NaN') duck=('0.3', '0.3333333333333333', '', '0.0', 'inf', 'nan')

**KQL**
```kql
print str_round = tostring(0.1 + 0.2), str_dbl = tostring(1.0 / 3.0), str_big = tostring(tolong(9223372036854775807)), str_neg0 = tostring(-0.0), str_inf = tostring(real(+inf)), str_nan = tostring(real(nan))
```
**Generated SQL**
```sql
SELECT COALESCE(TRY_CAST(0.1 + 0.2 AS TEXT), '') AS str_round, COALESCE(TRY_CAST(1.0 / 3.0 AS TEXT), '') AS str_dbl, COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(9223372036854775807 AS DOUBLE), TRY_CAST(TRY_CAST(9223372036854775807 AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS TEXT), '') AS str_big, COALESCE(TRY_CAST((-0.0) AS TEXT), '') AS str_neg0, COALESCE(TRY_CAST(CAST('inf' AS DOUBLE) AS TEXT), '') AS str_inf, COALESCE(TRY_CAST(CAST('nan' AS DOUBLE) AS TEXT), '') AS str_nan
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[str_round:String, str_dbl:String, str_big:String, str_neg0:String, str_inf:String, str_nan:String] rows=1
    - ('0.30000000000000007', '0.3333333333333333', '9223372036854775807', '-0.0', 'inf', 'NaN')
- DuckDB: cols=[str_round:String, str_dbl:String, str_big:String, str_neg0:String, str_inf:String, str_nan:String] rows=1
    - ('0.3', '0.3333333333333333', '', '0.0', 'inf', 'nan')

### `agent-type-casts-coercion-0025` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(True, True, True, null, True, null, null) duck=(True, True, True, null, null, null, True)

**KQL**
```kql
print bool_str1 = tobool("true"), bool_str2 = tobool("True"), bool_str3 = tobool("1"), bool_str4 = tobool("1.0"), bool_str5 = tobool("-1"), bool_str6 = tobool("0.0"), bool_str7 = tobool("yes")
```
**Generated SQL**
```sql
SELECT TRY_CAST('true' AS BOOLEAN) AS bool_str1, TRY_CAST('True' AS BOOLEAN) AS bool_str2, TRY_CAST('1' AS BOOLEAN) AS bool_str3, TRY_CAST('1.0' AS BOOLEAN) AS bool_str4, TRY_CAST('-1' AS BOOLEAN) AS bool_str5, TRY_CAST('0.0' AS BOOLEAN) AS bool_str6, TRY_CAST('yes' AS BOOLEAN) AS bool_str7
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[bool_str1:Bool, bool_str2:Bool, bool_str3:Bool, bool_str4:Bool, bool_str5:Bool, bool_str6:Bool, bool_str7:Bool] rows=1
    - (True, True, True, null, True, null, null)
- DuckDB: cols=[bool_str1:Bool, bool_str2:Bool, bool_str3:Bool, bool_str4:Bool, bool_str5:Bool, bool_str6:Bool, bool_str7:Bool] rows=1
    - (True, True, True, null, null, null, True)

### `agent-type-casts-coercion-0032` — MismatchRows (high)

*Detail:* first differing row[3]: kusto=('7e0', null, null, 7, False) duck=('7e0', 7, 7, 7, False)

**KQL**
```kql
datatable(s:string)[ "+7", "-7", " 7 ", "7e0", "007", "0", "-0", "+0", "  -0  ", "1.0" ] | extend ti = toint(s), tl = tolong(s), td = todouble(s), eqzero = toint(s) == 0
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ti, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tl, COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE)) AS td, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS INTEGER) = 0 AS eqzero FROM (VALUES ('+7'), ('-7'), (' 7 '), ('7e0'), ('007'), ('0'), ('-0'), ('+0'), ('  -0  '), ('1.0')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, ti:Int, tl:Int, td:Real, eqzero:Bool] rows=10
    - ('+7', 7, 7, 7, False)
    - ('-7', -7, -7, -7, False)
    - (' 7 ', 7, 7, 7, False)
    - ('7e0', null, null, 7, False)
    - ('007', 7, 7, 7, False)
    - ('0', 0, 0, 0, True)
    - ('-0', 0, 0, -0, True)
    - ('+0', 0, 0, 0, True)
- DuckDB: cols=[s:String, ti:Int, tl:Int, td:Real, eqzero:Bool] rows=10
    - ('+7', 7, 7, 7, False)
    - ('-7', -7, -7, -7, False)
    - (' 7 ', 7, 7, 7, False)
    - ('7e0', 7, 7, 7, False)
    - ('007', 7, 7, 7, False)
    - ('0', 0, 0, 0, True)
    - ('-0', 0, 0, -0, True)
    - ('+0', 0, 0, 0, True)

### `agent-type-casts-coercion-0033` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(9.223372036854776E+18, -9223372036854775808, False) duck=(9.223372036854776E+18, null, null)

**KQL**
```kql
print maxlong_real = todouble(9223372036854775807), back = tolong(todouble(9223372036854775807)), precision_loss = tolong(todouble(9223372036854775807)) == 9223372036854775807
```
**Generated SQL**
```sql
SELECT COALESCE(TRY_CAST(9223372036854775807 AS DOUBLE), TRY_CAST(TRY_CAST(9223372036854775807 AS BOOLEAN) AS DOUBLE)) AS maxlong_real, TRY_CAST(TRUNC(COALESCE(TRY_CAST(COALESCE(TRY_CAST(9223372036854775807 AS DOUBLE), TRY_CAST(TRY_CAST(9223372036854775807 AS BOOLEAN) AS DOUBLE)) AS DOUBLE), TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(9223372036854775807 AS DOUBLE), TRY_CAST(TRY_CAST(9223372036854775807 AS BOOLEAN) AS DOUBLE)) AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS back, TRY_CAST(TRUNC(COALESCE(TRY_CAST(COALESCE(TRY_CAST(9223372036854775807 AS DOUBLE), TRY_CAST(TRY_CAST(9223372036854775807 AS BOOLEAN) AS DOUBLE)) AS DOUBLE), TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(9223372036854775807 AS DOUBLE), TRY_CAST(TRY_CAST(9223372036854775807 AS BOOLEAN) AS DOUBLE)) AS BOOLEAN) AS DOUBLE))) AS BIGINT) = 9223372036854775807 AS precision_loss
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[maxlong_real:Real, back:Int, precision_loss:Bool] rows=1
    - (9.223372036854776E+18, -9223372036854775808, False)
- DuckDB: cols=[maxlong_real:Real, back:Int, precision_loss:Bool] rows=1
    - (9.223372036854776E+18, null, null)

### `agent-type-casts-coercion-0034` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(9.223372036854776E+18, 9223372036854775807, 2147483647, 9.223372036854776E+18) duck=(9.223372036854776E+18, null, null, null)

**KQL**
```kql
datatable(r:real)[ 9223372036854775807.0, 1e19, 1e30, -1e19, 1.5 ] | extend tolg = tolong(r), toin = toint(r), back = todouble(tolong(r))
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(r AS DOUBLE), TRY_CAST(TRY_CAST(r AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tolg, TRY_CAST(TRUNC(COALESCE(TRY_CAST(r AS DOUBLE), TRY_CAST(TRY_CAST(r AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS toin, COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(r AS DOUBLE), TRY_CAST(TRY_CAST(r AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(r AS DOUBLE), TRY_CAST(TRY_CAST(r AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS BOOLEAN) AS DOUBLE)) AS back FROM (VALUES (CAST(9.223372036854776E+18 AS DOUBLE)), (CAST(1E+19 AS DOUBLE)), (CAST(1E+30 AS DOUBLE)), (CAST(-1E+19 AS DOUBLE)), (CAST(1.5 AS DOUBLE))) AS t(r)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[r:Real, tolg:Int, toin:Int, back:Real] rows=5
    - (9.223372036854776E+18, 9223372036854775807, 2147483647, 9.223372036854776E+18)
    - (1E+19, 9223372036854775807, 2147483647, 9.223372036854776E+18)
    - (1E+30, 9223372036854775807, 2147483647, 9.223372036854776E+18)
    - (-1E+19, -9223372036854775808, -2147483648, -9.223372036854776E+18)
    - (1.5, 1, 1, 1)
- DuckDB: cols=[r:Real, tolg:Int, toin:Int, back:Real] rows=5
    - (9.223372036854776E+18, null, null, null)
    - (1E+19, null, null, null)
    - (1E+30, null, null, null)
    - (-1E+19, null, null, null)
    - (1.5, 1, 1, 1)

### `agent-type-casts-coercion-0039` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(null, null, null, False, True) duck=(null, null, null, null, True)

**KQL**
```kql
print empty_chain = toint(tostring(toint(""))), null_arith = toint("") + 1, null_mul = toint("abc") * 0, null_cmp = toint("xyz") == 0, null_isnull = isnull(toint("bad"))
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT), '') AS DOUBLE), TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT), '') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS empty_chain, TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1 AS null_arith, TRY_CAST(TRUNC(COALESCE(TRY_CAST('abc' AS DOUBLE), TRY_CAST(TRY_CAST('abc' AS BOOLEAN) AS DOUBLE))) AS INTEGER) * 0 AS null_mul, TRY_CAST(TRUNC(COALESCE(TRY_CAST('xyz' AS DOUBLE), TRY_CAST(TRY_CAST('xyz' AS BOOLEAN) AS DOUBLE))) AS INTEGER) = 0 AS null_cmp, (TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER) IS NULL) AS null_isnull
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[empty_chain:Int, null_arith:Int, null_mul:Int, null_cmp:Bool, null_isnull:Bool] rows=1
    - (null, null, null, False, True)
- DuckDB: cols=[empty_chain:Int, null_arith:Int, null_mul:Int, null_cmp:Bool, null_isnull:Bool] rows=1
    - (null, null, null, null, True)

### `agent-type-casts-coercion-0042` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('int', 'null', 'real', 'int') duck=('long', '"null"', 'real', 'long')

**KQL**
```kql
print gettype_null = gettype(toint("bad")), gettype_dynnull = gettype(dynamic(null)), gettype_realnan = gettype(real(nan)), gettype_emptystr = gettype(toint(""))
```
**Generated SQL**
```sql
SELECT CASE WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE '%[]' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'JSON' THEN (CASE json_type(CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER))) END AS gettype_null, CASE WHEN TYPEOF(NULL) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(NULL) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(NULL) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(NULL) = 'VARCHAR' THEN 'string' WHEN TYPEOF(NULL) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(NULL) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(NULL) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(NULL) = 'UUID' THEN 'guid' WHEN TYPEOF(NULL) LIKE '%[]' OR TYPEOF(NULL) LIKE 'STRUCT%' OR TYPEOF(NULL) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(NULL) = 'JSON' THEN (CASE json_type(CAST(NULL AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(NULL)) END AS gettype_dynnull, CASE WHEN TYPEOF(CAST('nan' AS DOUBLE)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(CAST('nan' AS DOUBLE)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(CAST('nan' AS DOUBLE)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(CAST('nan' AS DOUBLE)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(CAST('nan' AS DOUBLE)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(CAST('nan' AS DOUBLE)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(CAST('nan' AS DOUBLE)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(CAST('nan' AS DOUBLE)) = 'UUID' THEN 'guid' WHEN TYPEOF(CAST('nan' AS DOUBLE)) LIKE '%[]' OR TYPEOF(CAST('nan' AS DOUBLE)) LIKE 'STRUCT%' OR TYPEOF(CAST('nan' AS DOUBLE)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(CAST('nan' AS DOUBLE)) = 'JSON' THEN (CASE json_type(CAST(CAST('nan' AS DOUBLE) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(CAST('nan' AS DOUBLE))) END AS gettype_realnan, CASE WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE '%[]' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'JSON' THEN (CASE json_type(CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER))) END AS gettype_emptystr
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[gettype_null:String, gettype_dynnull:String, gettype_realnan:String, gettype_emptystr:String] rows=1
    - ('int', 'null', 'real', 'int')
- DuckDB: cols=[gettype_null:String, gettype_dynnull:String, gettype_realnan:String, gettype_emptystr:String] rows=1
    - ('long', '"null"', 'real', 'long')

### `agent-type-casts-coercion-0043` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(9223372036854775807, 9223372036854775807, 9.223372036854776E+18, -1, 'long') duck=('9223372036854775807', null, 9.223372036854776E+18, null, 'long')

**KQL**
```kql
datatable(d:dynamic)[ dynamic(9223372036854775807), dynamic(-9223372036854775808), dynamic(1.7976931348623157e308), dynamic(5e-324) ] | extend tl = tolong(d), td = todouble(d), ti = toint(d), gt = gettype(d)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tl, COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE)) AS td, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ti, CASE WHEN TYPEOF(CAST(d AS JSON)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(CAST(d AS JSON)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(CAST(d AS JSON)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(CAST(d AS JSON)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(CAST(d AS JSON)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(CAST(d AS JSON)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(CAST(d AS JSON)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(CAST(d AS JSON)) = 'UUID' THEN 'guid' WHEN TYPEOF(CAST(d AS JSON)) LIKE '%[]' OR TYPEOF(CAST(d AS JSON)) LIKE 'STRUCT%' OR TYPEOF(CAST(d AS JSON)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(CAST(d AS JSON)) = 'JSON' THEN (CASE json_type(CAST(CAST(d AS JSON) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(CAST(d AS JSON))) END AS gt FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST('9223372036854775807'::JSON AS JSON)), (CAST('-0'::JSON AS JSON)), (CAST('1.7976931348623157E+308'::JSON AS JSON)), (CAST('5E-324'::JSON AS JSON))) AS t(d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, tl:Int, td:Real, ti:Int, gt:String] rows=4
    - (9223372036854775807, 9223372036854775807, 9.223372036854776E+18, -1, 'long')
    - (-9223372036854775808, -9223372036854775808, -9.223372036854776E+18, 0, 'long')
    - (1.7976931348623157E+308, 9223372036854775807, 1.7976931348623157E+308, 2147483647, 'double')
    - (5E-324, 0, 5E-324, 0, 'double')
- DuckDB: cols=[d:String, tl:Int, td:Real, ti:Int, gt:String] rows=4
    - ('9223372036854775807', null, 9.223372036854776E+18, null, 'long')
    - ('-0', 0, 0, 0, 'long')
    - ('1.7976931348623157E+308', null, 1.7976931348623157E+308, null, 'real')
    - ('5E-324', 0, 5E-324, 0, 'real')

### `agent-type-casts-coercion-0046` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(0.3333333333333333, 0, False, True) duck=(0.3333333333333333, 0, True, True)

**KQL**
```kql
print decimal_div_int = todecimal("1") / 3, decimal_vs_real = todecimal("0.1") + todecimal("0.2") - todecimal("0.3"), real_vs_dec = (0.1 + 0.2 == 0.3), dec_eq = (todecimal("0.1") + todecimal("0.2") == todecimal("0.3"))
```
**Generated SQL**
```sql
SELECT TRY_CAST('1' AS DECIMAL(38, 18)) / 3 AS decimal_div_int, TRY_CAST('0.1' AS DECIMAL(38, 18)) + TRY_CAST('0.2' AS DECIMAL(38, 18)) - TRY_CAST('0.3' AS DECIMAL(38, 18)) AS decimal_vs_real, (0.1 + 0.2 = 0.3) AS real_vs_dec, (TRY_CAST('0.1' AS DECIMAL(38, 18)) + TRY_CAST('0.2' AS DECIMAL(38, 18)) = TRY_CAST('0.3' AS DECIMAL(38, 18))) AS dec_eq
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[decimal_div_int:Real, decimal_vs_real:Real, real_vs_dec:Bool, dec_eq:Bool] rows=1
    - (0.3333333333333333, 0, False, True)
- DuckDB: cols=[decimal_div_int:Real, decimal_vs_real:Real, real_vs_dec:Bool, dec_eq:Bool] rows=1
    - (0.3333333333333333, 0, True, True)

### `agent-type-casts-coercion-0004` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=('5.7', null, null, 5.7, True) duck=('5.7', 5, 5, 5.7, False)

**KQL**
```kql
datatable(s:string)[ "5", "5.7", " 5 ", "0x1F", "1e3", "+5", "-5", "abc", "", "  ", "5abc", "Infinity", "NaN" ] | extend i=toint(s), l=tolong(s), d=todouble(s), isn=isnull(toint(s))
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS i, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS l, COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE)) AS d, (TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS INTEGER) IS NULL) AS isn FROM (VALUES ('5'), ('5.7'), (' 5 '), ('0x1F'), ('1e3'), ('+5'), ('-5'), ('abc'), (''), ('  '), ('5abc'), ('Infinity'), ('NaN')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, i:Int, l:Int, d:Real, isn:Bool] rows=13
    - ('5', 5, 5, 5, False)
    - ('5.7', null, null, 5.7, True)
    - (' 5 ', 5, 5, 5, False)
    - ('0x1F', 31, 31, null, False)
    - ('1e3', null, null, 1000, True)
    - ('+5', 5, 5, 5, False)
    - ('-5', -5, -5, -5, False)
    - ('abc', null, null, null, True)
- DuckDB: cols=[s:String, i:Int, l:Int, d:Real, isn:Bool] rows=13
    - ('5', 5, 5, 5, False)
    - ('5.7', 5, 5, 5.7, False)
    - (' 5 ', 5, 5, 5, False)
    - ('0x1F', null, null, null, True)
    - ('1e3', 1000, 1000, 1000, False)
    - ('+5', 5, 5, 5, False)
    - ('-5', -5, -5, -5, False)
    - ('abc', null, null, null, True)

### `agent-type-casts-coercion-0005` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=('5.9', null, null, 5.9) duck=('5.9', 5, 5, 5.9)

**KQL**
```kql
datatable(s:string)[ "5", "5.9", "-5.9", "2147483648", "9223372036854775808" ] | extend toi=toint(s), tol=tolong(s), tod=todouble(s)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS toi, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tol, COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE)) AS tod FROM (VALUES ('5'), ('5.9'), ('-5.9'), ('2147483648'), ('9223372036854775808')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, toi:Int, tol:Int, tod:Real] rows=5
    - ('5', 5, 5, 5)
    - ('5.9', null, null, 5.9)
    - ('-5.9', null, null, -5.9)
    - ('2147483648', null, 2147483648, 2147483648)
    - ('9223372036854775808', null, null, 9.223372036854776E+18)
- DuckDB: cols=[s:String, toi:Int, tol:Int, tod:Real] rows=5
    - ('5', 5, 5, 5)
    - ('5.9', 5, 5, 5.9)
    - ('-5.9', -5, -5, -5.9)
    - ('2147483648', null, 2147483648, 2147483648)
    - ('9223372036854775808', null, null, 9.223372036854776E+18)

### `agent-type-casts-coercion-0012` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(3.141592653589793, 'decimal', 0.3333333333333333) duck=(3.141592653589793238, 'real', 0.3333333333333333)

**KQL**
```kql
print x = todecimal("3.14159265358979323846"), y = gettype(todecimal("1.5")), z = todecimal(1)/todecimal(3)
```
**Generated SQL**
```sql
SELECT TRY_CAST('3.14159265358979323846' AS DECIMAL(38, 18)) AS x, CASE WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) LIKE '%[]' OR TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18))) = 'JSON' THEN (CASE json_type(CAST(TRY_CAST('1.5' AS DECIMAL(38, 18)) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST('1.5' AS DECIMAL(38, 18)))) END AS y, TRY_CAST(1 AS DECIMAL(38, 18)) / TRY_CAST(3 AS DECIMAL(38, 18)) AS z
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Real, y:String, z:Real] rows=1
    - (3.141592653589793, 'decimal', 0.3333333333333333)
- DuckDB: cols=[x:Real, y:String, z:Real] rows=1
    - (3.141592653589793238, 'real', 0.3333333333333333)

### `agent-type-casts-coercion-0014` — MismatchRows (high)

*Detail:* first differing row[3]: kusto=('1.5', null, True) duck=('1.5', 00:00:01.5000000, False)

**KQL**
```kql
datatable(s:string)[ "1d", "2h", "01:00:00", "1.5", "90s", "-1d", "00:00:00.001" ] | extend ts=totimespan(s), isn=isnull(totimespan(s))
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(s AS INTERVAL) AS ts, (TRY_CAST(s AS INTERVAL) IS NULL) AS isn FROM (VALUES ('1d'), ('2h'), ('01:00:00'), ('1.5'), ('90s'), ('-1d'), ('00:00:00.001')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, ts:TimeSpan, isn:Bool] rows=7
    - ('1d', 1.00:00:00, False)
    - ('2h', 02:00:00, False)
    - ('01:00:00', 01:00:00, False)
    - ('1.5', null, True)
    - ('90s', 00:01:30, False)
    - ('-1d', -1.00:00:00, False)
    - ('00:00:00.001', 00:00:00.0010000, False)
- DuckDB: cols=[s:String, ts:TimeSpan, isn:Bool] rows=7
    - ('1d', 1.00:00:00, False)
    - ('2h', 02:00:00, False)
    - ('01:00:00', 01:00:00, False)
    - ('1.5', 00:00:01.5000000, False)
    - ('90s', 00:01:30, False)
    - ('-1d', -1.00:00:00, False)
    - ('00:00:00.001', 00:00:00.0010000, False)

### `agent-type-casts-coercion-0017` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(True, True, True, False, null, null, True) duck=(True, True, True, False, True, null, null)

**KQL**
```kql
print a = tobool("true"), b = tobool("True"), c = tobool("1"), d = tobool("0"), e = tobool("yes"), f = tobool(""), g = tobool("-1")
```
**Generated SQL**
```sql
SELECT TRY_CAST('true' AS BOOLEAN) AS a, TRY_CAST('True' AS BOOLEAN) AS b, TRY_CAST('1' AS BOOLEAN) AS c, TRY_CAST('0' AS BOOLEAN) AS d, TRY_CAST('yes' AS BOOLEAN) AS e, TRY_CAST('' AS BOOLEAN) AS f, TRY_CAST('-1' AS BOOLEAN) AS g
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Bool, b:Bool, c:Bool, d:Bool, e:Bool, f:Bool, g:Bool] rows=1
    - (True, True, True, False, null, null, True)
- DuckDB: cols=[a:Bool, b:Bool, c:Bool, d:Bool, e:Bool, f:Bool, g:Bool] rows=1
    - (True, True, True, False, True, null, null)

### `agent-type-casts-coercion-0021` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('1.0', '1.5', '0.30000000000000007', '100000000000.0') duck=('1.0', '1.5', '0.3', '100000000000.0')

**KQL**
```kql
print a = tostring(1.0), b = tostring(1.50000), c = tostring(0.1+0.2), d = tostring(100000000000.0)
```
**Generated SQL**
```sql
SELECT COALESCE(TRY_CAST(1.0 AS TEXT), '') AS a, COALESCE(TRY_CAST(1.5 AS TEXT), '') AS b, COALESCE(TRY_CAST(0.1 + 0.2 AS TEXT), '') AS c, COALESCE(TRY_CAST(100000000000.0 AS TEXT), '') AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String, b:String, c:String, d:String] rows=1
    - ('1.0', '1.5', '0.30000000000000007', '100000000000.0')
- DuckDB: cols=[a:String, b:String, c:String, d:String] rows=1
    - ('1.0', '1.5', '0.3', '100000000000.0')

### `agent-type-casts-coercion-0023` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('int', 'long', 'real', 'long', 'real', 'long') duck=('long', 'long', 'real', 'long', 'real', 'long')

**KQL**
```kql
print x = gettype(toint(1)), y = gettype(tolong(1)), z = gettype(todouble(1)), w = gettype(1+1), v = gettype(1+1.0), u = gettype(1/2)
```
**Generated SQL**
```sql
SELECT CASE WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE '%[]' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'JSON' THEN (CASE json_type(CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER))) END AS x, CASE WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) LIKE '%[]' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) = 'JSON' THEN (CASE json_type(CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT))) END AS y, CASE WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) = 'VARCHAR' THEN 'string' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) = 'UUID' THEN 'guid' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) LIKE '%[]' OR TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) LIKE 'STRUCT%' OR TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) = 'JSON' THEN (CASE json_type(CAST(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE)) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE)))) END AS z, CASE WHEN TYPEOF(1 + 1) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(1 + 1) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(1 + 1) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(1 + 1) = 'VARCHAR' THEN 'string' WHEN TYPEOF(1 + 1) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(1 + 1) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(1 + 1) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(1 + 1) = 'UUID' THEN 'guid' WHEN TYPEOF(1 + 1) LIKE '%[]' OR TYPEOF(1 + 1) LIKE 'STRUCT%' OR TYPEOF(1 + 1) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(1 + 1) = 'JSON' THEN (CASE json_type(CAST(1 + 1 AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(1 + 1)) END AS w, CASE WHEN TYPEOF(1 + 1.0) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(1 + 1.0) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(1 + 1.0) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(1 + 1.0) = 'VARCHAR' THEN 'string' WHEN TYPEOF(1 + 1.0) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(1 + 1.0) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(1 + 1.0) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(1 + 1.0) = 'UUID' THEN 'guid' WHEN TYPEOF(1 + 1.0) LIKE '%[]' OR TYPEOF(1 + 1.0) LIKE 'STRUCT%' OR TYPEOF(1 + 1.0) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(1 + 1.0) = 'JSON' THEN (CASE json_type(CAST(1 + 1.0 AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(1 + 1.0)) END AS v, CASE WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) = 'UUID' THEN 'guid' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) LIKE '%[]' OR TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) LIKE 'STRUCT%' OR TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) = 'JSON' THEN (CASE json_type(CAST(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT))) END AS u
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:String, y:String, z:String, w:String, v:String, u:String] rows=1
    - ('int', 'long', 'real', 'long', 'real', 'long')
- DuckDB: cols=[x:String, y:String, z:String, w:String, v:String, u:String] rows=1
    - ('long', 'long', 'real', 'long', 'real', 'long')

### `agent-type-casts-coercion-0024` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('null', 'long', 'string', 'array') duck=('"null"', 'long', 'string', 'array')

**KQL**
```kql
print x = gettype(dynamic(null)), y = gettype(dynamic(1)), z = gettype(dynamic("s")), w = gettype(dynamic([1,2]))
```
**Generated SQL**
```sql
SELECT CASE WHEN TYPEOF(NULL) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(NULL) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(NULL) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(NULL) = 'VARCHAR' THEN 'string' WHEN TYPEOF(NULL) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(NULL) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(NULL) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(NULL) = 'UUID' THEN 'guid' WHEN TYPEOF(NULL) LIKE '%[]' OR TYPEOF(NULL) LIKE 'STRUCT%' OR TYPEOF(NULL) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(NULL) = 'JSON' THEN (CASE json_type(CAST(NULL AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(NULL)) END AS x, CASE WHEN TYPEOF('1'::JSON) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF('1'::JSON) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF('1'::JSON) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF('1'::JSON) = 'VARCHAR' THEN 'string' WHEN TYPEOF('1'::JSON) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF('1'::JSON) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF('1'::JSON) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF('1'::JSON) = 'UUID' THEN 'guid' WHEN TYPEOF('1'::JSON) LIKE '%[]' OR TYPEOF('1'::JSON) LIKE 'STRUCT%' OR TYPEOF('1'::JSON) LIKE 'MAP%' THEN 'array' WHEN TYPEOF('1'::JSON) = 'JSON' THEN (CASE json_type(CAST('1'::JSON AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF('1'::JSON)) END AS y, CASE WHEN TYPEOF('"s"'::JSON) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF('"s"'::JSON) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF('"s"'::JSON) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF('"s"'::JSON) = 'VARCHAR' THEN 'string' WHEN TYPEOF('"s"'::JSON) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF('"s"'::JSON) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF('"s"'::JSON) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF('"s"'::JSON) = 'UUID' THEN 'guid' WHEN TYPEOF('"s"'::JSON) LIKE '%[]' OR TYPEOF('"s"'::JSON) LIKE 'STRUCT%' OR TYPEOF('"s"'::JSON) LIKE 'MAP%' THEN 'array' WHEN TYPEOF('"s"'::JSON) = 'JSON' THEN (CASE json_type(CAST('"s"'::JSON AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF('"s"'::JSON)) END AS z, CASE WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(LIST_VALUE(1, 2)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'UUID' THEN 'guid' WHEN TYPEOF(LIST_VALUE(1, 2)) LIKE '%[]' OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'STRUCT%' OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'JSON' THEN (CASE json_type(CAST(LIST_VALUE(1, 2) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(LIST_VALUE(1, 2))) END AS w
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:String, y:String, z:String, w:String] rows=1
    - ('null', 'long', 'string', 'array')
- DuckDB: cols=[x:String, y:String, z:String, w:String] rows=1
    - ('"null"', 'long', 'string', 'array')

### `agent-type-casts-coercion-0025` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[1]: kusto=(1.5, 'double', 1, 1.5, '1.5') duck=('1.5', 'real', 1, 1.5, '1.5')

**KQL**
```kql
datatable(d:dynamic)[ dynamic(1), dynamic(1.5), dynamic("5"), dynamic(true), dynamic(null) ] | extend gt=gettype(d), toi=toint(d), tod=todouble(d), tos=tostring(d)
```
**Generated SQL**
```sql
SELECT *, CASE WHEN TYPEOF(CAST(d AS JSON)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(CAST(d AS JSON)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(CAST(d AS JSON)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(CAST(d AS JSON)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(CAST(d AS JSON)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(CAST(d AS JSON)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(CAST(d AS JSON)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(CAST(d AS JSON)) = 'UUID' THEN 'guid' WHEN TYPEOF(CAST(d AS JSON)) LIKE '%[]' OR TYPEOF(CAST(d AS JSON)) LIKE 'STRUCT%' OR TYPEOF(CAST(d AS JSON)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(CAST(d AS JSON)) = 'JSON' THEN (CASE json_type(CAST(CAST(d AS JSON) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(CAST(d AS JSON))) END AS gt, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS toi, COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE)) AS tod, COALESCE(json_extract_string(d, '$'), '') AS tos FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST('1'::JSON AS JSON)), (CAST('1.5'::JSON AS JSON)), (CAST('"5"'::JSON AS JSON)), (CAST('true'::JSON AS JSON)), (CAST(NULL AS JSON))) AS t(d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, gt:String, toi:Int, tod:Real, tos:String] rows=5
    - (1, 'long', 1, 1, '1')
    - (1.5, 'double', 1, 1.5, '1.5')
    - ("5", 'string', 5, 5, '5')
    - (true, 'bool', 1, 1, 'true')
    - (null, 'null', null, null, '')
- DuckDB: cols=[d:String, gt:String, toi:Int, tod:Real, tos:String] rows=5
    - ('1', 'long', 1, 1, '1')
    - ('1.5', 'real', 1, 1.5, '1.5')
    - ('"5"', 'string', 5, 5, '5')
    - ('true', 'bool', 1, 1, 'true')
    - (null, 'dictionary', null, null, '')

### `agent-type-casts-coercion-0039` — MismatchRows (high)

*Detail:* first differing row[2]: kusto=('1_000', null, True) duck=('1_000', 1000, False)

**KQL**
```kql
datatable(s:string)[ "1,234", "1.234,56", "1_000", "1 000", "1e10", "0.5e-3", ".5", "5." ] | extend tod=todouble(s), isn=isnull(todouble(s))
```
**Generated SQL**
```sql
SELECT *, COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE)) AS tod, (COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE)) IS NULL) AS isn FROM (VALUES ('1,234'), ('1.234,56'), ('1_000'), ('1 000'), ('1e10'), ('0.5e-3'), ('.5'), ('5.')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tod:Real, isn:Bool] rows=8
    - ('1,234', null, True)
    - ('1.234,56', null, True)
    - ('1_000', null, True)
    - ('1 000', null, True)
    - ('1e10', 10000000000, False)
    - ('0.5e-3', 0.0005, False)
    - ('.5', 0.5, False)
    - ('5.', 5, False)
- DuckDB: cols=[s:String, tod:Real, isn:Bool] rows=8
    - ('1,234', null, True)
    - ('1.234,56', null, True)
    - ('1_000', 1000, False)
    - ('1 000', null, True)
    - ('1e10', 10000000000, False)
    - ('0.5e-3', 0.0005, False)
    - ('.5', 0.5, False)
    - ('5.', 5, False)

### `agent-type-casts-coercion-0042` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[x:Int|Real], TYPE_MISMATCH[z:Int|Real]  
*Detail:* first differing row[0]: kusto=(4, 4, -6, -6, 'long') duck=(4, 4, -6, -6, 'real')

**KQL**
```kql
print x = bin(5, 2), y = bin(5.5, 2), z = bin(-5, 2), w = bin(-5.5, 2.0), gt = gettype(bin(5,2))
```
**Generated SQL**
```sql
SELECT FLOOR((5)/(2))*(2) AS x, FLOOR((5.5)/(2))*(2) AS y, FLOOR(((-5))/(2))*(2) AS z, FLOOR(((-5.5))/(2.0))*(2.0) AS w, CASE WHEN TYPEOF(FLOOR((5)/(2))*(2)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(FLOOR((5)/(2))*(2)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(FLOOR((5)/(2))*(2)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(FLOOR((5)/(2))*(2)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(FLOOR((5)/(2))*(2)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(FLOOR((5)/(2))*(2)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(FLOOR((5)/(2))*(2)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(FLOOR((5)/(2))*(2)) = 'UUID' THEN 'guid' WHEN TYPEOF(FLOOR((5)/(2))*(2)) LIKE '%[]' OR TYPEOF(FLOOR((5)/(2))*(2)) LIKE 'STRUCT%' OR TYPEOF(FLOOR((5)/(2))*(2)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(FLOOR((5)/(2))*(2)) = 'JSON' THEN (CASE json_type(CAST(FLOOR((5)/(2))*(2) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(FLOOR((5)/(2))*(2))) END AS gt
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:Real, z:Int, w:Real, gt:String] rows=1
    - (4, 4, -6, -6, 'long')
- DuckDB: cols=[x:Real, y:Real, z:Real, w:Real, gt:String] rows=1
    - (4, 4, -6, -6, 'real')

### `agent-type-casts-coercion-0043` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=("123", 123, 123, True) duck=('"123"', 123, 123, null)

**KQL**
```kql
datatable(d:dynamic)[ dynamic("123"), dynamic("1.5"), dynamic("true"), dynamic("[1,2]"), dynamic("null") ] | extend toi=toint(d), tod=todouble(d), tob=tobool(d)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS toi, COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE)) AS tod, TRY_CAST(d AS BOOLEAN) AS tob FROM (SELECT CAST(d AS JSON) AS d FROM (VALUES (CAST('"123"'::JSON AS JSON)), (CAST('"1.5"'::JSON AS JSON)), (CAST('"true"'::JSON AS JSON)), (CAST('"[1,2]"'::JSON AS JSON)), (CAST('"null"'::JSON AS JSON))) AS t(d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, toi:Int, tod:Real, tob:Bool] rows=5
    - ("123", 123, 123, True)
    - ("1.5", null, 1.5, null)
    - ("true", null, null, True)
    - ("[1,2]", null, null, null)
    - ("null", null, null, null)
- DuckDB: cols=[d:String, toi:Int, tod:Real, tob:Bool] rows=5
    - ('"123"', 123, 123, null)
    - ('"1.5"', 1, 1.5, null)
    - ('"true"', 1, 1, True)
    - ('"[1,2]"', null, null, null)
    - ('"null"', null, null, null)

### `agent-type-casts-coercion-0047` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(0001-01-01T00:00:00.0000000Z, 0001-01-01T00:00:00.0000001Z, 'datetime', 0001-01-01T00:00:00.0000001Z) duck=(null, null, 'datetime', null)

**KQL**
```kql
print x = todatetime(0), y = todatetime(1), z = gettype(todatetime(1)), w = todatetime(1.5)
```
**Generated SQL**
```sql
SELECT COALESCE(TRY_CAST(0 AS TIMESTAMP), TRY_STRPTIME(CAST(0 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS x, COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS y, CASE WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) = 'VARCHAR' THEN 'string' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) = 'UUID' THEN 'guid' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) LIKE '%[]' OR TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) LIKE 'STRUCT%' OR TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) = 'JSON' THEN (CASE json_type(CAST(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS VARCHAR)) WHEN 'ARRAY' THEN 'array' WHEN 'OBJECT' THEN 'dictionary' WHEN 'VARCHAR' THEN 'string' WHEN 'BIGINT' THEN 'long' WHEN 'UBIGINT' THEN 'long' WHEN 'DOUBLE' THEN 'real' WHEN 'BOOLEAN' THEN 'bool' WHEN 'NULL' THEN 'null' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])))) END AS z, COALESCE(TRY_CAST(1.5 AS TIMESTAMP), TRY_STRPTIME(CAST(1.5 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS w
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:DateTime, y:DateTime, z:String, w:DateTime] rows=1
    - (0001-01-01T00:00:00.0000000Z, 0001-01-01T00:00:00.0000001Z, 'datetime', 0001-01-01T00:00:00.0000001Z)
- DuckDB: cols=[x:DateTime, y:DateTime, z:String, w:DateTime] rows=1
    - (null, null, 'datetime', null)

### `agent-type-casts-coercion-0049` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(False, 0.30000000000000004, True) duck=(True, 0.3, True)

**KQL**
```kql
print a = 0.1 + 0.2 == 0.3, b = 0.1 + 0.2, c = todecimal(0.1) + todecimal(0.2) == todecimal(0.3)
```
**Generated SQL**
```sql
SELECT 0.1 + 0.2 = 0.3 AS a, 0.1 + 0.2 AS b, TRY_CAST(0.1 AS DECIMAL(38, 18)) + TRY_CAST(0.2 AS DECIMAL(38, 18)) = TRY_CAST(0.3 AS DECIMAL(38, 18)) AS c
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Bool, b:Real, c:Bool] rows=1
    - (False, 0.30000000000000004, True)
- DuckDB: cols=[a:Bool, b:Real, c:Bool] rows=1
    - (True, 0.3, True)

## Family: window-series-scan (32)

### `agent-window-series-scan-0002` — MismatchRows (high)

*Detail:* first differing row[2]: kusto=(2, 30, 1) duck=(2, 30, 3)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30 ] | sort by k asc | serialize rn = row_number(1, k != prev(k))
```
**Generated SQL**
```sql
SELECT *, (ROW_NUMBER() OVER (ORDER BY k ASC NULLS FIRST) + (1 - 1)) AS rn FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, v:Int, rn:Int] rows=4
    - (1, 10, 1)
    - (1, 20, 2)
    - (2, 30, 1)
    - (2, 30, 2)
- DuckDB: cols=[k:Int, v:Int, rn:Int] rows=4
    - (1, 10, 1)
    - (1, 20, 2)
    - (2, 30, 3)
    - (2, 30, 4)

### `agent-window-series-scan-0013` — MismatchRows (high)

*Detail:* first differing row[3]: kusto=('b', 5, 1) duck=('b', 5, 3)

**KQL**
```kql
datatable(g:string, s:long)[ "a",10, "a",10, "a",20, "b",5, "b",5 ] | sort by g asc, s asc | serialize rd = row_rank_dense(s, g != prev(g))
```
**Generated SQL**
```sql
SELECT *, DENSE_RANK() OVER (ORDER BY g ASC NULLS FIRST, s ASC NULLS FIRST) AS rd FROM (VALUES ('a', CAST(10 AS BIGINT)), ('a', CAST(10 AS BIGINT)), ('a', CAST(20 AS BIGINT)), ('b', CAST(5 AS BIGINT)), ('b', CAST(5 AS BIGINT))) AS t(g, s) ORDER BY g ASC NULLS FIRST, s ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, s:Int, rd:Int] rows=5
    - ('a', 10, 1)
    - ('a', 10, 1)
    - ('a', 20, 2)
    - ('b', 5, 1)
    - ('b', 5, 1)
- DuckDB: cols=[g:String, s:Int, rd:Int] rows=5
    - ('a', 10, 1)
    - ('a', 10, 1)
    - ('a', 20, 2)
    - ('b', 5, 3)
    - ('b', 5, 3)

### `agent-window-series-scan-0028` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[t:Dynamic|Int]  
*Detail:* first differing row[0]: kusto=(10, [
  "2020-01-01T00:00:00.0000000Z",
  "2020-01-02T00:00:00.0000000Z",
  "2020-01-03T00:00:00.0000000Z"
]) duck=(["2020-01-01T00:00:00.0000000Z","2020-01-02T00:00:00.0000000Z","2020-01-03T00:00:00.0000000Z"], 10)

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2020-01-01),10, datetime(2020-01-02),20, datetime(2020-01-03),30 ] | make-series s = sum(v) default = 0 on t from datetime(2020-01-01) to datetime(2020-01-04) step 1d | mv-expand s to typeof(long)
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (s), CAST(u.value AS BIGINT) AS s FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-04 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2020-01-02 00:00:00', CAST(20 AS BIGINT)), (TIMESTAMP '2020-01-03 00:00:00', CAST(30 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket) AS t CROSS JOIN UNNEST(t.s) AS u(value)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, t:Dynamic] rows=3
    - (10, [
  "2020-01-01T00:00:00.0000000Z",
  "2020-01-02T00:00:00.0000000Z",
  "2020-01-03T00:00:00.0000000Z"
])
    - (20, [
  "2020-01-01T00:00:00.0000000Z",
  "2020-01-02T00:00:00.0000000Z",
  "2020-01-03T00:00:00.0000000Z"
])
    - (30, [
  "2020-01-01T00:00:00.0000000Z",
  "2020-01-02T00:00:00.0000000Z",
  "2020-01-03T00:00:00.0000000Z"
])
- DuckDB: cols=[t:Unknown, s:Int] rows=3
    - (["2020-01-01T00:00:00.0000000Z","2020-01-02T00:00:00.0000000Z","2020-01-03T00:00:00.0000000Z"], 10)
    - (["2020-01-01T00:00:00.0000000Z","2020-01-02T00:00:00.0000000Z","2020-01-03T00:00:00.0000000Z"], 20)
    - (["2020-01-01T00:00:00.0000000Z","2020-01-02T00:00:00.0000000Z","2020-01-03T00:00:00.0000000Z"], 30)

### `agent-window-series-scan-0029` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[e:Dynamic|String]  
*Detail:* first differing row[0]: kusto=([
  10,
  20
], [
  "2020-01-01T00:00:00.0000000Z",
  "2020-01-02T00:00:00.0000000Z"
], {
  "min": 10.0,
  "min_idx": 0,
  "max": 20.0,
  "max_idx": 1,
  "avg": 15.0,
  "stdev": 7.0710678118654755,
  "variance": 50.0,
  "sum": 30.0,
  "len": 2
}) duck=([10,20], ["2020-01-01T00:00:00.0000000Z","2020-01-02T00:00:00.0000000Z"], '{"min":10.0,"min_idx":0,"max":20.0,"max_idx":1,"avg":15.0,"sum":30.0,"stdev":7.0710678118654755,"variance":50.0}')

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2020-01-01),10, datetime(2020-01-02),20 ] | make-series s = sum(v) default = 0 on t from datetime(2020-01-01) to datetime(2020-01-03) step 1d | extend e = series_stats_dynamic(s)
```
**Generated SQL**
```sql
SELECT *, json_object('min', LIST_MIN(LIST_FILTER(s, x -> x IS NOT NULL)), 'min_idx', LIST_POSITION(s, LIST_MIN(LIST_FILTER(s, x -> x IS NOT NULL))) - 1, 'max', LIST_MAX(LIST_FILTER(s, x -> x IS NOT NULL)), 'max_idx', LIST_POSITION(s, LIST_MAX(LIST_FILTER(s, x -> x IS NOT NULL))) - 1, 'avg', LIST_AVG(LIST_FILTER(s, x -> x IS NOT NULL)), 'sum', LIST_SUM(LIST_FILTER(s, x -> x IS NOT NULL)), 'stdev', LIST_AGGREGATE(LIST_FILTER(s, x -> x IS NOT NULL), 'stddev_samp'), 'variance', LIST_AGGREGATE(LIST_FILTER(s, x -> x IS NOT NULL), 'var_samp')) AS e FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-03 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2020-01-02 00:00:00', CAST(20 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic, e:Dynamic] rows=1
    - ([
  10,
  20
], [
  "2020-01-01T00:00:00.0000000Z",
  "2020-01-02T00:00:00.0000000Z"
], {
  "min": 10.0,
  "min_idx": 0,
  "max": 20.0,
  "max_idx": 1,
  "avg": 15.0,
  "stdev": 7.0710678118654755,
  "variance": 50.0,
  "sum": 30.0,
  "len": 2
})
- DuckDB: cols=[s:Unknown, t:Unknown, e:String] rows=1
    - ([10,20], ["2020-01-01T00:00:00.0000000Z","2020-01-02T00:00:00.0000000Z"], '{"min":10.0,"min_idx":0,"max":20.0,"max_idx":1,"avg":15.0,"sum":30.0,"stdev":7.0710678118654755,"variance":50.0}')

### `agent-window-series-scan-0032` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[Sub:Int|String]  
*Detail:* first differing row[0]: kusto=('b', 30, 'x', 30) duck=('b', 30, 'x', 'x')

**KQL**
```kql
datatable(cat:string, sub:string, v:long)[ "a","x",10, "a","y",20, "b","x",30 ] | top-nested of cat by Tot=sum(v), top-nested of sub by Sub=sum(v)
```
**Generated SQL**
```sql
SELECT cat, Tot, sub, Sub FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY Sub DESC) AS _rn1 FROM (SELECT _src.cat, _src.sub, _prev.Tot, COALESCE(SUM(v), 0) AS Sub FROM (SELECT * FROM (VALUES ('a', 'x', CAST(10 AS BIGINT)), ('a', 'y', CAST(20 AS BIGINT)), ('b', 'x', CAST(30 AS BIGINT))) AS t(cat, sub, v)) AS _src INNER JOIN (SELECT cat, Tot FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY Tot DESC) AS _rn0 FROM (SELECT cat, COALESCE(SUM(v), 0) AS Tot FROM (VALUES ('a', 'x', CAST(10 AS BIGINT)), ('a', 'y', CAST(20 AS BIGINT)), ('b', 'x', CAST(30 AS BIGINT))) AS t(cat, sub, v) GROUP BY cat))) AS _prev ON _src.cat = _prev.cat GROUP BY _src.cat, _src.sub, _prev.Tot))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[cat:String, Tot:Int, sub:String, Sub:Int] rows=3
    - ('b', 30, 'x', 30)
    - ('a', 30, 'y', 20)
    - ('a', 30, 'x', 10)
- DuckDB: cols=[cat:String, Tot:Int, sub:String, sub:String] rows=3
    - ('b', 30, 'x', 'x')
    - ('a', 30, 'y', 'y')
    - ('a', 30, 'x', 'x')

### `agent-window-series-scan-0042` — MismatchRows (high)

*Detail:* row count: kusto=0 duck=1

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2020-01-01),5 ] | make-series s = sum(v) default = 0 on t from datetime(2020-01-01) to datetime(2020-01-01) step 1d
```
**Generated SQL**
```sql
SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00', CAST(5 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic] rows=0
- DuckDB: cols=[s:Unknown, t:Unknown] rows=1
    - (null, null)

### `agent-window-series-scan-0003` — MismatchRows (high)

*Detail:* first differing row[2]: kusto=('b', 10, 10) duck=('b', 10, 3.3333333333333335)

**KQL**
```kql
datatable(g:string, v:real)[ "a",1.5, "a",2.5, "b",10.0, "b",20.0, "b",30.0 ] | sort by g asc, v asc | serialize avgsofar = row_cumsum(v, g != prev(g)) / row_number(1, g != prev(g))
```
**Generated SQL**
```sql
SELECT * EXCLUDE (_rb0, _rg0), SUM(v) OVER (PARTITION BY _rg0 ROWS UNBOUNDED PRECEDING) / (ROW_NUMBER() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) + (1 - 1)) AS avgsofar FROM (SELECT *, SUM(CASE WHEN _rb0 THEN 1 ELSE 0 END) OVER (ROWS UNBOUNDED PRECEDING) AS _rg0 FROM (SELECT *, (g IS DISTINCT FROM LAG(g) OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST)) AS _rb0 FROM (SELECT * FROM (VALUES ('a', CAST(1.5 AS DOUBLE)), ('a', CAST(2.5 AS DOUBLE)), ('b', CAST(10.0 AS DOUBLE)), ('b', CAST(20.0 AS DOUBLE)), ('b', CAST(30.0 AS DOUBLE))) AS t(g, v) ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, v:Real, avgsofar:Real] rows=5
    - ('a', 1.5, 1.5)
    - ('a', 2.5, 2)
    - ('b', 10, 10)
    - ('b', 20, 15)
    - ('b', 30, 20)
- DuckDB: cols=[g:String, v:Real, avgsofar:Real] rows=5
    - ('a', 1.5, 1.5)
    - ('a', 2.5, 2)
    - ('b', 10, 3.3333333333333335)
    - ('b', 20, 7.5)
    - ('b', 30, 12)

### `agent-window-series-scan-0013` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[s:Int|DateTime], TYPE_MISMATCH[t:DateTime|Int]  
*Detail:* first differing row[0]: kusto=(10, 2021-01-01T00:00:00.0000000Z) duck=(2021-01-01T00:00:00.0000000Z, 10)

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-02),20, datetime(2021-01-03),30 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-04) step 1d | mv-expand t to typeof(datetime), s to typeof(long)
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (t, s), CAST(u.v0 AS TIMESTAMP) AS t, CAST(u.v1 AS BIGINT) AS s FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-04 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-02 00:00:00', CAST(20 AS BIGINT)), (TIMESTAMP '2021-01-03 00:00:00', CAST(30 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket) AS t, LATERAL (SELECT UNNEST(t.t) AS v0, UNNEST(t.s) AS v1) AS u
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, t:DateTime] rows=3
    - (10, 2021-01-01T00:00:00.0000000Z)
    - (20, 2021-01-02T00:00:00.0000000Z)
    - (30, 2021-01-03T00:00:00.0000000Z)
- DuckDB: cols=[t:DateTime, s:Int] rows=3
    - (2021-01-01T00:00:00.0000000Z, 10)
    - (2021-01-02T00:00:00.0000000Z, 20)
    - (2021-01-03T00:00:00.0000000Z, 30)

### `agent-window-series-scan-0014` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[t:Dynamic|Int]  
*Detail:* first differing row[0]: kusto=(1, [
  0,
  2,
  4,
  6
]) duck=([0,2,4,6], 1)

**KQL**
```kql
datatable(t:long, v:long)[ 0,1, 2,3, 4,5, 6,7 ] | make-series s = sum(v) default = -1 on t from 0 to 8 step 2 | mv-expand s to typeof(long)
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (s), CAST(u.value AS BIGINT) AS s FROM (SELECT LIST(COALESCE(s_val, (-1)) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT (0) + _i*(2) AS _ts FROM range(0, CAST(CEIL(((8) - (0))/(2)) AS BIGINT)) AS _r(_i)) AS _axis LEFT JOIN (SELECT ((0) + FLOOR(((t) - (0))/(2))*(2)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (CAST(0 AS BIGINT), CAST(1 AS BIGINT)), (CAST(2 AS BIGINT), CAST(3 AS BIGINT)), (CAST(4 AS BIGINT), CAST(5 AS BIGINT)), (CAST(6 AS BIGINT), CAST(7 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket) AS t CROSS JOIN UNNEST(t.s) AS u(value)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, t:Dynamic] rows=4
    - (1, [
  0,
  2,
  4,
  6
])
    - (3, [
  0,
  2,
  4,
  6
])
    - (5, [
  0,
  2,
  4,
  6
])
    - (7, [
  0,
  2,
  4,
  6
])
- DuckDB: cols=[t:Unknown, s:Int] rows=4
    - ([0,2,4,6], 1)
    - ([0,2,4,6], 3)
    - ([0,2,4,6], 5)
    - ([0,2,4,6], 7)

### `agent-window-series-scan-0015` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[st:Real|String]  
*Detail:* first differing row[0]: kusto=([
  10,
  0,
  0,
  0,
  50
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z"
], 0) duck=([10,0,0,0,50], ["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z","2021-01-05T00:00:00.0000000Z"], '{"min":0.0,"min_idx":1,"max":50.0,"max_idx":4,"avg":12.0,"sum":60.0,"stdev":21.6794833886788,"variance":470.0}')

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-05),50 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-06) step 1d | extend st = series_stats(s)
```
**Generated SQL**
```sql
SELECT *, json_object('min', LIST_MIN(LIST_FILTER(s, x -> x IS NOT NULL)), 'min_idx', LIST_POSITION(s, LIST_MIN(LIST_FILTER(s, x -> x IS NOT NULL))) - 1, 'max', LIST_MAX(LIST_FILTER(s, x -> x IS NOT NULL)), 'max_idx', LIST_POSITION(s, LIST_MAX(LIST_FILTER(s, x -> x IS NOT NULL))) - 1, 'avg', LIST_AVG(LIST_FILTER(s, x -> x IS NOT NULL)), 'sum', LIST_SUM(LIST_FILTER(s, x -> x IS NOT NULL)), 'stdev', LIST_AGGREGATE(LIST_FILTER(s, x -> x IS NOT NULL), 'stddev_samp'), 'variance', LIST_AGGREGATE(LIST_FILTER(s, x -> x IS NOT NULL), 'var_samp')) AS st FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-06 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-05 00:00:00', CAST(50 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic, st:Real] rows=1
    - ([
  10,
  0,
  0,
  0,
  50
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z"
], 0)
- DuckDB: cols=[s:Unknown, t:Unknown, st:String] rows=1
    - ([10,0,0,0,50], ["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z","2021-01-05T00:00:00.0000000Z"], '{"min":0.0,"min_idx":1,"max":50.0,"max_idx":4,"avg":12.0,"sum":60.0,"stdev":21.6794833886788,"variance":470.0}')

### `agent-window-series-scan-0016` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[fit:Real|String]  
*Detail:* first differing row[0]: kusto=([
  1.0,
  2.0,
  4.0,
  8.0
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z"
], 0.92) duck=([1,2,4,8], ["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z"], '{"slope":2.3,"interception":0.30000000000000027,"rsquare":0.9199999999999999,"line_fit":[0.30000000000000027,2.6,4.9,7.199999999999999]}')

**KQL**
```kql
datatable(t:datetime, v:real)[ datetime(2021-01-01),1.0, datetime(2021-01-02),2.0, datetime(2021-01-03),4.0, datetime(2021-01-04),8.0 ] | make-series s = avg(v) default = 0.0 on t from datetime(2021-01-01) to datetime(2021-01-05) step 1d | extend fit = series_fit_line(s)
```
**Generated SQL**
```sql
SELECT *, json_object('slope', ((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0)), 'interception', ((LIST_SUM(s)::DOUBLE - (((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0))) * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE) / NULLIF(LEN(s)::DOUBLE, 0)), 'rsquare', (1 - (LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> POWER(s[i] - ((((LIST_SUM(s)::DOUBLE - (((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0))) * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE) / NULLIF(LEN(s)::DOUBLE, 0))) + (((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0))) * (i - 1)), 2)))) / NULLIF(LIST_SUM(LIST_TRANSFORM(s, y -> POWER(y - (LIST_SUM(s)::DOUBLE / NULLIF(LEN(s)::DOUBLE, 0)), 2))), 0)), 'line_fit', LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> (((LIST_SUM(s)::DOUBLE - (((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0))) * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE) / NULLIF(LEN(s)::DOUBLE, 0))) + (((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0))) * x)) AS fit FROM (SELECT LIST(COALESCE(s_val, 0.0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-05 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(AVG(v), 'nan'::DOUBLE) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(1.0 AS DOUBLE)), (TIMESTAMP '2021-01-02 00:00:00', CAST(2.0 AS DOUBLE)), (TIMESTAMP '2021-01-03 00:00:00', CAST(4.0 AS DOUBLE)), (TIMESTAMP '2021-01-04 00:00:00', CAST(8.0 AS DOUBLE))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic, fit:Real] rows=1
    - ([
  1.0,
  2.0,
  4.0,
  8.0
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z"
], 0.92)
- DuckDB: cols=[s:Unknown, t:Unknown, fit:String] rows=1
    - ([1,2,4,8], ["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z"], '{"slope":2.3,"interception":0.30000000000000027,"rsquare":0.9199999999999999,"line_fit":[0.30000000000000027,2.6,4.9,7.199999999999999]}')

### `agent-window-series-scan-0018` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  10,
  0,
  0,
  40
], [
  10,
  0,
  0,
  40
], [
  10.0,
  null,
  null,
  40.0
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z"
]) duck=([10,0,0,40], [10,0,0,40], '<unreadable:IndexOutOfRangeException>', ["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z"])

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-04),40 ] | make-series mn = min(v), mx = max(v), av = avg(v) default = long(null) on t from datetime(2021-01-01) to datetime(2021-01-05) step 1d
```
**Generated SQL**
```sql
SELECT LIST(COALESCE(mn_val, 0) ORDER BY _ts) AS mn, LIST(COALESCE(mx_val, 0) ORDER BY _ts) AS mx, LIST(COALESCE(av_val, CAST(NULL AS BIGINT)) ORDER BY _ts) AS av, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-05 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(MIN(v) AS DOUBLE) AS mn_val, CAST(MAX(v) AS DOUBLE) AS mx_val, CAST(COALESCE(AVG(v), 'nan'::DOUBLE) AS DOUBLE) AS av_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-04 00:00:00', CAST(40 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[mn:Dynamic, mx:Dynamic, av:Dynamic, t:Dynamic] rows=1
    - ([
  10,
  0,
  0,
  40
], [
  10,
  0,
  0,
  40
], [
  10.0,
  null,
  null,
  40.0
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z"
])
- DuckDB: cols=[mn:Unknown, mx:Unknown, av:Unknown, t:Unknown] rows=1
    - ([10,0,0,40], [10,0,0,40], '<unreadable:IndexOutOfRangeException>', ["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z"])

### `agent-window-series-scan-0037` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  100,
  0,
  0
], [
  "2020-01-01T00:00:00.0000000Z",
  "2020-01-04T00:00:00.0000000Z",
  "2020-01-07T00:00:00.0000000Z"
]) duck=([0,0,0], ["2020-01-01T00:00:00.0000000Z","2020-01-04T00:00:00.0000000Z","2020-01-07T00:00:00.0000000Z"])

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2020-01-01),100 ] | make-series s = sum(v) default = 0 on t from datetime(2020-01-01) to datetime(2020-01-10) step 3d
```
**Generated SQL**
```sql
SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-10 00:00:00', 259200000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/259200000)*259200000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00', CAST(100 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic] rows=1
    - ([
  100,
  0,
  0
], [
  "2020-01-01T00:00:00.0000000Z",
  "2020-01-04T00:00:00.0000000Z",
  "2020-01-07T00:00:00.0000000Z"
])
- DuckDB: cols=[s:Unknown, t:Unknown] rows=1
    - ([0,0,0], ["2020-01-01T00:00:00.0000000Z","2020-01-04T00:00:00.0000000Z","2020-01-07T00:00:00.0000000Z"])

### `agent-window-series-scan-0039` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a', [
  10,
  30,
  0
], [
  1,
  3,
  5
], [
  10.0,
  40.0,
  40.0
]) duck=('a', [10,30,0], [1,3,5], [10,30,0])

**KQL**
```kql
datatable(t:long, v:long, g:string)[ 1,10,"a", 3,30,"a", 1,5,"b", 5,50,"b" ] | make-series s = sum(v) default = 0 on t from 1 to 6 step 2 by g | extend cums = series_iir(s, dynamic([1]), dynamic([1,-1]))
```
**Generated SQL**
```sql
SELECT *, LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(LIST_VALUE(1))), k -> CASE WHEN i - k + 1 >= 1 THEN LIST_VALUE(1)[k] * COALESCE(s[i - k + 1], 0) ELSE 0 END))) AS cums FROM (SELECT _axis.g, LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT _g.*, _t._ts FROM (SELECT DISTINCT g FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'a'), (CAST(3 AS BIGINT), CAST(30 AS BIGINT), 'a'), (CAST(1 AS BIGINT), CAST(5 AS BIGINT), 'b'), (CAST(5 AS BIGINT), CAST(50 AS BIGINT), 'b')) AS t(t, v, g)) AS _g CROSS JOIN (SELECT (1) + _i*(2) AS _ts FROM range(0, CAST(CEIL(((6) - (1))/(2)) AS BIGINT)) AS _r(_i)) AS _t) AS _axis LEFT JOIN (SELECT g, ((1) + FLOOR(((t) - (1))/(2))*(2)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'a'), (CAST(3 AS BIGINT), CAST(30 AS BIGINT), 'a'), (CAST(1 AS BIGINT), CAST(5 AS BIGINT), 'b'), (CAST(5 AS BIGINT), CAST(50 AS BIGINT), 'b')) AS t(t, v, g) GROUP BY g, _bucket) AS _data ON _axis.g = _data.g AND _axis._ts = _data._bucket GROUP BY _axis.g)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, s:Dynamic, t:Dynamic, cums:Dynamic] rows=2
    - ('a', [
  10,
  30,
  0
], [
  1,
  3,
  5
], [
  10.0,
  40.0,
  40.0
])
    - ('b', [
  5,
  0,
  50
], [
  1,
  3,
  5
], [
  5.0,
  5.0,
  55.0
])
- DuckDB: cols=[g:String, s:Unknown, t:Unknown, cums:Unknown] rows=2
    - ('a', [10,30,0], [1,3,5], [10,30,0])
    - ('b', [5,0,50], [1,3,5], [5,0,50])

### `agent-window-series-scan-0001` — MismatchRows (high)

*Detail:* first differing row[3]: kusto=('b', 3, 1, 1, 1, 1) duck=('b', 3, 3, 4, 4, 1)

**KQL**
```kql
datatable(g:string, v:long)[ "a",5, "a",5, "a",8, "b",3, "b",3 ] | sort by g asc, v asc | serialize rd = row_rank_dense(v, g != prev(g)), rm = row_rank_min(v, g != prev(g)), rn = row_number(1, g != prev(g)), gap = row_rank_min(v) - row_rank_dense(v)
```
**Generated SQL**
```sql
SELECT *, DENSE_RANK() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS rd, RANK() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS rm, (ROW_NUMBER() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) + (1 - 1)) AS rn, RANK() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) - DENSE_RANK() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS gap FROM (VALUES ('a', CAST(5 AS BIGINT)), ('a', CAST(5 AS BIGINT)), ('a', CAST(8 AS BIGINT)), ('b', CAST(3 AS BIGINT)), ('b', CAST(3 AS BIGINT))) AS t(g, v) ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, v:Int, rd:Int, rm:Int, rn:Int, gap:Int] rows=5
    - ('a', 5, 1, 1, 1, 0)
    - ('a', 5, 1, 1, 2, 0)
    - ('a', 8, 2, 3, 3, 1)
    - ('b', 3, 1, 1, 1, 1)
    - ('b', 3, 1, 1, 2, 1)
- DuckDB: cols=[g:String, v:Int, rd:Int, rm:Int, rn:Int, gap:Int] rows=5
    - ('a', 5, 1, 1, 1, 0)
    - ('a', 5, 1, 1, 2, 0)
    - ('a', 8, 2, 3, 3, 1)
    - ('b', 3, 3, 4, 4, 1)
    - ('b', 3, 3, 4, 5, 1)

### `agent-window-series-scan-0005` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[pctcum:Int|Real]  
*Detail:* first differing row[0]: kusto=(5, 15, 71, 1) duck=(5, 15, 71.42857142857143, 1)

**KQL**
```kql
datatable(x:long)[ 1,2,3,4,5,6 ] | sort by x asc | serialize cs = row_cumsum(x) | extend pctcum = cs * 100 / 21 | where pctcum > 50 | serialize rn2 = row_number()
```
**Generated SQL**
```sql
SELECT *, ROW_NUMBER() OVER () AS rn2 FROM (SELECT *, cs * 100 / 21 AS pctcum FROM (SELECT *, SUM(x) OVER (ORDER BY x ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS cs FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT)), (CAST(4 AS BIGINT)), (CAST(5 AS BIGINT)), (CAST(6 AS BIGINT))) AS t(x) ORDER BY x ASC NULLS FIRST)) WHERE pctcum > 50
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, cs:Int, pctcum:Int, rn2:Int] rows=2
    - (5, 15, 71, 1)
    - (6, 21, 100, 2)
- DuckDB: cols=[x:Int, cs:Int, pctcum:Real, rn2:Int] rows=2
    - (5, 15, 71.42857142857143, 1)
    - (6, 21, 100, 2)

### `agent-window-series-scan-0008` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[avg:Int|Real]  
*Detail:* first differing row[4]: kusto=(5, 2, 9, 2, 4) duck=(5, 2, 9, 2, 4.5)

**KQL**
```kql
datatable(t:long, v:long)[ 1,5, 2,3, 3,99, 4,7, 5,2, 6,99, 7,1 ] | sort by t asc | scan declare (cum:long=0, runlen:long=0) with (step s: true => cum = iff(v == 99, 0, s.cum + v), runlen = iff(v == 99, 0, s.runlen + 1);) | extend avg = iff(runlen == 0, 0, cum / runlen)
```
**Generated SQL**
```sql
SELECT *, CASE WHEN runlen = 0 THEN 0 ELSE cum / runlen END AS avg FROM (WITH RECURSIVE __src AS (SELECT *, ROW_NUMBER() OVER (ORDER BY t ASC NULLS FIRST) AS __rn FROM (VALUES (CAST(1 AS BIGINT), CAST(5 AS BIGINT)), (CAST(2 AS BIGINT), CAST(3 AS BIGINT)), (CAST(3 AS BIGINT), CAST(99 AS BIGINT)), (CAST(4 AS BIGINT), CAST(7 AS BIGINT)), (CAST(5 AS BIGINT), CAST(2 AS BIGINT)), (CAST(6 AS BIGINT), CAST(99 AS BIGINT)), (CAST(7 AS BIGINT), CAST(1 AS BIGINT))) AS t(t, v) ORDER BY t ASC NULLS FIRST), __scan(__rn, cum, runlen) AS (SELECT s.__rn, CAST(CASE WHEN v = 99 THEN 0 ELSE 0 + v END AS BIGINT) AS cum, CAST(CASE WHEN v = 99 THEN 0 ELSE 0 + 1 END AS BIGINT) AS runlen FROM __src AS s WHERE s.__rn = 1 UNION ALL SELECT s.__rn, CAST(CASE WHEN v = 99 THEN 0 ELSE p.cum + v END AS BIGINT) AS cum, CAST(CASE WHEN v = 99 THEN 0 ELSE p.runlen + 1 END AS BIGINT) AS runlen FROM __scan AS p JOIN __src AS s ON s.__rn = p.__rn + 1) SELECT __src.* EXCLUDE (__rn), __scan.cum, __scan.runlen FROM __src JOIN __scan USING (__rn) ORDER BY __src.__rn)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:Int, v:Int, cum:Int, runlen:Int, avg:Int] rows=7
    - (1, 5, 5, 1, 5)
    - (2, 3, 8, 2, 4)
    - (3, 99, 0, 0, 0)
    - (4, 7, 7, 1, 7)
    - (5, 2, 9, 2, 4)
    - (6, 99, 0, 0, 0)
    - (7, 1, 1, 1, 1)
- DuckDB: cols=[t:Int, v:Int, cum:Int, runlen:Int, avg:Real] rows=7
    - (1, 5, 5, 1, 5)
    - (2, 3, 8, 2, 4)
    - (3, 99, 0, 0, 0)
    - (4, 7, 7, 1, 7)
    - (5, 2, 9, 2, 4.5)
    - (6, 99, 0, 0, 0)
    - (7, 1, 1, 1, 1)

### `agent-window-series-scan-0017` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[s:Int|DateTime], TYPE_MISMATCH[t:DateTime|Int]  
*Detail:* first differing row[0]: kusto=('a', 10, 2021-01-01T00:00:00.0000000Z) duck=('a', 2021-01-01T00:00:00.0000000Z, 10)

**KQL**
```kql
datatable(t:datetime, v:long, g:string)[ datetime(2021-01-01),10,"a", datetime(2021-01-02),20,"a", datetime(2021-01-01),5,"b" ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-04) step 1d by g | mv-expand t to typeof(datetime), s to typeof(long) | sort by g asc, t asc
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (t, s), CAST(u.v0 AS TIMESTAMP) AS t, CAST(u.v1 AS BIGINT) AS s FROM (SELECT _axis.g, LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT _g.*, _t._ts FROM (SELECT DISTINCT g FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT), 'a'), (TIMESTAMP '2021-01-02 00:00:00', CAST(20 AS BIGINT), 'a'), (TIMESTAMP '2021-01-01 00:00:00', CAST(5 AS BIGINT), 'b')) AS t(t, v, g)) AS _g CROSS JOIN (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-04 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _t) AS _axis LEFT JOIN (SELECT g, EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT), 'a'), (TIMESTAMP '2021-01-02 00:00:00', CAST(20 AS BIGINT), 'a'), (TIMESTAMP '2021-01-01 00:00:00', CAST(5 AS BIGINT), 'b')) AS t(t, v, g) GROUP BY g, _bucket) AS _data ON _axis.g = _data.g AND _axis._ts = _data._bucket GROUP BY _axis.g) AS t, LATERAL (SELECT UNNEST(t.t) AS v0, UNNEST(t.s) AS v1) AS u ORDER BY g ASC NULLS FIRST, t ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, s:Int, t:DateTime] rows=6
    - ('a', 10, 2021-01-01T00:00:00.0000000Z)
    - ('a', 20, 2021-01-02T00:00:00.0000000Z)
    - ('a', 0, 2021-01-03T00:00:00.0000000Z)
    - ('b', 5, 2021-01-01T00:00:00.0000000Z)
    - ('b', 0, 2021-01-02T00:00:00.0000000Z)
    - ('b', 0, 2021-01-03T00:00:00.0000000Z)
- DuckDB: cols=[g:String, t:DateTime, s:Int] rows=6
    - ('a', 2021-01-01T00:00:00.0000000Z, 10)
    - ('a', 2021-01-02T00:00:00.0000000Z, 20)
    - ('a', 2021-01-03T00:00:00.0000000Z, 0)
    - ('b', 2021-01-01T00:00:00.0000000Z, 5)
    - ('b', 2021-01-02T00:00:00.0000000Z, 0)
    - ('b', 2021-01-03T00:00:00.0000000Z, 0)

### `agent-window-series-scan-0019` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[rsq:Dynamic|String], TYPE_MISMATCH[slope:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(1.0, 0.9999999999999999) duck=('1.0', '1.0')

**KQL**
```kql
datatable(t:long, v:real)[ 1,1.0, 2,2.0, 3,3.0, 4,4.0, 5,5.0 ] | make-series s = avg(v) default = 0.0 on t from 1 to 6 step 1 | extend fl = series_fit_line_dynamic(s) | project rsq = fl.rsquare, slope = fl.slope
```
**Generated SQL**
```sql
SELECT json_extract(fl, '$.rsquare') AS rsq, json_extract(fl, '$.slope') AS slope FROM (SELECT *, json_object('slope', ((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0)), 'interception', ((LIST_SUM(s)::DOUBLE - (((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0))) * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE) / NULLIF(LEN(s)::DOUBLE, 0)), 'rsquare', (1 - (LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> POWER(s[i] - ((((LIST_SUM(s)::DOUBLE - (((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0))) * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE) / NULLIF(LEN(s)::DOUBLE, 0))) + (((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0))) * (i - 1)), 2)))) / NULLIF(LIST_SUM(LIST_TRANSFORM(s, y -> POWER(y - (LIST_SUM(s)::DOUBLE / NULLIF(LEN(s)::DOUBLE, 0)), 2))), 0)), 'line_fit', LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> (((LIST_SUM(s)::DOUBLE - (((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0))) * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE) / NULLIF(LEN(s)::DOUBLE, 0))) + (((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> (i - 1) * s[i]))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(s)::DOUBLE) / NULLIF((LEN(s)::DOUBLE * LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(0, LEN(s) - 1), x -> x * x))::DOUBLE - LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE * LIST_SUM(GENERATE_SERIES(0, LEN(s) - 1))::DOUBLE), 0))) * x)) AS fl FROM (SELECT LIST(COALESCE(s_val, 0.0) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT (1) + _i*(1) AS _ts FROM range(0, CAST(CEIL(((6) - (1))/(1)) AS BIGINT)) AS _r(_i)) AS _axis LEFT JOIN (SELECT ((1) + FLOOR(((t) - (1))/(1))*(1)) AS _bucket, CAST(COALESCE(AVG(v), 'nan'::DOUBLE) AS DOUBLE) AS s_val FROM (VALUES (CAST(1 AS BIGINT), CAST(1.0 AS DOUBLE)), (CAST(2 AS BIGINT), CAST(2.0 AS DOUBLE)), (CAST(3 AS BIGINT), CAST(3.0 AS DOUBLE)), (CAST(4 AS BIGINT), CAST(4.0 AS DOUBLE)), (CAST(5 AS BIGINT), CAST(5.0 AS DOUBLE))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[rsq:Dynamic, slope:Dynamic] rows=1
    - (1.0, 0.9999999999999999)
- DuckDB: cols=[rsq:String, slope:String] rows=1
    - ('1.0', '1.0')

### `agent-window-series-scan-0021` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[t:Dynamic|Int]  
*Detail:* first differing row[0]: kusto=(10, [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z",
  "2021-01-06T00:00:00.0000000Z"
], 10) duck=(["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z","2021-01-05T00:00:00.0000000Z","2021-01-06T00:00:00.0000000Z"], 10, 10)

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-04),40, datetime(2021-01-06),60 ] | make-series s = sum(v) default = long(null) on t from datetime(2021-01-01) to datetime(2021-01-07) step 1d | extend ff = series_fill_forward(s) | mv-expand s to typeof(long), ff to typeof(long)
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (s, ff), CAST(u.v0 AS BIGINT) AS s, CAST(u.v1 AS BIGINT) AS ff FROM (SELECT *, LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> LIST_LAST(LIST_TRANSFORM(LIST_FILTER(GENERATE_SERIES(1, i), j -> s[j] IS NOT NULL), j -> s[j]))) AS ff FROM (SELECT LIST(COALESCE(s_val, CAST(NULL AS BIGINT)) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-07 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-04 00:00:00', CAST(40 AS BIGINT)), (TIMESTAMP '2021-01-06 00:00:00', CAST(60 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)) AS t, LATERAL (SELECT UNNEST(t.s) AS v0, UNNEST(t.ff) AS v1) AS u
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, t:Dynamic, ff:Int] rows=6
    - (10, [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z",
  "2021-01-06T00:00:00.0000000Z"
], 10)
    - (null, [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z",
  "2021-01-06T00:00:00.0000000Z"
], 10)
    - (null, [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z",
  "2021-01-06T00:00:00.0000000Z"
], 10)
    - (40, [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z",
  "2021-01-06T00:00:00.0000000Z"
], 40)
    - (null, [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z",
  "2021-01-06T00:00:00.0000000Z"
], 40)
    - (60, [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z",
  "2021-01-06T00:00:00.0000000Z"
], 60)
- DuckDB: cols=[t:Unknown, s:Int, ff:Int] rows=6
    - (["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z","2021-01-05T00:00:00.0000000Z","2021-01-06T00:00:00.0000000Z"], 10, 10)
    - (["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z","2021-01-05T00:00:00.0000000Z","2021-01-06T00:00:00.0000000Z"], null, 10)
    - (["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z","2021-01-05T00:00:00.0000000Z","2021-01-06T00:00:00.0000000Z"], null, 10)
    - (["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z","2021-01-05T00:00:00.0000000Z","2021-01-06T00:00:00.0000000Z"], 40, 40)
    - (["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z","2021-01-05T00:00:00.0000000Z","2021-01-06T00:00:00.0000000Z"], null, 40)
    - (["2021-01-01T00:00:00.0000000Z","2021-01-02T00:00:00.0000000Z","2021-01-03T00:00:00.0000000Z","2021-01-04T00:00:00.0000000Z","2021-01-05T00:00:00.0000000Z","2021-01-06T00:00:00.0000000Z"], 60, 60)

### `agent-window-series-scan-0030` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(3, 1) duck=(6, 1)

**KQL**
```kql
datatable(g:long, v:long)[ 1,100, 1,100, 2,1, 3,1, 4,1, 5,1, 6,1 ] | top-hitters 2 of g by v | sort by g asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT * FROM (SELECT g, SUM(v) AS approximate_sum_v FROM (VALUES (CAST(1 AS BIGINT), CAST(100 AS BIGINT)), (CAST(1 AS BIGINT), CAST(100 AS BIGINT)), (CAST(2 AS BIGINT), CAST(1 AS BIGINT)), (CAST(3 AS BIGINT), CAST(1 AS BIGINT)), (CAST(4 AS BIGINT), CAST(1 AS BIGINT)), (CAST(5 AS BIGINT), CAST(1 AS BIGINT)), (CAST(6 AS BIGINT), CAST(1 AS BIGINT))) AS t(g, v) GROUP BY g ORDER BY approximate_sum_v DESC, g DESC LIMIT 2) ORDER BY approximate_sum_v ASC, g ASC) ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:Int, approximate_sum_v:Int] rows=2
    - (1, 200)
    - (3, 1)
- DuckDB: cols=[g:Int, approximate_sum_v:Int] rows=2
    - (1, 200)
    - (6, 1)

### `agent-window-series-scan-0034` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(2, 3, 120) duck=(2, 5, 120)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,40, 2,50 ] | sort by k asc, v asc | serialize within = row_number(1, k != prev(k)), cumg = row_cumsum(v, k != prev(k)) | summarize maxwithin = max(within), tot = sum(v) by k | sort by k asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT k, MAX(within) AS maxwithin, COALESCE(SUM(v), 0) AS tot FROM (SELECT * EXCLUDE (_rb0, _rg0), (ROW_NUMBER() OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST) + (1 - 1)) AS within, SUM(v) OVER (PARTITION BY _rg0 ROWS UNBOUNDED PRECEDING) AS cumg FROM (SELECT *, SUM(CASE WHEN _rb0 THEN 1 ELSE 0 END) OVER (ROWS UNBOUNDED PRECEDING) AS _rg0 FROM (SELECT *, (k IS DISTINCT FROM LAG(k) OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST)) AS _rb0 FROM (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(40 AS BIGINT)), (CAST(2 AS BIGINT), CAST(50 AS BIGINT))) AS t(k, v) ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST)))) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, maxwithin:Int, tot:Int] rows=2
    - (1, 2, 30)
    - (2, 3, 120)
- DuckDB: cols=[k:Int, maxwithin:Int, tot:Int] rows=2
    - (1, 2, 30)
    - (2, 5, 120)

### `agent-window-series-scan-0035` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('CAFE', 4, 1, 1, '') duck=('CAFE', 4, 1, 1, null)

**KQL**
```kql
datatable(s:string, v:long)[ "café",1, "cafe",2, "Café",3, "CAFE",4 ] | sort by s asc | serialize rd = row_rank_dense(s), rm = row_rank_min(s), p = prev(s)
```
**Generated SQL**
```sql
SELECT *, DENSE_RANK() OVER (ORDER BY s ASC NULLS FIRST) AS rd, RANK() OVER (ORDER BY s ASC NULLS FIRST) AS rm, LAG(s) OVER (ORDER BY s ASC NULLS FIRST) AS p FROM (VALUES ('café', CAST(1 AS BIGINT)), ('cafe', CAST(2 AS BIGINT)), ('Café', CAST(3 AS BIGINT)), ('CAFE', CAST(4 AS BIGINT))) AS t(s, v) ORDER BY s ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, v:Int, rd:Int, rm:Int, p:String] rows=4
    - ('CAFE', 4, 1, 1, '')
    - ('Café', 3, 2, 2, 'CAFE')
    - ('cafe', 2, 3, 3, 'Café')
    - ('café', 1, 4, 4, 'cafe')
- DuckDB: cols=[s:String, v:Int, rd:Int, rm:Int, p:String] rows=4
    - ('CAFE', 4, 1, 1, null)
    - ('Café', 3, 2, 2, 'CAFE')
    - ('cafe', 2, 3, 3, 'Café')
    - ('café', 1, 4, 4, 'cafe')

### `agent-window-series-scan-0039` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[s:Int|DateTime], TYPE_MISMATCH[t:DateTime|Int]  
*Detail:* first differing row[0]: kusto=(0, 2021-01-01T00:00:00.0000000Z) duck=(2021-01-01T00:00:00.0000000Z, 0)

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01 23:00),1, datetime(2021-01-02 01:00),2, datetime(2021-01-02 23:00),3 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-03) step 12h | mv-expand t to typeof(datetime), s to typeof(long) | sort by t asc
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (t, s), CAST(u.v0 AS TIMESTAMP) AS t, CAST(u.v1 AS BIGINT) AS s FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-03 00:00:00', 43200000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/43200000)*43200000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 23:00:00', CAST(1 AS BIGINT)), (TIMESTAMP '2021-01-02 01:00:00', CAST(2 AS BIGINT)), (TIMESTAMP '2021-01-02 23:00:00', CAST(3 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket) AS t, LATERAL (SELECT UNNEST(t.t) AS v0, UNNEST(t.s) AS v1) AS u ORDER BY t ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, t:DateTime] rows=4
    - (0, 2021-01-01T00:00:00.0000000Z)
    - (1, 2021-01-01T12:00:00.0000000Z)
    - (2, 2021-01-02T00:00:00.0000000Z)
    - (3, 2021-01-02T12:00:00.0000000Z)
- DuckDB: cols=[t:DateTime, s:Int] rows=4
    - (2021-01-01T00:00:00.0000000Z, 0)
    - (2021-01-01T12:00:00.0000000Z, 1)
    - (2021-01-02T00:00:00.0000000Z, 2)
    - (2021-01-02T12:00:00.0000000Z, 3)

### `agent-window-series-scan-0040` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[t:Dynamic|Int]  
*Detail:* first differing row[0]: kusto=(-7, [
  1,
  2,
  3,
  4,
  5,
  6,
  7
], 0) duck=([1,2,3,4,5,6,7], 0, -7)

**KQL**
```kql
datatable(t:long, v:long)[ 2,20, 4,40, 6,60 ] | make-series s = sum(v) default = -7 on t from 1 to 8 step 1 | mv-expand idx = range(0, array_length(s)-1, 1) to typeof(long), s to typeof(long) | sort by idx asc
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (s), CAST(u.v0 AS BIGINT) AS idx, CAST(u.v1 AS BIGINT) AS s FROM (SELECT LIST(COALESCE(s_val, (-7)) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT (1) + _i*(1) AS _ts FROM range(0, CAST(CEIL(((8) - (1))/(1)) AS BIGINT)) AS _r(_i)) AS _axis LEFT JOIN (SELECT ((1) + FLOOR(((t) - (1))/(1))*(1)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (CAST(2 AS BIGINT), CAST(20 AS BIGINT)), (CAST(4 AS BIGINT), CAST(40 AS BIGINT)), (CAST(6 AS BIGINT), CAST(60 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket) AS t, LATERAL (SELECT UNNEST(GENERATE_SERIES(0, LEN(s) - 1, 1)) AS v0, UNNEST(t.s) AS v1) AS u ORDER BY idx ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, t:Dynamic, idx:Int] rows=7
    - (-7, [
  1,
  2,
  3,
  4,
  5,
  6,
  7
], 0)
    - (20, [
  1,
  2,
  3,
  4,
  5,
  6,
  7
], 1)
    - (-7, [
  1,
  2,
  3,
  4,
  5,
  6,
  7
], 2)
    - (40, [
  1,
  2,
  3,
  4,
  5,
  6,
  7
], 3)
    - (-7, [
  1,
  2,
  3,
  4,
  5,
  6,
  7
], 4)
    - (60, [
  1,
  2,
  3,
  4,
  5,
  6,
  7
], 5)
    - (-7, [
  1,
  2,
  3,
  4,
  5,
  6,
  7
], 6)
- DuckDB: cols=[t:Unknown, idx:Int, s:Int] rows=7
    - ([1,2,3,4,5,6,7], 0, -7)
    - ([1,2,3,4,5,6,7], 1, 20)
    - ([1,2,3,4,5,6,7], 2, -7)
    - ([1,2,3,4,5,6,7], 3, 40)
    - ([1,2,3,4,5,6,7], 4, -7)
    - ([1,2,3,4,5,6,7], 5, 60)
    - ([1,2,3,4,5,6,7], 6, -7)

### `agent-window-series-scan-0041` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[t:Dynamic|Int]  
*Detail:* first differing row[0]: kusto=('a', 1, [
  1,
  2,
  3
], 1) duck=('a', [1,2,3], 1, 1)

**KQL**
```kql
datatable(t:long, v:long, g:string)[ 1,1,"a", 2,2,"a", 1,10,"b", 2,20,"b", 3,30,"b" ] | make-series s = sum(v) default = 0 on t from 1 to 4 step 1 by g | extend cum = series_iir(s, dynamic([1]), dynamic([1,-1])) | mv-expand s to typeof(long), cum to typeof(long) | sort by g asc
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (s, cum), CAST(u.v0 AS BIGINT) AS s, CAST(u.v1 AS BIGINT) AS cum FROM (SELECT *, LIST_TRANSFORM(GENERATE_SERIES(1, LEN(s)), i -> LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN(LIST_VALUE(1))), k -> CASE WHEN i - k + 1 >= 1 THEN LIST_VALUE(1)[k] * COALESCE(s[i - k + 1], 0) ELSE 0 END))) AS cum FROM (SELECT _axis.g, LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT _g.*, _t._ts FROM (SELECT DISTINCT g FROM (VALUES (CAST(1 AS BIGINT), CAST(1 AS BIGINT), 'a'), (CAST(2 AS BIGINT), CAST(2 AS BIGINT), 'a'), (CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'b'), (CAST(2 AS BIGINT), CAST(20 AS BIGINT), 'b'), (CAST(3 AS BIGINT), CAST(30 AS BIGINT), 'b')) AS t(t, v, g)) AS _g CROSS JOIN (SELECT (1) + _i*(1) AS _ts FROM range(0, CAST(CEIL(((4) - (1))/(1)) AS BIGINT)) AS _r(_i)) AS _t) AS _axis LEFT JOIN (SELECT g, ((1) + FLOOR(((t) - (1))/(1))*(1)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (CAST(1 AS BIGINT), CAST(1 AS BIGINT), 'a'), (CAST(2 AS BIGINT), CAST(2 AS BIGINT), 'a'), (CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'b'), (CAST(2 AS BIGINT), CAST(20 AS BIGINT), 'b'), (CAST(3 AS BIGINT), CAST(30 AS BIGINT), 'b')) AS t(t, v, g) GROUP BY g, _bucket) AS _data ON _axis.g = _data.g AND _axis._ts = _data._bucket GROUP BY _axis.g)) AS t, LATERAL (SELECT UNNEST(t.s) AS v0, UNNEST(t.cum) AS v1) AS u ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, s:Int, t:Dynamic, cum:Int] rows=6
    - ('a', 1, [
  1,
  2,
  3
], 1)
    - ('a', 2, [
  1,
  2,
  3
], 3)
    - ('a', 0, [
  1,
  2,
  3
], 3)
    - ('b', 10, [
  1,
  2,
  3
], 10)
    - ('b', 20, [
  1,
  2,
  3
], 30)
    - ('b', 30, [
  1,
  2,
  3
], 60)
- DuckDB: cols=[g:String, t:Unknown, s:Int, cum:Int] rows=6
    - ('a', [1,2,3], 1, 1)
    - ('a', [1,2,3], 2, 2)
    - ('a', [1,2,3], 0, 0)
    - ('b', [1,2,3], 10, 10)
    - ('b', [1,2,3], 20, 20)
    - ('b', [1,2,3], 30, 30)

### `agent-window-series-scan-0035` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(g:string, v:long)[ "a",10, "b",20, "c",30, "d",1 ] | top-hitters 2 of g
```
**Generated SQL**
```sql
SELECT * FROM (SELECT g, COUNT(*) AS approximate_count_g FROM (VALUES ('a', CAST(10 AS BIGINT)), ('b', CAST(20 AS BIGINT)), ('c', CAST(30 AS BIGINT)), ('d', CAST(1 AS BIGINT))) AS t(g, v) GROUP BY g ORDER BY approximate_count_g DESC, g DESC LIMIT 2) ORDER BY approximate_count_g ASC, g ASC
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, approximate_count_g:Int] rows=2
    - ('d', 1)
    - ('c', 1)
- DuckDB: cols=[g:String, approximate_count_g:Int] rows=2
    - ('c', 1)
    - ('d', 1)

### `agent-window-series-scan-0000` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30, 3,5 ] | sort by k asc, v asc | serialize rn = row_number(), p2 = prev(v,2), n2 = next(v,2), cs = row_cumsum(v, k != prev(k))
```
**Generated SQL**
```sql
SELECT * EXCLUDE (_rb0, _rg0), ROW_NUMBER() OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST) AS rn, LAG(v, 2) OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST) AS p2, LEAD(v, 2) OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST) AS n2, SUM(v) OVER (PARTITION BY _rg0 ROWS UNBOUNDED PRECEDING) AS cs FROM (SELECT *, SUM(CASE WHEN _rb0 THEN 1 ELSE 0 END) OVER (ROWS UNBOUNDED PRECEDING) AS _rg0 FROM (SELECT *, (k IS DISTINCT FROM LAG(k) OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST)) AS _rb0 FROM (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(3 AS BIGINT), CAST(5 AS BIGINT))) AS t(k, v) ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, v:Int, rn:Int, p2:Int, n2:Int, cs:Int] rows=5
    - (1, 10, 1, null, 30, 10)
    - (1, 20, 2, null, 30, 30)
    - (2, 30, 3, 10, 5, 30)
    - (2, 30, 4, 20, null, 60)
    - (3, 5, 5, 30, null, 5)
- DuckDB: cols=[k:Int, v:Int, rn:Int, p2:Int, n2:Int, cs:Int] rows=5
    - (1, 10, 1, null, 30, 10)
    - (1, 20, 2, null, 30, 30)
    - (3, 5, 5, 30, null, 5)
    - (2, 30, 3, 10, 5, 30)
    - (2, 30, 4, 20, null, 60)

### `agent-window-series-scan-0024` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(g:string, v:long)[ "a",1, "a",2, "b",3, "c",4, "c",5, "c",6, "d",7, "d",8 ] | top-hitters 3 of g by v
```
**Generated SQL**
```sql
SELECT * FROM (SELECT g, SUM(v) AS approximate_sum_v FROM (VALUES ('a', CAST(1 AS BIGINT)), ('a', CAST(2 AS BIGINT)), ('b', CAST(3 AS BIGINT)), ('c', CAST(4 AS BIGINT)), ('c', CAST(5 AS BIGINT)), ('c', CAST(6 AS BIGINT)), ('d', CAST(7 AS BIGINT)), ('d', CAST(8 AS BIGINT))) AS t(g, v) GROUP BY g ORDER BY approximate_sum_v DESC, g DESC LIMIT 3) ORDER BY approximate_sum_v ASC, g ASC
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, approximate_sum_v:Int] rows=3
    - ('b', 3)
    - ('d', 15)
    - ('c', 15)
- DuckDB: cols=[g:String, approximate_sum_v:Int] rows=3
    - ('b', 3)
    - ('c', 15)
    - ('d', 15)

### `agent-window-series-scan-0040` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(cat:string, sub:string, v:long)[ "a","x",10, "a","x",20, "a","y",5, "b","x",30 ] | top-nested 2 of cat by sum(v), top-nested 2 of sub by sum(v), top-nested 1 of v by sum(v)
```
**Generated SQL**
```sql
SELECT cat, aggregated_cat, sub, aggregated_sub, v, aggregated_v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cat, sub ORDER BY aggregated_v DESC) AS _rn2 FROM (SELECT _src.cat, _src.sub, _src.v, _prev.aggregated_cat, _prev.aggregated_sub, COALESCE(SUM(v), 0) AS aggregated_v FROM (SELECT * FROM (VALUES ('a', 'x', CAST(10 AS BIGINT)), ('a', 'x', CAST(20 AS BIGINT)), ('a', 'y', CAST(5 AS BIGINT)), ('b', 'x', CAST(30 AS BIGINT))) AS t(cat, sub, v)) AS _src INNER JOIN (SELECT cat, aggregated_cat, sub, aggregated_sub FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY aggregated_sub DESC) AS _rn1 FROM (SELECT _src.cat, _src.sub, _prev.aggregated_cat, COALESCE(SUM(v), 0) AS aggregated_sub FROM (SELECT * FROM (VALUES ('a', 'x', CAST(10 AS BIGINT)), ('a', 'x', CAST(20 AS BIGINT)), ('a', 'y', CAST(5 AS BIGINT)), ('b', 'x', CAST(30 AS BIGINT))) AS t(cat, sub, v)) AS _src INNER JOIN (SELECT cat, aggregated_cat FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY aggregated_cat DESC) AS _rn0 FROM (SELECT cat, COALESCE(SUM(v), 0) AS aggregated_cat FROM (VALUES ('a', 'x', CAST(10 AS BIGINT)), ('a', 'x', CAST(20 AS BIGINT)), ('a', 'y', CAST(5 AS BIGINT)), ('b', 'x', CAST(30 AS BIGINT))) AS t(cat, sub, v) GROUP BY cat)) WHERE _rn0 <= 2) AS _prev ON _src.cat = _prev.cat GROUP BY _src.cat, _src.sub, _prev.aggregated_cat)) WHERE _rn1 <= 2) AS _prev ON _src.cat = _prev.cat AND _src.sub = _prev.sub GROUP BY _src.cat, _src.sub, _src.v, _prev.aggregated_cat, _prev.aggregated_sub)) WHERE _rn2 <= 1
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[cat:String, aggregated_cat:Int, sub:String, aggregated_sub:Int, v:Int, aggregated_v:Int] rows=3
    - ('a', 35, 'x', 30, 20, 20)
    - ('a', 35, 'y', 5, 5, 5)
    - ('b', 30, 'x', 30, 30, 30)
- DuckDB: cols=[cat:String, aggregated_cat:Int, sub:String, aggregated_sub:Int, v:Int, aggregated_v:Int] rows=3
    - ('a', 35, 'x', 30, 20, 20)
    - ('b', 30, 'x', 30, 30, 30)
    - ('a', 35, 'y', 5, 5, 5)

### `agent-window-series-scan-0004` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(g:string, v:long)[ "z",1, "z",2, "y",3, "y",4, "x",5 ] | sort by g desc, v asc | serialize first_in_g = iff(g != prev(g), v, prev(v)), runsum = row_cumsum(v, g != prev(g))
```
**Generated SQL**
```sql
SELECT * EXCLUDE (_rb0, _rg0), CASE WHEN g IS DISTINCT FROM LAG(g) OVER (ORDER BY g DESC NULLS LAST, v ASC NULLS FIRST) THEN v ELSE LAG(v) OVER (ORDER BY g DESC NULLS LAST, v ASC NULLS FIRST) END AS first_in_g, SUM(v) OVER (PARTITION BY _rg0 ROWS UNBOUNDED PRECEDING) AS runsum FROM (SELECT *, SUM(CASE WHEN _rb0 THEN 1 ELSE 0 END) OVER (ROWS UNBOUNDED PRECEDING) AS _rg0 FROM (SELECT *, (g IS DISTINCT FROM LAG(g) OVER (ORDER BY g DESC NULLS LAST, v ASC NULLS FIRST)) AS _rb0 FROM (SELECT * FROM (VALUES ('z', CAST(1 AS BIGINT)), ('z', CAST(2 AS BIGINT)), ('y', CAST(3 AS BIGINT)), ('y', CAST(4 AS BIGINT)), ('x', CAST(5 AS BIGINT))) AS t(g, v) ORDER BY g DESC NULLS LAST, v ASC NULLS FIRST)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, v:Int, first_in_g:Int, runsum:Int] rows=5
    - ('z', 1, 1, 1)
    - ('z', 2, 1, 3)
    - ('y', 3, 3, 3)
    - ('y', 4, 3, 7)
    - ('x', 5, 5, 5)
- DuckDB: cols=[g:String, v:Int, first_in_g:Int, runsum:Int] rows=5
    - ('y', 3, 3, 3)
    - ('y', 4, 3, 7)
    - ('x', 5, 5, 5)
    - ('z', 1, 1, 1)
    - ('z', 2, 1, 3)

### `agent-window-series-scan-0007` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(t:long, v:long, f:bool)[ 1,5,false, 2,3,true, 3,8,false, 4,1,true, 5,9,false ] | sort by t asc | serialize cs = row_cumsum(v, f)
```
**Generated SQL**
```sql
SELECT * EXCLUDE (_rb0, _rg0), SUM(v) OVER (PARTITION BY _rg0 ROWS UNBOUNDED PRECEDING) AS cs FROM (SELECT *, SUM(CASE WHEN _rb0 THEN 1 ELSE 0 END) OVER (ROWS UNBOUNDED PRECEDING) AS _rg0 FROM (SELECT *, (f) AS _rb0 FROM (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(5 AS BIGINT), FALSE), (CAST(2 AS BIGINT), CAST(3 AS BIGINT), TRUE), (CAST(3 AS BIGINT), CAST(8 AS BIGINT), FALSE), (CAST(4 AS BIGINT), CAST(1 AS BIGINT), TRUE), (CAST(5 AS BIGINT), CAST(9 AS BIGINT), FALSE)) AS t(t, v, f) ORDER BY t ASC NULLS FIRST)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:Int, v:Int, f:Bool, cs:Int] rows=5
    - (1, 5, False, 5)
    - (2, 3, True, 3)
    - (3, 8, False, 11)
    - (4, 1, True, 1)
    - (5, 9, False, 10)
- DuckDB: cols=[t:Int, v:Int, f:Bool, cs:Int] rows=5
    - (2, 3, True, 3)
    - (3, 8, False, 11)
    - (1, 5, False, 5)
    - (4, 1, True, 1)
    - (5, 9, False, 10)

