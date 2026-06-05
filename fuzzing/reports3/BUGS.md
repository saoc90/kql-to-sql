# KQL→SQL Translator — Differential Fuzzing Findings

Oracle: Kustainer (real Kusto). SUT: KqlToSqlConverter → DuckDB.
Total verdicts: 1662. Bug candidates: 441.

## Counts by outcome

| Outcome | Count |
|---|---|
| Match | 1063 |
| MismatchRows | 330 |
| KustoError | 127 |
| SqlExecError | 97 |
| SkippedNondeterministic | 31 |
| MismatchOrder | 11 |
| MismatchColumns | 3 |

## Family: aggregation (33)

### `agent-aggregation-0000` — SqlExecError (highest)

*Detail:* Out of Range Error: STDDEV_SAMP is out of range!

**KQL**
```kql
datatable(k:long, v:real)[ 1,real(nan), 1,real(nan), 2,real(+inf), 2,real(-inf), 2,1.5 ] | summarize s=sum(v), mx=max(v), mn=min(v), av=avg(v), st=stdev(v), va=variance(v) by k | order by k asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT k, COALESCE(SUM(v), 0) AS s, MAX(v) AS mx, MIN(v) AS mn, COALESCE(AVG(v), 'nan'::DOUBLE) AS av, STDDEV_SAMP(v) AS st, VAR_SAMP(v) AS va FROM (VALUES (CAST(1 AS BIGINT), CAST(CAST('nan' AS DOUBLE) AS DOUBLE)), (CAST(1 AS BIGINT), CAST(CAST('nan' AS DOUBLE) AS DOUBLE)), (CAST(2 AS BIGINT), CAST(CAST('inf' AS DOUBLE) AS DOUBLE)), (CAST(2 AS BIGINT), CAST(CAST('-inf' AS DOUBLE) AS DOUBLE)), (CAST(2 AS BIGINT), CAST(1.5 AS DOUBLE))) AS t(k, v) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, s:Real, mx:Real, mn:Real, av:Real, st:Real, va:Real] rows=2
    - (1, NaN, NaN, NaN, NaN, NaN, NaN)
    - (2, NaN, Infinity, -Infinity, NaN, NaN, NaN)
- DuckDB: ERROR — Out of Range Error: STDDEV_SAMP is out of range!

### `agent-aggregation-0013` — SqlExecError (highest)

*Detail:* Binder Error: aggregate function calls cannot be nested

LINE 1: ... s) FILTER (WHERE s IS NOT NULL), []) AS ms, STRING_AGG(LIST(s) FILTER (WHERE s IS NOT NULL), '|') AS joined FROM...
                                                                   ^

**KQL**
```kql
datatable(k:long, s:string)[ 1,"a", 1,"", 1,"b", 2,"", 2,"" ] | summarize ml=make_list(s), ms=make_set(s), joined=strcat_array(make_list(s), "|") by k | order by k asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT k, COALESCE(LIST(s) FILTER (WHERE s IS NOT NULL), []) AS ml, COALESCE(LIST(DISTINCT s) FILTER (WHERE s IS NOT NULL), []) AS ms, STRING_AGG(LIST(s) FILTER (WHERE s IS NOT NULL), '|') AS joined FROM (VALUES (CAST(1 AS BIGINT), 'a'), (CAST(1 AS BIGINT), ''), (CAST(1 AS BIGINT), 'b'), (CAST(2 AS BIGINT), ''), (CAST(2 AS BIGINT), '')) AS t(k, s) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, ml:Dynamic, ms:Dynamic, joined:String] rows=2
    - (1, [
  "a",
  "",
  "b"
], [
  "a",
  "",
  "b"
], 'a||b')
    - (2, [
  "",
  ""
], [
  ""
], '|')
- DuckDB: ERROR — Binder Error: aggregate function calls cannot be nested

LINE 1: ... s) FILTER (WHERE s IS NOT NULL), []) AS ms, STRING_AGG(LIST(s) FILTER (WHERE s IS NOT NULL), '|') AS joined FROM...
                                                                   ^

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
SELECT COALESCE(LIST(d) FILTER (WHERE d IS NOT NULL), []) AS list_d FROM (VALUES ('{"a":1}'::JSON), ('{"b":2}'::JSON), (LIST_VALUE(1, 2, 3))) AS t(d)
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
SELECT k, histogram(v) AS bag_v FROM (VALUES (CAST(1 AS BIGINT), '{"a":1}'::JSON), (CAST(1 AS BIGINT), '{"b":2}'::JSON), (CAST(2 AS BIGINT), '{"c":3}'::JSON)) AS t(k, v) GROUP BY ALL
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
*Detail:* first differing row[0]: kusto=(1, NaN, NaN, NaN, 1, NaN) duck=(1, NaN, 1.5, NaN, 1, NaN)

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
    - (1, NaN, 1.5, NaN, 1, NaN)
    - (2, NaN, NaN, NaN, 2, NaN)

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
SELECT k, histogram(v) AS bag FROM (VALUES (CAST(1 AS BIGINT), '{"a":1}'::JSON), (CAST(1 AS BIGINT), '{"a":2,"b":3}'::JSON), (CAST(1 AS BIGINT), '{"c":4}'::JSON), (CAST(2 AS BIGINT), '{"a":9}'::JSON)) AS t(k, v) GROUP BY ALL
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
]) duck=(1, {"{\u0022x\u0022:1}":1,"{\u0022y\u0022:2}":1}, ["{\u0022x\u0022:1}","{\u0022y\u0022:2}"], ["{\u0022x\u0022:1}",null,"{\u0022y\u0022:2}"])

**KQL**
```kql
datatable(k:long, v:dynamic)[ 1,dynamic({"x":1}), 1,dynamic(null), 1,dynamic({"y":2}), 2,dynamic(null) ] | summarize make_bag(v), make_list(v), make_list_with_nulls(v) by k
```
**Generated SQL**
```sql
SELECT k, histogram(v) AS bag_v, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS list_v, COALESCE(LIST(v), []) AS list_v FROM (VALUES (CAST(1 AS BIGINT), '{"x":1}'::JSON), (CAST(1 AS BIGINT), NULL), (CAST(1 AS BIGINT), '{"y":2}'::JSON), (CAST(2 AS BIGINT), NULL)) AS t(k, v) GROUP BY ALL
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
    - (1, {"{\u0022x\u0022:1}":1,"{\u0022y\u0022:2}":1}, ["{\u0022x\u0022:1}","{\u0022y\u0022:2}"], ["{\u0022x\u0022:1}",null,"{\u0022y\u0022:2}"])
    - (2, null, [], [null])

### `agent-aggregation-0013` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(0, 0, 0, 0) duck=(null, null, 0, 0)

**KQL**
```kql
datatable(x:long)[ 5 ] | summarize stdev(x), variance(x), stdevp(x), variancep(x)
```
**Generated SQL**
```sql
SELECT STDDEV_SAMP(x) AS stdev_x, VAR_SAMP(x) AS variance_x, STDDEV_POP(x) AS stdevp_x, VAR_POP(x) AS variancep_x FROM (VALUES (CAST(5 AS BIGINT))) AS t(x)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[stdev_x:Real, variance_x:Real, stdevp_x:Real, variancep_x:Real] rows=1
    - (0, 0, 0, 0)
- DuckDB: cols=[stdev_x:Real, variance_x:Real, stdevp_x:Real, variancep_x:Real] rows=1
    - (null, null, 0, 0)

### `agent-aggregation-0014` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a', 1, 1, 7.5, 2.5) duck=('b', null, null, 10, 10)

**KQL**
```kql
datatable(g:string, x:real)[ "a",1.5, "a",2.5, "a",3.5, "b",10.0 ] | summarize stdev(x), variance(x), sum(x), avg(x) by g
```
**Generated SQL**
```sql
SELECT g, STDDEV_SAMP(x) AS stdev_x, VAR_SAMP(x) AS variance_x, COALESCE(SUM(x), 0) AS sum_x, COALESCE(AVG(x), 'nan'::DOUBLE) AS avg_x FROM (VALUES ('a', CAST(1.5 AS DOUBLE)), ('a', CAST(2.5 AS DOUBLE)), ('a', CAST(3.5 AS DOUBLE)), ('b', CAST(10.0 AS DOUBLE))) AS t(g, x) GROUP BY ALL
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, stdev_x:Real, variance_x:Real, sum_x:Real, avg_x:Real] rows=2
    - ('a', 1, 1, 7.5, 2.5)
    - ('b', 0, 0, 10, 10)
- DuckDB: cols=[g:String, stdev_x:Real, variance_x:Real, sum_x:Real, avg_x:Real] rows=2
    - ('b', null, null, 10, 10)
    - ('a', 1, 1, 7.5, 2.5)

### `agent-aggregation-0016` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2007-02-14T00:00:00.0000000Z, 3) duck=(2007-12-13T00:00:00.0000000Z, 7)

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2007-03-09),1, datetime(2007-03-11),2, datetime(2007-12-31),3, datetime(2008-01-01),4 ] | summarize sum(v) by bin(t, 30d)
```
**Generated SQL**
```sql
SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(t AS TIMESTAMP))/2592000000)*2592000000 AS BIGINT)) AS t, COALESCE(SUM(v), 0) AS sum_v FROM (VALUES (TIMESTAMP '2007-03-09 00:00:00', CAST(1 AS BIGINT)), (TIMESTAMP '2007-03-11 00:00:00', CAST(2 AS BIGINT)), (TIMESTAMP '2007-12-31 00:00:00', CAST(3 AS BIGINT)), (TIMESTAMP '2008-01-01 00:00:00', CAST(4 AS BIGINT))) AS t(t, v) GROUP BY ALL
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, sum_v:Int] rows=2
    - (2007-02-14T00:00:00.0000000Z, 3)
    - (2007-12-11T00:00:00.0000000Z, 7)
- DuckDB: cols=[t:DateTime, sum_v:Int] rows=2
    - (2007-12-13T00:00:00.0000000Z, 7)
    - (2007-02-16T00:00:00.0000000Z, 3)

### `agent-aggregation-0019` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(60, 30, [
  30,
  30
], 2) duck=(60, 30, [{"IsPowerOfTwo":false,"IsZero":false,"IsOne":false,"IsEven":true,"Sign":1},{"IsPowerOfTwo":false,"IsZero":false,"IsOne":false,"IsEven":true,"Sign":1}], 2)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30 ] | summarize total=sum(v) by k | summarize gtot=sum(total), gmax=max(total), glist=make_list(total), gcnt=count()
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(total), 0) AS gtot, MAX(total) AS gmax, COALESCE(LIST(total) FILTER (WHERE total IS NOT NULL), []) AS glist, COUNT(*) AS gcnt FROM (SELECT k, COALESCE(SUM(v), 0) AS total FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v) GROUP BY ALL)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[gtot:Int, gmax:Int, glist:Dynamic, gcnt:Int] rows=1
    - (60, 30, [
  30,
  30
], 2)
- DuckDB: cols=[gtot:Int, gmax:Int, glist:Unknown, gcnt:Int] rows=1
    - (60, 30, [{"IsPowerOfTwo":false,"IsZero":false,"IsOne":false,"IsEven":true,"Sign":1},{"IsPowerOfTwo":false,"IsZero":false,"IsOne":false,"IsEven":true,"Sign":1}], 2)

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
], 2) duck=(1, [1,2], 2)

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
    - (1, [1,2], 2)
    - (2, [1], 1)

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
SELECT histogram(json_object(TRY_CAST(k AS TEXT), c)) AS histo FROM (SELECT k, COUNT(*) AS c FROM (VALUES (CAST(1 AS BIGINT)), (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT))) AS t(k) GROUP BY ALL)
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

### `agent-aggregation-0003` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  60
], 1) duck=([{"IsPowerOfTwo":false,"IsZero":false,"IsOne":false,"IsEven":true,"Sign":1}], 1)

**KQL**
```kql
datatable(k:long, v:long)[ 1,5, 1,15, 2,25, 2,35, 3,45 ] | summarize cnt=count(), tot=sum(v) by k | where cnt >= 2 and tot > 30 | order by k asc | summarize meta=make_list(tot), groups=count()
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(tot) FILTER (WHERE tot IS NOT NULL), []) AS meta, COUNT(*) AS groups FROM (SELECT * FROM (SELECT k, COUNT(*) AS cnt, COALESCE(SUM(v), 0) AS tot FROM (VALUES (CAST(1 AS BIGINT), CAST(5 AS BIGINT)), (CAST(1 AS BIGINT), CAST(15 AS BIGINT)), (CAST(2 AS BIGINT), CAST(25 AS BIGINT)), (CAST(2 AS BIGINT), CAST(35 AS BIGINT)), (CAST(3 AS BIGINT), CAST(45 AS BIGINT))) AS t(k, v) GROUP BY ALL) WHERE cnt >= 2 AND tot > 30 ORDER BY k ASC NULLS FIRST)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[meta:Dynamic, groups:Int] rows=1
    - ([
  60
], 1)
- DuckDB: cols=[meta:Unknown, groups:Int] rows=1
    - ([{"IsPowerOfTwo":false,"IsZero":false,"IsOne":false,"IsEven":true,"Sign":1}], 1)

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
SELECT * FROM (SELECT g, histogram(v) AS bag, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS lst FROM (VALUES (CAST(1 AS BIGINT), '{"a":1,"b":2}'::JSON), (CAST(1 AS BIGINT), '{"a":10,"c":3}'::JSON), (CAST(1 AS BIGINT), '{"b":99}'::JSON), (CAST(2 AS BIGINT), '{"a":5}'::JSON)) AS t(g, v) GROUP BY ALL) ORDER BY g ASC NULLS FIRST
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

### `agent-aggregation-0014` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(2, 0, 0, 0, 0, 1) duck=(2, null, 0, null, 0, 1)

**KQL**
```kql
datatable(g:long, v:long)[ 1,1, 1,2, 1,3, 1,4, 2,10 ] | summarize sd=stdev(v), sdp=stdevp(v), va=variance(v), vap=variancep(v), n=count() by g | order by g asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT g, STDDEV_SAMP(v) AS sd, STDDEV_POP(v) AS sdp, VAR_SAMP(v) AS va, VAR_POP(v) AS vap, COUNT(*) AS n FROM (VALUES (CAST(1 AS BIGINT), CAST(1 AS BIGINT)), (CAST(1 AS BIGINT), CAST(2 AS BIGINT)), (CAST(1 AS BIGINT), CAST(3 AS BIGINT)), (CAST(1 AS BIGINT), CAST(4 AS BIGINT)), (CAST(2 AS BIGINT), CAST(10 AS BIGINT))) AS t(g, v) GROUP BY ALL) ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:Int, sd:Real, sdp:Real, va:Real, vap:Real, n:Int] rows=2
    - (1, 1.2909944487487155, 1.118033988749895, 1.6666666667, 1.25, 4)
    - (2, 0, 0, 0, 0, 1)
- DuckDB: cols=[g:Int, sd:Real, sdp:Real, va:Real, vap:Real, n:Int] rows=2
    - (1, 1.2909944487358056, 1.118033988749895, 1.6666666666666667, 1.25, 4)
    - (2, null, 0, null, 0, 1)

### `agent-aggregation-0017` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(0, 0, [], [], {}, NaN, 0) duck=(0, 0, [], [], null, NaN, null)

**KQL**
```kql
datatable(x:long)[ 100,200,300 ] | where x < 0 | summarize s=sum(x), c=count(), ml=make_list(x), ms=make_set(x), mb=make_bag(pack("k", x)), av=avg(x), st=stdev(x)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(x), 0) AS s, COUNT(*) AS c, COALESCE(LIST(x) FILTER (WHERE x IS NOT NULL), []) AS ml, COALESCE(LIST(DISTINCT x) FILTER (WHERE x IS NOT NULL), []) AS ms, histogram(json_object('k', x)) AS mb, COALESCE(AVG(x), 'nan'::DOUBLE) AS av, STDDEV_SAMP(x) AS st FROM (SELECT * FROM (VALUES (CAST(100 AS BIGINT)), (CAST(200 AS BIGINT)), (CAST(300 AS BIGINT))) AS t(x) WHERE x < 0)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, c:Int, ml:Dynamic, ms:Dynamic, mb:Dynamic, av:Real, st:Real] rows=1
    - (0, 0, [], [], {}, NaN, 0)
- DuckDB: cols=[s:Int, c:Int, ml:Unknown, ms:Unknown, mb:Unknown, av:Real, st:Real] rows=1
    - (0, 0, [], [], null, NaN, null)

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
], 6, 3) duck=([[1,2,3],[1],[1,2]], 6, 3)

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
    - ([[1,2,3],[1],[1,2]], 6, 3)

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
]) duck=([1,2,3,4,5,6], [5,4,1,3,6,2])

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
    - ([1,2,3,4,5,6], [5,4,1,3,6,2])

### `agent-aggregation-0038` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(2, 0, 0, 10000, 1, 10000) duck=(2, null, null, 10000, 1, 10000)

**KQL**
```kql
datatable(k:long, x:real)[ 1,1.0, 1,2.0, 1,3.0, 1,4.0, 2,100.0 ] | summarize var_check=variance(x), stdev_check=stdev(x), sumsq=sum(x*x), n=count(), avgsq=avg(x*x) by k | order by k asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT k, VAR_SAMP(x) AS var_check, STDDEV_SAMP(x) AS stdev_check, COALESCE(SUM(x * x), 0) AS sumsq, COUNT(*) AS n, COALESCE(AVG(x * x), 'nan'::DOUBLE) AS avgsq FROM (VALUES (CAST(1 AS BIGINT), CAST(1.0 AS DOUBLE)), (CAST(1 AS BIGINT), CAST(2.0 AS DOUBLE)), (CAST(1 AS BIGINT), CAST(3.0 AS DOUBLE)), (CAST(1 AS BIGINT), CAST(4.0 AS DOUBLE)), (CAST(2 AS BIGINT), CAST(100.0 AS DOUBLE))) AS t(k, x) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, var_check:Real, stdev_check:Real, sumsq:Real, n:Int, avgsq:Real] rows=2
    - (1, 1.6666666667, 1.2909944487487155, 30, 4, 7.5)
    - (2, 0, 0, 10000, 1, 10000)
- DuckDB: cols=[k:Int, var_check:Real, stdev_check:Real, sumsq:Real, n:Int, avgsq:Real] rows=2
    - (1, 1.6666666666666667, 1.2909944487358056, 30, 4, 7.5)
    - (2, null, null, 10000, 1, 10000)

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
]) duck=('p', [[1,2],[3,4]], [[1,2],[3,4]])

**KQL**
```kql
datatable(g:string, v:dynamic)[ "p",dynamic([1,2]), "p",dynamic([3,4]), "q",dynamic(null), "q",dynamic([5]) ] | summarize ml=make_list(v), mln=make_list_with_nulls(v) by g | order by g asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT g, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS ml, COALESCE(LIST(v), []) AS mln FROM (VALUES ('p', LIST_VALUE(1, 2)), ('p', LIST_VALUE(3, 4)), ('q', NULL), ('q', LIST_VALUE(5))) AS t(g, v) GROUP BY ALL) ORDER BY g ASC NULLS FIRST
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
    - ('p', [[1,2],[3,4]], [[1,2],[3,4]])
    - ('q', [[5]], [null,[5]])

### `agent-aggregation-0042` — MismatchRows (high)

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
]) duck=({"{\u00221\u0022:2}":1,"{\u00222\u0022:1}":1,"{\u00223\u0022:2}":1}, ["{\u0022g\u0022:2,\u0022c\u0022:1}","{\u0022g\u0022:3,\u0022c\u0022:2}","{\u0022g\u0022:1,\u0022c\u0022:2}"])

**KQL**
```kql
datatable(g:long, v:long)[ 1,10, 1,20, 2,30, 3,40, 3,50 ] | summarize cnt=count() by g | summarize bag=make_bag(pack(tostring(g), cnt)), histlist=make_list(pack("g", g, "c", cnt))
```
**Generated SQL**
```sql
SELECT histogram(json_object(TRY_CAST(g AS TEXT), cnt)) AS bag, COALESCE(LIST(json_object('g', g, 'c', cnt)) FILTER (WHERE json_object('g', g, 'c', cnt) IS NOT NULL), []) AS histlist FROM (SELECT g, COUNT(*) AS cnt FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(3 AS BIGINT), CAST(40 AS BIGINT)), (CAST(3 AS BIGINT), CAST(50 AS BIGINT))) AS t(g, v) GROUP BY ALL)
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
- DuckDB: cols=[bag:Unknown, histlist:Unknown] rows=1
    - ({"{\u00221\u0022:2}":1,"{\u00222\u0022:1}":1,"{\u00223\u0022:2}":1}, ["{\u0022g\u0022:2,\u0022c\u0022:1}","{\u0022g\u0022:3,\u0022c\u0022:2}","{\u0022g\u0022:1,\u0022c\u0022:2}"])

## Family: datetime-timespan (49)

### `agent-datetime-timespan-0036` — SqlExecError (highest)

*Detail:* Conversion Error: Could not convert string 'time(1.02:03:04.5670000)' to INTERVAL

LINE 1: SELECT (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04.5670000)')) * 1000000) < 0 THEN '-' ELSE...
                                                        ^

**KQL**
```kql
print ft = format_timespan(time(1.02:03:04.5670000), 'dd.hh:mm:ss.fffffff')
```
**Generated SQL**
```sql
SELECT (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04.5670000)')) * 1000000) < 0 THEN '-' ELSE '' END || LPAD(CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04.5670000)')) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR), 2, '0') || '.' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04.5670000)')) * 1000000)), 86400000000) / 3600000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04.5670000)')) * 1000000)), 3600000000) / 60000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04.5670000)')) * 1000000)), 60000000) / 1000000) AS BIGINT) AS VARCHAR), 2, '0') || '.' || (LPAD(CAST(CAST(FLOOR(CAST(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04.5670000)')) * 1000000)), 1000000) AS BIGINT) / 1) AS BIGINT) AS VARCHAR), 6, '0') || '0')) AS ft
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ft:String] rows=1
    - ('01.02:03:04.5670000')
- DuckDB: ERROR — Conversion Error: Could not convert string 'time(1.02:03:04.5670000)' to INTERVAL

LINE 1: SELECT (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04.5670000)')) * 1000000) < 0 THEN '-' ELSE...
                                                        ^

### `agent-datetime-timespan-0042` — SqlExecError (highest)

*Detail:* Conversion Error: Type DOUBLE with value -nan can't be cast because the value is out of range for the destination type INT64

LINE 1: SELECT *, EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(t AS TIMESTAMP))/EPOCH_MS(CAST...
                           ^

**KQL**
```kql
datatable(t:datetime, span:timespan)[ datetime(1900-01-01),1d, datetime(2999-12-31 23:59:59),2h, datetime(1969-12-31 23:59:59.999),1tick ] | extend b = bin(t, span), added = t + span, dow = dayofweek(t)
```
**Generated SQL**
```sql
SELECT *, EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(t AS TIMESTAMP))/EPOCH_MS(CAST(TIMESTAMP 'epoch' + (span) AS TIMESTAMP)))*EPOCH_MS(CAST(TIMESTAMP 'epoch' + (span) AS TIMESTAMP)) AS BIGINT)) AS b, t + span AS added, CASE WHEN EXTRACT(DOW FROM t) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END AS dow FROM (VALUES (TIMESTAMP '1900-01-01 00:00:00', (86400000 * INTERVAL '1 millisecond')), (TIMESTAMP '2999-12-31 23:59:59', (7200000 * INTERVAL '1 millisecond')), (TIMESTAMP '1969-12-31 23:59:59.999000', ((1 / 10.0) * INTERVAL '1 microsecond'))) AS t(t, span)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, span:TimeSpan, b:DateTime, added:DateTime, dow:TimeSpan] rows=3
    - (1900-01-01T00:00:00.0000000Z, 1.00:00:00, 1900-01-01T00:00:00.0000000Z, 1900-01-02T00:00:00.0000000Z, 1.00:00:00)
    - (2999-12-31T23:59:59.0000000Z, 02:00:00, 2999-12-31T22:00:00.0000000Z, 3000-01-01T01:59:59.0000000Z, 2.00:00:00)
    - (1969-12-31T23:59:59.9990000Z, 00:00:00.0000001, 1969-12-31T23:59:59.9990000Z, 1969-12-31T23:59:59.9990001Z, 3.00:00:00)
- DuckDB: ERROR — Conversion Error: Type DOUBLE with value -nan can't be cast because the value is out of range for the destination type INT64

LINE 1: SELECT *, EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(t AS TIMESTAMP))/EPOCH_MS(CAST...
                           ^

### `agent-datetime-timespan-0044` — SqlExecError (highest)

*Detail:* Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ... AS b6h FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-01-01 00:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

**KQL**
```kql
range t from datetime(2020-01-01) to datetime(2020-01-04) step 1d | extend sow = startofweek(t), wd = dayofweek(t), b6h = bin(t, 6h)
```
**Generated SQL**
```sql
SELECT *, (DATE_TRUNC('week', t + INTERVAL '1 day') - INTERVAL '1 day') AS sow, CASE WHEN EXTRACT(DOW FROM t) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END AS wd, EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(t AS TIMESTAMP))/21600000)*21600000 AS BIGINT)) AS b6h FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-01-01 00:00:00' AS BIGINT), CAST(TIMESTAMP '2020-01-04 00:00:00' AS BIGINT), CAST((86400000 * INTERVAL '1 millisecond') AS BIGINT)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, sow:DateTime, wd:TimeSpan, b6h:DateTime] rows=4
    - (2020-01-01T00:00:00.0000000Z, 2019-12-29T00:00:00.0000000Z, 3.00:00:00, 2020-01-01T00:00:00.0000000Z)
    - (2020-01-02T00:00:00.0000000Z, 2019-12-29T00:00:00.0000000Z, 4.00:00:00, 2020-01-02T00:00:00.0000000Z)
    - (2020-01-03T00:00:00.0000000Z, 2019-12-29T00:00:00.0000000Z, 5.00:00:00, 2020-01-03T00:00:00.0000000Z)
    - (2020-01-04T00:00:00.0000000Z, 2019-12-29T00:00:00.0000000Z, 6.00:00:00, 2020-01-04T00:00:00.0000000Z)
- DuckDB: ERROR — Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ... AS b6h FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-01-01 00:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

### `agent-datetime-timespan-0002` — SqlExecError (highest)

*Detail:* Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ... AS woy FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2021-12-26 00:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

**KQL**
```kql
range t from datetime(2021-12-26) to datetime(2022-01-02) step 1d | extend sow = startofweek(t), eow = endofweek(t), wd = dayofweek(t) / 1d, woy = week_of_year(t)
```
**Generated SQL**
```sql
SELECT *, (DATE_TRUNC('week', t + INTERVAL '1 day') - INTERVAL '1 day') AS sow, (DATE_TRUNC('week', t + INTERVAL '1 day') - INTERVAL '1 day') + INTERVAL '7 days' - INTERVAL '1 microsecond' AS eow, CASE WHEN EXTRACT(DOW FROM t) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS wd, EXTRACT(WEEK FROM t) AS woy FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2021-12-26 00:00:00' AS BIGINT), CAST(TIMESTAMP '2022-01-02 00:00:00' AS BIGINT), CAST((86400000 * INTERVAL '1 millisecond') AS BIGINT)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, sow:DateTime, eow:DateTime, wd:Real, woy:Int] rows=8
    - (2021-12-26T00:00:00.0000000Z, 2021-12-26T00:00:00.0000000Z, 2022-01-01T23:59:59.9999999Z, 0, 51)
    - (2021-12-27T00:00:00.0000000Z, 2021-12-26T00:00:00.0000000Z, 2022-01-01T23:59:59.9999999Z, 1, 52)
    - (2021-12-28T00:00:00.0000000Z, 2021-12-26T00:00:00.0000000Z, 2022-01-01T23:59:59.9999999Z, 2, 52)
    - (2021-12-29T00:00:00.0000000Z, 2021-12-26T00:00:00.0000000Z, 2022-01-01T23:59:59.9999999Z, 3, 52)
    - (2021-12-30T00:00:00.0000000Z, 2021-12-26T00:00:00.0000000Z, 2022-01-01T23:59:59.9999999Z, 4, 52)
    - (2021-12-31T00:00:00.0000000Z, 2021-12-26T00:00:00.0000000Z, 2022-01-01T23:59:59.9999999Z, 5, 52)
    - (2022-01-01T00:00:00.0000000Z, 2021-12-26T00:00:00.0000000Z, 2022-01-01T23:59:59.9999999Z, 6, 52)
    - (2022-01-02T00:00:00.0000000Z, 2022-01-02T00:00:00.0000000Z, 2022-01-08T23:59:59.9999999Z, 0, 52)
- DuckDB: ERROR — Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ... AS woy FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2021-12-26 00:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

### `agent-datetime-timespan-0018` — SqlExecError (highest)

*Detail:* Conversion Error: Could not convert string 'time(0.00:00:00.0000001)' to INTERVAL

LINE 1: ...), 3, '0')) AS ft, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(0.00:00:00.0000001)')) * 1000000) < 0 THEN '-' ELSE...
                                                                       ^

**KQL**
```kql
print ft = format_timespan(1d + 2h + 3m + 4s + 5ms, 'd:hh:mm:ss.fff'), ft2 = format_timespan(time(0.00:00:00.0000001), 'fffffff')
```
**Generated SQL**
```sql
SELECT (CASE WHEN (EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000) < 0 THEN '-' ELSE '' END || CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR) || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000)), 86400000000) / 3600000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000)), 3600000000) / 60000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000)), 60000000) / 1000000) AS BIGINT) AS VARCHAR), 2, '0') || '.' || LPAD(CAST(CAST(FLOOR(CAST(MOD(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') + (180000 * INTERVAL '1 millisecond') + (4000 * INTERVAL '1 millisecond') + (5 * INTERVAL '1 millisecond'))) * 1000000)), 1000000) AS BIGINT) / 1000) AS BIGINT) AS VARCHAR), 3, '0')) AS ft, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(0.00:00:00.0000001)')) * 1000000) < 0 THEN '-' ELSE '' END || (LPAD(CAST(CAST(FLOOR(CAST(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(0.00:00:00.0000001)')) * 1000000)), 1000000) AS BIGINT) / 1) AS BIGINT) AS VARCHAR), 6, '0') || '0')) AS ft2
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ft:String, ft2:String] rows=1
    - ('1:02:03:04.005', '0000001')
- DuckDB: ERROR — Conversion Error: Could not convert string 'time(0.00:00:00.0000001)' to INTERVAL

LINE 1: ...), 3, '0')) AS ft, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(0.00:00:00.0000001)')) * 1000000) < 0 THEN '-' ELSE...
                                                                       ^

### `agent-datetime-timespan-0026` — SqlExecError (highest)

*Detail:* Conversion Error: Date out of range: 2020-13-1

**KQL**
```kql
print mk = make_datetime(2020, 2, 29, 13, 45, 9), mk2 = make_datetime(2020, 13, 1), mk3 = make_timespan(1, 2, 3, 4)
```
**Generated SQL**
```sql
SELECT MAKE_TIMESTAMP(2020, 2, 29, 13, 45, 9) AS mk, MAKE_TIMESTAMP(2020, 13, 1, 0, 0, 0) AS mk2, (1 * INTERVAL '1 day' + 2 * INTERVAL '1 hour' + 3 * INTERVAL '1 minute' + 4 * INTERVAL '1 second') AS mk3
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[mk:DateTime, mk2:DateTime, mk3:TimeSpan] rows=1
    - (2020-02-29T13:45:09.0000000Z, null, 1.02:03:04)
- DuckDB: ERROR — Conversion Error: Date out of range: 2020-13-1

### `agent-datetime-timespan-0028` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '/(VARCHAR, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	/(FLOAT, FLOAT) -> FLOAT
	/(DOUBLE, DOUBLE) -> DOUBLE
	/(INTERVAL, DOUBLE) -> INTERVAL


LINE 1: ...' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow, DATE_TRUNC...
                                                                          ^

**KQL**
```kql
datatable(t:datetime)[ datetime(1601-01-01 00:00:00), datetime(0001-01-01 00:00:00.0000001), datetime(9999-12-31 23:59:59.9999999) ] | extend y = getyear(t), woy = week_of_year(t), dow = dayofweek(t) / 1d, eom = endofmonth(t)
```
**Generated SQL**
```sql
SELECT *, EXTRACT(YEAR FROM t) AS y, EXTRACT(WEEK FROM t) AS woy, CASE WHEN EXTRACT(DOW FROM t) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow, DATE_TRUNC('month', t) + INTERVAL '1 month' - INTERVAL '1 microsecond' AS eom FROM (VALUES (TIMESTAMP '1601-01-01 00:00:00'), (TIMESTAMP '0001-01-01 00:00:00.000000'), (TIMESTAMP '9999-12-31 23:59:59.999999')) AS t(t)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, y:Int, woy:Int, dow:Real, eom:DateTime] rows=3
    - (1601-01-01T00:00:00.0000000Z, 1601, 1, 1, 1601-01-31T23:59:59.9999999Z)
    - (0001-01-01T00:00:00.0000001Z, 1, 1, 1, 0001-01-31T23:59:59.9999999Z)
    - (9999-12-31T23:59:59.9999999Z, 9999, 52, null, 9999-12-31T23:59:59.9999999Z)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '/(VARCHAR, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	/(FLOAT, FLOAT) -> FLOAT
	/(DOUBLE, DOUBLE) -> DOUBLE
	/(INTERVAL, DOUBLE) -> INTERVAL


LINE 1: ...' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow, DATE_TRUNC...
                                                                          ^

### `agent-datetime-timespan-0031` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types 'date_part(STRING_LITERAL, VARCHAR)'. You might need to add explicit type casts.
	Candidate functions:
	date_part(VARCHAR[], DATE) -> STRUCT()
	date_part(VARCHAR[], INTERVAL) -> STRUCT()
	date_part(VARCHAR[], TIME) -> STRUCT()
	date_part(VARCHAR[], TIMESTAMP) -> STRUCT()
	date_part(VARCHAR[], TIME WITH TIME ZONE) -> STRUCT()
	date_part(VARCHAR[], TIME_NS) -> STRUCT()
	date_part(VARCHAR, DATE) -> BIGINT
	date_part(VARCHAR, INTERVAL) -> BIGINT
	date_part(VARCHAR, TIME) -> BIGINT
	date_part(VARCHAR, TIMESTAMP) -> BIGINT
	date_part(VARCHAR, TIME WITH TIME ZONE) -> BIGINT
	date_part(VARCHAR, TIME_NS) -> BIGINT
	date_part(VARCHAR[], TIMESTAMP WITH TIME ZONE) -> STRUCT()
	date_part(VARCHAR, TIMESTAMP WITH TIME ZONE) -> BIGINT


LINE 1: ...isecond') + INTERVAL '1 day') - INTERVAL '1 day') AS sow, (EXTRACT(EPOCH FROM (CASE WHEN EXTRACT(DOW FROM TIMESTAMP...
                                                                      ^

**KQL**
```kql
range i from 0 to 6 step 1 | extend t = datetime(2023-01-01) + i * 1d, sow = startofweek(datetime(2023-01-01) + i * 1d), wd = dayofweek(datetime(2023-01-01) + i * 1d) / 1d
```
**Generated SQL**
```sql
SELECT *, TIMESTAMP '2023-01-01 00:00:00' + i * (86400000 * INTERVAL '1 millisecond') AS t, (DATE_TRUNC('week', TIMESTAMP '2023-01-01 00:00:00' + i * (86400000 * INTERVAL '1 millisecond') + INTERVAL '1 day') - INTERVAL '1 day') AS sow, (EXTRACT(EPOCH FROM (CASE WHEN EXTRACT(DOW FROM TIMESTAMP '2023-01-01 00:00:00' + i * (86400000 * INTERVAL '1 millisecond')) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM TIMESTAMP '2023-01-01 00:00:00' + i * (86400000 * INTERVAL '1 millisecond')) AS VARCHAR) || '.00:00:00' END)) / EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond')))) AS wd FROM (SELECT generate_series AS i FROM generate_series(CAST(0 AS BIGINT), CAST(6 AS BIGINT), CAST(1 AS BIGINT)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[i:Int, t:DateTime, sow:DateTime, wd:Real] rows=7
    - (0, 2023-01-01T00:00:00.0000000Z, 2023-01-01T00:00:00.0000000Z, 0)
    - (1, 2023-01-02T00:00:00.0000000Z, 2023-01-01T00:00:00.0000000Z, 1)
    - (2, 2023-01-03T00:00:00.0000000Z, 2023-01-01T00:00:00.0000000Z, 2)
    - (3, 2023-01-04T00:00:00.0000000Z, 2023-01-01T00:00:00.0000000Z, 3)
    - (4, 2023-01-05T00:00:00.0000000Z, 2023-01-01T00:00:00.0000000Z, 4)
    - (5, 2023-01-06T00:00:00.0000000Z, 2023-01-01T00:00:00.0000000Z, 5)
    - (6, 2023-01-07T00:00:00.0000000Z, 2023-01-01T00:00:00.0000000Z, 6)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types 'date_part(STRING_LITERAL, VARCHAR)'. You might need to add explicit type casts.
	Candidate functions:
	date_part(VARCHAR[], DATE) -> STRUCT()
	date_part(VARCHAR[], INTERVAL) -> STRUCT()
	date_part(VARCHAR[], TIME) -> STRUCT()
	date_part(VARCHAR[], TIMESTAMP) -> STRUCT()
	date_part(VARCHAR[], TIME WITH TIME ZONE) -> STRUCT()
	date_part(VARCHAR[], TIME_NS) -> STRUCT()
	date_part(VARCHAR, DATE) -> BIGINT
	date_part(VARCHAR, INTERVAL) -> BIGINT
	date_part(VARCHAR, TIME) -> BIGINT
	date_part(VARCHAR, TIMESTAMP) -> BIGINT
	date_part(VARCHAR, TIME WITH TIME ZONE) -> BIGINT
	date_part(VARCHAR, TIME_NS) -> BIGINT
	date_part(VARCHAR[], TIMESTAMP WITH TIME ZONE) -> STRUCT()
	date_part(VARCHAR, TIMESTAMP WITH TIME ZONE) -> BIGINT


LINE 1: ...isecond') + INTERVAL '1 day') - INTERVAL '1 day') AS sow, (EXTRACT(EPOCH FROM (CASE WHEN EXTRACT(DOW FROM TIMESTAMP...
                                                                      ^

### `agent-datetime-timespan-0048` — SqlExecError (highest)

*Detail:* Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ... AS eom FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-02-28 22:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

**KQL**
```kql
range t from datetime(2020-02-28 22:00:00) to datetime(2020-03-01 02:00:00) step 1h | extend b1d = bin(t, 1d), bm = monthofyear(t), eom = endofmonth(t)
```
**Generated SQL**
```sql
SELECT *, EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(t AS TIMESTAMP))/86400000)*86400000 AS BIGINT)) AS b1d, EXTRACT(MONTH FROM t) AS bm, DATE_TRUNC('month', t) + INTERVAL '1 month' - INTERVAL '1 microsecond' AS eom FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-02-28 22:00:00' AS BIGINT), CAST(TIMESTAMP '2020-03-01 02:00:00' AS BIGINT), CAST((3600000 * INTERVAL '1 millisecond') AS BIGINT)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, b1d:DateTime, bm:Int, eom:DateTime] rows=29
    - (2020-02-28T22:00:00.0000000Z, 2020-02-28T00:00:00.0000000Z, 2, 2020-02-29T23:59:59.9999999Z)
    - (2020-02-28T23:00:00.0000000Z, 2020-02-28T00:00:00.0000000Z, 2, 2020-02-29T23:59:59.9999999Z)
    - (2020-02-29T00:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2, 2020-02-29T23:59:59.9999999Z)
    - (2020-02-29T01:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2, 2020-02-29T23:59:59.9999999Z)
    - (2020-02-29T02:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2, 2020-02-29T23:59:59.9999999Z)
    - (2020-02-29T03:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2, 2020-02-29T23:59:59.9999999Z)
    - (2020-02-29T04:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2, 2020-02-29T23:59:59.9999999Z)
    - (2020-02-29T05:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2, 2020-02-29T23:59:59.9999999Z)
- DuckDB: ERROR — Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ... AS eom FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-02-28 22:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

### `agent-datetime-timespan-0002` — SqlExecError (highest)

*Detail:* Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ... AS dow FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2023-12-29 00:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

**KQL**
```kql
range t from datetime(2023-12-29) to datetime(2024-01-04) step 1d | extend sow = startofweek(t), eow = endofweek(t), som = startofmonth(t), soy = startofyear(t), woy = week_of_year(t), dow = dayofweek(t) / 1d
```
**Generated SQL**
```sql
SELECT *, (DATE_TRUNC('week', t + INTERVAL '1 day') - INTERVAL '1 day') AS sow, (DATE_TRUNC('week', t + INTERVAL '1 day') - INTERVAL '1 day') + INTERVAL '7 days' - INTERVAL '1 microsecond' AS eow, DATE_TRUNC('month', t) AS som, DATE_TRUNC('year', t) AS soy, EXTRACT(WEEK FROM t) AS woy, CASE WHEN EXTRACT(DOW FROM t) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2023-12-29 00:00:00' AS BIGINT), CAST(TIMESTAMP '2024-01-04 00:00:00' AS BIGINT), CAST((86400000 * INTERVAL '1 millisecond') AS BIGINT)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, sow:DateTime, eow:DateTime, som:DateTime, soy:DateTime, woy:Int, dow:Real] rows=7
    - (2023-12-29T00:00:00.0000000Z, 2023-12-24T00:00:00.0000000Z, 2023-12-30T23:59:59.9999999Z, 2023-12-01T00:00:00.0000000Z, 2023-01-01T00:00:00.0000000Z, 52, 5)
    - (2023-12-30T00:00:00.0000000Z, 2023-12-24T00:00:00.0000000Z, 2023-12-30T23:59:59.9999999Z, 2023-12-01T00:00:00.0000000Z, 2023-01-01T00:00:00.0000000Z, 52, 6)
    - (2023-12-31T00:00:00.0000000Z, 2023-12-31T00:00:00.0000000Z, 2024-01-06T23:59:59.9999999Z, 2023-12-01T00:00:00.0000000Z, 2023-01-01T00:00:00.0000000Z, 52, 0)
    - (2024-01-01T00:00:00.0000000Z, 2023-12-31T00:00:00.0000000Z, 2024-01-06T23:59:59.9999999Z, 2024-01-01T00:00:00.0000000Z, 2024-01-01T00:00:00.0000000Z, 1, 1)
    - (2024-01-02T00:00:00.0000000Z, 2023-12-31T00:00:00.0000000Z, 2024-01-06T23:59:59.9999999Z, 2024-01-01T00:00:00.0000000Z, 2024-01-01T00:00:00.0000000Z, 1, 2)
    - (2024-01-03T00:00:00.0000000Z, 2023-12-31T00:00:00.0000000Z, 2024-01-06T23:59:59.9999999Z, 2024-01-01T00:00:00.0000000Z, 2024-01-01T00:00:00.0000000Z, 1, 3)
    - (2024-01-04T00:00:00.0000000Z, 2023-12-31T00:00:00.0000000Z, 2024-01-06T23:59:59.9999999Z, 2024-01-01T00:00:00.0000000Z, 2024-01-01T00:00:00.0000000Z, 1, 4)
- DuckDB: ERROR — Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ... AS dow FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2023-12-29 00:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

### `agent-datetime-timespan-0004` — SqlExecError (highest)

*Detail:* Conversion Error: extract specifier "nanosecond" not recognized

**KQL**
```kql
print d1 = datetime_diff('microsecond', datetime(2020-01-01 00:00:00.0000020), datetime(2020-01-01 00:00:00.0000005)), d2 = datetime_diff('nanosecond', datetime(2020-01-01 00:00:00.0000002), datetime(2020-01-01 00:00:00))
```
**Generated SQL**
```sql
SELECT DATE_DIFF('microsecond', TIMESTAMP '2020-01-01 00:00:00.000000', TIMESTAMP '2020-01-01 00:00:00.000002') AS d1, DATE_DIFF('nanosecond', TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:00.000000') AS d2
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d1:Int, d2:Int] rows=1
    - (2, 200)
- DuckDB: ERROR — Conversion Error: extract specifier "nanosecond" not recognized

### `agent-datetime-timespan-0010` — SqlExecError (highest)

*Detail:* Conversion Error: Could not convert string 'time(10675199.02:48:05.4775807)' to INTERVAL

LINE 1: ...), 2, '0')) AS ft2, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(10675199.02:48:05.4775807)')) * 1000000) < 0 THEN...
                                                                        ^

**KQL**
```kql
print ft1 = format_timespan(90061123ms, 'd:h:m:s.fff'), ft2 = format_timespan(90061123ms, 'dd:hh:mm:ss'), ft3 = format_timespan(time(10675199.02:48:05.4775807), 'd.hh:mm:ss')
```
**Generated SQL**
```sql
SELECT (CASE WHEN (EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000) < 0 THEN '-' ELSE '' END || CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR) || ':' || CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000)), 86400000000) / 3600000000) AS BIGINT) AS VARCHAR) || ':' || CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000)), 3600000000) / 60000000) AS BIGINT) AS VARCHAR) || ':' || CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000)), 60000000) / 1000000) AS BIGINT) AS VARCHAR) || '.' || LPAD(CAST(CAST(FLOOR(CAST(MOD(ABS((EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000)), 1000000) AS BIGINT) / 1000) AS BIGINT) AS VARCHAR), 3, '0')) AS ft1, (CASE WHEN (EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000) < 0 THEN '-' ELSE '' END || LPAD(CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000)), 86400000000) / 3600000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000)), 3600000000) / 60000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM ((90061123 * INTERVAL '1 millisecond'))) * 1000000)), 60000000) / 1000000) AS BIGINT) AS VARCHAR), 2, '0')) AS ft2, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(10675199.02:48:05.4775807)')) * 1000000) < 0 THEN '-' ELSE '' END || CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(10675199.02:48:05.4775807)')) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR) || '.' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(10675199.02:48:05.4775807)')) * 1000000)), 86400000000) / 3600000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(10675199.02:48:05.4775807)')) * 1000000)), 3600000000) / 60000000) AS BIGINT) AS VARCHAR), 2, '0') || ':' || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(10675199.02:48:05.4775807)')) * 1000000)), 60000000) / 1000000) AS BIGINT) AS VARCHAR), 2, '0')) AS ft3
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ft1:String, ft2:String, ft3:String] rows=1
    - ('1:1:1:1.123', '01:01:01:01', '10675199.02:48:05')
- DuckDB: ERROR — Conversion Error: Could not convert string 'time(10675199.02:48:05.4775807)' to INTERVAL

LINE 1: ...), 2, '0')) AS ft2, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(10675199.02:48:05.4775807)')) * 1000000) < 0 THEN...
                                                                        ^

### `agent-datetime-timespan-0013` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '/(VARCHAR, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	/(FLOAT, FLOAT) -> FLOAT
	/(DOUBLE, DOUBLE) -> DOUBLE
	/(INTERVAL, DOUBLE) -> INTERVAL


LINE 1: ...IMESTAMP(y, m, d, 0, 0, 0)) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow FROM (VALUES...
                                                                      ^

**KQL**
```kql
datatable(y:int, m:int, d:int)[ 2020,2,29, 2021,2,28, 2000,1,1, 1999,12,31 ] | extend dt = make_datetime(y, m, d), eom = endofmonth(make_datetime(y, m, d)), woy = week_of_year(make_datetime(y, m, d)), dow = dayofweek(make_datetime(y, m, d)) / 1d
```
**Generated SQL**
```sql
SELECT *, MAKE_TIMESTAMP(y, m, d, 0, 0, 0) AS dt, DATE_TRUNC('month', MAKE_TIMESTAMP(y, m, d, 0, 0, 0)) + INTERVAL '1 month' - INTERVAL '1 microsecond' AS eom, EXTRACT(WEEK FROM MAKE_TIMESTAMP(y, m, d, 0, 0, 0)) AS woy, CASE WHEN EXTRACT(DOW FROM MAKE_TIMESTAMP(y, m, d, 0, 0, 0)) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM MAKE_TIMESTAMP(y, m, d, 0, 0, 0)) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow FROM (VALUES (2020, 2, 29), (2021, 2, 28), (2000, 1, 1), (1999, 12, 31)) AS t(y, m, d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[y:Int, m:Int, d:Int, dt:DateTime, eom:DateTime, woy:Int, dow:Real] rows=4
    - (2020, 2, 29, 2020-02-29T00:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 9, 6)
    - (2021, 2, 28, 2021-02-28T00:00:00.0000000Z, 2021-02-28T23:59:59.9999999Z, 8, 0)
    - (2000, 1, 1, 2000-01-01T00:00:00.0000000Z, 2000-01-31T23:59:59.9999999Z, 52, 6)
    - (1999, 12, 31, 1999-12-31T00:00:00.0000000Z, 1999-12-31T23:59:59.9999999Z, 52, 5)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '/(VARCHAR, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	/(FLOAT, FLOAT) -> FLOAT
	/(DOUBLE, DOUBLE) -> DOUBLE
	/(INTERVAL, DOUBLE) -> INTERVAL


LINE 1: ...IMESTAMP(y, m, d, 0, 0, 0)) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow FROM (VALUES...
                                                                      ^

### `agent-datetime-timespan-0016` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '/(VARCHAR, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	/(FLOAT, FLOAT) -> FLOAT
	/(DOUBLE, DOUBLE) -> DOUBLE
	/(INTERVAL, DOUBLE) -> INTERVAL


LINE 1: ...' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow FROM (VALUES...
                                                                          ^

**KQL**
```kql
datatable(t:datetime)[ datetime(0100-02-28), datetime(0200-02-28), datetime(0400-02-29), datetime(1900-02-28), datetime(2000-02-29) ] | extend eom = endofmonth(t), dom = dayofmonth(endofmonth(t)), dow = dayofweek(t) / 1d
```
**Generated SQL**
```sql
SELECT *, DATE_TRUNC('month', t) + INTERVAL '1 month' - INTERVAL '1 microsecond' AS eom, EXTRACT(DAY FROM DATE_TRUNC('month', t) + INTERVAL '1 month' - INTERVAL '1 microsecond') AS dom, CASE WHEN EXTRACT(DOW FROM t) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow FROM (VALUES (TIMESTAMP '0100-02-28 00:00:00'), (TIMESTAMP '0200-02-28 00:00:00'), (TIMESTAMP '0400-02-29 00:00:00'), (TIMESTAMP '1900-02-28 00:00:00'), (TIMESTAMP '2000-02-29 00:00:00')) AS t(t)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, eom:DateTime, dom:Int, dow:Real] rows=5
    - (0100-02-28T00:00:00.0000000Z, 0100-02-28T23:59:59.9999999Z, 28, 0)
    - (0200-02-28T00:00:00.0000000Z, 0200-02-28T23:59:59.9999999Z, 28, 5)
    - (0400-02-29T00:00:00.0000000Z, 0400-02-29T23:59:59.9999999Z, 29, 2)
    - (1900-02-28T00:00:00.0000000Z, 1900-02-28T23:59:59.9999999Z, 28, 3)
    - (2000-02-29T00:00:00.0000000Z, 2000-02-29T23:59:59.9999999Z, 29, 2)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '/(VARCHAR, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	/(FLOAT, FLOAT) -> FLOAT
	/(DOUBLE, DOUBLE) -> DOUBLE
	/(INTERVAL, DOUBLE) -> INTERVAL


LINE 1: ...' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow FROM (VALUES...
                                                                          ^

### `agent-datetime-timespan-0017` — SqlExecError (highest)

*Detail:* Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ...) AS mo FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-02-28 22:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

**KQL**
```kql
range t from datetime(2020-02-28 22:00:00) to datetime(2020-03-01 04:00:00) step 90m | extend b1d = bin(t, 1d), b6h = bin(t, 6h), eom = endofmonth(t), mo = monthofyear(t)
```
**Generated SQL**
```sql
SELECT *, EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(t AS TIMESTAMP))/86400000)*86400000 AS BIGINT)) AS b1d, EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(t AS TIMESTAMP))/21600000)*21600000 AS BIGINT)) AS b6h, DATE_TRUNC('month', t) + INTERVAL '1 month' - INTERVAL '1 microsecond' AS eom, EXTRACT(MONTH FROM t) AS mo FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-02-28 22:00:00' AS BIGINT), CAST(TIMESTAMP '2020-03-01 04:00:00' AS BIGINT), CAST((5400000 * INTERVAL '1 millisecond') AS BIGINT)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, b1d:DateTime, b6h:DateTime, eom:DateTime, mo:Int] rows=21
    - (2020-02-28T22:00:00.0000000Z, 2020-02-28T00:00:00.0000000Z, 2020-02-28T18:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 2)
    - (2020-02-28T23:30:00.0000000Z, 2020-02-28T00:00:00.0000000Z, 2020-02-28T18:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 2)
    - (2020-02-29T01:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 2)
    - (2020-02-29T02:30:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 2)
    - (2020-02-29T04:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 2)
    - (2020-02-29T05:30:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 2)
    - (2020-02-29T07:00:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2020-02-29T06:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 2)
    - (2020-02-29T08:30:00.0000000Z, 2020-02-29T00:00:00.0000000Z, 2020-02-29T06:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 2)
- DuckDB: ERROR — Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ...) AS mo FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-02-28 22:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

### `agent-datetime-timespan-0029` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '/(INTERVAL, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	/(FLOAT, FLOAT) -> FLOAT
	/(DOUBLE, DOUBLE) -> DOUBLE
	/(INTERVAL, DOUBLE) -> INTERVAL


LINE 1: ... DATE_DIFF('millisecond', b, a) AS dms, a - b AS span, (a - b) / ((1 / 10.0) * INTERVAL '1 microsecond') AS spanticks FROM...
                                                                          ^

**KQL**
```kql
datatable(a:datetime, b:datetime)[ datetime(2020-01-01 00:00:01.9999999),datetime(2020-01-01 00:00:00.0000001), datetime(2020-03-01 00:00:00),datetime(2020-02-29 23:59:59.9999999) ] | extend ds = datetime_diff('second', a, b), dms = datetime_diff('millisecond', a, b), span = a - b, spanticks = (a - b) / 1tick
```
**Generated SQL**
```sql
SELECT *, DATE_DIFF('second', b, a) AS ds, DATE_DIFF('millisecond', b, a) AS dms, a - b AS span, (a - b) / ((1 / 10.0) * INTERVAL '1 microsecond') AS spanticks FROM (VALUES (TIMESTAMP '2020-01-01 00:00:01.999999', TIMESTAMP '2020-01-01 00:00:00.000000'), (TIMESTAMP '2020-03-01 00:00:00', TIMESTAMP '2020-02-29 23:59:59.999999')) AS t(a, b)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:DateTime, b:DateTime, ds:Int, dms:Int, span:TimeSpan, spanticks:Real] rows=2
    - (2020-01-01T00:00:01.9999999Z, 2020-01-01T00:00:00.0000001Z, 1, 1999, 00:00:01.9999998, 19999998)
    - (2020-03-01T00:00:00.0000000Z, 2020-02-29T23:59:59.9999999Z, 1, 1, 00:00:00.0000001, 1)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '/(INTERVAL, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	/(FLOAT, FLOAT) -> FLOAT
	/(DOUBLE, DOUBLE) -> DOUBLE
	/(INTERVAL, DOUBLE) -> INTERVAL


LINE 1: ... DATE_DIFF('millisecond', b, a) AS dms, a - b AS span, (a - b) / ((1 / 10.0) * INTERVAL '1 microsecond') AS spanticks FROM...
                                                                          ^

### `agent-datetime-timespan-0037` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '/(VARCHAR, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	/(FLOAT, FLOAT) -> FLOAT
	/(DOUBLE, DOUBLE) -> DOUBLE
	/(INTERVAL, DOUBLE) -> INTERVAL


LINE 1: ...' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow FROM (VALUES...
                                                                          ^

**KQL**
```kql
datatable(t:datetime, s:timespan)[ datetime(2020-03-29 01:30:00),1h, datetime(2020-10-25 02:30:00),-1h, datetime(2024-02-29 23:00:00),2h ] | extend plus = t + s, minus = t - s, bin = bin(t, s), dow = dayofweek(t) / 1d
```
**Generated SQL**
```sql
SELECT *, t + s AS plus, t - s AS minus, EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(t AS TIMESTAMP))/EPOCH_MS(CAST(TIMESTAMP 'epoch' + (s) AS TIMESTAMP)))*EPOCH_MS(CAST(TIMESTAMP 'epoch' + (s) AS TIMESTAMP)) AS BIGINT)) AS bin, CASE WHEN EXTRACT(DOW FROM t) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow FROM (VALUES (TIMESTAMP '2020-03-29 01:30:00', (3600000 * INTERVAL '1 millisecond')), (TIMESTAMP '2020-10-25 02:30:00', (-3600000 * INTERVAL '1 millisecond')), (TIMESTAMP '2024-02-29 23:00:00', (7200000 * INTERVAL '1 millisecond'))) AS t(t, s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, s:TimeSpan, plus:DateTime, minus:DateTime, bin:DateTime, dow:Real] rows=3
    - (2020-03-29T01:30:00.0000000Z, 01:00:00, 2020-03-29T02:30:00.0000000Z, 2020-03-29T00:30:00.0000000Z, 2020-03-29T01:00:00.0000000Z, 0)
    - (2020-10-25T02:30:00.0000000Z, -01:00:00, 2020-10-25T01:30:00.0000000Z, 2020-10-25T03:30:00.0000000Z, null, 0)
    - (2024-02-29T23:00:00.0000000Z, 02:00:00, 2024-03-01T01:00:00.0000000Z, 2024-02-29T21:00:00.0000000Z, 2024-02-29T22:00:00.0000000Z, 4)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '/(VARCHAR, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	/(FLOAT, FLOAT) -> FLOAT
	/(DOUBLE, DOUBLE) -> DOUBLE
	/(INTERVAL, DOUBLE) -> INTERVAL


LINE 1: ...' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS dow FROM (VALUES...
                                                                          ^

### `agent-datetime-timespan-0039` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types 'date_part(STRING_LITERAL, VARCHAR)'. You might need to add explicit type casts.
	Candidate functions:
	date_part(VARCHAR[], DATE) -> STRUCT()
	date_part(VARCHAR[], INTERVAL) -> STRUCT()
	date_part(VARCHAR[], TIME) -> STRUCT()
	date_part(VARCHAR[], TIMESTAMP) -> STRUCT()
	date_part(VARCHAR[], TIME WITH TIME ZONE) -> STRUCT()
	date_part(VARCHAR[], TIME_NS) -> STRUCT()
	date_part(VARCHAR, DATE) -> BIGINT
	date_part(VARCHAR, INTERVAL) -> BIGINT
	date_part(VARCHAR, TIME) -> BIGINT
	date_part(VARCHAR, TIMESTAMP) -> BIGINT
	date_part(VARCHAR, TIME WITH TIME ZONE) -> BIGINT
	date_part(VARCHAR, TIME_NS) -> BIGINT
	date_part(VARCHAR[], TIMESTAMP WITH TIME ZONE) -> STRUCT()
	date_part(VARCHAR, TIMESTAMP WITH TIME ZONE) -> BIGINT


LINE 1: SELECT ((EXTRACT(EPOCH FROM (CASE WHEN EXTRACT(DOW FROM TIMESTAMP...
                 ^

**KQL**
```kql
print a = dayofweek(datetime(0001-01-01)) / 1d, b = dayofweek(datetime(9999-12-31)) / 1d, c = dayofyear(datetime(2020-12-31)), d = dayofyear(datetime(2021-12-31)), e = hourofday(datetime(2020-06-15 23:59:59.9999999))
```
**Generated SQL**
```sql
SELECT ((EXTRACT(EPOCH FROM (CASE WHEN EXTRACT(DOW FROM TIMESTAMP '0001-01-01 00:00:00') = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM TIMESTAMP '0001-01-01 00:00:00') AS VARCHAR) || '.00:00:00' END)) - EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00')) / EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond')))) AS a, ((EXTRACT(EPOCH FROM (CASE WHEN EXTRACT(DOW FROM TIMESTAMP '9999-12-31 00:00:00') = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM TIMESTAMP '9999-12-31 00:00:00') AS VARCHAR) || '.00:00:00' END)) - EXTRACT(EPOCH FROM TIMESTAMP '0001-01-01 00:00:00')) / EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond')))) AS b, EXTRACT(DOY FROM TIMESTAMP '2020-12-31 00:00:00') AS c, EXTRACT(DOY FROM TIMESTAMP '2021-12-31 00:00:00') AS d, EXTRACT(HOUR FROM TIMESTAMP '2020-06-15 23:59:59.999999') AS e
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Real, b:Real, c:Int, d:Int, e:Int] rows=1
    - (1, null, 366, 365, 23)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types 'date_part(STRING_LITERAL, VARCHAR)'. You might need to add explicit type casts.
	Candidate functions:
	date_part(VARCHAR[], DATE) -> STRUCT()
	date_part(VARCHAR[], INTERVAL) -> STRUCT()
	date_part(VARCHAR[], TIME) -> STRUCT()
	date_part(VARCHAR[], TIMESTAMP) -> STRUCT()
	date_part(VARCHAR[], TIME WITH TIME ZONE) -> STRUCT()
	date_part(VARCHAR[], TIME_NS) -> STRUCT()
	date_part(VARCHAR, DATE) -> BIGINT
	date_part(VARCHAR, INTERVAL) -> BIGINT
	date_part(VARCHAR, TIME) -> BIGINT
	date_part(VARCHAR, TIMESTAMP) -> BIGINT
	date_part(VARCHAR, TIME WITH TIME ZONE) -> BIGINT
	date_part(VARCHAR, TIME_NS) -> BIGINT
	date_part(VARCHAR[], TIMESTAMP WITH TIME ZONE) -> STRUCT()
	date_part(VARCHAR, TIMESTAMP WITH TIME ZONE) -> BIGINT


LINE 1: SELECT ((EXTRACT(EPOCH FROM (CASE WHEN EXTRACT(DOW FROM TIMESTAMP...
                 ^

### `agent-datetime-timespan-0044` — SqlExecError (highest)

*Detail:* Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ... AS som FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-12-28 00:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

**KQL**
```kql
range t from datetime(2020-12-28) to datetime(2021-01-04) step 1d | extend woy = week_of_year(t), iso_dow = dayofweek(t) / 1d, sow = startofweek(t), eow = endofweek(t), som = startofmonth(t)
```
**Generated SQL**
```sql
SELECT *, EXTRACT(WEEK FROM t) AS woy, CASE WHEN EXTRACT(DOW FROM t) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END / (86400000 * INTERVAL '1 millisecond') AS iso_dow, (DATE_TRUNC('week', t + INTERVAL '1 day') - INTERVAL '1 day') AS sow, (DATE_TRUNC('week', t + INTERVAL '1 day') - INTERVAL '1 day') + INTERVAL '7 days' - INTERVAL '1 microsecond' AS eow, DATE_TRUNC('month', t) AS som FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-12-28 00:00:00' AS BIGINT), CAST(TIMESTAMP '2021-01-04 00:00:00' AS BIGINT), CAST((86400000 * INTERVAL '1 millisecond') AS BIGINT)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, woy:Int, iso_dow:Real, sow:DateTime, eow:DateTime, som:DateTime] rows=8
    - (2020-12-28T00:00:00.0000000Z, 53, 1, 2020-12-27T00:00:00.0000000Z, 2021-01-02T23:59:59.9999999Z, 2020-12-01T00:00:00.0000000Z)
    - (2020-12-29T00:00:00.0000000Z, 53, 2, 2020-12-27T00:00:00.0000000Z, 2021-01-02T23:59:59.9999999Z, 2020-12-01T00:00:00.0000000Z)
    - (2020-12-30T00:00:00.0000000Z, 53, 3, 2020-12-27T00:00:00.0000000Z, 2021-01-02T23:59:59.9999999Z, 2020-12-01T00:00:00.0000000Z)
    - (2020-12-31T00:00:00.0000000Z, 53, 4, 2020-12-27T00:00:00.0000000Z, 2021-01-02T23:59:59.9999999Z, 2020-12-01T00:00:00.0000000Z)
    - (2021-01-01T00:00:00.0000000Z, 53, 5, 2020-12-27T00:00:00.0000000Z, 2021-01-02T23:59:59.9999999Z, 2021-01-01T00:00:00.0000000Z)
    - (2021-01-02T00:00:00.0000000Z, 53, 6, 2020-12-27T00:00:00.0000000Z, 2021-01-02T23:59:59.9999999Z, 2021-01-01T00:00:00.0000000Z)
    - (2021-01-03T00:00:00.0000000Z, 53, 0, 2021-01-03T00:00:00.0000000Z, 2021-01-09T23:59:59.9999999Z, 2021-01-01T00:00:00.0000000Z)
    - (2021-01-04T00:00:00.0000000Z, 1, 1, 2021-01-03T00:00:00.0000000Z, 2021-01-09T23:59:59.9999999Z, 2021-01-01T00:00:00.0000000Z)
- DuckDB: ERROR — Conversion Error: Unimplemented type for cast (TIMESTAMP -> BIGINT)

LINE 1: ... AS som FROM (SELECT generate_series AS t FROM generate_series(CAST(TIMESTAMP '2020-12-28 00:00:00' AS BIGINT), CAST(TIMES...
                                                                          ^

### `agent-datetime-timespan-0046` — SqlExecError (highest)

*Detail:* Conversion Error: Could not convert string 'time(1.02:03:04)' to INTERVAL

LINE 1: ...), 4, '0')) AS a, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04)')) * 1000000) < 0 THEN '-' ELSE '' END...
                                                                      ^

**KQL**
```kql
print a = format_timespan(1d, 'dddd'), b = format_timespan(time(1.02:03:04), 'd'), c = format_timespan(time(1.02:03:04), 'hh'), d = format_timespan(time(0.00:00:00.123), 'fff'), e = format_timespan(time(2.05:00:00), 'd.h')
```
**Generated SQL**
```sql
SELECT (CASE WHEN (EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond'))) * 1000000) < 0 THEN '-' ELSE '' END || LPAD(CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond'))) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR), 4, '0')) AS a, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04)')) * 1000000) < 0 THEN '-' ELSE '' END || CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04)')) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR)) AS b, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04)')) * 1000000) < 0 THEN '-' ELSE '' END || LPAD(CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04)')) * 1000000)), 86400000000) / 3600000000) AS BIGINT) AS VARCHAR), 2, '0')) AS c, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(0.00:00:00.123)')) * 1000000) < 0 THEN '-' ELSE '' END || LPAD(CAST(CAST(FLOOR(CAST(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(0.00:00:00.123)')) * 1000000)), 1000000) AS BIGINT) / 1000) AS BIGINT) AS VARCHAR), 3, '0')) AS d, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(2.05:00:00)')) * 1000000) < 0 THEN '-' ELSE '' END || CAST(CAST(FLOOR(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(2.05:00:00)')) * 1000000)) / 86400000000) AS BIGINT) AS VARCHAR) || '.' || CAST(CAST(FLOOR(MOD(ABS((EXTRACT(EPOCH FROM (INTERVAL 'time(2.05:00:00)')) * 1000000)), 86400000000) / 3600000000) AS BIGINT) AS VARCHAR)) AS e
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String, b:String, c:String, d:String, e:String] rows=1
    - ('0001', '1', '02', '123', '2.5')
- DuckDB: ERROR — Conversion Error: Could not convert string 'time(1.02:03:04)' to INTERVAL

LINE 1: ...), 4, '0')) AS a, (CASE WHEN (EXTRACT(EPOCH FROM (INTERVAL 'time(1.02:03:04)')) * 1000000) < 0 THEN '-' ELSE '' END...
                                                                      ^

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

### `agent-datetime-timespan-0004` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1955-11-05T00:00:00.0000000Z, 1955-10-31T00:00:00.0000000Z) duck=(1955-11-05T00:00:00.0000000Z, 1955-11-03T00:00:00.0000000Z)

**KQL**
```kql
print binneg = bin(datetime(1955-11-05 06:15:00), 1d), binneg2 = bin(datetime(1955-11-05 06:15:00), 7d)
```
**Generated SQL**
```sql
SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(TIMESTAMP '1955-11-05 06:15:00' AS TIMESTAMP))/86400000)*86400000 AS BIGINT)) AS binneg, EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(TIMESTAMP '1955-11-05 06:15:00' AS TIMESTAMP))/604800000)*604800000 AS BIGINT)) AS binneg2
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[binneg:DateTime, binneg2:DateTime] rows=1
    - (1955-11-05T00:00:00.0000000Z, 1955-10-31T00:00:00.0000000Z)
- DuckDB: cols=[binneg:DateTime, binneg2:DateTime] rows=1
    - (1955-11-05T00:00:00.0000000Z, 1955-11-03T00:00:00.0000000Z)

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

### `agent-datetime-timespan-0024` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1.02:03:04.5000000, -00:30:00, 13:00:00) duck=(null, -00:30:00, 13:00:00)

**KQL**
```kql
print sp = totimespan('1.02:03:04.5'), sp2 = totimespan('-00:30:00'), sp3 = totimespan('13:00')
```
**Generated SQL**
```sql
SELECT TRY_CAST('1.02:03:04.5' AS INTERVAL) AS sp, TRY_CAST('-00:30:00' AS INTERVAL) AS sp2, TRY_CAST('13:00' AS INTERVAL) AS sp3
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[sp:TimeSpan, sp2:TimeSpan, sp3:TimeSpan] rows=1
    - (1.02:03:04.5000000, -00:30:00, 13:00:00)
- DuckDB: cols=[sp:TimeSpan, sp2:TimeSpan, sp3:TimeSpan] rows=1
    - (null, -00:30:00, 13:00:00)

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

### `agent-datetime-timespan-0039` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1.01:01:01, 1.042372685185185, 25.016944444444444, 1501.0166666666667, '1.01:01:01') duck=(1.01:01:01, 1.042372685185185, 25.016944444444444, 1501.0166666666667, '25:01:01')

**KQL**
```kql
datatable(span:timespan)[ 90061s, 86400s, 3661s, -3661s ] | extend d = span / 1d, h = span / 1h, m = span / 1m, fmt = tostring(span)
```
**Generated SQL**
```sql
SELECT *, (EXTRACT(EPOCH FROM (span)) / EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond')))) AS d, (EXTRACT(EPOCH FROM (span)) / EXTRACT(EPOCH FROM ((3600000 * INTERVAL '1 millisecond')))) AS h, (EXTRACT(EPOCH FROM (span)) / EXTRACT(EPOCH FROM ((60000 * INTERVAL '1 millisecond')))) AS m, TRY_CAST(span AS TEXT) AS fmt FROM (VALUES ((90061000 * INTERVAL '1 millisecond')), ((86400000 * INTERVAL '1 millisecond')), ((3661000 * INTERVAL '1 millisecond')), ((-3661000 * INTERVAL '1 millisecond'))) AS t(span)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[span:TimeSpan, d:Real, h:Real, m:Real, fmt:String] rows=4
    - (1.01:01:01, 1.042372685185185, 25.016944444444444, 1501.0166666666667, '1.01:01:01')
    - (1.00:00:00, 1, 24, 1440, '1.00:00:00')
    - (01:01:01, 0.04237268518518519, 1.0169444444444444, 61.016666666666666, '01:01:01')
    - (-01:01:01, -0.04237268518518519, -1.0169444444444444, -61.016666666666666, '-01:01:01')
- DuckDB: cols=[span:TimeSpan, d:Real, h:Real, m:Real, fmt:String] rows=4
    - (1.01:01:01, 1.042372685185185, 25.016944444444444, 1501.0166666666667, '25:01:01')
    - (1.00:00:00, 1, 24, 1440, '24:00:00')
    - (01:01:01, 0.04237268518518519, 1.0169444444444444, 61.016666666666666, '01:01:01')
    - (-01:01:01, -0.04237268518518519, -1.0169444444444444, -61.016666666666666, '-01:01:01')

### `agent-datetime-timespan-0040` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(86400, '1.02:00:00', '-1.02:03:04') duck=(86400, '26:00:00', '-26:03:04')

**KQL**
```kql
print sec = totimespan(1d) / totimespan(1s), tot = tostring(1d + 2h), tot2 = tostring(-1d - 2h - 3m - 4s)
```
**Generated SQL**
```sql
SELECT (EXTRACT(EPOCH FROM (TRY_CAST((86400000 * INTERVAL '1 millisecond') AS INTERVAL))) / EXTRACT(EPOCH FROM (TRY_CAST((1000 * INTERVAL '1 millisecond') AS INTERVAL)))) AS sec, TRY_CAST((86400000 * INTERVAL '1 millisecond') + (7200000 * INTERVAL '1 millisecond') AS TEXT) AS tot, TRY_CAST((-(86400000 * INTERVAL '1 millisecond')) - (7200000 * INTERVAL '1 millisecond') - (180000 * INTERVAL '1 millisecond') - (4000 * INTERVAL '1 millisecond') AS TEXT) AS tot2
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[sec:Real, tot:String, tot2:String] rows=1
    - (86400, '1.02:00:00', '-1.02:03:04')
- DuckDB: cols=[sec:Real, tot:String, tot2:String] rows=1
    - (86400, '26:00:00', '-26:03:04')

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

### `agent-datetime-timespan-0025` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1955-11-05T06:00:00.0000000Z, 1955-10-31T00:00:00.0000000Z, 1955-11-05T00:00:00.0000000Z, 1955-11-05T03:30:00.0000000Z) duck=(1955-11-05T06:00:00.0000000Z, 1955-11-03T00:00:00.0000000Z, 1955-11-05T00:00:00.0000000Z, 1955-11-05T03:30:00.0000000Z)

**KQL**
```kql
print a = bin(datetime(1955-11-05 06:15:00.5), 1h), b = bin(datetime(1955-11-05 06:15:00.5), 7d), c = bin_at(datetime(1955-11-05 06:15:00), 1d, datetime(1970-01-01)), d = bin_at(datetime(1955-11-05 06:15:00), 3h, datetime(1955-11-05 00:30:00))
```
**Generated SQL**
```sql
SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(TIMESTAMP '1955-11-05 06:15:00.500000' AS TIMESTAMP))/3600000)*3600000 AS BIGINT)) AS a, EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(TIMESTAMP '1955-11-05 06:15:00.500000' AS TIMESTAMP))/604800000)*604800000 AS BIGINT)) AS b, EPOCH_MS(CAST(FLOOR((EPOCH_MS(CAST(TIMESTAMP '1955-11-05 06:15:00' AS TIMESTAMP)) - EPOCH_MS(CAST(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP)))/86400000)*86400000 + EPOCH_MS(CAST(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP)) AS BIGINT)) AS c, EPOCH_MS(CAST(FLOOR((EPOCH_MS(CAST(TIMESTAMP '1955-11-05 06:15:00' AS TIMESTAMP)) - EPOCH_MS(CAST(TIMESTAMP '1955-11-05 00:30:00' AS TIMESTAMP)))/10800000)*10800000 + EPOCH_MS(CAST(TIMESTAMP '1955-11-05 00:30:00' AS TIMESTAMP)) AS BIGINT)) AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:DateTime, b:DateTime, c:DateTime, d:DateTime] rows=1
    - (1955-11-05T06:00:00.0000000Z, 1955-10-31T00:00:00.0000000Z, 1955-11-05T00:00:00.0000000Z, 1955-11-05T03:30:00.0000000Z)
- DuckDB: cols=[a:DateTime, b:DateTime, c:DateTime, d:DateTime] rows=1
    - (1955-11-05T06:00:00.0000000Z, 1955-11-03T00:00:00.0000000Z, 1955-11-05T00:00:00.0000000Z, 1955-11-05T03:30:00.0000000Z)

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

### `agent-datetime-timespan-0031` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('2020-06-15T13:45:30.0000000Z', '2020-06-15T13:45:30.7654321Z', '2020-06-15T00:00:00.0000000Z', '2020-06-15T13:45:00.0000000Z') duck=('2020-06-15 13:45:30', '2020-06-15 13:45:30.765432', '2020-06-15 00:00:00', '2020-06-15 13:45:00')

**KQL**
```kql
print a = tostring(bin(datetime(2020-06-15 13:45:30.7654321), 1s)), b = tostring(datetime(2020-06-15 13:45:30.7654321)), c = tostring(datetime(2020-06-15 00:00:00)), d = tostring(datetime(2020-06-15 13:45:00))
```
**Generated SQL**
```sql
SELECT TRY_CAST(EPOCH_MS(CAST(FLOOR(EPOCH_MS(CAST(TIMESTAMP '2020-06-15 13:45:30.765432' AS TIMESTAMP))/1000)*1000 AS BIGINT)) AS TEXT) AS a, TRY_CAST(TIMESTAMP '2020-06-15 13:45:30.765432' AS TEXT) AS b, TRY_CAST(TIMESTAMP '2020-06-15 00:00:00' AS TEXT) AS c, TRY_CAST(TIMESTAMP '2020-06-15 13:45:00' AS TEXT) AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String, b:String, c:String, d:String] rows=1
    - ('2020-06-15T13:45:30.0000000Z', '2020-06-15T13:45:30.7654321Z', '2020-06-15T00:00:00.0000000Z', '2020-06-15T13:45:00.0000000Z')
- DuckDB: cols=[a:String, b:String, c:String, d:String] rows=1
    - ('2020-06-15 13:45:30', '2020-06-15 13:45:30.765432', '2020-06-15 00:00:00', '2020-06-15 13:45:00')

### `agent-datetime-timespan-0032` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1.02:03:04.5000000, -1.02:03:04.5000000, 23:59:59.9999999, 00:00:00.0010000, 100.00:00:00) duck=(null, null, 23:59:59.9999990, 00:00:00.0010000, null)

**KQL**
```kql
print a = totimespan('1.02:03:04.5'), b = totimespan('-1.02:03:04.5'), c = totimespan('23:59:59.9999999'), d = totimespan('0:0:0.001'), e = totimespan('100.00:00:00')
```
**Generated SQL**
```sql
SELECT TRY_CAST('1.02:03:04.5' AS INTERVAL) AS a, TRY_CAST('-1.02:03:04.5' AS INTERVAL) AS b, TRY_CAST('23:59:59.9999999' AS INTERVAL) AS c, TRY_CAST('0:0:0.001' AS INTERVAL) AS d, TRY_CAST('100.00:00:00' AS INTERVAL) AS e
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:TimeSpan, b:TimeSpan, c:TimeSpan, d:TimeSpan, e:TimeSpan] rows=1
    - (1.02:03:04.5000000, -1.02:03:04.5000000, 23:59:59.9999999, 00:00:00.0010000, 100.00:00:00)
- DuckDB: cols=[a:TimeSpan, b:TimeSpan, c:TimeSpan, d:TimeSpan, e:TimeSpan] rows=1
    - (null, null, 23:59:59.9999990, 00:00:00.0010000, null)

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

## Family: dynamic-json (74)

### `agent-dynamic-json-0019` — SqlExecError (highest)

*Detail:* Parser Error: syntax error at or near "AS"

LINE 1: ....* FROM (VALUES ('{"x":1}'::JSON), ('{"y":2}'::JSON)) AS t(d) AS t CROSS JOIN UNNEST(JSON_KEYS(d)) AS u(value), LATERAL...
                                                                         ^

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"x":1}), dynamic({"y":2}) ] | mv-apply k = bag_keys(d) on (extend kk = k)
```
**Generated SQL**
```sql
SELECT t.*, _sub.* FROM (VALUES ('{"x":1}'::JSON), ('{"y":2}'::JSON)) AS t(d) AS t CROSS JOIN UNNEST(JSON_KEYS(d)) AS u(value), LATERAL (SELECT *, k AS kk FROM (SELECT u.value AS k)) AS _sub
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, k:Dynamic, kk:Dynamic] rows=2
    - ({
  "x": 1
}, "x", "x")
    - ({
  "y": 2
}, "y", "y")
- DuckDB: ERROR — Parser Error: syntax error at or near "AS"

LINE 1: ....* FROM (VALUES ('{"x":1}'::JSON), ('{"y":2}'::JSON)) AS t(d) AS t CROSS JOIN UNNEST(JSON_KEYS(d)) AS u(value), LATERAL...
                                                                         ^

### `agent-dynamic-json-0022` — SqlExecError (highest)

*Detail:* Conversion Error: Malformed JSON at byte 0 of input: unexpected character.  Input: "hello"

LINE 1: ... 'dictionary' END) ELSE LOWER(TYPEOF(x)) END AS t FROM (SELECT CAST('hello' AS JSON) AS x)
                                                                          ^

**KQL**
```kql
print x = parse_json('"hello"') | extend t = gettype(x)
```
**Generated SQL**
```sql
SELECT *, CASE WHEN TYPEOF(x) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(x) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(x) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(x) = 'VARCHAR' THEN 'string' WHEN TYPEOF(x) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(x) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(x) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(x) = 'UUID' THEN 'guid' WHEN TYPEOF(x) LIKE '%[]' OR TYPEOF(x) LIKE 'STRUCT%' OR TYPEOF(x) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(x) = 'JSON' THEN (CASE WHEN json_type(CAST(x AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(x)) END AS t FROM (SELECT CAST('hello' AS JSON) AS x)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Dynamic, t:String] rows=1
    - ("hello", 'string')
- DuckDB: ERROR — Conversion Error: Malformed JSON at byte 0 of input: unexpected character.  Input: "hello"

LINE 1: ... 'dictionary' END) ELSE LOWER(TYPEOF(x)) END AS t FROM (SELECT CAST('hello' AS JSON) AS x)
                                                                          ^

### `agent-dynamic-json-0000` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ... ('{"a":[1,2],"b":[3,4,5]}'::JSON)) AS t(d)) AS t CROSS JOIN UNNEST(t.z) AS u(value))
                                                                        ^

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":[1,2],"b":[3,4,5]}) ] | extend z = array_concat(array_slice(d.a, 0, 0), array_reverse(d.b)) | mv-expand z to typeof(long) | summarize s = sum(z), c = count()
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(z), 0) AS s, COUNT(*) AS c FROM (SELECT t.* EXCLUDE (z), u.value AS z FROM (SELECT *, TO_JSON(LIST_CONCAT(json_extract(TO_JSON(LIST_SLICE(json_extract(json_extract(d, '$.a'), '$[*]'), 0 + 1, 0 + 1)), '$[*]'), json_extract(TO_JSON(LIST_REVERSE(json_extract(json_extract(d, '$.b'), '$[*]'))), '$[*]'))) AS z FROM (VALUES ('{"a":[1,2],"b":[3,4,5]}'::JSON)) AS t(d)) AS t CROSS JOIN UNNEST(t.z) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, c:Int] rows=1
    - (13, 4)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ... ('{"a":[1,2],"b":[3,4,5]}'::JSON)) AS t(d)) AS t CROSS JOIN UNNEST(t.z) AS u(value))
                                                                        ^

### `agent-dynamic-json-0004` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '+(VARCHAR, INTEGER_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	+(TINYINT) -> TINYINT
	+(TINYINT, TINYINT) -> TINYINT
	+(SMALLINT) -> SMALLINT
	+(SMALLINT, SMALLINT) -> SMALLINT
	+(INTEGER) -> INTEGER
	+(INTEGER, INTEGER) -> INTEGER
	+(BIGINT) -> BIGINT
	+(BIGINT, BIGINT) -> BIGINT
	+(HUGEINT) -> HUGEINT
	+(HUGEINT, HUGEINT) -> HUGEINT
	+(FLOAT) -> FLOAT
	+(FLOAT, FLOAT) -> FLOAT
	+(DOUBLE) -> DOUBLE
	+(DOUBLE, DOUBLE) -> DOUBLE
	+(DECIMAL) -> DECIMAL
	+(DECIMAL, DECIMAL) -> DECIMAL
	+(UTINYINT) -> UTINYINT
	+(UTINYINT, UTINYINT) -> UTINYINT
	+(USMALLINT) -> USMALLINT
	+(USMALLINT, USMALLINT) -> USMALLINT
	+(UINTEGER) -> UINTEGER
	+(UINTEGER, UINTEGER) -> UINTEGER
	+(UBIGINT) -> UBIGINT
	+(UBIGINT, UBIGINT) -> UBIGINT
	+(UHUGEINT) -> UHUGEINT
	+(UHUGEINT, UHUGEINT) -> UHUGEINT
	+(DATE, INTEGER) -> DATE
	+(INTEGER, DATE) -> DATE
	+(INTERVAL, INTERVAL) -> INTERVAL
	+(DATE, INTERVAL) -> TIMESTAMP
	+(INTERVAL, DATE) -> TIMESTAMP
	+(TIME, INTERVAL) -> TIME
	+(INTERVAL, TIME) -> TIME
	+(TIMESTAMP, INTERVAL) -> TIMESTAMP
	+(INTERVAL, TIMESTAMP) -> TIMESTAMP
	+(TIME WITH TIME ZONE, INTERVAL) -> TIME WITH TIME ZONE
	+(INTERVAL, TIME WITH TIME ZONE) -> TIME WITH TIME ZONE
	+(TIME, DATE) -> TIMESTAMP
	+(DATE, TIME) -> TIMESTAMP
	+(TIME WITH TIME ZONE, DATE) -> TIMESTAMP WITH TIME ZONE
	+(DATE, TIME WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE
	+([ANY[]...]) -> ANY[]
	+(BIGNUM, BIGNUM) -> BIGNUM
	+(TIMESTAMP WITH TIME ZONE, INTERVAL) -> TIMESTAMP WITH TIME ZONE
	+(INTERVAL, TIMESTAMP WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE


LINE 1: ... sum_val FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d[k + 1] AS DOUBLE), TRY_CAST(TRY_CAST(d[k + 1] AS BOOLEAN)...
                                                                         ^

**KQL**
```kql
print d = dynamic({"x":1,"y":2,"z":3}) | extend ks = bag_keys(d) | mv-expand k = ks to typeof(string) | extend val = toint(d[k]) | summarize sum(val)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(val), 0) AS sum_val FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d[k + 1] AS DOUBLE), TRY_CAST(TRY_CAST(d[k + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS val FROM (SELECT t.*, u.value AS k FROM (SELECT *, JSON_KEYS(d) AS ks FROM (SELECT '{"x":1,"y":2,"z":3}'::JSON AS d)) AS t CROSS JOIN UNNEST(ks) AS u(value)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[sum_val:Int] rows=1
    - (6)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '+(VARCHAR, INTEGER_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	+(TINYINT) -> TINYINT
	+(TINYINT, TINYINT) -> TINYINT
	+(SMALLINT) -> SMALLINT
	+(SMALLINT, SMALLINT) -> SMALLINT
	+(INTEGER) -> INTEGER
	+(INTEGER, INTEGER) -> INTEGER
	+(BIGINT) -> BIGINT
	+(BIGINT, BIGINT) -> BIGINT
	+(HUGEINT) -> HUGEINT
	+(HUGEINT, HUGEINT) -> HUGEINT
	+(FLOAT) -> FLOAT
	+(FLOAT, FLOAT) -> FLOAT
	+(DOUBLE) -> DOUBLE
	+(DOUBLE, DOUBLE) -> DOUBLE
	+(DECIMAL) -> DECIMAL
	+(DECIMAL, DECIMAL) -> DECIMAL
	+(UTINYINT) -> UTINYINT
	+(UTINYINT, UTINYINT) -> UTINYINT
	+(USMALLINT) -> USMALLINT
	+(USMALLINT, USMALLINT) -> USMALLINT
	+(UINTEGER) -> UINTEGER
	+(UINTEGER, UINTEGER) -> UINTEGER
	+(UBIGINT) -> UBIGINT
	+(UBIGINT, UBIGINT) -> UBIGINT
	+(UHUGEINT) -> UHUGEINT
	+(UHUGEINT, UHUGEINT) -> UHUGEINT
	+(DATE, INTEGER) -> DATE
	+(INTEGER, DATE) -> DATE
	+(INTERVAL, INTERVAL) -> INTERVAL
	+(DATE, INTERVAL) -> TIMESTAMP
	+(INTERVAL, DATE) -> TIMESTAMP
	+(TIME, INTERVAL) -> TIME
	+(INTERVAL, TIME) -> TIME
	+(TIMESTAMP, INTERVAL) -> TIMESTAMP
	+(INTERVAL, TIMESTAMP) -> TIMESTAMP
	+(TIME WITH TIME ZONE, INTERVAL) -> TIME WITH TIME ZONE
	+(INTERVAL, TIME WITH TIME ZONE) -> TIME WITH TIME ZONE
	+(TIME, DATE) -> TIMESTAMP
	+(DATE, TIME) -> TIMESTAMP
	+(TIME WITH TIME ZONE, DATE) -> TIMESTAMP WITH TIME ZONE
	+(DATE, TIME WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE
	+([ANY[]...]) -> ANY[]
	+(BIGNUM, BIGNUM) -> BIGNUM
	+(TIMESTAMP WITH TIME ZONE, INTERVAL) -> TIMESTAMP WITH TIME ZONE
	+(INTERVAL, TIMESTAMP WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE


LINE 1: ... sum_val FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d[k + 1] AS DOUBLE), TRY_CAST(TRY_CAST(d[k + 1] AS BOOLEAN)...
                                                                         ^

### `agent-dynamic-json-0011` — SqlExecError (highest)

*Detail:* Binder Error: Referenced column "idx" not found in FROM clause!
Candidate bindings: "arr"

LINE 1: SELECT idx, v, TRY_CAST(TRUNC(COALESCE(TRY_CAST(v AS DOUBLE), TRY_...
               ^

**KQL**
```kql
print arr = dynamic([10,20,30,40,50]) | mv-expand with_itemindex=idx v = arr | project idx, v, prod = toint(v) * idx
```
**Generated SQL**
```sql
SELECT idx, v, TRY_CAST(TRUNC(COALESCE(TRY_CAST(v AS DOUBLE), TRY_CAST(TRY_CAST(v AS BOOLEAN) AS DOUBLE))) AS INTEGER) * idx AS prod FROM (SELECT t.*, u.value AS v FROM (SELECT LIST_VALUE(10, 20, 30, 40, 50) AS arr) AS t CROSS JOIN UNNEST(arr) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[idx:Int, v:Dynamic, prod:Int] rows=5
    - (0, 10, 0)
    - (1, 20, 20)
    - (2, 30, 60)
    - (3, 40, 120)
    - (4, 50, 200)
- DuckDB: ERROR — Binder Error: Referenced column "idx" not found in FROM clause!
Candidate bindings: "arr"

LINE 1: SELECT idx, v, TRY_CAST(TRUNC(COALESCE(TRY_CAST(v AS DOUBLE), TRY_...
               ^

### `agent-dynamic-json-0034` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ...,"vals":[10,20]},{"id":2,"vals":[30]}]' AS s)) AS t CROSS JOIN UNNEST(d) AS u(value)) AS t CROSS JOIN UNNEST(CASE WHEN...
                                                                          ^

**KQL**
```kql
print s = '[{"id":1,"vals":[10,20]},{"id":2,"vals":[30]}]' | extend d = parse_json(s) | mv-expand o = d | mv-expand v = o.vals | summarize sum(toint(v)) by oid = tostring(o.id)
```
**Generated SQL**
```sql
SELECT TRY_CAST(json_extract(o, '$.id') AS TEXT) AS oid, COALESCE(SUM(TRY_CAST(TRUNC(COALESCE(TRY_CAST(v AS DOUBLE), TRY_CAST(TRY_CAST(v AS BOOLEAN) AS DOUBLE))) AS INTEGER)), 0) AS sum_TRY_CAST_TRUNC_COALESCE_TRY_CAST_v_AS_DOUBLE_TRY_CAST_TRY_CAST_v_AS_BOOLEAN_AS_DOUBLE_AS_INTEGER FROM (SELECT t.*, u.value AS v FROM (SELECT t.*, u.value AS o FROM (SELECT *, CAST(s AS JSON) AS d FROM (SELECT '[{"id":1,"vals":[10,20]},{"id":2,"vals":[30]}]' AS s)) AS t CROSS JOIN UNNEST(d) AS u(value)) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(o, '$.vals')) = 'ARRAY' THEN CAST(json_extract(o, '$.vals') AS JSON[]) ELSE list_transform(json_keys(json_extract(o, '$.vals')), lambda k: json_extract(json_extract(o, '$.vals'), '$.' || k)) END) AS u(value)) GROUP BY ALL
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[oid:String, sum_v:Int] rows=2
    - ('1', 30)
    - ('2', 30)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ...,"vals":[10,20]},{"id":2,"vals":[30]}]' AS s)) AS t CROSS JOIN UNNEST(d) AS u(value)) AS t CROSS JOIN UNNEST(CASE WHEN...
                                                                          ^

### `agent-dynamic-json-0037` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '%(JSON, INTEGER_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	%(TINYINT, TINYINT) -> TINYINT
	%(SMALLINT, SMALLINT) -> SMALLINT
	%(INTEGER, INTEGER) -> INTEGER
	%(BIGINT, BIGINT) -> BIGINT
	%(HUGEINT, HUGEINT) -> HUGEINT
	%(FLOAT, FLOAT) -> FLOAT
	%(DOUBLE, DOUBLE) -> DOUBLE
	%(DECIMAL, DECIMAL) -> DECIMAL
	%(UTINYINT, UTINYINT) -> UTINYINT
	%(USMALLINT, USMALLINT) -> USMALLINT
	%(UINTEGER, UINTEGER) -> UINTEGER
	%(UBIGINT, UBIGINT) -> UBIGINT
	%(UHUGEINT, UHUGEINT) -> UHUGEINT


LINE 1: ...xtract(d, '$.nums'), '$.' || k)) END) AS u(value)) WHERE n % 2 = 0)
                                                                      ^

**KQL**
```kql
print d = dynamic({"nums":[1,2,3,4,5]}) | extend evens = array_length(d.nums) | mv-expand n = d.nums to typeof(long) | where n % 2 == 0 | summarize make_list(n)
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(n) FILTER (WHERE n IS NOT NULL), []) AS list_n FROM (SELECT * FROM (SELECT t.*, u.value AS n FROM (SELECT *, LEN(json_extract(json_extract(d, '$.nums'), '$[*]')) AS evens FROM (SELECT '{"nums":[1,2,3,4,5]}'::JSON AS d)) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(d, '$.nums')) = 'ARRAY' THEN CAST(json_extract(d, '$.nums') AS JSON[]) ELSE list_transform(json_keys(json_extract(d, '$.nums')), lambda k: json_extract(json_extract(d, '$.nums'), '$.' || k)) END) AS u(value)) WHERE n % 2 = 0)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[list_n:Dynamic] rows=1
    - ([
  2,
  4
])
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '%(JSON, INTEGER_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	%(TINYINT, TINYINT) -> TINYINT
	%(SMALLINT, SMALLINT) -> SMALLINT
	%(INTEGER, INTEGER) -> INTEGER
	%(BIGINT, BIGINT) -> BIGINT
	%(HUGEINT, HUGEINT) -> HUGEINT
	%(FLOAT, FLOAT) -> FLOAT
	%(DOUBLE, DOUBLE) -> DOUBLE
	%(DECIMAL, DECIMAL) -> DECIMAL
	%(UTINYINT, UTINYINT) -> UTINYINT
	%(USMALLINT, USMALLINT) -> USMALLINT
	%(UINTEGER, UINTEGER) -> UINTEGER
	%(UBIGINT, UBIGINT) -> UBIGINT
	%(UHUGEINT, UHUGEINT) -> UHUGEINT


LINE 1: ...xtract(d, '$.nums'), '$.' || k)) END) AS u(value)) WHERE n % 2 = 0)
                                                                      ^

### `agent-dynamic-json-0041` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ...'{"x":[1,2],"y":[3,4],"z":[5,6]}'::JSON AS d)) AS t CROSS JOIN UNNEST(flat) AS u(value))
                                                                          ^

**KQL**
```kql
print d = dynamic({"x":[1,2],"y":[3,4],"z":[5,6]}) | extend flat = array_concat(d.x, d.y, d.z) | mv-expand f = flat to typeof(long) | summarize sum(f), avg(f)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(f), 0) AS sum_f, COALESCE(AVG(f), 'nan'::DOUBLE) AS avg_f FROM (SELECT t.*, u.value AS f FROM (SELECT *, TO_JSON(LIST_CONCAT(json_extract(json_extract(d, '$.x'), '$[*]'), json_extract(json_extract(d, '$.y'), '$[*]'), json_extract(json_extract(d, '$.z'), '$[*]'))) AS flat FROM (SELECT '{"x":[1,2],"y":[3,4],"z":[5,6]}'::JSON AS d)) AS t CROSS JOIN UNNEST(flat) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[sum_f:Int, avg_f:Real] rows=1
    - (21, 3.5)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ...'{"x":[1,2],"y":[3,4],"z":[5,6]}'::JSON AS d)) AS t CROSS JOIN UNNEST(flat) AS u(value))
                                                                          ^

### `agent-dynamic-json-0043` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ...) AS d) AS t CROSS JOIN UNNEST(d) AS u(value)) AS t CROSS JOIN UNNEST(row) AS u(value))
                                                                          ^

**KQL**
```kql
print d = dynamic([[1,2,3],[4,5,6]]) | mv-expand row = d | mv-expand cell = row to typeof(long) | summarize grand = sum(cell)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(cell), 0) AS grand FROM (SELECT t.*, u.value AS cell FROM (SELECT t.*, u.value AS row FROM (SELECT LIST_VALUE('[1,2,3]'::JSON, '[4,5,6]'::JSON) AS d) AS t CROSS JOIN UNNEST(d) AS u(value)) AS t CROSS JOIN UNNEST(row) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[grand:Int] rows=1
    - (21)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ...) AS d) AS t CROSS JOIN UNNEST(d) AS u(value)) AS t CROSS JOIN UNNEST(row) AS u(value))
                                                                          ^

### `agent-dynamic-json-0044` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '+(VARCHAR, INTEGER_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	+(TINYINT) -> TINYINT
	+(TINYINT, TINYINT) -> TINYINT
	+(SMALLINT) -> SMALLINT
	+(SMALLINT, SMALLINT) -> SMALLINT
	+(INTEGER) -> INTEGER
	+(INTEGER, INTEGER) -> INTEGER
	+(BIGINT) -> BIGINT
	+(BIGINT, BIGINT) -> BIGINT
	+(HUGEINT) -> HUGEINT
	+(HUGEINT, HUGEINT) -> HUGEINT
	+(FLOAT) -> FLOAT
	+(FLOAT, FLOAT) -> FLOAT
	+(DOUBLE) -> DOUBLE
	+(DOUBLE, DOUBLE) -> DOUBLE
	+(DECIMAL) -> DECIMAL
	+(DECIMAL, DECIMAL) -> DECIMAL
	+(UTINYINT) -> UTINYINT
	+(UTINYINT, UTINYINT) -> UTINYINT
	+(USMALLINT) -> USMALLINT
	+(USMALLINT, USMALLINT) -> USMALLINT
	+(UINTEGER) -> UINTEGER
	+(UINTEGER, UINTEGER) -> UINTEGER
	+(UBIGINT) -> UBIGINT
	+(UBIGINT, UBIGINT) -> UBIGINT
	+(UHUGEINT) -> UHUGEINT
	+(UHUGEINT, UHUGEINT) -> UHUGEINT
	+(DATE, INTEGER) -> DATE
	+(INTEGER, DATE) -> DATE
	+(INTERVAL, INTERVAL) -> INTERVAL
	+(DATE, INTERVAL) -> TIMESTAMP
	+(INTERVAL, DATE) -> TIMESTAMP
	+(TIME, INTERVAL) -> TIME
	+(INTERVAL, TIME) -> TIME
	+(TIMESTAMP, INTERVAL) -> TIMESTAMP
	+(INTERVAL, TIMESTAMP) -> TIMESTAMP
	+(TIME WITH TIME ZONE, INTERVAL) -> TIME WITH TIME ZONE
	+(INTERVAL, TIME WITH TIME ZONE) -> TIME WITH TIME ZONE
	+(TIME, DATE) -> TIMESTAMP
	+(DATE, TIME) -> TIMESTAMP
	+(TIME WITH TIME ZONE, DATE) -> TIMESTAMP WITH TIME ZONE
	+(DATE, TIME WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE
	+([ANY[]...]) -> ANY[]
	+(BIGNUM, BIGNUM) -> BIGNUM
	+(TIMESTAMP WITH TIME ZONE, INTERVAL) -> TIMESTAMP WITH TIME ZONE
	+(INTERVAL, TIMESTAMP WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE


LINE 1: ... INTEGER)) FILTER (WHERE TRY_CAST(TRUNC(COALESCE(TRY_CAST(d[k + 1] AS DOUBLE), TRY_CAST(TRY_CAST(d[k + 1] AS BOOLEAN)...
                                                                         ^

**KQL**
```kql
print d = dynamic({"a":1,"b":2,"c":3,"d":4}) | extend pairs = bag_keys(d) | mv-expand k = pairs to typeof(string) | order by k desc | summarize make_list(k), make_list(toint(d[k]))
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(k) FILTER (WHERE k IS NOT NULL), []) AS list_k, COALESCE(LIST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(d[k + 1] AS DOUBLE), TRY_CAST(TRY_CAST(d[k + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER)) FILTER (WHERE TRY_CAST(TRUNC(COALESCE(TRY_CAST(d[k + 1] AS DOUBLE), TRY_CAST(TRY_CAST(d[k + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) IS NOT NULL), []) AS list_TRY_CAST_TRUNC_COALESCE_TRY_CAST_d_k_1_AS_DOUBLE_TRY_CAST_TRY_CAST_d_k_1_AS_BOOLEAN_AS_DOUBLE_AS_INTEGER FROM (SELECT t.*, u.value AS k FROM (SELECT *, JSON_KEYS(d) AS pairs FROM (SELECT '{"a":1,"b":2,"c":3,"d":4}'::JSON AS d)) AS t CROSS JOIN UNNEST(pairs) AS u(value) ORDER BY k DESC NULLS LAST)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[list_k:Dynamic, list_d___obj_98b907a0-0b22-4423-a34a-283263a16706:Dynamic] rows=1
    - ([
  "d",
  "c",
  "b",
  "a"
], [
  4,
  3,
  2,
  1
])
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '+(VARCHAR, INTEGER_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	+(TINYINT) -> TINYINT
	+(TINYINT, TINYINT) -> TINYINT
	+(SMALLINT) -> SMALLINT
	+(SMALLINT, SMALLINT) -> SMALLINT
	+(INTEGER) -> INTEGER
	+(INTEGER, INTEGER) -> INTEGER
	+(BIGINT) -> BIGINT
	+(BIGINT, BIGINT) -> BIGINT
	+(HUGEINT) -> HUGEINT
	+(HUGEINT, HUGEINT) -> HUGEINT
	+(FLOAT) -> FLOAT
	+(FLOAT, FLOAT) -> FLOAT
	+(DOUBLE) -> DOUBLE
	+(DOUBLE, DOUBLE) -> DOUBLE
	+(DECIMAL) -> DECIMAL
	+(DECIMAL, DECIMAL) -> DECIMAL
	+(UTINYINT) -> UTINYINT
	+(UTINYINT, UTINYINT) -> UTINYINT
	+(USMALLINT) -> USMALLINT
	+(USMALLINT, USMALLINT) -> USMALLINT
	+(UINTEGER) -> UINTEGER
	+(UINTEGER, UINTEGER) -> UINTEGER
	+(UBIGINT) -> UBIGINT
	+(UBIGINT, UBIGINT) -> UBIGINT
	+(UHUGEINT) -> UHUGEINT
	+(UHUGEINT, UHUGEINT) -> UHUGEINT
	+(DATE, INTEGER) -> DATE
	+(INTEGER, DATE) -> DATE
	+(INTERVAL, INTERVAL) -> INTERVAL
	+(DATE, INTERVAL) -> TIMESTAMP
	+(INTERVAL, DATE) -> TIMESTAMP
	+(TIME, INTERVAL) -> TIME
	+(INTERVAL, TIME) -> TIME
	+(TIMESTAMP, INTERVAL) -> TIMESTAMP
	+(INTERVAL, TIMESTAMP) -> TIMESTAMP
	+(TIME WITH TIME ZONE, INTERVAL) -> TIME WITH TIME ZONE
	+(INTERVAL, TIME WITH TIME ZONE) -> TIME WITH TIME ZONE
	+(TIME, DATE) -> TIMESTAMP
	+(DATE, TIME) -> TIMESTAMP
	+(TIME WITH TIME ZONE, DATE) -> TIMESTAMP WITH TIME ZONE
	+(DATE, TIME WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE
	+([ANY[]...]) -> ANY[]
	+(BIGNUM, BIGNUM) -> BIGNUM
	+(TIMESTAMP WITH TIME ZONE, INTERVAL) -> TIMESTAMP WITH TIME ZONE
	+(INTERVAL, TIMESTAMP WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE


LINE 1: ... INTEGER)) FILTER (WHERE TRY_CAST(TRUNC(COALESCE(TRY_CAST(d[k + 1] AS DOUBLE), TRY_CAST(TRY_CAST(d[k + 1] AS BOOLEAN)...
                                                                         ^

### `agent-dynamic-json-0000` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ...{"a":[{"b":[1,2]},{"b":[3,4,5]}]}'::JSON AS d) AS t CROSS JOIN UNNEST(json_extract(d, '$.a')) AS u(value), LATERAL (SELECT...
                                                                          ^

**KQL**
```kql
print d = dynamic({"a":[{"b":[1,2]},{"b":[3,4,5]}]}) | mv-apply o = d.a on (mv-expand v = o.b to typeof(long) | summarize s = sum(v)) | summarize grand = sum(s)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(s), 0) AS grand FROM (SELECT t.*, _sub.* FROM (SELECT '{"a":[{"b":[1,2]},{"b":[3,4,5]}]}'::JSON AS d) AS t CROSS JOIN UNNEST(json_extract(d, '$.a')) AS u(value), LATERAL (SELECT COALESCE(SUM(v), 0) AS s FROM (SELECT t.*, u.value AS v FROM (SELECT u.value AS o) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(o, '$.b')) = 'ARRAY' THEN CAST(json_extract(o, '$.b') AS JSON[]) ELSE list_transform(json_keys(json_extract(o, '$.b')), lambda k: json_extract(json_extract(o, '$.b'), '$.' || k)) END) AS u(value))) AS _sub)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[grand:Int] rows=1
    - (15)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ...{"a":[{"b":[1,2]},{"b":[3,4,5]}]}'::JSON AS d) AS t CROSS JOIN UNNEST(json_extract(d, '$.a')) AS u(value), LATERAL (SELECT...
                                                                          ^

### `agent-dynamic-json-0001` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types 'sum(JSON)'. You might need to add explicit type casts.
	Candidate functions:
	sum(DECIMAL) -> DECIMAL
	sum(BOOLEAN) -> HUGEINT
	sum(SMALLINT) -> HUGEINT
	sum(INTEGER) -> HUGEINT
	sum(BIGINT) -> HUGEINT
	sum(HUGEINT) -> HUGEINT
	sum(DOUBLE) -> DOUBLE
	sum(BIGNUM) -> BIGNUM


LINE 1: ... TRY_CAST(json_extract(o, '$.g') AS TEXT) AS grp, COALESCE(SUM(n), 0) AS total, COUNT(*) AS cnt FROM (SELECT t.*, u...
                                                                      ^

**KQL**
```kql
print d = dynamic([{"g":"x","n":[1,2]},{"g":"y","n":[3]},{"g":"x","n":[4,5,6]}]) | mv-expand o = d | mv-expand n = o.n to typeof(long) | summarize total = sum(n), cnt = count() by grp = tostring(o.g) | sort by grp asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT TRY_CAST(json_extract(o, '$.g') AS TEXT) AS grp, COALESCE(SUM(n), 0) AS total, COUNT(*) AS cnt FROM (SELECT t.*, u.value AS n FROM (SELECT t.*, u.value AS o FROM (SELECT LIST_VALUE('{"g":"x","n":[1,2]}'::JSON, '{"g":"y","n":[3]}'::JSON, '{"g":"x","n":[4,5,6]}'::JSON) AS d) AS t CROSS JOIN UNNEST(d) AS u(value)) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(o, '$.n')) = 'ARRAY' THEN CAST(json_extract(o, '$.n') AS JSON[]) ELSE list_transform(json_keys(json_extract(o, '$.n')), lambda k: json_extract(json_extract(o, '$.n'), '$.' || k)) END) AS u(value)) GROUP BY ALL) ORDER BY grp ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[grp:String, total:Int, cnt:Int] rows=2
    - ('x', 18, 5)
    - ('y', 3, 1)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types 'sum(JSON)'. You might need to add explicit type casts.
	Candidate functions:
	sum(DECIMAL) -> DECIMAL
	sum(BOOLEAN) -> HUGEINT
	sum(SMALLINT) -> HUGEINT
	sum(INTEGER) -> HUGEINT
	sum(BIGINT) -> HUGEINT
	sum(HUGEINT) -> HUGEINT
	sum(DOUBLE) -> DOUBLE
	sum(BIGNUM) -> BIGNUM


LINE 1: ... TRY_CAST(json_extract(o, '$.g') AS TEXT) AS grp, COALESCE(SUM(n), 0) AS total, COUNT(*) AS cnt FROM (SELECT t.*, u...
                                                                      ^

### `agent-dynamic-json-0005` — SqlExecError (highest)

*Detail:* Binder Error: The upper and lower bounds of the slice must be a BIGINT

**KQL**
```kql
print d = dynamic([1,2,3,4,5,6,7,8]) | extend parts = array_split(d, dynamic([2,5])) | extend p0 = parts[0], p1 = parts[1], p2 = parts[2], np = array_length(parts)
```
**Generated SQL**
```sql
SELECT *, parts[0 + 1] AS p0, parts[1 + 1] AS p1, parts[2 + 1] AS p2, LEN(parts) AS np FROM (SELECT *, [LIST_SLICE(d, 1, LIST_VALUE(2, 5)), LIST_SLICE(d, LIST_VALUE(2, 5) + 1, LEN(d))] AS parts FROM (SELECT LIST_VALUE(1, 2, 3, 4, 5, 6, 7, 8) AS d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, parts:Dynamic, p0:Dynamic, p1:Dynamic, p2:Dynamic, np:Int] rows=1
    - ([
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
], [
  [
    1,
    2
  ],
  [
    3,
    4,
    5
  ],
  [
    6,
    7,
    8
  ]
], [
  1,
  2
], [
  3,
  4,
  5
], [
  6,
  7,
  8
], 3)
- DuckDB: ERROR — Binder Error: The upper and lower bounds of the slice must be a BIGINT

### `agent-dynamic-json-0009` — SqlExecError (highest)

*Detail:* Binder Error: Parameter type needs to be List

**KQL**
```kql
print d = dynamic({"vals":[1,2,3]}) | extend doubled = array_iff(dynamic([true,false,true]), d.vals, dynamic([0,0,0]))
```
**Generated SQL**
```sql
SELECT *, LIST_TRANSFORM(LIST_ZIP(LIST_VALUE(TRUE, FALSE, TRUE), json_extract(d, '$.vals'), LIST_VALUE(0, 0, 0)), x -> CASE WHEN x[1] THEN x[2] ELSE x[3] END) AS doubled FROM (SELECT '{"vals":[1,2,3]}'::JSON AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, doubled:Dynamic] rows=1
    - ({
  "vals": [
    1,
    2,
    3
  ]
}, [
  1,
  0,
  3
])
- DuckDB: ERROR — Binder Error: Parameter type needs to be List

### `agent-dynamic-json-0010` — SqlExecError (highest)

*Detail:* Binder Error: Referenced column "i" not found in FROM clause!
Candidate bindings: "d"

LINE 1: ...) AS x, cum FROM (SELECT *, LIST_SUM(LIST_SLICE(d, 0 + 1, i + 1)) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE)...
                                                                     ^

**KQL**
```kql
print d = dynamic([10,20,30]) | mv-expand with_itemindex=i x = d | extend cum = array_sum(array_slice(d, 0, i)) + toint(x) | project i, x = toint(x), cum
```
**Generated SQL**
```sql
SELECT i, TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS x, cum FROM (SELECT *, LIST_SUM(LIST_SLICE(d, 0 + 1, i + 1)) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS cum FROM (SELECT t.*, u.value AS x FROM (SELECT LIST_VALUE(10, 20, 30) AS d) AS t CROSS JOIN UNNEST(d) AS u(value)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[i:Int, x:Int, cum:Real] rows=3
    - (0, 10, 20)
    - (1, 20, 50)
    - (2, 30, 90)
- DuckDB: ERROR — Binder Error: Referenced column "i" not found in FROM clause!
Candidate bindings: "d"

LINE 1: ...) AS x, cum FROM (SELECT *, LIST_SUM(LIST_SLICE(d, 0 + 1, i + 1)) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE)...
                                                                     ^

### `agent-dynamic-json-0016` — SqlExecError (highest)

*Detail:* Binder Error: No matching aggregate function
Binder Error: No function matches the given name and argument types 'sum(JSON)'. You might need to add explicit type casts.
	Candidate functions:
	sum(DECIMAL) -> DECIMAL
	sum(BOOLEAN) -> HUGEINT
	sum(SMALLINT) -> HUGEINT
	sum(INTEGER) -> HUGEINT
	sum(BIGINT) -> HUGEINT
	sum(HUGEINT) -> HUGEINT
	sum(DOUBLE) -> DOUBLE
	sum(BIGNUM) -> BIGNUM


**KQL**
```kql
print d = dynamic([[1,2],[3,4],[5,6]]) | extend cols = array_concat(pack_array(d[0][0],d[1][0],d[2][0]), pack_array(d[0][1],d[1][1],d[2][1])) | extend colsum = array_sum(cols)
```
**Generated SQL**
```sql
SELECT *, LIST_SUM(cols) AS colsum FROM (SELECT *, LIST_CONCAT(LIST_VALUE(d[0 + 1][0 + 1], d[1 + 1][0 + 1], d[2 + 1][0 + 1]), LIST_VALUE(d[0 + 1][1 + 1], d[1 + 1][1 + 1], d[2 + 1][1 + 1])) AS cols FROM (SELECT LIST_VALUE('[1,2]'::JSON, '[3,4]'::JSON, '[5,6]'::JSON) AS d))
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
- DuckDB: ERROR — Binder Error: No matching aggregate function
Binder Error: No function matches the given name and argument types 'sum(JSON)'. You might need to add explicit type casts.
	Candidate functions:
	sum(DECIMAL) -> DECIMAL
	sum(BOOLEAN) -> HUGEINT
	sum(SMALLINT) -> HUGEINT
	sum(INTEGER) -> HUGEINT
	sum(BIGINT) -> HUGEINT
	sum(HUGEINT) -> HUGEINT
	sum(DOUBLE) -> DOUBLE
	sum(BIGNUM) -> BIGNUM


### `agent-dynamic-json-0018` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '+(VARCHAR, INTEGER_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	+(TINYINT) -> TINYINT
	+(TINYINT, TINYINT) -> TINYINT
	+(SMALLINT) -> SMALLINT
	+(SMALLINT, SMALLINT) -> SMALLINT
	+(INTEGER) -> INTEGER
	+(INTEGER, INTEGER) -> INTEGER
	+(BIGINT) -> BIGINT
	+(BIGINT, BIGINT) -> BIGINT
	+(HUGEINT) -> HUGEINT
	+(HUGEINT, HUGEINT) -> HUGEINT
	+(FLOAT) -> FLOAT
	+(FLOAT, FLOAT) -> FLOAT
	+(DOUBLE) -> DOUBLE
	+(DOUBLE, DOUBLE) -> DOUBLE
	+(DECIMAL) -> DECIMAL
	+(DECIMAL, DECIMAL) -> DECIMAL
	+(UTINYINT) -> UTINYINT
	+(UTINYINT, UTINYINT) -> UTINYINT
	+(USMALLINT) -> USMALLINT
	+(USMALLINT, USMALLINT) -> USMALLINT
	+(UINTEGER) -> UINTEGER
	+(UINTEGER, UINTEGER) -> UINTEGER
	+(UBIGINT) -> UBIGINT
	+(UBIGINT, UBIGINT) -> UBIGINT
	+(UHUGEINT) -> UHUGEINT
	+(UHUGEINT, UHUGEINT) -> UHUGEINT
	+(DATE, INTEGER) -> DATE
	+(INTEGER, DATE) -> DATE
	+(INTERVAL, INTERVAL) -> INTERVAL
	+(DATE, INTERVAL) -> TIMESTAMP
	+(INTERVAL, DATE) -> TIMESTAMP
	+(TIME, INTERVAL) -> TIME
	+(INTERVAL, TIME) -> TIME
	+(TIMESTAMP, INTERVAL) -> TIMESTAMP
	+(INTERVAL, TIMESTAMP) -> TIMESTAMP
	+(TIME WITH TIME ZONE, INTERVAL) -> TIME WITH TIME ZONE
	+(INTERVAL, TIME WITH TIME ZONE) -> TIME WITH TIME ZONE
	+(TIME, DATE) -> TIMESTAMP
	+(DATE, TIME) -> TIMESTAMP
	+(TIME WITH TIME ZONE, DATE) -> TIMESTAMP WITH TIME ZONE
	+(DATE, TIME WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE
	+([ANY[]...]) -> ANY[]
	+(BIGNUM, BIGNUM) -> BIGNUM
	+(TIMESTAMP WITH TIME ZONE, INTERVAL) -> TIMESTAMP WITH TIME ZONE
	+(INTERVAL, TIMESTAMP WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE


LINE 1: ...) AS keys FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d[k + 1] AS DOUBLE), TRY_CAST(TRY_CAST(d[k + 1] AS BOOLEAN)...
                                                                          ^

**KQL**
```kql
print d = dynamic({"a":1,"b":2,"c":3,"d":4,"e":5}) | extend ks = array_sort_desc(bag_keys(d)) | mv-expand k = ks to typeof(string) | extend v = toint(d[k]) | summarize vals = make_list(v), keys = make_list(k)
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS vals, COALESCE(LIST(k) FILTER (WHERE k IS NOT NULL), []) AS keys FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d[k + 1] AS DOUBLE), TRY_CAST(TRY_CAST(d[k + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS v FROM (SELECT t.*, u.value AS k FROM (SELECT *, LIST_REVERSE_SORT(JSON_KEYS(d)) AS ks FROM (SELECT '{"a":1,"b":2,"c":3,"d":4,"e":5}'::JSON AS d)) AS t CROSS JOIN UNNEST(ks) AS u(value)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[vals:Dynamic, keys:Dynamic] rows=1
    - ([
  5,
  4,
  3,
  2,
  1
], [
  "e",
  "d",
  "c",
  "b",
  "a"
])
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '+(VARCHAR, INTEGER_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	+(TINYINT) -> TINYINT
	+(TINYINT, TINYINT) -> TINYINT
	+(SMALLINT) -> SMALLINT
	+(SMALLINT, SMALLINT) -> SMALLINT
	+(INTEGER) -> INTEGER
	+(INTEGER, INTEGER) -> INTEGER
	+(BIGINT) -> BIGINT
	+(BIGINT, BIGINT) -> BIGINT
	+(HUGEINT) -> HUGEINT
	+(HUGEINT, HUGEINT) -> HUGEINT
	+(FLOAT) -> FLOAT
	+(FLOAT, FLOAT) -> FLOAT
	+(DOUBLE) -> DOUBLE
	+(DOUBLE, DOUBLE) -> DOUBLE
	+(DECIMAL) -> DECIMAL
	+(DECIMAL, DECIMAL) -> DECIMAL
	+(UTINYINT) -> UTINYINT
	+(UTINYINT, UTINYINT) -> UTINYINT
	+(USMALLINT) -> USMALLINT
	+(USMALLINT, USMALLINT) -> USMALLINT
	+(UINTEGER) -> UINTEGER
	+(UINTEGER, UINTEGER) -> UINTEGER
	+(UBIGINT) -> UBIGINT
	+(UBIGINT, UBIGINT) -> UBIGINT
	+(UHUGEINT) -> UHUGEINT
	+(UHUGEINT, UHUGEINT) -> UHUGEINT
	+(DATE, INTEGER) -> DATE
	+(INTEGER, DATE) -> DATE
	+(INTERVAL, INTERVAL) -> INTERVAL
	+(DATE, INTERVAL) -> TIMESTAMP
	+(INTERVAL, DATE) -> TIMESTAMP
	+(TIME, INTERVAL) -> TIME
	+(INTERVAL, TIME) -> TIME
	+(TIMESTAMP, INTERVAL) -> TIMESTAMP
	+(INTERVAL, TIMESTAMP) -> TIMESTAMP
	+(TIME WITH TIME ZONE, INTERVAL) -> TIME WITH TIME ZONE
	+(INTERVAL, TIME WITH TIME ZONE) -> TIME WITH TIME ZONE
	+(TIME, DATE) -> TIMESTAMP
	+(DATE, TIME) -> TIMESTAMP
	+(TIME WITH TIME ZONE, DATE) -> TIMESTAMP WITH TIME ZONE
	+(DATE, TIME WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE
	+([ANY[]...]) -> ANY[]
	+(BIGNUM, BIGNUM) -> BIGNUM
	+(TIMESTAMP WITH TIME ZONE, INTERVAL) -> TIMESTAMP WITH TIME ZONE
	+(INTERVAL, TIMESTAMP WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE


LINE 1: ...) AS keys FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d[k + 1] AS DOUBLE), TRY_CAST(TRY_CAST(d[k + 1] AS BOOLEAN)...
                                                                          ^

### `agent-dynamic-json-0024` — SqlExecError (highest)

*Detail:* Binder Error: Parameter type needs to be List

**KQL**
```kql
print d = dynamic({"a":[1,2,3],"b":[4,5,6]}) | extend zipped = zip(d.a, d.b) | mv-expand z = zipped | project lhs = toint(z[0]), rhs = toint(z[1]), prod = toint(z[0]) * toint(z[1])
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(z[0 + 1] AS DOUBLE), TRY_CAST(TRY_CAST(z[0 + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS lhs, TRY_CAST(TRUNC(COALESCE(TRY_CAST(z[1 + 1] AS DOUBLE), TRY_CAST(TRY_CAST(z[1 + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS rhs, TRY_CAST(TRUNC(COALESCE(TRY_CAST(z[0 + 1] AS DOUBLE), TRY_CAST(TRY_CAST(z[0 + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) * TRY_CAST(TRUNC(COALESCE(TRY_CAST(z[1 + 1] AS DOUBLE), TRY_CAST(TRY_CAST(z[1 + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS prod FROM (SELECT t.*, u.value AS z FROM (SELECT *, LIST_ZIP(json_extract(d, '$.a'), json_extract(d, '$.b')) AS zipped FROM (SELECT '{"a":[1,2,3],"b":[4,5,6]}'::JSON AS d)) AS t CROSS JOIN UNNEST(zipped) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[lhs:Int, rhs:Int, prod:Int] rows=3
    - (1, 4, 4)
    - (2, 5, 10)
    - (3, 6, 18)
- DuckDB: ERROR — Binder Error: Parameter type needs to be List

### `agent-dynamic-json-0025` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types 'sum(VARCHAR)'. You might need to add explicit type casts.
	Candidate functions:
	sum(DECIMAL) -> DECIMAL
	sum(BOOLEAN) -> HUGEINT
	sum(SMALLINT) -> HUGEINT
	sum(INTEGER) -> HUGEINT
	sum(BIGINT) -> HUGEINT
	sum(HUGEINT) -> HUGEINT
	sum(DOUBLE) -> DOUBLE
	sum(BIGNUM) -> BIGNUM


LINE 1: SELECT * FROM (SELECT type, COALESCE(SUM(v), 0) AS sum_v FROM (SELECT * EXCLUDE (d), d->>'$.type...
                                             ^

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"type":"A","v":1}), dynamic({"type":"B","v":2}), dynamic({"type":"A","v":3}) ] | evaluate bag_unpack(d) | summarize sum(v) by type | sort by type asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT type, COALESCE(SUM(v), 0) AS sum_v FROM (SELECT * EXCLUDE (d), d->>'$.type' AS type, d->>'$.v' AS v FROM (VALUES ('{"type":"A","v":1}'::JSON), ('{"type":"B","v":2}'::JSON), ('{"type":"A","v":3}'::JSON)) AS t(d)) GROUP BY ALL) ORDER BY type ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[type:String, sum_v:Int] rows=2
    - ('A', 4)
    - ('B', 2)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types 'sum(VARCHAR)'. You might need to add explicit type casts.
	Candidate functions:
	sum(DECIMAL) -> DECIMAL
	sum(BOOLEAN) -> HUGEINT
	sum(SMALLINT) -> HUGEINT
	sum(INTEGER) -> HUGEINT
	sum(BIGINT) -> HUGEINT
	sum(HUGEINT) -> HUGEINT
	sum(DOUBLE) -> DOUBLE
	sum(BIGNUM) -> BIGNUM


LINE 1: SELECT * FROM (SELECT type, COALESCE(SUM(v), 0) AS sum_v FROM (SELECT * EXCLUDE (d), d->>'$.type...
                                             ^

### `agent-dynamic-json-0026` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ... '{"matrix":[[1,2,3],[4,5,6]]}'::JSON AS d) AS t CROSS JOIN UNNEST(json_extract(d, '$.matrix')) AS u(value), LATERAL...
                                                                       ^

**KQL**
```kql
print d = dynamic({"matrix":[[1,2,3],[4,5,6]]}) | mv-apply row = d.matrix on (extend rs = array_sum(row)) | summarize total = sum(rs)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(rs), 0) AS total FROM (SELECT t.*, _sub.* FROM (SELECT '{"matrix":[[1,2,3],[4,5,6]]}'::JSON AS d) AS t CROSS JOIN UNNEST(json_extract(d, '$.matrix')) AS u(value), LATERAL (SELECT *, LIST_SUM(row) AS rs FROM (SELECT u.value AS row)) AS _sub)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[total:Real] rows=1
    - (21)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ... '{"matrix":[[1,2,3],[4,5,6]]}'::JSON AS d) AS t CROSS JOIN UNNEST(json_extract(d, '$.matrix')) AS u(value), LATERAL...
                                                                       ^

### `agent-dynamic-json-0027` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types 'list_aggr(JSON, STRING_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	list_aggr(ANY[], VARCHAR, [ANY...]) -> ANY


LINE 1: SELECT *, LEN(e) AS al, LIST_SUM(e) AS s2, LIST_SORT(e) AS srt...
               ^

**KQL**
```kql
print s = '[]' | extend e = parse_json(s) | extend al = array_length(e), s2 = array_sum(e), srt = array_sort_asc(e), keys = bag_keys(e)
```
**Generated SQL**
```sql
SELECT *, LEN(e) AS al, LIST_SUM(e) AS s2, LIST_SORT(e) AS srt, JSON_KEYS(e) AS keys FROM (SELECT *, CAST(s AS JSON) AS e FROM (SELECT '[]' AS s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, e:Dynamic, al:Int, s2:Real, srt:Dynamic, keys:Dynamic] rows=1
    - ('[]', [], 0, null, [], null)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types 'list_aggr(JSON, STRING_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	list_aggr(ANY[], VARCHAR, [ANY...]) -> ANY


LINE 1: SELECT *, LEN(e) AS al, LIST_SUM(e) AS s2, LIST_SORT(e) AS srt...
               ^

### `agent-dynamic-json-0032` — SqlExecError (highest)

*Detail:* Binder Error: Referenced column "i" not found in FROM clause!
Candidate bindings: "arr"

LINE 1: ... AS idx FROM (SELECT *, JSON_MERGE_PATCH(e, json_object('idx', i)) AS tagged FROM (SELECT t.*, u.value AS e FROM (SELECT...
                                                                          ^

**KQL**
```kql
print arr = dynamic([{"k":1},{"k":2},{"k":3}]) | mv-expand with_itemindex=i e = arr | extend tagged = bag_merge(e, pack("idx", i)) | project ki = toint(tagged.k), idx = toint(tagged.idx)
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(tagged, '$.k') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(tagged, '$.k') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ki, TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(tagged, '$.idx') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(tagged, '$.idx') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS idx FROM (SELECT *, JSON_MERGE_PATCH(e, json_object('idx', i)) AS tagged FROM (SELECT t.*, u.value AS e FROM (SELECT LIST_VALUE('{"k":1}'::JSON, '{"k":2}'::JSON, '{"k":3}'::JSON) AS arr) AS t CROSS JOIN UNNEST(arr) AS u(value)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ki:Int, idx:Int] rows=3
    - (1, 0)
    - (2, 1)
    - (3, 2)
- DuckDB: ERROR — Binder Error: Referenced column "i" not found in FROM clause!
Candidate bindings: "arr"

LINE 1: ... AS idx FROM (SELECT *, JSON_MERGE_PATCH(e, json_object('idx', i)) AS tagged FROM (SELECT t.*, u.value AS e FROM (SELECT...
                                                                          ^

### `agent-dynamic-json-0035` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ...,2]},{"g":"y","items":[3,4,5]}]}'::JSON AS d) AS t CROSS JOIN UNNEST(json_extract(d, '$.groups')) AS u(value), LATERAL...
                                                                         ^

**KQL**
```kql
print d = dynamic({"groups":[{"g":"x","items":[1,2]},{"g":"y","items":[3,4,5]}]}) | mv-apply grp = d.groups on (extend gs = array_sum(grp.items), gn = tostring(grp.g)) | project gn, gs | sort by gn asc
```
**Generated SQL**
```sql
SELECT gn, gs FROM (SELECT t.*, _sub.* FROM (SELECT '{"groups":[{"g":"x","items":[1,2]},{"g":"y","items":[3,4,5]}]}'::JSON AS d) AS t CROSS JOIN UNNEST(json_extract(d, '$.groups')) AS u(value), LATERAL (SELECT *, LIST_SUM(LIST_TRANSFORM(json_extract(json_extract(grp, '$.items'), '$[*]'), x -> CAST(x AS DOUBLE))) AS gs, TRY_CAST(json_extract(grp, '$.g') AS TEXT) AS gn FROM (SELECT u.value AS grp)) AS _sub) ORDER BY gn ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[gn:String, gs:Real] rows=2
    - ('x', 3)
    - ('y', 12)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ...,2]},{"g":"y","items":[3,4,5]}]}'::JSON AS d) AS t CROSS JOIN UNNEST(json_extract(d, '$.groups')) AS u(value), LATERAL...
                                                                         ^

### `agent-dynamic-json-0038` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '+(VARCHAR, INTEGER_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	+(TINYINT) -> TINYINT
	+(TINYINT, TINYINT) -> TINYINT
	+(SMALLINT) -> SMALLINT
	+(SMALLINT, SMALLINT) -> SMALLINT
	+(INTEGER) -> INTEGER
	+(INTEGER, INTEGER) -> INTEGER
	+(BIGINT) -> BIGINT
	+(BIGINT, BIGINT) -> BIGINT
	+(HUGEINT) -> HUGEINT
	+(HUGEINT, HUGEINT) -> HUGEINT
	+(FLOAT) -> FLOAT
	+(FLOAT, FLOAT) -> FLOAT
	+(DOUBLE) -> DOUBLE
	+(DOUBLE, DOUBLE) -> DOUBLE
	+(DECIMAL) -> DECIMAL
	+(DECIMAL, DECIMAL) -> DECIMAL
	+(UTINYINT) -> UTINYINT
	+(UTINYINT, UTINYINT) -> UTINYINT
	+(USMALLINT) -> USMALLINT
	+(USMALLINT, USMALLINT) -> USMALLINT
	+(UINTEGER) -> UINTEGER
	+(UINTEGER, UINTEGER) -> UINTEGER
	+(UBIGINT) -> UBIGINT
	+(UBIGINT, UBIGINT) -> UBIGINT
	+(UHUGEINT) -> UHUGEINT
	+(UHUGEINT, UHUGEINT) -> UHUGEINT
	+(DATE, INTEGER) -> DATE
	+(INTEGER, DATE) -> DATE
	+(INTERVAL, INTERVAL) -> INTERVAL
	+(DATE, INTERVAL) -> TIMESTAMP
	+(INTERVAL, DATE) -> TIMESTAMP
	+(TIME, INTERVAL) -> TIME
	+(INTERVAL, TIME) -> TIME
	+(TIMESTAMP, INTERVAL) -> TIMESTAMP
	+(INTERVAL, TIMESTAMP) -> TIMESTAMP
	+(TIME WITH TIME ZONE, INTERVAL) -> TIME WITH TIME ZONE
	+(INTERVAL, TIME WITH TIME ZONE) -> TIME WITH TIME ZONE
	+(TIME, DATE) -> TIMESTAMP
	+(DATE, TIME) -> TIMESTAMP
	+(TIME WITH TIME ZONE, DATE) -> TIMESTAMP WITH TIME ZONE
	+(DATE, TIME WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE
	+([ANY[]...]) -> ANY[]
	+(BIGNUM, BIGNUM) -> BIGNUM
	+(TIMESTAMP WITH TIME ZONE, INTERVAL) -> TIMESTAMP WITH TIME ZONE
	+(INTERVAL, TIMESTAMP WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE


LINE 1: ... (WHERE k IS NOT NULL), []) AS allkeys FROM (SELECT *, d[k + 1] AS arr, LIST_SUM(d[k + 1]) AS s FROM (SELECT t.*, u...
                                                                      ^

**KQL**
```kql
print d = dynamic({"a":[1,2],"b":[3,4],"c":[5,6]}) | extend ks = bag_keys(d) | mv-expand k = ks to typeof(string) | extend arr = d[k], s = array_sum(d[k]) | summarize grand = sum(s), allkeys = make_list(k)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(s), 0) AS grand, COALESCE(LIST(k) FILTER (WHERE k IS NOT NULL), []) AS allkeys FROM (SELECT *, d[k + 1] AS arr, LIST_SUM(d[k + 1]) AS s FROM (SELECT t.*, u.value AS k FROM (SELECT *, JSON_KEYS(d) AS ks FROM (SELECT '{"a":[1,2],"b":[3,4],"c":[5,6]}'::JSON AS d)) AS t CROSS JOIN UNNEST(ks) AS u(value)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[grand:Real, allkeys:Dynamic] rows=1
    - (21, [
  "a",
  "b",
  "c"
])
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '+(VARCHAR, INTEGER_LITERAL)'. You might need to add explicit type casts.
	Candidate functions:
	+(TINYINT) -> TINYINT
	+(TINYINT, TINYINT) -> TINYINT
	+(SMALLINT) -> SMALLINT
	+(SMALLINT, SMALLINT) -> SMALLINT
	+(INTEGER) -> INTEGER
	+(INTEGER, INTEGER) -> INTEGER
	+(BIGINT) -> BIGINT
	+(BIGINT, BIGINT) -> BIGINT
	+(HUGEINT) -> HUGEINT
	+(HUGEINT, HUGEINT) -> HUGEINT
	+(FLOAT) -> FLOAT
	+(FLOAT, FLOAT) -> FLOAT
	+(DOUBLE) -> DOUBLE
	+(DOUBLE, DOUBLE) -> DOUBLE
	+(DECIMAL) -> DECIMAL
	+(DECIMAL, DECIMAL) -> DECIMAL
	+(UTINYINT) -> UTINYINT
	+(UTINYINT, UTINYINT) -> UTINYINT
	+(USMALLINT) -> USMALLINT
	+(USMALLINT, USMALLINT) -> USMALLINT
	+(UINTEGER) -> UINTEGER
	+(UINTEGER, UINTEGER) -> UINTEGER
	+(UBIGINT) -> UBIGINT
	+(UBIGINT, UBIGINT) -> UBIGINT
	+(UHUGEINT) -> UHUGEINT
	+(UHUGEINT, UHUGEINT) -> UHUGEINT
	+(DATE, INTEGER) -> DATE
	+(INTEGER, DATE) -> DATE
	+(INTERVAL, INTERVAL) -> INTERVAL
	+(DATE, INTERVAL) -> TIMESTAMP
	+(INTERVAL, DATE) -> TIMESTAMP
	+(TIME, INTERVAL) -> TIME
	+(INTERVAL, TIME) -> TIME
	+(TIMESTAMP, INTERVAL) -> TIMESTAMP
	+(INTERVAL, TIMESTAMP) -> TIMESTAMP
	+(TIME WITH TIME ZONE, INTERVAL) -> TIME WITH TIME ZONE
	+(INTERVAL, TIME WITH TIME ZONE) -> TIME WITH TIME ZONE
	+(TIME, DATE) -> TIMESTAMP
	+(DATE, TIME) -> TIMESTAMP
	+(TIME WITH TIME ZONE, DATE) -> TIMESTAMP WITH TIME ZONE
	+(DATE, TIME WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE
	+([ANY[]...]) -> ANY[]
	+(BIGNUM, BIGNUM) -> BIGNUM
	+(TIMESTAMP WITH TIME ZONE, INTERVAL) -> TIMESTAMP WITH TIME ZONE
	+(INTERVAL, TIMESTAMP WITH TIME ZONE) -> TIMESTAMP WITH TIME ZONE


LINE 1: ... (WHERE k IS NOT NULL), []) AS allkeys FROM (SELECT *, d[k + 1] AS arr, LIST_SUM(d[k + 1]) AS s FROM (SELECT t.*, u...
                                                                      ^

### `t1-dynamic-json-0005` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types 'list_sort(JSON)'. You might need to add explicit type casts.
	Candidate functions:
	list_sort(ANY[]) -> ANY[]
	list_sort(ANY[], VARCHAR) -> ANY[]
	list_sort(ANY[], VARCHAR, VARCHAR) -> ANY[]


LINE 1: SELECT LIST_SORT(d) AS v FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON...
               ^

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]), dynamic({"nested":{"x":"y"}}), dynamic(null) ] | project v = array_sort_asc(d)
```
**Generated SQL**
```sql
SELECT LIST_SORT(d) AS v FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON), (LIST_VALUE(10, 20, 30)), ('{"nested":{"x":"y"}}'::JSON), (NULL)) AS t(d)
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
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types 'list_sort(JSON)'. You might need to add explicit type casts.
	Candidate functions:
	list_sort(ANY[]) -> ANY[]
	list_sort(ANY[], VARCHAR) -> ANY[]
	list_sort(ANY[], VARCHAR, VARCHAR) -> ANY[]


LINE 1: SELECT LIST_SORT(d) AS v FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON...
               ^

### `t1-dynamic-json-0006` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types 'list_concat(JSON, INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	list_concat([ANY[]...]) -> ANY[]


LINE 1: SELECT LIST_CONCAT(d, LIST_VALUE(99)) AS v FROM (VALUES ('{"a"...
               ^

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]), dynamic({"nested":{"x":"y"}}), dynamic(null) ] | project v = array_concat(d, dynamic([99]))
```
**Generated SQL**
```sql
SELECT LIST_CONCAT(d, LIST_VALUE(99)) AS v FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON), (LIST_VALUE(10, 20, 30)), ('{"nested":{"x":"y"}}'::JSON), (NULL)) AS t(d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[v:Dynamic] rows=4
    - ([
  99
])
    - ([
  10,
  20,
  30,
  99
])
    - ([
  99
])
    - ([
  99
])
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types 'list_concat(JSON, INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	list_concat([ANY[]...]) -> ANY[]


LINE 1: SELECT LIST_CONCAT(d, LIST_VALUE(99)) AS v FROM (VALUES ('{"a"...
               ^

### `t1-dynamic-json-0008` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ...{"nested":{"x":"y"}}'::JSON), (NULL)) AS t(d)) AS t CROSS JOIN UNNEST(t.d) AS u(value)
                                                                          ^

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]), dynamic({"nested":{"x":"y"}}), dynamic(null) ] | mv-expand d
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (d), u.value AS d FROM (SELECT * FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON), (LIST_VALUE(10, 20, 30)), ('{"nested":{"x":"y"}}'::JSON), (NULL)) AS t(d)) AS t CROSS JOIN UNNEST(t.d) AS u(value)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic] rows=7
    - ({
  "a": 1
})
    - ({
  "b": [
    1,
    2,
    3
  ]
})
    - (10)
    - (20)
    - (30)
    - ({
  "nested": {
    "x": "y"
  }
})
    - (null)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ...{"nested":{"x":"y"}}'::JSON), (NULL)) AS t(d)) AS t CROSS JOIN UNNEST(t.d) AS u(value)
                                                                          ^

### `agent-dynamic-json-0000` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=({
  "a": 1,
  "b": [
    1,
    2,
    3
  ]
}, null) duck=('{"a":1,"b":[1,2,3]}', 19)

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]), dynamic({"x":{"y":1}}) ] | extend al = array_length(d)
```
**Generated SQL**
```sql
SELECT *, LEN(d) AS al FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON), (LIST_VALUE(10, 20, 30)), ('{"x":{"y":1}}'::JSON)) AS t(d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, al:Int] rows=3
    - ({
  "a": 1,
  "b": [
    1,
    2,
    3
  ]
}, null)
    - ([
  10,
  20,
  30
], 3)
    - ({
  "x": {
    "y": 1
  }
}, null)
- DuckDB: cols=[d:String, al:Int] rows=3
    - ('{"a":1,"b":[1,2,3]}', 19)
    - ('[10,20,30]', 10)
    - ('{"x":{"y":1}}', 13)

### `agent-dynamic-json-0006` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String], TYPE_MISMATCH[m:Dynamic|String]  
*Detail:* first differing row[0]: kusto=({
  "a": 1,
  "b": 2
}, {
  "a": 1,
  "b": 2,
  "c": 4
}) duck=('{"a":1,"b":2}', '{"a":1,"b":3,"c":4}')

**KQL**
```kql
print d = dynamic({"a":1,"b":2}) | extend m = bag_merge(d, dynamic({"b":3,"c":4}))
```
**Generated SQL**
```sql
SELECT *, JSON_MERGE_PATCH(d, '{"b":3,"c":4}'::JSON) AS m FROM (SELECT '{"a":1,"b":2}'::JSON AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, m:Dynamic] rows=1
    - ({
  "a": 1,
  "b": 2
}, {
  "a": 1,
  "b": 2,
  "c": 4
})
- DuckDB: cols=[d:String, m:String] rows=1
    - ('{"a":1,"b":2}', '{"a":1,"b":3,"c":4}')

### `agent-dynamic-json-0010` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=({}, null, []) duck=('{}', 2, [])

**KQL**
```kql
print d = dynamic({}) | extend e = array_length(d), keys = bag_keys(d)
```
**Generated SQL**
```sql
SELECT *, LEN(d) AS e, JSON_KEYS(d) AS keys FROM (SELECT '{}'::JSON AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, e:Int, keys:Dynamic] rows=1
    - ({}, null, [])
- DuckDB: cols=[d:String, e:Int, keys:Unknown] rows=1
    - ('{}', 2, [])

### `agent-dynamic-json-0015` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=('[1,2,3]', [
  1,
  2,
  3
], 3) duck=('[1,2,3]', '[1,2,3]', 7)

**KQL**
```kql
print s = '[1,2,3]' | extend d = parse_json(s) | extend len = array_length(d)
```
**Generated SQL**
```sql
SELECT *, LEN(d) AS len FROM (SELECT *, CAST(s AS JSON) AS d FROM (SELECT '[1,2,3]' AS s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, d:Dynamic, len:Int] rows=1
    - ('[1,2,3]', [
  1,
  2,
  3
], 3)
- DuckDB: cols=[s:String, d:String, len:Int] rows=1
    - ('[1,2,3]', '[1,2,3]', 7)

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
SELECT *, CAST(d AS JSON) AS j FROM (SELECT '{"b":2,"a":1}'::JSON AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, j:String] rows=1
    - ({
  "b": 2,
  "a": 1
}, '{"a":1,"b":2}')
- DuckDB: cols=[d:String, j:String] rows=1
    - ('{"b":2,"a":1}', '{"b":2,"a":1}')

### `agent-dynamic-json-0021` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[x:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(null, 'null', True) duck=('null', 'dictionary', False)

**KQL**
```kql
print x = parse_json("null") | extend t = gettype(x), isn = isnull(x)
```
**Generated SQL**
```sql
SELECT *, CASE WHEN TYPEOF(x) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(x) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(x) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(x) = 'VARCHAR' THEN 'string' WHEN TYPEOF(x) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(x) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(x) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(x) = 'UUID' THEN 'guid' WHEN TYPEOF(x) LIKE '%[]' OR TYPEOF(x) LIKE 'STRUCT%' OR TYPEOF(x) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(x) = 'JSON' THEN (CASE WHEN json_type(CAST(x AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(x)) END AS t, (x IS NULL) AS isn FROM (SELECT CAST('null' AS JSON) AS x)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Dynamic, t:String, isn:Bool] rows=1
    - (null, 'null', True)
- DuckDB: cols=[x:String, t:String, isn:Bool] rows=1
    - ('null', 'dictionary', False)

### `agent-dynamic-json-0023` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[x:Dynamic|String], TYPE_MISMATCH[v:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(123, 'long', 123) duck=('123', 'dictionary', '123')

**KQL**
```kql
print x = parse_json("123") | extend t = gettype(x), v = x
```
**Generated SQL**
```sql
SELECT *, CASE WHEN TYPEOF(x) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(x) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(x) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(x) = 'VARCHAR' THEN 'string' WHEN TYPEOF(x) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(x) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(x) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(x) = 'UUID' THEN 'guid' WHEN TYPEOF(x) LIKE '%[]' OR TYPEOF(x) LIKE 'STRUCT%' OR TYPEOF(x) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(x) = 'JSON' THEN (CASE WHEN json_type(CAST(x AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(x)) END AS t, x AS v FROM (SELECT CAST('123' AS JSON) AS x)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Dynamic, t:String, v:Dynamic] rows=1
    - (123, 'long', 123)
- DuckDB: cols=[x:String, t:String, v:String] rows=1
    - ('123', 'dictionary', '123')

### `agent-dynamic-json-0024` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[x:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(true, 'bool') duck=('true', 'dictionary')

**KQL**
```kql
print x = parse_json("true") | extend t = gettype(x)
```
**Generated SQL**
```sql
SELECT *, CASE WHEN TYPEOF(x) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(x) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(x) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(x) = 'VARCHAR' THEN 'string' WHEN TYPEOF(x) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(x) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(x) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(x) = 'UUID' THEN 'guid' WHEN TYPEOF(x) LIKE '%[]' OR TYPEOF(x) LIKE 'STRUCT%' OR TYPEOF(x) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(x) = 'JSON' THEN (CASE WHEN json_type(CAST(x AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(x)) END AS t FROM (SELECT CAST('true' AS JSON) AS x)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Dynamic, t:String] rows=1
    - (true, 'bool')
- DuckDB: cols=[x:String, t:String] rows=1
    - ('true', 'dictionary')

### `agent-dynamic-json-0028` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('red', 1) duck=('"blue"', 1)

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"tags":["red","green","blue"]}) ] | mv-expand tag = d.tags | summarize cnt = count() by tostring(tag)
```
**Generated SQL**
```sql
SELECT TRY_CAST(tag AS TEXT) AS tag, COUNT(*) AS cnt FROM (SELECT t.*, u.value AS tag FROM (SELECT * FROM (VALUES ('{"tags":["red","green","blue"]}'::JSON)) AS t(d)) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(d, '$.tags')) = 'ARRAY' THEN CAST(json_extract(d, '$.tags') AS JSON[]) ELSE list_transform(json_keys(json_extract(d, '$.tags')), lambda k: json_extract(json_extract(d, '$.tags'), '$.' || k)) END) AS u(value)) GROUP BY ALL
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[tag:String, cnt:Int] rows=3
    - ('red', 1)
    - ('green', 1)
    - ('blue', 1)
- DuckDB: cols=[tag:String, cnt:Int] rows=3
    - ('"blue"', 1)
    - ('"red"', 1)
    - ('"green"', 1)

### `agent-dynamic-json-0029` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[inner:Dynamic|String], TYPE_MISMATCH[el:Dynamic|String]  
*Detail:* first differing row[0]: kusto=([
  [
    1,
    2
  ],
  [
    3,
    4
  ]
], [
  1,
  2
], 2) duck=(["[1,2]","[3,4]"], '[1,2]', null)

**KQL**
```kql
print d = dynamic([[1,2],[3,4]]) | extend inner = d[0], el = d[0][1]
```
**Generated SQL**
```sql
SELECT *, d[0 + 1] AS "inner", d[0 + 1][1 + 1] AS el FROM (SELECT LIST_VALUE('[1,2]'::JSON, '[3,4]'::JSON) AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, inner:Dynamic, el:Dynamic] rows=1
    - ([
  [
    1,
    2
  ],
  [
    3,
    4
  ]
], [
  1,
  2
], 2)
- DuckDB: cols=[d:Unknown, inner:String, el:String] rows=1
    - (["[1,2]","[3,4]"], '[1,2]', null)

### `agent-dynamic-json-0032` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3,
  4,
  5
], [
  5,
  4,
  3,
  2,
  1
], [
  4,
  5
]) duck=([1,2,3,4,5], [5,4,3,2,1], [])

**KQL**
```kql
print d = dynamic([1,2,3,4,5]) | extend rev = array_reverse(d), tail2 = array_slice(d, -2, -1)
```
**Generated SQL**
```sql
SELECT *, LIST_REVERSE(d) AS rev, LIST_SLICE(d, (-2) + 1, (-1) + 1) AS tail2 FROM (SELECT LIST_VALUE(1, 2, 3, 4, 5) AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, rev:Dynamic, tail2:Dynamic] rows=1
    - ([
  1,
  2,
  3,
  4,
  5
], [
  5,
  4,
  3,
  2,
  1
], [
  4,
  5
])
- DuckDB: cols=[d:Unknown, rev:Unknown, tail2:Unknown] rows=1
    - ([1,2,3,4,5], [5,4,3,2,1], [])

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

### `agent-dynamic-json-0042` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[b:Dynamic|Int], TYPE_MISMATCH[m:Dynamic|String]  
*Detail:* first differing row[0]: kusto=([
  1,
  2
], null, {
  "k": 1
}, [
  1,
  2
]) duck=([1,2], null, null, [1,2])

**KQL**
```kql
print a = dynamic([1,2]), b = dynamic(null) | extend m = bag_merge(dynamic({"k":1}), dynamic(null)), c = array_concat(a, b)
```
**Generated SQL**
```sql
SELECT *, JSON_MERGE_PATCH('{"k":1}'::JSON, NULL) AS m, LIST_CONCAT(a, b) AS c FROM (SELECT LIST_VALUE(1, 2) AS a, NULL AS b)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Dynamic, b:Dynamic, m:Dynamic, c:Dynamic] rows=1
    - ([
  1,
  2
], null, {
  "k": 1
}, [
  1,
  2
])
- DuckDB: cols=[a:Unknown, b:Int, m:String, c:Unknown] rows=1
    - ([1,2], null, null, [1,2])

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
SELECT *, json_extract(d, '$.' || TRY_CAST(keys AS TEXT)) AS kv FROM (SELECT t.* EXCLUDE (keys), u.value AS keys FROM (SELECT *, JSON_KEYS(d) AS keys FROM (SELECT '{"a":1,"b":2}'::JSON AS d)) AS t CROSS JOIN UNNEST(t.keys) AS u(value))
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

### `agent-dynamic-json-0001` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a', 4) duck=('"a"', 4)

**KQL**
```kql
datatable(d:dynamic)[ dynamic([{"k":"a","v":1},{"k":"b","v":2},{"k":"a","v":3}]) ] | mv-expand item = d | summarize tot = sum(toint(item.v)) by g = tostring(item.k) | sort by g asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT TRY_CAST(json_extract(item, '$.k') AS TEXT) AS g, COALESCE(SUM(TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(item, '$.v') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(item, '$.v') AS BOOLEAN) AS DOUBLE))) AS INTEGER)), 0) AS tot FROM (SELECT t.*, u.value AS item FROM (SELECT * FROM (VALUES (LIST_VALUE('{"k":"a","v":1}'::JSON, '{"k":"b","v":2}'::JSON, '{"k":"a","v":3}'::JSON))) AS t(d)) AS t CROSS JOIN UNNEST(d) AS u(value)) GROUP BY ALL) ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, tot:Int] rows=2
    - ('a', 4)
    - ('b', 2)
- DuckDB: cols=[g:String, tot:Int] rows=2
    - ('"a"', 4)
    - ('"b"', 2)

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
SELECT *, json_extract(json_extract(d, '$.a.b.c'), '$[' || (2) || ']') AS deep, LEN(json_extract(json_extract(d, '$.a.b.c'), '$[*]')) AS len, CAST(TRY_CAST(json_extract(d, '$.a.b.c') AS TEXT) AS JSON)[1 + 1] AS rt FROM (SELECT '{"a":{"b":{"c":[10,20,30]}}}'::JSON AS d)
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

### `agent-dynamic-json-0003` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(6, 3) duck=(15, 7)

**KQL**
```kql
print outer = dynamic([[1,2,3],[4,5],[6]]) | mv-expand inner = outer | extend l = array_length(inner) | summarize total = sum(l), maxlen = max(l)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(l), 0) AS total, MAX(l) AS maxlen FROM (SELECT *, LEN("inner") AS l FROM (SELECT t.*, u.value AS inner FROM (SELECT LIST_VALUE('[1,2,3]'::JSON, '[4,5]'::JSON, '[6]'::JSON) AS outer) AS t CROSS JOIN UNNEST("outer") AS u(value)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[total:Int, maxlen:Int] rows=1
    - (6, 3)
- DuckDB: cols=[total:Int, maxlen:Int] rows=1
    - (15, 7)

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
SELECT *, JSON_KEYS(merged) AS keys FROM (SELECT histogram(d) AS merged FROM (VALUES ('{"a":1}'::JSON), ('{"b":2}'::JSON), ('{"a":3,"c":4}'::JSON)) AS t(d))
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

### `agent-dynamic-json-0006` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3
], [
  3,
  4,
  5
], [
  5,
  6,
  7
], [
  1,
  2,
  3,
  4,
  5,
  6,
  7
], []) duck=([1,2,3], [3,4,5], [5,6,7], [1,2,3,4,5], [])

**KQL**
```kql
print a = dynamic([1,2,3]), b = dynamic([3,4,5]), c = dynamic([5,6,7]) | extend u = set_union(a, b, c), i = set_intersect(a, set_intersect(b, c))
```
**Generated SQL**
```sql
SELECT *, LIST_DISTINCT(LIST_CONCAT(a, b)) AS u, LIST_FILTER(a, x -> LIST_CONTAINS(LIST_FILTER(b, x -> LIST_CONTAINS(c, x)), x)) AS i FROM (SELECT LIST_VALUE(1, 2, 3) AS a, LIST_VALUE(3, 4, 5) AS b, LIST_VALUE(5, 6, 7) AS c)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Dynamic, b:Dynamic, c:Dynamic, u:Dynamic, i:Dynamic] rows=1
    - ([
  1,
  2,
  3
], [
  3,
  4,
  5
], [
  5,
  6,
  7
], [
  1,
  2,
  3,
  4,
  5,
  6,
  7
], [])
- DuckDB: cols=[a:Unknown, b:Unknown, c:Unknown, u:Unknown, i:Unknown] rows=1
    - ([1,2,3], [3,4,5], [5,6,7], [1,2,3,4,5], [])

### `agent-dynamic-json-0008` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 'x') duck=(2, '"z"')

**KQL**
```kql
print d = dynamic({"nested":{"arr":[{"id":1,"tags":["x","y"]},{"id":2,"tags":["z"]}]}}) | mv-expand obj = d.nested.arr | mv-expand t = obj.tags | project id = toint(obj.id), tag = tostring(t)
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(obj, '$.id') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(obj, '$.id') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS id, TRY_CAST(t AS TEXT) AS tag FROM (SELECT t.*, u.value AS t FROM (SELECT t.*, u.value AS obj FROM (SELECT '{"nested":{"arr":[{"id":1,"tags":["x","y"]},{"id":2,"tags":["z"]}]}}'::JSON AS d) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(d, '$.nested.arr')) = 'ARRAY' THEN CAST(json_extract(d, '$.nested.arr') AS JSON[]) ELSE list_transform(json_keys(json_extract(d, '$.nested.arr')), lambda k: json_extract(json_extract(d, '$.nested.arr'), '$.' || k)) END) AS u(value)) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(obj, '$.tags')) = 'ARRAY' THEN CAST(json_extract(obj, '$.tags') AS JSON[]) ELSE list_transform(json_keys(json_extract(obj, '$.tags')), lambda k: json_extract(json_extract(obj, '$.tags'), '$.' || k)) END) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[id:Int, tag:String] rows=3
    - (1, 'x')
    - (1, 'y')
    - (2, 'z')
- DuckDB: cols=[id:Int, tag:String] rows=3
    - (2, '"z"')
    - (1, '"y"')
    - (1, '"x"')

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
], null) duck=('{"a":1,"b":2}', '{"a":1,"b":{"nested":true},"c":3}', ["a","b","c"], 'true')

**KQL**
```kql
print d = dynamic({"a":1,"b":2}) | extend m1 = bag_merge(d, dynamic({"b":{"nested":true},"c":3})) | extend bk = bag_keys(m1), bval = m1.b.nested
```
**Generated SQL**
```sql
SELECT *, JSON_KEYS(m1) AS bk, json_extract(m1, '$.b.nested') AS bval FROM (SELECT *, JSON_MERGE_PATCH(d, '{"b":{"nested":true},"c":3}'::JSON) AS m1 FROM (SELECT '{"a":1,"b":2}'::JSON AS d))
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
    - ('{"a":1,"b":2}', '{"a":1,"b":{"nested":true},"c":3}', ["a","b","c"], 'true')

### `agent-dynamic-json-0012` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a', 2) duck=('"b"', 3)

**KQL**
```kql
print d = dynamic([{"name":"a","sub":[1,2]},{"name":"b","sub":[3,4,5]}]) | mv-apply x = d on (extend sl = array_length(x.sub)) | project nm = tostring(x.name), sl
```
**Generated SQL**
```sql
SELECT TRY_CAST(json_extract(x, '$.name') AS TEXT) AS nm, sl FROM (SELECT t.*, _sub.* FROM (SELECT LIST_VALUE('{"name":"a","sub":[1,2]}'::JSON, '{"name":"b","sub":[3,4,5]}'::JSON) AS d) AS t CROSS JOIN UNNEST(d) AS u(value), LATERAL (SELECT *, LEN(json_extract(json_extract(x, '$.sub'), '$[*]')) AS sl FROM (SELECT u.value AS x)) AS _sub)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[nm:String, sl:Int] rows=2
    - ('a', 2)
    - ('b', 3)
- DuckDB: cols=[nm:String, sl:Int] rows=2
    - ('"b"', 3)
    - ('"a"', 2)

### `agent-dynamic-json-0014` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3,
  4,
  5,
  6
], [
  3,
  4,
  5
], [
  4,
  5,
  6
], [
  1,
  2,
  3,
  4,
  5,
  6
]) duck=([1,2,3,4,5,6], [3,4,5,6], [], [1,2,3,4,5,6])

**KQL**
```kql
print d = dynamic([1,2,3,4,5,6]) | extend slmid = array_slice(d, 2, -2), slneg = array_slice(d, -3, -1), slbig = array_slice(d, 0, 100)
```
**Generated SQL**
```sql
SELECT *, LIST_SLICE(d, 2 + 1, (-2) + 1) AS slmid, LIST_SLICE(d, (-3) + 1, (-1) + 1) AS slneg, LIST_SLICE(d, 0 + 1, 100 + 1) AS slbig FROM (SELECT LIST_VALUE(1, 2, 3, 4, 5, 6) AS d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, slmid:Dynamic, slneg:Dynamic, slbig:Dynamic] rows=1
    - ([
  1,
  2,
  3,
  4,
  5,
  6
], [
  3,
  4,
  5
], [
  4,
  5,
  6
], [
  1,
  2,
  3,
  4,
  5,
  6
])
- DuckDB: cols=[d:Unknown, slmid:Unknown, slneg:Unknown, slbig:Unknown] rows=1
    - ([1,2,3,4,5,6], [3,4,5,6], [], [1,2,3,4,5,6])

### `agent-dynamic-json-0015` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(4) duck=(3)

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"v":[1,2,3]}), dynamic({"v":[]}), dynamic({"v":null}) ] | mv-expand e = d.v | summarize cnt = count()
```
**Generated SQL**
```sql
SELECT COUNT(*) AS cnt FROM (SELECT t.*, u.value AS e FROM (SELECT * FROM (VALUES ('{"v":[1,2,3]}'::JSON), ('{"v":[]}'::JSON), ('{"v":null}'::JSON)) AS t(d)) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(d, '$.v')) = 'ARRAY' THEN CAST(json_extract(d, '$.v') AS JSON[]) ELSE list_transform(json_keys(json_extract(d, '$.v')), lambda k: json_extract(json_extract(d, '$.v'), '$.' || k)) END) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[cnt:Int] rows=1
    - (4)
- DuckDB: cols=[cnt:Int] rows=1
    - (3)

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

### `agent-dynamic-json-0022` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(21) duck=(0)

**KQL**
```kql
print d = dynamic({"matrix":[[1,2],[3,4],[5,6]]}) | mv-expand row = d.matrix | extend rsum = toint(row[0]) + toint(row[1]) | summarize sum(rsum)
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(rsum), 0) AS sum_rsum FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(row[0 + 1] AS DOUBLE), TRY_CAST(TRY_CAST(row[0 + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(row[1 + 1] AS DOUBLE), TRY_CAST(TRY_CAST(row[1 + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS rsum FROM (SELECT t.*, u.value AS row FROM (SELECT '{"matrix":[[1,2],[3,4],[5,6]]}'::JSON AS d) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(d, '$.matrix')) = 'ARRAY' THEN CAST(json_extract(d, '$.matrix') AS JSON[]) ELSE list_transform(json_keys(json_extract(d, '$.matrix')), lambda k: json_extract(json_extract(d, '$.matrix'), '$.' || k)) END) AS u(value)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[sum_rsum:Int] rows=1
    - (21)
- DuckDB: cols=[sum_rsum:Int] rows=1
    - (0)

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

### `agent-dynamic-json-0027` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=2

**KQL**
```kql
print d = dynamic([{"x":1},{"x":2}]) | extend cnt = array_length(d) | mv-apply e = d on (summarize mx = max(toint(e.x)))
```
**Generated SQL**
```sql
SELECT t.*, _sub.* FROM (SELECT *, LEN(d) AS cnt FROM (SELECT LIST_VALUE('{"x":1}'::JSON, '{"x":2}'::JSON) AS d)) AS t CROSS JOIN UNNEST(d) AS u(value), LATERAL (SELECT MAX(TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(e, '$.x') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(e, '$.x') AS BOOLEAN) AS DOUBLE))) AS INTEGER)) AS mx FROM (SELECT u.value AS e)) AS _sub
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, cnt:Int, mx:Int] rows=1
    - ([
  {
    "x": 1
  },
  {
    "x": 2
  }
], 2, 2)
- DuckDB: cols=[d:Unknown, cnt:Int, mx:Int] rows=2
    - (["{\u0022x\u0022:1}","{\u0022x\u0022:2}"], 2, 2)
    - (["{\u0022x\u0022:1}","{\u0022x\u0022:2}"], 2, 1)

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

### `agent-dynamic-json-0031` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3
], [
  4,
  5,
  6
], [
  1,
  2,
  3,
  4,
  5,
  6
], [
  [
    1,
    4
  ],
  [
    2,
    5
  ],
  [
    3,
    6
  ]
]) duck=([1,2,3], [4,5,6], [1,2,3,4,5,6], [{"":4},{"":5},{"":6}])

**KQL**
```kql
print d = dynamic([1,2,3]), e = dynamic([4,5,6]) | extend zipped = array_concat(d, e), summ = zip(d, e)
```
**Generated SQL**
```sql
SELECT *, LIST_CONCAT(d, e) AS zipped, LIST_ZIP(d, e) AS summ FROM (SELECT LIST_VALUE(1, 2, 3) AS d, LIST_VALUE(4, 5, 6) AS e)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, e:Dynamic, zipped:Dynamic, summ:Dynamic] rows=1
    - ([
  1,
  2,
  3
], [
  4,
  5,
  6
], [
  1,
  2,
  3,
  4,
  5,
  6
], [
  [
    1,
    4
  ],
  [
    2,
    5
  ],
  [
    3,
    6
  ]
])
- DuckDB: cols=[d:Unknown, e:Unknown, zipped:Unknown, summ:Unknown] rows=1
    - ([1,2,3], [4,5,6], [1,2,3,4,5,6], [{"":4},{"":5},{"":6}])

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
SELECT COALESCE(LIST(pair) FILTER (WHERE pair IS NOT NULL), []) AS joined FROM (SELECT t.*, _sub.* FROM (SELECT *, JSON_KEYS(d) AS ks FROM (SELECT '{"a":1,"b":2,"c":3}'::JSON AS d)) AS t CROSS JOIN UNNEST(ks) AS u(value), LATERAL (SELECT *, CONCAT(TRY_CAST(k AS TEXT), '=', TRY_CAST(json_extract(d, '$.' || TRY_CAST(k AS TEXT)) AS TEXT)) AS pair FROM (SELECT u.value AS k)) AS _sub)
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
SELECT COALESCE(LIST(p) FILTER (WHERE p IS NOT NULL), []) AS list_p FROM (SELECT t.*, u.value AS p FROM (SELECT *, JSON_KEYS(d) AS paths FROM (SELECT '{"a":{"b":{"c":1}},"x":{"y":2}}'::JSON AS d)) AS t CROSS JOIN UNNEST(paths) AS u(value) ORDER BY p ASC NULLS FIRST)
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
SELECT * FROM (SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(o, '$.id') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(o, '$.id') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS id, COALESCE(LIST(tag) FILTER (WHERE tag IS NOT NULL), []) AS tags FROM (SELECT t.*, u.value AS tag FROM (SELECT t.*, u.value AS o FROM (SELECT LIST_VALUE('{"id":1,"t":["a","b"]}'::JSON, '{"id":2,"t":[]}'::JSON, '{"id":3,"t":["c"]}'::JSON) AS d) AS t CROSS JOIN UNNEST(d) AS u(value)) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(o, '$.t')) = 'ARRAY' THEN CAST(json_extract(o, '$.t') AS JSON[]) ELSE list_transform(json_keys(json_extract(o, '$.t')), lambda k: json_extract(json_extract(o, '$.t'), '$.' || k)) END) AS u(value)) GROUP BY ALL) ORDER BY id ASC NULLS FIRST
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

### `agent-dynamic-json-0014` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[a0:Dynamic|String]  
*Detail:* first differing row[0]: kusto=('', 0, null, '') duck=('"vé"', 4, '1', '"3\t"')

**KQL**
```kql
print s = '{"k":"vé","arr":[1,2,"3\t"]}' | extend d = parse_json(s) | project k = tostring(d.k), klen = strlen(tostring(d.k)), a0 = d.arr[0], a2 = tostring(d.arr[2])
```
**Generated SQL**
```sql
SELECT TRY_CAST(json_extract(d, '$.k') AS TEXT) AS k, LENGTH(CAST(TRY_CAST(json_extract(d, '$.k') AS TEXT) AS VARCHAR)) AS klen, json_extract(json_extract(d, '$.arr'), '$[' || (0) || ']') AS a0, TRY_CAST(json_extract(json_extract(d, '$.arr'), '$[' || (2) || ']') AS TEXT) AS a2 FROM (SELECT *, CAST(s AS JSON) AS d FROM (SELECT '{"k":"vé","arr":[1,2,"3\t"]}' AS s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:String, klen:Int, a0:Dynamic, a2:String] rows=1
    - ('', 0, null, '')
- DuckDB: cols=[k:String, klen:Int, a0:String, a2:String] rows=1
    - ('"vé"', 4, '1', '"3\t"')

### `agent-dynamic-json-0015` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[ma:Dynamic|String], TYPE_MISMATCH[mb:Dynamic|String]  
*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3
], 9, 3) duck=('[4,5]', '9', 2)

**KQL**
```kql
print d = dynamic({"a":[1,2,3]}) | extend m = bag_merge(d, dynamic({"a":[4,5],"b":9})) | project ma = m.a, mb = m.b, malen = array_length(m.a)
```
**Generated SQL**
```sql
SELECT json_extract(m, '$.a') AS ma, json_extract(m, '$.b') AS mb, LEN(json_extract(json_extract(m, '$.a'), '$[*]')) AS malen FROM (SELECT *, JSON_MERGE_PATCH(d, '{"a":[4,5],"b":9}'::JSON) AS m FROM (SELECT '{"a":[1,2,3]}'::JSON AS d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ma:Dynamic, mb:Dynamic, malen:Int] rows=1
    - ([
  1,
  2,
  3
], 9, 3)
- DuckDB: cols=[ma:String, mb:String, malen:Int] rows=1
    - ('[4,5]', '9', 2)

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

### `agent-dynamic-json-0028` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(7, [
  "a",
  "c"
]) duck=(7, ["\u0022c\u0022","\u0022a\u0022"])

**KQL**
```kql
print d = dynamic({"items":[{"name":"a","qty":2},{"name":"b","qty":0},{"name":"c","qty":5}]}) | mv-expand it = d.items | where toint(it.qty) > 0 | summarize total = sum(toint(it.qty)), names = make_set(tostring(it.name))
```
**Generated SQL**
```sql
SELECT COALESCE(SUM(TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(it, '$.qty') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(it, '$.qty') AS BOOLEAN) AS DOUBLE))) AS INTEGER)), 0) AS total, COALESCE(LIST(DISTINCT TRY_CAST(json_extract(it, '$.name') AS TEXT)) FILTER (WHERE TRY_CAST(json_extract(it, '$.name') AS TEXT) IS NOT NULL), []) AS names FROM (SELECT * FROM (SELECT t.*, u.value AS it FROM (SELECT '{"items":[{"name":"a","qty":2},{"name":"b","qty":0},{"name":"c","qty":5}]}'::JSON AS d) AS t CROSS JOIN UNNEST(CASE WHEN json_type(json_extract(d, '$.items')) = 'ARRAY' THEN CAST(json_extract(d, '$.items') AS JSON[]) ELSE list_transform(json_keys(json_extract(d, '$.items')), lambda k: json_extract(json_extract(d, '$.items'), '$.' || k)) END) AS u(value)) WHERE TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(it, '$.qty') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(it, '$.qty') AS BOOLEAN) AS DOUBLE))) AS INTEGER) > 0)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[total:Int, names:Dynamic] rows=1
    - (7, [
  "a",
  "c"
])
- DuckDB: cols=[total:Int, names:Unknown] rows=1
    - (7, ["\u0022c\u0022","\u0022a\u0022"])

### `agent-dynamic-json-0031` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  "a",
  "b",
  "c",
  "d"
], 2, 4) duck=(["a","b","c"], 3, 4)

**KQL**
```kql
print d = dynamic({"a":1,"b":2}), e = dynamic({"b":3,"c":4}), f = dynamic({"c":5,"d":6}) | extend m = bag_merge(d, e, f) | extend ks = array_sort_asc(bag_keys(m)) | project ks, mb = toint(m.b), mc = toint(m.c)
```
**Generated SQL**
```sql
SELECT ks, TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(m, '$.b') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(m, '$.b') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS mb, TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(m, '$.c') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(m, '$.c') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS mc FROM (SELECT *, LIST_SORT(JSON_KEYS(m)) AS ks FROM (SELECT *, JSON_MERGE_PATCH(d, e) AS m FROM (SELECT '{"a":1,"b":2}'::JSON AS d, '{"b":3,"c":4}'::JSON AS e, '{"c":5,"d":6}'::JSON AS f)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ks:Dynamic, mb:Int, mc:Int] rows=1
    - ([
  "a",
  "b",
  "c",
  "d"
], 2, 4)
- DuckDB: cols=[ks:Unknown, mb:Int, mc:Int] rows=1
    - (["a","b","c"], 3, 4)

### `agent-dynamic-json-0033` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(50, [
  50
]) duck=(50, [{"IsPowerOfTwo":false,"IsZero":false,"IsOne":false,"IsEven":true,"Sign":1}])

**KQL**
```kql
print d = dynamic([10,20,30,40]) | extend mid = array_slice(d, 1, 2) | mv-expand m = mid to typeof(long) | summarize ms = sum(m) | extend back = pack_array(ms)
```
**Generated SQL**
```sql
SELECT *, LIST_VALUE(ms) AS back FROM (SELECT COALESCE(SUM(m), 0) AS ms FROM (SELECT t.*, u.value AS m FROM (SELECT *, LIST_SLICE(d, 1 + 1, 2 + 1) AS mid FROM (SELECT LIST_VALUE(10, 20, 30, 40) AS d)) AS t CROSS JOIN UNNEST(mid) AS u(value)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ms:Int, back:Dynamic] rows=1
    - (50, [
  50
])
- DuckDB: cols=[ms:Int, back:Unknown] rows=1
    - (50, [{"IsPowerOfTwo":false,"IsZero":false,"IsOne":false,"IsEven":true,"Sign":1}])

### `agent-dynamic-json-0034` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[b1:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(True, True, 1, True, 3) duck=(False, False, '1', False, 3)

**KQL**
```kql
print s = '{"a":null,"b":[null,1,null],"c":{"d":null}}' | extend d = parse_json(s) | project an = isnull(d.a), b0 = isnull(d.b[0]), b1 = d.b[1], cd = isnull(d.c.d), blen = array_length(d.b)
```
**Generated SQL**
```sql
SELECT (json_extract(d, '$.a') IS NULL) AS an, (json_extract(json_extract(d, '$.b'), '$[' || (0) || ']') IS NULL) AS b0, json_extract(json_extract(d, '$.b'), '$[' || (1) || ']') AS b1, (json_extract(d, '$.c.d') IS NULL) AS cd, LEN(json_extract(json_extract(d, '$.b'), '$[*]')) AS blen FROM (SELECT *, CAST(s AS JSON) AS d FROM (SELECT '{"a":null,"b":[null,1,null],"c":{"d":null}}' AS s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[an:Bool, b0:Bool, b1:Dynamic, cd:Bool, blen:Int] rows=1
    - (True, True, 1, True, 3)
- DuckDB: cols=[an:Bool, b0:Bool, b1:String, cd:Bool, blen:Int] rows=1
    - (False, False, '1', False, 3)

### `agent-dynamic-json-0039` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=4

**KQL**
```kql
print d = dynamic([{"v":1},{"v":2},{"v":3},{"v":4}]) | mv-apply e = d on (where toint(e.v) % 2 == 0 | summarize evensum = sum(toint(e.v))) | project evensum
```
**Generated SQL**
```sql
SELECT evensum FROM (SELECT t.*, _sub.* FROM (SELECT LIST_VALUE('{"v":1}'::JSON, '{"v":2}'::JSON, '{"v":3}'::JSON, '{"v":4}'::JSON) AS d) AS t CROSS JOIN UNNEST(d) AS u(value), LATERAL (SELECT COALESCE(SUM(TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(e, '$.v') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(e, '$.v') AS BOOLEAN) AS DOUBLE))) AS INTEGER)), 0) AS evensum FROM (SELECT * FROM (SELECT u.value AS e) WHERE (((TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(e, '$.v') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(e, '$.v') AS BOOLEAN) AS DOUBLE))) AS INTEGER)) % NULLIF(2, 0)) + ABS(2)) % NULLIF(2, 0) = 0)) AS _sub)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[evensum:Int] rows=1
    - (6)
- DuckDB: cols=[evensum:Int] rows=4
    - (4)
    - (0)
    - (2)
    - (0)

### `agent-dynamic-json-0043` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  1,
  2,
  3
], [
  "a",
  "b",
  "c"
]) duck=([], [])

**KQL**
```kql
print x = dynamic([[1,"a"],[2,"b"],[3,"c"]]) | mv-expand pair = x | project num = toint(pair[0]), letter = tostring(pair[1]) | summarize make_list(num), make_list(letter)
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(num) FILTER (WHERE num IS NOT NULL), []) AS list_num, COALESCE(LIST(letter) FILTER (WHERE letter IS NOT NULL), []) AS list_letter FROM (SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(pair[0 + 1] AS DOUBLE), TRY_CAST(TRY_CAST(pair[0 + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS num, TRY_CAST(pair[1 + 1] AS TEXT) AS letter FROM (SELECT t.*, u.value AS pair FROM (SELECT LIST_VALUE('[1,"a"]'::JSON, '[2,"b"]'::JSON, '[3,"c"]'::JSON) AS x) AS t CROSS JOIN UNNEST(x) AS u(value)))
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
    - ([], [])

### `t1-dynamic-json-0000` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(null) duck=(19)

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]), dynamic({"nested":{"x":"y"}}), dynamic(null) ] | project v = array_length(d)
```
**Generated SQL**
```sql
SELECT LEN(d) AS v FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON), (LIST_VALUE(10, 20, 30)), ('{"nested":{"x":"y"}}'::JSON), (NULL)) AS t(d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[v:Int] rows=4
    - (null)
    - (3)
    - (null)
    - (null)
- DuckDB: cols=[v:Int] rows=4
    - (19)
    - (10)
    - (20)
    - (null)

### `t1-dynamic-json-0001` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[v:Dynamic|String]  
*Detail:* first differing row[1]: kusto=(10) duck=('20')

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]), dynamic({"nested":{"x":"y"}}), dynamic(null) ] | project v = d[0]
```
**Generated SQL**
```sql
SELECT d[0 + 1] AS v FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON), (LIST_VALUE(10, 20, 30)), ('{"nested":{"x":"y"}}'::JSON), (NULL)) AS t(d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[v:Dynamic] rows=4
    - (null)
    - (10)
    - (null)
    - (null)
- DuckDB: cols=[v:String] rows=4
    - (null)
    - ('20')
    - (null)
    - (null)

### `t1-dynamic-json-0007` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(null) duck=([])

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]), dynamic({"nested":{"x":"y"}}), dynamic(null) ] | project v = bag_keys(d)
```
**Generated SQL**
```sql
SELECT JSON_KEYS(d) AS v FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON), (LIST_VALUE(10, 20, 30)), ('{"nested":{"x":"y"}}'::JSON), (NULL)) AS t(d)
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

## Family: joins-lookup-union (6)

### `agent-joins-lookup-union-0038` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name guid does not exist!
Did you mean "uuid"?

LINE 1: WITH L AS NOT MATERIALIZED (SELECT * FROM (VALUES (guid(11111111-1111-1111-1111-111111111111), 'a'), (guid...
                                                           ^

**KQL**
```kql
let L = datatable(k:guid, lv:string)[ guid(11111111-1111-1111-1111-111111111111),"a", guid(22222222-2222-2222-2222-222222222222),"b" ]; let R = datatable(k:guid, rv:string)[ guid(11111111-1111-1111-1111-111111111111),"x" ]; L | join kind=leftouter R on k | order by lv asc
```
**Generated SQL**
```sql
WITH L AS NOT MATERIALIZED (SELECT * FROM (VALUES (guid(11111111-1111-1111-1111-111111111111), 'a'), (guid(22222222-2222-2222-2222-222222222222), 'b')) AS t(k, lv)), R AS NOT MATERIALIZED (SELECT * FROM (VALUES (guid(11111111-1111-1111-1111-111111111111), 'x')) AS t(k, rv)) SELECT L.*, R.k AS k1, COALESCE(R.rv, '') AS rv FROM L AS L LEFT OUTER JOIN R AS R ON L.k IS NOT DISTINCT FROM R.k ORDER BY lv ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Guid, lv:String, k1:Guid, rv:String] rows=2
    - ('11111111-1111-1111-1111-111111111111', 'a', '11111111-1111-1111-1111-111111111111', 'x')
    - ('22222222-2222-2222-2222-222222222222', 'b', null, '')
- DuckDB: ERROR — Catalog Error: Scalar Function with name guid does not exist!
Did you mean "uuid"?

LINE 1: WITH L AS NOT MATERIALIZED (SELECT * FROM (VALUES (guid(11111111-1111-1111-1111-111111111111), 'a'), (guid...
                                                           ^

### `agent-joins-lookup-union-0032` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[src:String|Int], TYPE_MISMATCH[x:Int|String]  
*Detail:* first differing row[0]: kusto=('union_arg0', 1, 'a') duck=(1, 'a', 'union_arg0')

**KQL**
```kql
let A = datatable(x:long, y:string)[ 1,"a", 2,"b" ]; let B = datatable(x:long, y:string)[ 3,"c", 4,"d" ]; union withsource=src A, B | order by x asc
```
**Generated SQL**
```sql
WITH A AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT), 'a'), (CAST(2 AS BIGINT), 'b')) AS t(x, y)), B AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(3 AS BIGINT), 'c'), (CAST(4 AS BIGINT), 'd')) AS t(x, y)) (SELECT *, 'union_arg0' AS src FROM (SELECT * FROM A)) UNION ALL BY NAME (SELECT *, 'union_arg1' AS src FROM (SELECT * FROM B)) ORDER BY x ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[src:String, x:Int, y:String] rows=4
    - ('union_arg0', 1, 'a')
    - ('union_arg0', 2, 'b')
    - ('union_arg1', 3, 'c')
    - ('union_arg1', 4, 'd')
- DuckDB: cols=[x:Int, y:String, src:String] rows=4
    - (1, 'a', 'union_arg0')
    - (2, 'b', 'union_arg0')
    - (3, 'c', 'union_arg1')
    - (4, 'd', 'union_arg1')

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

### `agent-joins-lookup-union-0017` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[tbl:String|Int], TYPE_MISMATCH[x:Int|String]  
*Detail:* first differing row[0]: kusto=('union_arg0', 1, 'a', 1, 'a') duck=(1, 'a', 'union_arg0', 1, 'a')

**KQL**
```kql
let A = datatable(x:long, y:string)[ 1,"a", 2,"b" ]; let B = datatable(x:long, y:string)[ 2,"c", 3,"d" ]; union withsource=tbl A, B | join kind=inner (A | project x, ay=y) on x | order by x asc, y asc
```
**Generated SQL**
```sql
WITH A AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT), 'a'), (CAST(2 AS BIGINT), 'b')) AS t(x, y)), B AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(2 AS BIGINT), 'c'), (CAST(3 AS BIGINT), 'd')) AS t(x, y)) SELECT L.*, R.* RENAME (x AS x1) FROM ((SELECT *, 'union_arg0' AS tbl FROM (SELECT * FROM A)) UNION ALL BY NAME (SELECT *, 'union_arg1' AS tbl FROM (SELECT * FROM B))) AS L INNER JOIN (SELECT x, y AS ay FROM A) AS R ON L.x IS NOT DISTINCT FROM R.x ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[tbl:String, x:Int, y:String, x1:Int, ay:String] rows=3
    - ('union_arg0', 1, 'a', 1, 'a')
    - ('union_arg0', 2, 'b', 2, 'b')
    - ('union_arg1', 2, 'c', 2, 'b')
- DuckDB: cols=[x:Int, y:String, tbl:String, x1:Int, ay:String] rows=3
    - (1, 'a', 'union_arg0', 1, 'a')
    - (2, 'b', 'union_arg0', 2, 'b')
    - (2, 'c', 'union_arg1', 2, 'b')

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

### `agent-joins-lookup-union-0041` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 200) duck=(1, 100)

**KQL**
```kql
let L = datatable(k:long, lv:long)[ 1,10, 2,20, 3,30 ]; let R = datatable(k:long, rv:long)[ 1,100, 2,200 ]; L | join kind=leftouter (R | extend rv = rv * 2) on k | summarize sum(rv) by k | order by k asc
```
**Generated SQL**
```sql
WITH L AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(2 AS BIGINT), CAST(20 AS BIGINT)), (CAST(3 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, lv)), R AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(100 AS BIGINT)), (CAST(2 AS BIGINT), CAST(200 AS BIGINT))) AS t(k, rv)) SELECT * FROM (SELECT k, COALESCE(SUM(rv), 0) AS sum_rv FROM (SELECT L.*, R.k AS k1, R.rv FROM L AS L LEFT OUTER JOIN (SELECT *, rv * 2 AS rv FROM R) AS R ON L.k IS NOT DISTINCT FROM R.k) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, sum_rv:Int] rows=3
    - (1, 200)
    - (2, 400)
    - (3, 0)
- DuckDB: cols=[k:Int, sum_rv:Int] rows=3
    - (1, 100)
    - (2, 200)
    - (3, 0)

## Family: nested-pipelines-let-cte (33)

### `agent-nested-pipelines-let-cte-0028` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ...]}'::JSON), (LIST_VALUE(10, 20, 30))) AS t(d)) AS t CROSS JOIN UNNEST(t.d) AS u(value))
                                                                          ^

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic([10,20,30]) ] | mv-expand d | project d
```
**Generated SQL**
```sql
SELECT d FROM (SELECT t.* EXCLUDE (d), u.value AS d FROM (SELECT * FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON), (LIST_VALUE(10, 20, 30))) AS t(d)) AS t CROSS JOIN UNNEST(t.d) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic] rows=5
    - ({
  "a": 1
})
    - ({
  "b": [
    1,
    2,
    3
  ]
})
    - (10)
    - (20)
    - (30)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ...]}'::JSON), (LIST_VALUE(10, 20, 30))) AS t(d)) AS t CROSS JOIN UNNEST(t.d) AS u(value))
                                                                          ^

### `agent-nested-pipelines-let-cte-0020` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ... a, 'y', b) AS c FROM (SELECT 1 AS a, 2 AS b)) AS t CROSS JOIN UNNEST(t.c) AS u(value)) ORDER BY TRY_CAST(c AS TEXT) ASC...
                                                                          ^

**KQL**
```kql
print a = 1, b = 2 | extend c = pack("x", a, "y", b) | mv-expand kind=array c | project c | sort by tostring(c) asc
```
**Generated SQL**
```sql
SELECT c FROM (SELECT t.* EXCLUDE (c), u.value AS c FROM (SELECT *, json_object('x', a, 'y', b) AS c FROM (SELECT 1 AS a, 2 AS b)) AS t CROSS JOIN UNNEST(t.c) AS u(value)) ORDER BY TRY_CAST(c AS TEXT) ASC NULLS FIRST
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
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ... a, 'y', b) AS c FROM (SELECT 1 AS a, 2 AS b)) AS t CROSS JOIN UNNEST(t.c) AS u(value)) ORDER BY TRY_CAST(c AS TEXT) ASC...
                                                                          ^

### `agent-nested-pipelines-let-cte-0000` — SqlExecError (highest)

*Detail:* Binder Error: Referenced column "count_1" not found in FROM clause!
Candidate bindings: "count__1", "count_", "Column1"

LINE 1: ... 0) AS Column1 FROM c GROUP BY ALL) SELECT k, count_, Column1, count_1 FROM (SELECT L.*, R.* RENAME (k AS k1) FROM d AS...
                                                                          ^

**KQL**
```kql
let a = datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30 ]; let b = a | summarize count() by k; let c = b | extend Column1 = count_; let d = c | summarize count_ = sum(count_), Column1 = sum(Column1) by k; d | join kind=inner (b) on k | project k, count_, Column1, count_1 | sort by k asc
```
**Generated SQL**
```sql
WITH a AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v)), b AS NOT MATERIALIZED (SELECT k, COUNT(*) AS count_ FROM a GROUP BY ALL), c AS NOT MATERIALIZED (SELECT *, count_ AS Column1 FROM b), d AS NOT MATERIALIZED (SELECT k, COALESCE(SUM(count_), 0) AS count_, COALESCE(SUM(Column1), 0) AS Column1 FROM c GROUP BY ALL) SELECT k, count_, Column1, count_1 FROM (SELECT L.*, R.* RENAME (k AS k1) FROM d AS L INNER JOIN b AS R ON L.k IS NOT DISTINCT FROM R.k) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, count_:Int, Column1:Int, count_1:Int] rows=2
    - (1, 2, 2, 2)
    - (2, 2, 2, 2)
- DuckDB: ERROR — Binder Error: Referenced column "count_1" not found in FROM clause!
Candidate bindings: "count__1", "count_", "Column1"

LINE 1: ... 0) AS Column1 FROM c GROUP BY ALL) SELECT k, count_, Column1, count_1 FROM (SELECT L.*, R.* RENAME (k AS k1) FROM d AS...
                                                                          ^

### `agent-nested-pipelines-let-cte-0005` — SqlExecError (highest)

*Detail:* Binder Error: Referenced column "count_1" not found in FROM clause!
Candidate bindings: "count__1", "count_"

LINE 1: ...(30 AS BIGINT))) AS t(k, v) GROUP BY ALL) SELECT k, count_, count_1 FROM (SELECT L.*, R.* RENAME (k AS k1) FROM (SELECT...
                                                                       ^

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30 ] | summarize count() by k | as Stage1 | extend count_ = count_ + 100 | join kind=inner (Stage1) on k | project k, count_, count_1 | sort by k asc
```
**Generated SQL**
```sql
WITH Stage1 AS NOT MATERIALIZED (SELECT k, COUNT(*) AS count_ FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v) GROUP BY ALL) SELECT k, count_, count_1 FROM (SELECT L.*, R.* RENAME (k AS k1) FROM (SELECT * EXCLUDE (count_), count_ + 100 AS count_ FROM (SELECT k, COUNT(*) AS count_ FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v) GROUP BY ALL)) AS L INNER JOIN Stage1 AS R ON L.k IS NOT DISTINCT FROM R.k) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, count_:Int, count_1:Int] rows=2
    - (1, 102, 2)
    - (2, 102, 2)
- DuckDB: ERROR — Binder Error: Referenced column "count_1" not found in FROM clause!
Candidate bindings: "count__1", "count_"

LINE 1: ...(30 AS BIGINT))) AS t(k, v) GROUP BY ALL) SELECT k, count_, count_1 FROM (SELECT L.*, R.* RENAME (k AS k1) FROM (SELECT...
                                                                       ^

### `agent-nested-pipelines-let-cte-0010` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ... t.*, u.value AS x FROM (SELECT * FROM e) AS t CROSS JOIN UNNEST(arr) AS u(value)) SELECT * FROM (SELECT a, COALESCE...
                                                                     ^

**KQL**
```kql
let d = datatable(d:dynamic)[ dynamic({"a":1,"b":[1,2,3]}), dynamic({"a":2,"b":[4,5]}), dynamic({"a":3,"b":[6]}) ]; let e = d | extend a = toint(d.a), arr = d.b; let f = e | mv-expand x = arr to typeof(long); f | summarize s = sum(x), cnt = count() by a | sort by a asc
```
**Generated SQL**
```sql
WITH d AS NOT MATERIALIZED (SELECT * FROM (VALUES ('{"a":1,"b":[1,2,3]}'::JSON), ('{"a":2,"b":[4,5]}'::JSON), ('{"a":3,"b":[6]}'::JSON)) AS t(d)), e AS NOT MATERIALIZED (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(d, '$.a') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(d, '$.a') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS a, json_extract(d, '$.b') AS arr FROM d), f AS NOT MATERIALIZED (SELECT t.*, u.value AS x FROM (SELECT * FROM e) AS t CROSS JOIN UNNEST(arr) AS u(value)) SELECT * FROM (SELECT a, COALESCE(SUM(x), 0) AS s, COUNT(*) AS cnt FROM f GROUP BY ALL) ORDER BY a ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Int, s:Int, cnt:Int] rows=3
    - (1, 6, 3)
    - (2, 9, 2)
    - (3, 6, 1)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ... t.*, u.value AS x FROM (SELECT * FROM e) AS t CROSS JOIN UNNEST(arr) AS u(value)) SELECT * FROM (SELECT a, COALESCE...
                                                                     ^

### `agent-nested-pipelines-let-cte-0014` — SqlExecError (highest)

*Detail:* Binder Error: Referenced column "s" not found in FROM clause!
Candidate bindings: "grp"

LINE 1: ... FROM (VALUES (1), (2), (3), (4), (5), (6)) AS t(i)) ORDER BY s ASC NULLS FIRST
                                                                         ^

**KQL**
```kql
datatable(i:int)[ 1, 2, 3, 4, 5, 6 ] | extend grp = i % 2 | partition by grp (summarize s = sum(i) | extend tag = "p") | sort by s asc
```
**Generated SQL**
```sql
SELECT *, 'p' AS tag FROM (SELECT *, (((i) % NULLIF(2, 0)) + ABS(2)) % NULLIF(2, 0) AS grp FROM (VALUES (1), (2), (3), (4), (5), (6)) AS t(i)) ORDER BY s ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, tag:String] rows=2
    - (9, 'p')
    - (12, 'p')
- DuckDB: ERROR — Binder Error: Referenced column "s" not found in FROM clause!
Candidate bindings: "grp"

LINE 1: ... FROM (VALUES (1), (2), (3), (4), (5), (6)) AS t(i)) ORDER BY s ASC NULLS FIRST
                                                                         ^

### `agent-nested-pipelines-let-cte-0033` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ... t.*, u.value AS v FROM (SELECT * FROM e) AS t CROSS JOIN UNNEST(vals) AS u(value)), g AS NOT MATERIALIZED (SELECT...
                                                                     ^

**KQL**
```kql
let d = datatable(d:dynamic)[ dynamic({"k":1,"vals":[10,20]}), dynamic({"k":2,"vals":[30]}) ]; let e = d | extend k = tolong(d.k), vals = d.vals; let f = e | mv-expand v = vals to typeof(long); let g = f | summarize total = sum(v), lst = make_list(v) by k; g | extend back = array_length(lst) | project k, total, back | sort by k asc
```
**Generated SQL**
```sql
WITH d AS NOT MATERIALIZED (SELECT * FROM (VALUES ('{"k":1,"vals":[10,20]}'::JSON), ('{"k":2,"vals":[30]}'::JSON)) AS t(d)), e AS NOT MATERIALIZED (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(json_extract(d, '$.k') AS DOUBLE), TRY_CAST(TRY_CAST(json_extract(d, '$.k') AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS k, json_extract(d, '$.vals') AS vals FROM d), f AS NOT MATERIALIZED (SELECT t.*, u.value AS v FROM (SELECT * FROM e) AS t CROSS JOIN UNNEST(vals) AS u(value)), g AS NOT MATERIALIZED (SELECT k, COALESCE(SUM(v), 0) AS total, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS lst FROM f GROUP BY ALL) SELECT k, total, back FROM (SELECT *, LEN(lst) AS back FROM g) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, total:Int, back:Int] rows=2
    - (1, 30, 2)
    - (2, 30, 1)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ... t.*, u.value AS v FROM (SELECT * FROM e) AS t CROSS JOIN UNNEST(vals) AS u(value)), g AS NOT MATERIALIZED (SELECT...
                                                                     ^

### `agent-nested-pipelines-let-cte-0039` — SqlExecError (highest)

*Detail:* Binder Error: Referenced column "y1" not found in FROM clause!
Candidate bindings: "y", "x1", "y_1"

LINE 1: ... (SELECT *, y + 1 AS z FROM m1 WHERE y > 2) SELECT x, y, y1, z FROM (SELECT L.*, R.* RENAME (x AS x1) FROM m1 AS...
                                                                    ^

**KQL**
```kql
let m1 = materialize(range x from 1 to 4 step 1 | extend y = x * x); let m2 = materialize(m1 | where y > 2 | extend z = y + 1); m1 | join kind=inner (m2) on x | project x, y, y1, z | sort by x asc
```
**Generated SQL**
```sql
WITH m1 AS MATERIALIZED (SELECT *, x * x AS y FROM (SELECT generate_series AS x FROM generate_series(CAST(1 AS BIGINT), CAST(4 AS BIGINT), CAST(1 AS BIGINT)))), m2 AS MATERIALIZED (SELECT *, y + 1 AS z FROM m1 WHERE y > 2) SELECT x, y, y1, z FROM (SELECT L.*, R.* RENAME (x AS x1) FROM m1 AS L INNER JOIN m2 AS R ON L.x IS NOT DISTINCT FROM R.x) ORDER BY x ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:Int, y1:Int, z:Int] rows=3
    - (2, 4, 4, 5)
    - (3, 9, 9, 10)
    - (4, 16, 16, 17)
- DuckDB: ERROR — Binder Error: Referenced column "y1" not found in FROM clause!
Candidate bindings: "y", "x1", "y_1"

LINE 1: ... (SELECT *, y + 1 AS z FROM m1 WHERE y > 2) SELECT x, y, y1, z FROM (SELECT L.*, R.* RENAME (x AS x1) FROM m1 AS...
                                                                    ^

### `agent-nested-pipelines-let-cte-0043` — SqlExecError (highest)

*Detail:* Parser Error: syntax error at or near "both"

LINE 1: SELECT "outer", "inner", ratio, both FROM (SELECT *, "outer" + "inner" AS both FROM (SELECT...
                                        ^

**KQL**
```kql
print outer = 1 | extend inner = toscalar(datatable(n:long)[ 10, 20, 30 ] | summarize sum(n)) | extend ratio = inner / outer | extend both = outer + inner | project outer, inner, ratio, both
```
**Generated SQL**
```sql
SELECT "outer", "inner", ratio, both FROM (SELECT *, "outer" + "inner" AS both FROM (SELECT *, "inner" / "outer" AS ratio FROM (SELECT *, (SELECT COALESCE(SUM(n), 0) AS sum_n FROM (VALUES (CAST(10 AS BIGINT)), (CAST(20 AS BIGINT)), (CAST(30 AS BIGINT))) AS t(n) LIMIT 1) AS "inner" FROM (SELECT 1 AS outer))))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[outer:Int, inner:Int, ratio:Int, both:Int] rows=1
    - (1, 60, 60, 61)
- DuckDB: ERROR — Parser Error: syntax error at or near "both"

LINE 1: SELECT "outer", "inner", ratio, both FROM (SELECT *, "outer" + "inner" AS both FROM (SELECT...
                                        ^

### `agent-nested-pipelines-let-cte-0002` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a', 203) duck=('a', 3)

**KQL**
```kql
datatable(id:long, name:string, score:real)[ 1,"a",1.5, 2,"b",2.5, 3,"c",3.5 ] | extend score = score + 100 | extend score = score * 2 | project name, score | order by score asc
```
**Generated SQL**
```sql
SELECT name, score FROM (SELECT * EXCLUDE (score), score * 2 AS score FROM (SELECT *, score + 100 AS score FROM (VALUES (CAST(1 AS BIGINT), 'a', CAST(1.5 AS DOUBLE)), (CAST(2 AS BIGINT), 'b', CAST(2.5 AS DOUBLE)), (CAST(3 AS BIGINT), 'c', CAST(3.5 AS DOUBLE))) AS t(id, name, score))) ORDER BY score ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[name:String, score:Real] rows=3
    - ('a', 203)
    - ('b', 205)
    - ('c', 207)
- DuckDB: cols=[name:String, score:Real] rows=3
    - ('a', 3)
    - ('b', 5)
    - ('c', 7)

### `agent-nested-pipelines-let-cte-0029` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2, 2) duck=(1, 2)

**KQL**
```kql
datatable(x:long, y:long)[ 1,2, 3,4 ] | extend x = y | extend y = x | project x, y | order by x asc
```
**Generated SQL**
```sql
SELECT x, y FROM (SELECT *, x AS y FROM (SELECT *, y AS x FROM (VALUES (CAST(1 AS BIGINT), CAST(2 AS BIGINT)), (CAST(3 AS BIGINT), CAST(4 AS BIGINT))) AS t(x, y))) ORDER BY x ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:Int] rows=2
    - (2, 2)
    - (4, 4)
- DuckDB: cols=[x:Int, y:Int] rows=2
    - (1, 2)
    - (3, 4)

### `agent-nested-pipelines-let-cte-0000` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 4, 10) duck=(1, 2, 10)

**KQL**
```kql
let a = datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30 ]; let b = a | summarize count() by k; let c = b | extend count_ = count_ * 2; c | join kind=inner (a) on k | project k, count_, v | sort by k asc, v asc
```
**Generated SQL**
```sql
WITH a AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v)), b AS NOT MATERIALIZED (SELECT k, COUNT(*) AS count_ FROM a GROUP BY ALL), c AS NOT MATERIALIZED (SELECT *, count_ * 2 AS count_ FROM b) SELECT k, count_, v FROM (SELECT L.*, R.* RENAME (k AS k1) FROM c AS L INNER JOIN a AS R ON L.k IS NOT DISTINCT FROM R.k) ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, count_:Int, v:Int] rows=4
    - (1, 4, 10)
    - (1, 4, 20)
    - (2, 4, 30)
    - (2, 4, 30)
- DuckDB: cols=[k:Int, count_:Int, v:Int] rows=4
    - (1, 2, 10)
    - (1, 2, 20)
    - (2, 2, 30)
    - (2, 2, 30)

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

### `agent-nested-pipelines-let-cte-0012` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 21) duck=(1, 20)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30 ] | summarize arg_max(v, *) by k | extend v = v + 1 | project k, v | sort by k asc
```
**Generated SQL**
```sql
SELECT k, v FROM (SELECT *, v + 1 AS v FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v) QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC) = 1) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, v:Int] rows=2
    - (1, 21)
    - (2, 31)
- DuckDB: cols=[k:Int, v:Int] rows=2
    - (1, 20)
    - (2, 30)

### `agent-nested-pipelines-let-cte-0017` — MismatchRows (high)

*Detail:* row count: kusto=0 duck=3

**KQL**
```kql
let p = datatable(k:long)[ 1, 2, 3 ]; let q = p | extend k = k + 10; let s = q | extend k = k * 2; s | join kind=inner (p) on $left.k == $right.k | project k, k1
```
**Generated SQL**
```sql
WITH p AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT))) AS t(k)), q AS NOT MATERIALIZED (SELECT *, k + 10 AS k FROM p), s AS NOT MATERIALIZED (SELECT *, k * 2 AS k FROM q) SELECT k, k1 FROM (SELECT L.*, R.k AS k1 FROM s AS L INNER JOIN p AS R ON L.k IS NOT DISTINCT FROM R.k)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, k1:Int] rows=0
- DuckDB: cols=[k:Int, k1:Int] rows=3
    - (1, 1)
    - (2, 2)
    - (3, 3)

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

### `agent-nested-pipelines-let-cte-0028` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=4

**KQL**
```kql
let s = datatable(g:string, v:long)[ "a",1, "a",2, "b",3, "b",4 ]; s | summarize total = sum(v), lst = make_list(v) by g | mv-apply x = lst to typeof(long) on (summarize mx = max(x)) | project g, total, mx | sort by g asc
```
**Generated SQL**
```sql
WITH s AS NOT MATERIALIZED (SELECT * FROM (VALUES ('a', CAST(1 AS BIGINT)), ('a', CAST(2 AS BIGINT)), ('b', CAST(3 AS BIGINT)), ('b', CAST(4 AS BIGINT))) AS t(g, v)) SELECT g, total, mx FROM (SELECT t.*, _sub.* FROM (SELECT g, COALESCE(SUM(v), 0) AS total, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS lst FROM s GROUP BY ALL) AS t CROSS JOIN UNNEST(lst) AS u(value), LATERAL (SELECT MAX(x) AS mx FROM (SELECT u.value AS x)) AS _sub) ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, total:Int, mx:Int] rows=2
    - ('a', 3, 2)
    - ('b', 7, 4)
- DuckDB: cols=[g:String, total:Int, mx:Int] rows=4
    - ('a', 3, 2)
    - ('a', 3, 1)
    - ('b', 7, 4)
    - ('b', 7, 3)

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

### `agent-nested-pipelines-let-cte-0032` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 17) duck=(1, 15)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 2,20, 3,30 ] | extend v = v + 1 | extend v = v * 2 | extend v = v - 5 | summarize sum(v) by k | sort by k asc
```
**Generated SQL**
```sql
SELECT * FROM (SELECT k, COALESCE(SUM(v), 0) AS sum_v FROM (SELECT * EXCLUDE (v), v - 5 AS v FROM (SELECT * EXCLUDE (v), v * 2 AS v FROM (SELECT *, v + 1 AS v FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(2 AS BIGINT), CAST(20 AS BIGINT)), (CAST(3 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v)))) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, sum_v:Int] rows=3
    - (1, 17)
    - (2, 37)
    - (3, 57)
- DuckDB: cols=[k:Int, sum_v:Int] rows=3
    - (1, 15)
    - (2, 35)
    - (3, 55)

### `agent-nested-pipelines-let-cte-0002` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 30, 31, 2, 2, 2) duck=(1, 30, 30, 2, 2, 2)

**KQL**
```kql
let m = materialize(datatable(k:long, v:long)[ 1,10, 1,20, 2,30 ] | summarize s = sum(v), c = count() by k); m | join kind=inner (m | extend s = s + 1) on k | join kind=leftouter (m | project k, c) on k | project k, s, s1, c, c1, c2 | sort by k asc
```
**Generated SQL**
```sql
WITH m AS MATERIALIZED (SELECT k, COALESCE(SUM(v), 0) AS s, COUNT(*) AS c FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v) GROUP BY ALL) SELECT k, s, s1, c, c1, c2 FROM (SELECT L.*, R.k AS k2, R.c AS c2 FROM (SELECT L.*, R.k AS k1, R.s AS s1, R.c AS c1 FROM m AS L INNER JOIN (SELECT *, s + 1 AS s FROM m) AS R ON L.k IS NOT DISTINCT FROM R.k) AS L LEFT OUTER JOIN (SELECT k, c FROM m) AS R ON L.k IS NOT DISTINCT FROM R.k) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, s:Int, s1:Int, c:Int, c1:Int, c2:Int] rows=2
    - (1, 30, 31, 2, 2, 2)
    - (2, 30, 31, 1, 1, 1)
- DuckDB: cols=[k:Int, s:Int, s1:Int, c:Int, c1:Int, c2:Int] rows=2
    - (1, 30, 30, 2, 2, 2)
    - (2, 30, 30, 1, 1, 1)

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

### `agent-nested-pipelines-let-cte-0009` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(10, 20) duck=(1, 20)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30 ] | summarize arg_max(v, *) by k | extend k = k * 10 | summarize arg_min(v, *) by k | project k, v | sort by k asc
```
**Generated SQL**
```sql
SELECT k, v FROM (SELECT *, k * 10 AS k FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v) QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC) = 1) QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC) = 1 ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, v:Int] rows=2
    - (10, 20)
    - (20, 30)
- DuckDB: cols=[k:Int, v:Int] rows=2
    - (1, 20)
    - (2, 30)

### `agent-nested-pipelines-let-cte-0011` — MismatchRows (high)

*Detail:* first differing row[3]: kusto=(99, null, 126) duck=(99, null, 60)

**KQL**
```kql
let g = datatable(k:long, v:long)[ 1,10, 2,20, 3,30 ]; let h = g | extend v = v + 1; let i2 = h | extend v = v * 2; let j = i2 | summarize sum(v); g | union (j | extend k = 99) | sort by k asc
```
**Generated SQL**
```sql
WITH g AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(2 AS BIGINT), CAST(20 AS BIGINT)), (CAST(3 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v)), h AS NOT MATERIALIZED (SELECT *, v + 1 AS v FROM g), i2 AS NOT MATERIALIZED (SELECT *, v * 2 AS v FROM h), j AS NOT MATERIALIZED (SELECT COALESCE(SUM(v), 0) AS sum_v FROM i2) (SELECT * FROM g) UNION ALL BY NAME (SELECT *, 99 AS k FROM j) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, v:Int, sum_v:Int] rows=4
    - (1, 10, null)
    - (2, 20, null)
    - (3, 30, null)
    - (99, null, 126)
- DuckDB: cols=[k:Int, v:Int, sum_v:Int] rows=4
    - (1, 10, null)
    - (2, 20, null)
    - (3, 30, null)
    - (99, null, 60)

### `agent-nested-pipelines-let-cte-0012` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[Column2:Dynamic|String]  
*Detail:* first differing row[0]: kusto=('x', 3, "Beta") duck=('x', 3, 'Beta')

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", "O'Brien","x" ] | summarize Column1 = count(), Column2 = make_list(s) by tag | mv-expand Column2 | project tag, Column1, Column2 | sort by tag asc, tostring(Column2) asc
```
**Generated SQL**
```sql
SELECT tag, Column1, Column2 FROM (SELECT t.* EXCLUDE (Column2), u.value AS Column2 FROM (SELECT tag, COUNT(*) AS Column1, COALESCE(LIST(s) FILTER (WHERE s IS NOT NULL), []) AS Column2 FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), ('O''Brien', 'x')) AS t(s, tag) GROUP BY ALL) AS t CROSS JOIN UNNEST(t.Column2) AS u(value)) ORDER BY tag ASC NULLS FIRST, TRY_CAST(Column2 AS TEXT) ASC NULLS FIRST
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

### `agent-nested-pipelines-let-cte-0015` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 10, 11) duck=(1, 5, 10)

**KQL**
```kql
let l1 = datatable(k:long, v:long)[ 1,5, 2,10, 3,15 ]; let l2 = l1 | extend w = v * 2; let l3 = l2 | extend v = w; let l4 = l3 | extend w = v + 1; l4 | project k, v, w | sort by k asc
```
**Generated SQL**
```sql
WITH l1 AS NOT MATERIALIZED (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(5 AS BIGINT)), (CAST(2 AS BIGINT), CAST(10 AS BIGINT)), (CAST(3 AS BIGINT), CAST(15 AS BIGINT))) AS t(k, v)), l2 AS NOT MATERIALIZED (SELECT *, v * 2 AS w FROM l1), l3 AS NOT MATERIALIZED (SELECT *, w AS v FROM l2), l4 AS NOT MATERIALIZED (SELECT *, v + 1 AS w FROM l3) SELECT k, v, w FROM l4 ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, v:Int, w:Int] rows=3
    - (1, 10, 11)
    - (2, 20, 21)
    - (3, 30, 31)
- DuckDB: cols=[k:Int, v:Int, w:Int] rows=3
    - (1, 5, 10)
    - (2, 10, 20)
    - (3, 15, 30)

### `agent-nested-pipelines-let-cte-0021` — MismatchColumns (high)

*Detail:* column count: kusto=3 duck=4 (kusto: [c, b, a]; duck: [c, b, a, b_1])

**KQL**
```kql
datatable(a:long, b:long, c:long)[ 1,2,3, 4,5,6, 7,8,9 ] | extend a = c, c = a | project a, b, c | extend b = a + c | project-reorder c, b, a | sort by a asc
```
**Generated SQL**
```sql
SELECT c, b, a, * EXCLUDE (c, b, a) FROM (SELECT *, a + c AS b FROM (SELECT a, b, c FROM (SELECT *, c AS a, a AS c FROM (VALUES (CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT)), (CAST(4 AS BIGINT), CAST(5 AS BIGINT), CAST(6 AS BIGINT)), (CAST(7 AS BIGINT), CAST(8 AS BIGINT), CAST(9 AS BIGINT))) AS t(a, b, c)))) ORDER BY a ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[c:Int, b:Int, a:Int] rows=3
    - (1, 4, 3)
    - (4, 10, 6)
    - (7, 16, 9)
- DuckDB: cols=[c:Int, b:Int, a:Int, b_1:Int] rows=3
    - (3, 2, 1, 4)
    - (6, 5, 4, 10)
    - (9, 8, 7, 16)

### `agent-nested-pipelines-let-cte-0024` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=4

**KQL**
```kql
let f = datatable(g:string, v:long)[ "a",1, "a",2, "b",3, "b",4 ]; f | summarize lst = make_list(v) by g | mv-apply x = lst to typeof(long) on (summarize sm = sum(x), mx = max(x)) | extend ratio = mx * 1.0 / sm | project g, sm, mx, ratio | sort by g asc
```
**Generated SQL**
```sql
WITH f AS NOT MATERIALIZED (SELECT * FROM (VALUES ('a', CAST(1 AS BIGINT)), ('a', CAST(2 AS BIGINT)), ('b', CAST(3 AS BIGINT)), ('b', CAST(4 AS BIGINT))) AS t(g, v)) SELECT g, sm, mx, ratio FROM (SELECT *, mx * 1.0 / sm AS ratio FROM (SELECT t.*, _sub.* FROM (SELECT g, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS lst FROM f GROUP BY ALL) AS t CROSS JOIN UNNEST(lst) AS u(value), LATERAL (SELECT COALESCE(SUM(x), 0) AS sm, MAX(x) AS mx FROM (SELECT u.value AS x)) AS _sub)) ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, sm:Int, mx:Int, ratio:Real] rows=2
    - ('a', 3, 2, 0.6666666666666666)
    - ('b', 7, 4, 0.5714285714285714)
- DuckDB: cols=[g:String, sm:Int, mx:Int, ratio:Real] rows=4
    - ('a', 2, 2, 1)
    - ('a', 1, 1, 1)
    - ('b', 4, 4, 1)
    - ('b', 3, 3, 1)

### `agent-nested-pipelines-let-cte-0035` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=5

**KQL**
```kql
let s = datatable(g:string, v:long)[ "a",1, "a",2, "a",3, "b",4, "b",5 ]; let agg = s | summarize lst = make_list(v), mn = min(v), mx = max(v) by g; let norm = agg | mv-apply x = lst to typeof(long) on (extend scaled = (x - mn) * 1.0 / (mx - mn) | summarize avgscaled = avg(scaled)); norm | project g, avgscaled | sort by g asc
```
**Generated SQL**
```sql
WITH s AS NOT MATERIALIZED (SELECT * FROM (VALUES ('a', CAST(1 AS BIGINT)), ('a', CAST(2 AS BIGINT)), ('a', CAST(3 AS BIGINT)), ('b', CAST(4 AS BIGINT)), ('b', CAST(5 AS BIGINT))) AS t(g, v)), agg AS NOT MATERIALIZED (SELECT g, COALESCE(LIST(v) FILTER (WHERE v IS NOT NULL), []) AS lst, MIN(v) AS mn, MAX(v) AS mx FROM s GROUP BY ALL), norm AS NOT MATERIALIZED (SELECT t.*, _sub.* FROM agg AS t CROSS JOIN UNNEST(lst) AS u(value), LATERAL (SELECT COALESCE(AVG(scaled), 'nan'::DOUBLE) AS avgscaled FROM (SELECT *, (x - mn) * 1.0 / (mx - mn) AS scaled FROM (SELECT u.value AS x))) AS _sub) SELECT g, avgscaled FROM norm ORDER BY g ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, avgscaled:Real] rows=2
    - ('a', 0.5)
    - ('b', 0.5)
- DuckDB: cols=[g:String, avgscaled:Real] rows=5
    - ('a', 1)
    - ('a', 0.5)
    - ('a', 0)
    - ('b', 1)
    - ('b', 0)

### `agent-nested-pipelines-let-cte-0036` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 11) duck=(1, 10)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 2,20, 3,30 ] | extend v = v + 1 | summarize sum_v = sum(v) by k | extend v = sum_v | summarize sum(v) by k | project k, sum_v1 = sum_v | sort by k asc
```
**Generated SQL**
```sql
SELECT k, sum_v AS sum_v1 FROM (SELECT k, COALESCE(SUM(v), 0) AS sum_v FROM (SELECT *, sum_v AS v FROM (SELECT k, COALESCE(SUM(v), 0) AS sum_v FROM (SELECT *, v + 1 AS v FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(2 AS BIGINT), CAST(20 AS BIGINT)), (CAST(3 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v)) GROUP BY ALL)) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, sum_v1:Int] rows=3
    - (1, 11)
    - (2, 21)
    - (3, 31)
- DuckDB: cols=[k:Int, sum_v1:Int] rows=3
    - (1, 10)
    - (2, 20)
    - (3, 30)

### `agent-nested-pipelines-let-cte-0038` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(10, 1, 9) duck=(3, 30, -27)

**KQL**
```kql
datatable(x:long, y:long)[ 1,10, 2,20, 3,30 ] | extend tmp = x | extend x = y | extend y = tmp | project-away tmp | extend z = x - y | project x, y, z | sort by z asc
```
**Generated SQL**
```sql
SELECT x, y, z FROM (SELECT *, x - y AS z FROM (SELECT * EXCLUDE (tmp) FROM (SELECT *, tmp AS y FROM (SELECT *, y AS x FROM (SELECT *, x AS tmp FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(2 AS BIGINT), CAST(20 AS BIGINT)), (CAST(3 AS BIGINT), CAST(30 AS BIGINT))) AS t(x, y)))))) ORDER BY z ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:Int, z:Int] rows=3
    - (10, 1, 9)
    - (20, 2, 18)
    - (30, 3, 27)
- DuckDB: cols=[x:Int, y:Int, z:Int] rows=3
    - (3, 30, -27)
    - (2, 20, -18)
    - (1, 10, -9)

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

## Family: null-and-edge (44)

### `agent-null-and-edge-0008` — SqlExecError (highest)

*Detail:* Binder Error: UNION (ALL) BY NAME operation doesn't support duplicate names in the SELECT list - the name "s" occurs multiple times

**KQL**
```kql
datatable(s:string)[ "a", "" ] | union (datatable(s:string)[ "c" ] | extend s=tostring(dynamic(null))) | extend e=isempty(s), n=isnull(s) | project s, e, n
```
**Generated SQL**
```sql
SELECT s, e, n FROM (SELECT *, (s IS NULL OR CAST(s AS VARCHAR) = '') AS e, (s IS NULL) AS n FROM ((SELECT * FROM (VALUES ('a'), ('')) AS t(s)) UNION ALL BY NAME (SELECT *, TRY_CAST(NULL AS TEXT) AS s FROM (VALUES ('c')) AS t(s))))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, e:Bool, n:Bool] rows=3
    - ('a', False, False)
    - ('', True, False)
    - ('', True, False)
- DuckDB: ERROR — Binder Error: UNION (ALL) BY NAME operation doesn't support duplicate names in the SELECT list - the name "s" occurs multiple times

### `agent-null-and-edge-0028` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

**KQL**
```kql
print x = int(2147483647) + 1
```
**Generated SQL**
```sql
SELECT 2147483647 + 1 AS x
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int] rows=1
    - (2147483648)
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

### `agent-null-and-edge-0011` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!

**KQL**
```kql
datatable(x:long)[ 9223372036854775807, -9223372036854775808, 0 ] | extend p=x+1, m=x-1, d=x*2 | project x, p, m, d
```
**Generated SQL**
```sql
SELECT x, p, m, d FROM (SELECT *, x + 1 AS p, x - 1 AS m, x * 2 AS d FROM (VALUES (CAST(9223372036854775807 AS BIGINT)), (CAST(-9223372036854775808 AS BIGINT)), (CAST(0 AS BIGINT))) AS t(x))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, p:Int, m:Int, d:Int] rows=3
    - (9223372036854775807, -9223372036854775808, 9223372036854775806, -2)
    - (-9223372036854775808, -9223372036854775807, 9223372036854775807, 0)
    - (0, 1, -1, 0)
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!

### `agent-null-and-edge-0012` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

**KQL**
```kql
print a=int(2147483647)+int(1), b=int(-2147483648)-int(1), c=long(2147483647)+long(1), d=int(2147483647)*int(2)
```
**Generated SQL**
```sql
SELECT 2147483647 + 1 AS a, -2147483648 - 1 AS b, 2147483647 + 1 AS c, 2147483647 * 2 AS d
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Int, b:Int, c:Int, d:Int] rows=1
    - (2147483648, -2147483649, 2147483648, 4294967294)
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

### `agent-null-and-edge-0015` — SqlExecError (highest)

*Detail:* Out of Range Error: cannot take logarithm of zero

**KQL**
```kql
datatable(r:real)[ 1.0, 0.0, -1.0 ] | extend d=1.0/r, l=log(r), sq=sqrt(r) | project r, d, l, sq | order by r asc
```
**Generated SQL**
```sql
SELECT r, d, l, sq FROM (SELECT *, 1.0 / r AS d, LN(r) AS l, SQRT(r) AS sq FROM (VALUES (CAST(1.0 AS DOUBLE)), (CAST(0.0 AS DOUBLE)), (CAST(-1.0 AS DOUBLE))) AS t(r)) ORDER BY r ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[r:Real, d:Real, l:Real, sq:Real] rows=3
    - (-1, -1, null, NaN)
    - (0, Infinity, -Infinity, 0)
    - (1, 1, 0, 1)
- DuckDB: ERROR — Out of Range Error: cannot take logarithm of zero

### `agent-null-and-edge-0025` — SqlExecError (highest)

*Detail:* Binder Error: UNNEST requires a single list as input

LINE 1: ...), (NULL), (LIST_VALUE(1, 2, 3))) AS t(d)) AS t CROSS JOIN UNNEST(t.d) AS u(value))
                                                                      ^

**KQL**
```kql
datatable(d:dynamic)[ dynamic({"k":1}), dynamic(null), dynamic([1,2,3]) ] | mv-expand d | project d
```
**Generated SQL**
```sql
SELECT d FROM (SELECT t.* EXCLUDE (d), u.value AS d FROM (SELECT * FROM (VALUES ('{"k":1}'::JSON), (NULL), (LIST_VALUE(1, 2, 3))) AS t(d)) AS t CROSS JOIN UNNEST(t.d) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic] rows=5
    - ({
  "k": 1
})
    - (null)
    - (1)
    - (2)
    - (3)
- DuckDB: ERROR — Binder Error: UNNEST requires a single list as input

LINE 1: ...), (NULL), (LIST_VALUE(1, 2, 3))) AS t(d)) AS t CROSS JOIN UNNEST(t.d) AS u(value))
                                                                      ^

### `agent-null-and-edge-0028` — SqlExecError (highest)

*Detail:* Conversion Error: Malformed JSON at byte 0 of input: input length is 0.  Input: ""

LINE 1: ... JSON) AS a, CAST('[]' AS JSON) AS b, CAST('{}' AS JSON) AS c, CAST('' AS JSON) AS d, CAST('not json' AS JSON) AS e, (CAST...
                                                                          ^

**KQL**
```kql
print a=parse_json("null"), b=parse_json("[]"), c=parse_json("{}"), d=parse_json(""), e=parse_json("not json"), f=isnull(parse_json("null"))
```
**Generated SQL**
```sql
SELECT CAST('null' AS JSON) AS a, CAST('[]' AS JSON) AS b, CAST('{}' AS JSON) AS c, CAST('' AS JSON) AS d, CAST('not json' AS JSON) AS e, (CAST('null' AS JSON) IS NULL) AS f
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Dynamic, b:Dynamic, c:Dynamic, d:Dynamic, e:Dynamic, f:Bool] rows=1
    - (null, [], {}, null, "not json", True)
- DuckDB: ERROR — Conversion Error: Malformed JSON at byte 0 of input: input length is 0.  Input: ""

LINE 1: ... JSON) AS a, CAST('[]' AS JSON) AS b, CAST('{}' AS JSON) AS c, CAST('' AS JSON) AS d, CAST('not json' AS JSON) AS e, (CAST...
                                                                          ^

### `agent-null-and-edge-0014` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!

**KQL**
```kql
datatable(x:long)[ 9223372036854775807, long(null) ] | extend y=x+1, z=coalesce(x, 0)+1 | project x, y, z
```
**Generated SQL**
```sql
SELECT x, y, z FROM (SELECT *, x + 1 AS y, COALESCE(x, 0) + 1 AS z FROM (VALUES (CAST(9223372036854775807 AS BIGINT)), (CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(x))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:Int, z:Int] rows=2
    - (9223372036854775807, -9223372036854775808, -9223372036854775808)
    - (null, null, 1)
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!

### `agent-null-and-edge-0038` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(0, 0) duck=(0, null)

**KQL**
```kql
print l = strlen(""), n = strlen(tostring(dynamic(null)))
```
**Generated SQL**
```sql
SELECT LENGTH(CAST('' AS VARCHAR)) AS l, LENGTH(CAST(TRY_CAST(NULL AS TEXT) AS VARCHAR)) AS n
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[l:Int, n:Int] rows=1
    - (0, 0)
- DuckDB: cols=[l:Int, n:Int] rows=1
    - (0, null)

### `agent-null-and-edge-0039` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('', 'c') duck=('', 'a')

**KQL**
```kql
print sub = substring("abc", 5, 3), neg = substring("abc", -1, 2)
```
**Generated SQL**
```sql
SELECT SUBSTR(CAST('abc' AS VARCHAR), (5) + 1, 3) AS sub, SUBSTR(CAST('abc' AS VARCHAR), ((-1)) + 1, 2) AS neg
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[sub:String, neg:String] rows=1
    - ('', 'c')
- DuckDB: cols=[sub:String, neg:String] rows=1
    - ('', 'a')

### `agent-null-and-edge-0044` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('null', True, null) duck=('dictionary', True, null)

**KQL**
```kql
datatable(d:dynamic)[ dynamic(null), dynamic({"a":1}), dynamic([]) ] | extend t=gettype(d), n=isnull(d), an=array_length(d) | project t, n, an
```
**Generated SQL**
```sql
SELECT t, n, an FROM (SELECT *, CASE WHEN TYPEOF(d) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(d) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(d) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(d) = 'VARCHAR' THEN 'string' WHEN TYPEOF(d) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(d) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(d) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(d) = 'UUID' THEN 'guid' WHEN TYPEOF(d) LIKE '%[]' OR TYPEOF(d) LIKE 'STRUCT%' OR TYPEOF(d) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(d) = 'JSON' THEN (CASE WHEN json_type(CAST(d AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(d)) END AS t, (d IS NULL) AS n, LEN(d) AS an FROM (VALUES (NULL), ('{"a":1}'::JSON), (LIST_VALUE())) AS t(d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:String, n:Bool, an:Int] rows=3
    - ('null', True, null)
    - ('dictionary', False, null)
    - ('array', False, 0)
- DuckDB: cols=[t:String, n:Bool, an:Int] rows=3
    - ('dictionary', True, null)
    - ('dictionary', False, 7)
    - ('array', False, 2)

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

### `agent-null-and-edge-0010` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a', '', True, False, False, 'aZ') duck=('a', null, True, False, True, 'aZ')

**KQL**
```kql
datatable(s:string)[ "a", "", "b", "" ] | extend n=tostring(dynamic(null)) | extend r1=isempty(n), r2=isempty(s), r3=isnull(n), r4=strcat(s, n, "Z") | project s, n, r1, r2, r3, r4
```
**Generated SQL**
```sql
SELECT s, n, r1, r2, r3, r4 FROM (SELECT *, (n IS NULL OR CAST(n AS VARCHAR) = '') AS r1, (s IS NULL OR CAST(s AS VARCHAR) = '') AS r2, (n IS NULL) AS r3, CONCAT(s, n, 'Z') AS r4 FROM (SELECT *, TRY_CAST(NULL AS TEXT) AS n FROM (VALUES ('a'), (''), ('b'), ('')) AS t(s)))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, n:String, r1:Bool, r2:Bool, r3:Bool, r4:String] rows=4
    - ('a', '', True, False, False, 'aZ')
    - ('', '', True, True, False, 'Z')
    - ('b', '', True, False, False, 'bZ')
    - ('', '', True, True, False, 'Z')
- DuckDB: cols=[s:String, n:String, r1:Bool, r2:Bool, r3:Bool, r4:String] rows=4
    - ('a', null, True, False, True, 'aZ')
    - ('', null, True, True, True, 'Z')
    - ('b', null, True, False, True, 'bZ')
    - ('', null, True, True, True, 'Z')

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

### `agent-null-and-edge-0021` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('', '', '', '', '', '') duck=(null, null, null, null, null, null)

**KQL**
```kql
print a=tostring(int(null)), b=tostring(real(null)), c=tostring(datetime(null)), d=tostring(bool(null)), e=tostring(long(null)), f=tostring(dynamic(null))
```
**Generated SQL**
```sql
SELECT TRY_CAST(CAST(NULL AS INTEGER) AS TEXT) AS a, TRY_CAST(CAST(NULL AS DOUBLE) AS TEXT) AS b, TRY_CAST(CAST(NULL AS TIMESTAMP) AS TEXT) AS c, TRY_CAST(CAST(NULL AS BOOLEAN) AS TEXT) AS d, TRY_CAST(CAST(NULL AS BIGINT) AS TEXT) AS e, TRY_CAST(NULL AS TEXT) AS f
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String, b:String, c:String, d:String, e:String, f:String] rows=1
    - ('', '', '', '', '', '')
- DuckDB: cols=[a:String, b:String, c:String, d:String, e:String, f:String] rows=1
    - (null, null, null, null, null, null)

### `agent-null-and-edge-0024` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[va:Dynamic|String], TYPE_MISMATCH[vab:Dynamic|String], TYPE_MISMATCH[idx0:Dynamic|String]  
*Detail:* first differing row[0]: kusto=('null', null, null, null, null) duck=('dictionary', null, null, null, null)

**KQL**
```kql
datatable(d:dynamic)[ dynamic(null), dynamic({"a":null}), dynamic({"a":{"b":null}}), dynamic([null,1,null]) ] | extend va=d.a, vab=d.a.b, idx0=d[0], al=array_length(d), t=gettype(d) | project t, va, vab, idx0, al
```
**Generated SQL**
```sql
SELECT t, va, vab, idx0, al FROM (SELECT *, json_extract(d, '$.a') AS va, json_extract(d, '$.a.b') AS vab, d[0 + 1] AS idx0, LEN(d) AS al, CASE WHEN TYPEOF(d) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(d) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(d) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(d) = 'VARCHAR' THEN 'string' WHEN TYPEOF(d) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(d) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(d) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(d) = 'UUID' THEN 'guid' WHEN TYPEOF(d) LIKE '%[]' OR TYPEOF(d) LIKE 'STRUCT%' OR TYPEOF(d) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(d) = 'JSON' THEN (CASE WHEN json_type(CAST(d AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(d)) END AS t FROM (VALUES (NULL), ('{"a":null}'::JSON), ('{"a":{"b":null}}'::JSON), (LIST_VALUE(NULL, 1, NULL))) AS t(d))
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
    - ('dictionary', 'null', null, null, 10)
    - ('dictionary', '{"b":null}', 'null', null, 16)
    - ('array', null, null, '1', 13)

### `agent-null-and-edge-0026` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(3, 3) duck=(2, 2)

**KQL**
```kql
datatable(d:dynamic)[ dynamic([]), dynamic(null), dynamic([1,2]) ] | mv-expand x=d | summarize c=count(), cx=count(x)
```
**Generated SQL**
```sql
SELECT COUNT(*) AS c, COUNT(*) AS cx FROM (SELECT t.*, u.value AS x FROM (SELECT * FROM (VALUES (LIST_VALUE()), (NULL), (LIST_VALUE(1, 2))) AS t(d)) AS t CROSS JOIN UNNEST(d) AS u(value))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[c:Int, cx:Int] rows=1
    - (3, 3)
- DuckDB: cols=[c:Int, cx:Int] rows=1
    - (2, 2)

### `agent-null-and-edge-0027` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[s:Real|Int]  
*Detail:* first differing row[0]: kusto=(4, 3, [
  1,
  3,
  null
]) duck=(4, 3, '<unreadable:IndexOutOfRangeException>')

**KQL**
```kql
datatable(d:dynamic)[ dynamic([1,null,3]), dynamic(null) ] | extend s=array_sum(d), l=array_length(d), srt=array_sort_asc(d) | project s, l, srt
```
**Generated SQL**
```sql
SELECT s, l, srt FROM (SELECT *, LIST_SUM(d) AS s, LEN(d) AS l, LIST_SORT(d) AS srt FROM (VALUES (LIST_VALUE(1, NULL, 3)), (NULL)) AS t(d))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Real, l:Int, srt:Dynamic] rows=2
    - (4, 3, [
  1,
  3,
  null
])
    - (null, null, null)
- DuckDB: cols=[s:Int, l:Int, srt:Unknown] rows=2
    - (4, 3, '<unreadable:IndexOutOfRangeException>')
    - (null, null, null)

### `agent-null-and-edge-0036` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('abc', 'c', 'bc', null, 'abc') duck=('abc', 'c', 'c', 0, 'abc')

**KQL**
```kql
datatable(s:string)[ "abc", "", "x" ] | extend sub1=substring(s, 2, 10), sub2=substring(s, -2, 5), idx=indexof(s, ""), rep=replace_string(s, "", "_") | project s, sub1, sub2, idx, rep
```
**Generated SQL**
```sql
SELECT s, sub1, sub2, idx, rep FROM (SELECT *, SUBSTR(CAST(s AS VARCHAR), (2) + 1, 10) AS sub1, SUBSTR(CAST(s AS VARCHAR), ((-2)) + 1, 5) AS sub2, (INSTR(CAST(s AS VARCHAR), '') - 1) AS idx, REPLACE(s, '', '_') AS rep FROM (VALUES ('abc'), (''), ('x')) AS t(s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, sub1:String, sub2:String, idx:Int, rep:String] rows=3
    - ('abc', 'c', 'bc', null, 'abc')
    - ('', '', '', null, '')
    - ('x', '', '', null, 'x')
- DuckDB: cols=[s:String, sub1:String, sub2:String, idx:Int, rep:String] rows=3
    - ('abc', 'c', 'c', 0, 'abc')
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

### `agent-null-and-edge-0041` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a', 1, 1) duck=('a', 3, 1)

**KQL**
```kql
datatable(g:string, x:long)[ "a",3, "a",long(null), "a",1, "b",long(null) ] | sort by g asc, x asc nulls last | extend rn=row_number(), rng=row_number(1, g != prev(g)) | project g, x, rn
```
**Generated SQL**
```sql
SELECT g, x, rn FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY g ASC NULLS FIRST, x DESC NULLS LAST) AS rn, ROW_NUMBER() OVER (ORDER BY g ASC NULLS FIRST, x DESC NULLS LAST) AS rng FROM (VALUES ('a', CAST(3 AS BIGINT)), ('a', CAST(CAST(NULL AS BIGINT) AS BIGINT)), ('a', CAST(1 AS BIGINT)), ('b', CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(g, x) ORDER BY g ASC NULLS FIRST, x DESC NULLS LAST)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, x:Int, rn:Int] rows=4
    - ('a', 1, 1)
    - ('a', 3, 2)
    - ('a', null, 3)
    - ('b', null, 4)
- DuckDB: cols=[g:String, x:Int, rn:Int] rows=4
    - ('a', 3, 1)
    - ('a', 1, 2)
    - ('a', null, 3)
    - ('b', null, 4)

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
SELECT m FROM (SELECT histogram(json_object(TRY_CAST(k AS TEXT), v)) AS m FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(CAST(NULL AS BIGINT) AS BIGINT)), (CAST(2 AS BIGINT), CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(k, v))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[m:Dynamic] rows=1
    - ({
  "1": 10,
  "2": null
})
- DuckDB: cols=[m:Unknown] rows=1
    - ({"{\u00221\u0022:10}":1,"{\u00221\u0022:null}":1,"{\u00222\u0022:null}":1})

### `agent-null-and-edge-0002` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=([
  "",
  "",
  ""
], [
  "abc",
  "",
  "x"
], 0, 3) duck=([], ["","abc","x"], 3, 3)

**KQL**
```kql
datatable(s:string)[ "abc", "", "x" ] | extend n=tostring(dynamic(null)) | summarize ml=make_list(n), ms=make_set(s), cn=countif(isnull(n)), ce=countif(isempty(n))
```
**Generated SQL**
```sql
SELECT COALESCE(LIST(n) FILTER (WHERE n IS NOT NULL), []) AS ml, COALESCE(LIST(DISTINCT s) FILTER (WHERE s IS NOT NULL), []) AS ms, COUNT(*) FILTER (WHERE (n IS NULL)) AS cn, COUNT(*) FILTER (WHERE (n IS NULL OR CAST(n AS VARCHAR) = '')) AS ce FROM (SELECT *, TRY_CAST(NULL AS TEXT) AS n FROM (VALUES ('abc'), (''), ('x')) AS t(s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ml:Dynamic, ms:Dynamic, cn:Int, ce:Int] rows=1
    - ([
  "",
  "",
  ""
], [
  "abc",
  "",
  "x"
], 0, 3)
- DuckDB: cols=[ml:Unknown, ms:Unknown, cn:Int, ce:Int] rows=1
    - ([], ["","abc","x"], 3, 3)

### `agent-null-and-edge-0006` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('pq', 'fb') duck=('pq', '')

**KQL**
```kql
print a=iif(isnull(int(null)) and isempty(""), strcat("p",tostring(int(null)),"q"), "x"), b=coalesce(strcat(tostring(real(null))), "fb")
```
**Generated SQL**
```sql
SELECT CASE WHEN (CAST(NULL AS INTEGER) IS NULL) AND ('' IS NULL OR CAST('' AS VARCHAR) = '') THEN CONCAT('p', TRY_CAST(CAST(NULL AS INTEGER) AS TEXT), 'q') ELSE 'x' END AS a, COALESCE(CONCAT(TRY_CAST(CAST(NULL AS DOUBLE) AS TEXT)), 'fb') AS b
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

### `agent-null-and-edge-0011` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1) duck=(0)

**KQL**
```kql
datatable(s:string)[ "a", "", "b" ] | extend n=tostring(dynamic(null)) | where s == n | count
```
**Generated SQL**
```sql
SELECT COUNT(*) AS Count FROM (SELECT *, TRY_CAST(NULL AS TEXT) AS n FROM (VALUES ('a'), (''), ('b')) AS t(s)) WHERE s = n
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[Count:Int] rows=1
    - (1)
- DuckDB: cols=[Count:Int] rows=1
    - (0)

### `agent-null-and-edge-0013` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1.5, 1, 1.5, 2) duck=(2.5, 2, 2, 3)

**KQL**
```kql
datatable(x:real)[ 1.5, real(null), 2.5 ] | extend b=bin(x, 1.0), f=floor(x, 0.5), c=ceiling(x) | project x, b, f, c | order by x asc nulls last
```
**Generated SQL**
```sql
SELECT x, b, f, c FROM (SELECT *, FLOOR((x)/(1.0))*(1.0) AS b, FLOOR(x) AS f, CEILING(x) AS c FROM (VALUES (CAST(1.5 AS DOUBLE)), (CAST(CAST(NULL AS DOUBLE) AS DOUBLE)), (CAST(2.5 AS DOUBLE))) AS t(x)) ORDER BY x DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Real, b:Real, f:Real, c:Real] rows=3
    - (1.5, 1, 1.5, 2)
    - (2.5, 2, 2.5, 3)
    - (null, null, null, null)
- DuckDB: cols=[x:Real, b:Real, f:Real, c:Real] rows=3
    - (2.5, 2, 2, 3)
    - (1.5, 1, 1, 2)
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

### `agent-null-and-edge-0029` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-01-01T00:00:00.0000000Z, null, null, '', '2020-01-01T00:00:00.0000000Z') duck=(2020-01-01T00:00:00.0000000Z, null, null, null, '2020-01-01 00:00:00')

**KQL**
```kql
datatable(t:datetime, s:timespan)[ datetime(2020-01-01),timespan(null), datetime(null),1d ] | extend e=t+s, ts=tostring(s), tt=tostring(t) | project t, s, e, ts, tt
```
**Generated SQL**
```sql
SELECT t, s, e, ts, tt FROM (SELECT *, t + s AS e, TRY_CAST(s AS TEXT) AS ts, TRY_CAST(t AS TEXT) AS tt FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00', CAST(NULL AS INTERVAL)), (CAST(NULL AS TIMESTAMP), (86400000 * INTERVAL '1 millisecond'))) AS t(t, s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, s:TimeSpan, e:DateTime, ts:String, tt:String] rows=2
    - (2020-01-01T00:00:00.0000000Z, null, null, '', '2020-01-01T00:00:00.0000000Z')
    - (null, 1.00:00:00, null, '1.00:00:00', '')
- DuckDB: cols=[t:DateTime, s:TimeSpan, e:DateTime, ts:String, tt:String] rows=2
    - (2020-01-01T00:00:00.0000000Z, null, null, null, '2020-01-01 00:00:00')
    - (null, 1.00:00:00, null, '24:00:00', null)

### `agent-null-and-edge-0031` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(null, null, null, '', null) duck=(null, null, null, null, null)

**KQL**
```kql
print a=tolong(int(null)), b=toint(long(null)), c=todouble(int(null)), d=tostring(toint("")), e=tolong(todouble("inf"))
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(CAST(NULL AS INTEGER) AS DOUBLE), TRY_CAST(TRY_CAST(CAST(NULL AS INTEGER) AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS a, TRY_CAST(TRUNC(COALESCE(TRY_CAST(CAST(NULL AS BIGINT) AS DOUBLE), TRY_CAST(TRY_CAST(CAST(NULL AS BIGINT) AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS b, COALESCE(TRY_CAST(CAST(NULL AS INTEGER) AS DOUBLE), TRY_CAST(TRY_CAST(CAST(NULL AS INTEGER) AS BOOLEAN) AS DOUBLE)) AS c, TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT) AS d, TRY_CAST(TRUNC(COALESCE(TRY_CAST(COALESCE(TRY_CAST('inf' AS DOUBLE), TRY_CAST(TRY_CAST('inf' AS BOOLEAN) AS DOUBLE)) AS DOUBLE), TRY_CAST(TRY_CAST(COALESCE(TRY_CAST('inf' AS DOUBLE), TRY_CAST(TRY_CAST('inf' AS BOOLEAN) AS DOUBLE)) AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS e
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Int, b:Int, c:Real, d:String, e:Int] rows=1
    - (null, null, null, '', null)
- DuckDB: cols=[a:Int, b:Int, c:Real, d:String, e:Int] rows=1
    - (null, null, null, null, null)

### `agent-null-and-edge-0032` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(False, 0, 'False', True, False, False) duck=(True, 1, 'true', False, True, True)

**KQL**
```kql
datatable(b:bool)[ true, false, bool(null) ] | extend i=toint(b), s=tostring(b), n=not(b), a=b and true, o=b or false | project b, i, s, n, a, o | order by b asc nulls last
```
**Generated SQL**
```sql
SELECT b, i, s, n, a, o FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(b AS DOUBLE), TRY_CAST(TRY_CAST(b AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS i, TRY_CAST(b AS TEXT) AS s, NOT (b) AS n, b AND TRUE AS a, b OR FALSE AS o FROM (VALUES (TRUE), (FALSE), (FALSE)) AS t(b)) ORDER BY b DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[b:Bool, i:Int, s:String, n:Bool, a:Bool, o:Bool] rows=3
    - (False, 0, 'False', True, False, False)
    - (True, 1, 'True', False, True, True)
    - (null, null, '', null, null, null)
- DuckDB: cols=[b:Bool, i:Int, s:String, n:Bool, a:Bool, o:Bool] rows=3
    - (True, 1, 'true', False, True, True)
    - (False, 0, 'false', True, False, False)
    - (False, 0, 'false', True, False, False)

### `agent-null-and-edge-0034` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a', null, 0) duck=('a', 2, 1)

**KQL**
```kql
datatable(g:string, x:long)[ "a",long(null), "a",2, "b",long(null), "b",long(null) ] | sort by g asc, x desc nulls first | extend rn=row_number(0, g!=prev(g)) | project g, x, rn
```
**Generated SQL**
```sql
SELECT g, x, rn FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY g ASC NULLS FIRST, x DESC NULLS LAST) AS rn FROM (VALUES ('a', CAST(CAST(NULL AS BIGINT) AS BIGINT)), ('a', CAST(2 AS BIGINT)), ('b', CAST(CAST(NULL AS BIGINT) AS BIGINT)), ('b', CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(g, x) ORDER BY g ASC NULLS FIRST, x DESC NULLS LAST)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, x:Int, rn:Int] rows=4
    - ('a', null, 0)
    - ('a', 2, 1)
    - ('b', null, 0)
    - ('b', null, 1)
- DuckDB: cols=[g:String, x:Int, rn:Int] rows=4
    - ('a', 2, 1)
    - ('a', null, 2)
    - ('b', null, 3)
    - ('b', null, 4)

### `agent-null-and-edge-0043` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[c:Int|Real]  
*Detail:* first differing row[1]: kusto=('a"b', 3, 'A"B', '"b', 1) duck=('a\"b', 4, 'A\"B', '\"b', 1)

**KQL**
```kql
datatable(s:string)[ "O'Brien", "a\"b", "tab\ttab", "" ] | extend l=strlen(s), u=toupper(s), sub=substring(s, 1, 100), c=countof(s, "b") | project s, l, u, sub, c
```
**Generated SQL**
```sql
SELECT s, l, u, sub, c FROM (SELECT *, LENGTH(CAST(s AS VARCHAR)) AS l, UPPER(s) AS u, SUBSTR(CAST(s AS VARCHAR), (1) + 1, 100) AS sub, (LENGTH(s) - LENGTH(REPLACE(s, 'b', ''))) / LENGTH('b') AS c FROM (VALUES ('O''Brien'), ('a\"b'), ('tab\ttab'), ('')) AS t(s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, l:Int, u:String, sub:String, c:Int] rows=4
    - ('O'Brien', 7, 'O'BRIEN', ''Brien', 0)
    - ('a"b', 3, 'A"B', '"b', 1)
    - ('tab	tab', 7, 'TAB	TAB', 'ab	tab', 2)
    - ('', 0, '', '', 0)
- DuckDB: cols=[s:String, l:Int, u:String, sub:String, c:Real] rows=4
    - ('O'Brien', 7, 'O'BRIEN', ''Brien', 0)
    - ('a\"b', 4, 'A\"B', '\"b', 1)
    - ('tab\ttab', 8, 'TAB\TTAB', 'ab\ttab', 2)
    - ('', 0, '', '', 0)

### `agent-null-and-edge-0044` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(-2147483649, 2147483647, 2147483647) duck=(2147483648, null, null)

**KQL**
```kql
datatable(x:long)[ 2147483648, -2147483649, long(null) ] | extend i=toint(x), back=tolong(toint(x)) | project x, i, back | order by x asc nulls last
```
**Generated SQL**
```sql
SELECT x, i, back FROM (SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS i, TRY_CAST(TRUNC(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS back FROM (VALUES (CAST(2147483648 AS BIGINT)), (CAST(-2147483649 AS BIGINT)), (CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(x)) ORDER BY x DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, i:Int, back:Int] rows=3
    - (-2147483649, 2147483647, 2147483647)
    - (2147483648, -2147483648, -2147483648)
    - (null, null, null)
- DuckDB: cols=[x:Int, i:Int, back:Int] rows=3
    - (2147483648, null, null)
    - (-2147483649, null, null)
    - (null, null, null)

### `agent-null-and-edge-0006` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(a:long)[ 1, long(null), 2 ] | join kind=fullouter (datatable(a:long)[ long(null), 2, 3 ]) on a | project left=a, right=a1 | order by left asc nulls last, right asc nulls last
```
**Generated SQL**
```sql
SELECT a AS "left", a1 AS "right" FROM (SELECT L.*, R.a AS a1 FROM (SELECT * FROM (VALUES (CAST(1 AS BIGINT)), (CAST(CAST(NULL AS BIGINT) AS BIGINT)), (CAST(2 AS BIGINT))) AS t(a)) AS L FULL OUTER JOIN (SELECT * FROM (VALUES (CAST(CAST(NULL AS BIGINT) AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT))) AS t(a)) AS R ON L.a IS NOT DISTINCT FROM R.a) ORDER BY "left" DESC NULLS LAST, "right" DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[left:Int, right:Int] rows=4
    - (1, null)
    - (2, 2)
    - (null, 3)
    - (null, null)
- DuckDB: cols=[left:Int, right:Int] rows=4
    - (2, 2)
    - (1, null)
    - (null, 3)
    - (null, null)

### `agent-null-and-edge-0007` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(k:long)[ 1, 2, long(null) ] | lookup (datatable(k:long, name:string)[ 1,"x", long(null),"nullname" ]) on k | project k, name | order by k asc nulls first
```
**Generated SQL**
```sql
SELECT k, name FROM (SELECT L.*, COALESCE(R.name, '') AS name FROM (SELECT * FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(CAST(NULL AS BIGINT) AS BIGINT))) AS t(k)) AS L LEFT OUTER JOIN (SELECT * FROM (VALUES (CAST(1 AS BIGINT), 'x'), (CAST(CAST(NULL AS BIGINT) AS BIGINT), 'nullname')) AS t(k, name)) AS R ON L.k IS NOT DISTINCT FROM R.k) ORDER BY k DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, name:String] rows=3
    - (null, 'nullname')
    - (1, 'x')
    - (2, '')
- DuckDB: cols=[k:Int, name:String] rows=3
    - (2, '')
    - (1, 'x')
    - (null, 'nullname')

### `agent-null-and-edge-0019` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(t:datetime, span:timespan)[ datetime(2020-01-01),1d, datetime(null),timespan(null) ] | extend e=t+span | project t, span, e | order by e asc nulls first
```
**Generated SQL**
```sql
SELECT t, span, e FROM (SELECT *, t + span AS e FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00', (86400000 * INTERVAL '1 millisecond')), (CAST(NULL AS TIMESTAMP), CAST(NULL AS INTERVAL))) AS t(t, span)) ORDER BY e DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, span:TimeSpan, e:DateTime] rows=2
    - (null, null, null)
    - (2020-01-01T00:00:00.0000000Z, 1.00:00:00, 2020-01-02T00:00:00.0000000Z)
- DuckDB: cols=[t:DateTime, span:TimeSpan, e:DateTime] rows=2
    - (2020-01-01T00:00:00.0000000Z, 1.00:00:00, 2020-01-02T00:00:00.0000000Z)
    - (null, null, null)

### `agent-null-and-edge-0028` — MismatchOrder (medium)

*Sub-verdicts:* TYPE_MISMATCH[w:TimeSpan|String]  
*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(t:datetime)[ datetime(2020-01-01), datetime(null) ] | extend a=t+timespan(null), b=t-1d, w=dayofweek(t), m=monthofyear(t) | project t, a, b, w, m | order by t asc nulls first
```
**Generated SQL**
```sql
SELECT t, a, b, w, m FROM (SELECT *, t + CAST(NULL AS INTERVAL) AS a, t - (86400000 * INTERVAL '1 millisecond') AS b, CASE WHEN EXTRACT(DOW FROM t) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM t) AS VARCHAR) || '.00:00:00' END AS w, EXTRACT(MONTH FROM t) AS m FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00'), (CAST(NULL AS TIMESTAMP))) AS t(t)) ORDER BY t DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, a:DateTime, b:DateTime, w:TimeSpan, m:Int] rows=2
    - (null, null, null, null, null)
    - (2020-01-01T00:00:00.0000000Z, null, 2019-12-31T00:00:00.0000000Z, 3.00:00:00, 1)
- DuckDB: cols=[t:DateTime, a:DateTime, b:DateTime, w:String, m:Int] rows=2
    - (2020-01-01T00:00:00.0000000Z, null, 2019-12-31T00:00:00.0000000Z, '3.00:00:00', 1)
    - (null, null, null, null, null)

### `agent-null-and-edge-0039` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(k:long)[ long(null), long(null), 1 ] | summarize c=count() by k | order by k asc nulls first
```
**Generated SQL**
```sql
SELECT * FROM (SELECT k, COUNT(*) AS c FROM (VALUES (CAST(CAST(NULL AS BIGINT) AS BIGINT)), (CAST(CAST(NULL AS BIGINT) AS BIGINT)), (CAST(1 AS BIGINT))) AS t(k) GROUP BY ALL) ORDER BY k DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, c:Int] rows=2
    - (null, 2)
    - (1, 1)
- DuckDB: cols=[k:Int, c:Int] rows=2
    - (1, 1)
    - (null, 2)

### `agent-null-and-edge-0041` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(x:long)[ 0, long(null), -1 ] | extend d=10/x, m=10%x | project x, d, m | order by x asc nulls last
```
**Generated SQL**
```sql
SELECT x, d, m FROM (SELECT *, CAST(TRUNC(CAST(10 AS DOUBLE) / NULLIF(x, 0)) AS BIGINT) AS d, (((10) % NULLIF(x, 0)) + ABS(x)) % NULLIF(x, 0) AS m FROM (VALUES (CAST(0 AS BIGINT)), (CAST(CAST(NULL AS BIGINT) AS BIGINT)), (CAST(-1 AS BIGINT))) AS t(x)) ORDER BY x DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, d:Int, m:Int] rows=3
    - (-1, -10, 0)
    - (0, null, null)
    - (null, null, null)
- DuckDB: cols=[x:Int, d:Int, m:Int] rows=3
    - (0, null, null)
    - (-1, -10, 0)
    - (null, null, null)

## Family: parse-search (17)

### `agent-parse-search-0012` — SqlExecError (highest)

*Detail:* Conversion Error: Could not convert string 'error' to BOOL

LINE 1: ... BIGINT)), ('info', CAST(3 AS BIGINT))) AS t(s, n) WHERE NOT ('error'))
                                                                         ^

**KQL**
```kql
datatable(s:string, n:long)[ "error",1, "warn",2, "info",3 ] | search not("error") | project s
```
**Generated SQL**
```sql
SELECT s FROM (SELECT 'search_arg0' AS "$table", * FROM (VALUES ('error', CAST(1 AS BIGINT)), ('warn', CAST(2 AS BIGINT)), ('info', CAST(3 AS BIGINT))) AS t(s, n) WHERE NOT ('error'))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String] rows=2
    - ('warn')
    - ('info')
- DuckDB: ERROR — Conversion Error: Could not convert string 'error' to BOOL

LINE 1: ... BIGINT)), ('info', CAST(3 AS BIGINT))) AS t(s, n) WHERE NOT ('error'))
                                                                         ^

### `agent-parse-search-0021` — SqlExecError (highest)

*Detail:* Invalid Input Error: Type INT64 with value 17280000000 can't be cast because the value is out of range for the destination type INT32

**KQL**
```kql
datatable(t:datetime)[ datetime(2020-01-01), datetime(2020-07-01), datetime(2021-01-01), datetime(2021-07-01) ] | where t between (datetime(2020-06-01) .. (datetime(2020-06-01)+200d)) | project t
```
**Generated SQL**
```sql
SELECT t FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00'), (TIMESTAMP '2020-07-01 00:00:00'), (TIMESTAMP '2021-01-01 00:00:00'), (TIMESTAMP '2021-07-01 00:00:00')) AS t(t) WHERE t BETWEEN TIMESTAMP '2020-06-01 00:00:00' AND (TIMESTAMP '2020-06-01 00:00:00' + (17280000000 * INTERVAL '1 millisecond'))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime] rows=1
    - (2020-07-01T00:00:00.0000000Z)
- DuckDB: ERROR — Invalid Input Error: Type INT64 with value 17280000000 can't be cast because the value is out of range for the destination type INT32

### `agent-parse-search-0033` — SqlExecError (highest)

*Detail:* Invalid Input Error: Type INT64 with value 63072000000 can't be cast because the value is out of range for the destination type INT32

**KQL**
```kql
datatable(s:string)[ "from=2020-01-01 to=2021-12-31" ] | parse s with "from=" d1:datetime " to=" d2:datetime | where d2 between (d1 .. (d1 + 730d)) | project d1, d2
```
**Generated SQL**
```sql
SELECT d1, d2 FROM (SELECT *, TRY_CAST(REGEXP_EXTRACT(s, 'from=(.*?)\ to=(.*)', 1) AS TIMESTAMP) AS d1, TRY_CAST(REGEXP_EXTRACT(s, 'from=(.*?)\ to=(.*)', 2) AS TIMESTAMP) AS d2 FROM (VALUES ('from=2020-01-01 to=2021-12-31')) AS t(s)) WHERE d2 BETWEEN d1 AND (d1 + (63072000000 * INTERVAL '1 millisecond'))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d1:DateTime, d2:DateTime] rows=1
    - (2020-01-01T00:00:00.0000000Z, 2021-12-31T00:00:00.0000000Z)
- DuckDB: ERROR — Invalid Input Error: Type INT64 with value 63072000000 can't be cast because the value is out of range for the destination type INT32

### `agent-parse-search-0040` — SqlExecError (highest)

*Detail:* Conversion Error: Could not convert string 'low' to BOOL

LINE 1: ...)) AS t(s, score) WHERE (score BETWEEN 50.0 AND 100.0 AND NOT ('low')))
                                                                          ^

**KQL**
```kql
datatable(s:string, score:real)[ "high",95.5, "low",12.3, "mid",50.0 ] | search score between (50.0 .. 100.0) and not("low") | project s, score
```
**Generated SQL**
```sql
SELECT s, score FROM (SELECT 'search_arg0' AS "$table", * FROM (VALUES ('high', CAST(95.5 AS DOUBLE)), ('low', CAST(12.3 AS DOUBLE)), ('mid', CAST(50.0 AS DOUBLE))) AS t(s, score) WHERE (score BETWEEN 50.0 AND 100.0 AND NOT ('low')))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, score:Real] rows=2
    - ('high', 95.5)
    - ('mid', 50)
- DuckDB: ERROR — Conversion Error: Could not convert string 'low' to BOOL

LINE 1: ...)) AS t(s, score) WHERE (score BETWEEN 50.0 AND 100.0 AND NOT ('low')))
                                                                          ^

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

*Detail:* first differing row[0]: kusto=('name="John Smith";city="New York"', 'John Smith', 'New York') duck=('name=\"John Smith\";city=\"New York\', '\"John Smith\"', '\"New York\')

**KQL**
```kql
datatable(s:string)[ "name=\"John Smith\";city=\"New York\"" ] | parse-kv s as (name:string, city:string) with (pair_delimiter=';', kv_delimiter='=', quote='"')
```
**Generated SQL**
```sql
SELECT *, TRIM(REGEXP_EXTRACT(s, '(^|;)\s*name\s*=\s*([^;]*)', 2)) AS name, TRIM(REGEXP_EXTRACT(s, '(^|;)\s*city\s*=\s*([^;]*)', 2)) AS city FROM (VALUES ('name=\"John Smith\";city=\"New York\')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, name:String, city:String] rows=1
    - ('name="John Smith";city="New York"', 'John Smith', 'New York')
- DuckDB: cols=[s:String, name:String, city:String] rows=1
    - ('name=\"John Smith\";city=\"New York\', '\"John Smith\"', '\"New York\')

### `agent-parse-search-0010` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=0

**KQL**
```kql
datatable(s:string, n:long)[ "alpha apple",1, "beta banana",2, "gamma grape",3 ] | search "a*" | project s
```
**Generated SQL**
```sql
SELECT s FROM (SELECT 'search_arg0' AS "$table", * FROM (VALUES ('alpha apple', CAST(1 AS BIGINT)), ('beta banana', CAST(2 AS BIGINT)), ('gamma grape', CAST(3 AS BIGINT))) AS t(s, n) WHERE (REGEXP_MATCHES(LOWER(CAST(s AS VARCHAR)), '(^|[^A-Za-z0-9])a\*([^A-Za-z0-9]|$)') OR REGEXP_MATCHES(LOWER(CAST(n AS VARCHAR)), '(^|[^A-Za-z0-9])a\*([^A-Za-z0-9]|$)')))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String] rows=1
    - ('alpha apple')
- DuckDB: cols=[s:String] rows=0

### `agent-parse-search-0011` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=0

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","y", "café","z", "delta","x" ] | where * has "café"
```
**Generated SQL**
```sql
SELECT * FROM (VALUES ('alpha', 'x'), ('Beta', 'y'), ('café', 'z'), ('delta', 'x')) AS t(s, tag) WHERE (regexp_matches(CAST(CAST(s AS VARCHAR) AS VARCHAR), '(?i)\bcafé\b') OR regexp_matches(CAST(CAST(tag AS VARCHAR) AS VARCHAR), '(?i)\bcafé\b'))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tag:String] rows=1
    - ('café', 'z')
- DuckDB: cols=[s:String, tag:String] rows=0

### `agent-parse-search-0018` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=0

**KQL**
```kql
datatable(s:string)[ "10.5", "abc", "20", "-3.14" ] | search "10*"
```
**Generated SQL**
```sql
SELECT 'search_arg0' AS "$table", * FROM (VALUES ('10.5'), ('abc'), ('20'), ('-3.14')) AS t(s) WHERE REGEXP_MATCHES(LOWER(CAST(s AS VARCHAR)), '(^|[^A-Za-z0-9])10\*([^A-Za-z0-9]|$)')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[$table:String, s:String] rows=1
    - ('search_arg0', '10.5')
- DuckDB: cols=[$table:String, s:String] rows=0

### `agent-parse-search-0019` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=0

**KQL**
```kql
datatable(host:string, env:string)[ "web01","prod", "db02","dev", "web03","prod" ] | search "web*" and env == "prod" | project host
```
**Generated SQL**
```sql
SELECT host FROM (SELECT 'search_arg0' AS "$table", * FROM (VALUES ('web01', 'prod'), ('db02', 'dev'), ('web03', 'prod')) AS t(host, env) WHERE ((REGEXP_MATCHES(LOWER(CAST(host AS VARCHAR)), '(^|[^A-Za-z0-9])web\*([^A-Za-z0-9]|$)') OR REGEXP_MATCHES(LOWER(CAST(env AS VARCHAR)), '(^|[^A-Za-z0-9])web\*([^A-Za-z0-9]|$)')) AND env = 'prod'))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[host:String] rows=2
    - ('web01')
    - ('web03')
- DuckDB: cols=[host:String] rows=0

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

### `agent-parse-search-0043` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=0

**KQL**
```kql
datatable(s:string, num:int)[ "match123",123, "nomatch",456 ] | where s startswith "match" and num between (100 .. 200) | search "match*" | project s, num
```
**Generated SQL**
```sql
SELECT s, num FROM (SELECT 'search_arg0' AS "$table", * FROM (SELECT * FROM (VALUES ('match123', 123), ('nomatch', 456)) AS t(s, num) WHERE s ILIKE 'match%' AND num BETWEEN 100 AND 200) WHERE (REGEXP_MATCHES(LOWER(CAST(s AS VARCHAR)), '(^|[^A-Za-z0-9])match\*([^A-Za-z0-9]|$)') OR REGEXP_MATCHES(LOWER(CAST(num AS VARCHAR)), '(^|[^A-Za-z0-9])match\*([^A-Za-z0-9]|$)')))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, num:Int] rows=1
    - ('match123', 123)
- DuckDB: cols=[s:String, num:Int] rows=0

### `agent-parse-search-0012` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=0

**KQL**
```kql
datatable(s:string)[ "100", "200", "abc", "10.5" ] | search "1*" and not("abc") | project s
```
**Generated SQL**
```sql
SELECT s FROM (SELECT 'search_arg0' AS "$table", * FROM (VALUES ('100'), ('200'), ('abc'), ('10.5')) AS t(s) WHERE (REGEXP_MATCHES(LOWER(CAST(s AS VARCHAR)), '(^|[^A-Za-z0-9])1\*([^A-Za-z0-9]|$)') AND NOT ('abc')))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String] rows=2
    - ('100')
    - ('10.5')
- DuckDB: cols=[s:String] rows=0

### `agent-parse-search-0025` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('tab', 'here', 'end') duck=('', '', '')

**KQL**
```kql
datatable(s:string)[ "tab\there\tend" ] | parse s with a:string "\t" b:string "\t" c:string | project a, b, c
```
**Generated SQL**
```sql
SELECT a, b, c FROM (SELECT *, REGEXP_EXTRACT(s, '(.*?)\t(.*?)\t(.*)', 1) AS a, REGEXP_EXTRACT(s, '(.*?)\t(.*?)\t(.*)', 2) AS b, REGEXP_EXTRACT(s, '(.*?)\t(.*?)\t(.*)', 3) AS c FROM (VALUES ('tab\there\tend')) AS t(s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:String, b:String, c:String] rows=1
    - ('tab', 'here', 'end')
- DuckDB: cols=[a:String, b:String, c:String] rows=1
    - ('', '', '')

### `agent-parse-search-0026` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('line1', 'line2', 'line3') duck=('', '', '')

**KQL**
```kql
datatable(s:string)[ "line1\nline2\nline3" ] | parse s with x:string "\n" y:string "\n" z:string | project x, y, z
```
**Generated SQL**
```sql
SELECT x, y, z FROM (SELECT *, REGEXP_EXTRACT(s, '(.*?)\n(.*?)\n(.*)', 1) AS x, REGEXP_EXTRACT(s, '(.*?)\n(.*?)\n(.*)', 2) AS y, REGEXP_EXTRACT(s, '(.*?)\n(.*?)\n(.*)', 3) AS z FROM (VALUES ('line1\nline2\nline3')) AS t(s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:String, y:String, z:String] rows=1
    - ('line1', 'line2', 'line3')
- DuckDB: cols=[x:String, y:String, z:String] rows=1
    - ('', '', '')

### `agent-parse-search-0031` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=1

**KQL**
```kql
datatable(s:string, n:long)[ "café au lait",1, "plain coffee",2 ] | where * has "café" or n between (2 .. 2) | project n
```
**Generated SQL**
```sql
SELECT n FROM (VALUES ('café au lait', CAST(1 AS BIGINT)), ('plain coffee', CAST(2 AS BIGINT))) AS t(s, n) WHERE (regexp_matches(CAST(CAST(s AS VARCHAR) AS VARCHAR), '(?i)\bcafé\b') OR regexp_matches(CAST(CAST(n AS VARCHAR) AS VARCHAR), '(?i)\bcafé\b')) OR n BETWEEN 2 AND 2
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[n:Int] rows=2
    - (1)
    - (2)
- DuckDB: cols=[n:Int] rows=1
    - (2)

### `agent-parse-search-0037` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('quoted value', 'nested ;semicolon') duck=(''quoted value'', ''nested')

**KQL**
```kql
datatable(s:string)[ "key='quoted value';other='nested ;semicolon'" ] | parse-kv s as (key:string, other:string) with (pair_delimiter=';', kv_delimiter='=', quote="'") | project key, other
```
**Generated SQL**
```sql
SELECT "key", other FROM (SELECT *, TRIM(REGEXP_EXTRACT(s, '(^|;)\s*key\s*=\s*([^;]*)', 2)) AS key, TRIM(REGEXP_EXTRACT(s, '(^|;)\s*other\s*=\s*([^;]*)', 2)) AS other FROM (VALUES ('key=''quoted value'';other=''nested ;semicolon')) AS t(s))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[key:String, other:String] rows=1
    - ('quoted value', 'nested ;semicolon')
- DuckDB: cols=[key:String, other:String] rows=1
    - (''quoted value'', ''nested')

## Family: string-ops (66)

### `agent-string-ops-0021` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types 'regexp_extract_all(STRING_LITERAL, INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	regexp_extract_all(VARCHAR, VARCHAR) -> VARCHAR[]
	regexp_extract_all(VARCHAR, VARCHAR, INTEGER) -> VARCHAR[]
	regexp_extract_all(VARCHAR, VARCHAR, INTEGER, VARCHAR) -> VARCHAR[]
	regexp_extract_all(VARCHAR, VARCHAR, VARCHAR[]) -> VARCHAR[]
	regexp_extract_all(VARCHAR, VARCHAR, VARCHAR[], VARCHAR) -> VARCHAR[]


LINE 1: SELECT s, REGEXP_EXTRACT_ALL('@"(\d+)', s) AS "all", REGEXP_EXTRACT_ALL('@"(\d)(\d)', LIST_VALUE(1, 2)) AS all2...
                                                             ^

**KQL**
```kql
datatable(s:string)[ "2020-01-02 03:04","a1b2c3d4","nope" ] | project s, all=extract_all(@"(\d+)",s), all2=extract_all(@"(\d)(\d)",dynamic([1,2]),s)
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT_ALL('@"(\d+)', s) AS "all", REGEXP_EXTRACT_ALL('@"(\d)(\d)', LIST_VALUE(1, 2)) AS all2 FROM (VALUES ('2020-01-02 03:04'), ('a1b2c3d4'), ('nope')) AS t(s)
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
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types 'regexp_extract_all(STRING_LITERAL, INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	regexp_extract_all(VARCHAR, VARCHAR) -> VARCHAR[]
	regexp_extract_all(VARCHAR, VARCHAR, INTEGER) -> VARCHAR[]
	regexp_extract_all(VARCHAR, VARCHAR, INTEGER, VARCHAR) -> VARCHAR[]
	regexp_extract_all(VARCHAR, VARCHAR, VARCHAR[]) -> VARCHAR[]
	regexp_extract_all(VARCHAR, VARCHAR, VARCHAR[], VARCHAR) -> VARCHAR[]


LINE 1: SELECT s, REGEXP_EXTRACT_ALL('@"(\d+)', s) AS "all", REGEXP_EXTRACT_ALL('@"(\d)(\d)', LIST_VALUE(1, 2)) AS all2...
                                                             ^

### `agent-string-ops-0004` — MismatchRows (high)

*Detail:* row count: kusto=3 duck=2

**KQL**
```kql
datatable(s:string)[ "alpha-beta","alpha_beta","alpha beta","alphabeta" ] | where s has "alpha" | project s
```
**Generated SQL**
```sql
SELECT s FROM (VALUES ('alpha-beta'), ('alpha_beta'), ('alpha beta'), ('alphabeta')) AS t(s) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)\balpha\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String] rows=3
    - ('alpha-beta')
    - ('alpha_beta')
    - ('alpha beta')
- DuckDB: cols=[s:String] rows=2
    - ('alpha-beta')
    - ('alpha beta')

### `agent-string-ops-0013` — MismatchRows (high)

*Detail:* row count: kusto=3 duck=0

**KQL**
```kql
datatable(s:string)[ "a1b2c3","abc","123","a1","9z9" ] | where s matches regex @"[a-z][0-9]" | project s
```
**Generated SQL**
```sql
SELECT s FROM (VALUES ('a1b2c3'), ('abc'), ('123'), ('a1'), ('9z9')) AS t(s) WHERE REGEXP_MATCHES(s, '@"[a-z][0-9]')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String] rows=3
    - ('a1b2c3')
    - ('a1')
    - ('9z9')
- DuckDB: cols=[s:String] rows=0

### `agent-string-ops-0014` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=0

**KQL**
```kql
datatable(s:string)[ "2020-01-15","not a date","1999-12-31","abc-de-fg" ] | where s matches regex @"^\d{4}-\d{2}-\d{2}$" | project s
```
**Generated SQL**
```sql
SELECT s FROM (VALUES ('2020-01-15'), ('not a date'), ('1999-12-31'), ('abc-de-fg')) AS t(s) WHERE REGEXP_MATCHES(s, '@"^\d{4}-\d{2}-\d{2}$')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String] rows=2
    - ('2020-01-15')
    - ('1999-12-31')
- DuckDB: cols=[s:String] rows=0

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

### `agent-string-ops-0016` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('tab	here', 8, -1) duck=('tab\there', 9, -1)

**KQL**
```kql
datatable(s:string)[ "tab\there","newline","quote\"here","back\\slash" ] | project s, len=strlen(s), idx=indexof(s,"\\")
```
**Generated SQL**
```sql
SELECT s, LENGTH(CAST(s AS VARCHAR)) AS len, (INSTR(CAST(s AS VARCHAR), '\\') - 1) AS idx FROM (VALUES ('tab\there'), ('newline'), ('quote\"here'), ('back\\slash')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, len:Int, idx:Int] rows=4
    - ('tab	here', 8, -1)
    - ('newline', 7, -1)
    - ('quote"here', 10, -1)
    - ('back\slash', 10, 4)
- DuckDB: cols=[s:String, len:Int, idx:Int] rows=4
    - ('tab\there', 9, -1)
    - ('newline', 7, -1)
    - ('quote\"here', 11, -1)
    - ('back\\slash', 11, 4)

### `agent-string-ops-0017` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('hello', 'ell', 'lo', 'llo') duck=('hello', 'ell', 'o', 'llo')

**KQL**
```kql
datatable(s:string)[ "hello","world","kusto" ] | project orig=s, sub1=substring(s,1,3), subneg=substring(s,-2,2), suboor=substring(s,2,100)
```
**Generated SQL**
```sql
SELECT s AS orig, SUBSTR(CAST(s AS VARCHAR), (1) + 1, 3) AS sub1, SUBSTR(CAST(s AS VARCHAR), ((-2)) + 1, 2) AS subneg, SUBSTR(CAST(s AS VARCHAR), (2) + 1, 100) AS suboor FROM (VALUES ('hello'), ('world'), ('kusto')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[orig:String, sub1:String, subneg:String, suboor:String] rows=3
    - ('hello', 'ell', 'lo', 'llo')
    - ('world', 'orl', 'ld', 'rld')
    - ('kusto', 'ust', 'to', 'sto')
- DuckDB: cols=[orig:String, sub1:String, subneg:String, suboor:String] rows=3
    - ('hello', 'ell', 'o', 'llo')
    - ('world', 'orl', 'd', 'rld')
    - ('kusto', 'ust', 'o', 'sto')

### `agent-string-ops-0018` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('hello', '', '', '') duck=('hello', 'h', '', '')

**KQL**
```kql
datatable(s:string)[ "hello","abc","" ] | project s, sub_neglen=substring(s,1,-1), sub_zero=substring(s,0,0), sub_big=substring(s,10,5)
```
**Generated SQL**
```sql
SELECT s, SUBSTR(CAST(s AS VARCHAR), (1) + 1, (-1)) AS sub_neglen, SUBSTR(CAST(s AS VARCHAR), (0) + 1, 0) AS sub_zero, SUBSTR(CAST(s AS VARCHAR), (10) + 1, 5) AS sub_big FROM (VALUES ('hello'), ('abc'), ('')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, sub_neglen:String, sub_zero:String, sub_big:String] rows=3
    - ('hello', '', '', '')
    - ('abc', '', '', '')
    - ('', '', '', '')
- DuckDB: cols=[s:String, sub_neglen:String, sub_zero:String, sub_big:String] rows=3
    - ('hello', 'h', '', '')
    - ('abc', 'a', '', '')
    - ('', '', '', '')

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

### `agent-string-ops-0023` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('xxabcxx', 'xabcx', 'xabcxx') duck=('xxabcxx', 'abc', 'abcxx')

**KQL**
```kql
datatable(s:string)[ "xxabcxx","abc","xxx","" ] | project s, t=trim("x",s), tl=trim_start("x",s)
```
**Generated SQL**
```sql
SELECT s, TRIM(s, 'x') AS t, LTRIM(s, 'x') AS tl FROM (VALUES ('xxabcxx'), ('abc'), ('xxx'), ('')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, t:String, tl:String] rows=4
    - ('xxabcxx', 'xabcx', 'xabcxx')
    - ('abc', 'abc', 'abc')
    - ('xxx', 'x', 'xx')
    - ('', '', '')
- DuckDB: cols=[s:String, t:String, tl:String] rows=4
    - ('xxabcxx', 'abc', 'abcxx')
    - ('abc', 'abc', 'abc')
    - ('xxx', '', '')
    - ('', '', '')

### `agent-string-ops-0024` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[c:Int|Real], TYPE_MISMATCH[cregex:Int|Real]  
*Detail:* first differing row[0]: kusto=('hello world hello', 2, 3) duck=('hello world hello', 2, 0)

**KQL**
```kql
datatable(s:string)[ "hello world hello","hello","world" ] | project s, c=countof(s,"hello"), cregex=countof(s,"l+","regex")
```
**Generated SQL**
```sql
SELECT s, (LENGTH(s) - LENGTH(REPLACE(s, 'hello', ''))) / LENGTH('hello') AS c, (LENGTH(s) - LENGTH(REPLACE(s, 'l+', ''))) / LENGTH('l+') AS cregex FROM (VALUES ('hello world hello'), ('hello'), ('world')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, c:Int, cregex:Int] rows=3
    - ('hello world hello', 2, 3)
    - ('hello', 1, 1)
    - ('world', 0, 1)
- DuckDB: cols=[s:String, c:Real, cregex:Real] rows=3
    - ('hello world hello', 2, 0)
    - ('hello', 1, 0)
    - ('world', 0, 0)

### `agent-string-ops-0025` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('abcabcabc', 0, 3, -1) duck=('abcabcabc', 0, 0, -1)

**KQL**
```kql
datatable(s:string)[ "abcabcabc","aaa","xyz" ] | project s, i_first=indexof(s,"a"), i_from=indexof(s,"a",2), i_none=indexof(s,"z")
```
**Generated SQL**
```sql
SELECT s, (INSTR(CAST(s AS VARCHAR), 'a') - 1) AS i_first, (INSTR(CAST(s AS VARCHAR), 'a') - 1) AS i_from, (INSTR(CAST(s AS VARCHAR), 'z') - 1) AS i_none FROM (VALUES ('abcabcabc'), ('aaa'), ('xyz')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, i_first:Int, i_from:Int, i_none:Int] rows=3
    - ('abcabcabc', 0, 3, -1)
    - ('aaa', 0, 2, -1)
    - ('xyz', -1, -1, 2)
- DuckDB: cols=[s:String, i_first:Int, i_from:Int, i_none:Int] rows=3
    - ('abcabcabc', 0, 0, -1)
    - ('aaa', 0, 0, -1)
    - ('xyz', -1, -1, 2)

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

*Detail:* first differing row[0]: kusto=('one two three', 'one_two_three', '[one] [two] [three]') duck=('one two three', 'one_two_three', 'one two three')

**KQL**
```kql
datatable(s:string)[ "one two three","a-b-c","x" ] | project s, repl=replace_string(s," ","_"), reg=replace_regex(s,@"(\w+)",@"[\1]")
```
**Generated SQL**
```sql
SELECT s, REPLACE(s, ' ', '_') AS repl, REGEXP_REPLACE(s, '@"(\w+)', '@"[\1]') AS reg FROM (VALUES ('one two three'), ('a-b-c'), ('x')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, repl:String, reg:String] rows=3
    - ('one two three', 'one_two_three', '[one] [two] [three]')
    - ('a-b-c', 'a-b-c', '[a]-[b]-[c]')
    - ('x', 'x', '[x]')
- DuckDB: cols=[s:String, repl:String, reg:String] rows=3
    - ('one two three', 'one_two_three', 'one two three')
    - ('a-b-c', 'a-b-c', 'a-b-c')
    - ('x', 'x', 'x')

### `agent-string-ops-0031` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('2020-12-31T10:20:30', '12', '2020') duck=('2020-12-31T10:20:30', '', '')

**KQL**
```kql
datatable(s:string)[ "2020-12-31T10:20:30","no match here","42-and-7" ] | project s, ext=extract(@"(\d+)-(\d+)",2,s), ext0=extract(@"(\d+)",0,s)
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT(s, '@"(\d+)-(\d+)', 2) AS ext, REGEXP_EXTRACT(s, '@"(\d+)', 0) AS ext0 FROM (VALUES ('2020-12-31T10:20:30'), ('no match here'), ('42-and-7')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, ext:String, ext0:String] rows=3
    - ('2020-12-31T10:20:30', '12', '2020')
    - ('no match here', '', '')
    - ('42-and-7', '', '42')
- DuckDB: cols=[s:String, ext:String, ext0:String] rows=3
    - ('2020-12-31T10:20:30', '', '')
    - ('no match here', '', '')
    - ('42-and-7', '', '')

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
]) duck=('a1 b2 c3', [])

**KQL**
```kql
datatable(s:string)[ "a1 b2 c3","no digits","x9" ] | project s, all=extract_all(@"([a-z])(\d)",s)
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT_ALL('@"([a-z])(\d)', s) AS "all" FROM (VALUES ('a1 b2 c3'), ('no digits'), ('x9')) AS t(s)
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
    - ('a1 b2 c3', [])
    - ('no digits', [])
    - ('x9', [])

### `agent-string-ops-0043` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('hello
world', [
  "hello",
  "world"
], 5) duck=('hello\nworld', ["hello","world"], 5)

**KQL**
```kql
datatable(s:string)[ "hello\nworld","line1\r\nline2","tab\tsep" ] | project s, lines=split(s,"\n"), nl=indexof(s,"\n")
```
**Generated SQL**
```sql
SELECT s, STRING_SPLIT(CAST(s AS VARCHAR), '\n') AS lines, (INSTR(CAST(s AS VARCHAR), '\n') - 1) AS nl FROM (VALUES ('hello\nworld'), ('line1\r\nline2'), ('tab\tsep')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, lines:Dynamic, nl:Int] rows=3
    - ('hello
world', [
  "hello",
  "world"
], 5)
    - ('line1
line2', [
  "line1\r",
  "line2"
], 6)
    - ('tab	sep', [
  "tab\tsep"
], -1)
- DuckDB: cols=[s:String, lines:Unknown, nl:Int] rows=3
    - ('hello\nworld', ["hello","world"], 5)
    - ('line1\r\nline2', ["line1\\r","line2"], 7)
    - ('tab\tsep', ["tab\\tsep"], -1)

### `agent-string-ops-0044` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=0

**KQL**
```kql
datatable(s:string)[ "ПриветМир","Привет Мир","МИР привет" ] | where s has "привет" | project s, up=toupper(s)
```
**Generated SQL**
```sql
SELECT s, UPPER(s) AS up FROM (VALUES ('ПриветМир'), ('Привет Мир'), ('МИР привет')) AS t(s) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)\bпривет\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, up:String] rows=2
    - ('Привет Мир', 'ПРИВЕТ МИР')
    - ('МИР привет', 'МИР ПРИВЕТ')
- DuckDB: cols=[s:String, up:String] rows=0

### `agent-string-ops-0006` — MismatchRows (high)

*Detail:* row count: kusto=3 duck=0

**KQL**
```kql
datatable(s:string)[ "café résumé","cafe resume","CAFÉ RÉSUMÉ","Café Résumé" ] | where s has "café" | project s, eq = s =~ "café résumé"
```
**Generated SQL**
```sql
SELECT s, UPPER(s) = UPPER('café résumé') AS eq FROM (VALUES ('café résumé'), ('cafe resume'), ('CAFÉ RÉSUMÉ'), ('Café Résumé')) AS t(s) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)\bcafé\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, eq:Bool] rows=3
    - ('café résumé', True)
    - ('CAFÉ RÉSUMÉ', True)
    - ('Café Résumé', True)
- DuckDB: cols=[s:String, eq:Bool] rows=0

### `agent-string-ops-0012` — MismatchRows (high)

*Detail:* row count: kusto=3 duck=1

**KQL**
```kql
datatable(s:string)[ "Hello\tWorld","Hello World","Hello\nWorld" ] | where s has "Hello" and s has "World" | project s, sp = s contains " "
```
**Generated SQL**
```sql
SELECT s, s ILIKE '% %' AS sp FROM (VALUES ('Hello\tWorld'), ('Hello World'), ('Hello\nWorld')) AS t(s) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)\bHello\b') AND regexp_matches(CAST(s AS VARCHAR), '(?i)\bWorld\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, sp:Bool] rows=3
    - ('Hello	World', False)
    - ('Hello World', True)
    - ('Hello
World', False)
- DuckDB: cols=[s:String, sp:Bool] rows=1
    - ('Hello World', True)

### `agent-string-ops-0013` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[cnt:Int|Real]  
*Detail:* first differing row[0]: kusto=('key:"val"', 'val', 1) duck=('key:\"val\', '', 1)

**KQL**
```kql
datatable(s:string)[ "key:\"val\"","key:'val'","key:val" ] | project s, ext=extract(@"key:.?(\w+)",1,s), cnt=countof(s,"val")
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT(s, '@"key:.?(\w+)', 1) AS ext, (LENGTH(s) - LENGTH(REPLACE(s, 'val', ''))) / LENGTH('val') AS cnt FROM (VALUES ('key:\"val\'), ('key:''val'), ('key:val')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, ext:String, cnt:Int] rows=3
    - ('key:"val"', 'val', 1)
    - ('key:'val'', 'val', 1)
    - ('key:val', 'al', 1)
- DuckDB: cols=[s:String, ext:String, cnt:Real] rows=3
    - ('key:\"val\', '', 1)
    - ('key:'val', '', 1)
    - ('key:val', '', 1)

### `agent-string-ops-0014` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[c:Int|Real]  
*Detail:* first differing row[0]: kusto=('a''b', 2, 1, 2) duck=('a''b', NaN, 0, 0)

**KQL**
```kql
datatable(s:string)[ "a''b","a'b'c","'start","end'","''" ] | project s, c=countof(s,"'"), i=indexof(s,"'"), i2=indexof(s,"'",2)
```
**Generated SQL**
```sql
SELECT s, (LENGTH(s) - LENGTH(REPLACE(s, '', ''))) / LENGTH('') AS c, (INSTR(CAST(s AS VARCHAR), '') - 1) AS i, (INSTR(CAST(s AS VARCHAR), '') - 1) AS i2 FROM (VALUES ('a''''b'), ('a''b''c'), ('start'), ('end'), ('')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, c:Int, i:Int, i2:Int] rows=5
    - ('a''b', 2, 1, 2)
    - ('a'b'c', 2, 1, 3)
    - (''start', 1, 0, -1)
    - ('end'', 1, 3, 3)
    - ('''', 2, 0, -1)
- DuckDB: cols=[s:String, c:Real, i:Int, i2:Int] rows=5
    - ('a''b', NaN, 0, 0)
    - ('a'b'c', NaN, 0, 0)
    - ('start', NaN, 0, 0)
    - ('end', NaN, 0, 0)
    - ('', NaN, 0, 0)

### `agent-string-ops-0015` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[cnt:Int|Real]  
*Detail:* first differing row[0]: kusto=('back\slash', 10, 4, 0) duck=('back\\slash', 11, 4, 0)

**KQL**
```kql
datatable(s:string)[ "back\\slash","double\\\\slash","none" ] | project s, len=strlen(s), idx=indexof(s,"\\"), cnt=countof(s,@"\\")
```
**Generated SQL**
```sql
SELECT s, LENGTH(CAST(s AS VARCHAR)) AS len, (INSTR(CAST(s AS VARCHAR), '\\') - 1) AS idx, (LENGTH(s) - LENGTH(REPLACE(s, '@"\\', ''))) / LENGTH('@"\\') AS cnt FROM (VALUES ('back\\slash'), ('double\\\\slash'), ('none')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, len:Int, idx:Int, cnt:Int] rows=3
    - ('back\slash', 10, 4, 0)
    - ('double\\slash', 13, 6, 1)
    - ('none', 4, -1, 0)
- DuckDB: cols=[s:String, len:Int, idx:Int, cnt:Real] rows=3
    - ('back\\slash', 11, 4, 0)
    - ('double\\\\slash', 15, 6, 0)
    - ('none', 4, -1, 0)

### `agent-string-ops-0016` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('line1
line2
line3', [
  "line1",
  "line2",
  "line3"
], 3) duck=('line1\r\nline2\r\nline3', ["line1","line2","line3"], 3)

**KQL**
```kql
datatable(s:string)[ "line1\r\nline2\r\nline3","one\ntwo","solo" ] | project s, parts=split(s,"\r\n"), n=array_length(split(s,"\r\n"))
```
**Generated SQL**
```sql
SELECT s, STRING_SPLIT(CAST(s AS VARCHAR), '\r\n') AS parts, LEN(STRING_SPLIT(CAST(s AS VARCHAR), '\r\n')) AS n FROM (VALUES ('line1\r\nline2\r\nline3'), ('one\ntwo'), ('solo')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, parts:Dynamic, n:Int] rows=3
    - ('line1
line2
line3', [
  "line1",
  "line2",
  "line3"
], 3)
    - ('one
two', [
  "one\ntwo"
], 1)
    - ('solo', [
  "solo"
], 1)
- DuckDB: cols=[s:String, parts:Unknown, n:Int] rows=3
    - ('line1\r\nline2\r\nline3', ["line1","line2","line3"], 3)
    - ('one\ntwo', ["one\\ntwo"], 1)
    - ('solo', ["solo"], 1)

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

*Detail:* first differing row[0]: kusto=('one two three', 'one-two-three', 'noe wto htere') duck=('one two three', 'one two three', 'one two three')

**KQL**
```kql
datatable(s:string)[ "one two three","wordword","   " ] | project s, rr=replace_regex(s,@"\s+","-"), rr2=replace_regex(s,@"(\w)(\w)",@"\2\1")
```
**Generated SQL**
```sql
SELECT s, REGEXP_REPLACE(s, '@"\s+', '-') AS rr, REGEXP_REPLACE(s, '@"(\w)(\w)', '@"\2\1') AS rr2 FROM (VALUES ('one two three'), ('wordword'), ('   ')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, rr:String, rr2:String] rows=3
    - ('one two three', 'one-two-three', 'noe wto htere')
    - ('wordword', 'wordword', 'owdrowdr')
    - ('   ', '-', '   ')
- DuckDB: cols=[s:String, rr:String, rr2:String] rows=3
    - ('one two three', 'one two three', 'one two three')
    - ('wordword', 'wordword', 'wordword')
    - ('   ', '   ', '   ')

### `agent-string-ops-0023` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('hello', 'll', '', '') duck=('hello', 'lo', '', '')

**KQL**
```kql
datatable(s:string)[ "hello","HELLO","Hello" ] | project s, sub_neg=substring(s,-3,2), sub_negstart=substring(s,-100,3), sub_zerolen=substring(s,2,0)
```
**Generated SQL**
```sql
SELECT s, SUBSTR(CAST(s AS VARCHAR), ((-3)) + 1, 2) AS sub_neg, SUBSTR(CAST(s AS VARCHAR), ((-100)) + 1, 3) AS sub_negstart, SUBSTR(CAST(s AS VARCHAR), (2) + 1, 0) AS sub_zerolen FROM (VALUES ('hello'), ('HELLO'), ('Hello')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, sub_neg:String, sub_negstart:String, sub_zerolen:String] rows=3
    - ('hello', 'll', '', '')
    - ('HELLO', 'LL', '', '')
    - ('Hello', 'll', '', '')
- DuckDB: cols=[s:String, sub_neg:String, sub_negstart:String, sub_zerolen:String] rows=3
    - ('hello', 'lo', '', '')
    - ('HELLO', 'LO', '', '')
    - ('Hello', 'lo', '', '')

### `agent-string-ops-0024` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('abcdef', 'def', 'ef', '') duck=('abcdef', 'def', 'f', '')

**KQL**
```kql
datatable(s:string)[ "abcdef","xy","" ] | project s, sm1=substring(s,3), sm2=substring(s,-2), sm3=substring(s,100)
```
**Generated SQL**
```sql
SELECT s, SUBSTR(CAST(s AS VARCHAR), (3) + 1) AS sm1, SUBSTR(CAST(s AS VARCHAR), ((-2)) + 1) AS sm2, SUBSTR(CAST(s AS VARCHAR), (100) + 1) AS sm3 FROM (VALUES ('abcdef'), ('xy'), ('')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, sm1:String, sm2:String, sm3:String] rows=3
    - ('abcdef', 'def', 'ef', '')
    - ('xy', '', 'xy', '')
    - ('', '', '', '')
- DuckDB: cols=[s:String, sm1:String, sm2:String, sm3:String] rows=3
    - ('abcdef', 'def', 'f', '')
    - ('xy', '', 'y', '')
    - ('', '', '', '')

### `agent-string-ops-0026` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('  pad  ', ' pad ', ' pad ', '  pad  ') duck=('  pad  ', '  pad  ', 'pad', '  pad  ')

**KQL**
```kql
datatable(s:string)[ "  pad  ","\t\tx\t\t","yyy","" ] | project s, t=trim(@"\s",s), ts=trim(" ",s), tcustom=trim("y",s)
```
**Generated SQL**
```sql
SELECT s, TRIM(s, '@"\s') AS t, TRIM(s, ' ') AS ts, TRIM(s, 'y') AS tcustom FROM (VALUES ('  pad  '), ('\t\tx\t\t'), ('yyy'), ('')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, t:String, ts:String, tcustom:String] rows=4
    - ('  pad  ', ' pad ', ' pad ', '  pad  ')
    - ('		x		', '	x	', '		x		', '		x		')
    - ('yyy', 'yyy', 'yyy', 'y')
    - ('', '', '', '')
- DuckDB: cols=[s:String, t:String, ts:String, tcustom:String] rows=4
    - ('  pad  ', '  pad  ', 'pad', '  pad  ')
    - ('\t\tx\t\t', 't\tx\t\t', '\t\tx\t\t', '\t\tx\t\t')
    - ('yyy', 'yyy', 'yyy', '')
    - ('', '', '', '')

### `agent-string-ops-0027` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('xxhelloxx', 'xhelloxx', 'xxhelloxx', 'xhellox') duck=('xxhelloxx', 'helloxx', 'xxhelloxx', 'hello')

**KQL**
```kql
datatable(s:string)[ "xxhelloxx","__world__","midxxmid" ] | project s, ts=trim_start("x",s), te=trim_end("_",s), full=trim(@"[x_]",s)
```
**Generated SQL**
```sql
SELECT s, LTRIM(s, 'x') AS ts, RTRIM(s, '_') AS te, TRIM(s, '@"[x_]') AS "full" FROM (VALUES ('xxhelloxx'), ('__world__'), ('midxxmid')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, ts:String, te:String, full:String] rows=3
    - ('xxhelloxx', 'xhelloxx', 'xxhelloxx', 'xhellox')
    - ('__world__', '__world__', '__world_', '_world_')
    - ('midxxmid', 'midxxmid', 'midxxmid', 'midxxmid')
- DuckDB: cols=[s:String, ts:String, te:String, full:String] rows=3
    - ('xxhelloxx', 'helloxx', 'xxhelloxx', 'hello')
    - ('__world__', '__world__', '__world', 'world')
    - ('midxxmid', 'midxxmid', 'midxxmid', 'midxxmid')

### `agent-string-ops-0034` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('100%done', '100') duck=('100%done', '')

**KQL**
```kql
datatable(s:string)[ "100%done","50% off","%start","end%" ] | where s contains "%" | project s, e=extract(@"(\d+)%",1,s)
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT(s, '@"(\d+)%', 1) AS e FROM (VALUES ('100%done'), ('50% off'), ('%start'), ('end%')) AS t(s) WHERE s ILIKE '%%%'
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, e:String] rows=4
    - ('100%done', '100')
    - ('50% off', '50')
    - ('%start', '')
    - ('end%', '')
- DuckDB: cols=[s:String, e:String] rows=4
    - ('100%done', '')
    - ('50% off', '')
    - ('%start', '')
    - ('end%', '')

### `agent-string-ops-0035` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=4

**KQL**
```kql
datatable(s:string)[ "a_b","a%b","a\\b","a[b]" ] | where s contains "%" or s contains "_" | project s
```
**Generated SQL**
```sql
SELECT s FROM (VALUES ('a_b'), ('a%b'), ('a\\b'), ('a[b]')) AS t(s) WHERE s ILIKE '%%%' OR s ILIKE '%_%'
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String] rows=2
    - ('a_b')
    - ('a%b')
- DuckDB: cols=[s:String] rows=4
    - ('a_b')
    - ('a%b')
    - ('a\\b')
    - ('a[b]')

### `agent-string-ops-0042` — MismatchRows (high)

*Detail:* row count: kusto=3 duck=0

**KQL**
```kql
datatable(s:string)[ "Привет мир","ПРИВЕТ","привет","МИР" ] | where s has "мир" or s has_cs "ПРИВЕТ" | project s, up=toupper(s), lo=tolower(s)
```
**Generated SQL**
```sql
SELECT s, UPPER(s) AS up, LOWER(s) AS lo FROM (VALUES ('Привет мир'), ('ПРИВЕТ'), ('привет'), ('МИР')) AS t(s) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)\bмир\b') OR regexp_matches(CAST(s AS VARCHAR), '\bПРИВЕТ\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, up:String, lo:String] rows=3
    - ('Привет мир', 'ПРИВЕТ МИР', 'привет мир')
    - ('ПРИВЕТ', 'ПРИВЕТ', 'привет')
    - ('МИР', 'МИР', 'мир')
- DuckDB: cols=[s:String, up:String, lo:String] rows=0

### `agent-string-ops-0002` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=0

**KQL**
```kql
datatable(s:string)[ "version=1.2.3","v1.2.3","1.2.3-rc","1_2_3" ] | where s has "1" and s has "3" and s !has "2.3" | project s
```
**Generated SQL**
```sql
SELECT s FROM (VALUES ('version=1.2.3'), ('v1.2.3'), ('1.2.3-rc'), ('1_2_3')) AS t(s) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)\b1\b') AND regexp_matches(CAST(s AS VARCHAR), '(?i)\b3\b') AND NOT regexp_matches(CAST(s AS VARCHAR), '(?i)\b2\.3\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String] rows=1
    - ('1_2_3')
- DuckDB: cols=[s:String] rows=0

### `agent-string-ops-0003` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a=b&c=d', 'a=b&c=d', 'a=b&c=d', '') duck=('a=b&c=d', 'a=b&c=d', 'a=b&c=d', null)

**KQL**
```kql
datatable(s:string)[ "key:value;k2:v2","a=b&c=d","x;y;z" ] | mv-expand kv=split(s,";") to typeof(string) | extend pair=split(tostring(kv),":") | project s, kv=tostring(kv), k=tostring(pair[0]), v=tostring(pair[1]) | order by s asc, kv asc
```
**Generated SQL**
```sql
SELECT s, TRY_CAST(kv AS TEXT) AS kv, TRY_CAST(pair[0 + 1] AS TEXT) AS k, TRY_CAST(pair[1 + 1] AS TEXT) AS v FROM (SELECT *, STRING_SPLIT(CAST(TRY_CAST(kv AS TEXT) AS VARCHAR), ':') AS pair FROM (SELECT t.*, u.value AS kv FROM (SELECT * FROM (VALUES ('key:value;k2:v2'), ('a=b&c=d'), ('x;y;z')) AS t(s)) AS t CROSS JOIN UNNEST(STRING_SPLIT(CAST(s AS VARCHAR), ';')) AS u(value))) ORDER BY s ASC NULLS FIRST, kv ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, kv:String, k:String, v:String] rows=6
    - ('a=b&c=d', 'a=b&c=d', 'a=b&c=d', '')
    - ('key:value;k2:v2', 'k2:v2', 'k2', 'v2')
    - ('key:value;k2:v2', 'key:value', 'key', 'value')
    - ('x;y;z', 'x', 'x', '')
    - ('x;y;z', 'y', 'y', '')
    - ('x;y;z', 'z', 'z', '')
- DuckDB: cols=[s:String, kv:String, k:String, v:String] rows=6
    - ('a=b&c=d', 'a=b&c=d', 'a=b&c=d', null)
    - ('key:value;k2:v2', 'k2:v2', 'k2', 'v2')
    - ('key:value;k2:v2', 'key:value', 'key', 'value')
    - ('x;y;z', 'x', 'x', null)
    - ('x;y;z', 'y', 'y', null)
    - ('x;y;z', 'z', 'z', null)

### `agent-string-ops-0007` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('  spaced  ', ' spaced ', 8, True) duck=('  spaced  ', '  spaced  ', 10, False)

**KQL**
```kql
datatable(s:string)[ "  spaced  "," nbsp ","tab\ttab","plain" ] | project s, t=trim(@"\s",s), tlen=strlen(trim(@"\s",s)), hasws=(s matches regex @"\s")
```
**Generated SQL**
```sql
SELECT s, TRIM(s, '@"\s') AS t, LENGTH(CAST(TRIM(s, '@"\s') AS VARCHAR)) AS tlen, (REGEXP_MATCHES(s, '@"\s')) AS hasws FROM (VALUES ('  spaced  '), (' nbsp '), ('tab\ttab'), ('plain')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, t:String, tlen:Int, hasws:Bool] rows=4
    - ('  spaced  ', ' spaced ', 8, True)
    - (' nbsp ', 'nbsp', 4, True)
    - ('tab	tab', 'tab	tab', 7, True)
    - ('plain', 'plain', 5, False)
- DuckDB: cols=[s:String, t:String, tlen:Int, hasws:Bool] rows=4
    - ('  spaced  ', '  spaced  ', 10, False)
    - (' nbsp ', ' nbsp ', 6, False)
    - ('tab\ttab', 'tab\ttab', 8, False)
    - ('plain', 'plain', 5, False)

### `agent-string-ops-0008` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[cnt:Int|Real]  
*Detail:* first differing row[0]: kusto=('abcABCabc', '###', 'abc_ABCabc', 6) duck=('abcABCabc', 'abcABCabc', 'abcABCabc', 0)

**KQL**
```kql
datatable(s:string)[ "abcABCabc","XYZxyz","123" ] | project s, ri=replace_regex(s,@"(?i)abc","#"), rg=replace_regex(s,@"([a-z])([A-Z])",@"\1_\2"), cnt=countof(s,@"[a-z]","regex")
```
**Generated SQL**
```sql
SELECT s, REGEXP_REPLACE(s, '@"(?i)abc', '#') AS ri, REGEXP_REPLACE(s, '@"([a-z])([A-Z])', '@"\1_\2') AS rg, (LENGTH(s) - LENGTH(REPLACE(s, '@"[a-z]', ''))) / LENGTH('@"[a-z]') AS cnt FROM (VALUES ('abcABCabc'), ('XYZxyz'), ('123')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, ri:String, rg:String, cnt:Int] rows=3
    - ('abcABCabc', '###', 'abc_ABCabc', 6)
    - ('XYZxyz', 'XYZxyz', 'XYZxyz', 3)
    - ('123', '123', '123', 0)
- DuckDB: cols=[s:String, ri:String, rg:String, cnt:Real] rows=3
    - ('abcABCabc', 'abcABCabc', 'abcABCabc', 0)
    - ('XYZxyz', 'XYZxyz', 'XYZxyz', 0)
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
], 3) duck=('a1b2c3', [], 0)

**KQL**
```kql
datatable(s:string)[ "a1b2c3","aXbYcZ","abc" ] | project s, ea=extract_all(@"([a-z])(\d)?",s), n=array_length(extract_all(@"([a-z])(\d)?",s))
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT_ALL('@"([a-z])(\d)?', s) AS ea, LEN(REGEXP_EXTRACT_ALL('@"([a-z])(\d)?', s)) AS n FROM (VALUES ('a1b2c3'), ('aXbYcZ'), ('abc')) AS t(s)
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
    - ('a1b2c3', [], 0)
    - ('aXbYcZ', [], 0)
    - ('abc', [], 0)

### `agent-string-ops-0010` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[p0:Dynamic|String], TYPE_MISMATCH[pneg:Dynamic|String]  
*Detail:* first differing row[0]: kusto=('first.second.third', "first", "third", 3, 'first/second') duck=('first.second.third', 'first', null, 3, 'first/second')

**KQL**
```kql
datatable(s:string)[ "first.second.third","onlyone","..","a..b" ] | project s, p0=split(s,".")[0], pneg=split(s,".")[-1], cnt=array_length(split(s,".")), joined=strcat_delim("/",tostring(split(s,".")[0]),tostring(split(s,".")[1]))
```
**Generated SQL**
```sql
SELECT s, STRING_SPLIT(CAST(s AS VARCHAR), '.')[0 + 1] AS p0, STRING_SPLIT(CAST(s AS VARCHAR), '.')[(-1) + 1] AS pneg, LEN(STRING_SPLIT(CAST(s AS VARCHAR), '.')) AS cnt, CONCAT_WS('/', TRY_CAST(STRING_SPLIT(CAST(s AS VARCHAR), '.')[0 + 1] AS TEXT), TRY_CAST(STRING_SPLIT(CAST(s AS VARCHAR), '.')[1 + 1] AS TEXT)) AS joined FROM (VALUES ('first.second.third'), ('onlyone'), ('..'), ('a..b')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, p0:Dynamic, pneg:Dynamic, cnt:Int, joined:String] rows=4
    - ('first.second.third', "first", "third", 3, 'first/second')
    - ('onlyone', "onlyone", "onlyone", 1, 'onlyone/')
    - ('..', "", "", 3, '/')
    - ('a..b', "a", "b", 3, 'a/')
- DuckDB: cols=[s:String, p0:String, pneg:String, cnt:Int, joined:String] rows=4
    - ('first.second.third', 'first', null, 3, 'first/second')
    - ('onlyone', 'onlyone', null, 1, 'onlyone')
    - ('..', '', null, 3, '/')
    - ('a..b', 'a', null, 3, 'a/')

### `agent-string-ops-0011` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[q:Int|Real]  
*Detail:* first differing row[0]: kusto=('He said "hi"', 12, 2, 8, -1) duck=('He said \"hi\', 13, 2, 8, -1)

**KQL**
```kql
datatable(s:string)[ "He said \"hi\"","tab\there","mix\"\t\"end" ] | project s, l=strlen(s), q=countof(s,"\""), idxq=indexof(s,"\""), idxt=indexof(s,"\t")
```
**Generated SQL**
```sql
SELECT s, LENGTH(CAST(s AS VARCHAR)) AS l, (LENGTH(s) - LENGTH(REPLACE(s, '\', ''))) / LENGTH('\') AS q, (INSTR(CAST(s AS VARCHAR), '\') - 1) AS idxq, (INSTR(CAST(s AS VARCHAR), '\t') - 1) AS idxt FROM (VALUES ('He said \"hi\'), ('tab\there'), ('mix\"\t\"end')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, l:Int, q:Int, idxq:Int, idxt:Int] rows=3
    - ('He said "hi"', 12, 2, 8, -1)
    - ('tab	here', 8, 0, -1, 3)
    - ('mix"	"end', 9, 2, 3, 4)
- DuckDB: cols=[s:String, l:Int, q:Real, idxq:Int, idxt:Int] rows=3
    - ('He said \"hi\', 13, 2, 8, -1)
    - ('tab\there', 9, 1, 3, 3)
    - ('mix\"\t\"end', 12, 3, 3, 5)

### `agent-string-ops-0012` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[c:Int|Real]  
*Detail:* first differing row[0]: kusto=('O'Brien's', 2, 'O''Brien''s', 7) duck=('O'Brien's', NaN, 'O'Brien's', 0)

**KQL**
```kql
datatable(s:string)[ "O'Brien's","it''s","''","a'b'c'd" ] | project s, c=countof(s,"'"), repl=replace_string(s,"'","''"), i3=indexof(s,"'",3)
```
**Generated SQL**
```sql
SELECT s, (LENGTH(s) - LENGTH(REPLACE(s, '', ''))) / LENGTH('') AS c, REPLACE(s, '', '') AS repl, (INSTR(CAST(s AS VARCHAR), '') - 1) AS i3 FROM (VALUES ('O''Brien''s'), ('it''''s'), (''), ('a''b''c''d')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, c:Int, repl:String, i3:Int] rows=4
    - ('O'Brien's', 2, 'O''Brien''s', 7)
    - ('it''s', 2, 'it''''s', 3)
    - ('''', 2, '''''', -1)
    - ('a'b'c'd', 3, 'a''b''c''d', 3)
- DuckDB: cols=[s:String, c:Real, repl:String, i3:Int] rows=4
    - ('O'Brien's', NaN, 'O'Brien's', 0)
    - ('it''s', NaN, 'it''s', 0)
    - ('', NaN, '', 0)
    - ('a'b'c'd', NaN, 'a'b'c'd', 0)

### `agent-string-ops-0015` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('x', 'y', '', 'x') duck=('x', 'x', '', 'x')

**KQL**
```kql
datatable(s:string)[ "x","xx","xxx","xxxx" ] | project s, rep=replace_regex(s,@"x",strcat("y")), trimmed=trim_end("x",s), subneg=substring(s,strlen(s)-2,2)
```
**Generated SQL**
```sql
SELECT s, REGEXP_REPLACE(s, '@"x', CONCAT('y')) AS rep, RTRIM(s, 'x') AS trimmed, SUBSTR(CAST(s AS VARCHAR), (LENGTH(CAST(s AS VARCHAR)) - 2) + 1, 2) AS subneg FROM (VALUES ('x'), ('xx'), ('xxx'), ('xxxx')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, rep:String, trimmed:String, subneg:String] rows=4
    - ('x', 'y', '', 'x')
    - ('xx', 'yy', 'x', 'xx')
    - ('xxx', 'yyy', 'xx', 'xx')
    - ('xxxx', 'yyyy', 'xxx', 'xx')
- DuckDB: cols=[s:String, rep:String, trimmed:String, subneg:String] rows=4
    - ('x', 'x', '', 'x')
    - ('xx', 'xx', '', 'xx')
    - ('xxx', 'xxx', '', 'xx')
    - ('xxxx', 'xxxx', '', 'xx')

### `agent-string-ops-0017` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[cnt:Int|Real]  
*Detail:* first differing row[0]: kusto=('Hello\nWorld', 12, False, True, 1) duck=('Hello\\nWorld', 13, True, True, 1)

**KQL**
```kql
datatable(s:string)[ "Hello\\nWorld","Hello\nWorld","a\\tb","a\tb" ] | project s, l=strlen(s), hasnl=(s contains "\n"), hasbs=(s contains "\\"), cnt=countof(s,"\\")
```
**Generated SQL**
```sql
SELECT s, LENGTH(CAST(s AS VARCHAR)) AS l, (s ILIKE '%\n%') AS hasnl, (s ILIKE '%\\%') AS hasbs, (LENGTH(s) - LENGTH(REPLACE(s, '\\', ''))) / LENGTH('\\') AS cnt FROM (VALUES ('Hello\\nWorld'), ('Hello\nWorld'), ('a\\tb'), ('a\tb')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, l:Int, hasnl:Bool, hasbs:Bool, cnt:Int] rows=4
    - ('Hello\nWorld', 12, False, True, 1)
    - ('Hello
World', 11, True, False, 0)
    - ('a\tb', 4, False, True, 1)
    - ('a	b', 3, False, False, 0)
- DuckDB: cols=[s:String, l:Int, hasnl:Bool, hasbs:Bool, cnt:Real] rows=4
    - ('Hello\\nWorld', 13, True, True, 1)
    - ('Hello\nWorld', 12, True, False, 0)
    - ('a\\tb', 5, False, True, 1)
    - ('a\tb', 4, False, False, 0)

### `agent-string-ops-0020` — MismatchRows (high)

*Detail:* row count: kusto=2 duck=0

**KQL**
```kql
datatable(s:string)[ "192.168.0.1","10.0.0.255","not.an.ip","1.2.3" ] | where s matches regex @"^\d{1,3}(\.\d{1,3}){3}$" | project s, parts=split(s,"."), oct0=toint(split(s,".")[0])
```
**Generated SQL**
```sql
SELECT s, STRING_SPLIT(CAST(s AS VARCHAR), '.') AS parts, TRY_CAST(TRUNC(COALESCE(TRY_CAST(STRING_SPLIT(CAST(s AS VARCHAR), '.')[0 + 1] AS DOUBLE), TRY_CAST(TRY_CAST(STRING_SPLIT(CAST(s AS VARCHAR), '.')[0 + 1] AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS oct0 FROM (VALUES ('192.168.0.1'), ('10.0.0.255'), ('not.an.ip'), ('1.2.3')) AS t(s) WHERE REGEXP_MATCHES(s, '@"^\d{1,3}(\.\d{1,3}){3}$')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, parts:Dynamic, oct0:Int] rows=2
    - ('192.168.0.1', [
  "192",
  "168",
  "0",
  "1"
], 192)
    - ('10.0.0.255', [
  "10",
  "0",
  "0",
  "255"
], 10)
- DuckDB: cols=[s:String, parts:Unknown, oct0:Int] rows=0

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
], 3) duck=('ababab', 'ababab', [], 3)

**KQL**
```kql
datatable(s:string)[ "ababab","aaa","abc","" ] | project s, ra=replace_regex(s,@"(ab)+","X"), ea=extract_all(@"(a)(b)?",s), cnt=countof(s,"ab")
```
**Generated SQL**
```sql
SELECT s, REGEXP_REPLACE(s, '@"(ab)+', 'X') AS ra, REGEXP_EXTRACT_ALL('@"(a)(b)?', s) AS ea, (LENGTH(s) - LENGTH(REPLACE(s, 'ab', ''))) / LENGTH('ab') AS cnt FROM (VALUES ('ababab'), ('aaa'), ('abc'), ('')) AS t(s)
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
    - ('ababab', 'ababab', [], 3)
    - ('aaa', 'aaa', [], 0)
    - ('abc', 'abc', [], 1)
    - ('', '', ["","","","","","","","","",""], 0)

### `agent-string-ops-0024` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('trailing   ', 'trailing  ', 'trailing   ', 10, 'trailing  ') duck=('trailing   ', 'trailing', 'trailing   ', 8, 'trailing')

**KQL**
```kql
datatable(s:string)[ "trailing   ","   leading","  both  ","none" ] | project s, te=trim_end(" ",s), ts=trim_start(" ",s), lente=strlen(trim_end(" ",s)), full=trim(" ",s)
```
**Generated SQL**
```sql
SELECT s, RTRIM(s, ' ') AS te, LTRIM(s, ' ') AS ts, LENGTH(CAST(RTRIM(s, ' ') AS VARCHAR)) AS lente, TRIM(s, ' ') AS "full" FROM (VALUES ('trailing   '), ('   leading'), ('  both  '), ('none')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, te:String, ts:String, lente:Int, full:String] rows=4
    - ('trailing   ', 'trailing  ', 'trailing   ', 10, 'trailing  ')
    - ('   leading', '   leading', '  leading', 10, '  leading')
    - ('  both  ', '  both ', ' both  ', 7, ' both ')
    - ('none', 'none', 'none', 4, 'none')
- DuckDB: cols=[s:String, te:String, ts:String, lente:Int, full:String] rows=4
    - ('trailing   ', 'trailing', 'trailing   ', 8, 'trailing')
    - ('   leading', '   leading', 'leading', 10, 'leading')
    - ('  both  ', '  both', 'both  ', 6, 'both')
    - ('none', 'none', 'none', 4, 'none')

### `agent-string-ops-0026` — MismatchRows (high)

*Detail:* row count: kusto=3 duck=0

**KQL**
```kql
datatable(s:string)[ "ＡＢ１２","AB12","ＡＢ12" ] | where s matches regex @"\d" | project s, dig=extract_all(@"(\d)",s), l=strlen(s)
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT_ALL('@"(\d)', s) AS dig, LENGTH(CAST(s AS VARCHAR)) AS l FROM (VALUES ('ＡＢ１２'), ('AB12'), ('ＡＢ12')) AS t(s) WHERE REGEXP_MATCHES(s, '@"\d')
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
- DuckDB: cols=[s:String, dig:Unknown, l:Int] rows=0

### `agent-string-ops-0027` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[cnt:Int|Real]  
*Detail:* first differing row[0]: kusto=('a\b\c', [
  "a",
  "b",
  "c"
], 3, 2) duck=('a\\b\\c', ["a\\\\b\\\\c"], 1, 0)

**KQL**
```kql
datatable(s:string)[ "a\\b\\c","a\\\\b","\\","noslash" ] | project s, parts=split(s,@"\"), n=array_length(split(s,@"\")), cnt=countof(s,@"\")
```
**Generated SQL**
```sql
SELECT s, STRING_SPLIT(CAST(s AS VARCHAR), '@"\') AS parts, LEN(STRING_SPLIT(CAST(s AS VARCHAR), '@"\')) AS n, (LENGTH(s) - LENGTH(REPLACE(s, '@"\', ''))) / LENGTH('@"\') AS cnt FROM (VALUES ('a\\b\\c'), ('a\\\\b'), ('\\'), ('noslash')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, parts:Dynamic, n:Int, cnt:Int] rows=4
    - ('a\b\c', [
  "a",
  "b",
  "c"
], 3, 2)
    - ('a\\b', [
  "a",
  "",
  "b"
], 3, 2)
    - ('\', [
  "",
  ""
], 2, 1)
    - ('noslash', [
  "noslash"
], 1, 0)
- DuckDB: cols=[s:String, parts:Unknown, n:Int, cnt:Real] rows=4
    - ('a\\b\\c', ["a\\\\b\\\\c"], 1, 0)
    - ('a\\\\b', ["a\\\\\\\\b"], 1, 0)
    - ('\\', ["\\\\"], 1, 0)
    - ('noslash', ["noslash"], 1, 0)

### `agent-string-ops-0029` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('abcdefghij', 'defg', 'hij', 'fghij', 'gh') duck=('abcdefghij', 'defg', 'hij', 'fghij', 'hi')

**KQL**
```kql
datatable(s:string)[ "abcdefghij","short","" ] | project s, mid=substring(s,3,4), tail=substring(s,strlen(s)-3), oor=substring(s,5,100), negpair=substring(s,-4,2)
```
**Generated SQL**
```sql
SELECT s, SUBSTR(CAST(s AS VARCHAR), (3) + 1, 4) AS mid, SUBSTR(CAST(s AS VARCHAR), (LENGTH(CAST(s AS VARCHAR)) - 3) + 1) AS tail, SUBSTR(CAST(s AS VARCHAR), (5) + 1, 100) AS oor, SUBSTR(CAST(s AS VARCHAR), ((-4)) + 1, 2) AS negpair FROM (VALUES ('abcdefghij'), ('short'), ('')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, mid:String, tail:String, oor:String, negpair:String] rows=3
    - ('abcdefghij', 'defg', 'hij', 'fghij', 'gh')
    - ('short', 'rt', 'ort', '', 'ho')
    - ('', '', '', '', '')
- DuckDB: cols=[s:String, mid:String, tail:String, oor:String, negpair:String] rows=3
    - ('abcdefghij', 'defg', 'hij', 'fghij', 'hi')
    - ('short', 'rt', 'ort', '', 'or')
    - ('', '', '', '', '')

### `agent-string-ops-0030` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('x;y,z|w', [
  ";",
  ",",
  "|"
], 3, 'x_y_z_w') duck=('x;y,z|w', [], 0, 'x;y,z|w')

**KQL**
```kql
datatable(s:string)[ "x;y,z|w","a-b","single" ] | project s, sp=extract_all(@"([;,|-])",s), n=array_length(extract_all(@"([;,|-])",s)), repl=replace_regex(s,@"[;,|-]","_")
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT_ALL('@"([;,|-])', s) AS sp, LEN(REGEXP_EXTRACT_ALL('@"([;,|-])', s)) AS n, REGEXP_REPLACE(s, '@"[;,|-]', '_') AS repl FROM (VALUES ('x;y,z|w'), ('a-b'), ('single')) AS t(s)
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
    - ('x;y,z|w', [], 0, 'x;y,z|w')
    - ('a-b', [], 0, 'a-b')
    - ('single', [], 0, 'single')

### `agent-string-ops-0032` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[cnt:Int|Real]  
*Detail:* first differing row[0]: kusto=('<tag>text</tag>', 'tag', [
  "tag",
  "/tag"
], 2) duck=('<tag>text</tag>', '', [], 2)

**KQL**
```kql
datatable(s:string)[ "<tag>text</tag>","<br/>","plain text","<a href=\"x\">" ] | project s, inner=extract(@"<([a-z]+)",1,s), all=extract_all(@"<(/?[a-z]+)",s), cnt=countof(s,"<")
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT(s, '@"<([a-z]+)', 1) AS "inner", REGEXP_EXTRACT_ALL('@"<(/?[a-z]+)', s) AS "all", (LENGTH(s) - LENGTH(REPLACE(s, '<', ''))) / LENGTH('<') AS cnt FROM (VALUES ('<tag>text</tag>'), ('<br/>'), ('plain text'), ('<a href=\"x\">')) AS t(s)
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
    - ('<tag>text</tag>', '', [], 2)
    - ('<br/>', '', [], 1)
    - ('plain text', '', [], 0)
    - ('<a href=\"x\">', '', [], 1)

### `agent-string-ops-0036` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=('a	b', 3, 3, 'ab', False) duck=('a\tb', 4, 4, 'ab', False)

**KQL**
```kql
datatable(s:string)[ "aaa​bbb","a\tb","a b","ab" ] | project s, l=strlen(s), b=string_size(s), nospace=replace_string(replace_string(s," ",""),"\t",""), hasspace=(s has " ")
```
**Generated SQL**
```sql
SELECT s, LENGTH(CAST(s AS VARCHAR)) AS l, OCTET_LENGTH(ENCODE(CAST(s AS VARCHAR))) AS b, REPLACE(REPLACE(s, ' ', ''), '\t', '') AS nospace, (regexp_matches(CAST(s AS VARCHAR), '(?i)\b \b')) AS hasspace FROM (VALUES ('aaa​bbb'), ('a\tb'), ('a b'), ('ab')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, l:Int, b:Int, nospace:String, hasspace:Bool] rows=4
    - ('aaa​bbb', 7, 9, 'aaa​bbb', False)
    - ('a	b', 3, 3, 'ab', False)
    - ('a b', 3, 3, 'ab', True)
    - ('ab', 2, 2, 'ab', False)
- DuckDB: cols=[s:String, l:Int, b:Int, nospace:String, hasspace:Bool] rows=4
    - ('aaa​bbb', 7, 9, 'aaa​bbb', False)
    - ('a\tb', 4, 4, 'ab', False)
    - ('a b', 3, 3, 'ab', True)
    - ('ab', 2, 2, 'ab', False)

### `agent-string-ops-0038` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('{"k":"v"}', True, '"k":"v"', 9) duck=('{\"k\":\"v\"}', True, '', 13)

**KQL**
```kql
datatable(s:string)[ "{\"k\":\"v\"}","[1,2,3]","plain","{}" ] | project s, isjson=(s startswith "{" or s startswith "["), inner=extract(@"\{(.*)\}",1,s), l=strlen(s)
```
**Generated SQL**
```sql
SELECT s, (s ILIKE '{%' OR s ILIKE '[%') AS isjson, REGEXP_EXTRACT(s, '@"\{(.*)\}', 1) AS "inner", LENGTH(CAST(s AS VARCHAR)) AS l FROM (VALUES ('{\"k\":\"v\"}'), ('[1,2,3]'), ('plain'), ('{}')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, isjson:Bool, inner:String, l:Int] rows=4
    - ('{"k":"v"}', True, '"k":"v"', 9)
    - ('[1,2,3]', True, '', 7)
    - ('plain', False, '', 5)
    - ('{}', True, '', 2)
- DuckDB: cols=[s:String, isjson:Bool, inner:String, l:Int] rows=4
    - ('{\"k\":\"v\"}', True, '', 13)
    - ('[1,2,3]', True, '', 7)
    - ('plain', False, '', 5)
    - ('{}', True, '', 2)

### `agent-string-ops-0039` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('abcXYZabc', False, '#xyz#', True) duck=('abcXYZabc', False, 'abcxyzabc', True)

**KQL**
```kql
datatable(s:string)[ "abcXYZabc","ABCxyzABC","mix" ] | project s, lo_has=(tolower(s) has "abc"), rep=replace_regex(tolower(s),@"abc","#"), eqi=(s =~ "abcxyzabc")
```
**Generated SQL**
```sql
SELECT s, (regexp_matches(CAST(LOWER(s) AS VARCHAR), '(?i)\babc\b')) AS lo_has, REGEXP_REPLACE(LOWER(s), '@"abc', '#') AS rep, (UPPER(s) = UPPER('abcxyzabc')) AS eqi FROM (VALUES ('abcXYZabc'), ('ABCxyzABC'), ('mix')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, lo_has:Bool, rep:String, eqi:Bool] rows=3
    - ('abcXYZabc', False, '#xyz#', True)
    - ('ABCxyzABC', False, '#xyz#', True)
    - ('mix', False, 'mix', False)
- DuckDB: cols=[s:String, lo_has:Bool, rep:String, eqi:Bool] rows=3
    - ('abcXYZabc', False, 'abcxyzabc', True)
    - ('ABCxyzABC', False, 'abcxyzabc', True)
    - ('mix', False, 'mix', False)

### `agent-string-ops-0043` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('prefix-MIDDLE-suffix', 'MIDDLE', [
  "prefix",
  "MIDDLE",
  "suffix"
], 'MIDDLE') duck=('prefix-MIDDLE-suffix', '', ["prefix","MIDDLE","suffix"], 'MIDDLE')

**KQL**
```kql
datatable(s:string)[ "prefix-MIDDLE-suffix","a-B-c","nodash" ] | project s, mid=extract(@"-([A-Za-z]+)-",1,s), parts=split(s,"-"), upmid=toupper(tostring(split(s,"-")[1]))
```
**Generated SQL**
```sql
SELECT s, REGEXP_EXTRACT(s, '@"-([A-Za-z]+)-', 1) AS mid, STRING_SPLIT(CAST(s AS VARCHAR), '-') AS parts, UPPER(TRY_CAST(STRING_SPLIT(CAST(s AS VARCHAR), '-')[1 + 1] AS TEXT)) AS upmid FROM (VALUES ('prefix-MIDDLE-suffix'), ('a-B-c'), ('nodash')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, mid:String, parts:Dynamic, upmid:String] rows=3
    - ('prefix-MIDDLE-suffix', 'MIDDLE', [
  "prefix",
  "MIDDLE",
  "suffix"
], 'MIDDLE')
    - ('a-B-c', 'B', [
  "a",
  "B",
  "c"
], 'B')
    - ('nodash', '', [
  "nodash"
], '')
- DuckDB: cols=[s:String, mid:String, parts:Unknown, upmid:String] rows=3
    - ('prefix-MIDDLE-suffix', '', ["prefix","MIDDLE","suffix"], 'MIDDLE')
    - ('a-B-c', '', ["a","B","c"], 'B')
    - ('nodash', '', ["nodash"], null)

### `agent-string-ops-0044` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('trim	me	', 'trim	me', 7, 8, 1) duck=('trim\tme\t', 'trim\tme\t', 10, 10, 0)

**KQL**
```kql
datatable(s:string)[ "trim\tme\t","  pad  ","xxstripxx","" ] | project s, multi=trim(@"[\sx]",s), tslen=strlen(trim(@"[\sx]",s)), origlen=strlen(s), diff=(strlen(s) - strlen(trim(@"[\sx]",s)))
```
**Generated SQL**
```sql
SELECT s, TRIM(s, '@"[\sx]') AS multi, LENGTH(CAST(TRIM(s, '@"[\sx]') AS VARCHAR)) AS tslen, LENGTH(CAST(s AS VARCHAR)) AS origlen, (LENGTH(CAST(s AS VARCHAR)) - LENGTH(CAST(TRIM(s, '@"[\sx]') AS VARCHAR))) AS diff FROM (VALUES ('trim\tme\t'), ('  pad  '), ('xxstripxx'), ('')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, multi:String, tslen:Int, origlen:Int, diff:Int] rows=4
    - ('trim	me	', 'trim	me', 7, 8, 1)
    - ('  pad  ', ' pad ', 5, 7, 2)
    - ('xxstripxx', 'xstripx', 7, 9, 2)
    - ('', '', 0, 0, 0)
- DuckDB: cols=[s:String, multi:String, tslen:Int, origlen:Int, diff:Int] rows=4
    - ('trim\tme\t', 'trim\tme\t', 10, 10, 0)
    - ('  pad  ', '  pad  ', 7, 7, 0)
    - ('xxstripxx', 'trip', 4, 9, 5)
    - ('', '', 0, 0, 0)

### `t1-string-ops-0021` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=0

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", " spaced ","y", "O'Brien","x", "where","kw", "MiXeD","y" ] | where s has "café"
```
**Generated SQL**
```sql
SELECT * FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), (' spaced ', 'y'), ('O''Brien', 'x'), ('where', 'kw'), ('MiXeD', 'y')) AS t(s, tag) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)\bcafé\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tag:String] rows=1
    - ('café', 'y')
- DuckDB: cols=[s:String, tag:String] rows=0

### `t1-string-ops-0022` — MismatchRows (high)

*Detail:* row count: kusto=8 duck=7

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", " spaced ","y", "O'Brien","x", "where","kw", "MiXeD","y" ] | where s has ""
```
**Generated SQL**
```sql
SELECT * FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), (' spaced ', 'y'), ('O''Brien', 'x'), ('where', 'kw'), ('MiXeD', 'y')) AS t(s, tag) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)\b\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tag:String] rows=8
    - ('alpha', 'x')
    - ('Beta', 'x')
    - ('café', 'y')
    - ('', 'z')
    - (' spaced ', 'y')
    - ('O'Brien', 'x')
    - ('where', 'kw')
    - ('MiXeD', 'y')
- DuckDB: cols=[s:String, tag:String] rows=7
    - ('alpha', 'x')
    - ('Beta', 'x')
    - ('café', 'y')
    - (' spaced ', 'y')
    - ('O'Brien', 'x')
    - ('where', 'kw')
    - ('MiXeD', 'y')

### `t1-string-ops-0026` — MismatchRows (high)

*Detail:* row count: kusto=7 duck=8

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", " spaced ","y", "O'Brien","x", "where","kw", "MiXeD","y" ] | where s !has "café"
```
**Generated SQL**
```sql
SELECT * FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), (' spaced ', 'y'), ('O''Brien', 'x'), ('where', 'kw'), ('MiXeD', 'y')) AS t(s, tag) WHERE NOT regexp_matches(CAST(s AS VARCHAR), '(?i)\bcafé\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tag:String] rows=7
    - ('alpha', 'x')
    - ('Beta', 'x')
    - ('', 'z')
    - (' spaced ', 'y')
    - ('O'Brien', 'x')
    - ('where', 'kw')
    - ('MiXeD', 'y')
- DuckDB: cols=[s:String, tag:String] rows=8
    - ('alpha', 'x')
    - ('Beta', 'x')
    - ('café', 'y')
    - ('', 'z')
    - (' spaced ', 'y')
    - ('O'Brien', 'x')
    - ('where', 'kw')
    - ('MiXeD', 'y')

### `t1-string-ops-0027` — MismatchRows (high)

*Detail:* row count: kusto=0 duck=1

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", " spaced ","y", "O'Brien","x", "where","kw", "MiXeD","y" ] | where s !has ""
```
**Generated SQL**
```sql
SELECT * FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), (' spaced ', 'y'), ('O''Brien', 'x'), ('where', 'kw'), ('MiXeD', 'y')) AS t(s, tag) WHERE NOT regexp_matches(CAST(s AS VARCHAR), '(?i)\b\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tag:String] rows=0
- DuckDB: cols=[s:String, tag:String] rows=1
    - ('', 'z')

### `t1-string-ops-0031` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=0

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", " spaced ","y", "O'Brien","x", "where","kw", "MiXeD","y" ] | where s has_cs "café"
```
**Generated SQL**
```sql
SELECT * FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), (' spaced ', 'y'), ('O''Brien', 'x'), ('where', 'kw'), ('MiXeD', 'y')) AS t(s, tag) WHERE regexp_matches(CAST(s AS VARCHAR), '\bcafé\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tag:String] rows=1
    - ('café', 'y')
- DuckDB: cols=[s:String, tag:String] rows=0

### `t1-string-ops-0032` — MismatchRows (high)

*Detail:* row count: kusto=8 duck=7

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", " spaced ","y", "O'Brien","x", "where","kw", "MiXeD","y" ] | where s has_cs ""
```
**Generated SQL**
```sql
SELECT * FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), (' spaced ', 'y'), ('O''Brien', 'x'), ('where', 'kw'), ('MiXeD', 'y')) AS t(s, tag) WHERE regexp_matches(CAST(s AS VARCHAR), '\b\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tag:String] rows=8
    - ('alpha', 'x')
    - ('Beta', 'x')
    - ('café', 'y')
    - ('', 'z')
    - (' spaced ', 'y')
    - ('O'Brien', 'x')
    - ('where', 'kw')
    - ('MiXeD', 'y')
- DuckDB: cols=[s:String, tag:String] rows=7
    - ('alpha', 'x')
    - ('Beta', 'x')
    - ('café', 'y')
    - (' spaced ', 'y')
    - ('O'Brien', 'x')
    - ('where', 'kw')
    - ('MiXeD', 'y')

### `t1-string-ops-0062` — MismatchRows (high)

*Detail:* row count: kusto=8 duck=7

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", " spaced ","y", "O'Brien","x", "where","kw", "MiXeD","y" ] | where s hasprefix ""
```
**Generated SQL**
```sql
SELECT * FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), (' spaced ', 'y'), ('O''Brien', 'x'), ('where', 'kw'), ('MiXeD', 'y')) AS t(s, tag) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tag:String] rows=8
    - ('alpha', 'x')
    - ('Beta', 'x')
    - ('café', 'y')
    - ('', 'z')
    - (' spaced ', 'y')
    - ('O'Brien', 'x')
    - ('where', 'kw')
    - ('MiXeD', 'y')
- DuckDB: cols=[s:String, tag:String] rows=7
    - ('alpha', 'x')
    - ('Beta', 'x')
    - ('café', 'y')
    - (' spaced ', 'y')
    - ('O'Brien', 'x')
    - ('where', 'kw')
    - ('MiXeD', 'y')

### `t1-string-ops-0066` — MismatchRows (high)

*Detail:* row count: kusto=1 duck=0

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", " spaced ","y", "O'Brien","x", "where","kw", "MiXeD","y" ] | where s hassuffix "café"
```
**Generated SQL**
```sql
SELECT * FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), (' spaced ', 'y'), ('O''Brien', 'x'), ('where', 'kw'), ('MiXeD', 'y')) AS t(s, tag) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)café\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tag:String] rows=1
    - ('café', 'y')
- DuckDB: cols=[s:String, tag:String] rows=0

### `t1-string-ops-0067` — MismatchRows (high)

*Detail:* row count: kusto=8 duck=7

**KQL**
```kql
datatable(s:string, tag:string)[ "alpha","x", "Beta","x", "café","y", "","z", " spaced ","y", "O'Brien","x", "where","kw", "MiXeD","y" ] | where s hassuffix ""
```
**Generated SQL**
```sql
SELECT * FROM (VALUES ('alpha', 'x'), ('Beta', 'x'), ('café', 'y'), ('', 'z'), (' spaced ', 'y'), ('O''Brien', 'x'), ('where', 'kw'), ('MiXeD', 'y')) AS t(s, tag) WHERE regexp_matches(CAST(s AS VARCHAR), '(?i)\b')
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, tag:String] rows=8
    - ('alpha', 'x')
    - ('Beta', 'x')
    - ('café', 'y')
    - ('', 'z')
    - (' spaced ', 'y')
    - ('O'Brien', 'x')
    - ('where', 'kw')
    - ('MiXeD', 'y')
- DuckDB: cols=[s:String, tag:String] rows=7
    - ('alpha', 'x')
    - ('Beta', 'x')
    - ('café', 'y')
    - (' spaced ', 'y')
    - ('O'Brien', 'x')
    - ('where', 'kw')
    - ('MiXeD', 'y')

## Family: type-casts-coercion (69)

### `agent-type-casts-coercion-0003` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

**KQL**
```kql
print x = toint(2147483647) + toint(1)
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS x
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int] rows=1
    - (2147483648)
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

### `agent-type-casts-coercion-0019` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!

**KQL**
```kql
datatable(l:long)[ 9223372036854775807, -9223372036854775808 ] | extend plus = l + 1, minus = l - 1
```
**Generated SQL**
```sql
SELECT *, l + 1 AS plus, l - 1 AS minus FROM (VALUES (CAST(9223372036854775807 AS BIGINT)), (CAST(-9223372036854775808 AS BIGINT))) AS t(l)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[l:Int, plus:Int, minus:Int] rows=2
    - (9223372036854775807, -9223372036854775808, 9223372036854775806)
    - (-9223372036854775808, -9223372036854775807, 9223372036854775807)
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!

### `agent-type-casts-coercion-0005` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

**KQL**
```kql
print over1 = toint(2147483647) + 1, over2 = tolong(toint(2147483647) + 1), over3 = 2147483647 + 1, over4 = int(2147483647) * int(2)
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1 AS over1, TRY_CAST(TRUNC(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1 AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1 AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS over2, 2147483647 + 1 AS over3, 2147483647 * 2 AS over4
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[over1:Int, over2:Int, over3:Int, over4:Int] rows=1
    - (2147483648, 2147483648, 2147483648, 4294967294)
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

### `agent-type-casts-coercion-0017` — SqlExecError (highest)

*Detail:* Binder Error: No function matches the given name and argument types '%(INTERVAL, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	%(TINYINT, TINYINT) -> TINYINT
	%(SMALLINT, SMALLINT) -> SMALLINT
	%(INTEGER, INTEGER) -> INTEGER
	%(BIGINT, BIGINT) -> BIGINT
	%(HUGEINT, HUGEINT) -> HUGEINT
	%(FLOAT, FLOAT) -> FLOAT
	%(DOUBLE, DOUBLE) -> DOUBLE
	%(DECIMAL, DECIMAL) -> DECIMAL
	%(UTINYINT, UTINYINT) -> UTINYINT
	%(USMALLINT, USMALLINT) -> USMALLINT
	%(UINTEGER, UINTEGER) -> UINTEGER
	%(UBIGINT, UBIGINT) -> UBIGINT
	%(UHUGEINT, UHUGEINT) -> UHUGEINT


LINE 1: ...cond')))) AS ts_div, (86400000 * INTERVAL '1 millisecond') % (18000000 * INTERVAL '1 millisecond') AS ts_mod, TRY_CAST...
                                                                      ^

**KQL**
```kql
print ts_long = tolong(1d), ts_real = todouble(1d), ts_back = totimespan(tolong(1d)), ts_div = 1d / 1h, ts_mod = 1d % 5h, ts_int = toint(1h)
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST((86400000 * INTERVAL '1 millisecond') AS DOUBLE), TRY_CAST(TRY_CAST((86400000 * INTERVAL '1 millisecond') AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS ts_long, COALESCE(TRY_CAST((86400000 * INTERVAL '1 millisecond') AS DOUBLE), TRY_CAST(TRY_CAST((86400000 * INTERVAL '1 millisecond') AS BOOLEAN) AS DOUBLE)) AS ts_real, TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST((86400000 * INTERVAL '1 millisecond') AS DOUBLE), TRY_CAST(TRY_CAST((86400000 * INTERVAL '1 millisecond') AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS INTERVAL) AS ts_back, (EXTRACT(EPOCH FROM ((86400000 * INTERVAL '1 millisecond'))) / EXTRACT(EPOCH FROM ((3600000 * INTERVAL '1 millisecond')))) AS ts_div, (86400000 * INTERVAL '1 millisecond') % (18000000 * INTERVAL '1 millisecond') AS ts_mod, TRY_CAST(TRUNC(COALESCE(TRY_CAST((3600000 * INTERVAL '1 millisecond') AS DOUBLE), TRY_CAST(TRY_CAST((3600000 * INTERVAL '1 millisecond') AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ts_int
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ts_long:Int, ts_real:Real, ts_back:TimeSpan, ts_div:Real, ts_mod:TimeSpan, ts_int:Int] rows=1
    - (864000000000, 864000000000, 1.00:00:00, 24, 04:00:00, 1640261632)
- DuckDB: ERROR — Binder Error: No function matches the given name and argument types '%(INTERVAL, INTERVAL)'. You might need to add explicit type casts.
	Candidate functions:
	%(TINYINT, TINYINT) -> TINYINT
	%(SMALLINT, SMALLINT) -> SMALLINT
	%(INTEGER, INTEGER) -> INTEGER
	%(BIGINT, BIGINT) -> BIGINT
	%(HUGEINT, HUGEINT) -> HUGEINT
	%(FLOAT, FLOAT) -> FLOAT
	%(DOUBLE, DOUBLE) -> DOUBLE
	%(DECIMAL, DECIMAL) -> DECIMAL
	%(UTINYINT, UTINYINT) -> UTINYINT
	%(USMALLINT, USMALLINT) -> USMALLINT
	%(UINTEGER, UINTEGER) -> UINTEGER
	%(UBIGINT, UBIGINT) -> UBIGINT
	%(UHUGEINT, UHUGEINT) -> UHUGEINT


LINE 1: ...cond')))) AS ts_div, (86400000 * INTERVAL '1 millisecond') % (18000000 * INTERVAL '1 millisecond') AS ts_mod, TRY_CAST...
                                                                      ^

### `agent-type-casts-coercion-0031` — SqlExecError (highest)

*Detail:* Out of Range Error: cannot take square root of a negative number

**KQL**
```kql
print sqrt_neg = sqrt(-1.0), log_neg = log(-1.0), log_zero = log(0.0), exp_big = exp(1000.0), isnan_sqrt = isnan(sqrt(-1.0))
```
**Generated SQL**
```sql
SELECT SQRT((-1.0)) AS sqrt_neg, LN((-1.0)) AS log_neg, LN(0.0) AS log_zero, EXP(1000.0) AS exp_big, ISNAN(SQRT((-1.0))) AS isnan_sqrt
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[sqrt_neg:Real, log_neg:Real, log_zero:Real, exp_big:Real, isnan_sqrt:Bool] rows=1
    - (NaN, null, -Infinity, Infinity, True)
- DuckDB: ERROR — Out of Range Error: cannot take square root of a negative number

### `agent-type-casts-coercion-0038` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in negation of numeric value!

**KQL**
```kql
datatable(i:int)[ 2147483647, -2147483648, 1000000, -1000000 ] | extend sq = i * i, sq_long = tolong(i) * tolong(i), neg = -i, abs_v = abs(i), as_long = tolong(i) * 1000
```
**Generated SQL**
```sql
SELECT *, i * i AS sq, TRY_CAST(TRUNC(COALESCE(TRY_CAST(i AS DOUBLE), TRY_CAST(TRY_CAST(i AS BOOLEAN) AS DOUBLE))) AS BIGINT) * TRY_CAST(TRUNC(COALESCE(TRY_CAST(i AS DOUBLE), TRY_CAST(TRY_CAST(i AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS sq_long, (-i) AS neg, GREATEST(i, -(i)) AS abs_v, TRY_CAST(TRUNC(COALESCE(TRY_CAST(i AS DOUBLE), TRY_CAST(TRY_CAST(i AS BOOLEAN) AS DOUBLE))) AS BIGINT) * 1000 AS as_long FROM (VALUES (2147483647), (-2147483648), (1000000), (-1000000)) AS t(i)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[i:Int, sq:Int, sq_long:Int, neg:Int, abs_v:Int, as_long:Int] rows=4
    - (2147483647, 4611686014132420609, 4611686014132420609, -2147483647, 2147483647, 2147483647000)
    - (-2147483648, 4611686018427387904, 4611686018427387904, 2147483648, 2147483648, -2147483648000)
    - (1000000, 1000000000000, 1000000000000, -1000000, 1000000, 1000000000)
    - (-1000000, 1000000000000, 1000000000000, 1000000, 1000000, -1000000000)
- DuckDB: ERROR — Out of Range Error: Overflow in negation of numeric value!

### `agent-type-casts-coercion-0002` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

**KQL**
```kql
print x = toint(2147483647)+toint(1)
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS x
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int] rows=1
    - (2147483648)
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

### `agent-type-casts-coercion-0035` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!

**KQL**
```kql
datatable(x:long)[ 9223372036854775807, -9223372036854775808, 0 ] | extend plus1=x+1, dbl=todouble(x), back=tolong(todouble(x)), gt=gettype(x+1)
```
**Generated SQL**
```sql
SELECT *, x + 1 AS plus1, COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE)) AS dbl, TRY_CAST(TRUNC(COALESCE(TRY_CAST(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE)) AS DOUBLE), TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE)) AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS back, CASE WHEN TYPEOF(x + 1) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(x + 1) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(x + 1) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(x + 1) = 'VARCHAR' THEN 'string' WHEN TYPEOF(x + 1) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(x + 1) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(x + 1) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(x + 1) = 'UUID' THEN 'guid' WHEN TYPEOF(x + 1) LIKE '%[]' OR TYPEOF(x + 1) LIKE 'STRUCT%' OR TYPEOF(x + 1) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(x + 1) = 'JSON' THEN (CASE WHEN json_type(CAST(x + 1 AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(x + 1)) END AS gt FROM (VALUES (CAST(9223372036854775807 AS BIGINT)), (CAST(-9223372036854775808 AS BIGINT)), (CAST(0 AS BIGINT))) AS t(x)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, plus1:Int, dbl:Real, back:Int, gt:String] rows=3
    - (9223372036854775807, -9223372036854775808, 9.223372036854776E+18, 9223372036854775807, 'long')
    - (-9223372036854775808, -9223372036854775807, -9.223372036854776E+18, -9223372036854775808, 'long')
    - (0, 1, 0, 0, 'long')
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!

### `agent-type-casts-coercion-0036` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!

**KQL**
```kql
print x = 9223372036854775807 + 1, y = gettype(9223372036854775807)
```
**Generated SQL**
```sql
SELECT 9223372036854775807 + 1 AS x, CASE WHEN TYPEOF(9223372036854775807) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(9223372036854775807) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(9223372036854775807) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(9223372036854775807) = 'VARCHAR' THEN 'string' WHEN TYPEOF(9223372036854775807) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(9223372036854775807) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(9223372036854775807) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(9223372036854775807) = 'UUID' THEN 'guid' WHEN TYPEOF(9223372036854775807) LIKE '%[]' OR TYPEOF(9223372036854775807) LIKE 'STRUCT%' OR TYPEOF(9223372036854775807) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(9223372036854775807) = 'JSON' THEN (CASE WHEN json_type(CAST(9223372036854775807 AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(9223372036854775807)) END AS y
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:String] rows=1
    - (-9223372036854775808, 'long')
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!

### `agent-type-casts-coercion-0055` — SqlExecError (highest)

*Detail:* Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

**KQL**
```kql
print x = 2147483647 + 1, y = gettype(2147483647 + 1), z = toint(2147483647) + 1, w = gettype(toint(2147483647) + 1)
```
**Generated SQL**
```sql
SELECT 2147483647 + 1 AS x, CASE WHEN TYPEOF(2147483647 + 1) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(2147483647 + 1) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(2147483647 + 1) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(2147483647 + 1) = 'VARCHAR' THEN 'string' WHEN TYPEOF(2147483647 + 1) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(2147483647 + 1) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(2147483647 + 1) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(2147483647 + 1) = 'UUID' THEN 'guid' WHEN TYPEOF(2147483647 + 1) LIKE '%[]' OR TYPEOF(2147483647 + 1) LIKE 'STRUCT%' OR TYPEOF(2147483647 + 1) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(2147483647 + 1) = 'JSON' THEN (CASE WHEN json_type(CAST(2147483647 + 1 AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(2147483647 + 1)) END AS y, TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1 AS z, CASE WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) LIKE '%[]' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1) = 'JSON' THEN (CASE WHEN json_type(CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1 AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(2147483647 AS DOUBLE), TRY_CAST(TRY_CAST(2147483647 AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1)) END AS w
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:String, z:Int, w:String] rows=1
    - (2147483648, 'long', 2147483648, 'long')
- DuckDB: ERROR — Out of Range Error: Overflow in addition of INT32 (2147483647 + 1)!

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

### `agent-type-casts-coercion-0009` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(3.14159, null, 3.3333333333333335) duck=(3.142, null, 3.3333333333333335)

**KQL**
```kql
print d1 = todecimal("3.14159"), d2 = todecimal("abc"), d3 = todecimal(10) / todecimal(3)
```
**Generated SQL**
```sql
SELECT TRY_CAST('3.14159' AS DECIMAL) AS d1, TRY_CAST('abc' AS DECIMAL) AS d2, TRY_CAST(10 AS DECIMAL) / TRY_CAST(3 AS DECIMAL) AS d3
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d1:Real, d2:Real, d3:Real] rows=1
    - (3.14159, null, 3.3333333333333335)
- DuckDB: cols=[d1:Real, d2:Real, d3:Real] rows=1
    - (3.142, null, 3.3333333333333335)

### `agent-type-casts-coercion-0015` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('dictionary', 'array', 'timespan', 'decimal') duck=('dictionary', 'array', 'timespan', 'real')

**KQL**
```kql
print g7 = gettype(dynamic({"a":1})), g8 = gettype(dynamic([1,2])), g9 = gettype(1s), g10 = gettype(todecimal(1))
```
**Generated SQL**
```sql
SELECT CASE WHEN TYPEOF('{"a":1}'::JSON) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF('{"a":1}'::JSON) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF('{"a":1}'::JSON) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF('{"a":1}'::JSON) = 'VARCHAR' THEN 'string' WHEN TYPEOF('{"a":1}'::JSON) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF('{"a":1}'::JSON) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF('{"a":1}'::JSON) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF('{"a":1}'::JSON) = 'UUID' THEN 'guid' WHEN TYPEOF('{"a":1}'::JSON) LIKE '%[]' OR TYPEOF('{"a":1}'::JSON) LIKE 'STRUCT%' OR TYPEOF('{"a":1}'::JSON) LIKE 'MAP%' THEN 'array' WHEN TYPEOF('{"a":1}'::JSON) = 'JSON' THEN (CASE WHEN json_type(CAST('{"a":1}'::JSON AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF('{"a":1}'::JSON)) END AS g7, CASE WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(LIST_VALUE(1, 2)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'UUID' THEN 'guid' WHEN TYPEOF(LIST_VALUE(1, 2)) LIKE '%[]' OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'STRUCT%' OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'JSON' THEN (CASE WHEN json_type(CAST(LIST_VALUE(1, 2) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(LIST_VALUE(1, 2))) END AS g8, CASE WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF((1000 * INTERVAL '1 millisecond')) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) = 'VARCHAR' THEN 'string' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) = 'UUID' THEN 'guid' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) LIKE '%[]' OR TYPEOF((1000 * INTERVAL '1 millisecond')) LIKE 'STRUCT%' OR TYPEOF((1000 * INTERVAL '1 millisecond')) LIKE 'MAP%' THEN 'array' WHEN TYPEOF((1000 * INTERVAL '1 millisecond')) = 'JSON' THEN (CASE WHEN json_type(CAST((1000 * INTERVAL '1 millisecond') AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF((1000 * INTERVAL '1 millisecond'))) END AS g9, CASE WHEN TYPEOF(TRY_CAST(1 AS DECIMAL)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(1 AS DECIMAL)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL)) LIKE '%[]' OR TYPEOF(TRY_CAST(1 AS DECIMAL)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(1 AS DECIMAL)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL)) = 'JSON' THEN (CASE WHEN json_type(CAST(TRY_CAST(1 AS DECIMAL) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(1 AS DECIMAL))) END AS g10
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g7:String, g8:String, g9:String, g10:String] rows=1
    - ('dictionary', 'array', 'timespan', 'decimal')
- DuckDB: cols=[g7:String, g8:String, g9:String, g10:String] rows=1
    - ('dictionary', 'array', 'timespan', 'real')

### `agent-type-casts-coercion-0021` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('123', '1.5', 'True', '2020-01-01T00:00:00.0000000Z', '1.00:00:00') duck=('123', '1.5', 'true', '2020-01-01 00:00:00', '24:00:00')

**KQL**
```kql
print c1 = tostring(123), c2 = tostring(1.5), c3 = tostring(true), c4 = tostring(datetime(2020-01-01)), c5 = tostring(1d)
```
**Generated SQL**
```sql
SELECT TRY_CAST(123 AS TEXT) AS c1, TRY_CAST(1.5 AS TEXT) AS c2, TRY_CAST(TRUE AS TEXT) AS c3, TRY_CAST(TIMESTAMP '2020-01-01 00:00:00' AS TEXT) AS c4, TRY_CAST((86400000 * INTERVAL '1 millisecond') AS TEXT) AS c5
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[c1:String, c2:String, c3:String, c4:String, c5:String] rows=1
    - ('123', '1.5', 'True', '2020-01-01T00:00:00.0000000Z', '1.00:00:00')
- DuckDB: cols=[c1:String, c2:String, c3:String, c4:String, c5:String] rows=1
    - ('123', '1.5', 'true', '2020-01-01 00:00:00', '24:00:00')

### `agent-type-casts-coercion-0022` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1.02:03:04, 01:00:00, null) duck=(null, 01:00:00, null)

**KQL**
```kql
print ts1 = totimespan("1.02:03:04"), ts2 = totimespan("01:00:00"), ts3 = totimespan("abc")
```
**Generated SQL**
```sql
SELECT TRY_CAST('1.02:03:04' AS INTERVAL) AS ts1, TRY_CAST('01:00:00' AS INTERVAL) AS ts2, TRY_CAST('abc' AS INTERVAL) AS ts3
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[ts1:TimeSpan, ts2:TimeSpan, ts3:TimeSpan] rows=1
    - (1.02:03:04, 01:00:00, null)
- DuckDB: cols=[ts1:TimeSpan, ts2:TimeSpan, ts3:TimeSpan] rows=1
    - (null, 01:00:00, null)

### `agent-type-casts-coercion-0024` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2020-01-01T00:00:00.0000000Z, 637134336000000000, 6.37134336E+17) duck=(2020-01-01T00:00:00.0000000Z, null, null)

**KQL**
```kql
datatable(t:datetime)[ datetime(2020-01-01), datetime(2021-06-15 12:30:00) ] | extend asl = tolong(t), asr = todouble(t)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(t AS DOUBLE), TRY_CAST(TRY_CAST(t AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS asl, COALESCE(TRY_CAST(t AS DOUBLE), TRY_CAST(TRY_CAST(t AS BOOLEAN) AS DOUBLE)) AS asr FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00'), (TIMESTAMP '2021-06-15 12:30:00')) AS t(t)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, asl:Int, asr:Real] rows=2
    - (2020-01-01T00:00:00.0000000Z, 637134336000000000, 6.37134336E+17)
    - (2021-06-15T12:30:00.0000000Z, 637593570000000000, 6.3759357E+17)
- DuckDB: cols=[t:DateTime, asl:Int, asr:Real] rows=2
    - (2020-01-01T00:00:00.0000000Z, null, null)
    - (2021-06-15T12:30:00.0000000Z, null, null)

### `agent-type-casts-coercion-0025` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1.00:00:00, 864000000000, 36000000000) duck=(1.00:00:00, null, null)

**KQL**
```kql
print span = totimespan("1d"), aslong = tolong(1d), asdouble = todouble(1h)
```
**Generated SQL**
```sql
SELECT TRY_CAST('1d' AS INTERVAL) AS span, TRY_CAST(TRUNC(COALESCE(TRY_CAST((86400000 * INTERVAL '1 millisecond') AS DOUBLE), TRY_CAST(TRY_CAST((86400000 * INTERVAL '1 millisecond') AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS aslong, COALESCE(TRY_CAST((3600000 * INTERVAL '1 millisecond') AS DOUBLE), TRY_CAST(TRY_CAST((3600000 * INTERVAL '1 millisecond') AS BOOLEAN) AS DOUBLE)) AS asdouble
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[span:TimeSpan, aslong:Int, asdouble:Real] rows=1
    - (1.00:00:00, 864000000000, 36000000000)
- DuckDB: cols=[span:TimeSpan, aslong:Int, asdouble:Real] rows=1
    - (1.00:00:00, null, null)

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

### `agent-type-casts-coercion-0029` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 0, 'True') duck=(1, 0, 'true')

**KQL**
```kql
print bool2int = toint(true), bool2int2 = toint(false), bool2str = tostring(true)
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(TRUE AS DOUBLE), TRY_CAST(TRY_CAST(TRUE AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS bool2int, TRY_CAST(TRUNC(COALESCE(TRY_CAST(FALSE AS DOUBLE), TRY_CAST(TRY_CAST(FALSE AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS bool2int2, TRY_CAST(TRUE AS TEXT) AS bool2str
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[bool2int:Int, bool2int2:Int, bool2str:String] rows=1
    - (1, 0, 'True')
- DuckDB: cols=[bool2int:Int, bool2int2:Int, bool2str:String] rows=1
    - (1, 0, 'true')

### `agent-type-casts-coercion-0033` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(10, 3.3333333333333335, 'decimal') duck=(10.0, 3.3333333333333335, 'real')

**KQL**
```kql
datatable(d:decimal)[ decimal(10), decimal(3), decimal(0) ] | extend div = d / decimal(3), gt = gettype(d)
```
**Generated SQL**
```sql
SELECT *, d / 3.0 AS div, CASE WHEN TYPEOF(d) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(d) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(d) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(d) = 'VARCHAR' THEN 'string' WHEN TYPEOF(d) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(d) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(d) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(d) = 'UUID' THEN 'guid' WHEN TYPEOF(d) LIKE '%[]' OR TYPEOF(d) LIKE 'STRUCT%' OR TYPEOF(d) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(d) = 'JSON' THEN (CASE WHEN json_type(CAST(d AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(d)) END AS gt FROM (VALUES (10.0), (3.0), (0.0)) AS t(d)
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

### `agent-type-casts-coercion-0037` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1.5, 2, 0.5) duck=(1.5, 2.0, -1.5)

**KQL**
```kql
print mixmod = 7.5 % 2, mixmod2 = 7 % 2.5, negmod = -7.5 % 2
```
**Generated SQL**
```sql
SELECT 7.5 % 2 AS mixmod, 7 % 2.5 AS mixmod2, (-7.5) % 2 AS negmod
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[mixmod:Real, mixmod2:Real, negmod:Real] rows=1
    - (1.5, 2, 0.5)
- DuckDB: cols=[mixmod:Real, mixmod2:Real, negmod:Real] rows=1
    - (1.5, 2.0, -1.5)

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

### `agent-type-casts-coercion-0045` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[x:Dynamic|String]  
*Detail:* first differing row[2]: kusto=("3", 3, 3, '3') duck=('"3"', 3, 3, '"3"')

**KQL**
```kql
datatable(x:dynamic)[ dynamic(1), dynamic(1.5), dynamic("3"), dynamic(true), dynamic(null) ] | extend ti = toint(x), td = todouble(x), ts = tostring(x)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ti, COALESCE(TRY_CAST(x AS DOUBLE), TRY_CAST(TRY_CAST(x AS BOOLEAN) AS DOUBLE)) AS td, TRY_CAST(x AS TEXT) AS ts FROM (VALUES ('1'::JSON), ('1.5'::JSON), ('"3"'::JSON), ('true'::JSON), (NULL)) AS t(x)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Dynamic, ti:Int, td:Real, ts:String] rows=5
    - (1, 1, 1, '1')
    - (1.5, 1, 1.5, '1.5')
    - ("3", 3, 3, '3')
    - (true, 1, 1, 'true')
    - (null, null, null, '')
- DuckDB: cols=[x:String, ti:Int, td:Real, ts:String] rows=5
    - ('1', 1, 1, '1')
    - ('1.5', 1, 1.5, '1.5')
    - ('"3"', 3, 3, '"3"')
    - ('true', 1, 1, 'true')
    - (null, null, null, null)

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
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ti, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tl, COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE)) AS td, TRY_CAST(d AS BOOLEAN) AS tb FROM (VALUES ('2147483648'::JSON), ('1.9'::JSON), ('"42"'::JSON), ('"3.14"'::JSON), ('"0x10"'::JSON), ('true'::JSON), (NULL), (LIST_VALUE(1))) AS t(d)
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

*Detail:* first differing row[0]: kusto=(1970-01-01T00:00:00.0000000Z, 621355968000000000, 6.21355968E+17, 1970-01-01T00:00:00.0000000Z) duck=(1970-01-01T00:00:00.0000000Z, null, null, null)

**KQL**
```kql
datatable(t:datetime)[ datetime(1970-01-01), datetime(0001-01-01), datetime(9999-12-31 23:59:59.9999999), datetime(2020-02-29) ] | extend asl = tolong(t), asr = todouble(t), roundtrip = todatetime(tolong(t))
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(t AS DOUBLE), TRY_CAST(TRY_CAST(t AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS asl, COALESCE(TRY_CAST(t AS DOUBLE), TRY_CAST(TRY_CAST(t AS BOOLEAN) AS DOUBLE)) AS asr, COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(t AS DOUBLE), TRY_CAST(TRY_CAST(t AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS TIMESTAMP), TRY_STRPTIME(CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(t AS DOUBLE), TRY_CAST(TRY_CAST(t AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS roundtrip FROM (VALUES (TIMESTAMP '1970-01-01 00:00:00'), (TIMESTAMP '0001-01-01 00:00:00'), (TIMESTAMP '9999-12-31 23:59:59.999999'), (TIMESTAMP '2020-02-29 00:00:00')) AS t(t)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, asl:Int, asr:Real, roundtrip:DateTime] rows=4
    - (1970-01-01T00:00:00.0000000Z, 621355968000000000, 6.21355968E+17, 1970-01-01T00:00:00.0000000Z)
    - (0001-01-01T00:00:00.0000000Z, 0, 0, 0001-01-01T00:00:00.0000000Z)
    - (9999-12-31T23:59:59.9999999Z, 3155378975999999999, 3.155378976E+18, 9999-12-31T23:59:59.9999999Z)
    - (2020-02-29T00:00:00.0000000Z, 637185312000000000, 6.37185312E+17, 2020-02-29T00:00:00.0000000Z)
- DuckDB: cols=[t:DateTime, asl:Int, asr:Real, roundtrip:DateTime] rows=4
    - (1970-01-01T00:00:00.0000000Z, null, null, null)
    - (0001-01-01T00:00:00.0000000Z, null, null, null)
    - (9999-12-31T23:59:59.9999990Z, null, null, null)
    - (2020-02-29T00:00:00.0000000Z, null, null, null)

### `agent-type-casts-coercion-0019` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(3.141592653589793, 0.3333333333333333, True, 'decimal', 4294967296) duck=(3.142, 0.3333333333333333, True, 'real', 4294967296.000000)

**KQL**
```kql
print dec1 = todecimal("3.14159265358979323846"), dec2 = todecimal(1) / todecimal(3), dec3 = decimal(0.1) + decimal(0.2) == decimal(0.3), dec4 = gettype(todecimal(1) + toint(1)), dec5 = todecimal(2147483648) * todecimal(2)
```
**Generated SQL**
```sql
SELECT TRY_CAST('3.14159265358979323846' AS DECIMAL) AS dec1, TRY_CAST(1 AS DECIMAL) / TRY_CAST(3 AS DECIMAL) AS dec2, 0.1 + 0.2 = 0.3 AS dec3, CASE WHEN TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE '%[]' OR TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'JSON' THEN (CASE WHEN json_type(CAST(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(1 AS DECIMAL) + TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER))) END AS dec4, TRY_CAST(2147483648 AS DECIMAL) * TRY_CAST(2 AS DECIMAL) AS dec5
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[dec1:Real, dec2:Real, dec3:Bool, dec4:String, dec5:Real] rows=1
    - (3.141592653589793, 0.3333333333333333, True, 'decimal', 4294967296)
- DuckDB: cols=[dec1:Real, dec2:Real, dec3:Bool, dec4:String, dec5:Real] rows=1
    - (3.142, 0.3333333333333333, True, 'real', 4294967296.000000)

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
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('42' AS DOUBLE), TRY_CAST(TRY_CAST('42' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('42' AS DOUBLE), TRY_CAST(TRY_CAST('42' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS BOOLEAN) AS DOUBLE)) AS TEXT) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('42' AS DOUBLE), TRY_CAST(TRY_CAST('42' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('42' AS DOUBLE), TRY_CAST(TRY_CAST('42' AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS BOOLEAN) AS DOUBLE)) AS TEXT) AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS chain1, COALESCE(TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(3.99 AS DOUBLE), TRY_CAST(TRY_CAST(3.99 AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(3.99 AS DOUBLE), TRY_CAST(TRY_CAST(3.99 AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT) AS BOOLEAN) AS DOUBLE)) AS chain2, TRY_CAST(TRUNC(COALESCE(TRY_CAST(TRY_CAST(TRY_CAST('100' AS DECIMAL) AS TEXT) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRY_CAST('100' AS DECIMAL) AS TEXT) AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS chain3, TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(TRUE AS DOUBLE), TRY_CAST(TRY_CAST(TRUE AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT) = TRY_CAST(1 AS TEXT) AS chain4
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[chain1:Int, chain2:Real, chain3:Int, chain4:Bool] rows=1
    - (null, 3, 100, True)
- DuckDB: cols=[chain1:Int, chain2:Real, chain3:Int, chain4:Bool] rows=1
    - (42, 3, 100, True)

### `agent-type-casts-coercion-0023` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('0.30000000000000007', '0.3333333333333333', '9223372036854775807', '-0.0', 'inf', 'NaN') duck=('0.3', '0.3333333333333333', null, '0.0', 'inf', 'nan')

**KQL**
```kql
print str_round = tostring(0.1 + 0.2), str_dbl = tostring(1.0 / 3.0), str_big = tostring(tolong(9223372036854775807)), str_neg0 = tostring(-0.0), str_inf = tostring(real(+inf)), str_nan = tostring(real(nan))
```
**Generated SQL**
```sql
SELECT TRY_CAST(0.1 + 0.2 AS TEXT) AS str_round, TRY_CAST(1.0 / 3.0 AS TEXT) AS str_dbl, TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(9223372036854775807 AS DOUBLE), TRY_CAST(TRY_CAST(9223372036854775807 AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS TEXT) AS str_big, TRY_CAST((-0.0) AS TEXT) AS str_neg0, TRY_CAST(CAST('inf' AS DOUBLE) AS TEXT) AS str_inf, TRY_CAST(CAST('nan' AS DOUBLE) AS TEXT) AS str_nan
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[str_round:String, str_dbl:String, str_big:String, str_neg0:String, str_inf:String, str_nan:String] rows=1
    - ('0.30000000000000007', '0.3333333333333333', '9223372036854775807', '-0.0', 'inf', 'NaN')
- DuckDB: cols=[str_round:String, str_dbl:String, str_big:String, str_neg0:String, str_inf:String, str_nan:String] rows=1
    - ('0.3', '0.3333333333333333', null, '0.0', 'inf', 'nan')

### `agent-type-casts-coercion-0024` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(True, 1, 1, 1, 'True', 1, True) duck=(True, 1, 1, 1, 'true', 1.000, True)

**KQL**
```kql
datatable(b:bool)[ true, false ] | extend asint = toint(b), aslong = tolong(b), asreal = todouble(b), asstr = tostring(b), asdec = todecimal(b), back = tobool(toint(b))
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(b AS DOUBLE), TRY_CAST(TRY_CAST(b AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS asint, TRY_CAST(TRUNC(COALESCE(TRY_CAST(b AS DOUBLE), TRY_CAST(TRY_CAST(b AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS aslong, COALESCE(TRY_CAST(b AS DOUBLE), TRY_CAST(TRY_CAST(b AS BOOLEAN) AS DOUBLE)) AS asreal, TRY_CAST(b AS TEXT) AS asstr, TRY_CAST(b AS DECIMAL) AS asdec, TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(b AS DOUBLE), TRY_CAST(TRY_CAST(b AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS BOOLEAN) AS back FROM (VALUES (TRUE), (FALSE)) AS t(b)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[b:Bool, asint:Int, aslong:Int, asreal:Real, asstr:String, asdec:Real, back:Bool] rows=2
    - (True, 1, 1, 1, 'True', 1, True)
    - (False, 0, 0, 0, 'False', 0, False)
- DuckDB: cols=[b:Bool, asint:Int, aslong:Int, asreal:Real, asstr:String, asdec:Real, back:Bool] rows=2
    - (True, 1, 1, 1, 'true', 1.000, True)
    - (False, 0, 0, 0, 'false', 0.000, False)

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

### `agent-type-casts-coercion-0035` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 2, 1, 0.5, 2.5, 1) duck=(1, 2, 1, -2.5, 2.5, -2.0)

**KQL**
```kql
print modneg1 = (-5) % 3, modneg2 = 5 % (-3), modneg3 = (-5) % (-3), modreal1 = (-5.5) % 3.0, modreal2 = 5.5 % (-3.0), modmix = (-5) % 3.0
```
**Generated SQL**
```sql
SELECT (((((-5))) % NULLIF(3, 0)) + ABS(3)) % NULLIF(3, 0) AS modneg1, (((5) % NULLIF(((-3)), 0)) + ABS(((-3)))) % NULLIF(((-3)), 0) AS modneg2, (((((-5))) % NULLIF(((-3)), 0)) + ABS(((-3)))) % NULLIF(((-3)), 0) AS modneg3, ((-5.5)) % 3.0 AS modreal1, 5.5 % ((-3.0)) AS modreal2, ((-5)) % 3.0 AS modmix
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[modneg1:Int, modneg2:Int, modneg3:Int, modreal1:Real, modreal2:Real, modmix:Real] rows=1
    - (1, 2, 1, 0.5, 2.5, 1)
- DuckDB: cols=[modneg1:Int, modneg2:Int, modneg3:Int, modreal1:Real, modreal2:Real, modmix:Real] rows=1
    - (1, 2, 1, -2.5, 2.5, -2.0)

### `agent-type-casts-coercion-0039` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(null, null, null, False, True) duck=(null, null, null, null, True)

**KQL**
```kql
print empty_chain = toint(tostring(toint(""))), null_arith = toint("") + 1, null_mul = toint("abc") * 0, null_cmp = toint("xyz") == 0, null_isnull = isnull(toint("bad"))
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRUNC(COALESCE(TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT) AS DOUBLE), TRY_CAST(TRY_CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT) AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS empty_chain, TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER) + 1 AS null_arith, TRY_CAST(TRUNC(COALESCE(TRY_CAST('abc' AS DOUBLE), TRY_CAST(TRY_CAST('abc' AS BOOLEAN) AS DOUBLE))) AS INTEGER) * 0 AS null_mul, TRY_CAST(TRUNC(COALESCE(TRY_CAST('xyz' AS DOUBLE), TRY_CAST(TRY_CAST('xyz' AS BOOLEAN) AS DOUBLE))) AS INTEGER) = 0 AS null_cmp, (TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER) IS NULL) AS null_isnull
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
SELECT CASE WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE '%[]' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'JSON' THEN (CASE WHEN json_type(CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('bad' AS DOUBLE), TRY_CAST(TRY_CAST('bad' AS BOOLEAN) AS DOUBLE))) AS INTEGER))) END AS gettype_null, CASE WHEN TYPEOF(NULL) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(NULL) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(NULL) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(NULL) = 'VARCHAR' THEN 'string' WHEN TYPEOF(NULL) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(NULL) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(NULL) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(NULL) = 'UUID' THEN 'guid' WHEN TYPEOF(NULL) LIKE '%[]' OR TYPEOF(NULL) LIKE 'STRUCT%' OR TYPEOF(NULL) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(NULL) = 'JSON' THEN (CASE WHEN json_type(CAST(NULL AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(NULL)) END AS gettype_dynnull, CASE WHEN TYPEOF(CAST('nan' AS DOUBLE)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(CAST('nan' AS DOUBLE)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(CAST('nan' AS DOUBLE)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(CAST('nan' AS DOUBLE)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(CAST('nan' AS DOUBLE)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(CAST('nan' AS DOUBLE)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(CAST('nan' AS DOUBLE)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(CAST('nan' AS DOUBLE)) = 'UUID' THEN 'guid' WHEN TYPEOF(CAST('nan' AS DOUBLE)) LIKE '%[]' OR TYPEOF(CAST('nan' AS DOUBLE)) LIKE 'STRUCT%' OR TYPEOF(CAST('nan' AS DOUBLE)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(CAST('nan' AS DOUBLE)) = 'JSON' THEN (CASE WHEN json_type(CAST(CAST('nan' AS DOUBLE) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(CAST('nan' AS DOUBLE))) END AS gettype_realnan, CASE WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE '%[]' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'JSON' THEN (CASE WHEN json_type(CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST('' AS DOUBLE), TRY_CAST(TRY_CAST('' AS BOOLEAN) AS DOUBLE))) AS INTEGER))) END AS gettype_emptystr
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[gettype_null:String, gettype_dynnull:String, gettype_realnan:String, gettype_emptystr:String] rows=1
    - ('int', 'null', 'real', 'int')
- DuckDB: cols=[gettype_null:String, gettype_dynnull:String, gettype_realnan:String, gettype_emptystr:String] rows=1
    - ('long', '"null"', 'real', 'long')

### `agent-type-casts-coercion-0043` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(9223372036854775807, 9223372036854775807, 9.223372036854776E+18, -1, 'long') duck=('9223372036854775807', null, 9.223372036854776E+18, null, 'dictionary')

**KQL**
```kql
datatable(d:dynamic)[ dynamic(9223372036854775807), dynamic(-9223372036854775808), dynamic(1.7976931348623157e308), dynamic(5e-324) ] | extend tl = tolong(d), td = todouble(d), ti = toint(d), gt = gettype(d)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tl, COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE)) AS td, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS ti, CASE WHEN TYPEOF(d) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(d) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(d) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(d) = 'VARCHAR' THEN 'string' WHEN TYPEOF(d) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(d) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(d) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(d) = 'UUID' THEN 'guid' WHEN TYPEOF(d) LIKE '%[]' OR TYPEOF(d) LIKE 'STRUCT%' OR TYPEOF(d) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(d) = 'JSON' THEN (CASE WHEN json_type(CAST(d AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(d)) END AS gt FROM (VALUES ('9223372036854775807'::JSON), ('-0'::JSON), ('1.7976931348623157E+308'::JSON), ('5E-324'::JSON)) AS t(d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, tl:Int, td:Real, ti:Int, gt:String] rows=4
    - (9223372036854775807, 9223372036854775807, 9.223372036854776E+18, -1, 'long')
    - (-9223372036854775808, -9223372036854775808, -9.223372036854776E+18, 0, 'long')
    - (1.7976931348623157E+308, 9223372036854775807, 1.7976931348623157E+308, 2147483647, 'double')
    - (5E-324, 0, 5E-324, 0, 'double')
- DuckDB: cols=[d:String, tl:Int, td:Real, ti:Int, gt:String] rows=4
    - ('9223372036854775807', null, 9.223372036854776E+18, null, 'dictionary')
    - ('-0', 0, 0, 0, 'dictionary')
    - ('1.7976931348623157E+308', null, 1.7976931348623157E+308, null, 'dictionary')
    - ('5E-324', 0, 5E-324, 0, 'dictionary')

### `agent-type-casts-coercion-0046` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(0.3333333333333333, 0, False, True) duck=(0.3333333333333333, 0.000, True, True)

**KQL**
```kql
print decimal_div_int = todecimal("1") / 3, decimal_vs_real = todecimal("0.1") + todecimal("0.2") - todecimal("0.3"), real_vs_dec = (0.1 + 0.2 == 0.3), dec_eq = (todecimal("0.1") + todecimal("0.2") == todecimal("0.3"))
```
**Generated SQL**
```sql
SELECT TRY_CAST('1' AS DECIMAL) / 3 AS decimal_div_int, TRY_CAST('0.1' AS DECIMAL) + TRY_CAST('0.2' AS DECIMAL) - TRY_CAST('0.3' AS DECIMAL) AS decimal_vs_real, (0.1 + 0.2 = 0.3) AS real_vs_dec, (TRY_CAST('0.1' AS DECIMAL) + TRY_CAST('0.2' AS DECIMAL) = TRY_CAST('0.3' AS DECIMAL)) AS dec_eq
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[decimal_div_int:Real, decimal_vs_real:Real, real_vs_dec:Bool, dec_eq:Bool] rows=1
    - (0.3333333333333333, 0, False, True)
- DuckDB: cols=[decimal_div_int:Real, decimal_vs_real:Real, real_vs_dec:Bool, dec_eq:Bool] rows=1
    - (0.3333333333333333, 0.000, True, True)

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

*Detail:* first differing row[0]: kusto=(3.141592653589793, 'decimal', 0.3333333333333333) duck=(3.142, 'real', 0.3333333333333333)

**KQL**
```kql
print x = todecimal("3.14159265358979323846"), y = gettype(todecimal("1.5")), z = todecimal(1)/todecimal(3)
```
**Generated SQL**
```sql
SELECT TRY_CAST('3.14159265358979323846' AS DECIMAL) AS x, CASE WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST('1.5' AS DECIMAL)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL)) LIKE '%[]' OR TYPEOF(TRY_CAST('1.5' AS DECIMAL)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST('1.5' AS DECIMAL)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST('1.5' AS DECIMAL)) = 'JSON' THEN (CASE WHEN json_type(CAST(TRY_CAST('1.5' AS DECIMAL) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST('1.5' AS DECIMAL))) END AS y, TRY_CAST(1 AS DECIMAL) / TRY_CAST(3 AS DECIMAL) AS z
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Real, y:String, z:Real] rows=1
    - (3.141592653589793, 'decimal', 0.3333333333333333)
- DuckDB: cols=[x:Real, y:String, z:Real] rows=1
    - (3.142, 'real', 0.3333333333333333)

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

### `agent-type-casts-coercion-0015` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1.02:03:04.5000000, 'timespan', 24) duck=(null, 'timespan', 24)

**KQL**
```kql
print x = totimespan("1.02:03:04.5"), y = gettype(totimespan("1d")), tot = totimespan("1d")/totimespan("1h")
```
**Generated SQL**
```sql
SELECT TRY_CAST('1.02:03:04.5' AS INTERVAL) AS x, CASE WHEN TYPEOF(TRY_CAST('1d' AS INTERVAL)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST('1d' AS INTERVAL)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST('1d' AS INTERVAL)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST('1d' AS INTERVAL)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST('1d' AS INTERVAL)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST('1d' AS INTERVAL)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST('1d' AS INTERVAL)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST('1d' AS INTERVAL)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST('1d' AS INTERVAL)) LIKE '%[]' OR TYPEOF(TRY_CAST('1d' AS INTERVAL)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST('1d' AS INTERVAL)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST('1d' AS INTERVAL)) = 'JSON' THEN (CASE WHEN json_type(CAST(TRY_CAST('1d' AS INTERVAL) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST('1d' AS INTERVAL))) END AS y, (EXTRACT(EPOCH FROM (TRY_CAST('1d' AS INTERVAL))) / EXTRACT(EPOCH FROM (TRY_CAST('1h' AS INTERVAL)))) AS tot
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:TimeSpan, y:String, tot:Real] rows=1
    - (1.02:03:04.5000000, 'timespan', 24)
- DuckDB: cols=[x:TimeSpan, y:String, tot:Real] rows=1
    - (null, 'timespan', 24)

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

### `agent-type-casts-coercion-0019` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(True, 1, 1, 1, 'True', True) duck=(True, 1, 1, 1, 'true', True)

**KQL**
```kql
datatable(b:bool)[ true, false ] | extend i=toint(b), l=tolong(b), d=todouble(b), s=tostring(b), back=tobool(toint(b))
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(b AS DOUBLE), TRY_CAST(TRY_CAST(b AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS i, TRY_CAST(TRUNC(COALESCE(TRY_CAST(b AS DOUBLE), TRY_CAST(TRY_CAST(b AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS l, COALESCE(TRY_CAST(b AS DOUBLE), TRY_CAST(TRY_CAST(b AS BOOLEAN) AS DOUBLE)) AS d, TRY_CAST(b AS TEXT) AS s, TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(b AS DOUBLE), TRY_CAST(TRY_CAST(b AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS BOOLEAN) AS back FROM (VALUES (TRUE), (FALSE)) AS t(b)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[b:Bool, i:Int, l:Int, d:Real, s:String, back:Bool] rows=2
    - (True, 1, 1, 1, 'True', True)
    - (False, 0, 0, 0, 'False', False)
- DuckDB: cols=[b:Bool, i:Int, l:Int, d:Real, s:String, back:Bool] rows=2
    - (True, 1, 1, 1, 'true', True)
    - (False, 0, 0, 0, 'false', False)

### `agent-type-casts-coercion-0020` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('123', '1.5', 'True', '2020-01-01T00:00:00.0000000Z', '1.00:00:00') duck=('123', '1.5', 'true', '2020-01-01 00:00:00', '24:00:00')

**KQL**
```kql
print x = tostring(123), y = tostring(1.5), z = tostring(true), w = tostring(datetime(2020-01-01)), v = tostring(1d)
```
**Generated SQL**
```sql
SELECT TRY_CAST(123 AS TEXT) AS x, TRY_CAST(1.5 AS TEXT) AS y, TRY_CAST(TRUE AS TEXT) AS z, TRY_CAST(TIMESTAMP '2020-01-01 00:00:00' AS TEXT) AS w, TRY_CAST((86400000 * INTERVAL '1 millisecond') AS TEXT) AS v
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:String, y:String, z:String, w:String, v:String] rows=1
    - ('123', '1.5', 'True', '2020-01-01T00:00:00.0000000Z', '1.00:00:00')
- DuckDB: cols=[x:String, y:String, z:String, w:String, v:String] rows=1
    - ('123', '1.5', 'true', '2020-01-01 00:00:00', '24:00:00')

### `agent-type-casts-coercion-0021` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('1.0', '1.5', '0.30000000000000007', '100000000000.0') duck=('1.0', '1.5', '0.3', '100000000000.0')

**KQL**
```kql
print a = tostring(1.0), b = tostring(1.50000), c = tostring(0.1+0.2), d = tostring(100000000000.0)
```
**Generated SQL**
```sql
SELECT TRY_CAST(1.0 AS TEXT) AS a, TRY_CAST(1.5 AS TEXT) AS b, TRY_CAST(0.1 + 0.2 AS TEXT) AS c, TRY_CAST(100000000000.0 AS TEXT) AS d
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
SELECT CASE WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE '%[]' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER)) = 'JSON' THEN (CASE WHEN json_type(CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS INTEGER))) END AS x, CASE WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) = 'UUID' THEN 'guid' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) LIKE '%[]' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) LIKE 'STRUCT%' OR TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT)) = 'JSON' THEN (CASE WHEN json_type(CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(TRY_CAST(TRUNC(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) AS BIGINT))) END AS y, CASE WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) = 'VARCHAR' THEN 'string' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) = 'UUID' THEN 'guid' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) LIKE '%[]' OR TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) LIKE 'STRUCT%' OR TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE))) = 'JSON' THEN (CASE WHEN json_type(CAST(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE)) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(COALESCE(TRY_CAST(1 AS DOUBLE), TRY_CAST(TRY_CAST(1 AS BOOLEAN) AS DOUBLE)))) END AS z, CASE WHEN TYPEOF(1 + 1) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(1 + 1) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(1 + 1) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(1 + 1) = 'VARCHAR' THEN 'string' WHEN TYPEOF(1 + 1) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(1 + 1) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(1 + 1) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(1 + 1) = 'UUID' THEN 'guid' WHEN TYPEOF(1 + 1) LIKE '%[]' OR TYPEOF(1 + 1) LIKE 'STRUCT%' OR TYPEOF(1 + 1) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(1 + 1) = 'JSON' THEN (CASE WHEN json_type(CAST(1 + 1 AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(1 + 1)) END AS w, CASE WHEN TYPEOF(1 + 1.0) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(1 + 1.0) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(1 + 1.0) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(1 + 1.0) = 'VARCHAR' THEN 'string' WHEN TYPEOF(1 + 1.0) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(1 + 1.0) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(1 + 1.0) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(1 + 1.0) = 'UUID' THEN 'guid' WHEN TYPEOF(1 + 1.0) LIKE '%[]' OR TYPEOF(1 + 1.0) LIKE 'STRUCT%' OR TYPEOF(1 + 1.0) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(1 + 1.0) = 'JSON' THEN (CASE WHEN json_type(CAST(1 + 1.0 AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(1 + 1.0)) END AS v, CASE WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) = 'UUID' THEN 'guid' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) LIKE '%[]' OR TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) LIKE 'STRUCT%' OR TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT)) = 'JSON' THEN (CASE WHEN json_type(CAST(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(CAST(TRUNC(CAST(1 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT))) END AS u
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:String, y:String, z:String, w:String, v:String, u:String] rows=1
    - ('int', 'long', 'real', 'long', 'real', 'long')
- DuckDB: cols=[x:String, y:String, z:String, w:String, v:String, u:String] rows=1
    - ('long', 'long', 'real', 'long', 'real', 'long')

### `agent-type-casts-coercion-0024` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('null', 'long', 'string', 'array') duck=('"null"', 'dictionary', 'dictionary', 'array')

**KQL**
```kql
print x = gettype(dynamic(null)), y = gettype(dynamic(1)), z = gettype(dynamic("s")), w = gettype(dynamic([1,2]))
```
**Generated SQL**
```sql
SELECT CASE WHEN TYPEOF(NULL) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(NULL) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(NULL) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(NULL) = 'VARCHAR' THEN 'string' WHEN TYPEOF(NULL) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(NULL) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(NULL) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(NULL) = 'UUID' THEN 'guid' WHEN TYPEOF(NULL) LIKE '%[]' OR TYPEOF(NULL) LIKE 'STRUCT%' OR TYPEOF(NULL) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(NULL) = 'JSON' THEN (CASE WHEN json_type(CAST(NULL AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(NULL)) END AS x, CASE WHEN TYPEOF('1'::JSON) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF('1'::JSON) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF('1'::JSON) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF('1'::JSON) = 'VARCHAR' THEN 'string' WHEN TYPEOF('1'::JSON) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF('1'::JSON) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF('1'::JSON) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF('1'::JSON) = 'UUID' THEN 'guid' WHEN TYPEOF('1'::JSON) LIKE '%[]' OR TYPEOF('1'::JSON) LIKE 'STRUCT%' OR TYPEOF('1'::JSON) LIKE 'MAP%' THEN 'array' WHEN TYPEOF('1'::JSON) = 'JSON' THEN (CASE WHEN json_type(CAST('1'::JSON AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF('1'::JSON)) END AS y, CASE WHEN TYPEOF('"s"'::JSON) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF('"s"'::JSON) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF('"s"'::JSON) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF('"s"'::JSON) = 'VARCHAR' THEN 'string' WHEN TYPEOF('"s"'::JSON) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF('"s"'::JSON) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF('"s"'::JSON) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF('"s"'::JSON) = 'UUID' THEN 'guid' WHEN TYPEOF('"s"'::JSON) LIKE '%[]' OR TYPEOF('"s"'::JSON) LIKE 'STRUCT%' OR TYPEOF('"s"'::JSON) LIKE 'MAP%' THEN 'array' WHEN TYPEOF('"s"'::JSON) = 'JSON' THEN (CASE WHEN json_type(CAST('"s"'::JSON AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF('"s"'::JSON)) END AS z, CASE WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(LIST_VALUE(1, 2)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(LIST_VALUE(1, 2)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'UUID' THEN 'guid' WHEN TYPEOF(LIST_VALUE(1, 2)) LIKE '%[]' OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'STRUCT%' OR TYPEOF(LIST_VALUE(1, 2)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(LIST_VALUE(1, 2)) = 'JSON' THEN (CASE WHEN json_type(CAST(LIST_VALUE(1, 2) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(LIST_VALUE(1, 2))) END AS w
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:String, y:String, z:String, w:String] rows=1
    - ('null', 'long', 'string', 'array')
- DuckDB: cols=[x:String, y:String, z:String, w:String] rows=1
    - ('"null"', 'dictionary', 'dictionary', 'array')

### `agent-type-casts-coercion-0025` — MismatchRows (high)

*Sub-verdicts:* TYPE_MISMATCH[d:Dynamic|String]  
*Detail:* first differing row[0]: kusto=(1, 'long', 1, 1, '1') duck=('1', 'dictionary', 1, 1, '1')

**KQL**
```kql
datatable(d:dynamic)[ dynamic(1), dynamic(1.5), dynamic("5"), dynamic(true), dynamic(null) ] | extend gt=gettype(d), toi=toint(d), tod=todouble(d), tos=tostring(d)
```
**Generated SQL**
```sql
SELECT *, CASE WHEN TYPEOF(d) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(d) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(d) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(d) = 'VARCHAR' THEN 'string' WHEN TYPEOF(d) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(d) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(d) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(d) = 'UUID' THEN 'guid' WHEN TYPEOF(d) LIKE '%[]' OR TYPEOF(d) LIKE 'STRUCT%' OR TYPEOF(d) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(d) = 'JSON' THEN (CASE WHEN json_type(CAST(d AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(d)) END AS gt, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS toi, COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE)) AS tod, TRY_CAST(d AS TEXT) AS tos FROM (VALUES ('1'::JSON), ('1.5'::JSON), ('"5"'::JSON), ('true'::JSON), (NULL)) AS t(d)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[d:Dynamic, gt:String, toi:Int, tod:Real, tos:String] rows=5
    - (1, 'long', 1, 1, '1')
    - (1.5, 'double', 1, 1.5, '1.5')
    - ("5", 'string', 5, 5, '5')
    - (true, 'bool', 1, 1, 'true')
    - (null, 'null', null, null, '')
- DuckDB: cols=[d:String, gt:String, toi:Int, tod:Real, tos:String] rows=5
    - ('1', 'dictionary', 1, 1, '1')
    - ('1.5', 'dictionary', 1, 1.5, '1.5')
    - ('"5"', 'dictionary', 5, 5, '"5"')
    - ('true', 'dictionary', 1, 1, 'true')
    - (null, 'dictionary', null, null, null)

### `agent-type-casts-coercion-0027` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2, 2.5, 2.5, 1, 1.5, 0.5) duck=(2, 2.5, 2.5, 1, 1.5, -1.5)

**KQL**
```kql
print x = 5 / 2, y = 5.0 / 2, z = 5 / 2.0, w = 5 % 2, v = 5.5 % 2.0, u = -5.5 % 2.0
```
**Generated SQL**
```sql
SELECT CAST(TRUNC(CAST(5 AS DOUBLE) / NULLIF(2, 0)) AS BIGINT) AS x, 5.0 / 2 AS y, 5 / 2.0 AS z, (((5) % NULLIF(2, 0)) + ABS(2)) % NULLIF(2, 0) AS w, 5.5 % 2.0 AS v, (-5.5) % 2.0 AS u
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, y:Real, z:Real, w:Int, v:Real, u:Real] rows=1
    - (2, 2.5, 2.5, 1, 1.5, 0.5)
- DuckDB: cols=[x:Int, y:Real, z:Real, w:Int, v:Real, u:Real] rows=1
    - (2, 2.5, 2.5, 1, 1.5, -1.5)

### `agent-type-casts-coercion-0028` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 2, 1, 0.5, 2.5) duck=(1, 2, 1, -2.5, 2.5)

**KQL**
```kql
print a = (-5) % 3, b = 5 % (-3), c = (-5) % (-3), d = (-5.5) % 3.0, e = 5.5 % (-3.0)
```
**Generated SQL**
```sql
SELECT (((((-5))) % NULLIF(3, 0)) + ABS(3)) % NULLIF(3, 0) AS a, (((5) % NULLIF(((-3)), 0)) + ABS(((-3)))) % NULLIF(((-3)), 0) AS b, (((((-5))) % NULLIF(((-3)), 0)) + ABS(((-3)))) % NULLIF(((-3)), 0) AS c, ((-5.5)) % 3.0 AS d, 5.5 % ((-3.0)) AS e
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Int, b:Int, c:Int, d:Real, e:Real] rows=1
    - (1, 2, 1, 0.5, 2.5)
- DuckDB: cols=[a:Int, b:Int, c:Int, d:Real, e:Real] rows=1
    - (1, 2, 1, -2.5, 2.5)

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
SELECT FLOOR((5)/(2))*(2) AS x, FLOOR((5.5)/(2))*(2) AS y, FLOOR(((-5))/(2))*(2) AS z, FLOOR(((-5.5))/(2.0))*(2.0) AS w, CASE WHEN TYPEOF(FLOOR((5)/(2))*(2)) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(FLOOR((5)/(2))*(2)) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(FLOOR((5)/(2))*(2)) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(FLOOR((5)/(2))*(2)) = 'VARCHAR' THEN 'string' WHEN TYPEOF(FLOOR((5)/(2))*(2)) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(FLOOR((5)/(2))*(2)) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(FLOOR((5)/(2))*(2)) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(FLOOR((5)/(2))*(2)) = 'UUID' THEN 'guid' WHEN TYPEOF(FLOOR((5)/(2))*(2)) LIKE '%[]' OR TYPEOF(FLOOR((5)/(2))*(2)) LIKE 'STRUCT%' OR TYPEOF(FLOOR((5)/(2))*(2)) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(FLOOR((5)/(2))*(2)) = 'JSON' THEN (CASE WHEN json_type(CAST(FLOOR((5)/(2))*(2) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(FLOOR((5)/(2))*(2))) END AS gt
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
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS toi, COALESCE(TRY_CAST(d AS DOUBLE), TRY_CAST(TRY_CAST(d AS BOOLEAN) AS DOUBLE)) AS tod, TRY_CAST(d AS BOOLEAN) AS tob FROM (VALUES ('"123"'::JSON), ('"1.5"'::JSON), ('"true"'::JSON), ('"[1,2]"'::JSON), ('"null"'::JSON)) AS t(d)
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
SELECT COALESCE(TRY_CAST(0 AS TIMESTAMP), TRY_STRPTIME(CAST(0 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS x, COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS y, CASE WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) IN ('FLOAT','DOUBLE','REAL') OR TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) LIKE 'DECIMAL%' THEN 'real' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) = 'VARCHAR' THEN 'string' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) = 'BOOLEAN' THEN 'bool' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) LIKE 'INTERVAL%' THEN 'timespan' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) = 'UUID' THEN 'guid' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) LIKE '%[]' OR TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) LIKE 'STRUCT%' OR TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) LIKE 'MAP%' THEN 'array' WHEN TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))) = 'JSON' THEN (CASE WHEN json_type(CAST(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) ELSE LOWER(TYPEOF(COALESCE(TRY_CAST(1 AS TIMESTAMP), TRY_STRPTIME(CAST(1 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])))) END AS z, COALESCE(TRY_CAST(1.5 AS TIMESTAMP), TRY_STRPTIME(CAST(1.5 AS VARCHAR), ['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', '%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M'])) AS w
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:DateTime, y:DateTime, z:String, w:DateTime] rows=1
    - (0001-01-01T00:00:00.0000000Z, 0001-01-01T00:00:00.0000001Z, 'datetime', 0001-01-01T00:00:00.0000001Z)
- DuckDB: cols=[x:DateTime, y:DateTime, z:String, w:DateTime] rows=1
    - (null, null, 'datetime', null)

### `agent-type-casts-coercion-0048` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=('	456	', 456, 456) duck=('\t456\t', null, null)

**KQL**
```kql
datatable(s:string)[ "  123  ", "\t456\t", "00789", "+0", "-0", "007", "0b101" ] | extend toi=toint(s), tol=tolong(s)
```
**Generated SQL**
```sql
SELECT *, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS toi, TRY_CAST(TRUNC(COALESCE(TRY_CAST(s AS DOUBLE), TRY_CAST(TRY_CAST(s AS BOOLEAN) AS DOUBLE))) AS BIGINT) AS tol FROM (VALUES ('  123  '), ('\t456\t'), ('00789'), ('+0'), ('-0'), ('007'), ('0b101')) AS t(s)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:String, toi:Int, tol:Int] rows=7
    - ('  123  ', 123, 123)
    - ('	456	', 456, 456)
    - ('00789', 789, 789)
    - ('+0', 0, 0)
    - ('-0', 0, 0)
    - ('007', 7, 7)
    - ('0b101', null, null)
- DuckDB: cols=[s:String, toi:Int, tol:Int] rows=7
    - ('  123  ', 123, 123)
    - ('\t456\t', null, null)
    - ('00789', 789, 789)
    - ('+0', 0, 0)
    - ('-0', 0, 0)
    - ('007', 7, 7)
    - ('0b101', null, null)

### `agent-type-casts-coercion-0049` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(False, 0.30000000000000004, True) duck=(True, 0.3, True)

**KQL**
```kql
print a = 0.1 + 0.2 == 0.3, b = 0.1 + 0.2, c = todecimal(0.1) + todecimal(0.2) == todecimal(0.3)
```
**Generated SQL**
```sql
SELECT 0.1 + 0.2 = 0.3 AS a, 0.1 + 0.2 AS b, TRY_CAST(0.1 AS DECIMAL) + TRY_CAST(0.2 AS DECIMAL) = TRY_CAST(0.3 AS DECIMAL) AS c
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[a:Bool, b:Real, c:Bool] rows=1
    - (False, 0.30000000000000004, True)
- DuckDB: cols=[a:Bool, b:Real, c:Bool] rows=1
    - (True, 0.3, True)

### `agent-type-casts-coercion-0057` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('', 0, True) duck=(null, null, null)

**KQL**
```kql
print x = tostring(toint("abc")), y = strlen(tostring(toint("abc"))), z = tostring(toint("abc")) == ""
```
**Generated SQL**
```sql
SELECT TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('abc' AS DOUBLE), TRY_CAST(TRY_CAST('abc' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT) AS x, LENGTH(CAST(TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('abc' AS DOUBLE), TRY_CAST(TRY_CAST('abc' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT) AS VARCHAR)) AS y, TRY_CAST(TRY_CAST(TRUNC(COALESCE(TRY_CAST('abc' AS DOUBLE), TRY_CAST(TRY_CAST('abc' AS BOOLEAN) AS DOUBLE))) AS INTEGER) AS TEXT) = '' AS z
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:String, y:Int, z:Bool] rows=1
    - ('', 0, True)
- DuckDB: cols=[x:String, y:Int, z:Bool] rows=1
    - (null, null, null)

## Family: window-series-scan (50)

### `agent-window-series-scan-0029` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_stats_dynamic does not exist!
Did you mean "era"?

LINE 1: SELECT *, series_stats_dynamic(s) AS e FROM (SELECT LIST(COALESCE...
                  ^

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2020-01-01),10, datetime(2020-01-02),20 ] | make-series s = sum(v) default = 0 on t from datetime(2020-01-01) to datetime(2020-01-03) step 1d | extend e = series_stats_dynamic(s)
```
**Generated SQL**
```sql
SELECT *, series_stats_dynamic(s) AS e FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-03 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2020-01-02 00:00:00', CAST(20 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
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
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_stats_dynamic does not exist!
Did you mean "era"?

LINE 1: SELECT *, series_stats_dynamic(s) AS e FROM (SELECT LIST(COALESCE...
                  ^

### `agent-window-series-scan-0010` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_fill_forward does not exist!
Did you mean "jaro_winkler_similarity"?

LINE 1: SELECT *, series_fill_forward(s) AS f FROM (SELECT LIST(COALESCE(s_val...
                  ^

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-03),30, datetime(2021-01-07),70 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-08) step 2d | extend f = series_fill_forward(s)
```
**Generated SQL**
```sql
SELECT *, series_fill_forward(s) AS f FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-08 00:00:00', 172800000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/172800000)*172800000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-03 00:00:00', CAST(30 AS BIGINT)), (TIMESTAMP '2021-01-07 00:00:00', CAST(70 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic, f:Dynamic] rows=1
    - ([
  10,
  30,
  0,
  70
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z",
  "2021-01-07T00:00:00.0000000Z"
], [
  10,
  30,
  0,
  70
])
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_fill_forward does not exist!
Did you mean "jaro_winkler_similarity"?

LINE 1: SELECT *, series_fill_forward(s) AS f FROM (SELECT LIST(COALESCE(s_val...
                  ^

### `agent-window-series-scan-0011` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_fill_linear does not exist!
Did you mean "json_serialize_plan"?

LINE 1: SELECT *, series_fill_linear(s) AS lf FROM (SELECT LIST(COALESCE(s_val...
                  ^

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-02),0, datetime(2021-01-04),40 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-05) step 1d | extend lf = series_fill_linear(s)
```
**Generated SQL**
```sql
SELECT *, series_fill_linear(s) AS lf FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-05 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-02 00:00:00', CAST(0 AS BIGINT)), (TIMESTAMP '2021-01-04 00:00:00', CAST(40 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic, lf:Dynamic] rows=1
    - ([
  10,
  0,
  0,
  40
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z"
], [
  10,
  0,
  0,
  40
])
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_fill_linear does not exist!
Did you mean "json_serialize_plan"?

LINE 1: SELECT *, series_fill_linear(s) AS lf FROM (SELECT LIST(COALESCE(s_val...
                  ^

### `agent-window-series-scan-0015` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_stats does not exist!
Did you mean "starts_with"?

LINE 1: SELECT *, series_stats(s) AS st FROM (SELECT LIST(COALESCE(s_val,...
                  ^

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-05),50 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-06) step 1d | extend st = series_stats(s)
```
**Generated SQL**
```sql
SELECT *, series_stats(s) AS st FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-06 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-05 00:00:00', CAST(50 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
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
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_stats does not exist!
Did you mean "starts_with"?

LINE 1: SELECT *, series_stats(s) AS st FROM (SELECT LIST(COALESCE(s_val,...
                  ^

### `agent-window-series-scan-0016` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_fit_line does not exist!
Did you mean "reservoir_quantile"?

LINE 1: SELECT *, series_fit_line(s) AS fit FROM (SELECT LIST(COALESCE(s_val...
                  ^

**KQL**
```kql
datatable(t:datetime, v:real)[ datetime(2021-01-01),1.0, datetime(2021-01-02),2.0, datetime(2021-01-03),4.0, datetime(2021-01-04),8.0 ] | make-series s = avg(v) default = 0.0 on t from datetime(2021-01-01) to datetime(2021-01-05) step 1d | extend fit = series_fit_line(s)
```
**Generated SQL**
```sql
SELECT *, series_fit_line(s) AS fit FROM (SELECT LIST(COALESCE(s_val, 0.0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-05 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(AVG(v), 'nan'::DOUBLE) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(1.0 AS DOUBLE)), (TIMESTAMP '2021-01-02 00:00:00', CAST(2.0 AS DOUBLE)), (TIMESTAMP '2021-01-03 00:00:00', CAST(4.0 AS DOUBLE)), (TIMESTAMP '2021-01-04 00:00:00', CAST(8.0 AS DOUBLE))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
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
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_fit_line does not exist!
Did you mean "reservoir_quantile"?

LINE 1: SELECT *, series_fit_line(s) AS fit FROM (SELECT LIST(COALESCE(s_val...
                  ^

### `agent-window-series-scan-0039` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_iir does not exist!
Did you mean "list_reverse"?

LINE 1: SELECT *, series_iir(s, LIST_VALUE(1), LIST_VALUE(1, (-1))) AS cums...
                  ^

**KQL**
```kql
datatable(t:long, v:long, g:string)[ 1,10,"a", 3,30,"a", 1,5,"b", 5,50,"b" ] | make-series s = sum(v) default = 0 on t from 1 to 6 step 2 by g | extend cums = series_iir(s, dynamic([1]), dynamic([1,-1]))
```
**Generated SQL**
```sql
SELECT *, series_iir(s, LIST_VALUE(1), LIST_VALUE(1, (-1))) AS cums FROM (SELECT _axis.g, LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT _g.*, _t._ts FROM (SELECT DISTINCT g FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'a'), (CAST(3 AS BIGINT), CAST(30 AS BIGINT), 'a'), (CAST(1 AS BIGINT), CAST(5 AS BIGINT), 'b'), (CAST(5 AS BIGINT), CAST(50 AS BIGINT), 'b')) AS t(t, v, g)) AS _g CROSS JOIN (SELECT (1) + _i*(2) AS _ts FROM range(0, CAST(CEIL(((6) - (1))/(2)) AS BIGINT)) AS _r(_i)) AS _t) AS _axis LEFT JOIN (SELECT g, ((1) + FLOOR(((t) - (1))/(2))*(2)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'a'), (CAST(3 AS BIGINT), CAST(30 AS BIGINT), 'a'), (CAST(1 AS BIGINT), CAST(5 AS BIGINT), 'b'), (CAST(5 AS BIGINT), CAST(50 AS BIGINT), 'b')) AS t(t, v, g) GROUP BY g, _bucket) AS _data ON _axis.g = _data.g AND _axis._ts = _data._bucket GROUP BY _axis.g)
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
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_iir does not exist!
Did you mean "list_reverse"?

LINE 1: SELECT *, series_iir(s, LIST_VALUE(1), LIST_VALUE(1, (-1))) AS cums...
                  ^

### `agent-window-series-scan-0043` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_pearson_correlation does not exist!
Did you mean "sin"?

LINE 1: SELECT *, series_pearson_correlation(s, s) AS pf FROM (SELECT LIST...
                  ^

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),0, datetime(2021-01-02),0, datetime(2021-01-03),5 ] | make-series s = max(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-04) step 1d | extend pf = series_pearson_correlation(s, s)
```
**Generated SQL**
```sql
SELECT *, series_pearson_correlation(s, s) AS pf FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-04 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(MAX(v) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(0 AS BIGINT)), (TIMESTAMP '2021-01-02 00:00:00', CAST(0 AS BIGINT)), (TIMESTAMP '2021-01-03 00:00:00', CAST(5 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic, pf:Real] rows=1
    - ([
  0,
  0,
  5
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z"
], 1.0000000000000002)
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_pearson_correlation does not exist!
Did you mean "sin"?

LINE 1: SELECT *, series_pearson_correlation(s, s) AS pf FROM (SELECT LIST...
                  ^

### `agent-window-series-scan-0014` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_fill_forward does not exist!
Did you mean "jaro_winkler_similarity"?

LINE 1: SELECT *, series_fill_forward(s) AS ff, series_fill_backward(s) AS...
                  ^

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-03),30, datetime(2021-01-08),80 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-10) step 2d | extend ff = series_fill_forward(s), fb = series_fill_backward(s), lin = series_fill_linear(s)
```
**Generated SQL**
```sql
SELECT *, series_fill_forward(s) AS ff, series_fill_backward(s) AS fb, series_fill_linear(s) AS lin FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-10 00:00:00', 172800000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/172800000)*172800000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-03 00:00:00', CAST(30 AS BIGINT)), (TIMESTAMP '2021-01-08 00:00:00', CAST(80 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic, ff:Dynamic, fb:Dynamic, lin:Dynamic] rows=1
    - ([
  10,
  30,
  0,
  80,
  0
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-05T00:00:00.0000000Z",
  "2021-01-07T00:00:00.0000000Z",
  "2021-01-09T00:00:00.0000000Z"
], [
  10,
  30,
  0,
  80,
  0
], [
  10,
  30,
  0,
  80,
  0
], [
  10,
  30,
  0,
  80,
  0
])
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_fill_forward does not exist!
Did you mean "jaro_winkler_similarity"?

LINE 1: SELECT *, series_fill_forward(s) AS ff, series_fill_backward(s) AS...
                  ^

### `agent-window-series-scan-0015` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_subtract does not exist!
Did you mean "session_user"?

LINE 1: SELECT *, series_subtract(s, series_fill_const(s, 5)) AS d, series_mu...
                  ^

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-02),20, datetime(2021-01-03),30, datetime(2021-01-04),40 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-05) step 1d | extend d = series_subtract(s, series_fill_const(s, 5)), m = series_multiply(s, s)
```
**Generated SQL**
```sql
SELECT *, series_subtract(s, series_fill_const(s, 5)) AS d, series_multiply(s, s) AS m FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-05 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-02 00:00:00', CAST(20 AS BIGINT)), (TIMESTAMP '2021-01-03 00:00:00', CAST(30 AS BIGINT)), (TIMESTAMP '2021-01-04 00:00:00', CAST(40 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic, d:Dynamic, m:Dynamic] rows=1
    - ([
  10,
  20,
  30,
  40
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z"
], [
  0,
  0,
  0,
  0
], [
  100,
  400,
  900,
  1600
])
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_subtract does not exist!
Did you mean "session_user"?

LINE 1: SELECT *, series_subtract(s, series_fill_const(s, 5)) AS d, series_mu...
                  ^

### `agent-window-series-scan-0019` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_fit_line_dynamic does not exist!
Did you mean "setseed"?

LINE 1: ...) AS rsq, json_extract(fl, '$.slope') AS slope FROM (SELECT *, series_fit_line_dynamic(s) AS fl FROM (SELECT LIST(COALESCE...
                                                                          ^

**KQL**
```kql
datatable(t:long, v:real)[ 1,1.0, 2,2.0, 3,3.0, 4,4.0, 5,5.0 ] | make-series s = avg(v) default = 0.0 on t from 1 to 6 step 1 | extend fl = series_fit_line_dynamic(s) | project rsq = fl.rsquare, slope = fl.slope
```
**Generated SQL**
```sql
SELECT json_extract(fl, '$.rsquare') AS rsq, json_extract(fl, '$.slope') AS slope FROM (SELECT *, series_fit_line_dynamic(s) AS fl FROM (SELECT LIST(COALESCE(s_val, 0.0) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT (1) + _i*(1) AS _ts FROM range(0, CAST(CEIL(((6) - (1))/(1)) AS BIGINT)) AS _r(_i)) AS _axis LEFT JOIN (SELECT ((1) + FLOOR(((t) - (1))/(1))*(1)) AS _bucket, CAST(COALESCE(AVG(v), 'nan'::DOUBLE) AS DOUBLE) AS s_val FROM (VALUES (CAST(1 AS BIGINT), CAST(1.0 AS DOUBLE)), (CAST(2 AS BIGINT), CAST(2.0 AS DOUBLE)), (CAST(3 AS BIGINT), CAST(3.0 AS DOUBLE)), (CAST(4 AS BIGINT), CAST(4.0 AS DOUBLE)), (CAST(5 AS BIGINT), CAST(5.0 AS DOUBLE))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket))
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[rsq:Dynamic, slope:Dynamic] rows=1
    - (1.0, 0.9999999999999999)
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_fit_line_dynamic does not exist!
Did you mean "setseed"?

LINE 1: ...) AS rsq, json_extract(fl, '$.slope') AS slope FROM (SELECT *, series_fit_line_dynamic(s) AS fl FROM (SELECT LIST(COALESCE...
                                                                          ^

### `agent-window-series-scan-0020` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_fir does not exist!
Did you mean "list_reverse"?

LINE 1: SELECT *, series_fir(s, LIST_VALUE(1, (-1)), FALSE, FALSE) AS dlt...
                  ^

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),5, datetime(2021-01-02),15, datetime(2021-01-03),10, datetime(2021-01-04),20 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-05) step 1d | extend dlt = series_fir(s, dynamic([1,-1]), false, false)
```
**Generated SQL**
```sql
SELECT *, series_fir(s, LIST_VALUE(1, (-1)), FALSE, FALSE) AS dlt FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-05 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(5 AS BIGINT)), (TIMESTAMP '2021-01-02 00:00:00', CAST(15 AS BIGINT)), (TIMESTAMP '2021-01-03 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-04 00:00:00', CAST(20 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic, dlt:Dynamic] rows=1
    - ([
  5,
  15,
  10,
  20
], [
  "2021-01-01T00:00:00.0000000Z",
  "2021-01-02T00:00:00.0000000Z",
  "2021-01-03T00:00:00.0000000Z",
  "2021-01-04T00:00:00.0000000Z"
], [
  5,
  10,
  -5,
  10
])
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_fir does not exist!
Did you mean "list_reverse"?

LINE 1: SELECT *, series_fir(s, LIST_VALUE(1, (-1)), FALSE, FALSE) AS dlt...
                  ^

### `agent-window-series-scan-0021` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_fill_forward does not exist!
Did you mean "jaro_winkler_similarity"?

LINE 1: ... (s, ff), u0.value AS s, u1.value AS ff FROM (SELECT *, series_fill_forward(s) AS ff FROM (SELECT LIST(COALESCE...
                                                                   ^

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-04),40, datetime(2021-01-06),60 ] | make-series s = sum(v) default = long(null) on t from datetime(2021-01-01) to datetime(2021-01-07) step 1d | extend ff = series_fill_forward(s) | mv-expand s to typeof(long), ff to typeof(long)
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (s, ff), u0.value AS s, u1.value AS ff FROM (SELECT *, series_fill_forward(s) AS ff FROM (SELECT LIST(COALESCE(s_val, CAST(NULL AS BIGINT)) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-07 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-04 00:00:00', CAST(40 AS BIGINT)), (TIMESTAMP '2021-01-06 00:00:00', CAST(60 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)) AS t CROSS JOIN UNNEST(t.s) AS u0(value) CROSS JOIN UNNEST(t.ff) AS u1(value)
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
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_fill_forward does not exist!
Did you mean "jaro_winkler_similarity"?

LINE 1: ... (s, ff), u0.value AS s, u1.value AS ff FROM (SELECT *, series_fill_forward(s) AS ff FROM (SELECT LIST(COALESCE...
                                                                   ^

### `agent-window-series-scan-0022` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_greater does not exist!
Did you mean "str_split_regex"?

LINE 1: SELECT *, LIST_TRANSFORM(LIST_ZIP(series_greater(s, 15), s, (-1)), x -> CASE WHEN x[1] THEN...
                                          ^

**KQL**
```kql
datatable(t:long, v:long)[ 1,10, 2,20, 3,30 ] | make-series s = sum(v) default = 0 on t from 1 to 10 step 3 | extend gt15 = array_iff(series_greater(s, 15), s, -1)
```
**Generated SQL**
```sql
SELECT *, LIST_TRANSFORM(LIST_ZIP(series_greater(s, 15), s, (-1)), x -> CASE WHEN x[1] THEN x[2] ELSE x[3] END) AS gt15 FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT (1) + _i*(3) AS _ts FROM range(0, CAST(CEIL(((10) - (1))/(3)) AS BIGINT)) AS _r(_i)) AS _axis LEFT JOIN (SELECT ((1) + FLOOR(((t) - (1))/(3))*(3)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(2 AS BIGINT), CAST(20 AS BIGINT)), (CAST(3 AS BIGINT), CAST(30 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Dynamic, t:Dynamic, gt15:Dynamic] rows=1
    - ([
  60,
  0,
  0
], [
  1,
  4,
  7
], [
  60,
  -1,
  -1
])
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_greater does not exist!
Did you mean "str_split_regex"?

LINE 1: SELECT *, LIST_TRANSFORM(LIST_ZIP(series_greater(s, 15), s, (-1)), x -> CASE WHEN x[1] THEN...
                                          ^

### `agent-window-series-scan-0040` — SqlExecError (highest)

*Detail:* INTERNAL Error: Failed to cast expression to type - expression type mismatch
This error signals an assertion failure within DuckDB. This usually occurs due to unexpected conditions or errors in the program's logic.
For more information, see https://duckdb.org/docs/stable/dev/internal_errors

**KQL**
```kql
datatable(t:long, v:long)[ 2,20, 4,40, 6,60 ] | make-series s = sum(v) default = -7 on t from 1 to 8 step 1 | mv-expand idx = range(0, array_length(s)-1, 1) to typeof(long), s to typeof(long) | sort by idx asc
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (s), u0.value AS idx, u1.value AS s FROM (SELECT LIST(COALESCE(s_val, (-7)) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT (1) + _i*(1) AS _ts FROM range(0, CAST(CEIL(((8) - (1))/(1)) AS BIGINT)) AS _r(_i)) AS _axis LEFT JOIN (SELECT ((1) + FLOOR(((t) - (1))/(1))*(1)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (CAST(2 AS BIGINT), CAST(20 AS BIGINT)), (CAST(4 AS BIGINT), CAST(40 AS BIGINT)), (CAST(6 AS BIGINT), CAST(60 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket) AS t CROSS JOIN UNNEST(GENERATE_SERIES(0, LEN(s) - 1, 1)) AS u0(value) CROSS JOIN UNNEST(t.s) AS u1(value) ORDER BY idx ASC NULLS FIRST
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
- DuckDB: ERROR — INTERNAL Error: Failed to cast expression to type - expression type mismatch
This error signals an assertion failure within DuckDB. This usually occurs due to unexpected conditions or errors in the program's logic.
For more information, see https://duckdb.org/docs/stable/dev/internal_errors

### `agent-window-series-scan-0041` — SqlExecError (highest)

*Detail:* Catalog Error: Scalar Function with name series_iir does not exist!
Did you mean "list_reverse"?

LINE 1: ... (s, cum), u0.value AS s, u1.value AS cum FROM (SELECT *, series_iir(s, LIST_VALUE(1), LIST_VALUE(1, (-1))) AS cum...
                                                                     ^

**KQL**
```kql
datatable(t:long, v:long, g:string)[ 1,1,"a", 2,2,"a", 1,10,"b", 2,20,"b", 3,30,"b" ] | make-series s = sum(v) default = 0 on t from 1 to 4 step 1 by g | extend cum = series_iir(s, dynamic([1]), dynamic([1,-1])) | mv-expand s to typeof(long), cum to typeof(long) | sort by g asc
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (s, cum), u0.value AS s, u1.value AS cum FROM (SELECT *, series_iir(s, LIST_VALUE(1), LIST_VALUE(1, (-1))) AS cum FROM (SELECT _axis.g, LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT _g.*, _t._ts FROM (SELECT DISTINCT g FROM (VALUES (CAST(1 AS BIGINT), CAST(1 AS BIGINT), 'a'), (CAST(2 AS BIGINT), CAST(2 AS BIGINT), 'a'), (CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'b'), (CAST(2 AS BIGINT), CAST(20 AS BIGINT), 'b'), (CAST(3 AS BIGINT), CAST(30 AS BIGINT), 'b')) AS t(t, v, g)) AS _g CROSS JOIN (SELECT (1) + _i*(1) AS _ts FROM range(0, CAST(CEIL(((4) - (1))/(1)) AS BIGINT)) AS _r(_i)) AS _t) AS _axis LEFT JOIN (SELECT g, ((1) + FLOOR(((t) - (1))/(1))*(1)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (CAST(1 AS BIGINT), CAST(1 AS BIGINT), 'a'), (CAST(2 AS BIGINT), CAST(2 AS BIGINT), 'a'), (CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'b'), (CAST(2 AS BIGINT), CAST(20 AS BIGINT), 'b'), (CAST(3 AS BIGINT), CAST(30 AS BIGINT), 'b')) AS t(t, v, g) GROUP BY g, _bucket) AS _data ON _axis.g = _data.g AND _axis._ts = _data._bucket GROUP BY _axis.g)) AS t CROSS JOIN UNNEST(t.s) AS u0(value) CROSS JOIN UNNEST(t.cum) AS u1(value) ORDER BY g ASC NULLS FIRST
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
- DuckDB: ERROR — Catalog Error: Scalar Function with name series_iir does not exist!
Did you mean "list_reverse"?

LINE 1: ... (s, cum), u0.value AS s, u1.value AS cum FROM (SELECT *, series_iir(s, LIST_VALUE(1), LIST_VALUE(1, (-1))) AS cum...
                                                                     ^

### `agent-window-series-scan-0001` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 20, 100) duck=(1, 20, 1)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30 ] | sort by k asc, v desc | serialize rn = row_number(100)
```
**Generated SQL**
```sql
SELECT *, ROW_NUMBER() OVER (ORDER BY k ASC NULLS FIRST, v DESC NULLS LAST) AS rn FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v) ORDER BY k ASC NULLS FIRST, v DESC NULLS LAST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, v:Int, rn:Int] rows=4
    - (1, 20, 100)
    - (1, 10, 101)
    - (2, 30, 102)
    - (2, 30, 103)
- DuckDB: cols=[k:Int, v:Int, rn:Int] rows=4
    - (1, 20, 1)
    - (1, 10, 2)
    - (2, 30, 3)
    - (2, 30, 4)

### `agent-window-series-scan-0002` — MismatchRows (high)

*Detail:* first differing row[2]: kusto=(2, 30, 1) duck=(2, 30, 3)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30 ] | sort by k asc | serialize rn = row_number(1, k != prev(k))
```
**Generated SQL**
```sql
SELECT *, ROW_NUMBER() OVER (ORDER BY k ASC NULLS FIRST) AS rn FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT))) AS t(k, v) ORDER BY k ASC NULLS FIRST
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

### `agent-window-series-scan-0004` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a', 1, null, 5) duck=('a', 1, null, 2)

**KQL**
```kql
datatable(g:string, v:long)[ "a",1, "a",2, "b",5, "b",7, "b",9 ] | sort by g asc, v asc | serialize p2 = prev(v, 2), n2 = next(v, 2)
```
**Generated SQL**
```sql
SELECT *, LAG(v) OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS p2, LEAD(v) OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS n2 FROM (VALUES ('a', CAST(1 AS BIGINT)), ('a', CAST(2 AS BIGINT)), ('b', CAST(5 AS BIGINT)), ('b', CAST(7 AS BIGINT)), ('b', CAST(9 AS BIGINT))) AS t(g, v) ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, v:Int, p2:Int, n2:Int] rows=5
    - ('a', 1, null, 5)
    - ('a', 2, null, 7)
    - ('b', 5, 1, 9)
    - ('b', 7, 2, null)
    - ('b', 9, 5, null)
- DuckDB: cols=[g:String, v:Int, p2:Int, n2:Int] rows=5
    - ('a', 1, null, 2)
    - ('a', 2, 1, 5)
    - ('b', 5, 2, 7)
    - ('b', 7, 5, 9)
    - ('b', 9, 7, null)

### `agent-window-series-scan-0005` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=('a', 1, -999, 2) duck=('a', 1, null, 2)

**KQL**
```kql
datatable(g:string, v:long)[ "a",1, "a",2, "b",5, "b",7 ] | sort by g asc, v asc | serialize p = prev(v, 1, -999), n = next(v, 1, -999)
```
**Generated SQL**
```sql
SELECT *, LAG(v) OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS p, LEAD(v) OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS n FROM (VALUES ('a', CAST(1 AS BIGINT)), ('a', CAST(2 AS BIGINT)), ('b', CAST(5 AS BIGINT)), ('b', CAST(7 AS BIGINT))) AS t(g, v) ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, v:Int, p:Int, n:Int] rows=4
    - ('a', 1, -999, 2)
    - ('a', 2, 1, 5)
    - ('b', 5, 2, 7)
    - ('b', 7, 5, -999)
- DuckDB: cols=[g:String, v:Int, p:Int, n:Int] rows=4
    - ('a', 1, null, 2)
    - ('a', 2, 1, 5)
    - ('b', 5, 2, 7)
    - ('b', 7, 5, null)

### `agent-window-series-scan-0008` — MismatchRows (high)

*Detail:* first differing row[2]: kusto=('b', 1, 1) duck=('b', 1, 9)

**KQL**
```kql
datatable(g:string, x:long)[ "a",5, "a",3, "b",8, "b",1, "b",9 ] | sort by g asc, x asc | serialize cs = row_cumsum(x, g != prev(g))
```
**Generated SQL**
```sql
SELECT *, SUM(x) OVER (ORDER BY g ASC NULLS FIRST, x ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS cs FROM (VALUES ('a', CAST(5 AS BIGINT)), ('a', CAST(3 AS BIGINT)), ('b', CAST(8 AS BIGINT)), ('b', CAST(1 AS BIGINT)), ('b', CAST(9 AS BIGINT))) AS t(g, x) ORDER BY g ASC NULLS FIRST, x ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, x:Int, cs:Int] rows=5
    - ('a', 3, 3)
    - ('a', 5, 8)
    - ('b', 1, 1)
    - ('b', 8, 9)
    - ('b', 9, 18)
- DuckDB: cols=[g:String, x:Int, cs:Int] rows=5
    - ('a', 3, 3)
    - ('a', 5, 8)
    - ('b', 1, 9)
    - ('b', 8, 17)
    - ('b', 9, 26)

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

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[t:Dynamic|Real]  
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
SELECT t.* EXCLUDE (s), u.value AS s FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-04 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2020-01-02 00:00:00', CAST(20 AS BIGINT)), (TIMESTAMP '2020-01-03 00:00:00', CAST(30 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket) AS t CROSS JOIN UNNEST(t.s) AS u(value)
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
- DuckDB: cols=[t:Unknown, s:Real] rows=3
    - (["2020-01-01T00:00:00.0000000Z","2020-01-02T00:00:00.0000000Z","2020-01-03T00:00:00.0000000Z"], 10)
    - (["2020-01-01T00:00:00.0000000Z","2020-01-02T00:00:00.0000000Z","2020-01-03T00:00:00.0000000Z"], 20)
    - (["2020-01-01T00:00:00.0000000Z","2020-01-02T00:00:00.0000000Z","2020-01-03T00:00:00.0000000Z"], 30)

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

### `agent-window-series-scan-0000` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 10, 1, null, 30, 10) duck=(1, 10, 1, null, 20, 10)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30, 3,5 ] | sort by k asc, v asc | serialize rn = row_number(), p2 = prev(v,2), n2 = next(v,2), cs = row_cumsum(v, k != prev(k))
```
**Generated SQL**
```sql
SELECT *, ROW_NUMBER() OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST) AS rn, LAG(v) OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST) AS p2, LEAD(v) OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST) AS n2, SUM(v) OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS cs FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(3 AS BIGINT), CAST(5 AS BIGINT))) AS t(k, v) ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, v:Int, rn:Int, p2:Int, n2:Int, cs:Int] rows=5
    - (1, 10, 1, null, 30, 10)
    - (1, 20, 2, null, 30, 30)
    - (2, 30, 3, 10, 5, 30)
    - (2, 30, 4, 20, null, 60)
    - (3, 5, 5, 30, null, 5)
- DuckDB: cols=[k:Int, v:Int, rn:Int, p2:Int, n2:Int, cs:Int] rows=5
    - (1, 10, 1, null, 20, 10)
    - (1, 20, 2, 10, 30, 30)
    - (2, 30, 3, 20, 30, 60)
    - (2, 30, 4, 30, 5, 90)
    - (3, 5, 5, 30, null, 95)

### `agent-window-series-scan-0003` — MismatchRows (high)

*Detail:* first differing row[2]: kusto=('b', 10, 10) duck=('b', 10, 4.666666666666667)

**KQL**
```kql
datatable(g:string, v:real)[ "a",1.5, "a",2.5, "b",10.0, "b",20.0, "b",30.0 ] | sort by g asc, v asc | serialize avgsofar = row_cumsum(v, g != prev(g)) / row_number(1, g != prev(g))
```
**Generated SQL**
```sql
SELECT *, SUM(v) OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) / ROW_NUMBER() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS avgsofar FROM (VALUES ('a', CAST(1.5 AS DOUBLE)), ('a', CAST(2.5 AS DOUBLE)), ('b', CAST(10.0 AS DOUBLE)), ('b', CAST(20.0 AS DOUBLE)), ('b', CAST(30.0 AS DOUBLE))) AS t(g, v) ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST
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
    - ('b', 10, 4.666666666666667)
    - ('b', 20, 8.5)
    - ('b', 30, 12.8)

### `agent-window-series-scan-0013` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[s:Int|String], TYPE_MISMATCH[t:DateTime|Real]  
*Detail:* row count: kusto=3 duck=9

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),10, datetime(2021-01-02),20, datetime(2021-01-03),30 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-04) step 1d | mv-expand t to typeof(datetime), s to typeof(long)
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (t, s), u0.value AS t, u1.value AS s FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-04 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT)), (TIMESTAMP '2021-01-02 00:00:00', CAST(20 AS BIGINT)), (TIMESTAMP '2021-01-03 00:00:00', CAST(30 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket) AS t CROSS JOIN UNNEST(t.t) AS u0(value) CROSS JOIN UNNEST(t.s) AS u1(value)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, t:DateTime] rows=3
    - (10, 2021-01-01T00:00:00.0000000Z)
    - (20, 2021-01-02T00:00:00.0000000Z)
    - (30, 2021-01-03T00:00:00.0000000Z)
- DuckDB: cols=[t:String, s:Real] rows=9
    - ('2021-01-01T00:00:00.0000000Z', 10)
    - ('2021-01-01T00:00:00.0000000Z', 20)
    - ('2021-01-01T00:00:00.0000000Z', 30)
    - ('2021-01-02T00:00:00.0000000Z', 10)
    - ('2021-01-02T00:00:00.0000000Z', 20)
    - ('2021-01-02T00:00:00.0000000Z', 30)
    - ('2021-01-03T00:00:00.0000000Z', 10)
    - ('2021-01-03T00:00:00.0000000Z', 20)

### `agent-window-series-scan-0014` — MismatchRows (high)

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[t:Dynamic|Real]  
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
SELECT t.* EXCLUDE (s), u.value AS s FROM (SELECT LIST(COALESCE(s_val, (-1)) ORDER BY _ts) AS s, LIST(_axis._ts ORDER BY _axis._ts) AS t FROM (SELECT (0) + _i*(2) AS _ts FROM range(0, CAST(CEIL(((8) - (0))/(2)) AS BIGINT)) AS _r(_i)) AS _axis LEFT JOIN (SELECT ((0) + FLOOR(((t) - (0))/(2))*(2)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (CAST(0 AS BIGINT), CAST(1 AS BIGINT)), (CAST(2 AS BIGINT), CAST(3 AS BIGINT)), (CAST(4 AS BIGINT), CAST(5 AS BIGINT)), (CAST(6 AS BIGINT), CAST(7 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket) AS t CROSS JOIN UNNEST(t.s) AS u(value)
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
- DuckDB: cols=[t:Unknown, s:Real] rows=4
    - ([0,2,4,6], 1)
    - ([0,2,4,6], 3)
    - ([0,2,4,6], 5)
    - ([0,2,4,6], 7)

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

### `agent-window-series-scan-0033` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(2021-01-01T00:00:00.0000000Z, 5, null, 5, 5) duck=(2021-01-01T00:00:00.0000000Z, 5, null, 5, null)

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01),5, datetime(2021-01-02),3, datetime(2021-01-03),8 ] | sort by t asc | serialize dt = t - prev(t), cs = row_cumsum(v), rate = v - prev(v, 1, 0)
```
**Generated SQL**
```sql
SELECT *, t - LAG(t) OVER (ORDER BY t ASC NULLS FIRST) AS dt, SUM(v) OVER (ORDER BY t ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS cs, v - LAG(v) OVER (ORDER BY t ASC NULLS FIRST) AS rate FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(5 AS BIGINT)), (TIMESTAMP '2021-01-02 00:00:00', CAST(3 AS BIGINT)), (TIMESTAMP '2021-01-03 00:00:00', CAST(8 AS BIGINT))) AS t(t, v) ORDER BY t ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:DateTime, v:Int, dt:TimeSpan, cs:Int, rate:Int] rows=3
    - (2021-01-01T00:00:00.0000000Z, 5, null, 5, 5)
    - (2021-01-02T00:00:00.0000000Z, 3, 1.00:00:00, 8, -2)
    - (2021-01-03T00:00:00.0000000Z, 8, 1.00:00:00, 16, 5)
- DuckDB: cols=[t:DateTime, v:Int, dt:TimeSpan, cs:Int, rate:Int] rows=3
    - (2021-01-01T00:00:00.0000000Z, 5, null, 5, null)
    - (2021-01-02T00:00:00.0000000Z, 3, 1.00:00:00, 8, -2)
    - (2021-01-03T00:00:00.0000000Z, 8, 1.00:00:00, 16, 5)

### `agent-window-series-scan-0035` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, -1, -1) duck=(1, null, 2)

**KQL**
```kql
datatable(x:long)[ 1,2,3 ] | sort by x asc | serialize p3 = prev(x, 3, -1), n3 = next(x, 3, -1)
```
**Generated SQL**
```sql
SELECT *, LAG(x) OVER (ORDER BY x ASC NULLS FIRST) AS p3, LEAD(x) OVER (ORDER BY x ASC NULLS FIRST) AS n3 FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT))) AS t(x) ORDER BY x ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[x:Int, p3:Int, n3:Int] rows=3
    - (1, -1, -1)
    - (2, -1, -1)
    - (3, -1, -1)
- DuckDB: cols=[x:Int, p3:Int, n3:Int] rows=3
    - (1, null, 2)
    - (2, 1, 3)
    - (3, 2, null)

### `agent-window-series-scan-0036` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 10, 0, 0, 10) duck=(1, 10, null, null, null)

**KQL**
```kql
datatable(t:long, v:long)[ 1,10, 2,20, 3,30, 4,40, 5,50 ] | sort by t asc | serialize p1 = prev(v,1,0), p2 = prev(v,2,0) | extend w = v + p1 + p2
```
**Generated SQL**
```sql
SELECT *, v + p1 + p2 AS w FROM (SELECT *, LAG(v) OVER (ORDER BY t ASC NULLS FIRST) AS p1, LAG(v) OVER (ORDER BY t ASC NULLS FIRST) AS p2 FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(2 AS BIGINT), CAST(20 AS BIGINT)), (CAST(3 AS BIGINT), CAST(30 AS BIGINT)), (CAST(4 AS BIGINT), CAST(40 AS BIGINT)), (CAST(5 AS BIGINT), CAST(50 AS BIGINT))) AS t(t, v) ORDER BY t ASC NULLS FIRST)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:Int, v:Int, p1:Int, p2:Int, w:Int] rows=5
    - (1, 10, 0, 0, 10)
    - (2, 20, 10, 0, 30)
    - (3, 30, 20, 10, 60)
    - (4, 40, 30, 20, 90)
    - (5, 50, 40, 30, 120)
- DuckDB: cols=[t:Int, v:Int, p1:Int, p2:Int, w:Int] rows=5
    - (1, 10, null, null, null)
    - (2, 20, 10, 10, 40)
    - (3, 30, 20, 20, 70)
    - (4, 40, 30, 30, 100)
    - (5, 50, 40, 40, 130)

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

### `agent-window-series-scan-0042` — MismatchRows (high)

*Detail:* first differing row[3]: kusto=('b', 4, 4, 4) duck=('b', 4, 4, 7)

**KQL**
```kql
datatable(g:string, x:long)[ "a",3, "a",1, "a",2, "b",6, "b",4, "b",5 ] | sort by g asc, x asc | serialize delta = iff(g != prev(g), x, x - prev(x)) | serialize cm = row_cumsum(delta, g != prev(g))
```
**Generated SQL**
```sql
SELECT *, SUM(delta) OVER (ORDER BY g ASC NULLS FIRST, x ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS cm FROM (SELECT *, CASE WHEN g IS DISTINCT FROM LAG(g) OVER (ORDER BY g ASC NULLS FIRST, x ASC NULLS FIRST) THEN x ELSE x - LAG(x) OVER (ORDER BY g ASC NULLS FIRST, x ASC NULLS FIRST) END AS delta FROM (VALUES ('a', CAST(3 AS BIGINT)), ('a', CAST(1 AS BIGINT)), ('a', CAST(2 AS BIGINT)), ('b', CAST(6 AS BIGINT)), ('b', CAST(4 AS BIGINT)), ('b', CAST(5 AS BIGINT))) AS t(g, x) ORDER BY g ASC NULLS FIRST, x ASC NULLS FIRST)
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, x:Int, delta:Int, cm:Int] rows=6
    - ('a', 1, 1, 1)
    - ('a', 2, 1, 2)
    - ('a', 3, 1, 3)
    - ('b', 4, 4, 4)
    - ('b', 5, 1, 5)
    - ('b', 6, 1, 6)
- DuckDB: cols=[g:String, x:Int, delta:Int, cm:Int] rows=6
    - ('a', 1, 1, 1)
    - ('a', 2, 1, 2)
    - ('a', 3, 1, 3)
    - ('b', 4, 4, 7)
    - ('b', 5, 1, 8)
    - ('b', 6, 1, 9)

### `agent-window-series-scan-0000` — MismatchRows (high)

*Detail:* first differing row[3]: kusto=('b', 1, 1, 1, 0.01639344262295082) duck=('b', 1, 61, 1, 1)

**KQL**
```kql
datatable(g:string, v:long)[ "a",10, "a",20, "a",30, "b",1, "b",2 ] | sort by g asc, v asc | serialize cs = row_cumsum(v, g != prev(g)), tot = toscalar(1), share = todouble(row_cumsum(v, g != prev(g))) / row_cumsum(v, false)
```
**Generated SQL**
```sql
SELECT *, SUM(v) OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS cs, (SELECT 1 AS value LIMIT 1) AS tot, COALESCE(TRY_CAST(SUM(v) OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS DOUBLE), TRY_CAST(TRY_CAST(SUM(v) OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS BOOLEAN) AS DOUBLE)) / SUM(v) OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS share FROM (VALUES ('a', CAST(10 AS BIGINT)), ('a', CAST(20 AS BIGINT)), ('a', CAST(30 AS BIGINT)), ('b', CAST(1 AS BIGINT)), ('b', CAST(2 AS BIGINT))) AS t(g, v) ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, v:Int, cs:Int, tot:Int, share:Real] rows=5
    - ('a', 10, 10, 1, 1)
    - ('a', 20, 30, 1, 1)
    - ('a', 30, 60, 1, 1)
    - ('b', 1, 1, 1, 0.01639344262295082)
    - ('b', 2, 3, 1, 0.047619047619047616)
- DuckDB: cols=[g:String, v:Int, cs:Int, tot:Int, share:Real] rows=5
    - ('a', 10, 10, 1, 1)
    - ('a', 20, 30, 1, 1)
    - ('a', 30, 60, 1, 1)
    - ('b', 1, 61, 1, 1)
    - ('b', 2, 63, 1, 1)

### `agent-window-series-scan-0001` — MismatchRows (high)

*Detail:* first differing row[3]: kusto=('b', 3, 1, 1, 1, 1) duck=('b', 3, 3, 4, 4, 1)

**KQL**
```kql
datatable(g:string, v:long)[ "a",5, "a",5, "a",8, "b",3, "b",3 ] | sort by g asc, v asc | serialize rd = row_rank_dense(v, g != prev(g)), rm = row_rank_min(v, g != prev(g)), rn = row_number(1, g != prev(g)), gap = row_rank_min(v) - row_rank_dense(v)
```
**Generated SQL**
```sql
SELECT *, DENSE_RANK() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS rd, RANK() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS rm, ROW_NUMBER() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS rn, RANK() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) - DENSE_RANK() OVER (ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST) AS gap FROM (VALUES ('a', CAST(5 AS BIGINT)), ('a', CAST(5 AS BIGINT)), ('a', CAST(8 AS BIGINT)), ('b', CAST(3 AS BIGINT)), ('b', CAST(3 AS BIGINT))) AS t(g, v) ORDER BY g ASC NULLS FIRST, v ASC NULLS FIRST
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

### `agent-window-series-scan-0004` — MismatchRows (high)

*Detail:* first differing row[2]: kusto=('y', 3, 3, 3) duck=('y', 3, 3, 6)

**KQL**
```kql
datatable(g:string, v:long)[ "z",1, "z",2, "y",3, "y",4, "x",5 ] | sort by g desc, v asc | serialize first_in_g = iff(g != prev(g), v, prev(v)), runsum = row_cumsum(v, g != prev(g))
```
**Generated SQL**
```sql
SELECT *, CASE WHEN g IS DISTINCT FROM LAG(g) OVER (ORDER BY g DESC NULLS LAST, v ASC NULLS FIRST) THEN v ELSE LAG(v) OVER (ORDER BY g DESC NULLS LAST, v ASC NULLS FIRST) END AS first_in_g, SUM(v) OVER (ORDER BY g DESC NULLS LAST, v ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS runsum FROM (VALUES ('z', CAST(1 AS BIGINT)), ('z', CAST(2 AS BIGINT)), ('y', CAST(3 AS BIGINT)), ('y', CAST(4 AS BIGINT)), ('x', CAST(5 AS BIGINT))) AS t(g, v) ORDER BY g DESC NULLS LAST, v ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, v:Int, first_in_g:Int, runsum:Int] rows=5
    - ('z', 1, 1, 1)
    - ('z', 2, 1, 3)
    - ('y', 3, 3, 3)
    - ('y', 4, 3, 7)
    - ('x', 5, 5, 5)
- DuckDB: cols=[g:String, v:Int, first_in_g:Int, runsum:Int] rows=5
    - ('z', 1, 1, 1)
    - ('z', 2, 1, 3)
    - ('y', 3, 3, 6)
    - ('y', 4, 3, 10)
    - ('x', 5, 5, 15)

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

### `agent-window-series-scan-0006` — MismatchRows (high)

*Detail:* first differing row[0]: kusto=(1, 20, 20) duck=(1, 20, null)

**KQL**
```kql
datatable(k:long, v:long)[ 1,10, 1,10, 1,20, 2,5, 2,5 ] | sort by k asc, v asc | summarize mx = max(v) by k | sort by k asc | serialize d = mx - prev(mx, 1, 0)
```
**Generated SQL**
```sql
SELECT *, mx - LAG(mx) OVER (ORDER BY k ASC NULLS FIRST) AS d FROM (SELECT k, MAX(v) AS mx FROM (SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(5 AS BIGINT)), (CAST(2 AS BIGINT), CAST(5 AS BIGINT))) AS t(k, v) ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[k:Int, mx:Int, d:Int] rows=2
    - (1, 20, 20)
    - (2, 5, -15)
- DuckDB: cols=[k:Int, mx:Int, d:Int] rows=2
    - (1, 20, null)
    - (2, 5, -15)

### `agent-window-series-scan-0007` — MismatchRows (high)

*Detail:* first differing row[1]: kusto=(2, 3, True, 3) duck=(2, 3, True, 8)

**KQL**
```kql
datatable(t:long, v:long, f:bool)[ 1,5,false, 2,3,true, 3,8,false, 4,1,true, 5,9,false ] | sort by t asc | serialize cs = row_cumsum(v, f)
```
**Generated SQL**
```sql
SELECT *, SUM(v) OVER (ORDER BY t ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS cs FROM (VALUES (CAST(1 AS BIGINT), CAST(5 AS BIGINT), FALSE), (CAST(2 AS BIGINT), CAST(3 AS BIGINT), TRUE), (CAST(3 AS BIGINT), CAST(8 AS BIGINT), FALSE), (CAST(4 AS BIGINT), CAST(1 AS BIGINT), TRUE), (CAST(5 AS BIGINT), CAST(9 AS BIGINT), FALSE)) AS t(t, v, f) ORDER BY t ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[t:Int, v:Int, f:Bool, cs:Int] rows=5
    - (1, 5, False, 5)
    - (2, 3, True, 3)
    - (3, 8, False, 11)
    - (4, 1, True, 1)
    - (5, 9, False, 10)
- DuckDB: cols=[t:Int, v:Int, f:Bool, cs:Int] rows=5
    - (1, 5, False, 5)
    - (2, 3, True, 8)
    - (3, 8, False, 16)
    - (4, 1, True, 17)
    - (5, 9, False, 26)

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

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[s:Int|String], TYPE_MISMATCH[t:DateTime|Real]  
*Detail:* row count: kusto=6 duck=18

**KQL**
```kql
datatable(t:datetime, v:long, g:string)[ datetime(2021-01-01),10,"a", datetime(2021-01-02),20,"a", datetime(2021-01-01),5,"b" ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-04) step 1d by g | mv-expand t to typeof(datetime), s to typeof(long) | sort by g asc, t asc
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (t, s), u0.value AS t, u1.value AS s FROM (SELECT _axis.g, LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT _g.*, _t._ts FROM (SELECT DISTINCT g FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT), 'a'), (TIMESTAMP '2021-01-02 00:00:00', CAST(20 AS BIGINT), 'a'), (TIMESTAMP '2021-01-01 00:00:00', CAST(5 AS BIGINT), 'b')) AS t(t, v, g)) AS _g CROSS JOIN (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-04 00:00:00', 86400000 * INTERVAL '1 millisecond')) AS _ts) AS _t) AS _axis LEFT JOIN (SELECT g, EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/86400000)*86400000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 00:00:00', CAST(10 AS BIGINT), 'a'), (TIMESTAMP '2021-01-02 00:00:00', CAST(20 AS BIGINT), 'a'), (TIMESTAMP '2021-01-01 00:00:00', CAST(5 AS BIGINT), 'b')) AS t(t, v, g) GROUP BY g, _bucket) AS _data ON _axis.g = _data.g AND _axis._ts = _data._bucket GROUP BY _axis.g) AS t CROSS JOIN UNNEST(t.t) AS u0(value) CROSS JOIN UNNEST(t.s) AS u1(value) ORDER BY g ASC NULLS FIRST, t ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[g:String, s:Int, t:DateTime] rows=6
    - ('a', 10, 2021-01-01T00:00:00.0000000Z)
    - ('a', 20, 2021-01-02T00:00:00.0000000Z)
    - ('a', 0, 2021-01-03T00:00:00.0000000Z)
    - ('b', 5, 2021-01-01T00:00:00.0000000Z)
    - ('b', 0, 2021-01-02T00:00:00.0000000Z)
    - ('b', 0, 2021-01-03T00:00:00.0000000Z)
- DuckDB: cols=[g:String, t:String, s:Real] rows=18
    - ('a', '2021-01-01T00:00:00.0000000Z', 10)
    - ('a', '2021-01-01T00:00:00.0000000Z', 20)
    - ('a', '2021-01-01T00:00:00.0000000Z', 0)
    - ('a', '2021-01-02T00:00:00.0000000Z', 10)
    - ('a', '2021-01-02T00:00:00.0000000Z', 20)
    - ('a', '2021-01-02T00:00:00.0000000Z', 0)
    - ('a', '2021-01-03T00:00:00.0000000Z', 10)
    - ('a', '2021-01-03T00:00:00.0000000Z', 20)

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
SELECT * FROM (SELECT k, MAX(within) AS maxwithin, COALESCE(SUM(v), 0) AS tot FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST) AS within, SUM(v) OVER (ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS cumg FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(1 AS BIGINT), CAST(20 AS BIGINT)), (CAST(2 AS BIGINT), CAST(30 AS BIGINT)), (CAST(2 AS BIGINT), CAST(40 AS BIGINT)), (CAST(2 AS BIGINT), CAST(50 AS BIGINT))) AS t(k, v) ORDER BY k ASC NULLS FIRST, v ASC NULLS FIRST) GROUP BY ALL) ORDER BY k ASC NULLS FIRST
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

*Sub-verdicts:* NAME_MISMATCH, TYPE_MISMATCH[s:Int|String], TYPE_MISMATCH[t:DateTime|Real]  
*Detail:* row count: kusto=4 duck=16

**KQL**
```kql
datatable(t:datetime, v:long)[ datetime(2021-01-01 23:00),1, datetime(2021-01-02 01:00),2, datetime(2021-01-02 23:00),3 ] | make-series s = sum(v) default = 0 on t from datetime(2021-01-01) to datetime(2021-01-03) step 12h | mv-expand t to typeof(datetime), s to typeof(long) | sort by t asc
```
**Generated SQL**
```sql
SELECT t.* EXCLUDE (t, s), u0.value AS t, u1.value AS s FROM (SELECT LIST(COALESCE(s_val, 0) ORDER BY _ts) AS s, LIST(strftime(_axis._ts, '%Y-%m-%dT%H:%M:%S.%f') || '0Z' ORDER BY _axis._ts) AS t FROM (SELECT UNNEST(range(TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2021-01-03 00:00:00', 43200000 * INTERVAL '1 millisecond')) AS _ts) AS _axis LEFT JOIN (SELECT EPOCH_MS(CAST(FLOOR(EPOCH_MS(t)/43200000)*43200000 AS BIGINT)) AS _bucket, CAST(COALESCE(SUM(v), 0) AS DOUBLE) AS s_val FROM (VALUES (TIMESTAMP '2021-01-01 23:00:00', CAST(1 AS BIGINT)), (TIMESTAMP '2021-01-02 01:00:00', CAST(2 AS BIGINT)), (TIMESTAMP '2021-01-02 23:00:00', CAST(3 AS BIGINT))) AS t(t, v) GROUP BY _bucket) AS _data ON _axis._ts = _data._bucket) AS t CROSS JOIN UNNEST(t.t) AS u0(value) CROSS JOIN UNNEST(t.s) AS u1(value) ORDER BY t ASC NULLS FIRST
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[s:Int, t:DateTime] rows=4
    - (0, 2021-01-01T00:00:00.0000000Z)
    - (1, 2021-01-01T12:00:00.0000000Z)
    - (2, 2021-01-02T00:00:00.0000000Z)
    - (3, 2021-01-02T12:00:00.0000000Z)
- DuckDB: cols=[t:String, s:Real] rows=16
    - ('2021-01-01T00:00:00.0000000Z', 0)
    - ('2021-01-01T00:00:00.0000000Z', 1)
    - ('2021-01-01T00:00:00.0000000Z', 2)
    - ('2021-01-01T00:00:00.0000000Z', 3)
    - ('2021-01-01T12:00:00.0000000Z', 0)
    - ('2021-01-01T12:00:00.0000000Z', 1)
    - ('2021-01-01T12:00:00.0000000Z', 2)
    - ('2021-01-01T12:00:00.0000000Z', 3)

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

### `agent-window-series-scan-0020` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(cat:string, sub:string, v:long)[ "a","x",10, "a","y",20, "a","y",5, "b","x",30, "b","z",5, "c","x",1 ] | top-nested 2 of cat by Tot=sum(v), top-nested 1 of sub by SubTot=sum(v)
```
**Generated SQL**
```sql
SELECT cat, Tot, sub, SubTot FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY SubTot DESC) AS _rn1 FROM (SELECT _src.cat, _src.sub, _prev.Tot, COALESCE(SUM(v), 0) AS SubTot FROM (SELECT * FROM (VALUES ('a', 'x', CAST(10 AS BIGINT)), ('a', 'y', CAST(20 AS BIGINT)), ('a', 'y', CAST(5 AS BIGINT)), ('b', 'x', CAST(30 AS BIGINT)), ('b', 'z', CAST(5 AS BIGINT)), ('c', 'x', CAST(1 AS BIGINT))) AS t(cat, sub, v)) AS _src INNER JOIN (SELECT cat, Tot FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY Tot DESC) AS _rn0 FROM (SELECT cat, COALESCE(SUM(v), 0) AS Tot FROM (VALUES ('a', 'x', CAST(10 AS BIGINT)), ('a', 'y', CAST(20 AS BIGINT)), ('a', 'y', CAST(5 AS BIGINT)), ('b', 'x', CAST(30 AS BIGINT)), ('b', 'z', CAST(5 AS BIGINT)), ('c', 'x', CAST(1 AS BIGINT))) AS t(cat, sub, v) GROUP BY cat)) WHERE _rn0 <= 2) AS _prev ON _src.cat = _prev.cat GROUP BY _src.cat, _src.sub, _prev.Tot)) WHERE _rn1 <= 1
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[cat:String, Tot:Int, sub:String, SubTot:Int] rows=2
    - ('b', 35, 'x', 30)
    - ('a', 35, 'y', 25)
- DuckDB: cols=[cat:String, Tot:Int, sub:String, SubTot:Int] rows=2
    - ('a', 35, 'y', 25)
    - ('b', 35, 'x', 30)

### `agent-window-series-scan-0021` — MismatchOrder (medium)

*Detail:* rows match as a set but order differs

**KQL**
```kql
datatable(cat:string, sub:string, v:long)[ "a","x",10, "a","y",20, "b","x",30, "b","z",5 ] | top-nested 3 of cat by sum(v), top-nested 2 of sub by max(v)
```
**Generated SQL**
```sql
SELECT cat, aggregated_cat, sub, aggregated_sub FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY aggregated_sub DESC) AS _rn1 FROM (SELECT _src.cat, _src.sub, _prev.aggregated_cat, MAX(v) AS aggregated_sub FROM (SELECT * FROM (VALUES ('a', 'x', CAST(10 AS BIGINT)), ('a', 'y', CAST(20 AS BIGINT)), ('b', 'x', CAST(30 AS BIGINT)), ('b', 'z', CAST(5 AS BIGINT))) AS t(cat, sub, v)) AS _src INNER JOIN (SELECT cat, aggregated_cat FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY aggregated_cat DESC) AS _rn0 FROM (SELECT cat, COALESCE(SUM(v), 0) AS aggregated_cat FROM (VALUES ('a', 'x', CAST(10 AS BIGINT)), ('a', 'y', CAST(20 AS BIGINT)), ('b', 'x', CAST(30 AS BIGINT)), ('b', 'z', CAST(5 AS BIGINT))) AS t(cat, sub, v) GROUP BY cat)) WHERE _rn0 <= 3) AS _prev ON _src.cat = _prev.cat GROUP BY _src.cat, _src.sub, _prev.aggregated_cat)) WHERE _rn1 <= 2
```
**Kusto (oracle)** vs **DuckDB (translated)**

- Kusto: cols=[cat:String, aggregated_cat:Int, sub:String, aggregated_sub:Int] rows=4
    - ('b', 35, 'x', 30)
    - ('b', 35, 'z', 5)
    - ('a', 30, 'y', 20)
    - ('a', 30, 'x', 10)
- DuckDB: cols=[cat:String, aggregated_cat:Int, sub:String, aggregated_sub:Int] rows=4
    - ('a', 30, 'y', 20)
    - ('a', 30, 'x', 10)
    - ('b', 35, 'x', 30)
    - ('b', 35, 'z', 5)

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

