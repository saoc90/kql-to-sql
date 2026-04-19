- [x] != (not equals)
- [x] !between
- [x] !contains
- [x] !contains_cs
- [x] !endswith
- [x] !endswith_cs
- [x] !has
- [x] !has_cs
- [x] !hasprefix
- [x] !hasprefix_cs
- [x] !hassuffix
- [x] !hassuffix_cs
- [x] !in
- [x] !in~
- [x] !startswith
- [x] !startswith_cs
- [x] !~ (not equals)
- [x] == (equals)
- [x] =~ (equals)
- [x] binary_and()
- [x] binary_or()
- [x] binary_xor()
- [x] binary_not()
- [x] binary_shift_left()
- [x] binary_shift_right()
- [x] arg_max
- [x] arg_min
- [x] as
- [x] between
- [x] bin()
- [x] consume
- [x] contains
- [x] contains_cs
- [x] count
- [x] datatable
- [x] distinct
- [x] endswith
- [x] endswith_cs
- [x] evaluate (pivot, narrow, bag_unpack)
- [x] extend
- [x] externaldata
- [ ] ~~facet~~ (not supported — see below)
- [ ] ~~find~~ (not supported — see below)
- [ ] ~~fork~~ (not supported — see below)
- [x] getschema
- [x] has
- [x] has_all
- [x] has_any
- [x] has_cs
- [x] hasprefix
- [x] hasprefix_cs
- [x] hassuffix
- [x] hassuffix_cs
- [x] in
- [ ] ~~invoke~~ (not supported — see below)
- [x] in~
- [x] join
- [x] lookup
- [ ] ~~macro-expand~~ (not supported — see below)
- [x] make-series
- [x] matches regex
- [x] mv-apply
- [x] mv-expand
- [x] parse
- [x] parse-kv
- [x] parse-where
- [ ] ~~partition~~ (not supported — see below)
- [x] print
- [x] project
- [x] project-away
- [ ] ~~project-by-names~~ (not supported — see below)
- [x] project-keep
- [x] project-rename
- [x] project-reorder
- [x] range
- [ ] ~~reduce~~ (not supported — see below)
- [x] render
- [x] sample
- [x] sample-distinct
- [x] scan (partial — single-step patterns: cumulative sum, forward fill, cumulative-with-reset)
- [x] search
- [x] serialize
- [x] sort
- [x] startswith
- [x] startswith_cs
- [x] summarize
- [x] take
- [x] top
- [x] top-hitters
- [x] top-nested
- [x] union
- [x] where
- [x] materialize()
- [x] create table (command)
- [x] view (command)
- [x] view() (function declaration)

## Scalar functions
- [x] strlen()
- [x] substring()
- [x] tolower()
- [x] toupper()
- [x] now()
- [x] ago()
- [x] pack_array()
- [x] abs()
- [x] acos()
- [x] asin()
- [x] atan()
- [x] atan2()
- [x] ceiling()
- [x] floor()
- [x] round()
- [x] sqrt()
- [x] exp()
- [x] log()
- [x] log10()
- [x] pow()
- [x] rand()
- [x] format_datetime()
- [x] parse_json()
- [x] parse_url()
- [x] iif()
- [x] iff()
- [x] isnull()
- [x] isempty()
- [x] coalesce()
- [x] case()
- [x] datetime_add()
- [x] datetime_diff()
- [x] startofday()
- [x] startofweek()
- [x] startofmonth()
- [x] startofyear()
- [x] endofday()
- [x] endofweek()
- [x] endofmonth()
- [x] endofyear()
- [x] not()
- [x] isnotempty()
- [x] isnotnull()
- [x] strcat()
- [x] strcat_delim()
- [x] replace_string()
- [x] trim()
- [x] trim_start()
- [x] trim_end()
- [x] indexof()
- [x] countof()
- [x] reverse()
- [x] split()
- [x] extract()
- [x] sign()
- [x] log2()
- [x] exp2()
- [x] exp10()
- [x] pi()
- [x] cos()
- [x] sin()
- [x] tan()
- [x] min_of()
- [x] max_of()
- [x] todynamic()
- [x] row_number()
- [x] prev()
- [x] next()
- [x] dayofweek()
- [x] dayofmonth()
- [x] dayofyear()
- [x] getmonth()
- [x] getyear()
- [x] monthofyear()
- [x] weekofyear()
- [x] hourofday()
- [x] minuteofhour()
- [x] secondofminute()
- [x] make_datetime()
- [x] make_timespan()
- [x] unixtime_seconds_todatetime()
- [x] unixtime_milliseconds_todatetime()
- [x] unixtime_microseconds_todatetime()
- [x] unixtime_nanoseconds_todatetime()
- [x] datetime_part()
- [x] format_timespan()
- [x] base64_encode_tostring()
- [x] base64_decode_tostring()
- [x] translate()
- [x] strcmp()
- [x] string_size()
- [x] repeat()
- [x] unicode()
- [x] make_string()
- [x] url_encode_component()
- [x] url_decode_component()
- [x] parse_path()
- [x] to_utf8()
- [x] hash()
- [x] hash_md5()
- [x] hash_sha256()
- [x] hash_sha1()
- [x] array_length()
- [x] array_index_of()
- [x] array_sort_asc()
- [x] array_sort_desc()
- [x] array_concat()
- [x] array_reverse()
- [x] array_slice()
- [x] array_sum()
- [x] bag_keys()
- [x] bag_has_key()
- [x] bag_merge()
- [x] set_difference()
- [x] set_intersect()
- [x] set_union()
- [x] zip()
- [x] gettype()
- [x] isnan()
- [x] isinf()
- [x] isfinite()
- [x] extract_all()
- [x] replace_regex()
- [x] parse_csv()
- [x] dynamic_to_json()
- [x] tohex()
- [x] bag_remove_keys()
- [x] datetime_local_to_utc()
- [x] datetime_utc_to_local()
- [x] extract_json() / extractjson()
- [x] set_has_element()
- [x] row_cumsum()
- [x] row_rank_dense()
- [x] row_rank_min()
- [x] degrees()
- [x] radians()
- [x] cot()
- [x] gamma()
- [x] loggamma()
- [x] bitset_count_ones()
- [x] bin_at()
- [x] toguid()
- [x] bag_set_key()
- [x] replace_strings()
- [x] strrep()
- [x] has_any_index()
- [x] parse_urlquery()
- [x] parse_version()
- [x] hash_combine()
- [x] hash_many()
- [x] hash_xxhash64()
- [x] parse_ipv4()
- [x] parse_ipv4_mask()
- [x] ipv4_compare()
- [x] ipv4_is_in_range()
- [x] ipv4_is_private()
- [x] format_ipv4()
- [x] base64_encode_fromguid()
- [x] base64_decode_toguid()
- [x] base64_decode_toarray()
- [x] array_iff()
- [x] array_rotate_left()
- [x] array_rotate_right()
- [x] array_split()
- [x] pack_dictionary()
- [x] range() (scalar)
- [x] row_window_session()
- [x] array_shift_left()
- [x] array_shift_right()
- [x] ipv4_is_match()
- [x] ipv4_netmask_suffix()
- [x] format_ipv4_mask()
- [x] jaccard_index()
- [x] toscalar()

## Aggregate functions
- [x] avg()
- [x] avgif()
- [x] binary_all_and()
- [x] binary_all_or()
- [x] binary_all_xor()
- [x] buildschema()
- [x] count()
- [x] count_distinct()
- [x] count_distinctif()
- [x] countif()
- [x] covariance()
- [x] covarianceif()
- [x] covariancep()
- [x] covariancepif()
- [x] dcount()
- [x] dcountif()
- [x] hll()
- [x] hll_if()
- [x] hll_merge()
- [x] make_bag()
- [x] make_bag_if()
- [x] make_list()
- [x] sum()
- [x] make_list_if()
- [x] make_list_with_nulls()
- [x] make_set()
- [x] make_set_if()
- [x] maxif()
- [x] minif()
- [x] percentile()
- [x] percentiles()
- [x] percentilew()
- [x] percentilesw()
- [x] stdev()
- [x] stdevif()
- [x] stdevp()
- [x] sumif()
- [x] take_any()
- [x] take_anyif()
- [x] variance()
- [x] varianceif()
- [x] variancep()
- [x] variancepif()
- [x] percentiles_array()
- [x] percentilesw_array()
- [x] min()
- [x] max()

## Explicitly not supported

These operators have no SQL equivalent or require capabilities beyond single-query SQL translation:

| Operator | Reason |
|---|---|
| `facet` | Produces multiple result sets from a single input — SQL returns one result set per query |
| `fork` | Branches pipeline into multiple outputs — same limitation as facet |
| `invoke` | Calls user-defined functions by name — requires a function registry not available at translation time |
| `macro-expand` | Expands macros — compile-time construct with no SQL equivalent |
| `partition` | Executes sub-pipelines per partition independently — would require dynamic SQL generation per group |
| `reduce` | Clusters string patterns using heuristics — algorithmic operation with no SQL equivalent |
| `scan` (multi-step pattern matching) | Sequence detection across rows with multiple named steps and match-id — full state-machine semantics can't be expressed with standard window functions. Single-step scans (cumulative sum, forward fill, conditional reset) are supported; multi-step pattern matching is not. |
| `project-by-names` | Selects columns matching a wildcard pattern — requires schema introspection at translation time |
| `find` | Searches across multiple tables in a database — requires catalog access not available at translation time |
| `evaluate autocluster()` | Machine-learning clustering plugin — no SQL equivalent |
| `evaluate basket()` | Market-basket analysis plugin — no SQL equivalent |
| `evaluate diffpatterns()` | Pattern diffing plugin — no SQL equivalent |
| `evaluate preview()` | Interactive preview plugin — no SQL equivalent |
| `make-graph` | Graph analytics — creates graph objects, no SQL equivalent |
| `graph-match` | Graph analytics — pattern matching on graph structures |
| `graph-to-table` | Graph analytics — converts graph back to tabular form |
| `graph-shortest-paths` | Graph analytics — shortest path algorithms |
| `graph-mark-components` | Graph analytics — connected component detection |

## Silently ignored (no-op)

These are execution hints for the ADX distributed engine. They don't change query results and have no SQL equivalent — they're safely ignored during translation:

| Hint | Context |
|---|---|
| `hint.shufflekey` | join, summarize, make-series — controls data distribution across cluster nodes |
| `hint.strategy = shuffle/broadcast` | join — controls join strategy on distributed engine |
| `hint.materialized` | as, evaluate — controls materialization on distributed engine |
| `hint.num_partitions` | shuffle — controls partition count on distributed engine |
| `hint.spread` | join — controls spread factor on distributed engine |
| `hint.remote` | join — controls remote execution on cross-cluster joins |
