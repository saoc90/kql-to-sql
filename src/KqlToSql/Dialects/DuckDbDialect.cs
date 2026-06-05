using System;
using System.Collections.Generic;
using System.Linq;

namespace KqlToSql.Dialects;

/// <summary>
/// SQL dialect implementation for DuckDB.
/// </summary>
public class DuckDbDialect : ISqlDialect
{
    public string Name => "DuckDB";

    public string CaseInsensitiveLike => "ILIKE";

    public string? TryTranslateFunction(string name, string[] args)
    {
        return name switch
        {
            "bag_pack" or "pack" => $"json_object({string.Join(", ", args)})",
            // bag_pack_columns packs named columns: bag_pack_columns(a, b) → {"a": a, "b": b}
            "bag_pack_columns" => args.Length == 0 ? "json_object()" :
                $"json_object({string.Join(", ", args.Select(a => $"'{a}', {a}"))})",
            "tolower" => $"LOWER({args[0]})",
            "toupper" => $"UPPER({args[0]})",
            "strlen" => $"LENGTH(CAST({args[0]} AS VARCHAR))",
            "now" => "NOW()",
            // KQL pack_array flattens nested arrays: pack_array([a,b], [c,d]) → [a,b,c,d].
            // Emit LIST_CONCAT when every arg is itself array-like. Heterogeneous scalars
            // (e.g. strings mixed with numbers) can't unify under LIST<T>, so route through
            // TO_JSON so the result is LIST<JSON> and downstream JSON indexing still works.
            "pack_array" when args.Length > 1 && args.All(IsArrayLikeText) => $"LIST_CONCAT({string.Join(", ", args)})",
            "pack_array" when args.Length > 1 && HasMixedScalarTypes(args) =>
                $"LIST_VALUE({string.Join(", ", args.Select(a => $"TO_JSON({a})"))})",
            "pack_array" => $"LIST_VALUE({string.Join(", ", args)})",
            // KQL column_ifexists("Name", default) — returns the column's value if it exists,
            // else the default. DuckDB has no equivalent function; emit a COALESCE of the column
            // reference so the SQL parses. At bind time DuckDB still needs the column to exist,
            // but for queries that know the schema this is a workable approximation. Empty
            // column names degrade to the default value.
            "column_ifexists" when args.Length >= 2 =>
                string.IsNullOrEmpty(args[0].Trim('\'', '"'))
                    ? args[1]
                    : $"COALESCE({args[0].Trim('\'', '"')}, {args[1]})",
            "column_ifexists" when args.Length == 1 =>
                string.IsNullOrEmpty(args[0].Trim('\'', '"'))
                    ? "NULL"
                    : args[0].Trim('\'', '"'),
            "isempty" => $"({args[0]} IS NULL OR CAST({args[0]} AS VARCHAR) = '')",
            // isnotempty is the negation of isempty (false for NULL *and* for ''); it is NOT the same
            // as isnotnull, which only checks for NULL.
            "isnotempty" => $"({args[0]} IS NOT NULL AND CAST({args[0]} AS VARCHAR) <> '')",
            "isnotnull" => $"({args[0]} IS NOT NULL)",
            "isnull" => $"({args[0]} IS NULL)",
            "not" => $"NOT ({args[0]})",
            "strcat" => $"CONCAT({string.Join(", ", args)})",
            "replace_string" => $"REPLACE({string.Join(", ", args)})",
            "indexof" => $"(INSTR(CAST({args[0]} AS VARCHAR), {args[1]}) - 1)",
            "coalesce" => $"COALESCE({string.Join(", ", args)})",
            "countof" => $"(LENGTH({args[0]}) - LENGTH(REPLACE({args[0]}, {args[1]}, ''))) / LENGTH({args[1]})",
            "reverse" => $"REVERSE({args[0]})",
            "split" => $"STRING_SPLIT(CAST({args[0]} AS VARCHAR), {args[1]})",
            "floor" => $"FLOOR({args[0]})",
            "ceiling" => $"CEILING({args[0]})",
            "abs" => $"GREATEST({args[0]}, -({args[0]}))",
            "sqrt" => $"SQRT({args[0]})",
            "log" => $"LN({args[0]})",
            "log10" => $"LOG10({args[0]})",
            "log2" => $"LOG2({args[0]})",
            "exp" => $"EXP({args[0]})",
            "exp2" => $"POWER(2, {args[0]})",
            "exp10" => $"POWER(10, {args[0]})",
            "pow" or "power" => $"POWER({args[0]}, {args[1]})",
            "pi" => "PI()",
            "cos" => $"COS({args[0]})",
            "sin" => $"SIN({args[0]})",
            "tan" => $"TAN({args[0]})",
            "acos" => $"ACOS({args[0]})",
            "asin" => $"ASIN({args[0]})",
            "atan" => $"ATAN({args[0]})",
            "atan2" => $"ATAN2({args[0]}, {args[1]})",
            "sign" => $"SIGN({args[0]})",
            "rand" => "RANDOM()",
            // todynamic(X) parses a string as JSON. When X is already a list-producing expression
            // (LIST_VALUE(...), scalar subquery returning a LIST, etc), casting to JSON strips LIST
            // semantics and downstream LIST_FILTER/LIST_CONTAINS break. Pass through array-like sources.
            "parse_json" or "todynamic" => IsArrayLikeText(args[0]) ? args[0] : $"CAST({args[0]} AS JSON)",
            "format_datetime" => $"STRFTIME({args[0]}, {TranslateDateTimeFormat(args[1])})",
            "startofday" => $"DATE_TRUNC('day', {args[0]})",
            // KQL weeks start on SUNDAY; DuckDB's DATE_TRUNC('week') starts on Monday. Shift the
            // input forward a day before truncating and back a day after, so Sunday becomes the anchor.
            "startofweek" => $"(DATE_TRUNC('week', {args[0]} + INTERVAL '1 day') - INTERVAL '1 day')",
            "startofmonth" => $"DATE_TRUNC('month', {args[0]})",
            "startofyear" => $"DATE_TRUNC('year', {args[0]})",
            "endofday" => $"DATE_TRUNC('day', {args[0]}) + INTERVAL '1 day' - INTERVAL '1 microsecond'",
            "endofweek" => $"(DATE_TRUNC('week', {args[0]} + INTERVAL '1 day') - INTERVAL '1 day') + INTERVAL '7 days' - INTERVAL '1 microsecond'",
            "endofmonth" => $"DATE_TRUNC('month', {args[0]}) + INTERVAL '1 month' - INTERVAL '1 microsecond'",
            "endofyear" => $"DATE_TRUNC('year', {args[0]}) + INTERVAL '1 year' - INTERVAL '1 microsecond'",
            "min_of" => $"LEAST({string.Join(", ", args)})",
            "max_of" => $"GREATEST({string.Join(", ", args)})",
            "row_number" => "ROW_NUMBER() OVER ()",
            "prev" => $"LAG({args[0]}) OVER ()",
            "next" => $"LEAD({args[0]}) OVER ()",

            // Date/time functions
            // KQL dayofweek() returns a *timespan* equal to the number of whole days since the
            // preceding Sunday (Sunday=0d, Monday=1d, ... Saturday=6d), rendered as Kusto's
            // timespan string: "00:00:00" for 0 days, "N.00:00:00" for N>0. DuckDB's
            // EXTRACT(DOW) already uses the same 0=Sunday..6=Saturday numbering, so format it
            // as that timespan literal to match the ground truth.
            "dayofweek" => $"CASE WHEN EXTRACT(DOW FROM {args[0]}) = 0 THEN '00:00:00' ELSE CAST(EXTRACT(DOW FROM {args[0]}) AS VARCHAR) || '.00:00:00' END",
            "dayofmonth" => $"EXTRACT(DAY FROM {args[0]})",
            "dayofyear" => $"EXTRACT(DOY FROM {args[0]})",
            "getmonth" => $"EXTRACT(MONTH FROM {args[0]})",
            "getyear" => $"EXTRACT(YEAR FROM {args[0]})",
            "monthofyear" => $"EXTRACT(MONTH FROM {args[0]})",
            "weekofyear" or "week_of_year" => $"EXTRACT(WEEK FROM {args[0]})",
            "hourofday" => $"EXTRACT(HOUR FROM {args[0]})",
            "minuteofhour" => $"EXTRACT(MINUTE FROM {args[0]})",
            "secondofminute" => $"EXTRACT(SECOND FROM {args[0]})",
            "make_datetime" when args.Length == 6 =>
                $"MAKE_TIMESTAMP({args[0]}, {args[1]}, {args[2]}, {args[3]}, {args[4]}, {args[5]})",
            // KQL make_datetime accepts year[,month[,day[,hour[,minute[,second]]]]]; missing trailing
            // components default to their start value. DuckDB MAKE_TIMESTAMP needs all six.
            "make_datetime" when args.Length is 3 or 4 or 5 =>
                $"MAKE_TIMESTAMP({args[0]}, {args[1]}, {args[2]}, " +
                $"{(args.Length > 3 ? args[3] : "0")}, {(args.Length > 4 ? args[4] : "0")}, 0)",
            "make_datetime" when args.Length == 2 => $"MAKE_TIMESTAMP({args[0]}, {args[1]}, 1, 0, 0, 0)",
            "make_datetime" when args.Length == 1 => $"CAST({args[0]} AS TIMESTAMP)",
            "make_timespan" when args.Length == 3 =>
                $"({args[0]} * INTERVAL '1 hour' + {args[1]} * INTERVAL '1 minute' + {args[2]} * INTERVAL '1 second')",
            // make_timespan(hours, minutes)
            "make_timespan" when args.Length == 2 =>
                $"({args[0]} * INTERVAL '1 hour' + {args[1]} * INTERVAL '1 minute')",
            // make_timespan(days, hours, minutes, seconds)
            "make_timespan" when args.Length == 4 =>
                $"({args[0]} * INTERVAL '1 day' + {args[1]} * INTERVAL '1 hour' + {args[2]} * INTERVAL '1 minute' + {args[3]} * INTERVAL '1 second')",
            // TO_TIMESTAMP returns TIMESTAMP WITH TIME ZONE; KQL datetime is tz-agnostic and our
            // Timestamp columns are naive TIMESTAMP. Project to UTC wall-clock so comparisons don't
            // depend on the DuckDB session TimeZone (the ms/us/ns variants below are already naive).
            "unixtime_seconds_todatetime" => $"(TO_TIMESTAMP(CAST({args[0]} AS DOUBLE)) AT TIME ZONE 'UTC')",
            "unixtime_milliseconds_todatetime" => $"EPOCH_MS(CAST({args[0]} AS BIGINT))",
            "unixtime_microseconds_todatetime" => $"MAKE_TIMESTAMP(CAST({args[0]} AS BIGINT))",
            "unixtime_nanoseconds_todatetime" => $"MAKE_TIMESTAMP(CAST({args[0]} AS BIGINT) / 1000)",
            "datetime_part" => $"EXTRACT({args[0].Trim('\'')} FROM CAST({args[1]} AS TIMESTAMP))",
            "format_timespan" when args.Length >= 2 => FormatTimespan(args[0], args[1]),
            "format_timespan" => $"CAST({args[0]} AS VARCHAR)",

            // String functions
            "parse_url" => $"json_object('Scheme', REGEXP_EXTRACT({args[0]}, '^(\\w+)://', 1), 'Host', REGEXP_EXTRACT({args[0]}, '://([^/:]+)', 1), 'Port', REGEXP_EXTRACT({args[0]}, ':(\\d+)', 1), 'Path', REGEXP_EXTRACT({args[0]}, '://[^/]+([\\/][^?#]*)', 1))",
            "base64_encode_tostring" or "base64_encode_fromarray" => $"BASE64(CAST({args[0]} AS BLOB))",
            "base64_decode_tostring" => $"CAST(FROM_BASE64({args[0]}) AS VARCHAR)",
            "translate" => $"TRANSLATE({args[0]}, {args[1]}, {args[2]})",
            "strcmp" => $"CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} = {args[1]} THEN 0 ELSE 1 END",
            // string_size = number of UTF-8 bytes. OCTET_LENGTH needs a BLOB; ENCODE() does the
            // UTF-8 conversion (plain CAST AS BLOB rejects non-ASCII bytes).
            "string_size" => $"OCTET_LENGTH(ENCODE(CAST({args[0]} AS VARCHAR)))",
            "repeat" => $"REPEAT({args[0]}, {args[1]})",
            "unicode" => $"UNICODE({args[0]})",
            "make_string" => $"CHR({args[0]})",
            "url_encode_component" or "url_encode" => $"URL_ENCODE({args[0]})",
            "url_decode_component" or "url_decode" => $"URL_DECODE({args[0]})",
            "parse_path" => $"REGEXP_EXTRACT({args[0]}, '([^/\\\\]+)$', 1)",
            "to_utf8" => $"CAST({args[0]} AS BLOB)",

            // Hash functions
            "hash" => $"HASH({args[0]})",
            "hash_md5" => $"MD5({args[0]})",
            "hash_sha256" => $"SHA256({args[0]})",
            "hash_sha1" => $"SHA1({args[0]})",

            // ── series_* plugin functions (operate on make-series LIST output) ──────────
            // The series argument (args[0]) is a DuckDB native LIST. KQL semantics are
            // reproduced with list lambdas; statistically-complex ones emit a runnable
            // (sometimes approximate) value — the bar is "executes without error".

            // Forward fill: each null becomes the previous non-null. An optional placeholder
            // (args[1]) marks "missing" values; default missing is NULL. NB: DuckDB list *slicing*
            // (s[1:i]) errors on lists that contain NULLs, but element indexing (s[j]) does not — so
            // we filter the prefix INDICES by non-nullness and map them back to values, taking the
            // last one. (See series_fill_backward for the symmetric suffix form.)
            "series_fill_forward" when args.Length >= 2 =>
                SeriesFillDirectional(args[0], forward: true, args[1]),
            "series_fill_forward" =>
                SeriesFillDirectional(args[0], forward: true, null),

            // Backward fill: each null becomes the next non-null.
            "series_fill_backward" when args.Length >= 2 =>
                SeriesFillDirectional(args[0], forward: false, args[1]),
            "series_fill_backward" =>
                SeriesFillDirectional(args[0], forward: false, null),

            // Linear interpolation of nulls: for each position i find the nearest non-null on the
            // left (value vl at index il) and right (vr at index ir) and linearly interpolate.
            // If only one side exists, copy it (degrades to forward/backward fill at the ends).
            "series_fill_linear" =>
                SeriesFillLinear(args[0]),

            // Element-wise arithmetic over two equal-length lists.
            "series_add" => $"LIST_TRANSFORM(GENERATE_SERIES(1, LEAST(LEN({args[0]}), LEN({args[1]}))), i -> {args[0]}[i] + {args[1]}[i])",
            "series_subtract" => $"LIST_TRANSFORM(GENERATE_SERIES(1, LEAST(LEN({args[0]}), LEN({args[1]}))), i -> {args[0]}[i] - {args[1]}[i])",
            "series_multiply" => $"LIST_TRANSFORM(GENERATE_SERIES(1, LEAST(LEN({args[0]}), LEN({args[1]}))), i -> {args[0]}[i] * {args[1]}[i])",
            "series_divide" => $"LIST_TRANSFORM(GENERATE_SERIES(1, LEAST(LEN({args[0]}), LEN({args[1]}))), i -> CASE WHEN {args[1]}[i] = 0 THEN 'nan'::DOUBLE ELSE ({args[0]}[i]::DOUBLE / {args[1]}[i]) END)",

            // Element-wise comparison against a scalar → list of booleans.
            "series_greater" => $"LIST_TRANSFORM({args[0]}, x -> x > {args[1]})",
            "series_less" => $"LIST_TRANSFORM({args[0]}, x -> x < {args[1]})",
            "series_equals" => $"LIST_TRANSFORM({args[0]}, x -> x = {args[1]})",
            "series_not_equals" => $"LIST_TRANSFORM({args[0]}, x -> x <> {args[1]})",
            "series_greater_equals" => $"LIST_TRANSFORM({args[0]}, x -> x >= {args[1]})",
            "series_less_equals" => $"LIST_TRANSFORM({args[0]}, x -> x <= {args[1]})",

            // FIR (finite impulse response): weighted moving sum where args[1] holds the filter
            // coefficients. For output position i, sum over the window of the last LEN(filter)
            // samples ending at i, weighting sample s[i-k] by filter[k+1]. Out-of-range taps
            // (before the series start) contribute 0. normalize (args[2]) divides by the sum of
            // coefficients. center is ignored (acceptable approximation).
            "series_fir" => SeriesFir(args),

            // IIR (infinite impulse response): recursive filter. A true recursion isn't expressible
            // as a single list lambda; emit a runnable FIR-style approximation using only the
            // numerator (feed-forward) coefficients, which keeps the result a sensible same-length
            // list. (Approximation — priority is that it executes.)
            "series_iir" => SeriesFir(new[] { args[0], args[1] }),

            // Statistics. Dynamic form returns a property bag; the multi-column series_stats() form
            // is also emitted as a bag here (true multi-column expansion is structural and out of
            // scope) so the SQL still executes.
            "series_stats_dynamic" or "series_stats" => SeriesStats(args[0]),

            // Linear regression (least squares) over the list, using the element index (0-based) as
            // the independent variable. Returns slope/intercept; dynamic form returns a full bag.
            "series_fit_line" or "series_fit_line_dynamic" => SeriesFitLine(args[0]),

            // Pearson correlation coefficient of two equal-length lists.
            "series_pearson_correlation" => SeriesPearson(args[0], args[1]),

            // Outlier scores via Tukey-style/z-score: distance from the mean in stdevs. Returns a
            // same-length list of scores (approximation of Kusto's median-based detector).
            "series_outliers" => SeriesOutliers(args[0]),

            // Seasonality/period detection. A real autocorrelation scan is heavy; emit a runnable
            // bag with a no-period sentinel so downstream SQL binds. (Approximation.)
            "series_periods_detect" => "json_object('periods', [], 'scores', [])",
            "series_periods_validate" => $"LIST_TRANSFORM(GENERATE_SERIES(2, LEN(LIST_VALUE({string.Join(", ", args.Skip(1))}))), i -> 0.0)",

            // Array/dynamic functions
            // strcat_array(array, delimiter) joins array elements — a scalar (the aggregate map has a
            // same-named string_agg for the aggregate form; this is the scalar array form).
            "strcat_array" when args.Length == 2 => $"ARRAY_TO_STRING({CoerceArr(args[0])}, {args[1]})",
            // Arrays reach these either as native LIST (dynamic([..])/split) or as a JSON value
            // (from dynamic property/index navigation, e.g. d.a). CoerceArr normalizes a JSON array
            // to a native LIST<JSON> via '$[*]' so LIST_* builtins type-check on both forms. When an
            // input was JSON, the array-returning result is wrapped in TO_JSON so it serializes as a
            // JSON array (numbers unquoted) matching the dynamic oracle, and stays navigable downstream.
            "array_length" => $"LEN({CoerceArr(args[0])})",
            "array_index_of" => $"(LIST_POSITION({CoerceArr(args[0])}, {args[1]}) - 1)",
            "array_sort_asc" => WrapArr(IsJsonArg(args[0]), $"LIST_SORT({CoerceArr(args[0])})"),
            "array_sort_desc" => WrapArr(IsJsonArg(args[0]), $"LIST_REVERSE_SORT({CoerceArr(args[0])})"),
            "array_concat" => WrapArr(args.Any(IsJsonArg), $"LIST_CONCAT({string.Join(", ", args.Select(CoerceArr))})"),
            "array_reverse" => WrapArr(IsJsonArg(args[0]), $"LIST_REVERSE({CoerceArr(args[0])})"),
            "array_slice" when args.Length == 3 => WrapArr(IsJsonArg(args[0]), $"LIST_SLICE({CoerceArr(args[0])}, {args[1]} + 1, {args[2]} + 1)"),
            // array_sum: elements may be JSON text — cast each to DOUBLE before summing.
            "array_sum" => IsJsonArg(args[0])
                ? $"LIST_SUM(LIST_TRANSFORM({CoerceArr(args[0])}, x -> CAST(x AS DOUBLE)))"
                : $"LIST_SUM({args[0]})",
            "bag_keys" => $"JSON_KEYS({args[0]})",
            "bag_has_key" => $"(JSON_EXTRACT({args[0]}, '$.' || TRIM({args[1]}, '\"''')) IS NOT NULL)",
            "bag_merge" => $"JSON_MERGE_PATCH({args[0]}, {args[1]})",
            "set_difference" => $"LIST_FILTER({args[0]}, x -> NOT LIST_CONTAINS({args[1]}, x))",
            "set_intersect" => $"LIST_FILTER({args[0]}, x -> LIST_CONTAINS({args[1]}, x))",
            "set_union" => $"LIST_DISTINCT(LIST_CONCAT({args[0]}, {args[1]}))",
            "treepath" => $"JSON_KEYS({args[0]})",
            "zip" => $"LIST_ZIP({string.Join(", ", args)})",

            // Bitwise functions
            "binary_and" => $"({args[0]} & {args[1]})",
            "binary_or" => $"({args[0]} | {args[1]})",
            "binary_xor" => $"XOR({args[0]}, {args[1]})",
            "binary_not" => $"(~{args[0]})",
            "binary_shift_left" => $"({args[0]} << {args[1]})",
            "binary_shift_right" => $"({args[0]} >> {args[1]})",

            // Additional string functions
            "extract_all" => $"REGEXP_EXTRACT_ALL({args[0]}, {args[1]})",
            "replace_regex" => $"REGEXP_REPLACE({args[0]}, {args[1]}, {args[2]})",
            "parse_csv" => $"STRING_SPLIT({args[0]}, ',')",
            "dynamic_to_json" => $"CAST({args[0]} AS JSON)",
            "tohex" => $"PRINTF('%x', {args[0]})",
            "bag_remove_keys" => $"json_merge_patch({args[0]}, json_object({string.Join(", ", args.Skip(1).Select(a => $"{a}, NULL"))}))",

            // Timezone functions
            "datetime_local_to_utc" => args.Length >= 2 ? $"({args[0]} AT TIME ZONE {args[1]} AT TIME ZONE 'UTC')" : $"({args[0]} AT TIME ZONE 'UTC')",
            "datetime_utc_to_local" => args.Length >= 2 ? $"({args[0]} AT TIME ZONE 'UTC' AT TIME ZONE {args[1]})" : args[0],

            // JSON functions
            "extract_json" or "extractjson" => $"JSON_EXTRACT({args[1]}, {args[0]})",

            // Window functions (beyond row_number/prev/next)
            "row_cumsum" => $"SUM({args[0]}) OVER (ROWS UNBOUNDED PRECEDING)",
            "row_rank_dense" => "DENSE_RANK() OVER ()",
            "row_rank_min" => "RANK() OVER ()",
            "row_window_session" => $"SUM(CASE WHEN {args[0]} > LAG({args[0]}) OVER () + {args[1]} THEN 1 ELSE 0 END) OVER (ROWS UNBOUNDED PRECEDING)",

            // Math functions
            "degrees" => $"DEGREES({args[0]})",
            "radians" => $"RADIANS({args[0]})",
            "cot" => $"(1.0 / TAN({args[0]}))",
            "gamma" => $"GAMMA({args[0]})",
            "loggamma" => $"LGAMMA({args[0]})",
            "bitset_count_ones" => $"BIT_COUNT({args[0]})",
            "range" when args.Length == 3 => $"GENERATE_SERIES({args[0]}, {args[1]}, {args[2]})",

            // Array/set functions
            "set_has_element" => $"LIST_CONTAINS({args[0]}, {args[1]})",
            "array_iff" => $"LIST_TRANSFORM(LIST_ZIP({args[0]}, {args[1]}, {args[2]}), x -> CASE WHEN x[1] THEN x[2] ELSE x[3] END)",
            "array_rotate_left" => $"LIST_CONCAT(LIST_SLICE({args[0]}, {args[1]} + 1, LEN({args[0]})), LIST_SLICE({args[0]}, 1, {args[1]}))",
            "array_rotate_right" => $"LIST_CONCAT(LIST_SLICE({args[0]}, LEN({args[0]}) - {args[1]} + 1, LEN({args[0]})), LIST_SLICE({args[0]}, 1, LEN({args[0]}) - {args[1]}))",
            "array_split" when args.Length == 2 => $"[LIST_SLICE({args[0]}, 1, {args[1]}), LIST_SLICE({args[0]}, {args[1]} + 1, LEN({args[0]}))]",
            "bag_set_key" => $"JSON_MERGE_PATCH({args[0]}, JSON_OBJECT({args[1]}, {args[2]}))",
            "pack_dictionary" => $"JSON_OBJECT({string.Join(", ", args)})",

            // String functions (additional)
            "replace_strings" when args.Length == 3 => $"LIST_REDUCE(LIST_ZIP({args[1]}, {args[2]}), {args[0]}, (acc, pair) -> REPLACE(acc, pair[1], pair[2]))",
            "strrep" => $"REPEAT({args[0]}, {args[1]})",
            "has_any_index" => $"LIST_POSITION(LIST_TRANSFORM({args[1]}, term -> CASE WHEN {args[0]} ILIKE '%' || term || '%' THEN 1 ELSE 0 END), 1) - 1",
            "parse_urlquery" => $"json_object('Query', REGEXP_EXTRACT({args[0]}, '\\?(.+)', 1))",

            // Hash functions (additional)
            "hash_combine" => $"HASH({args[0]}) # HASH({args[1]})",
            "hash_many" => $"HASH({string.Join(", ", args)})",
            "hash_xxhash64" => $"xxhash64({args[0]})",

            // IPv4 functions
            "parse_ipv4" => $"(SPLIT_PART({args[0]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[0]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[0]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[0]}, '.', 4)::BIGINT)",
            "parse_ipv4_mask" => $"(SPLIT_PART({args[0]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[0]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[0]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[0]}, '.', 4)::BIGINT) & ((-1::BIGINT) << (32 - {args[1]}))",
            "ipv4_compare" when args.Length == 2 => $"SIGN((SPLIT_PART({args[0]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[0]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[0]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[0]}, '.', 4)::BIGINT) - (SPLIT_PART({args[1]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[1]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[1]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[1]}, '.', 4)::BIGINT))",
            "ipv4_is_in_range" => $"((SPLIT_PART({args[0]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[0]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[0]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[0]}, '.', 4)::BIGINT) & ((-1::BIGINT) << (32 - SPLIT_PART({args[1]}, '/', 2)::INT))) = ((SPLIT_PART(SPLIT_PART({args[1]}, '/', 1), '.', 1)::BIGINT * 16777216 + SPLIT_PART(SPLIT_PART({args[1]}, '/', 1), '.', 2)::BIGINT * 65536 + SPLIT_PART(SPLIT_PART({args[1]}, '/', 1), '.', 3)::BIGINT * 256 + SPLIT_PART(SPLIT_PART({args[1]}, '/', 1), '.', 4)::BIGINT) & ((-1::BIGINT) << (32 - SPLIT_PART({args[1]}, '/', 2)::INT)))",
            "ipv4_is_private" => $"((SPLIT_PART({args[0]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[0]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[0]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[0]}, '.', 4)::BIGINT) BETWEEN 167772160 AND 184549375 OR (SPLIT_PART({args[0]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[0]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[0]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[0]}, '.', 4)::BIGINT) BETWEEN 2886729728 AND 2887778303 OR (SPLIT_PART({args[0]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[0]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[0]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[0]}, '.', 4)::BIGINT) BETWEEN 3232235520 AND 3232301055)",
            "format_ipv4" when args.Length == 1 => args[0],
            "format_ipv4" when args.Length == 2 => $"CONCAT(({args[0]}::BIGINT >> 24) & 255, '.', ({args[0]}::BIGINT >> 16) & 255, '.', ({args[0]}::BIGINT >> 8) & 255, '.', {args[0]}::BIGINT & 255)",

            // Base64 GUID variants
            "base64_encode_fromguid" => $"BASE64(CAST({args[0]} AS BLOB))",
            "base64_decode_toguid" => $"CAST(FROM_BASE64({args[0]}) AS UUID)",
            "base64_decode_toarray" => $"LIST_TRANSFORM(GENERATE_SERIES(1, OCTET_LENGTH(FROM_BASE64({args[0]}))), i -> GET_BIT(FROM_BASE64({args[0]}), i - 1))",

            // Parse functions
            "parse_version" => $"STRING_SPLIT({args[0]}, '.')",

            // Array shift functions
            "array_shift_left" when args.Length >= 2 => args.Length >= 3
                ? $"LIST_CONCAT(LIST_SLICE({args[0]}, {args[1]} + 1, LEN({args[0]})), LIST_TRANSFORM(GENERATE_SERIES(1, {args[1]}), x -> {args[2]}))"
                : $"LIST_CONCAT(LIST_SLICE({args[0]}, {args[1]} + 1, LEN({args[0]})), LIST_TRANSFORM(GENERATE_SERIES(1, {args[1]}), x -> NULL))",
            "array_shift_right" when args.Length >= 2 => args.Length >= 3
                ? $"LIST_CONCAT(LIST_TRANSFORM(GENERATE_SERIES(1, {args[1]}), x -> {args[2]}), LIST_SLICE({args[0]}, 1, LEN({args[0]}) - {args[1]}))"
                : $"LIST_CONCAT(LIST_TRANSFORM(GENERATE_SERIES(1, {args[1]}), x -> NULL), LIST_SLICE({args[0]}, 1, LEN({args[0]}) - {args[1]}))",

            // IPv4 additional
            "ipv4_is_match" when args.Length == 2 => $"((SPLIT_PART({args[0]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[0]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[0]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[0]}, '.', 4)::BIGINT) = (SPLIT_PART({args[1]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[1]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[1]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[1]}, '.', 4)::BIGINT))",
            "ipv4_is_match" when args.Length == 3 => $"(((SPLIT_PART({args[0]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[0]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[0]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[0]}, '.', 4)::BIGINT) & ((-1::BIGINT) << (32 - {args[2]}))) = ((SPLIT_PART({args[1]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[1]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[1]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[1]}, '.', 4)::BIGINT) & ((-1::BIGINT) << (32 - {args[2]}))))",
            "ipv4_netmask_suffix" => $"SPLIT_PART({args[0]}, '/', 2)::INT",
            "format_ipv4_mask" when args.Length == 2 => $"CONCAT(({args[0]}::BIGINT >> 24) & 255, '.', ({args[0]}::BIGINT >> 16) & 255, '.', ({args[0]}::BIGINT >> 8) & 255, '.', {args[0]}::BIGINT & 255, '/', {args[1]})",

            // Set functions
            "jaccard_index" => $"(LEN(LIST_FILTER({args[0]}, x -> LIST_CONTAINS({args[1]}, x)))::DOUBLE / LEN(LIST_DISTINCT(LIST_CONCAT({args[0]}, {args[1]})))::DOUBLE)",

            // Conditional / type functions
            "iff" => null, // handled structurally in ExpressionSqlBuilder
            // typeof() exposes the engine type name (used internally, e.g. buildschema). gettype()
            // must return Kusto type names — map DuckDB's TYPEOF onto long/real/string/bool/datetime/
            // timespan/guid/array/dictionary. JSON values resolve array vs dictionary via json_type.
            "gettype" => GetTypeKusto(args[0]),
            "typeof" => $"TYPEOF({args[0]})",
            "isnan" => $"ISNAN({args[0]})",
            "isinf" => $"ISINF({args[0]})",
            "isfinite" => $"ISFINITE({args[0]})",

            // Aggregate names that also appear inside scalar wrappers (e.g. toreal(any(Value)))
            // — alias to the DuckDB aggregate so the enclosing GROUP BY remains valid.
            "any" or "take_any" => $"ANY_VALUE({args[0]})",
            "anyif" or "take_anyif" => $"ANY_VALUE({args[0]}) FILTER (WHERE {args[1]})",
            "make_list" or "makelist" => $"LIST({args[0]}) FILTER (WHERE {args[0]} IS NOT NULL)",
            // make_set is an unordered aggregate: canonicalize order so DuckDB output is
            // deterministic and matches Kusto's set content (LIST_SORT == Kusto array_sort_asc ordinal order).
            "make_set" or "makeset" => $"LIST_SORT(LIST(DISTINCT {args[0]}) FILTER (WHERE {args[0]} IS NOT NULL))",
            "sumif" => SumPossiblyInterval(args[0], args[1]),
            "countif" => $"COUNT(*) FILTER (WHERE {args[0]})",
            "avgif" when args.Length >= 2 => $"AVG({args[0]}) FILTER (WHERE {args[1]})",
            "minif" => $"MIN({args[0]}) FILTER (WHERE {args[1]})",
            "maxif" => $"MAX({args[0]}) FILTER (WHERE {args[1]})",
            "percentile" when args.Length == 2 => $"quantile_disc({args[0]}, {args[1]} / 100.0)",
            "percentilew" when args.Length == 3 => $"quantile_disc({args[0]}, {args[2]} / 100.0)",
            "dcount" => $"APPROX_COUNT_DISTINCT({args[0]})",
            "dcountif" => $"APPROX_COUNT_DISTINCT({args[0]}) FILTER (WHERE {args[1]})",
            "count_distinct" => $"COUNT(DISTINCT {args[0]})",
            "count_distinctif" => $"COUNT(DISTINCT {args[0]}) FILTER (WHERE {args[1]})",
            "stdev" => $"STDDEV_SAMP({args[0]})",
            "stdevp" => $"STDDEV_POP({args[0]})",
            "variance" => $"VAR_SAMP({args[0]})",
            "variancep" => $"VAR_POP({args[0]})",

            _ => null
        };
    }

    public string? TryTranslateAggregate(string name, string[] args)
    {
        return name switch
        {
            "count" => "COUNT(*)",
            "sum" => SumPossiblyInterval(args[0], null),
            "sumif" => SumPossiblyInterval(args[0], args.Length >= 2 ? args[1] : null),
            // Kusto avg/avgif over an empty (or all-null) group -> NaN; DuckDB AVG -> NULL. COALESCE to NaN.
            "avg" => $"COALESCE(AVG({args[0]}), 'nan'::DOUBLE)",
            "avgif" => $"COALESCE(AVG({args[0]}) FILTER (WHERE {args[1]}), 'nan'::DOUBLE)",
            "binary_all_and" => $"BIT_AND({args[0]})",
            "binary_all_or" => $"BIT_OR({args[0]})",
            "binary_all_xor" => $"BIT_XOR({args[0]})",
            "buildschema" => $"MIN(typeof({args[0]}))",
            "count_distinct" => $"COUNT(DISTINCT {args[0]})",
            "count_distinctif" => $"COUNT(DISTINCT {args[0]}) FILTER (WHERE {args[1]})",
            "countif" => $"COUNT(*) FILTER (WHERE {args[0]})",
            "covariance" => $"COVAR_SAMP({args[0]}, {args[1]})",
            "covarianceif" => $"COVAR_SAMP({args[0]}, {args[1]}) FILTER (WHERE {args[2]})",
            "covariancep" => $"COVAR_POP({args[0]}, {args[1]})",
            "covariancepif" => $"COVAR_POP({args[0]}, {args[1]}) FILTER (WHERE {args[2]})",
            // Kusto dcount is exact at low/medium cardinality (HLL accuracy level 1) and within
            // sub-1% at high cardinality; DuckDB APPROX_COUNT_DISTINCT over-estimates even at low
            // cardinality (e.g. 256 distinct -> 278). Use exact COUNT(DISTINCT) to match Kusto.
            // The optional accuracy argument (args[1]) is intentionally ignored.
            "dcount" => $"COUNT(DISTINCT {args[0]})",
            "dcountif" => $"COUNT(DISTINCT {args[0]}) FILTER (WHERE {args[1]})",
            "hll" => $"hll({args[0]})",
            "hll_if" => $"hll({args[0]}) FILTER (WHERE {args[1]})",
            "hll_merge" => $"hll_merge({args[0]})",
            "make_bag" => $"histogram({args[0]})",
            "make_bag_if" => $"histogram({args[0]}) FILTER (WHERE {args[1]})",
            // Kusto make_list/make_set IGNORE null values (only make_list_with_nulls keeps them).
            // Over an empty/all-null group Kusto -> [] (empty array); DuckDB LIST -> NULL, so COALESCE to [].
            "make_list" or "makelist" => $"COALESCE(LIST({args[0]}) FILTER (WHERE {args[0]} IS NOT NULL), [])",
            "make_list_if" or "makelistif" => $"COALESCE(LIST({args[0]}) FILTER (WHERE ({args[1]}) AND {args[0]} IS NOT NULL), [])",
            "make_list_with_nulls" => $"COALESCE(LIST({args[0]}), [])",
            "make_set" or "makeset" => $"COALESCE(LIST(DISTINCT {args[0]}) FILTER (WHERE {args[0]} IS NOT NULL), [])",
            "make_set_if" or "makesetif" => $"COALESCE(LIST(DISTINCT {args[0]}) FILTER (WHERE ({args[1]}) AND {args[0]} IS NOT NULL), [])",
            "min" => $"MIN({args[0]})",
            "minif" => $"MIN({args[0]}) FILTER (WHERE {args[1]})",
            "max" => $"MAX({args[0]})",
            "maxif" => $"MAX({args[0]}) FILTER (WHERE {args[1]})",
            "percentile" => $"quantile_disc({args[0]}, {args[1]} / 100.0)",
            "percentilew" => $"quantile_disc({args[0]}, {args[2]} / 100.0)",
            "stdev" => $"STDDEV_SAMP({args[0]})",
            "stdevif" => $"STDDEV_SAMP({args[0]}) FILTER (WHERE {args[1]})",
            "stdevp" => $"STDDEV_POP({args[0]})",
            "any" or "take_any" => $"ANY_VALUE({args[0]})",
            "strcat_array" => $"STRING_AGG({args[0]}, {args[1]})",
            "anyif" or "take_anyif" => $"ANY_VALUE({args[0]}) FILTER (WHERE {args[1]})",
            "variance" => $"VAR_SAMP({args[0]})",
            "varianceif" => $"VAR_SAMP({args[0]}) FILTER (WHERE {args[1]})",
            "variancep" => $"VAR_POP({args[0]})",
            "variancepif" => $"VAR_POP({args[0]}) FILTER (WHERE {args[1]})",
            _ => null
        };
    }

    public string MapType(string kustoType)
    {
        return kustoType.ToLowerInvariant() switch
        {
            "bool" or "boolean" => "BOOLEAN",
            "int" => "INT",
            "long" => "BIGINT",
            "real" => "DOUBLE",
            "decimal" => "DECIMAL",
            "datetime" => "TIMESTAMP",
            "date" => "DATE",
            "string" => "VARCHAR",
            "dynamic" => "JSON",
            "guid" => "UUID",
            _ => throw new NotSupportedException($"Unsupported type '{kustoType}'")
        };
    }

    public string JsonAccess(string baseSql, string jsonPath)
    {
        // Return the navigable JSON value (not a trimmed scalar string) so chained property/index
        // access and array functions operate consistently on the result, and so the dynamic value
        // matches the Kusto oracle's JSON serialization (e.g. d.a.b → {"c":42}, d.s → "hello").
        return $"json_extract({baseSql}, '$.{jsonPath}')";
    }

    /// <summary>KQL toint/tolong truncate toward zero (toint(2.6)=2, toint(-2.6)=-2); DuckDB's
    /// CAST(real AS INTEGER) rounds to nearest. Route integer targets through TRUNC(double) so the
    /// result matches Kusto. String inputs still parse (TRY_CAST(... AS DOUBLE)) and non-numeric → NULL,
    /// matching KQL's null-on-failure. Non-integer targets use the default TRY_CAST.</summary>
    /// <summary>KQL todatetime() parses many textual formats; DuckDB's CAST AS TIMESTAMP only accepts
    /// ISO-8601, silently yielding NULL for locale/US/abbreviated date strings (which then collapse a
    /// whole `summarize by bin(todatetime(col), 1d)` into a single NULL group). Fall back to
    /// try_strptime over common non-ISO formats. TRY_CAST still handles ISO input and already-typed
    /// timestamps first; casting to VARCHAR keeps try_strptime valid when expr is already a TIMESTAMP.</summary>
    public string ParseDateTime(string expr)
    {
        return $"COALESCE(TRY_CAST({expr} AS TIMESTAMP), TRY_STRPTIME(CAST({expr} AS VARCHAR), "
            + "['%-d.%-m.%Y, %H:%M:%S', '%-d.%-m.%Y %H:%M:%S', '%-d.%-m.%Y', "
            + "'%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y', "
            + "'%d-%b-%y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%d %b %Y %H:%M:%S', "
            + "'%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M']))";
    }

    public string SafeCast(string expr, string sqlType)
    {
        // KQL toint/tolong/toreal/todouble coerce a *dynamic* JSON boolean: true→1, false→0
        // (verified live: toint(dynamic(true))==1). Dynamic columns serialize to text here, so a
        // bare numeric cast of 'true'/'false' yields NULL — fall back to a boolean coercion when the
        // numeric parse fails. TRY_CAST('5' AS BOOLEAN) is NULL, so numeric strings keep numeric
        // semantics and only genuine boolean text routes through the fallback.
        if (string.Equals(sqlType, "INTEGER", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(sqlType, "BIGINT", StringComparison.OrdinalIgnoreCase))
            return $"TRY_CAST(TRUNC(COALESCE(TRY_CAST({expr} AS DOUBLE), TRY_CAST(TRY_CAST({expr} AS BOOLEAN) AS DOUBLE))) AS {sqlType})";
        if (string.Equals(sqlType, "DOUBLE", StringComparison.OrdinalIgnoreCase))
            return $"COALESCE(TRY_CAST({expr} AS DOUBLE), TRY_CAST(TRY_CAST({expr} AS BOOLEAN) AS DOUBLE))";
        return $"TRY_CAST({expr} AS {sqlType})";
    }

    public string SelectExclude(string[] columns)
    {
        return $"* EXCLUDE ({string.Join(", ", columns)})";
    }

    public string SelectRename(string[] mappings)
    {
        return $"* RENAME ({string.Join(", ", mappings)})";
    }

    public string Qualify(string innerSql, string condition)
    {
        if (innerSql.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase))
        {
            // QUALIFY must come before ORDER BY / LIMIT and belongs to a single SELECT block.
            // Wrap in a subquery when:
            //  - the SELECT already has a trailing top-level ORDER BY / LIMIT, or
            //  - the SQL is a UNION / EXCEPT / INTERSECT at the top level — QUALIFY can't attach
            //    to the set-op result; it must apply to a wrapped SELECT, or
            //  - the SELECT already ends with QUALIFY — chaining a second QUALIFY is invalid.
            if (HasTopLevelTail(innerSql, " ORDER BY ") ||
                HasTopLevelTail(innerSql, " LIMIT ") ||
                HasTopLevelTail(innerSql, " UNION ALL ") ||
                HasTopLevelTail(innerSql, " UNION ") ||
                HasTopLevelTail(innerSql, " EXCEPT ") ||
                HasTopLevelTail(innerSql, " INTERSECT ") ||
                HasTopLevelTail(innerSql, " QUALIFY ") ||
                // DuckDB: QUALIFY cannot combine with GROUP BY ALL in the same SELECT.
                HasTopLevelTail(innerSql, " GROUP BY "))
                return $"SELECT * FROM ({innerSql}) QUALIFY {condition}";
            return $"{innerSql} QUALIFY {condition}";
        }
        // innerSql does not start with SELECT — it may be a bare table name, a CTE reference,
        // or a set-op expression like "(SELECT ...) UNION ALL BY NAME (SELECT ...)".
        // Never blindly append QUALIFY; always wrap so the clause attaches to a fresh SELECT.
        if (HasTopLevelSetOp(innerSql))
            return $"SELECT * FROM ({innerSql}) QUALIFY {condition}";
        return $"SELECT * FROM {innerSql} QUALIFY {condition}";
    }

    private static bool HasMixedScalarTypes(string[] args)
    {
        bool sawString = false, sawNonString = false;
        foreach (var raw in args)
        {
            var a = raw.TrimStart('(').TrimStart();
            bool looksString = a.StartsWith("'") || a.StartsWith("\"")
                || a.StartsWith("CONCAT(", StringComparison.OrdinalIgnoreCase)
                || a.StartsWith("UPPER(", StringComparison.OrdinalIgnoreCase)
                || a.StartsWith("LOWER(", StringComparison.OrdinalIgnoreCase)
                || a.StartsWith("CAST(", StringComparison.OrdinalIgnoreCase) && a.Contains("AS VARCHAR", StringComparison.OrdinalIgnoreCase)
                || a.StartsWith("REPLACE(", StringComparison.OrdinalIgnoreCase)
                || a.StartsWith("SUBSTR(", StringComparison.OrdinalIgnoreCase);
            if (looksString) sawString = true;
            else sawNonString = true;
            if (sawString && sawNonString) return true;
        }
        return false;
    }

    /// <summary>True when an array argument is a JSON value (a dynamic-navigation result, dynamic
    /// literal, or JSON builtin) rather than a native LIST. Such inputs must be element-extracted with
    /// '$[*]' before LIST_* builtins, and array-returning results re-serialized with TO_JSON.</summary>
    private static bool IsJsonArg(string sql)
    {
        var t = sql.TrimEnd();
        var s = t.TrimStart('(').TrimStart();
        return t.EndsWith("::JSON", StringComparison.OrdinalIgnoreCase)
            || s.StartsWith("json_extract(", StringComparison.OrdinalIgnoreCase)
            || s.StartsWith("JSON_EXTRACT(", StringComparison.OrdinalIgnoreCase)
            || s.StartsWith("json_object(", StringComparison.OrdinalIgnoreCase)
            || s.StartsWith("JSON_MERGE_PATCH(", StringComparison.OrdinalIgnoreCase)
            || s.StartsWith("TO_JSON(", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Normalize an array argument so LIST_* builtins type-check. Native LIST expressions pass
    /// through unchanged; a JSON value (dynamic navigation result, e.g. json_extract(d, '$.a')) is
    /// converted to a native LIST&lt;JSON&gt; by extracting all elements with the '$[*]' wildcard path.</summary>
    private static string CoerceArr(string sql) =>
        IsJsonArg(sql) ? $"json_extract({sql}, '$[*]')" : sql;

    /// <summary>Re-serialize a LIST-producing array result as a JSON array (TO_JSON) when the input(s)
    /// were JSON — so the output matches the dynamic oracle's JSON serialization (unquoted numbers) and
    /// remains navigable. Native-LIST inputs are left as native lists (they already serialize correctly).</summary>
    private static string WrapArr(bool fromJson, string listSql) =>
        fromJson ? $"TO_JSON({listSql})" : listSql;

    private static bool IsArrayLikeText(string sql)
    {
        var t = sql.TrimStart('(').TrimEnd(')').TrimStart();
        if (t.StartsWith("LIST_VALUE", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("LIST(", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("SELECT LIST(", StringComparison.OrdinalIgnoreCase)) return true;
        if (sql.Contains("SELECT LIST(", StringComparison.OrdinalIgnoreCase) && sql.Contains("LIMIT 1")) return true;
        // Scalar-let substitutions for dynamic arrays emit something like '(SELECT l_filled FROM ... LIMIT 1)'.
        // We can't be 100% sure, but if the subquery selects a single column name that looks like a list,
        // skip the cast.
        return false;
    }

    /// <summary>DuckDB rejects sum(INTERVAL). Convert to epoch-ms sum and back.
    /// We detect intervals by the INTERVAL keyword appearing in the argument text — all
    /// our timespan emissions go through '(N * INTERVAL 1 millisecond)'.</summary>
    private static string SumPossiblyInterval(string valueExpr, string? filter)
    {
        bool isInterval = valueExpr.Contains("INTERVAL ", StringComparison.OrdinalIgnoreCase) ||
                          valueExpr.Contains(" AS INTERVAL", StringComparison.OrdinalIgnoreCase);
        if (!isInterval)
        {
            // Kusto sum/sumif over an empty group -> 0; DuckDB SUM -> NULL. COALESCE to 0.
            return filter is null
                ? $"COALESCE(SUM({valueExpr}), 0)"
                : $"COALESCE(SUM({valueExpr}) FILTER (WHERE {filter}), 0)";
        }
        // (timestamp 'epoch' + interval) → timestamp, then epoch_ms → BIGINT ms.
        var ms = $"EPOCH_MS(CAST(TIMESTAMP 'epoch' + ({valueExpr}) AS TIMESTAMP))";
        var inner = filter is null ? $"SUM({ms})" : $"SUM({ms}) FILTER (WHERE {filter})";
        return $"(({inner}) * INTERVAL '1 millisecond')";
    }

    /// <summary>
    /// Translate a KQL format_datetime() format string into a DuckDB STRFTIME format string.
    /// The argument arrives already rendered as a single-quoted SQL string literal (e.g.
    /// '''yyyy-MM-dd HH:mm:ss'''); we operate on its content and re-quote.
    ///
    /// KQL specifiers (.NET-style) → DuckDB strftime, mapped longest-first so "MM" never
    /// degrades into two "M"s. Any non-specifier text is copied verbatim. Specifiers that
    /// DuckDB cannot reproduce exactly (sub-second widths other than 3/6 digits — KQL ticks
    /// are 100ns/7 digits, DuckDB is microsecond/6) fall back to the nearest supported width.
    /// </summary>
    private static string TranslateDateTimeFormat(string sqlLiteral)
    {
        // Only translate genuine string literals; pass anything else (column, expr) through.
        if (sqlLiteral.Length < 2 || sqlLiteral[0] != '\'' || sqlLiteral[^1] != '\'')
            return sqlLiteral;
        var fmt = sqlLiteral.Substring(1, sqlLiteral.Length - 2);

        var sb = new System.Text.StringBuilder(fmt.Length + 8);
        int i = 0;
        while (i < fmt.Length)
        {
            char c = fmt[i];
            // length of the current run of identical specifier characters
            int run = 1;
            while (i + run < fmt.Length && fmt[i + run] == c) run++;

            string? mapped = c switch
            {
                'y' => run >= 4 ? "%Y" : run == 2 ? "%y" : "%-y",
                'M' => run >= 2 ? "%m" : "%-m",
                'd' => run >= 2 ? "%d" : "%-d",
                'H' => run >= 2 ? "%H" : "%-H",
                'h' => run >= 2 ? "%I" : "%-I",
                'm' => run >= 2 ? "%M" : "%-M",
                's' => run >= 2 ? "%S" : "%-S",
                'f' or 'F' => run >= 4 ? "%f" : "%g", // %g=3-digit ms, %f=6-digit us
                't' => "%p",
                _ => null,
            };

            if (mapped != null)
            {
                sb.Append(mapped);
                i += run;
            }
            else
            {
                // verbatim literal char (escape % so STRFTIME doesn't treat it as a specifier)
                if (c == '%') sb.Append('%');
                sb.Append(c);
                i++;
            }
        }

        return $"'{sb.ToString().Replace("'", "''")}'";
    }

    /// <summary>
    /// Render a KQL format_timespan(timespan, format) call. The format string is a .NET-style custom
    /// TimeSpan format (e.g. 'd.hh:mm:ss', 'hh:mm', 'ss.fff'). We compute total signed microseconds from
    /// the interval, take a leading sign + absolute components (days/hours/minutes/seconds/fraction) and
    /// concatenate per the format. Each specifier run uses its length for zero-padding (h vs hh, etc).
    /// 'h'/'m'/'s' are within-their-parent (hours 0-23, minutes 0-59, …) matching .NET semantics.
    /// </summary>
    private static string FormatTimespan(string intervalSql, string formatLiteral)
    {
        // Only handle genuine string-literal formats; otherwise fall back to a raw cast.
        if (formatLiteral.Length < 2 || formatLiteral[0] != '\'' || formatLiteral[^1] != '\'')
            return $"CAST({intervalSql} AS VARCHAR)";
        var fmt = formatLiteral.Substring(1, formatLiteral.Length - 2).Replace("''", "'");

        // Signed total microseconds, and the absolute value used for component math.
        var totalUs = $"(EXTRACT(EPOCH FROM ({intervalSql})) * 1000000)";
        var absUs = $"ABS({totalUs})";

        string Comp(long unitUs, long? modUnits) =>
            modUnits is { } m
                ? $"CAST(FLOOR(MOD({absUs}, {unitUs * m}) / {unitUs}) AS BIGINT)"
                : $"CAST(FLOOR({absUs} / {unitUs}) AS BIGINT)";
        string Pad(string numeric, int width) =>
            width <= 1 ? $"CAST({numeric} AS VARCHAR)" : $"LPAD(CAST({numeric} AS VARCHAR), {width}, '0')";

        var parts = new List<string>();
        var literalRun = new System.Text.StringBuilder();
        void FlushLiteral()
        {
            if (literalRun.Length == 0) return;
            parts.Add($"'{literalRun.ToString().Replace("'", "''")}'");
            literalRun.Clear();
        }

        int i = 0;
        while (i < fmt.Length)
        {
            char c = fmt[i];
            int run = 1;
            while (i + run < fmt.Length && fmt[i + run] == c) run++;

            switch (c)
            {
                case 'd':
                    FlushLiteral();
                    parts.Add(Pad(Comp(86400000000L, null), run));
                    i += run;
                    break;
                case 'h':
                    FlushLiteral();
                    parts.Add(Pad(Comp(3600000000L, 24), run));
                    i += run;
                    break;
                case 'H':
                    FlushLiteral();
                    parts.Add(Pad(Comp(3600000000L, 24), run));
                    i += run;
                    break;
                case 'm':
                    FlushLiteral();
                    parts.Add(Pad(Comp(60000000L, 60), run));
                    i += run;
                    break;
                case 's':
                    FlushLiteral();
                    parts.Add(Pad(Comp(1000000L, 60), run));
                    i += run;
                    break;
                case 'f':
                case 'F':
                {
                    FlushLiteral();
                    // N fractional digits of the second. Microsecond precision (max 6 meaningful);
                    // request beyond 6 is right-padded with zeros.
                    int n = run;
                    int meaningful = Math.Min(n, 6);
                    var fracUs = $"CAST(MOD({absUs}, 1000000) AS BIGINT)";
                    var digits = $"CAST(FLOOR({fracUs} / {(long)Math.Pow(10, 6 - meaningful)}) AS BIGINT)";
                    var padded = $"LPAD(CAST({digits} AS VARCHAR), {meaningful}, '0')";
                    if (n > 6) padded = $"({padded} || '{new string('0', n - 6)}')";
                    if (c == 'F')
                        // Uppercase F: omit if the fractional part is zero (no padding kept).
                        parts.Add($"CASE WHEN {fracUs} = 0 THEN '' ELSE {padded} END");
                    else
                        parts.Add(padded);
                    i += run;
                    break;
                }
                case '\\':
                    // escape next char as literal
                    if (i + 1 < fmt.Length) { literalRun.Append(fmt[i + 1]); i += 2; }
                    else { literalRun.Append(c); i++; }
                    break;
                default:
                    literalRun.Append(c);
                    i++;
                    break;
            }
        }
        FlushLiteral();

        var sign = $"CASE WHEN {totalUs} < 0 THEN '-' ELSE '' END";
        var body = parts.Count == 0 ? "''" : string.Join(" || ", parts);
        return $"({sign} || {body})";
    }

    /// <summary>Map DuckDB's TYPEOF onto Kusto's gettype() type names. JSON values disambiguate
    /// array vs dictionary via json_type; native LIST types ('...[]') are arrays.</summary>
    private static string GetTypeKusto(string arg)
    {
        var t = $"TYPEOF({arg})";
        return
            $"CASE " +
            $"WHEN {t} IN ('TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','USMALLINT','UINTEGER','UBIGINT','UHUGEINT') THEN 'long' " +
            $"WHEN {t} IN ('FLOAT','DOUBLE','REAL') OR {t} LIKE 'DECIMAL%' THEN 'real' " +
            $"WHEN {t} = 'VARCHAR' THEN 'string' " +
            $"WHEN {t} = 'BOOLEAN' THEN 'bool' " +
            $"WHEN {t} IN ('TIMESTAMP','DATE','TIMESTAMP WITH TIME ZONE','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') THEN 'datetime' " +
            $"WHEN {t} LIKE 'INTERVAL%' THEN 'timespan' " +
            $"WHEN {t} = 'UUID' THEN 'guid' " +
            $"WHEN {t} LIKE '%[]' OR {t} LIKE 'STRUCT%' OR {t} LIKE 'MAP%' THEN 'array' " +
            $"WHEN {t} = 'JSON' THEN (CASE WHEN json_type(CAST({arg} AS VARCHAR)) = 'ARRAY' THEN 'array' ELSE 'dictionary' END) " +
            $"ELSE LOWER({t}) END";
    }

    private static bool HasTopLevelSetOp(string sql)
        => HasTopLevelTail(sql, " UNION ") ||
           HasTopLevelTail(sql, " EXCEPT ") ||
           HasTopLevelTail(sql, " INTERSECT ");

    private static bool HasTopLevelTail(string sql, string clause)
    {
        int depth = 0;
        bool inStr = false;
        char quote = ' ';
        for (int i = 0; i <= sql.Length - clause.Length; i++)
        {
            var c = sql[i];
            if (inStr) { if (c == quote) inStr = false; continue; }
            if (c == '\'' || c == '"') { inStr = true; quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && string.Compare(sql, i, clause, 0, clause.Length, StringComparison.OrdinalIgnoreCase) == 0)
                return true;
        }
        return false;
    }

    // ── series_* helper SQL builders ───────────────────────────────────────────
    // Each returns a single runnable DuckDB scalar expression over the LIST `s`.

    /// <summary>Forward/backward fill. DuckDB list slicing errors on NULL-containing lists, so we
    /// filter the prefix/suffix INDICES by non-nullness (element indexing is NULL-safe) and pick the
    /// nearest. `missing` (optional) is an extra placeholder value also treated as a gap.</summary>
    private static string SeriesFillDirectional(string s, bool forward, string? missing)
    {
        var notMissing = missing is null
            ? $"{s}[j] IS NOT NULL"
            : $"({s}[j] IS NOT NULL AND {s}[j] <> {missing})";
        // forward: search the prefix indices 1..i, take the LAST non-missing.
        // backward: search the suffix indices i..n, take the FIRST non-missing.
        var range = forward ? "GENERATE_SERIES(1, i)" : $"GENERATE_SERIES(i, LEN({s}))";
        var pick = forward ? "LIST_LAST" : "LIST_FIRST";
        return
            $"LIST_TRANSFORM(GENERATE_SERIES(1, LEN({s})), i -> " +
            $"{pick}(LIST_TRANSFORM(LIST_FILTER({range}, j -> {notMissing}), j -> {s}[j])))";
    }

    /// <summary>Linear interpolation of nulls. For each index i find the nearest non-null index on
    /// the left (il) and right (ir) and interpolate. Single-sided gaps copy the available endpoint.
    /// Uses NULL-safe index filtering (no slicing, no subqueries-in-lambdas).</summary>
    private static string SeriesFillLinear(string s)
    {
        // il / ir computed inline per element via index filtering over the prefix/suffix.
        var il = $"LIST_LAST(LIST_FILTER(GENERATE_SERIES(1, i), j -> {s}[j] IS NOT NULL))";
        var ir = $"LIST_FIRST(LIST_FILTER(GENERATE_SERIES(i, LEN({s})), j -> {s}[j] IS NOT NULL))";
        return
            $"LIST_TRANSFORM(GENERATE_SERIES(1, LEN({s})), i -> CASE " +
            $"WHEN {s}[i] IS NOT NULL THEN {s}[i]::DOUBLE " +
            $"WHEN {il} IS NOT NULL AND {ir} IS NOT NULL THEN " +
            $"{s}[{il}]::DOUBLE + ({s}[{ir}] - {s}[{il}]) * ((i - {il})::DOUBLE / ({ir} - {il})) " +
            $"WHEN {il} IS NOT NULL THEN {s}[{il}]::DOUBLE " +
            $"WHEN {ir} IS NOT NULL THEN {s}[{ir}]::DOUBLE " +
            $"ELSE NULL END)";
    }

    /// <summary>FIR weighted moving sum. args[0]=series, args[1]=filter coefficients,
    /// optional args[2]=normalize (bool), args[3]=center (ignored).</summary>
    private static string SeriesFir(string[] args)
    {
        var s = args[0];
        var f = args.Length > 1 ? args[1] : "[1]";
        bool normalize = args.Length > 2 &&
            (args[2].Trim().Equals("true", StringComparison.OrdinalIgnoreCase) || args[2].Trim() == "1");
        // window output i = Σ_k filter[k+1] * s[i-k] ; taps before the start contribute 0.
        var conv = $"LIST_TRANSFORM(GENERATE_SERIES(1, LEN({s})), i -> " +
                   $"LIST_SUM(LIST_TRANSFORM(GENERATE_SERIES(1, LEN({f})), k -> " +
                   $"CASE WHEN i - k + 1 >= 1 THEN {f}[k] * COALESCE({s}[i - k + 1], 0) ELSE 0 END)))";
        if (normalize)
            return $"LIST_TRANSFORM({conv}, x -> x::DOUBLE / NULLIF(LIST_SUM(LIST_TRANSFORM({f}, c -> ABS(c))), 0))";
        return conv;
    }

    /// <summary>min/max/avg/stdev/variance/sum/count bag for series_stats[_dynamic].</summary>
    private static string SeriesStats(string s)
    {
        var clean = $"LIST_FILTER({s}, x -> x IS NOT NULL)";
        return
            $"json_object(" +
            $"'min', LIST_MIN({clean}), " +
            $"'min_idx', LIST_POSITION({s}, LIST_MIN({clean})) - 1, " +
            $"'max', LIST_MAX({clean}), " +
            $"'max_idx', LIST_POSITION({s}, LIST_MAX({clean})) - 1, " +
            $"'avg', LIST_AVG({clean}), " +
            $"'sum', LIST_SUM({clean}), " +
            $"'stdev', LIST_AGGREGATE({clean}, 'stddev_samp'), " +
            $"'variance', LIST_AGGREGATE({clean}, 'var_samp')" +
            $")";
    }

    /// <summary>Least-squares line fit over the 0-based element index. Returns a bag with
    /// slope/interception/rsquare plus the fitted line as a same-length list.</summary>
    private static string SeriesFitLine(string s)
    {
        var n = $"LEN({s})::DOUBLE";
        var xs = $"GENERATE_SERIES(0, LEN({s}) - 1)";                 // 0..n-1
        var idx1 = $"GENERATE_SERIES(1, LEN({s}))";                    // 1..n (for s[i])
        var sumX = $"LIST_SUM({xs})::DOUBLE";
        var sumY = $"LIST_SUM({s})::DOUBLE";
        var sumXX = $"LIST_SUM(LIST_TRANSFORM({xs}, x -> x * x))::DOUBLE";
        var sumXY = $"LIST_SUM(LIST_TRANSFORM({idx1}, i -> (i - 1) * {s}[i]))::DOUBLE";
        // slope = (n*Σxy - Σx*Σy) / (n*Σxx - Σx²); intercept = (Σy - slope*Σx)/n
        var denom = $"NULLIF(({n} * {sumXX} - {sumX} * {sumX}), 0)";
        var slope = $"(({n} * {sumXY} - {sumX} * {sumY}) / {denom})";
        var intercept = $"(({sumY} - ({slope}) * {sumX}) / NULLIF({n}, 0))";
        var line = $"LIST_TRANSFORM({xs}, x -> ({intercept}) + ({slope}) * x)";
        // rsquare = 1 - SSres/SStot
        var mean = $"({sumY} / NULLIF({n}, 0))";
        var ssRes = $"LIST_SUM(LIST_TRANSFORM({idx1}, i -> POWER({s}[i] - (({intercept}) + ({slope}) * (i - 1)), 2)))";
        var ssTot = $"LIST_SUM(LIST_TRANSFORM({s}, y -> POWER(y - {mean}, 2)))";
        var rsquare = $"(1 - ({ssRes}) / NULLIF({ssTot}, 0))";
        return
            $"json_object('slope', {slope}, 'interception', {intercept}, " +
            $"'rsquare', {rsquare}, 'line_fit', {line})";
    }

    /// <summary>Pearson correlation coefficient of two equal-length lists.</summary>
    private static string SeriesPearson(string a, string b)
    {
        var nlen = $"LEAST(LEN({a}), LEN({b}))";
        var idx = $"GENERATE_SERIES(1, {nlen})";
        var n = $"{nlen}::DOUBLE";
        var sx = $"LIST_SUM(LIST_TRANSFORM({idx}, i -> {a}[i]))::DOUBLE";
        var sy = $"LIST_SUM(LIST_TRANSFORM({idx}, i -> {b}[i]))::DOUBLE";
        var sxy = $"LIST_SUM(LIST_TRANSFORM({idx}, i -> {a}[i] * {b}[i]))::DOUBLE";
        var sxx = $"LIST_SUM(LIST_TRANSFORM({idx}, i -> {a}[i] * {a}[i]))::DOUBLE";
        var syy = $"LIST_SUM(LIST_TRANSFORM({idx}, i -> {b}[i] * {b}[i]))::DOUBLE";
        var num = $"({n} * {sxy} - {sx} * {sy})";
        var den = $"SQRT(NULLIF(({n} * {sxx} - {sx} * {sx}) * ({n} * {syy} - {sy} * {sy}), 0))";
        return $"({num} / NULLIF({den}, 0))";
    }

    /// <summary>Per-element outlier score: (x - mean) / stdev (z-score). Same-length list.</summary>
    private static string SeriesOutliers(string s)
    {
        var clean = $"LIST_FILTER({s}, x -> x IS NOT NULL)";
        var mean = $"LIST_AVG({clean})";
        var sd = $"NULLIF(LIST_AGGREGATE({clean}, 'stddev_samp'), 0)";
        return $"LIST_TRANSFORM({s}, x -> CASE WHEN x IS NULL THEN NULL ELSE (x - {mean}) / {sd} END)";
    }

    public string GenerateSeries(string alias, string start, string end, string step)
    {
        // Datetime/timespan range: the step is an INTERVAL, so generate a timestamp series directly
        // (casting a TIMESTAMP to BIGINT is invalid). generate_series is end-inclusive, matching KQL range.
        if (step.Contains("INTERVAL", StringComparison.OrdinalIgnoreCase))
            return $"SELECT generate_series AS {alias} FROM generate_series({start}, {end}, {step})";
        return $"SELECT generate_series AS {alias} FROM generate_series(CAST({start} AS BIGINT), CAST({end} AS BIGINT), CAST({step} AS BIGINT))";
    }

    public string Unnest(string sourceAlias, string column, string unnestAlias)
    {
        return $"CROSS JOIN UNNEST({sourceAlias}.{column}) AS {unnestAlias}(value)";
    }

    public bool SupportsGroupByAll => true;

    public string? SampleClause(string fromSql, string count)
    {
        return $"SELECT * FROM {fromSql} USING SAMPLE {count} ROWS";
    }
}
