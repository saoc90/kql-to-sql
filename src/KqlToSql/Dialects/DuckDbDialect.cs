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
            "bag_pack" => $"json_object({string.Join(", ", args)})",
            "tolower" => $"LOWER({args[0]})",
            "toupper" => $"UPPER({args[0]})",
            "strlen" => $"LENGTH({args[0]})",
            "now" => "NOW()",
            "pack_array" => $"LIST_VALUE({string.Join(", ", args)})",
            "isempty" => $"({args[0]} IS NULL OR CAST({args[0]} AS VARCHAR) = '')",
            "isnotempty" or "isnotnull" => $"({args[0]} IS NOT NULL)",
            "isnull" => $"({args[0]} IS NULL)",
            "not" => $"NOT ({args[0]})",
            "strcat" => $"CONCAT({string.Join(", ", args)})",
            "replace_string" => $"REPLACE({string.Join(", ", args)})",
            "indexof" => $"(INSTR({args[0]}, {args[1]}) - 1)",
            "coalesce" => $"COALESCE({string.Join(", ", args)})",
            "countof" => $"(LENGTH({args[0]}) - LENGTH(REPLACE({args[0]}, {args[1]}, ''))) / LENGTH({args[1]})",
            "reverse" => $"REVERSE({args[0]})",
            "split" => $"STRING_SPLIT({args[0]}, {args[1]})",
            "floor" => $"FLOOR({args[0]})",
            "ceiling" => $"CEILING({args[0]})",
            "abs" => $"ABS({args[0]})",
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
            "parse_json" or "todynamic" => $"CAST({args[0]} AS JSON)",
            "format_datetime" => $"STRFTIME({args[0]}, {args[1]})",
            "startofday" => $"DATE_TRUNC('day', {args[0]})",
            "startofweek" => $"DATE_TRUNC('week', {args[0]})",
            "startofmonth" => $"DATE_TRUNC('month', {args[0]})",
            "startofyear" => $"DATE_TRUNC('year', {args[0]})",
            "endofday" => $"DATE_TRUNC('day', {args[0]}) + INTERVAL '1 day' - INTERVAL '1 microsecond'",
            "endofweek" => $"DATE_TRUNC('week', {args[0]}) + INTERVAL '7 days' - INTERVAL '1 microsecond'",
            "endofmonth" => $"DATE_TRUNC('month', {args[0]}) + INTERVAL '1 month' - INTERVAL '1 microsecond'",
            "endofyear" => $"DATE_TRUNC('year', {args[0]}) + INTERVAL '1 year' - INTERVAL '1 microsecond'",
            "min_of" => $"LEAST({string.Join(", ", args)})",
            "max_of" => $"GREATEST({string.Join(", ", args)})",
            "row_number" => "ROW_NUMBER() OVER ()",
            "prev" => $"LAG({args[0]}) OVER ()",
            "next" => $"LEAD({args[0]}) OVER ()",

            // Date/time functions
            "dayofweek" => $"EXTRACT(DOW FROM {args[0]})",
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
            "make_datetime" when args.Length == 1 => $"CAST({args[0]} AS TIMESTAMP)",
            "make_timespan" when args.Length == 3 =>
                $"({args[0]} * INTERVAL '1 hour' + {args[1]} * INTERVAL '1 minute' + {args[2]} * INTERVAL '1 second')",
            "unixtime_seconds_todatetime" => $"TO_TIMESTAMP({args[0]})",
            "unixtime_milliseconds_todatetime" => $"TO_TIMESTAMP_MS({args[0]})",
            "unixtime_microseconds_todatetime" => $"TO_TIMESTAMP_US({args[0]})",
            "unixtime_nanoseconds_todatetime" => $"TO_TIMESTAMP_NS({args[0]})",
            "datetime_part" => $"EXTRACT({args[0].Trim('\'')} FROM {args[1]})",
            "format_timespan" => $"CAST({args[0]} AS VARCHAR)",

            // String functions
            "parse_url" => $"json_object('Scheme', REGEXP_EXTRACT({args[0]}, '^(\\w+)://', 1), 'Host', REGEXP_EXTRACT({args[0]}, '://([^/:]+)', 1), 'Port', REGEXP_EXTRACT({args[0]}, ':(\\d+)', 1), 'Path', REGEXP_EXTRACT({args[0]}, '://[^/]+([\\/][^?#]*)', 1))",
            "base64_encode_tostring" or "base64_encode_fromarray" => $"BASE64(CAST({args[0]} AS BLOB))",
            "base64_decode_tostring" => $"CAST(FROM_BASE64({args[0]}) AS VARCHAR)",
            "translate" => $"TRANSLATE({args[0]}, {args[1]}, {args[2]})",
            "strcmp" => $"CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} = {args[1]} THEN 0 ELSE 1 END",
            "string_size" => $"OCTET_LENGTH({args[0]})",
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

            // Array/dynamic functions
            "array_length" => $"LEN({args[0]})",
            "array_index_of" => $"(LIST_POSITION({args[0]}, {args[1]}) - 1)",
            "array_sort_asc" => $"LIST_SORT({args[0]})",
            "array_sort_desc" => $"LIST_REVERSE_SORT({args[0]})",
            "array_concat" => $"LIST_CONCAT({string.Join(", ", args)})",
            "array_reverse" => $"LIST_REVERSE({args[0]})",
            "array_slice" when args.Length == 3 => $"LIST_SLICE({args[0]}, {args[1]} + 1, {args[2]} + 1)",
            "array_sum" => $"LIST_SUM({args[0]})",
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

            // Conditional / type functions
            "iff" => null, // handled structurally in ExpressionSqlBuilder
            "gettype" or "typeof" => $"TYPEOF({args[0]})",
            "isnan" => $"ISNAN({args[0]})",
            "isinf" => $"ISINF({args[0]})",
            "isfinite" => $"ISFINITE({args[0]})",

            _ => null
        };
    }

    public string? TryTranslateAggregate(string name, string[] args)
    {
        return name switch
        {
            "count" => "COUNT(*)",
            "sum" => $"SUM({args[0]})",
            "avg" => $"AVG({args[0]})",
            "avgif" => $"AVG({args[0]}) FILTER (WHERE {args[1]})",
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
            "dcount" => $"APPROX_COUNT_DISTINCT({args[0]})",
            "dcountif" => $"APPROX_COUNT_DISTINCT({args[0]}) FILTER (WHERE {args[1]})",
            "hll" => $"hll({args[0]})",
            "hll_if" => $"hll({args[0]}) FILTER (WHERE {args[1]})",
            "hll_merge" => $"hll_merge({args[0]})",
            "make_bag" => $"histogram({args[0]})",
            "make_bag_if" => $"histogram({args[0]}) FILTER (WHERE {args[1]})",
            "make_list" => $"LIST({args[0]})",
            "make_list_if" => $"LIST({args[0]}) FILTER (WHERE {args[1]})",
            "make_list_with_nulls" => $"LIST({args[0]})",
            "make_set" => $"LIST(DISTINCT {args[0]})",
            "make_set_if" => $"LIST(DISTINCT {args[0]}) FILTER (WHERE {args[1]})",
            "min" => $"MIN({args[0]})",
            "minif" => $"MIN({args[0]}) FILTER (WHERE {args[1]})",
            "max" => $"MAX({args[0]})",
            "maxif" => $"MAX({args[0]}) FILTER (WHERE {args[1]})",
            "percentile" => $"quantile_cont({args[0]}, {args[1]} / 100.0)",
            "percentilew" => $"quantile_cont({args[0]}, {args[2]} / 100.0)",
            "stdev" => $"STDDEV_SAMP({args[0]})",
            "stdevif" => $"STDDEV_SAMP({args[0]}) FILTER (WHERE {args[1]})",
            "stdevp" => $"STDDEV_POP({args[0]})",
            "sumif" => $"SUM({args[0]}) FILTER (WHERE {args[1]})",
            "take_any" => $"ANY_VALUE({args[0]})",
            "take_anyif" => $"ANY_VALUE({args[0]}) FILTER (WHERE {args[1]})",
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
        return $"trim(both '\"' from json_extract({baseSql}, '$.{jsonPath}'))";
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
            return $"{innerSql} QUALIFY {condition}";
        return $"SELECT * FROM {innerSql} QUALIFY {condition}";
    }

    public string GenerateSeries(string alias, string start, string end, string step)
    {
        return $"SELECT generate_series AS {alias} FROM generate_series({start}, {end}, {step})";
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
