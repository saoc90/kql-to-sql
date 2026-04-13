using System;
using System.Collections.Generic;
using System.Linq;

namespace KqlToSql.Dialects;

/// <summary>
/// SQL dialect implementation for PGlite (PostgreSQL WASM).
/// PGlite is compiled from PostgreSQL source and supports standard PostgreSQL SQL.
/// </summary>
public class PGliteDialect : ISqlDialect
{
    public string Name => "PGlite";

    public string CaseInsensitiveLike => "ILIKE";

    public string? TryTranslateFunction(string name, string[] args)
    {
        return name switch
        {
            "bag_pack" => $"jsonb_build_object({string.Join(", ", args)})",
            "tolower" => $"LOWER({args[0]})",
            "toupper" => $"UPPER({args[0]})",
            "strlen" => $"LENGTH({args[0]})",
            "now" => "NOW()",
            "pack_array" => $"ARRAY[{string.Join(", ", args)}]",
            "isempty" => $"({args[0]} IS NULL OR CAST({args[0]} AS TEXT) = '')",
            "isnotempty" or "isnotnull" => $"({args[0]} IS NOT NULL)",
            "isnull" => $"({args[0]} IS NULL)",
            "not" => $"NOT ({args[0]})",
            "strcat" => $"CONCAT({string.Join(", ", args)})",
            "replace_string" => $"REPLACE({string.Join(", ", args)})",
            "indexof" => $"(POSITION({args[1]} IN {args[0]}) - 1)",
            "coalesce" => $"COALESCE({string.Join(", ", args)})",
            "countof" => $"(LENGTH({args[0]}) - LENGTH(REPLACE({args[0]}, {args[1]}, ''))) / LENGTH({args[1]})",
            "reverse" => $"REVERSE({args[0]})",
            "split" => $"string_to_array({args[0]}, {args[1]})",
            "floor" => $"FLOOR({args[0]})",
            "ceiling" => $"CEILING({args[0]})",
            "abs" => $"ABS({args[0]})",
            "sqrt" => $"SQRT({args[0]})",
            "log" => $"LN({args[0]})",
            "log10" => $"LOG(10, {args[0]})",
            "log2" => $"LOG(2, {args[0]})",
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
            "parse_json" or "todynamic" => $"CAST({args[0]} AS JSONB)",
            "format_datetime" => $"TO_CHAR({args[0]}, {args[1]})",
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

            // Window functions
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
            "unixtime_milliseconds_todatetime" => $"TO_TIMESTAMP({args[0]}::double precision / 1000)",
            "unixtime_microseconds_todatetime" => $"TO_TIMESTAMP({args[0]}::double precision / 1000000)",
            "unixtime_nanoseconds_todatetime" => $"TO_TIMESTAMP({args[0]}::double precision / 1000000000)",
            "datetime_part" => $"EXTRACT({args[0].Trim('\'')} FROM {args[1]})",
            "format_timespan" => $"CAST({args[0]} AS TEXT)",

            // String functions
            "parse_url" => $"jsonb_build_object('Scheme', split_part({args[0]}, '://', 1), 'Host', split_part(split_part({args[0]}, '://', 2), '/', 1))",
            "base64_encode_tostring" or "base64_encode_fromarray" => $"ENCODE(CAST({args[0]} AS BYTEA), 'base64')",
            "base64_decode_tostring" => $"CONVERT_FROM(DECODE({args[0]}, 'base64'), 'UTF8')",
            "translate" => $"TRANSLATE({args[0]}, {args[1]}, {args[2]})",
            "strcmp" => $"CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} = {args[1]} THEN 0 ELSE 1 END",
            "string_size" => $"OCTET_LENGTH({args[0]})",
            "repeat" => $"REPEAT({args[0]}, {args[1]})",
            "unicode" => $"ASCII({args[0]})",
            "make_string" => $"CHR({args[0]})",
            "parse_path" => $"SUBSTRING({args[0]} FROM '[^/\\\\]+$')",
            "to_utf8" => $"CAST({args[0]} AS BYTEA)",

            // Hash functions
            "hash" => $"HASHTEXTEXTENDED({args[0]}, 0)",
            "hash_md5" => $"MD5({args[0]})",
            "hash_sha256" => $"ENCODE(SHA256(CAST({args[0]} AS BYTEA)), 'hex')",
            "hash_sha1" => $"ENCODE(DIGEST(CAST({args[0]} AS BYTEA), 'sha1'), 'hex')",

            // Array/dynamic functions
            "array_length" => $"ARRAY_LENGTH({args[0]}, 1)",
            "array_index_of" => $"(ARRAY_POSITION({args[0]}, {args[1]}) - 1)",
            "array_sort_asc" => $"(SELECT ARRAY_AGG(x ORDER BY x) FROM UNNEST({args[0]}) x)",
            "array_sort_desc" => $"(SELECT ARRAY_AGG(x ORDER BY x DESC) FROM UNNEST({args[0]}) x)",
            "array_concat" => $"({string.Join(" || ", args)})",
            "array_reverse" => $"(SELECT ARRAY_AGG(x) FROM (SELECT UNNEST({args[0]}) x ORDER BY ORDINALITY DESC) t)",
            "array_slice" when args.Length == 3 => $"{args[0]}[{args[1]}+1:{args[2]}+1]",
            "bag_keys" => $"(SELECT ARRAY_AGG(key) FROM JSONB_OBJECT_KEYS({args[0]}) key)",
            "bag_has_key" => $"({args[0]}::jsonb ? {args[1]})",
            "bag_merge" => $"({args[0]}::jsonb || {args[1]}::jsonb)",
            "set_difference" => $"(SELECT ARRAY_AGG(x) FROM UNNEST({args[0]}) x WHERE x <> ALL({args[1]}))",
            "set_intersect" => $"(SELECT ARRAY_AGG(x) FROM UNNEST({args[0]}) x WHERE x = ANY({args[1]}))",
            "set_union" => $"(SELECT ARRAY_AGG(DISTINCT x) FROM UNNEST({args[0]} || {args[1]}) x)",
            "zip" => $"(SELECT ARRAY_AGG(ARRAY[a,b]) FROM UNNEST({args[0]}, {args[1]}) AS t(a,b))",

            // Bitwise functions
            "binary_and" => $"({args[0]} & {args[1]})",
            "binary_or" => $"({args[0]} | {args[1]})",
            "binary_xor" => $"({args[0]} # {args[1]})",
            "binary_not" => $"(~{args[0]})",
            "binary_shift_left" => $"({args[0]} << {args[1]})",
            "binary_shift_right" => $"({args[0]} >> {args[1]})",

            // Additional string functions
            "extract_all" => $"(SELECT ARRAY_AGG(m[1]) FROM REGEXP_MATCHES({args[0]}, {args[1]}, 'g') m)",
            "replace_regex" => $"REGEXP_REPLACE({args[0]}, {args[1]}, {args[2]}, 'g')",
            "parse_csv" => $"STRING_TO_ARRAY({args[0]}, ',')",
            "dynamic_to_json" => $"TO_JSON({args[0]})",
            "tohex" => $"TO_HEX({args[0]}::bigint)",
            "bag_remove_keys" => $"({args[0]}::jsonb - ARRAY[{string.Join(", ", args.Skip(1))}]::text[])",

            // Timezone functions
            "datetime_local_to_utc" => args.Length >= 2 ? $"({args[0]} AT TIME ZONE {args[1]} AT TIME ZONE 'UTC')" : $"({args[0]} AT TIME ZONE 'UTC')",
            "datetime_utc_to_local" => args.Length >= 2 ? $"({args[0]} AT TIME ZONE 'UTC' AT TIME ZONE {args[1]})" : args[0],

            // JSON functions
            "extract_json" or "extractjson" => $"({args[1]}::jsonb #>> string_to_array(TRIM({args[0]}, '$.'), '.'))",

            // Window functions
            "row_cumsum" => $"SUM({args[0]}) OVER (ROWS UNBOUNDED PRECEDING)",
            "row_rank_dense" => "DENSE_RANK() OVER ()",
            "row_rank_min" => "RANK() OVER ()",

            // Math functions
            "degrees" => $"DEGREES({args[0]})",
            "radians" => $"RADIANS({args[0]})",
            "cot" => $"(1.0 / TAN({args[0]}))",
            "gamma" => $"(EXP(LGAMMA({args[0]})))",
            "loggamma" => $"LGAMMA({args[0]})",
            "bitset_count_ones" => $"BIT_COUNT({args[0]}::bit(64))",

            // Array/set functions
            "set_has_element" => $"({args[1]} = ANY({args[0]}))",
            "bag_set_key" => $"({args[0]}::jsonb || jsonb_build_object({args[1]}, {args[2]}))",
            "strrep" => $"REPEAT({args[0]}, {args[1]})",
            "has_any_index" => $"(SELECT MIN(i) - 1 FROM UNNEST({args[1]}) WITH ORDINALITY AS t(term, i) WHERE {args[0]} ILIKE '%' || term || '%')",
            "parse_urlquery" => $"jsonb_build_object('Query', SUBSTRING({args[0]} FROM '\\?(.+)'))",

            // Hash functions (additional)
            "hash_combine" => $"(HASHTEXTEXTENDED({args[0]}::text, 0) # HASHTEXTEXTENDED({args[1]}::text, 0))",
            "hash_many" => $"HASHTEXTEXTENDED(CONCAT({string.Join(", ", args)}), 0)",

            // IPv4 functions
            "parse_ipv4" => $"(SPLIT_PART({args[0]}, '.', 1)::BIGINT * 16777216 + SPLIT_PART({args[0]}, '.', 2)::BIGINT * 65536 + SPLIT_PART({args[0]}, '.', 3)::BIGINT * 256 + SPLIT_PART({args[0]}, '.', 4)::BIGINT)",
            "ipv4_is_in_range" => $"(INET({args[0]}) <<= INET({args[1]}))",
            "ipv4_is_private" => $"(INET({args[0]}) <<= INET('10.0.0.0/8') OR INET({args[0]}) <<= INET('172.16.0.0/12') OR INET({args[0]}) <<= INET('192.168.0.0/16'))",

            // Base64 GUID variants
            "base64_encode_fromguid" => $"ENCODE(CAST({args[0]} AS BYTEA), 'base64')",
            "base64_decode_toguid" => $"CAST(DECODE({args[0]}, 'base64') AS UUID)",

            // Parse functions
            "parse_version" => $"STRING_TO_ARRAY({args[0]}, '.')",

            // Array shift functions
            "array_shift_left" when args.Length >= 2 => args.Length >= 3
                ? $"({args[0]}[{args[1]}+1:] || (SELECT ARRAY_AGG({args[2]}) FROM GENERATE_SERIES(1, {args[1]})))"
                : $"({args[0]}[{args[1]}+1:] || (SELECT ARRAY_AGG(NULL) FROM GENERATE_SERIES(1, {args[1]})))",
            "array_shift_right" when args.Length >= 2 => args.Length >= 3
                ? $"((SELECT ARRAY_AGG({args[2]}) FROM GENERATE_SERIES(1, {args[1]})) || {args[0]}[:ARRAY_LENGTH({args[0]},1)-{args[1]}])"
                : $"((SELECT ARRAY_AGG(NULL) FROM GENERATE_SERIES(1, {args[1]})) || {args[0]}[:ARRAY_LENGTH({args[0]},1)-{args[1]}])",

            // IPv4 additional (PostgreSQL has native INET type)
            "ipv4_is_match" when args.Length == 2 => $"(INET({args[0]}) = INET({args[1]}))",
            "ipv4_is_match" when args.Length == 3 => $"(INET({args[0]}) <<= NETWORK(SET_MASKLEN(INET({args[1]}), {args[2]})))",
            "ipv4_netmask_suffix" => $"SPLIT_PART({args[0]}, '/', 2)::INT",
            "format_ipv4_mask" when args.Length == 2 => $"CONCAT(HOST(INET({args[0]}::text)), '/', {args[1]})",

            // Set functions
            "jaccard_index" => $"((SELECT COUNT(*) FROM UNNEST({args[0]}) x WHERE x = ANY({args[1]}))::DOUBLE PRECISION / (SELECT COUNT(DISTINCT x) FROM UNNEST({args[0]} || {args[1]}) x)::DOUBLE PRECISION)",

            // Type/conditional functions
            "gettype" or "typeof" => $"PG_TYPEOF({args[0]})::text",
            "isnan" => $"({args[0]} = 'NaN'::double precision)",
            "isinf" => $"({args[0]} = 'Infinity'::double precision OR {args[0]} = '-Infinity'::double precision)",
            "isfinite" => $"ISFINITE({args[0]}::timestamp)",

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
            "avgif" => $"AVG(CASE WHEN {args[1]} THEN {args[0]} END)",
            "binary_all_and" => $"BIT_AND({args[0]}::int)",
            "binary_all_or" => $"BIT_OR({args[0]}::int)",
            "binary_all_xor" => $"BIT_XOR({args[0]}::int)",
            "buildschema" => $"MIN(pg_typeof({args[0]})::text)",
            "count_distinct" => $"COUNT(DISTINCT {args[0]})",
            "count_distinctif" => $"COUNT(DISTINCT CASE WHEN {args[1]} THEN {args[0]} END)",
            "countif" => $"COUNT(CASE WHEN {args[0]} THEN 1 END)",
            "covariance" => $"COVAR_SAMP({args[0]}, {args[1]})",
            "covarianceif" => $"COVAR_SAMP(CASE WHEN {args[2]} THEN {args[0]} END, CASE WHEN {args[2]} THEN {args[1]} END)",
            "covariancep" => $"COVAR_POP({args[0]}, {args[1]})",
            "covariancepif" => $"COVAR_POP(CASE WHEN {args[2]} THEN {args[0]} END, CASE WHEN {args[2]} THEN {args[1]} END)",
            "dcount" => $"COUNT(DISTINCT {args[0]})",
            "dcountif" => $"COUNT(DISTINCT CASE WHEN {args[1]} THEN {args[0]} END)",
            "make_bag" => $"jsonb_agg({args[0]})",
            "make_bag_if" => $"jsonb_agg(CASE WHEN {args[1]} THEN {args[0]} END)",
            "make_list" => $"array_agg({args[0]})",
            "make_list_if" => $"array_agg(CASE WHEN {args[1]} THEN {args[0]} END)",
            "make_list_with_nulls" => $"array_agg({args[0]})",
            "make_set" => $"array_agg(DISTINCT {args[0]})",
            "make_set_if" => $"array_agg(DISTINCT CASE WHEN {args[1]} THEN {args[0]} END)",
            "min" => $"MIN({args[0]})",
            "minif" => $"MIN(CASE WHEN {args[1]} THEN {args[0]} END)",
            "max" => $"MAX({args[0]})",
            "maxif" => $"MAX(CASE WHEN {args[1]} THEN {args[0]} END)",
            "percentile" => $"PERCENTILE_CONT({args[1]} / 100.0) WITHIN GROUP (ORDER BY {args[0]})",
            "percentilew" => $"PERCENTILE_CONT({args[2]} / 100.0) WITHIN GROUP (ORDER BY {args[0]})",
            "stdev" => $"STDDEV_SAMP({args[0]})",
            "stdevif" => $"STDDEV_SAMP(CASE WHEN {args[1]} THEN {args[0]} END)",
            "stdevp" => $"STDDEV_POP({args[0]})",
            "sumif" => $"SUM(CASE WHEN {args[1]} THEN {args[0]} END)",
            "take_any" => $"(array_agg({args[0]}))[1]",
            "take_anyif" => $"(array_agg(CASE WHEN {args[1]} THEN {args[0]} END))[1]",
            "variance" => $"VAR_SAMP({args[0]})",
            "varianceif" => $"VAR_SAMP(CASE WHEN {args[1]} THEN {args[0]} END)",
            "variancep" => $"VAR_POP({args[0]})",
            "variancepif" => $"VAR_POP(CASE WHEN {args[1]} THEN {args[0]} END)",
            _ => null
        };
    }

    public string MapType(string kustoType)
    {
        return kustoType.ToLowerInvariant() switch
        {
            "bool" or "boolean" => "BOOLEAN",
            "int" => "INTEGER",
            "long" => "BIGINT",
            "real" => "DOUBLE PRECISION",
            "decimal" => "NUMERIC",
            "datetime" => "TIMESTAMP",
            "date" => "DATE",
            "string" => "TEXT",
            "dynamic" => "JSONB",
            "guid" => "UUID",
            _ => throw new NotSupportedException($"Unsupported type '{kustoType}'")
        };
    }

    public string JsonAccess(string baseSql, string jsonPath)
    {
        var parts = jsonPath.Split('.');
        if (parts.Length == 1)
        {
            return $"({baseSql}::jsonb->>'{parts[0]}')";
        }
        // For nested access: col::jsonb->'a'->>'b'
        var intermediate = string.Join("", parts.Take(parts.Length - 1).Select(p => $"->'{p}'"));
        return $"({baseSql}::jsonb{intermediate}->>'{parts.Last()}')";
    }

    public string SelectExclude(string[] columns)
    {
        // PostgreSQL does not support EXCLUDE syntax.
        // Return a placeholder that OperatorSqlTranslator can use.
        // The caller must handle column enumeration for true exclusion.
        return $"* /* EXCLUDE ({string.Join(", ", columns)}) */";
    }

    public string SelectRename(string[] mappings)
    {
        // PostgreSQL does not support RENAME syntax.
        // Return a placeholder that callers can interpret.
        return $"* /* RENAME ({string.Join(", ", mappings)}) */";
    }

    public string Qualify(string innerSql, string condition)
    {
        // PostgreSQL does not support QUALIFY. Use subquery with window function.
        var eqIdx = condition.LastIndexOf("= ");
        if (eqIdx > 0)
        {
            var windowExpr = condition.Substring(0, eqIdx).TrimEnd();
            var value = condition.Substring(eqIdx + 2).Trim();
            // Avoid double-wrapping: if innerSql is already a SELECT, use it directly
            var innerWrapped = innerSql.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase)
                ? $"({innerSql})"
                : $"(SELECT * FROM {innerSql})";
            return $"SELECT * FROM (SELECT *, {windowExpr} AS _rn FROM {innerWrapped} _q) _ranked WHERE _rn = {value}";
        }
        return $"SELECT * FROM ({innerSql}) WHERE {condition}";
    }

    public string GenerateSeries(string alias, string start, string end, string step)
    {
        return $"SELECT generate_series AS {alias} FROM generate_series({start}, {end}, {step})";
    }

    public string Unnest(string sourceAlias, string column, string unnestAlias)
    {
        return $"CROSS JOIN LATERAL UNNEST({sourceAlias}.{column}) AS {unnestAlias}(value)";
    }
}
