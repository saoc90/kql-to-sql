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
            "make_bag" => $"jsonb_object_agg(({args[0]})::text, {args[0]})",
            "make_bag_if" => $"jsonb_object_agg(CASE WHEN {args[1]} THEN ({args[0]})::text END, CASE WHEN {args[1]} THEN {args[0]} END)",
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
            return $"({baseSql}->>'{parts[0]}')";
        }
        // For nested access: col->'a'->>'b'
        var intermediate = string.Join("", parts.Take(parts.Length - 1).Select(p => $"->'{p}'"));
        return $"({baseSql}{intermediate}->>'{parts.Last()}')";
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

    public string Qualify(string condition)
    {
        // PostgreSQL does not support QUALIFY. Use a subquery wrapper pattern.
        // The caller wraps in: SELECT * FROM (...) WHERE condition
        return $"/* QUALIFY */ WHERE {condition}";
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
