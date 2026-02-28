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
            "binary_all_and" => $"BIT_AND({args[0]})",
            "binary_all_or" => $"BIT_OR({args[0]})",
            "binary_all_xor" => $"BIT_XOR({args[0]})",
            "buildschema" => $"MIN(typeof({args[0]}))",
            "count_distinct" => $"COUNT(DISTINCT {args[0]})",
            "count_distinctif" => $"COUNT(DISTINCT CASE WHEN {args[1]} THEN {args[0]} END)",
            "countif" => $"COUNT(CASE WHEN {args[0]} THEN 1 END)",
            "covariance" => $"COVAR_SAMP({args[0]}, {args[1]})",
            "covarianceif" => $"COVAR_SAMP(CASE WHEN {args[2]} THEN {args[0]} END, CASE WHEN {args[2]} THEN {args[1]} END)",
            "covariancep" => $"COVAR_POP({args[0]}, {args[1]})",
            "covariancepif" => $"COVAR_POP(CASE WHEN {args[2]} THEN {args[0]} END, CASE WHEN {args[2]} THEN {args[1]} END)",
            "dcount" => $"APPROX_COUNT_DISTINCT({args[0]})",
            "dcountif" => $"APPROX_COUNT_DISTINCT(CASE WHEN {args[1]} THEN {args[0]} END)",
            "hll" => $"hll({args[0]})",
            "hll_if" => $"hll(CASE WHEN {args[1]} THEN {args[0]} END)",
            "hll_merge" => $"hll_merge({args[0]})",
            "make_bag" => $"histogram({args[0]})",
            "make_bag_if" => $"histogram(CASE WHEN {args[1]} THEN {args[0]} END)",
            "make_list" => $"LIST({args[0]})",
            "make_list_if" => $"LIST(CASE WHEN {args[1]} THEN {args[0]} END)",
            "make_list_with_nulls" => $"LIST({args[0]})",
            "make_set" => $"LIST(DISTINCT {args[0]})",
            "make_set_if" => $"LIST(DISTINCT CASE WHEN {args[1]} THEN {args[0]} END)",
            "min" => $"MIN({args[0]})",
            "minif" => $"MIN(CASE WHEN {args[1]} THEN {args[0]} END)",
            "max" => $"MAX({args[0]})",
            "maxif" => $"MAX(CASE WHEN {args[1]} THEN {args[0]} END)",
            "percentile" => $"quantile_cont({args[0]}, {args[1]} / 100.0)",
            "percentilew" => $"quantile_cont({args[0]}, {args[2]} / 100.0)",
            "stdev" => $"STDDEV_SAMP({args[0]})",
            "stdevif" => $"STDDEV_SAMP(CASE WHEN {args[1]} THEN {args[0]} END)",
            "stdevp" => $"STDDEV_POP({args[0]})",
            "sumif" => $"SUM(CASE WHEN {args[1]} THEN {args[0]} END)",
            "take_any" => $"ANY_VALUE({args[0]})",
            "take_anyif" => $"ANY_VALUE(CASE WHEN {args[1]} THEN {args[0]} END)",
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
        return $"SELECT * FROM ({innerSql}) QUALIFY {condition}";
    }

    public string GenerateSeries(string alias, string start, string end, string step)
    {
        return $"SELECT generate_series AS {alias} FROM generate_series({start}, {end}, {step})";
    }

    public string Unnest(string sourceAlias, string column, string unnestAlias)
    {
        return $"CROSS JOIN UNNEST({sourceAlias}.{column}) AS {unnestAlias}(value)";
    }
}
