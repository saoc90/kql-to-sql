using System;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Functions;

public class AggregationFunctionTests
{
    [Fact]
    public void Summarize_Avg_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize avg(INJURIES_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT AVG(INJURIES_DIRECT) AS avg_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT AVG(INJURIES_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Sum_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize sum(DEATHS_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT SUM(DEATHS_DIRECT) AS sum_DEATHS_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT SUM(DEATHS_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_AvgIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize avgif(INJURIES_DIRECT, DEATHS_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT AVG(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) AS avgif_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT AVG(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_BinaryAllAnd_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize binary_all_and(INJURIES_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT BIT_AND(INJURIES_DIRECT) AS binary_all_and_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT BIT_AND(INJURIES_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_BinaryAllOr_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize binary_all_or(INJURIES_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT BIT_OR(INJURIES_DIRECT) AS binary_all_or_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT BIT_OR(INJURIES_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_BinaryAllXor_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize binary_all_xor(INJURIES_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT BIT_XOR(INJURIES_DIRECT) AS binary_all_xor_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT BIT_XOR(INJURIES_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Buildschema_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize buildschema(State)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT MIN(typeof(State)) AS buildschema_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT MIN(typeof(State)) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_CountDistinct_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize count_distinct(State)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(DISTINCT State) AS count_distinct_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COUNT(DISTINCT State) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_CountDistinctIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize count_distinctif(State, INJURIES_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(DISTINCT CASE WHEN INJURIES_DIRECT > 0 THEN State END) AS count_distinctif_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COUNT(DISTINCT CASE WHEN INJURIES_DIRECT > 0 THEN State END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_CountIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize countif(INJURIES_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(CASE WHEN INJURIES_DIRECT > 0 THEN 1 END) AS countif FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COUNT(CASE WHEN INJURIES_DIRECT > 0 THEN 1 END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Covariance_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize covariance(INJURIES_DIRECT, DEATHS_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COVAR_SAMP(INJURIES_DIRECT, DEATHS_DIRECT) AS covariance_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COVAR_SAMP(INJURIES_DIRECT, DEATHS_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_CovarianceIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize covarianceif(INJURIES_DIRECT, DEATHS_DIRECT, INJURIES_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COVAR_SAMP(CASE WHEN INJURIES_DIRECT > 0 THEN INJURIES_DIRECT END, CASE WHEN INJURIES_DIRECT > 0 THEN DEATHS_DIRECT END) AS covarianceif_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COVAR_SAMP(CASE WHEN INJURIES_DIRECT > 0 THEN INJURIES_DIRECT END, CASE WHEN INJURIES_DIRECT > 0 THEN DEATHS_DIRECT END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_CovarianceP_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize covariancep(INJURIES_DIRECT, DEATHS_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COVAR_POP(INJURIES_DIRECT, DEATHS_DIRECT) AS covariancep_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COVAR_POP(INJURIES_DIRECT, DEATHS_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_CovariancePIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize covariancepif(INJURIES_DIRECT, DEATHS_DIRECT, INJURIES_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COVAR_POP(CASE WHEN INJURIES_DIRECT > 0 THEN INJURIES_DIRECT END, CASE WHEN INJURIES_DIRECT > 0 THEN DEATHS_DIRECT END) AS covariancepif_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COVAR_POP(CASE WHEN INJURIES_DIRECT > 0 THEN INJURIES_DIRECT END, CASE WHEN INJURIES_DIRECT > 0 THEN DEATHS_DIRECT END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Dcount_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize dcount(State)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT APPROX_COUNT_DISTINCT(State) AS dcount_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT APPROX_COUNT_DISTINCT(State) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_DcountIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize dcountif(State, INJURIES_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT APPROX_COUNT_DISTINCT(CASE WHEN INJURIES_DIRECT > 0 THEN State END) AS dcountif_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT APPROX_COUNT_DISTINCT(CASE WHEN INJURIES_DIRECT > 0 THEN State END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact(Skip = "Requires hll extension")]
    public void Summarize_Hll_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize hll(State)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT hll(State) AS hll_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSTALL hll";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "LOAD hll";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        var result = (byte[])cmd.ExecuteScalar();
        cmd.CommandText = "SELECT hll(State) FROM StormEvents";
        var expected = (byte[])cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact(Skip = "Requires hll extension")]
    public void Summarize_HllIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize hll_if(State, INJURIES_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT hll(CASE WHEN INJURIES_DIRECT > 0 THEN State END) AS hll_if_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSTALL hll";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "LOAD hll";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        var result = (byte[])cmd.ExecuteScalar();
        cmd.CommandText = "SELECT hll(CASE WHEN INJURIES_DIRECT > 0 THEN State END) FROM StormEvents";
        var expected = (byte[])cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact(Skip = "Requires hll extension")]
    public void Summarize_HllMerge_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize hll(State) | summarize hll_merge(hll_State)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT hll_merge(hll_State) AS hll_merge_hll_State FROM (SELECT hll(State) AS hll_State FROM StormEvents)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSTALL hll";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "LOAD hll";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        var result = (byte[])cmd.ExecuteScalar();
        cmd.CommandText = "SELECT hll_merge(hll_State) FROM (SELECT hll(State) AS hll_State FROM StormEvents)";
        var expected = (byte[])cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_MakeBag_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize make_bag(State)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT histogram(State) AS make_bag_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT histogram(State) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected?.ToString(), result?.ToString());
    }

    [Fact]
    public void Summarize_MakeBagIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize make_bag_if(State, INJURIES_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT histogram(CASE WHEN INJURIES_DIRECT > 0 THEN State END) AS make_bag_if_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT histogram(CASE WHEN INJURIES_DIRECT > 0 THEN State END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected?.ToString(), result?.ToString());
    }

    [Fact]
    public void Summarize_MakeList_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize make_list(State)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT LIST(State) AS make_list_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT LIST(State) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected?.ToString(), result?.ToString());
    }

    [Fact]
    public void Summarize_MakeListIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize make_list_if(State, INJURIES_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT LIST(CASE WHEN INJURIES_DIRECT > 0 THEN State END) AS make_list_if_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT LIST(CASE WHEN INJURIES_DIRECT > 0 THEN State END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected?.ToString(), result?.ToString());
    }

    [Fact]
    public void Summarize_MakeListWithNulls_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize make_list_with_nulls(State)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT LIST(State) AS make_list_with_nulls_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT LIST(State) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected?.ToString(), result?.ToString());
    }

    [Fact]
    public void Summarize_MakeSet_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize make_set(State)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT LIST(DISTINCT State) AS make_set_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT LIST(DISTINCT State) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected?.ToString(), result?.ToString());
    }

    [Fact]
    public void Summarize_MakeSetIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize make_set_if(State, INJURIES_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT LIST(DISTINCT CASE WHEN INJURIES_DIRECT > 0 THEN State END) AS make_set_if_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT LIST(DISTINCT CASE WHEN INJURIES_DIRECT > 0 THEN State END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected?.ToString(), result?.ToString());
    }

    [Fact]
    public void Summarize_MaxIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize maxif(INJURIES_DIRECT, DEATHS_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT MAX(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) AS maxif_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT MAX(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_MinIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize minif(INJURIES_DIRECT, DEATHS_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT MIN(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) AS minif_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT MIN(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Percentile_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize percentile(INJURIES_DIRECT, 50)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT quantile_cont(INJURIES_DIRECT, 50 / 100.0) AS percentile_50_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT quantile_cont(INJURIES_DIRECT, 50 / 100.0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Percentiles_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize percentiles(INJURIES_DIRECT, 5, 95)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT quantile_cont(INJURIES_DIRECT, 5 / 100.0) AS percentiles_5_INJURIES_DIRECT, quantile_cont(INJURIES_DIRECT, 95 / 100.0) AS percentiles_95_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        reader.Read();
        var result1 = reader.GetValue(0);
        var result2 = reader.GetValue(1);
        cmd.CommandText = "SELECT quantile_cont(INJURIES_DIRECT, 5 / 100.0) AS p5, quantile_cont(INJURIES_DIRECT, 95 / 100.0) AS p95 FROM StormEvents";
        using var reader2 = cmd.ExecuteReader();
        reader2.Read();
        var expected1 = reader2.GetValue(0);
        var expected2 = reader2.GetValue(1);
        Assert.Equal(expected1, result1);
        Assert.Equal(expected2, result2);
    }

    [Fact]
    public void Summarize_Percentilew_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize percentilew(INJURIES_DIRECT, DEATHS_DIRECT, 50)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT quantile_cont(INJURIES_DIRECT, 50 / 100.0) AS percentilew_50_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT quantile_cont(INJURIES_DIRECT, 50 / 100.0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Percentilesw_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize percentilesw(INJURIES_DIRECT, DEATHS_DIRECT, 5, 95)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT quantile_cont(INJURIES_DIRECT, 5 / 100.0) AS percentilesw_5_INJURIES_DIRECT, quantile_cont(INJURIES_DIRECT, 95 / 100.0) AS percentilesw_95_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        reader.Read();
        var result1 = reader.GetValue(0);
        var result2 = reader.GetValue(1);
        cmd.CommandText = "SELECT quantile_cont(INJURIES_DIRECT, 5 / 100.0) AS p5, quantile_cont(INJURIES_DIRECT, 95 / 100.0) AS p95 FROM StormEvents";
        using var reader2 = cmd.ExecuteReader();
        reader2.Read();
        var expected1 = reader2.GetValue(0);
        var expected2 = reader2.GetValue(1);
        Assert.Equal(expected1, result1);
        Assert.Equal(expected2, result2);
    }

    [Fact]
    public void Summarize_Stdev_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize stdev(INJURIES_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STDDEV_SAMP(INJURIES_DIRECT) AS stdev_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT STDDEV_SAMP(INJURIES_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_StdevIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize stdevif(INJURIES_DIRECT, DEATHS_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STDDEV_SAMP(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) AS stdevif_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT STDDEV_SAMP(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Stdevp_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize stdevp(INJURIES_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STDDEV_POP(INJURIES_DIRECT) AS stdevp_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT STDDEV_POP(INJURIES_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_SumIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize sumif(INJURIES_DIRECT, DEATHS_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT SUM(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) AS sumif_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT SUM(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_TakeAny_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize take_any(State)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT ANY_VALUE(State) AS take_any_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT ANY_VALUE(State) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_TakeAnyIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize take_anyif(State, INJURIES_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT ANY_VALUE(CASE WHEN INJURIES_DIRECT > 0 THEN State END) AS take_anyif_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT ANY_VALUE(CASE WHEN INJURIES_DIRECT > 0 THEN State END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Variance_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize variance(INJURIES_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT VAR_SAMP(INJURIES_DIRECT) AS variance_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT VAR_SAMP(INJURIES_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_VarianceIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize varianceif(INJURIES_DIRECT, DEATHS_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT VAR_SAMP(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) AS varianceif_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT VAR_SAMP(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Variancep_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize variancep(INJURIES_DIRECT)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT VAR_POP(INJURIES_DIRECT) AS variancep_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT VAR_POP(INJURIES_DIRECT) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_VariancepIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize variancepif(INJURIES_DIRECT, DEATHS_DIRECT > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT VAR_POP(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) AS variancepif_INJURIES_DIRECT FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT VAR_POP(CASE WHEN DEATHS_DIRECT > 0 THEN INJURIES_DIRECT END) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("tdigest(INJURIES_DIRECT)")]
    [InlineData("tdigest_merge(State)")]
    [InlineData("merge_tdigest(State)")]
    public void Unsupported_Aggregation_Functions(string aggregate)
    {
        var converter = new KqlToSqlConverter();
        var kql = $"StormEvents | summarize {aggregate}";
        Assert.Throws<NotSupportedException>(() => converter.Convert(kql));
    }
}
