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
        var kql = "StormEvents | summarize avg(InjuriesDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT AVG(InjuriesDirect) AS avg_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT AVG(InjuriesDirect) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Sum_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize sum(DeathsDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT SUM(DeathsDirect) AS sum_DeathsDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT SUM(DeathsDirect) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_AvgIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize avgif(InjuriesDirect, DeathsDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT AVG(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) AS avgif_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT AVG(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_BinaryAllAnd_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize binary_all_and(InjuriesDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT BIT_AND(InjuriesDirect) AS binary_all_and_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT BIT_AND(InjuriesDirect) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_BinaryAllOr_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize binary_all_or(InjuriesDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT BIT_OR(InjuriesDirect) AS binary_all_or_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT BIT_OR(InjuriesDirect) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_BinaryAllXor_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize binary_all_xor(InjuriesDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT BIT_XOR(InjuriesDirect) AS binary_all_xor_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT BIT_XOR(InjuriesDirect) FROM StormEvents";
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
        var kql = "StormEvents | summarize count_distinctif(State, InjuriesDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(DISTINCT State) FILTER (WHERE InjuriesDirect > 0) AS count_distinctif_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COUNT(DISTINCT State) FILTER (WHERE InjuriesDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_CountIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize countif(InjuriesDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COUNT(*) FILTER (WHERE InjuriesDirect > 0) AS countif FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COUNT(*) FILTER (WHERE InjuriesDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Covariance_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize covariance(InjuriesDirect, DeathsDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COVAR_SAMP(InjuriesDirect, DeathsDirect) AS covariance_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COVAR_SAMP(InjuriesDirect, DeathsDirect) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_CovarianceIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize covarianceif(InjuriesDirect, DeathsDirect, InjuriesDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COVAR_SAMP(InjuriesDirect, DeathsDirect) FILTER (WHERE InjuriesDirect > 0) AS covarianceif_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COVAR_SAMP(InjuriesDirect, DeathsDirect) FILTER (WHERE InjuriesDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_CovarianceP_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize covariancep(InjuriesDirect, DeathsDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COVAR_POP(InjuriesDirect, DeathsDirect) AS covariancep_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COVAR_POP(InjuriesDirect, DeathsDirect) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_CovariancePIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize covariancepif(InjuriesDirect, DeathsDirect, InjuriesDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT COVAR_POP(InjuriesDirect, DeathsDirect) FILTER (WHERE InjuriesDirect > 0) AS covariancepif_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT COVAR_POP(InjuriesDirect, DeathsDirect) FILTER (WHERE InjuriesDirect > 0) FROM StormEvents";
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
        var kql = "StormEvents | summarize dcountif(State, InjuriesDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT APPROX_COUNT_DISTINCT(State) FILTER (WHERE InjuriesDirect > 0) AS dcountif_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT APPROX_COUNT_DISTINCT(State) FILTER (WHERE InjuriesDirect > 0) FROM StormEvents";
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
        var kql = "StormEvents | summarize hll_if(State, InjuriesDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT hll(State) FILTER (WHERE InjuriesDirect > 0) AS hll_if_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSTALL hll";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "LOAD hll";
        cmd.ExecuteNonQuery();
        cmd.CommandText = sql;
        var result = (byte[])cmd.ExecuteScalar();
        cmd.CommandText = "SELECT hll(State) FILTER (WHERE InjuriesDirect > 0) FROM StormEvents";
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
        var kql = "StormEvents | summarize make_bag_if(State, InjuriesDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT histogram(State) FILTER (WHERE InjuriesDirect > 0) AS make_bag_if_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT histogram(State) FILTER (WHERE InjuriesDirect > 0) FROM StormEvents";
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
        var kql = "StormEvents | summarize make_list_if(State, InjuriesDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT LIST(State) FILTER (WHERE InjuriesDirect > 0) AS make_list_if_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT LIST(State) FILTER (WHERE InjuriesDirect > 0) FROM StormEvents";
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
        var kql = "StormEvents | summarize make_set_if(State, InjuriesDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT LIST(DISTINCT State) FILTER (WHERE InjuriesDirect > 0) AS make_set_if_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT LIST(DISTINCT State) FILTER (WHERE InjuriesDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected?.ToString(), result?.ToString());
    }

    [Fact]
    public void Summarize_MaxIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize maxif(InjuriesDirect, DeathsDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT MAX(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) AS maxif_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT MAX(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_MinIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize minif(InjuriesDirect, DeathsDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT MIN(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) AS minif_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT MIN(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Percentile_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize percentile(InjuriesDirect, 50)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT quantile_cont(InjuriesDirect, 50 / 100.0) AS percentile_50_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT quantile_cont(InjuriesDirect, 50 / 100.0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Percentiles_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize percentiles(InjuriesDirect, 5, 95)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT quantile_cont(InjuriesDirect, 5 / 100.0) AS percentiles_5_InjuriesDirect, quantile_cont(InjuriesDirect, 95 / 100.0) AS percentiles_95_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        reader.Read();
        var result1 = reader.GetValue(0);
        var result2 = reader.GetValue(1);
        cmd.CommandText = "SELECT quantile_cont(InjuriesDirect, 5 / 100.0) AS p5, quantile_cont(InjuriesDirect, 95 / 100.0) AS p95 FROM StormEvents";
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
        var kql = "StormEvents | summarize percentilew(InjuriesDirect, DeathsDirect, 50)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT quantile_cont(InjuriesDirect, 50 / 100.0) AS percentilew_50_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT quantile_cont(InjuriesDirect, 50 / 100.0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Percentilesw_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize percentilesw(InjuriesDirect, DeathsDirect, 5, 95)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT quantile_cont(InjuriesDirect, 5 / 100.0) AS percentilesw_5_InjuriesDirect, quantile_cont(InjuriesDirect, 95 / 100.0) AS percentilesw_95_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        reader.Read();
        var result1 = reader.GetValue(0);
        var result2 = reader.GetValue(1);
        cmd.CommandText = "SELECT quantile_cont(InjuriesDirect, 5 / 100.0) AS p5, quantile_cont(InjuriesDirect, 95 / 100.0) AS p95 FROM StormEvents";
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
        var kql = "StormEvents | summarize stdev(InjuriesDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STDDEV_SAMP(InjuriesDirect) AS stdev_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT STDDEV_SAMP(InjuriesDirect) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_StdevIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize stdevif(InjuriesDirect, DeathsDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STDDEV_SAMP(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) AS stdevif_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT STDDEV_SAMP(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Stdevp_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize stdevp(InjuriesDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT STDDEV_POP(InjuriesDirect) AS stdevp_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT STDDEV_POP(InjuriesDirect) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_SumIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize sumif(InjuriesDirect, DeathsDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT SUM(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) AS sumif_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT SUM(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) FROM StormEvents";
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
        var kql = "StormEvents | summarize take_anyif(State, InjuriesDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT ANY_VALUE(State) FILTER (WHERE InjuriesDirect > 0) AS take_anyif_State FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT ANY_VALUE(State) FILTER (WHERE InjuriesDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Variance_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize variance(InjuriesDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT VAR_SAMP(InjuriesDirect) AS variance_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT VAR_SAMP(InjuriesDirect) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_VarianceIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize varianceif(InjuriesDirect, DeathsDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT VAR_SAMP(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) AS varianceif_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT VAR_SAMP(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_Variancep_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize variancep(InjuriesDirect)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT VAR_POP(InjuriesDirect) AS variancep_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT VAR_POP(InjuriesDirect) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Summarize_VariancepIf_Function()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize variancepif(InjuriesDirect, DeathsDirect > 0)";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT VAR_POP(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) AS variancepif_InjuriesDirect FROM StormEvents", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        cmd.CommandText = "SELECT VAR_POP(InjuriesDirect) FILTER (WHERE DeathsDirect > 0) FROM StormEvents";
        var expected = cmd.ExecuteScalar();
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("tdigest(InjuriesDirect)")]
    [InlineData("tdigest_merge(State)")]
    [InlineData("merge_tdigest(State)")]
    public void Unsupported_Aggregation_Functions(string aggregate)
    {
        var converter = new KqlToSqlConverter();
        var kql = $"StormEvents | summarize {aggregate}";
        Assert.Throws<NotSupportedException>(() => converter.Convert(kql));
    }
}
