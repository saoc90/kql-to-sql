using KqlToSql.Fuzzer;

namespace KqlToSql.DifferentialTests;

/// <summary>
/// Unit tests for the comparator over hand-built result pairs — NO container needed. These must be
/// trustworthy before any differential verdict is believed, so they pin the tricky normalization
/// rules (type widening, float/datetime tolerance, multiset vs ordered, dynamic/JSON, errors).
/// </summary>
public class ComparatorTests
{
    private static ColumnInfo Col(string name, TypeClass cls) => new(name, cls, cls.ToString());

    private static EngineResult Result(ColumnInfo[] cols, params object?[][] rows) =>
        new(cols, rows, null, ErrorStage.None);

    private static GeneratedQuery Query(ComparisonMode mode = ComparisonMode.Multiset, bool nondet = false,
        bool unsupported = false) =>
        new() { Id = "t", Kql = "x", ExpectedMode = mode, Nondeterministic = nondet, ExpectedUnsupported = unsupported };

    private static Verdict Compare(EngineResult k, EngineResult d, GeneratedQuery? q = null, ComparisonOptions? o = null)
        => Comparator.Compare(q ?? Query(), k, d, o);

    [Fact]
    public void Identical_rows_match()
    {
        var cols = new[] { Col("a", TypeClass.Int), Col("b", TypeClass.String) };
        var k = Result(cols, new object?[] { 1L, "x" }, new object?[] { 2L, "y" });
        var d = Result(cols, new object?[] { 1L, "x" }, new object?[] { 2L, "y" });
        Assert.Equal(Outcome.Match, Compare(k, d).Outcome);
    }

    [Fact]
    public void Row_order_is_ignored_in_multiset_mode()
    {
        var cols = new[] { Col("a", TypeClass.Int) };
        var k = Result(cols, new object?[] { 1L }, new object?[] { 2L });
        var d = Result(cols, new object?[] { 2L }, new object?[] { 1L });
        Assert.Equal(Outcome.Match, Compare(k, d).Outcome);
    }

    [Fact]
    public void Row_order_matters_in_ordered_mode()
    {
        var cols = new[] { Col("a", TypeClass.Int) };
        var k = Result(cols, new object?[] { 1L }, new object?[] { 2L });
        var d = Result(cols, new object?[] { 2L }, new object?[] { 1L });
        Assert.Equal(Outcome.MismatchOrder, Compare(k, d, Query(ComparisonMode.Ordered)).Outcome);
    }

    [Fact]
    public void Int_long_widening_is_equal()
    {
        var k = Result(new[] { Col("a", TypeClass.Int) }, new object?[] { 5L });
        var d = Result(new[] { Col("a", TypeClass.Int) }, new object?[] { 5 });
        Assert.Equal(Outcome.Match, Compare(k, d).Outcome);
    }

    [Fact]
    public void Float_within_epsilon_is_equal()
    {
        var k = Result(new[] { Col("a", TypeClass.Real) }, new object?[] { 0.1 + 0.2 });
        var d = Result(new[] { Col("a", TypeClass.Real) }, new object?[] { 0.3 });
        Assert.Equal(Outcome.Match, Compare(k, d).Outcome);
    }

    [Fact]
    public void Different_numbers_mismatch()
    {
        var k = Result(new[] { Col("a", TypeClass.Real) }, new object?[] { 1.0 });
        var d = Result(new[] { Col("a", TypeClass.Real) }, new object?[] { 2.0 });
        Assert.Equal(Outcome.MismatchRows, Compare(k, d).Outcome);
    }

    [Fact]
    public void NaN_equals_NaN()
    {
        var k = Result(new[] { Col("a", TypeClass.Real) }, new object?[] { double.NaN });
        var d = Result(new[] { Col("a", TypeClass.Real) }, new object?[] { double.NaN });
        Assert.Equal(Outcome.Match, Compare(k, d).Outcome);
    }

    [Fact]
    public void Naive_datetime_is_treated_as_utc_not_local()
    {
        // Kusto parses as UTC; DuckDB.NET returns Kind=Unspecified for the same instant.
        var k = Result(new[] { Col("t", TypeClass.DateTime) },
            new object?[] { new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
        var d = Result(new[] { Col("t", TypeClass.DateTime) },
            new object?[] { new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Unspecified) });
        Assert.Equal(Outcome.Match, Compare(k, d).Outcome);
    }

    [Fact]
    public void Dynamic_json_key_order_is_ignored()
    {
        var k = Result(new[] { Col("d", TypeClass.Dynamic) }, new object?[] { new DynamicJson("{\"a\":1,\"b\":2}") });
        var d = Result(new[] { Col("d", TypeClass.Dynamic) }, new object?[] { new DynamicJson("{\"b\":2,\"a\":1}") });
        Assert.Equal(Outcome.Match, Compare(k, d).Outcome);
    }

    [Fact]
    public void Dynamic_json_value_difference_mismatches()
    {
        var k = Result(new[] { Col("d", TypeClass.Dynamic) }, new object?[] { new DynamicJson("{\"a\":1}") });
        var d = Result(new[] { Col("d", TypeClass.Dynamic) }, new object?[] { new DynamicJson("{\"a\":2}") });
        Assert.Equal(Outcome.MismatchRows, Compare(k, d).Outcome);
    }

    [Fact]
    public void Null_versus_empty_string_mismatch_by_default()
    {
        var k = Result(new[] { Col("s", TypeClass.String) }, new object?[] { null });
        var d = Result(new[] { Col("s", TypeClass.String) }, new object?[] { "" });
        Assert.Equal(Outcome.MismatchRows, Compare(k, d).Outcome);
    }

    [Fact]
    public void Null_equals_empty_when_opted_in()
    {
        var k = Result(new[] { Col("s", TypeClass.String) }, new object?[] { null });
        var d = Result(new[] { Col("s", TypeClass.String) }, new object?[] { "" });
        var v = Compare(k, d, Query(), new ComparisonOptions { NullEqualsEmpty = true });
        Assert.Equal(Outcome.Match, v.Outcome);
        Assert.Contains("NULL_VS_EMPTY", v.SubVerdicts);
    }

    [Fact]
    public void Row_count_difference_is_mismatch()
    {
        var cols = new[] { Col("a", TypeClass.Int) };
        var k = Result(cols, new object?[] { 1L });
        var d = Result(cols, new object?[] { 1L }, new object?[] { 1L });
        Assert.Equal(Outcome.MismatchRows, Compare(k, d).Outcome);
    }

    [Fact]
    public void Column_count_difference_is_mismatch_columns()
    {
        var k = Result(new[] { Col("a", TypeClass.Int) }, new object?[] { 1L });
        var d = Result(new[] { Col("a", TypeClass.Int), Col("b", TypeClass.Int) }, new object?[] { 1L, 2L });
        Assert.Equal(Outcome.MismatchColumns, Compare(k, d).Outcome);
    }

    [Fact]
    public void Translate_error_on_supported_op_is_a_bug()
    {
        var k = Result(new[] { Col("a", TypeClass.Int) }, new object?[] { 1L });
        var d = EngineResult.Failure("boom", ErrorStage.Translate);
        Assert.Equal(Outcome.TranslateError, Compare(k, d).Outcome);
    }

    [Fact]
    public void Translate_error_on_unsupported_op_is_skipped()
    {
        var k = Result(new[] { Col("a", TypeClass.Int) }, new object?[] { 1L });
        var d = EngineResult.Failure("not supported", ErrorStage.Translate);
        Assert.Equal(Outcome.SkippedUnsupported, Compare(k, d, Query(unsupported: true)).Outcome);
    }

    [Fact]
    public void Sql_exec_error_with_valid_kusto_is_highest_severity_bug()
    {
        var k = Result(new[] { Col("a", TypeClass.Int) }, new object?[] { 1L });
        var d = EngineResult.Failure("binder error", ErrorStage.Execute);
        var v = Compare(k, d);
        Assert.Equal(Outcome.SqlExecError, v.Outcome);
        Assert.Equal("highest", v.Severity);
        Assert.True(v.IsBug);
    }

    [Fact]
    public void Kusto_error_is_discarded_not_a_bug()
    {
        var k = EngineResult.Failure("bad request", ErrorStage.Execute);
        var d = Result(new[] { Col("a", TypeClass.Int) }, new object?[] { 1L });
        var v = Compare(k, d);
        Assert.Equal(Outcome.KustoError, v.Outcome);
        Assert.False(v.IsBug);
    }

    [Fact]
    public void Nondeterministic_query_is_skipped()
    {
        var k = Result(new[] { Col("a", TypeClass.Int) }, new object?[] { 1L });
        var d = Result(new[] { Col("a", TypeClass.Int) }, new object?[] { 2L });
        Assert.Equal(Outcome.SkippedNondeterministic, Compare(k, d, Query(nondet: true)).Outcome);
    }

    [Fact]
    public void Tie_block_order_is_relaxed_in_ordered_mode()
    {
        // Ordered by k; rows with equal k may appear in any order within their block.
        var cols = new[] { Col("k", TypeClass.Int), Col("v", TypeClass.Int) };
        var q = Query(ComparisonMode.Ordered) with { OrderKeys = new[] { "k" } };
        var k = Result(cols, new object?[] { 1L, 10L }, new object?[] { 1L, 20L }, new object?[] { 2L, 30L });
        var d = Result(cols, new object?[] { 1L, 20L }, new object?[] { 1L, 10L }, new object?[] { 2L, 30L });
        Assert.Equal(Outcome.Match, Compare(k, d, q).Outcome);
    }

    [Fact]
    public void Type_mismatch_is_recorded_but_values_still_compared()
    {
        var k = Result(new[] { Col("a", TypeClass.Int) }, new object?[] { 5L });
        var d = Result(new[] { Col("a", TypeClass.Real) }, new object?[] { 5.0 });
        var v = Compare(k, d);
        Assert.Equal(Outcome.Match, v.Outcome);
        Assert.Contains(v.SubVerdicts, s => s.StartsWith("TYPE_MISMATCH"));
    }
}
