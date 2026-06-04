using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class TopHittersOperatorTests
{
    [Fact]
    public void Converts_TopHitters_Simple()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | top-hitters 5 of State";
        var sql = converter.Convert(kql);
        // Plain form: weight is COUNT(*) per group, column named approximate_count_<of>.
        // Kusto returns the heaviest N groups ordered by weight ascending, so the heaviest are
        // selected with weight DESC then re-sorted ascending.
        Assert.Equal(
            "SELECT * FROM (SELECT State, COUNT(*) AS approximate_count_State FROM T GROUP BY State ORDER BY approximate_count_State DESC, State DESC LIMIT 5) ORDER BY approximate_count_State ASC, State ASC",
            sql);
    }

    [Fact]
    public void Converts_TopHitters_WithByClause()
    {
        var converter = new KqlToSqlConverter();
        // Kusto's `by` clause takes a plain numeric column (the per-group weight), summed internally —
        // aggregate calls like count() are rejected by Kusto.
        var kql = "T | top-hitters 5 of State by Cnt";
        var sql = converter.Convert(kql);
        Assert.Contains("SUM(Cnt) AS approximate_sum_Cnt", sql);
        Assert.Contains("GROUP BY State", sql);
        Assert.Contains("LIMIT 5", sql);
    }
}
