using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class BugFixTests
{
    [Fact]
    public void Sort_AfterSummarize_WrapsInSubquery_AndExecutes()
    {
        var converter = new KqlToSqlConverter();
        var kql = "StormEvents | summarize n = count() by State | sort by n desc";
        var sql = converter.Convert(kql);

        // Summarize result must be wrapped so ORDER BY doesn't reference ungrouped columns.
        Assert.StartsWith("SELECT * FROM (", sql);
        Assert.EndsWith(") ORDER BY n DESC", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(string.IsNullOrWhiteSpace(reader.GetString(0)));
        Assert.True(reader.GetInt64(1) > 0);
    }


    [Fact]
    public void Sort_DefaultDirection_IsDescending()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | sort by State");
        Assert.Equal("SELECT * FROM StormEvents ORDER BY State DESC", sql);
    }

    [Fact]
    public void Sort_ExplicitAsc_IsAscending()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | sort by State asc");
        Assert.Equal("SELECT * FROM StormEvents ORDER BY State ASC", sql);
    }

    [Fact]
    public void Sort_ExplicitDesc_IsDescending()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | sort by State desc");
        Assert.Equal("SELECT * FROM StormEvents ORDER BY State DESC", sql);
    }

    [Fact]
    public void Extend_SubtractExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend diff = InjuriesDirect - InjuriesIndirect");
        Assert.Equal("SELECT *, InjuriesDirect - InjuriesIndirect AS diff FROM StormEvents", sql);
    }

    [Fact]
    public void Extend_MultiplyExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend doubled = InjuriesDirect * 2");
        Assert.Equal("SELECT *, InjuriesDirect * 2 AS doubled FROM StormEvents", sql);
    }

    [Fact]
    public void Extend_DivideExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend halved = InjuriesDirect / 2");
        Assert.Equal("SELECT *, InjuriesDirect / 2 AS halved FROM StormEvents", sql);
    }

    [Fact]
    public void Extend_ModuloExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend remainder = InjuriesDirect % 2");
        Assert.Equal("SELECT *, InjuriesDirect % 2 AS remainder FROM StormEvents", sql);
    }

    [Fact]
    public void Iff_Function_MapsToCase()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend result = iff(InjuriesDirect > 0, 'Yes', 'No')");
        Assert.Equal("SELECT *, CASE WHEN InjuriesDirect > 0 THEN 'Yes' ELSE 'No' END AS result FROM StormEvents", sql);
    }

    [Fact]
    public void Iif_Function_MapsToCase()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend result = iif(InjuriesDirect > 0, 'Yes', 'No')");
        Assert.Equal("SELECT *, CASE WHEN InjuriesDirect > 0 THEN 'Yes' ELSE 'No' END AS result FROM StormEvents", sql);
    }

    [Fact]
    public void BoolTypedLiteral_EmitsTrueOrFalse_NotFunctionCall()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | where bool(true)");
        Assert.Contains("TRUE", sql);
        Assert.DoesNotContain("bool(", sql);
    }
}
