using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class BugFixTests
{
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
        var sql = converter.Convert("StormEvents | extend diff = INJURIES_DIRECT - INJURIES_INDIRECT");
        Assert.Equal("SELECT *, INJURIES_DIRECT - INJURIES_INDIRECT AS diff FROM StormEvents", sql);
    }

    [Fact]
    public void Extend_MultiplyExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend doubled = INJURIES_DIRECT * 2");
        Assert.Equal("SELECT *, INJURIES_DIRECT * 2 AS doubled FROM StormEvents", sql);
    }

    [Fact]
    public void Extend_DivideExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend halved = INJURIES_DIRECT / 2");
        Assert.Equal("SELECT *, INJURIES_DIRECT / 2 AS halved FROM StormEvents", sql);
    }

    [Fact]
    public void Extend_ModuloExpression()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend remainder = INJURIES_DIRECT % 2");
        Assert.Equal("SELECT *, INJURIES_DIRECT % 2 AS remainder FROM StormEvents", sql);
    }

    [Fact]
    public void Iff_Function_MapsToCase()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend result = iff(INJURIES_DIRECT > 0, 'Yes', 'No')");
        Assert.Equal("SELECT *, CASE WHEN INJURIES_DIRECT > 0 THEN 'Yes' ELSE 'No' END AS result FROM StormEvents", sql);
    }

    [Fact]
    public void Iif_Function_MapsToCase()
    {
        var converter = new KqlToSqlConverter();
        var sql = converter.Convert("StormEvents | extend result = iif(INJURIES_DIRECT > 0, 'Yes', 'No')");
        Assert.Equal("SELECT *, CASE WHEN INJURIES_DIRECT > 0 THEN 'Yes' ELSE 'No' END AS result FROM StormEvents", sql);
    }
}
