using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class LookupOperatorTests
{
    [Fact]
    public void Converts_Lookup_Default_LeftOuter()
    {
        var converter = new KqlToSqlConverter();
        var kql = "Facts | lookup Dims on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* EXCLUDE (Key) FROM Facts AS L LEFT OUTER JOIN Dims AS R ON L.Key = R.Key", sql);
    }

    [Fact]
    public void Converts_Lookup_Inner()
    {
        var converter = new KqlToSqlConverter();
        var kql = "Facts | lookup kind=inner Dims on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* EXCLUDE (Key) FROM Facts AS L INNER JOIN Dims AS R ON L.Key = R.Key", sql);
    }

    [Fact]
    public void Converts_Lookup_MultipleKeys()
    {
        var converter = new KqlToSqlConverter();
        var kql = "Facts | lookup Dims on Key1, $left.Col1 == $right.Col2";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* EXCLUDE (Key1, Col1) FROM Facts AS L LEFT OUTER JOIN Dims AS R ON L.Key1 = R.Key1 AND L.Col1 = R.Col2", sql);
    }
}
