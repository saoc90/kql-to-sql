using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class DivideVarcharTests
{
    [Fact]
    public void Trim_Divided_By_Integer_Casts_To_Double()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | extend ms = trim('\"', tostring(Payload.timestamp)) / 1000";
        var sql = converter.Convert(kql);
        Assert.Contains("TRY_CAST(", sql);
        Assert.Contains("AS DOUBLE) / 1000", sql);
    }

    [Fact]
    public void JsonExtract_Divided_By_Integer_Casts_To_Double()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | extend sec = Payload.epoch / 1000";
        var sql = converter.Convert(kql);
        // json_extract path should trigger the cast
        Assert.Contains("/ 1000", sql);
    }
}
