using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ExternalDataTests
{
    [Fact]
    public void Converts_ExternalData_Csv()
    {
        var converter = new KqlToSqlConverter();
        var kql = "externaldata(Name:string, Age:int)['https://example.com/data.csv']";
        var sql = converter.Convert(kql);
        Assert.Contains("read_csv_auto", sql);
        Assert.Contains("https://example.com/data.csv", sql);
    }

    [Fact]
    public void Converts_ExternalData_Parquet()
    {
        var converter = new KqlToSqlConverter();
        var kql = "externaldata(Name:string, Age:int)['https://example.com/data.parquet']";
        var sql = converter.Convert(kql);
        Assert.Contains("read_parquet", sql);
    }
}
