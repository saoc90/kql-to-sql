using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class IifStringColumnCastTests
{
    private readonly KqlToSqlConverter _converter = new();

    [Fact]
    public void Iif_StringLiteral_Vs_NumericColumn_CastsColumnToVarchar()
    {
        var sql = _converter.Convert("StormEvents | extend x = iif(State == 'X', 'foo', InjuriesDirect)");
        Assert.Contains("TRY_CAST(InjuriesDirect AS VARCHAR)", sql);
    }

    [Fact]
    public void Iif_StringLiteral_Vs_NumericColumn_ExecutesInDuckDb()
    {
        var sql = _converter.Convert("StormEvents | extend x = iif(State == 'X', 'foo', InjuriesDirect) | take 1");

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        // Must not throw a DuckDB CASE type-mismatch error
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
    }
}
