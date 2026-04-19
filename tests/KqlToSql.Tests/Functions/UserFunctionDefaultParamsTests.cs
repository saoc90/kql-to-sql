using System;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Functions;

public class UserFunctionDefaultParamsTests
{
    [Fact]
    public void Missing_Arg_Binds_To_Int_Null_Default()
    {
        var converter = new KqlToSqlConverter();
        var kql = "let f = (x:int, y:int=int(null)) { iif(isnull(y), x, x+y) }; print f(5)";
        var sql = converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(5L, Convert.ToInt64(result));
    }

    [Fact]
    public void Provided_Arg_Overrides_Default()
    {
        var converter = new KqlToSqlConverter();
        var kql = "let g = (x:int, y:int=int(null)) { iif(isnull(y), x, x+y) }; print g(5, 3)";
        var sql = converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        Assert.Equal(8L, Convert.ToInt64(result));
    }
}
