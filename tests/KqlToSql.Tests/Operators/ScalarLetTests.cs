using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class ScalarLetTests
{
    [Fact]
    public void UserFunction_Returning_Scalar_Inlines_As_Scalar_Let()
    {
        // Regression: a scalar user function bound to a let must be inlined as a scalar,
        // not wrapped in a CTE (which would produce invalid SELECT * FROM (<scalar-expr>)).
        var converter = new KqlToSqlConverter();
        var kql = @"let getfirstkey = (T: datetime) { datetime(0001) + 1s };
let Anchor = getfirstkey(datetime(2026-04-12));
StormEvents | where StartTime >= Anchor | take 1 | project StartTime";
        var sql = converter.Convert(kql);

        Assert.DoesNotContain("Anchor AS NOT MATERIALIZED", sql);
        Assert.DoesNotContain("Anchor AS MATERIALIZED", sql);
        Assert.Contains("TIMESTAMP '0001-01-01 00:00:00'", sql);
    }

    [Fact]
    public void Scalar_User_Function_Expression_Uses_Body_Expression()
    {
        var converter = new KqlToSqlConverter();
        var kql = @"let halve = (x: real) { x / 2.0 };
let v = halve(10.0);
StormEvents | where InjuriesIndirect > v | take 1 | project InjuriesIndirect";
        var sql = converter.Convert(kql);
        Assert.DoesNotContain(" v AS NOT MATERIALIZED", sql);
        Assert.DoesNotContain(" v AS MATERIALIZED", sql);
        // Numeric LiteralValue rendering drops trailing zero: 10.0 → "10", 2.0 → "2".
        Assert.Contains("10 / 2", sql);
    }
}
