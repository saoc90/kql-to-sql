using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.Tests.Commands;

public class MaterializedViewCommandTests
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    [Fact]
    public void Translates_Create_Materialized_View()
    {
        var sql = _converter.Convert(".create materialized-view MyView on table StormEvents { StormEvents | summarize count() by State }");
        Assert.Equal("CREATE MATERIALIZED VIEW MyView AS SELECT State, COUNT(*) AS count FROM StormEvents GROUP BY ALL", sql);
    }

    [Fact]
    public void Translates_Create_Materialized_View_With_Properties()
    {
        var sql = _converter.Convert(".create materialized-view with (backfill=true) MyView on table StormEvents { StormEvents | summarize count() by State }");
        Assert.Equal("CREATE MATERIALIZED VIEW MyView AS SELECT State, COUNT(*) AS count FROM StormEvents GROUP BY ALL", sql);
    }

    [Fact]
    public void Translates_Create_Or_Alter_Materialized_View()
    {
        var sql = _converter.Convert(".create-or-alter materialized-view MyView on table StormEvents { StormEvents | summarize count() by State }");
        Assert.Equal("DROP MATERIALIZED VIEW IF EXISTS MyView; CREATE MATERIALIZED VIEW MyView AS SELECT State, COUNT(*) AS count FROM StormEvents GROUP BY ALL", sql);
    }

    [Fact]
    public void Translates_Drop_Materialized_View()
    {
        var sql = _converter.Convert(".drop materialized-view MyView");
        Assert.Equal("DROP MATERIALIZED VIEW MyView", sql);
    }

    [Fact]
    public void Translates_Drop_Materialized_View_IfExists()
    {
        var sql = _converter.Convert(".drop materialized-view MyView ifexists");
        Assert.Equal("DROP MATERIALIZED VIEW IF EXISTS MyView", sql);
    }

    [Fact]
    public void Translates_Show_Materialized_Views()
    {
        var sql = _converter.Convert(".show materialized-views");
        Assert.Equal("SELECT schemaname, matviewname, matviewowner, definition FROM pg_matviews", sql);
    }

    [Fact]
    public void Translates_Show_Materialized_View()
    {
        var sql = _converter.Convert(".show materialized-view MyView");
        Assert.Equal("SELECT * FROM pg_matviews WHERE matviewname = 'MyView'", sql);
    }
}
