using System.Linq;
using Kusto.Language;
using Kusto.Language.Syntax;
using KqlToSql;
using KqlToSql.Render;
using Xunit;

namespace KqlToSql.Tests.Render;

public class RenderInfoExtractorTests
{
    private static RenderInfo Extract(string kql)
    {
        var op = KustoCode.Parse(kql).Syntax.GetDescendants<RenderOperator>().Last();
        return RenderInfoExtractor.Extract(op);
    }

    [Fact]
    public void Timechart_Bare_Has_Only_Visualization()
    {
        var r = Extract("T | render timechart");
        Assert.Equal("timechart", r.Visualization);
        Assert.Null(r.Title);
        Assert.Null(r.XColumn);
        Assert.Null(r.YColumns);
        Assert.Null(r.Series);
        Assert.Null(r.Kind);
        Assert.False(r.Accumulate);
    }

    [Fact]
    public void Piechart_Bare()
    {
        var r = Extract("T | summarize count() by State | render piechart");
        Assert.Equal("piechart", r.Visualization);
        Assert.Null(r.Kind);
    }

    [Fact]
    public void Columnchart_With_Title_Ytitle_Kind_Legend()
    {
        var r = Extract("T | render columnchart with (title='My Title', ytitle='Y', kind=stacked, legend=hidden)");
        Assert.Equal("columnchart", r.Visualization);
        Assert.Equal("My Title", r.Title);
        Assert.Equal("Y", r.YTitle);
        Assert.Equal("stacked", r.Kind);
        Assert.Equal("hidden", r.Legend);
    }

    [Fact]
    public void Timechart_With_Xcolumn_Ycolumns_Series()
    {
        var r = Extract("T | render timechart with (xcolumn=Ts, ycolumns=A, B, series=State)");
        Assert.Equal("Ts", r.XColumn);
        Assert.Equal(new[] { "A", "B" }, r.YColumns);
        Assert.Equal(new[] { "State" }, r.Series);
    }

    [Fact]
    public void Anomalychart_With_AnomalyColumns()
    {
        var r = Extract("T | render anomalychart with (anomalycolumns=Score)");
        Assert.Equal("anomalychart", r.Visualization);
        Assert.Equal(new[] { "Score" }, r.AnomalyColumns);
    }

    [Fact]
    public void Barchart_With_Axes_Bounds_Accumulate()
    {
        var r = Extract("T | render barchart with (xaxis=log, yaxis=log, ymin=0, ymax=100, accumulate=true)");
        Assert.Equal("log", r.XAxis);
        Assert.Equal("log", r.YAxis);
        Assert.Equal("0", r.Ymin);
        Assert.Equal("100", r.Ymax);
        Assert.True(r.Accumulate);
    }

    [Fact]
    public void ConvertWithRender_Produces_Identical_Sql_Plus_Metadata()
    {
        var converter = new KqlToSqlConverter();
        var kql = "T | summarize count() by State | render piechart";
        var plain = converter.Convert(kql);
        var (sql, render) = converter.ConvertWithRender(kql);
        Assert.Equal(plain, sql); // render must not change the SQL
        Assert.NotNull(render);
        Assert.Equal("piechart", render!.Visualization);
    }

    [Fact]
    public void ConvertWithRender_Without_Render_Returns_Null_Metadata()
    {
        var converter = new KqlToSqlConverter();
        var (sql, render) = converter.ConvertWithRender("T | summarize count() by State");
        Assert.False(string.IsNullOrEmpty(sql));
        Assert.Null(render);
    }
}
