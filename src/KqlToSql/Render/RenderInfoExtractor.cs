using System;
using System.Linq;
using Kusto.Language.Syntax;

namespace KqlToSql.Render;

/// <summary>
/// Extracts <see cref="RenderInfo"/> from a parsed <see cref="RenderOperator"/> using typed AST accessors
/// only (per repo convention — never regex/string-munge syntax tokens). Verified against Kusto.Language v12:
/// the chart kind is <c>op.ChartType.Text</c> ("timechart", …); the optional <c>with(...)</c> block is
/// <c>op.WithClause</c> whose <c>Properties</c> are <see cref="NamedParameter"/>s. Each parameter's
/// <c>Name.Name.SimpleName</c> is the property name and its <c>Expression</c> is one of:
/// a <see cref="LiteralExpression"/> (string/token/number/bool), a single <see cref="NameReference"/>
/// (xcolumn), or a <c>NameReferenceList</c> (series/ycolumns/anomalycolumns).
/// </summary>
public static class RenderInfoExtractor
{
    public static RenderInfo Extract(RenderOperator op)
    {
        var visualization = op.ChartType?.Text;
        if (string.IsNullOrEmpty(visualization)) visualization = "table";

        string? title = null, xcolumn = null, xtitle = null, ytitle = null;
        string? xaxis = null, yaxis = null, legend = null, ysplit = null, kind = null;
        string? ymin = null, ymax = null, xmin = null, xmax = null;
        bool accumulate = false;
        string[]? series = null, ycolumns = null, anomalycolumns = null;

        if (op.WithClause != null)
        {
            foreach (var np in op.WithClause.GetDescendants<NamedParameter>())
            {
                var name = np.Name?.Name?.SimpleName?.ToLowerInvariant();
                if (string.IsNullOrEmpty(name)) continue;
                switch (name)
                {
                    case "title": title = LiteralString(np); break;
                    case "xtitle": xtitle = LiteralString(np); break;
                    case "ytitle": ytitle = LiteralString(np); break;
                    case "xcolumn": xcolumn = Columns(np).FirstOrDefault(); break;
                    case "series": series = Columns(np); break;
                    case "ycolumns": ycolumns = Columns(np); break;
                    case "anomalycolumns": anomalycolumns = Columns(np); break;
                    case "xaxis": xaxis = Scalar(np); break;
                    case "yaxis": yaxis = Scalar(np); break;
                    case "legend": legend = Scalar(np); break;
                    case "ysplit": ysplit = Scalar(np); break;
                    case "kind": kind = Scalar(np); break;
                    case "ymin": ymin = Scalar(np); break;
                    case "ymax": ymax = Scalar(np); break;
                    case "xmin": xmin = Scalar(np); break;
                    case "xmax": xmax = Scalar(np); break;
                    case "accumulate": accumulate = Bool(np); break;
                }
            }
        }

        return new RenderInfo(
            Visualization: visualization,
            Title: title,
            XColumn: xcolumn,
            Series: series,
            YColumns: ycolumns,
            AnomalyColumns: anomalycolumns,
            XTitle: xtitle,
            YTitle: ytitle,
            XAxis: xaxis,
            YAxis: yaxis,
            Legend: legend,
            YSplit: ysplit,
            Accumulate: accumulate,
            IsQuerySorted: false,
            Kind: kind,
            Ymin: ymin,
            Ymax: ymax,
            Xmin: xmin,
            Xmax: xmax);
    }

    /// <summary>String-valued property (title/xtitle/ytitle): <c>'My Title'</c> → <c>My Title</c>.</summary>
    private static string? LiteralString(NamedParameter np)
        => np.Expression is LiteralExpression le ? le.LiteralValue?.ToString() : np.Expression?.ToString()?.Trim();

    /// <summary>Token/numeric property (kind=stacked, legend=hidden, xaxis=log, ymin=0) → the value as written.</summary>
    private static string? Scalar(NamedParameter np)
    {
        if (np.Expression is LiteralExpression le && le.LiteralValue != null)
            return le.LiteralValue.ToString();
        return np.Expression?.ToString()?.Trim();
    }

    private static bool Bool(NamedParameter np)
        => np.Expression is LiteralExpression le && le.LiteralValue is bool b
            ? b
            : string.Equals(np.Expression?.ToString()?.Trim(), "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>Column reference(s): <c>xcolumn=Ts</c> → [Ts]; <c>ycolumns=A, B</c> → [A, B].
    /// <c>NameReference</c> children are descendants of the parameter for both the single and list forms.</summary>
    private static string[] Columns(NamedParameter np)
        => np.GetDescendants<NameReference>()
             .Select(r => r.Name?.SimpleName)
             .Where(s => !string.IsNullOrEmpty(s))
             .Select(s => s!)
             .ToArray();
}
