using System;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

/// <summary>
/// Routes KQL operators to the appropriate handler class.
/// Replaces the monolithic OperatorSqlTranslator with delegation to focused handler objects.
/// </summary>
internal sealed class OperatorDispatcher
{
    private readonly TabularHandlers _tabular;
    private readonly JoinHandlers _joins;
    private readonly AggregationHandlers _aggregation;
    private readonly ParseHandlers _parse;
    private readonly AdvancedHandlers _advanced;

    internal ExpressionSqlBuilder ExpressionBuilder { get; }

    internal OperatorDispatcher(KqlToSqlConverter converter)
    {
        ExpressionBuilder = new ExpressionSqlBuilder(converter.Dialect);
        ExpressionBuilder.SetNodeConverter(node => converter.ConvertNode(node));
        _tabular = new TabularHandlers(converter, ExpressionBuilder);
        _joins = new JoinHandlers(converter, ExpressionBuilder);
        _aggregation = new AggregationHandlers(converter, ExpressionBuilder);
        _parse = new ParseHandlers(converter, ExpressionBuilder);
        _advanced = new AdvancedHandlers(converter, ExpressionBuilder);
    }

    internal string ApplyOperator(string leftSql, QueryOperator op, Expression? leftExpression = null)
    {
        return op switch
        {
            // Tabular operators
            FilterOperator filter => _tabular.ApplyFilter(leftSql, filter),
            ProjectOperator project => _tabular.ApplyProject(leftSql, project),
            ProjectAwayOperator projectAway => _tabular.ApplyProjectAway(leftSql, projectAway),
            ProjectRenameOperator projectRename => _tabular.ApplyProjectRename(leftSql, projectRename),
            ProjectKeepOperator projectKeep => _tabular.ApplyProjectKeep(leftSql, projectKeep),
            ProjectReorderOperator projectReorder => _tabular.ApplyProjectReorder(leftSql, projectReorder),
            ExtendOperator extend => _tabular.ApplyExtend(leftSql, extend),
            SortOperator sort => _tabular.ApplySort(leftSql, sort),
            TakeOperator take => _tabular.ApplyTake(leftSql, take),
            TopOperator top => _tabular.ApplyTop(leftSql, top),
            CountOperator count => _tabular.ApplyCount(leftSql, count),
            DistinctOperator distinct => _tabular.ApplyDistinct(leftSql, distinct),

            // Join operators
            JoinOperator join => _joins.ApplyJoin(leftSql, join),
            LookupOperator lookup => _joins.ApplyLookup(leftSql, lookup),
            UnionOperator union => _joins.ApplyUnion(leftSql, union, leftExpression),

            // Aggregation
            SummarizeOperator summarize => _aggregation.ApplySummarize(leftSql, summarize),

            // Parse operators
            ParseOperator parse => _parse.ApplyParse(leftSql, parse),
            ParseWhereOperator parseWhere => _parse.ApplyParseWhere(leftSql, parseWhere),
            ParseKvOperator parseKv => _parse.ApplyParseKv(leftSql, parseKv),
            SearchOperator search => _parse.ApplySearch(leftSql, search),

            // Advanced operators
            EvaluateOperator evaluate => _advanced.ApplyEvaluate(leftSql, evaluate),
            MakeSeriesOperator makeSeries => _advanced.ApplyMakeSeries(leftSql, makeSeries),
            MvExpandOperator mvExpand => _advanced.ApplyMvExpand(leftSql, mvExpand),
            MvApplyOperator mvApply => _advanced.ApplyMvApply(leftSql, mvApply),
            TopHittersOperator topHitters => _advanced.ApplyTopHitters(leftSql, topHitters),
            TopNestedOperator topNested => _advanced.ApplyTopNested(leftSql, topNested),
            SampleOperator sample => _advanced.ApplySample(leftSql, sample),
            SampleDistinctOperator sampleDistinct => _advanced.ApplySampleDistinct(leftSql, sampleDistinct),
            SerializeOperator serialize => _advanced.ApplySerialize(leftSql, serialize),
            GetSchemaOperator => _advanced.ApplyGetSchema(leftSql),

            // Invoke: calls a stored function on the current result set
            InvokeOperator invoke => ApplyInvoke(leftSql, invoke),

            // Pass-through operators
            AsOperator => leftSql,
            ConsumeOperator => leftSql,
            RenderOperator => leftSql,

            _ => throw new NotSupportedException($"Unsupported operator {op.Kind}")
        };
    }

    private string ApplyInvoke(string leftSql, InvokeOperator invoke)
    {
        // invoke calls a stored function on the current result set.
        // If the function is a known CTE, the CTE body already contains the query logic.
        // The invoke just pipes leftSql as the input — return the CTE reference since it
        // already encapsulates the transformation.
        if (invoke.Function is FunctionCallExpression fce)
        {
            var funcName = fce.Name.ToString().Trim();
            // The function body (stored as CTE) already has the full pipeline.
            // Invoke is essentially: use leftSql as input, apply funcName's pipeline.
            // Since CTEs are self-contained, just SELECT from the CTE.
            return $"SELECT * FROM {funcName}";
        }
        return leftSql;
    }

    // Standalone converters (not piped from a left expression)
    internal string ConvertRange(RangeOperator range) => _advanced.ConvertRange(range);
    internal string ConvertUnion(UnionOperator union) => _joins.ConvertUnion(union);
    internal string ConvertPrint(PrintOperator print) => _advanced.ConvertPrint(print);
    internal string ConvertDataTable(DataTableExpression dt) => _advanced.ConvertDataTable(dt);
    internal string ConvertExternalData(ExternalDataExpression ed) => _advanced.ConvertExternalData(ed);
}
