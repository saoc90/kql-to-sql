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
    private readonly ScanHandler _scan;
    private readonly KqlToSqlConverter _converter;

    internal ExpressionSqlBuilder ExpressionBuilder { get; }

    internal OperatorDispatcher(KqlToSqlConverter converter)
    {
        _converter = converter;
        ExpressionBuilder = new ExpressionSqlBuilder(converter.Dialect);
        ExpressionBuilder.SetNodeConverter(node => converter.ConvertNode(node));
        _tabular = new TabularHandlers(converter, ExpressionBuilder);
        _joins = new JoinHandlers(converter, ExpressionBuilder);
        _aggregation = new AggregationHandlers(converter, ExpressionBuilder);
        _parse = new ParseHandlers(converter, ExpressionBuilder);
        _advanced = new AdvancedHandlers(converter, ExpressionBuilder);
        _scan = new ScanHandler(converter, ExpressionBuilder);
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
            JoinOperator join => _joins.ApplyJoin(leftSql, join, leftExpression),
            LookupOperator lookup => _joins.ApplyLookup(leftSql, lookup, leftExpression),
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
            ScanOperator scanOp => _scan.ApplyScan(leftSql, scanOp),

            // Invoke: calls a stored function on the current result set
            InvokeOperator invoke => ApplyInvoke(leftSql, invoke),

            // Partition: run subquery per group, then UNION ALL the results
            PartitionOperator partition => ApplyPartition(leftSql, partition),

            // | as Name — register the pipeline output as a CTE so later references to Name work.
            AsOperator asOp => RegisterAsCte(leftSql, asOp),
            ConsumeOperator => leftSql,
            RenderOperator => leftSql,

            _ => throw new NotSupportedException($"Unsupported operator {op.Kind}")
        };
    }

    private string RegisterAsCte(string leftSql, AsOperator asOp)
    {
        var nameNode = asOp.GetDescendants<Kusto.Language.Syntax.NameDeclaration>().FirstOrDefault();
        var name = nameNode?.Name?.ToString().Trim();
        if (!string.IsNullOrEmpty(name))
        {
            // KQL allows rebinding the same `| as Name` multiple times. In SQL, two CTEs can't
            // share a name, and the new body typically references the old one (self-looking reference).
            // Rename the previous CTE to <Name>_N and rewrite the new body's references so each
            // version is a distinct, non-circular CTE.
            if (_converter.TryGetCte(name, out var existing))
            {
                int version = 1;
                string versioned;
                do { versioned = $"{name}_{version++}"; } while (_converter.TryGetCte(versioned, out _));
                _converter.RenameCte(name, versioned);
                leftSql = RewriteTableReferences(leftSql, name, versioned);
            }
            _converter.AddCte(name, leftSql, materialized: false);
        }
        return leftSql;
    }

    private static string RewriteTableReferences(string sql, string oldName, string newName)
    {
        // Replace whole-word occurrences of oldName in SQL (bounded by non-identifier chars).
        // Approximate — doesn't avoid string literals. Fine for identifier-like names.
        var pattern = $@"\b{System.Text.RegularExpressions.Regex.Escape(oldName)}\b";
        return System.Text.RegularExpressions.Regex.Replace(sql, pattern, newName);
    }

    private string ApplyPartition(string leftSql, PartitionOperator partition)
    {
        // partition by Col (subquery) — run subquery per partition then UNION ALL.
        // Approximation: apply the subquery to the whole leftSql with the by-column kept.
        // For simple aggregation/top-N subqueries this gives the right shape.
        var byCol = ExpressionBuilder.ConvertExpression(partition.ByExpression);
        if (partition.Operand is PartitionSubquery sub)
        {
            // The subquery may be a QueryOperator OR a PipeExpression (chain of operators).
            // Handle both: for pipe, walk its chain applying each operator to leftSql.
            if (sub.Subquery is QueryOperator qOp)
                return ApplyOperator(leftSql, qOp);
            if (sub.Subquery is PipeExpression pipe)
            {
                // Reconstruct the pipeline by walking the pipe chain
                var sql = leftSql;
                var current = pipe;
                var ops = new List<QueryOperator>();
                // Collect operators left-to-right
                while (current != null)
                {
                    ops.Insert(0, current.Operator);
                    current = current.Expression as PipeExpression;
                }
                foreach (var op in ops)
                    sql = ApplyOperator(sql, op);
                return sql;
            }
        }
        return leftSql;
    }

    private string ApplyInvoke(string leftSql, InvokeOperator invoke)
    {
        if (invoke.Function is not FunctionCallExpression fce)
            return leftSql;

        var funcName = fce.Name.SimpleName ?? fce.Name.ToString().Trim();

        // Tabular user-function: inline body with leftSql bound to the tabular parameter.
        if (_converter.TryGetUserFunction(funcName, out var fn) && fn.paramNames.Length > 0)
        {
            var tblParam = fn.paramNames[0];

            // Save any existing CTE under tblParam so we can restore it after inlining.
            var hadPrevCte = _converter.TryGetCte(tblParam, out var prevCte);

            // Bind the tabular parameter: register leftSql as a non-materialised CTE.
            _converter.AddCte(tblParam, leftSql, materialized: false);

            // Bind scalar parameters (paramNames[1..] ↔ invoke args[0..]).
            var scalarLets = _converter.ScalarLets;
            var savedScalars = new Dictionary<string, string>();
            var addedScalars = new List<string>();
            var invokeArgs = fce.ArgumentList.Expressions;
            for (int i = 1; i < fn.paramNames.Length; i++)
            {
                var pname = fn.paramNames[i];
                int argIdx = i - 1; // arg[0] corresponds to paramNames[1]
                string? boundSql = null;
                if (argIdx < invokeArgs.Count)
                    boundSql = ExpressionBuilder.ConvertExpression(invokeArgs[argIdx].Element);
                else if (fn.paramDefaults[i] != null)
                    boundSql = ExpressionBuilder.ConvertExpression(fn.paramDefaults[i]!);

                if (boundSql != null)
                {
                    if (scalarLets.TryGetValue(pname, out var prev))
                        savedScalars[pname] = prev;
                    else
                        addedScalars.Add(pname);
                    scalarLets[pname] = boundSql;
                }
            }

            try
            {
                return _converter.ConvertNode(fn.body);
            }
            finally
            {
                // Remove the tabular CTE binding (restore previous if it existed).
                if (hadPrevCte)
                    _converter.AddCte(tblParam, prevCte.sql, prevCte.materialized);
                else
                    _converter.RemoveCte(tblParam);

                // Restore scalar lets.
                foreach (var kv in savedScalars) scalarLets[kv.Key] = kv.Value;
                foreach (var k in addedScalars) scalarLets.Remove(k);
            }
        }

        // Parameterless user function or CTE: fall back to SELECT * FROM name.
        return $"SELECT * FROM {funcName}";
    }

    // Standalone converters (not piped from a left expression)
    internal string ConvertRange(RangeOperator range) => _advanced.ConvertRange(range);
    internal string ConvertUnion(UnionOperator union) => _joins.ConvertUnion(union);
    internal string ConvertPrint(PrintOperator print) => _advanced.ConvertPrint(print);
    internal string ConvertDataTable(DataTableExpression dt) => _advanced.ConvertDataTable(dt);
    internal string ConvertExternalData(ExternalDataExpression ed) => _advanced.ConvertExternalData(ed);
}
