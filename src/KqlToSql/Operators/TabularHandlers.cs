using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Kusto.Language;
using Kusto.Language.Syntax;
using KqlToSql.Expressions;

namespace KqlToSql.Operators;

internal class TabularHandlers : OperatorHandlerBase
{
    internal TabularHandlers(KqlToSqlConverter converter, ExpressionSqlBuilder expr)
        : base(converter, expr) { }

    // Matches a window-function call followed by OVER (...), e.g. LAG(x) OVER () or LEAD(x,1) OVER (PARTITION BY y ORDER BY z)
    private static readonly Regex _windowFnRe = new(
        @"((?:LAG|LEAD|ROW_NUMBER|RANK|DENSE_RANK|NTILE|FIRST_VALUE|LAST_VALUE|NTH_VALUE)\s*\((?:[^()]|\([^()]*\))*\)\s*OVER\s*\((?:[^()]|\([^()]*\))*\))",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// If <paramref name="condition"/> contains window functions (LAG/LEAD/…OVER(…)),
    /// hoists them to computed columns in a subquery and rewrites the condition to reference aliases.
    /// Returns null if no window functions detected.
    /// </summary>
    private static (string innerSql, string rewrittenCondition)? HoistWindowFunctions(string leftSql, string condition)
    {
        var matches = _windowFnRe.Matches(condition);
        if (matches.Count == 0) return null;

        var extras = new List<string>();
        var rewritten = condition;
        // Replace each unique window expression with an alias _w_0, _w_1, …
        var seen = new Dictionary<string, string>(StringComparer.Ordinal);
        int idx = 0;
        foreach (Match m in matches)
        {
            var expr = m.Value;
            if (!seen.TryGetValue(expr, out var alias))
            {
                alias = $"_w_{idx++}";
                seen[expr] = alias;
                extras.Add($"{expr} AS {alias}");
            }
            rewritten = rewritten.Replace(expr, alias);
        }

        var inner = $"SELECT *, {string.Join(", ", extras)} FROM ({leftSql})";
        return (inner, rewritten);
    }

    /// <summary>Resolves the input column names of a filter by re-analyzing the query with the bound
    /// semantic model (same technique as the search operator). Empty list if binding is unavailable.</summary>
    private static List<string> ResolveStarColumns(FilterOperator filter)
    {
        try
        {
            var analyzed = KustoCode.ParseAndAnalyze(filter.Root.ToString());
            var condText = filter.Condition.ToString();
            var match = analyzed.Syntax.GetDescendants<FilterOperator>()
                .FirstOrDefault(f => f.Condition.ToString() == condText);
            if (match?.Parent is PipeExpression pipe &&
                pipe.Expression.ResultType is Kusto.Language.Symbols.TableSymbol ts)
                return ts.Columns.Select(c => c.Name).ToList();
        }
        catch { /* unbound source — fall back to no expansion */ }
        return new List<string>();
    }

    /// <summary>Resolves the INPUT columns of a query operator via the Kusto semantic model — i.e. the
    /// columns flowing into it from upstream (a CTE/let, datatable, prior operators). Returns null when
    /// the query can't be bound (e.g. an unknown base table), so callers fall back to text heuristics.
    /// Used so an extend that redefines an existing column EXCLUDEs it even when that column comes from a
    /// referenced CTE and is therefore not textually visible in the generated SQL.</summary>
    private static HashSet<string>? ResolveOperatorInputColumns(QueryOperator op)
    {
        try
        {
            var analyzed = KustoCode.ParseAndAnalyze(op.Root.ToString());
            var opText = op.ToString();
            var match = analyzed.Syntax.GetDescendants<QueryOperator>()
                .FirstOrDefault(o => o.Kind == op.Kind && o.ToString() == opText && o.Parent is PipeExpression);
            if (match?.Parent is PipeExpression pipe &&
                pipe.Expression.ResultType is Kusto.Language.Symbols.TableSymbol ts)
                return new HashSet<string>(ts.Columns.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
        }
        catch { /* unbound source — caller falls back to text heuristics */ }
        return null;
    }

    internal string ApplyFilter(string leftSql, FilterOperator filter)
    {
        // `where * <op> rhs` expands over every input column — resolve them via the bound semantic model.
        bool hasStar = filter.Condition.GetDescendants<StarExpression>().Count > 0;
        if (hasStar) Expr.SetAllColumns(ResolveStarColumns(filter));
        var condition = Expr.ConvertExpression(filter.Condition);
        if (hasStar) Expr.ClearAllColumns();

        // If condition contains window functions, hoist them into an inner subquery
        if (HoistWindowFunctions(leftSql, condition) is var hoisted && hoisted.HasValue)
            return $"SELECT * FROM ({hoisted.Value.innerSql}) WHERE {hoisted.Value.rewrittenCondition}";

        // Only append WHERE directly if there's no ORDER BY, LIMIT, or GROUP BY trailing
        if (IsSimpleSelectStar(leftSql) &&
            !leftSql.Contains(" WHERE ", StringComparison.OrdinalIgnoreCase) &&
            !leftSql.Contains(" ORDER BY ", StringComparison.OrdinalIgnoreCase) &&
            !leftSql.Contains(" LIMIT ", StringComparison.OrdinalIgnoreCase) &&
            !leftSql.Contains(" GROUP BY ", StringComparison.OrdinalIgnoreCase))
            return $"{leftSql} WHERE {condition}";

        if (CanAppendWhere(leftSql))
            return $"{leftSql} AND {condition}";

        return $"SELECT * FROM ({leftSql}) WHERE {condition}";
    }

    internal string ApplyProject(string leftSql, ProjectOperator project)
    {
        int anonCounter = 0;
        var columns = project.Expressions.Select(se =>
        {
            if (se.Element is SimpleNamedExpression sne)
            {
                var name = sne.Name.SimpleName;
                var sql = Expr.ConvertExpression(sne.Expression);
                if (LooksLikeIntervalResult(sne.Expression, sql) || Expr.IsIntervalExpression(sql))
                    Expr.MarkIntervalColumn(name);
                if (Expressions.ExpressionSqlBuilder.LooksLikeJsonResult(sql))
                    Expr.MarkJsonColumn(name);
                return $"{sql} AS {Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(name)}";
            }
            var synthesized = SynthesizePathAlias(se.Element);
            var bareSql = Expr.ConvertExpression(se.Element);
            if (synthesized != null)
                return $"{bareSql} AS {synthesized}";
            // Kusto auto-names bare project expressions Column1, Column2, … when the AST
            // isn't a simple name or path. Match that so downstream references bind.
            if (se.Element is NameReference)
                return bareSql;
            anonCounter++;
            return $"{bareSql} AS Column{anonCounter}";
        }).ToArray();

        return ReplaceSelectStar(leftSql, string.Join(", ", columns));
    }

    private static string? SynthesizePathAlias(SyntaxElement element)
    {
        // Drill through single-arg conversion wrappers like tostring(...), toreal(...), toint(...) —
        // KQL auto-names 'tostring(DataMetadata.SectionName)' as 'DataMetadata_SectionName' too.
        SyntaxNode? current = element as SyntaxNode;
        bool drilledWrapper = false;
        while (current is FunctionCallExpression fce && fce.ArgumentList.Expressions.Count == 1)
        {
            if (fce.IsAny(Functions.ToString, Functions.ToReal, Functions.ToDouble, Functions.ToInt, Functions.ToLong, Functions.ToBool, Functions.ToDateTime, Functions.ToDynamic_)
                || fce.Name.SimpleName.Equals("tofloat", StringComparison.OrdinalIgnoreCase)) // TODO: no Kusto.Language symbol for 'tofloat'
            { current = fce.ArgumentList.Expressions[0].Element; drilledWrapper = true; }
            else break;
        }

        var segments = new List<string>();
        while (current is PathExpression pe)
        {
            segments.Insert(0, (pe.Selector as NameReference)?.Name.SimpleName ?? pe.Selector.ToString().Trim());
            current = pe.Expression;
        }
        if (current is not NameReference nr) return null;
        // Bare NameReference with no path: only synthesize alias if we drilled a conversion wrapper
        // (KQL auto-names toreal(X) → X). A raw 'project X' needs no alias.
        if (segments.Count == 0)
            return drilledWrapper ? Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(nr.Name.SimpleName) : null;
        segments.Insert(0, nr.Name.SimpleName);
        return Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(string.Join("_", segments));
    }

    internal string ApplyProjectAway(string leftSql, ProjectAwayOperator projectAway)
    {
        var columns = projectAway.Expressions
            .Select(se => Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(
                se.Element is NameReference nr ? nr.SimpleName : se.Element.ToString().Trim()))
            .ToArray();
        return ReplaceSelectStar(leftSql, Dialect.SelectExclude(columns));
    }

    internal string ApplyProjectRename(string leftSql, ProjectRenameOperator projectRename)
    {
        var mappings = projectRename.Expressions.Select(se =>
        {
            if (se.Element is SimpleNamedExpression sne)
                return $"{Expr.ConvertExpression(sne.Expression)} AS {Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(sne.Name.SimpleName)}";
            throw new NotSupportedException("Unsupported project-rename expression");
        }).ToArray();

        return ReplaceSelectStar(leftSql, Dialect.SelectRename(mappings));
    }

    internal string ApplyProjectKeep(string leftSql, ProjectKeepOperator projectKeep)
    {
        var columns = projectKeep.Expressions
            .Select(se => Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(
                se.Element is NameReference nr ? nr.SimpleName : se.Element.ToString().Trim()))
            .ToArray();
        return ReplaceSelectStar(leftSql, string.Join(", ", columns));
    }

    internal string ApplyProjectReorder(string leftSql, ProjectReorderOperator projectReorder)
    {
        var columns = projectReorder.Expressions
            .Select(se => Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(
                se.Element is NameReference nr ? nr.SimpleName : se.Element.ToString().Trim()))
            .ToArray();
        return ReplaceSelectStar(leftSql, $"{string.Join(", ", columns)}, {Dialect.SelectExclude(columns)}");
    }

    internal string ApplyExtend(string leftSql, ExtendOperator extend)
    {
        // Carry a preceding sort's order into serialization-order window functions (prev/next/…)
        // referenced by this extend, so DuckDB evaluates them in Kusto's serialized row order.
        Expr.SetWindowOrder(ExtractTrailingOrderBy(leftSql));
        try
        {
            return ApplyExtendCore(leftSql, extend);
        }
        finally { Expr.SetWindowOrder(null); }
    }

    private string ApplyExtendCore(string leftSql, ExtendOperator extend)
    {
        // AppendExpr = "<expr> AS <name>" (or bare for a no-op name ref) for the append path;
        // RawExpr / QuotedName feed the replace-in-place path; Name is the unquoted name for detection.
        var extras = new List<(string AppendExpr, string RawExpr, string Name, string QuotedName)>();
        // KQL auto-numbers anonymous (unnamed, non-path, non-identifier) extend results Column1, Column2, ...
        int anonCounter = 0;
        foreach (var se in extend.Expressions)
        {
            if (se.Element is SimpleNamedExpression sne)
            {
                var name = sne.Name.SimpleName;
                var quotedName = Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(name);
                var convertedSql = Expr.ConvertExpression(sne.Expression);
                // Mark interval columns so downstream sum/sumif/divide pick the epoch-ms path.
                // LooksLikeIntervalResult covers patterns the AST can recognise without
                // scanning converted SQL; Expr.IsIntervalExpression catches additive chains
                // of already-marked interval columns (DurationGood + DurationFine + ...).
                if (LooksLikeIntervalResult(sne.Expression, convertedSql) ||
                    Expr.IsIntervalExpression(convertedSql))
                    Expr.MarkIntervalColumn(name);
                // Tag JSON/dynamic results so a later bare reference (mv-expand / array funcs) coerces.
                if (Expressions.ExpressionSqlBuilder.LooksLikeJsonResult(convertedSql))
                    Expr.MarkJsonColumn(name);
                extras.Add(($"{convertedSql} AS {quotedName}", convertedSql, name, quotedName));
            }
            else if (se.Element is CompoundNamedExpression cne)
            {
                // (a, b) = expr — expand each name as indexed access
                var rhs = Expr.ConvertExpression(cne.Expression);
                int idx = 0;
                foreach (var nameNode in cne.Names.Names)
                {
                    var n = nameNode.Element is NameDeclaration nd
                        ? nd.SimpleName
                        : nameNode.Element.ToString().Trim();
                    extras.Add(($"{rhs}[{idx + 1}] AS {n}", $"{rhs}[{idx + 1}]", n, n));
                    idx++;
                }
            }
            else
            {
                // Bare expression in extend — KQL auto-names:
                //   DataMetadata.Section  → DataMetadata_Section (path join)
                //   NameReference         → unchanged (already a column)
                //   round(name = expr, 2) → KQL uses the inner SimpleNamedExpression's name
                //   Anything else (CASE/iif/arithmetic) → Column1, Column2, ... (positional)
                var synthesized = SynthesizePathAlias(se.Element);
                var colSql = Expr.ConvertExpression(se.Element);
                if (synthesized != null)
                {
                    extras.Add(($"{colSql} AS {synthesized}", colSql, synthesized, synthesized));
                }
                else if (se.Element is NameReference)
                {
                    extras.Add((colSql, colSql, colSql, colSql));
                }
                else if (se.Element is FunctionCallExpression outerFce &&
                         outerFce.ArgumentList.Expressions.Count > 0 &&
                         outerFce.ArgumentList.Expressions[0].Element is SimpleNamedExpression innerNamed)
                {
                    // KQL: extend round(Pct = x/y*100, 2) — the inner name=... labels the extend's output.
                    var innerName = innerNamed.Name.SimpleName;
                    extras.Add(($"{colSql} AS {innerName}", colSql, innerName, innerName));
                }
                else
                {
                    anonCounter++;
                    var autoName = $"Column{anonCounter}";
                    extras.Add(($"{colSql} AS {autoName}", colSql, autoName, autoName));
                }
            }
        }

        // KQL extend replaces columns with the same name.
        // Check if any extended column name already exists as an alias at the OUTER level of leftSql
        // (paren depth 0). Inner-scope AS aliases (from deeply nested subqueries) may not be visible
        // to the enclosing FROM, so using EXCLUDE on them would cause 'column not found' errors.
        // A redefined column must be EXCLUDEd so it isn't emitted twice (which shadows the new value
        // and breaks UNION BY NAME). Detect existing columns three ways: the cheap text scans (alias /
        // relation-alias column list), plus the semantic model for columns that flow in from a referenced
        // CTE/let and so aren't textually present in leftSql.
        // The semantic-model path is only used when leftSql is a bare CTE/table reference
        // (`SELECT * FROM <name>`): there the input columns equal the referenced relation's columns, so
        // an EXCLUDE on them is valid. For any other shape (e.g. an mv-apply on-subquery's reconstructed
        // `(SELECT u.value AS x)`) the model's column view can diverge from the generated SQL, which would
        // EXCLUDE a column not present in the FROM — so we stick to the text heuristics there.
        // Only when leftSql is a bare CTE/table reference (`SELECT * FROM <name>`) do the input columns
        // equal the referenced relation's columns, making an EXCLUDE on a model-reported column valid.
        // mv-apply on-subqueries reconstruct an explicit projection (`(SELECT u.value AS x)`) with NO
        // top-level `*`, so the model's column view can diverge from the generated SQL there — those use
        // text heuristics only. But any shape that carries a top-level `*` (`SELECT * FROM t`,
        // `SELECT *, expr AS y FROM (subquery)`) propagates ALL input columns opaquely, so a model-reported
        // column is genuinely present and an EXCLUDE/REPLACE on it is valid. This covers extend chains that
        // redefine a column arriving via `*` (e.g. `… | extend y=x+1 | extend x=y*10`), which the text
        // scans can't see through.
        bool useModel = Regex.IsMatch(leftSql, @"^SELECT \* FROM [A-Za-z_][A-Za-z0-9_]*\s*$")
                        || HasTopLevelStar(leftSql);
        var inputCols = useModel ? ResolveOperatorInputColumns(extend) : null;
        bool IsRedef((string AppendExpr, string RawExpr, string Name, string QuotedName) e) =>
            IsExistingOutputColumn(leftSql, e.Name, inputCols);
        var columnsToExclude = extras
            .Where(IsRedef)
            .Select(e => Expressions.ExpressionSqlBuilder.QuoteIdentifierIfReserved(e.Name))
            .Distinct()
            .ToArray();

        var joined = string.Join(", ", extras.Select(e => e.AppendExpr));

        // A scan-window function with a restart predicate (e.g. row_cumsum(v, g != prev(g))) emits a
        // __RESETGRP__(<pred>) marker; hoist each into a reset-group column in an inner SELECT and
        // partition the window by it (a window can't reference another window directly).
        if (HasResetGroupMarker(joined))
            return HoistResetGroups(leftSql, joined, columnsToExclude);

        // Redefined columns replace in place (Kusto keeps a redefined column's original position);
        // new columns append after the star. Prefer DuckDB's `* REPLACE (...)` so position is preserved.
        // Dialects without replace-in-place fall back to EXCLUDE-the-old + append-the-new below (which
        // moves the redefined column to the end — acceptable only as a last resort for those dialects).
        var redefined = extras.Where(IsRedef)
            .GroupBy(e => e.Name, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.Last())            // last write wins, and dedup so REPLACE has no repeated target
            .Select(e => (Col: e.QuotedName, Expr: e.RawExpr))
            .ToList();
        if (redefined.Count > 0 && Dialect.SelectStarReplace(redefined) is { } replaceList)
        {
            var fresh = extras.Where(e => !IsRedef(e)).Select(e => e.AppendExpr).ToList();
            var tail = fresh.Count > 0 ? ", " + string.Join(", ", fresh) : "";
            var from = ExtractFrom(leftSql);
            return $"SELECT {replaceList}{tail} FROM {from}";
        }

        if (columnsToExclude.Length > 0)
        {
            var exclude = Dialect.SelectExclude(columnsToExclude);
            var from = ExtractFrom(leftSql);
            return $"SELECT {exclude}, {joined} FROM {from}";
        }

        return AppendToSelectStar(leftSql, joined);
    }


    internal string ApplySort(string leftSql, SortOperator sort)
    {
        var orderings = new List<string>();
        foreach (var se in sort.Expressions)
        {
            if (se.Element is OrderedExpression oe)
                orderings.Add($"{ConvertOrderExpression(oe.Expression)} {OrderingSuffix(oe.Ordering)}");
            else
                // A bare sort key defaults to descending in KQL.
                orderings.Add($"{ConvertOrderExpression(se.Element)} DESC NULLS LAST");
        }

        // Build "<ASC|DESC> NULLS <FIRST|LAST>" from the ordering clause. The direction lives in the
        // AscOrDescKeyword token and the (optional) null placement in NullsClause.FirstOrLastKeyword —
        // reading Ordering.ToString() instead conflates them ("asc nulls first" != "asc"), which used to
        // flip `asc nulls first` to `DESC NULLS LAST`. KQL defaults: direction desc; nulls first on asc,
        // last on desc; an explicit nulls clause overrides the default.
        static string OrderingSuffix(Kusto.Language.Syntax.OrderingClause? ord)
        {
            var dirText = ord?.AscOrDescKeyword?.Text?.Trim();
            bool asc = string.Equals(dirText, "asc", StringComparison.OrdinalIgnoreCase);
            var dir = asc ? "ASC" : "DESC";
            var nullsText = ord?.NullsClause?.FirstOrLastKeyword?.Text?.Trim();
            string nulls = string.Equals(nullsText, "first", StringComparison.OrdinalIgnoreCase) ? "NULLS FIRST"
                : string.Equals(nullsText, "last", StringComparison.OrdinalIgnoreCase) ? "NULLS LAST"
                : asc ? "NULLS FIRST" : "NULLS LAST";
            return $"{dir} {nulls}";
        }

        // Wrap in a subquery if:
        //   (a) leftSql has a trailing ORDER BY (e.g. from | top) — prevents invalid double ORDER BY, or
        //   (b) leftSql contains a top-level GROUP BY — appending ORDER BY after GROUP BY can reference
        //       columns not in the aggregation output, causing DuckDB "must appear in GROUP BY" errors.
        if (HasTrailingOrderBy(leftSql) || HasTopLevelGroupBy(leftSql))
            return $"SELECT * FROM ({leftSql}) ORDER BY {string.Join(", ", orderings)}";
        return $"{leftSql} ORDER BY {string.Join(", ", orderings)}";
    }

    /// <summary>Returns true if 'AS name' appears at paren depth 0 in sql (outer SELECT level),
    /// meaning the column is visible from an enclosing FROM.</summary>
    // Detects a column introduced via a relation-alias column list at the OUTER level of leftSql,
    // e.g. a datatable's `... AS t(s)` / `... AS t(d, k)`. extend redefining such a column must EXCLUDE
    // it (same as a top-level `AS name`) so it isn't emitted twice (which UNION BY NAME rejects).
    // Only depth-0 aliases count — a nested `AS t(k, v)` may be shadowed by an intermediate summarize,
    // so its columns are NOT in the current output and must not be excluded.
    private static bool HasRelationAliasColumn(string sql, string name)
    {
        foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(
                     sql, @"\bAS\s+""?\w+""?\s*\(([^()]*)\)"))
        {
            // Compute paren depth at the match start; the relation alias must be at the top level.
            int depth = 0;
            bool inStr = false; char q = ' ';
            for (int i = 0; i < m.Index; i++)
            {
                var c = sql[i];
                if (inStr) { if (c == q) inStr = false; continue; }
                if (c is '\'' or '"') { inStr = true; q = c; }
                else if (c == '(') depth++;
                else if (c == ')') depth--;
            }
            if (depth != 0) continue;
            foreach (var col in m.Groups[1].Value.Split(','))
                if (col.Trim().Trim('"').Equals(name, StringComparison.OrdinalIgnoreCase))
                    return true;
        }
        return false;
    }

    /// <summary>True if <paramref name="name"/> is already an output column of <paramref name="leftSql"/>
    /// — i.e. visible to an enclosing FROM, so an extend redefining it must replace it in place rather than
    /// append a duplicate. Covers a depth-0 <c>expr AS name</c>; a relation-alias column list <c>AS t(name,…)</c>;
    /// a star-modifier target <c>* RENAME/REPLACE (… AS name)</c> (whose AS sits at paren depth 1, hidden from
    /// HasTopLevelAlias); a bare identifier in an explicit top-level SELECT list; and — only when leftSql is a
    /// bare CTE/table reference — a column the semantic model reports flowing in from the referenced relation.</summary>
    private static bool IsExistingOutputColumn(string leftSql, string name, HashSet<string>? inputCols) =>
        HasTopLevelAlias(leftSql, name)
        || HasRelationAliasColumn(leftSql, name)
        || HasStarModifierTarget(leftSql, name)
        || HasTopLevelBareColumn(leftSql, name)
        || (inputCols?.Contains(name) ?? false);

    /// <summary>Detects a column produced by a top-level <c>* RENAME (… AS name)</c> or
    /// <c>* REPLACE (… AS name)</c> modifier. The <c>AS name</c> inside the modifier's parens is at paren
    /// depth 1, so HasTopLevelAlias misses it, yet the column IS in leftSql's output. Only a modifier whose
    /// own open-paren is at the OUTER select level (depth 0) counts — a nested one is in a subquery.</summary>
    private static bool HasStarModifierTarget(string sql, string name)
    {
        foreach (Match m in Regex.Matches(sql, @"\b(?:RENAME|REPLACE)\s*\(", RegexOptions.IgnoreCase))
        {
            int open = sql.IndexOf('(', m.Index);
            if (open < 0) continue;
            // The modifier's open-paren must be at the OUTER select level (depth 0 just before it).
            int depth = 0; bool inStr = false; char q = ' ';
            for (int i = 0; i < open; i++)
            {
                var c = sql[i];
                if (inStr) { if (c == q) inStr = false; continue; }
                if (c is '\'' or '"') { inStr = true; q = c; }
                else if (c == '(') depth++;
                else if (c == ')') depth--;
            }
            if (depth != 0) continue;
            // Scan the modifier group (paren-balanced from `open`) for an `AS name` target. Nested parens in
            // a REPLACE expression are tolerated — a target name is always the simple identifier after AS.
            int d = 0; int end = open;
            for (int i = open; i < sql.Length; i++)
            {
                if (sql[i] == '(') d++;
                else if (sql[i] == ')') { d--; if (d == 0) { end = i; break; } }
            }
            var inner = sql.Substring(open + 1, end - open - 1);
            foreach (Match a in Regex.Matches(inner, @"\bAS\s+""?(\w+)""?", RegexOptions.IgnoreCase))
                if (a.Groups[1].Value.Equals(name, StringComparison.OrdinalIgnoreCase))
                    return true;
        }
        return false;
    }

    /// <summary>Detects a bare column identifier in leftSql's top-level explicit SELECT list — e.g.
    /// <c>SELECT a, b, c FROM …</c> — these are output columns carrying no <c>AS</c> alias and so are missed
    /// by HasTopLevelAlias. A <c>*</c>-based list can't be enumerated from text, so such items are skipped.</summary>
    /// <summary>True if leftSql's top-level SELECT list contains a <c>*</c> item (`SELECT *`,
    /// `SELECT *, expr AS y`, `SELECT * EXCLUDE/REPLACE/RENAME (…)`) — meaning all input columns flow
    /// through opaquely, so a semantic-model-reported column is genuinely present in the output.</summary>
    private static bool HasTopLevelStar(string sql)
    {
        var list = TopLevelSelectList(sql);
        if (list == null) return false;
        foreach (var item in SplitTopLevel(list))
        {
            var t = item.Trim();
            if (t == "*" || t.StartsWith("* ", StringComparison.Ordinal)) return true;
        }
        return false;
    }

    /// <summary>Returns leftSql's top-level SELECT-list substring (between `SELECT ` and the depth-0
    /// ` FROM `), or null when sql is not a leading SELECT or has no top-level FROM.</summary>
    private static string? TopLevelSelectList(string sql)
    {
        if (!sql.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase)) return null;
        int depth = 0; bool inStr = false; char q = ' ';
        for (int i = 7; i + 6 <= sql.Length; i++)
        {
            var c = sql[i];
            if (inStr) { if (c == q) inStr = false; continue; }
            if (c is '\'' or '"') { inStr = true; q = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && string.Compare(sql, i, " FROM ", 0, 6, StringComparison.OrdinalIgnoreCase) == 0)
                return sql.Substring(7, i - 7);
        }
        return null;
    }

    /// <summary>Splits a SELECT-list substring on depth-0 commas (ignoring parens and string literals).</summary>
    private static IEnumerable<string> SplitTopLevel(string list)
    {
        int depth = 0; bool inStr = false; char q = ' '; int start = 0;
        for (int i = 0; i <= list.Length; i++)
        {
            bool atEnd = i == list.Length;
            char c = atEnd ? ',' : list[i];
            if (!atEnd && inStr) { if (c == q) inStr = false; continue; }
            if (!atEnd && (c is '\'' or '"')) { inStr = true; q = c; continue; }
            if (!atEnd && c == '(') { depth++; continue; }
            if (!atEnd && c == ')') { depth--; continue; }
            if ((atEnd || c == ',') && depth == 0)
            {
                yield return list.Substring(start, i - start);
                start = i + 1;
            }
        }
    }

    private static bool HasTopLevelBareColumn(string sql, string name)
    {
        var list = TopLevelSelectList(sql);
        if (list == null) return false;
        // A bare (optionally-quoted) identifier select item == name is an output column carrying no alias.
        foreach (var item in SplitTopLevel(list))
        {
            var t = item.Trim();
            if (Regex.IsMatch(t, @"^""?\w+""?$") && t.Trim('"').Equals(name, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    private static bool HasTopLevelAlias(string sql, string name)
    {
        var patterns = new[] { $" AS {name} ", $" AS {name},", $" AS {name}\n", $" AS {name}\r",
                                $" AS \"{name}\" ", $" AS \"{name}\",", $" AS \"{name}\"\n", $" AS \"{name}\"\r" };
        foreach (var pat in patterns)
        {
            int depth = 0;
            bool inStr = false; char q = ' ';
            int start = 0;
            while (start <= sql.Length - pat.Length)
            {
                var c = sql[start];
                if (inStr) { if (c == q) inStr = false; start++; continue; }
                if (c == '\'' || c == '"') { inStr = true; q = c; start++; continue; }
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (depth == 0 && string.Compare(sql, start, pat, 0, pat.Length, StringComparison.OrdinalIgnoreCase) == 0)
                    return true;
                start++;
            }
        }
        // Also match trailing alias (AS name at end of leftSql)
        var endPat = $" AS {name}";
        if (sql.EndsWith(endPat, StringComparison.OrdinalIgnoreCase))
        {
            // Check depth at end
            int d = 0; bool s = false; char qq = ' ';
            foreach (var c in sql)
            {
                if (s) { if (c == qq) s = false; continue; }
                if (c == '\'' || c == '"') { s = true; qq = c; continue; }
                if (c == '(') d++;
                else if (c == ')') d--;
            }
            if (d == 0) return true;
        }
        return false;
    }

    private bool LooksLikeIntervalResult(Expression sourceExpr, string convertedSql)
    {
        // Interval-producing emission signatures we recognize at the outer level:
        //   '(N * INTERVAL '1 ms')' — from our timespan literal emission
        //   'X - Y' where X,Y are timestamps → INTERVAL (detect via AST)
        //   'datetime_diff(...) * (N * INTERVAL ...)' → INTERVAL
        if (System.Text.RegularExpressions.Regex.IsMatch(convertedSql, @"^\s*\(\s*\d+\s*\*\s*INTERVAL\s+'")) return true;
        if (convertedSql.TrimEnd().EndsWith("millisecond')", StringComparison.OrdinalIgnoreCase)) return true;
        if (sourceExpr is BinaryExpression bin)
        {
            if (bin.Kind == SyntaxKind.SubtractExpression)
            {
                bool leftLooksLikeTs = bin.Left is NameReference || (bin.Left is LiteralExpression ll && ll.Kind == SyntaxKind.DateTimeLiteralExpression);
                bool rightLooksLikeTs = bin.Right is NameReference || (bin.Right is LiteralExpression lr && lr.Kind == SyntaxKind.DateTimeLiteralExpression);
                if (leftLooksLikeTs && rightLooksLikeTs) return true;
            }
            // datetime_diff(...) * <timespan> — outermost multiply with a raw timespan literal on either side
            if (bin.Kind == SyntaxKind.MultiplyExpression)
            {
                bool leftIsTimespan = bin.Left is LiteralExpression lt && lt.Kind == SyntaxKind.TimespanLiteralExpression;
                bool rightIsTimespan = bin.Right is LiteralExpression rt && rt.Kind == SyntaxKind.TimespanLiteralExpression;
if (leftIsTimespan || rightIsTimespan) return true;
            }
            // Add / Subtract of interval-typed operands stays interval-typed:
            //   DurationGood + DurationFine + DurationCoarse where each side is marked interval.
            if (bin.Kind == SyntaxKind.AddExpression || bin.Kind == SyntaxKind.SubtractExpression)
            {
                if (IsIntervalLeaf(bin.Left) && IsIntervalLeaf(bin.Right))
                    return true;
            }
        }
        return false;
    }

    private bool IsIntervalLeaf(Expression expr) => expr switch
    {
        NameReference nr => Expr.IsIntervalColumn(nr.Name.SimpleName),
        BinaryExpression be when be.Kind == SyntaxKind.AddExpression || be.Kind == SyntaxKind.SubtractExpression
            => IsIntervalLeaf(be.Left) && IsIntervalLeaf(be.Right),
        LiteralExpression le when le.Kind == SyntaxKind.TimespanLiteralExpression => true,
        _ => false,
    };

    private static bool HasTrailingOrderBy(string sql)
    {
        // Look for a top-level ORDER BY that is NOT wrapped in a subquery.
        int depth = 0;
        bool inStr = false;
        char quote = ' ';
        int lastOrderBy = -1;
        for (int i = 0; i <= sql.Length - 10; i++)
        {
            var c = sql[i];
            if (inStr) { if (c == quote) inStr = false; continue; }
            if (c == '\'' || c == '"') { inStr = true; quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && string.Compare(sql, i, " ORDER BY ", 0, 10, StringComparison.OrdinalIgnoreCase) == 0)
                lastOrderBy = i;
        }
        return lastOrderBy >= 0;
    }

    private static bool HasTopLevelGroupBy(string sql)
    {
        // Return true if there is a top-level GROUP BY (depth 0) in the SQL.
        int depth = 0;
        bool inStr = false;
        char quote = ' ';
        for (int i = 0; i <= sql.Length - 9; i++)
        {
            var c = sql[i];
            if (inStr) { if (c == quote) inStr = false; continue; }
            if (c == '\'' || c == '"') { inStr = true; quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && string.Compare(sql, i, " GROUP BY ", 0, 10, StringComparison.OrdinalIgnoreCase) == 0)
                return true;
        }
        return false;
    }

    internal string ApplyTake(string leftSql, TakeOperator take)
    {
        var count = Expr.ConvertExpression(take.Expression);
        return $"{leftSql} LIMIT {count}";
    }

    internal string ApplyTop(string leftSql, TopOperator top)
    {
        var count = Expr.ConvertExpression(top.Expression);

        string order;
        if (top.ByExpression is OrderedExpression oe)
        {
            var expr = ConvertOrderExpression(oe.Expression);
            var dir = oe.Ordering?.ToString().Trim().ToUpperInvariant() ?? "DESC";
            order = $"{expr} {dir}";
        }
        else
        {
            var expr = ConvertOrderExpression(top.ByExpression);
            order = $"{expr} DESC";
        }

        // Inside `partition by <key> (top N by ...)`, top-N applies PER partition: rank within each key
        // group and keep the first N (QUALIFY window), preserving all columns as Kusto does.
        var partKey = Expr.PartitionKey;
        if (partKey != null)
        {
            var inner = $"SELECT * FROM {ExtractFromAsRelation(leftSql)}";
            return Dialect.Qualify(inner, $"ROW_NUMBER() OVER (PARTITION BY {partKey} ORDER BY {order}) <= {count}");
        }

        // Mirror ApplySort: if leftSql already carries a top-level trailing ORDER BY
        // (e.g. a preceding | sort by / | order by / | top) or a top-level GROUP BY,
        // wrap it as a subquery so we don't emit an invalid doubled ORDER BY or place
        // ORDER BY inside the GROUP BY scope. Kusto's `top` fully supersedes any prior
        // sort, so re-ordering the wrapped result is semantically correct.
        if (HasTrailingOrderBy(leftSql) || HasTopLevelGroupBy(leftSql))
            return $"SELECT * FROM ({leftSql}) ORDER BY {order} LIMIT {count}";
        return $"{leftSql} ORDER BY {order} LIMIT {count}";
    }

    private string ConvertOrderExpression(Expression expression)
        => expression is SimpleNamedExpression sne
            ? Expr.ConvertExpression(sne.Expression)
            : Expr.ConvertExpression(expression);

    internal string ApplyCount(string leftSql, CountOperator count)
        => ReplaceSelectStar(leftSql, "COUNT(*) AS Count");

    internal string ApplyDistinct(string leftSql, DistinctOperator distinct)
    {
        var cols = string.Join(", ", distinct.Expressions.Select(e =>
        {
            var sql = Expr.ConvertExpression(e.Element);
            var synthesized = SynthesizePathAlias(e.Element);
            return synthesized != null ? $"{sql} AS {synthesized}" : sql;
        }));
        return ReplaceSelectStar(leftSql, $"DISTINCT {cols}");
    }
}
