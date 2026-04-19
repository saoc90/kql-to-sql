using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kusto.Language;
using Kusto.Language.Syntax;
using KqlToSql.Operators;
using KqlToSql.Commands;
using KqlToSql.Dialects;

namespace KqlToSql;

public class KqlToSqlConverter
{
    private readonly OperatorDispatcher _operators;
    private readonly CommandSqlTranslator _commands;
    private readonly Dictionary<string, (string sql, bool materialized)> _ctes = new();
    private readonly Dictionary<string, string> _scalarLets = new();
    private readonly Dictionary<string, (string[] paramNames, FunctionBody body)> _userFunctions = new();

    /// <summary>The SQL dialect used for engine-specific translations.</summary>
    public ISqlDialect Dialect { get; }

    public KqlToSqlConverter() : this(new DuckDbDialect()) { }

    public KqlToSqlConverter(ISqlDialect dialect)
    {
        Dialect = dialect;
        _operators = new OperatorDispatcher(this);
        _commands = new CommandSqlTranslator(this);
    }

    public string Convert(string kql)
    {
        kql = StripComments(kql);

        var code = KustoCode.Parse(kql);
        var root = code.Syntax;

        _ctes.Clear();
        _scalarLets.Clear();
        _userFunctions.Clear();
        
        if (root is CommandBlock)
        {
            return _commands.Translate(kql);
        }

        if (root is QueryBlock queryBlock)
        {
            return ConvertQueryBlock(queryBlock);
        }

        var pipe = root.GetFirstDescendant<PipeExpression>();
        if (pipe != null)
        {
            return ConvertNode(pipe);
        }

        var range = root.GetFirstDescendant<RangeOperator>();
        if (range != null)
        {
            return ConvertNode(range);
        }

        var name = root.GetFirstDescendant<NameReference>();
        if (name != null)
        {
            return $"SELECT * FROM {name.Name}";
        }

        throw new NotSupportedException("Unsupported KQL query");
    }

    private string ConvertQueryBlock(QueryBlock queryBlock)
    {
        var statements = queryBlock.GetDescendants<Statement>().ToList();

        // Wire the (initially empty) dictionaries to the expression builder FIRST,
        // so substitutions work while processing each subsequent let statement.
        _operators.ExpressionBuilder.SetScalarLets(_scalarLets);
        _operators.ExpressionBuilder.SetUserFunctions(_userFunctions);

        // Now process let statements in order — each can reference earlier scalars.
        foreach (var statement in statements)
        {
            if (statement is LetStatement letStatement)
            {
                ProcessLetStatement(letStatement);
            }
        }
        
        // Find the main query (the last statement that's not a let statement)
        var mainStatement = statements.LastOrDefault(s => s is not LetStatement);
        string mainSql;

        if (mainStatement != null)
        {
            mainSql = mainStatement is ExpressionStatement exprStatement
                ? ConvertNode(exprStatement.Expression)
                : ConvertNode(mainStatement);
        }
        else
        {
            // All statements are let — use the last CTE as the main query
            if (_ctes.Any())
            {
                var lastCte = _ctes.Last();
                _ctes.Remove(lastCte.Key);
                mainSql = $"SELECT * FROM ({lastCte.Value.sql})";
            }
            else
            {
                throw new NotSupportedException("No main query found in query block");
            }
        }
        
        // If we have CTEs, wrap the main query
        if (_ctes.Any())
        {
            var cteList = string.Join(", ", _ctes.Select(kvp => 
                $"{kvp.Key} AS {(kvp.Value.materialized ? "MATERIALIZED" : "NOT MATERIALIZED")} ({kvp.Value.sql})"));
            return $"WITH {cteList} {mainSql}";
        }
        
        return mainSql;
    }

    private void ProcessLetStatement(LetStatement letStatement)
    {
        var name = letStatement.Name.ToString().Trim();
        var expression = letStatement.Expression;

        // Try to handle scalar literals (datetime, timespan, string, number)
        if (TryConvertScalarLet(name, expression))
            return;

        bool materialized = false;
        string sql;

        if (expression is MaterializeExpression matExpr)
        {
            materialized = true;
            sql = ConvertNode(matExpr.Expression);
        }
        else if (expression is FunctionDeclaration funcDecl)
        {
            materialized = false;

            var parameters = funcDecl.Parameters?.Parameters
                .Select(p => p.Element?.NameAndType?.Name?.ToString().Trim())
                .Where(p => p != null)
                .Cast<string>()
                .ToArray() ?? Array.Empty<string>();

            var viewKeyword = funcDecl.ViewKeyword?.ToString().Trim().ToLowerInvariant();

            if (parameters.Length > 0)
            {
                // Store the whole FunctionBody — it may have nested lets that need scoping at call time
                _userFunctions[name] = (parameters, funcDecl.Body);
                return;
            }

            sql = ConvertNode(funcDecl.Body);
        }
        else if (expression is FunctionCallExpression fce)
        {
            var functionName = fce.Name.ToString().Trim();
            var functionNameLower = functionName.ToLowerInvariant();
            if (functionNameLower == "view")
            {
                materialized = false;
                if (fce.ArgumentList.Expressions.Count != 1)
                {
                    throw new NotSupportedException("view() expects exactly one argument");
                }
                sql = ConvertNode(fce.ArgumentList.Expressions[0].Element);
            }
            else if (_userFunctions.ContainsKey(functionName))
            {
                // User-defined parameterized function call returning tabular result.
                // Inline the body with parameter substitution.
                materialized = false;
                sql = _operators.ExpressionBuilder.ConvertExpression(fce);
                // If the result isn't a SELECT statement, wrap it
                if (!sql.TrimStart().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) &&
                    !sql.TrimStart().StartsWith("WITH", StringComparison.OrdinalIgnoreCase))
                {
                    sql = $"SELECT * FROM ({sql})";
                }
            }
            else
            {
                materialized = false;
                sql = ConvertNode(expression);
            }
        }
        else
        {
            materialized = false;
            sql = ConvertNode(expression);
        }

        _ctes[name] = (sql, materialized);
    }

    private bool TryConvertScalarLet(string name, Expression expression)
    {
        var exprBuilder = _operators.ExpressionBuilder;

        // datetime literal: let X = datetime(...)
        if (expression is LiteralExpression lit)
        {
            if (lit.Kind == SyntaxKind.DateTimeLiteralExpression)
            {
                _scalarLets[name] = Expressions.ExpressionSqlBuilder.ConvertDateTimeLiteral(lit);
                return true;
            }
            if (lit.Kind == SyntaxKind.TimespanLiteralExpression)
            {
                var text = lit.ToString().Trim();
                if (Expressions.ExpressionSqlBuilder.TryParseTimespan(text, out var ms))
                {
                    _scalarLets[name] = $"({ms} * INTERVAL '1 millisecond')";
                    return true;
                }
            }
            if (lit.Kind == SyntaxKind.StringLiteralExpression ||
                lit.Kind == SyntaxKind.LongLiteralExpression ||
                lit.Kind == SyntaxKind.IntLiteralExpression ||
                lit.Kind == SyntaxKind.RealLiteralExpression ||
                lit.Kind == SyntaxKind.BooleanLiteralExpression)
            {
                _scalarLets[name] = exprBuilder.ConvertExpression(lit);
                return true;
            }
        }

        // Computed scalar: let X = ago(1h), let X = now(), let X = EndTime - StartTime
        // These are expressions that reference other scalars or call scalar functions
        if (expression is FunctionCallExpression fce2)
        {
            var fname = fce2.Name.ToString().Trim();
            var fnameLower = fname.ToLowerInvariant();

            // User-defined function call: let x = myFunc(arg)
            // Skip if it's a user function — those should be handled as CTEs (tabular result)
            // by ProcessLetStatement's main flow, not stored as scalar substitutions.
            if (_userFunctions.ContainsKey(fname))
            {
                return false;
            }

            // Try converting any function call as a scalar expression.
            // If the expression builder can handle it (known scalar/cast function), use it.
            try
            {
                _scalarLets[name] = exprBuilder.ConvertExpression(fce2);
                return true;
            }
            catch
            {
                // Not a scalar function — fall through to CTE handling
            }
        }

        // toscalar(): let X = toscalar(query) → (SELECT ... LIMIT 1)
        if (expression is ToScalarExpression tse)
        {
            _scalarLets[name] = exprBuilder.ConvertExpression(tse);
            return true;
        }

        // Dynamic expressions: let x = dynamic(["a","b"])
        if (expression is DynamicExpression)
        {
            _scalarLets[name] = exprBuilder.ConvertExpression(expression);
            return true;
        }

        // Binary/arithmetic expressions on scalars: let X = EndTime - StartTime, let X = a / b
        if (expression is BinaryExpression)
        {
            try
            {
                _scalarLets[name] = exprBuilder.ConvertExpression(expression);
                return true;
            }
            catch
            {
                // If conversion fails, it's probably a tabular expression — let it fall through
            }
        }

        return false;
    }

    internal string ConvertNode(SyntaxNode node)
    {
        return node switch
        {
            QueryBlock qb => ConvertQueryBlock(qb),
            PipeExpression pipe => ConvertPipe(pipe),
            RangeOperator range => _operators.ConvertRange(range),
            UnionOperator union => _operators.ConvertUnion(union),
            NameReference nr => $"SELECT * FROM {nr.Name.ToString().Trim()}",
            ParenthesizedExpression pe => ConvertNode(pe.Expression),
            FunctionCallExpression fce => ConvertFunctionCall(fce),
            MaterializeExpression matExpr => ConvertNode(matExpr.Expression),
            ExpressionStatement exprStmt => ConvertNode(exprStmt.Expression),
            PrintOperator print => _operators.ConvertPrint(print),
            DataTableExpression dt => _operators.ConvertDataTable(dt),
            ExternalDataExpression ed => _operators.ConvertExternalData(ed),
            FunctionBody fb => ConvertFunctionBody(fb),
            // Standalone operators (not piped) — wrap in a subquery
            QueryOperator op => _operators.ApplyOperator("SELECT *", op),
            // toscalar() in non-let context — wrap as a SELECT
            ToScalarExpression tse => $"SELECT {_operators.ExpressionBuilder.ConvertExpression(tse)} AS Value",
            // Scalar expressions that appear as standalone nodes (e.g. in let statements)
            Expression expr when expr is BinaryExpression or LiteralExpression or DynamicExpression =>
                $"SELECT {_operators.ExpressionBuilder.ConvertExpression(expr)} AS value",
            _ => throw new NotSupportedException($"Unsupported node type {node.Kind}")
        };
    }

    private string ConvertFunctionCall(FunctionCallExpression fce)
    {
        var functionName = fce.Name.ToString().Trim();
        var functionNameLower = functionName.ToLowerInvariant();

        if (functionNameLower == "view")
        {
            if (fce.ArgumentList.Expressions.Count != 1)
            {
                throw new NotSupportedException($"{functionName}() expects exactly one argument");
            }
            return ConvertNode(fce.ArgumentList.Expressions[0].Element);
        }

        // CTE reference: let myFunc = view() { ... }; ... myFunc() → SELECT * FROM myFunc
        if (_ctes.ContainsKey(functionName) && fce.ArgumentList.Expressions.Count == 0)
        {
            return $"SELECT * FROM {functionName}";
        }

        // User-defined parameterized function call → inline body, wrap as query
        if (_userFunctions.ContainsKey(functionName))
        {
            var inlined = _operators.ExpressionBuilder.ConvertExpression(fce);
            if (inlined.TrimStart().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) ||
                inlined.TrimStart().StartsWith("WITH", StringComparison.OrdinalIgnoreCase))
                return inlined;
            return $"SELECT * FROM ({inlined})";
        }

        // Try the expression builder (handles case, iif, scalar functions, etc.)
        try
        {
            var sql = _operators.ExpressionBuilder.ConvertExpression(fce);
            return $"SELECT {sql} AS Value";
        }
        catch
        {
            throw new NotSupportedException($"Unsupported function {functionName}");
        }
    }

    private string ConvertFunctionBody(FunctionBody fb)
    {
        // FunctionBody has direct children: Statements (let bindings) and Expression (result)
        var savedCtes = new Dictionary<string, (string, bool)>(_ctes);
        var savedScalars = new Dictionary<string, string>(_scalarLets);
        var savedFuncs = new Dictionary<string, (string[], FunctionBody)>(_userFunctions);
        try
        {
            // Process only the IMMEDIATE let statements (not nested ones in deeper bodies)
            foreach (var stmt in fb.Statements)
            {
                if (stmt.Element is LetStatement ls)
                    ProcessLetStatement(ls);
            }

            // The Expression property is the result
            if (fb.Expression != null)
                return ConvertNode(fb.Expression);

            // Fallback: look for a name reference (simple table reference)
            var nameReference = fb.GetDescendants<NameReference>().FirstOrDefault();
            if (nameReference != null)
                return ConvertNode(nameReference);

            throw new NotSupportedException($"Function body contains no recognizable query pattern. Content: '{fb}'");
        }
        finally
        {
            _ctes.Clear();
            foreach (var kv in savedCtes) _ctes[kv.Key] = kv.Value;
            _scalarLets.Clear();
            foreach (var kv in savedScalars) _scalarLets[kv.Key] = kv.Value;
            _userFunctions.Clear();
            foreach (var kv in savedFuncs) _userFunctions[kv.Key] = kv.Value;
        }
    }

    private string ConvertPipe(PipeExpression pipe)
    {
        var leftSql = ConvertNode(pipe.Expression);
        return _operators.ApplyOperator(leftSql, pipe.Operator, pipe.Expression);
    }

    private static string StripComments(string text)
    {
        var sb = new StringBuilder();
        bool inSingleLine = false, inMultiLine = false;
        bool inSingleQuote = false, inDoubleQuote = false;
        bool lineHasCode = false;
        int lineStart = 0;
        bool skipWhiteUntilNewline = false;

        for (int i = 0; i < text.Length; i++)
        {
            var c = text[i];

            if (skipWhiteUntilNewline)
            {
                if (c == '\n')
                {
                    skipWhiteUntilNewline = false;
                    lineHasCode = false;
                    lineStart = sb.Length;
                }
                else if (char.IsWhiteSpace(c))
                {
                    continue;
                }
                else
                {
                    skipWhiteUntilNewline = false;
                }
            }

            if (inSingleLine)
            {
                if (c == '\n')
                {
                    inSingleLine = false;
                    if (lineHasCode)
                    {
                        sb.Append('\n');
                    }
                    lineHasCode = false;
                    lineStart = sb.Length;
                }
                continue;
            }

            if (inMultiLine)
            {
                if (c == '*' && i + 1 < text.Length && text[i + 1] == '/')
                {
                    inMultiLine = false;
                    i++;
                    if (!lineHasCode)
                    {
                        skipWhiteUntilNewline = true;
                    }
                }
                continue;
            }

            if (!inSingleQuote && !inDoubleQuote && c == '/' && i + 1 < text.Length)
            {
                if (text[i + 1] == '/')
                {
                    inSingleLine = true;
                    if (!lineHasCode)
                    {
                        sb.Length = lineStart;
                    }
                    i++;
                    continue;
                }
                if (text[i + 1] == '*')
                {
                    inMultiLine = true;
                    if (!lineHasCode)
                    {
                        sb.Length = lineStart;
                    }
                    i++;
                    continue;
                }
            }

            if (c == '\'' && !inDoubleQuote)
            {
                inSingleQuote = !inSingleQuote;
                sb.Append(c);
                lineHasCode = true;
                continue;
            }

            if (c == '"' && !inSingleQuote)
            {
                inDoubleQuote = !inDoubleQuote;
                sb.Append(c);
                lineHasCode = true;
                continue;
            }

            if (c == '\n')
            {
                sb.Append('\n');
                lineHasCode = false;
                lineStart = sb.Length;
                continue;
            }

            if (!char.IsWhiteSpace(c))
            {
                lineHasCode = true;
            }

            sb.Append(c);
        }

        return sb.ToString();
    }
}

