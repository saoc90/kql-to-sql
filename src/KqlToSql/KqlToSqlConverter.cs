using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kusto.Language;
using Kusto.Language.Syntax;
using KqlToSql.Operators;
using KqlToSql.Commands;

namespace KqlToSql;

public class KqlToSqlConverter
{
    private readonly OperatorSqlTranslator _operators;
    private readonly CommandSqlTranslator _commands;
    private readonly Dictionary<string, (string sql, bool materialized)> _ctes = new();

    public KqlToSqlConverter()
    {
        _operators = new OperatorSqlTranslator(this);
        _commands = new CommandSqlTranslator(this);
    }

    public string Convert(string kql)
    {
        kql = StripComments(kql);

        var code = KustoCode.Parse(kql);
        var root = code.Syntax;
        
        _ctes.Clear(); // Reset CTEs for each conversion
        
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
        
        // Process let statements first to build CTEs
        foreach (var statement in statements)
        {
            if (statement is LetStatement letStatement)
            {
                ProcessLetStatement(letStatement);
            }
        }
        
        // Find the main query (the last statement that's not a let statement)
        var mainStatement = statements.LastOrDefault(s => s is not LetStatement);
        if (mainStatement == null)
        {
            throw new NotSupportedException("No main query found in query block");
        }
        
        string mainSql;
        if (mainStatement is ExpressionStatement exprStatement)
        {
            mainSql = ConvertNode(exprStatement.Expression);
        }
        else
        {
            mainSql = ConvertNode(mainStatement);
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
        
        bool materialized = false;
        string sql;
        
        if (expression is MaterializeExpression matExpr)
        {
            materialized = true;
            sql = ConvertNode(matExpr.Expression);
        }
        else if (expression is FunctionDeclaration funcDecl)
        {
            // This handles view () { ... } syntax
            materialized = false;
            
            // Check if it's a view function declaration using ViewKeyword
            var viewKeyword = funcDecl.ViewKeyword?.ToString().Trim().ToLowerInvariant();
            if (viewKeyword == "view")
            {
                // Extract the body of the view function
                sql = ConvertNode(funcDecl.Body);
            }
            else
            {
                throw new NotSupportedException($"Function declaration with keyword '{viewKeyword}' is not supported. Only 'view' function declarations are supported.");
            }
        }
        else if (expression is FunctionCallExpression fce)
        {
            var functionName = fce.Name.ToString().Trim().ToLowerInvariant();
            if (functionName == "view")
            {
                materialized = false;
                if (fce.ArgumentList.Expressions.Count != 1)
                {
                    throw new NotSupportedException("view() expects exactly one argument");
                }
                sql = ConvertNode(fce.ArgumentList.Expressions[0].Element);
            }
            else
            {
                // Regular function call - treat as non-materialized
                materialized = false;
                sql = ConvertNode(expression);
            }
        }
        else
        {
            // Regular expression - treat as non-materialized (view-like behavior)
            materialized = false;
            sql = ConvertNode(expression);
        }
        
        _ctes[name] = (sql, materialized);
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
            FunctionBody fb => ConvertFunctionBody(fb),
            _ => throw new NotSupportedException($"Unsupported node type {node.Kind}")
        };
    }

    private string ConvertFunctionCall(FunctionCallExpression fce)
    {
        var functionName = fce.Name.ToString().Trim().ToLowerInvariant();
        
        if (functionName == "view")
        {
            // Direct view function call without let statement
            if (fce.ArgumentList.Expressions.Count != 1)
            {
                throw new NotSupportedException($"{functionName}() expects exactly one argument");
            }
            return ConvertNode(fce.ArgumentList.Expressions[0].Element);
        }
        
        throw new NotSupportedException($"Unsupported function {functionName}");
    }

    private string ConvertFunctionBody(FunctionBody fb)
    {
        // FunctionBody doesn't use the Statements collection for view functions
        // The actual query is stored as descendants. Look for PipeExpression or other query nodes
        
        // Try to find a PipeExpression first (most common case)
        var pipeExpression = fb.GetDescendants<PipeExpression>().FirstOrDefault();
        if (pipeExpression != null)
        {
            return ConvertNode(pipeExpression);
        }
        
        // If no pipe expression, look for a NameReference (simple table reference)
        var nameReference = fb.GetDescendants<NameReference>().FirstOrDefault();
        if (nameReference != null)
        {
            return ConvertNode(nameReference);
        }
        
        // If no recognizable query pattern found
        throw new NotSupportedException($"Function body contains no recognizable query pattern. Content: '{fb}'");
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

