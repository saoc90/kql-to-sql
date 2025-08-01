using System;
using Kusto.Language;
using Kusto.Language.Syntax;
using KqlToSql.Operators;
using KqlToSql.Commands;

namespace KqlToSql;

public class KqlToSqlConverter
{
    private readonly OperatorSqlTranslator _operators;
    private readonly CommandSqlTranslator _commands;

    public KqlToSqlConverter()
    {
        _operators = new OperatorSqlTranslator(this);
        _commands = new CommandSqlTranslator();
    }

    public string Convert(string kql)
    {
        var code = KustoCode.Parse(kql);
        var root = code.Syntax;
        if (root is CommandBlock)
        {
            return _commands.Translate(kql);
        }

        var pipe = root.GetFirstDescendant<PipeExpression>();
        if (pipe != null)
        {
            return ConvertNode(pipe);
        }

        var name = root.GetFirstDescendant<NameReference>();
        if (name != null)
        {
            return $"SELECT * FROM {name.Name}";
        }

        throw new NotSupportedException("Unsupported KQL query");
    }

    internal string ConvertNode(SyntaxNode node)
    {
        return node switch
        {
            PipeExpression pipe => ConvertPipe(pipe),
            NameReference nr => $"SELECT * FROM {nr.Name.ToString().Trim()}",
            _ => throw new NotSupportedException($"Unsupported node type {node.Kind}")
        };
    }

    private string ConvertPipe(PipeExpression pipe)
    {
        var leftSql = ConvertNode(pipe.Expression);
        return _operators.ApplyOperator(leftSql, pipe.Operator);
    }
}

