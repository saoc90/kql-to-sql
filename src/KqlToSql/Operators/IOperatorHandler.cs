using Kusto.Language.Syntax;

namespace KqlToSql.Operators;

/// <summary>
/// Context passed to operator handlers, providing access to shared services.
/// </summary>
internal sealed class OperatorContext
{
    internal KqlToSqlConverter Converter { get; }
    internal Expressions.ExpressionSqlBuilder Expr { get; }
    internal ISqlDialect Dialect => Converter.Dialect;

    internal OperatorContext(KqlToSqlConverter converter)
    {
        Converter = converter;
        Expr = new Expressions.ExpressionSqlBuilder(converter.Dialect);
    }
}
