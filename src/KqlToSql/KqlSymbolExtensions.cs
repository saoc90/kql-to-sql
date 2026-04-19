using System;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;

namespace KqlToSql;

/// <summary>
/// Helpers for comparing a parsed KQL <see cref="FunctionCallExpression"/> against
/// strongly typed <see cref="FunctionSymbol"/>s from <c>Kusto.Language.Aggregates</c>
/// and <c>Kusto.Language.Functions</c>. Prefer <c>fce.Is(Aggregates.ArgMax)</c> over
/// magic-string equality so the parser owns the canonical name.
/// </summary>
internal static class KqlSymbolExtensions
{
    internal static bool Is(this FunctionCallExpression fce, FunctionSymbol symbol) =>
        string.Equals(fce.Name.SimpleName, symbol.Name, StringComparison.OrdinalIgnoreCase);

    internal static bool IsAny(this FunctionCallExpression fce, params FunctionSymbol[] symbols)
    {
        foreach (var s in symbols)
            if (fce.Is(s)) return true;
        return false;
    }
}
