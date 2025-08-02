using System.Linq;
using Kusto.Language;
using Kusto.Language.Syntax;
using Xunit;
using Xunit.Abstractions;

namespace KqlToSql.Tests.Operators
{
    public class ViewParsingDebugTests
    {
        private readonly ITestOutputHelper _output;

        public ViewParsingDebugTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void DebugViewFunctionStructure()
        {
            // Parse the view function declaration
            var query = "let T_view = view () { StormEvents | where STATE == 'TEXAS' };";
            var parsedQuery = KustoCode.Parse(query);
            var letStatement = parsedQuery.Syntax.GetDescendants<LetStatement>().First();
            var functionDeclaration = letStatement.Expression as FunctionDeclaration;

            _output.WriteLine($"Full FunctionDeclaration: '{functionDeclaration}'");
            _output.WriteLine($"ViewKeyword: '{functionDeclaration.ViewKeyword}'");
            _output.WriteLine($"Body: '{functionDeclaration.Body}'");
            _output.WriteLine($"Body Type: {functionDeclaration.Body.GetType().Name}");
            _output.WriteLine($"Body.Statements.Count: {functionDeclaration.Body.Statements.Count}");
            
            // Let's examine what's inside the body
            _output.WriteLine($"Raw Body Content: '{functionDeclaration.Body.ToString()}'");
            
            // Let's also look at specific properties
            if (functionDeclaration.Body.Statements.Count > 0)
            {
                for (int i = 0; i < functionDeclaration.Body.Statements.Count; i++)
                {
                    var statement = functionDeclaration.Body.Statements[i];
                    _output.WriteLine($"Statement {i}: Type={statement.GetType().Name}, Content='{statement}'");
                }
            }
            else
            {
                _output.WriteLine("No statements found in function body");
                
                // Let's check if the body has other properties or content
                _output.WriteLine($"Body's string representation: '{functionDeclaration.Body}'");
                _output.WriteLine($"Body.Kind: {functionDeclaration.Body.Kind}");
                
                // Maybe the query is in a different property?
                // Let's see if we can find any query-related content
                var allDescendants = functionDeclaration.GetDescendants<SyntaxNode>().ToList();
                _output.WriteLine($"Total descendants in FunctionDeclaration: {allDescendants.Count}");
                
                foreach (var descendant in allDescendants.Take(10))
                {
                    _output.WriteLine($"Descendant: Type={descendant.GetType().Name}, Content='{descendant.ToString().Replace("\n", "\\n").Replace("\r", "")}'");
                }
            }
        }
    }
}
