using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace KqlToSql.Tests.Operators
{
    public class FunctionBodyDebugTests2
    {
        private readonly ITestOutputHelper _output;

        public FunctionBodyDebugTests2(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void DebugFunctionBodyStatements()
        {
            // Test view function declaration
            var kql = "let T_view = view () { StormEvents | where STATE == 'TEXAS' };";
            
            var converter = new KqlToSqlConverter();
            
            try
            {
                var sql = converter.Convert(kql);
                _output.WriteLine($"Converted SQL: {sql}");
            }
            catch (System.NotSupportedException ex) when (ex.Message.Contains("Function body must contain exactly one statement"))
            {
                _output.WriteLine($"Function body contains more than one statement - this is the issue we need to debug");
                
                // We need to understand the structure better
                // Let's create a simpler test to see what's in the function body
                _output.WriteLine("Expected: The view() function body should be treated as a single query expression");
            }
        }
    }
}
