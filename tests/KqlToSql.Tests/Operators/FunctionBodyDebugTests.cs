using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace KqlToSql.Tests.Operators
{
    public class FunctionBodyDebugTests
    {
        private readonly ITestOutputHelper _output;

        public FunctionBodyDebugTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void DebugViewFunctionTranslation()
        {
            // Test view function declaration
            var kql = "let T_view = view () { StormEvents | where STATE == 'TEXAS' };";
            
            var converter = new KqlToSqlConverter();
            
            try
            {
                var sql = converter.Convert(kql);
                _output.WriteLine($"Converted SQL: {sql}");
            }
            catch (System.Exception ex)
            {
                _output.WriteLine($"Exception: {ex.Message}");
                _output.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }
    }
}
