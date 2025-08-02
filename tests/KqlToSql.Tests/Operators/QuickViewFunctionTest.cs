using Xunit;
using Xunit.Abstractions;

namespace KqlToSql.Tests.Operators
{
    public class QuickViewFunctionTest
    {
        private readonly ITestOutputHelper _output;

        public QuickViewFunctionTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void Test_View_Function_Declaration_Works()
        {
            // Test the view function declaration syntax with a main query
            var kql = @"
let T_view = view () { StormEvents | where STATE == 'TEXAS' };
T_view | top 5 by EVENT_TYPE
";
            
            var converter = new KqlToSqlConverter();
            var sql = converter.Convert(kql);
            
            _output.WriteLine($"KQL: {kql}");
            _output.WriteLine($"SQL: {sql}");
            
            // Basic assertions
            Assert.NotNull(sql);
            Assert.Contains("T_view", sql);
            Assert.Contains("NOT MATERIALIZED", sql);
            Assert.Contains("SELECT * FROM StormEvents WHERE STATE = 'TEXAS'", sql);
        }
    }
}
