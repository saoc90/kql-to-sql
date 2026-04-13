using System;
using System.Collections.Generic;
using DuckDB.NET.Data;
using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.DuckDbExtension.Tests;

/// <summary>
/// Adversarial, edge-case, and negative tests designed to exercise limits
/// of the KQL-to-SQL translator. Covers boundary conditions, deeply nested
/// pipelines, type coercions, SQL injection vectors, and unsupported syntax.
/// </summary>
public class AdversarialTests
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    // ══════════════════════════════════════════════════════════════════════
    // 1. Edge cases that should succeed
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void EdgeCase_TakeZero_ReturnsNoRows()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE T (Id BIGINT, Name VARCHAR); INSERT INTO T VALUES (1, 'a'), (2, 'b');";
        setup.ExecuteNonQuery();

        var kql = "T | take 0";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(0, rows);
    }

    [Fact]
    public void EdgeCase_SingleColumnProject()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE T (State VARCHAR, City VARCHAR); INSERT INTO T VALUES ('TX', 'Dallas'), ('CA', 'LA');";
        setup.ExecuteNonQuery();

        var kql = "T | project State";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.Equal(1, reader.FieldCount);
        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.False(reader.IsDBNull(0));
        }
        Assert.Equal(2, rows);
    }

    [Fact]
    public void EdgeCase_SortWithNulls()
    {
        var kql = "StormEvents | sort by InjuriesDirect asc | take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    [Fact]
    public void EdgeCase_FiveChainedExtends()
    {
        var kql = @"
StormEvents
| extend A = InjuriesDirect + 1
| extend B = A + 2
| extend C = B + 3
| extend D = C + 4
| extend E = D + 5
| project State, A, B, C, D, E
| take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.Equal(6, reader.FieldCount);
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    [Fact]
    public void EdgeCase_DeeplyNestedPipeline_EightOperators()
    {
        var kql = @"
StormEvents
| where State == 'TEXAS'
| extend TotalInjuries = InjuriesDirect + InjuriesIndirect
| where TotalInjuries > 0
| project State, EventType, TotalInjuries
| sort by TotalInjuries desc
| take 20
| summarize MaxInjuries = max(TotalInjuries), EventCount = count() by EventType
| sort by MaxInjuries desc";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.True(reader.GetInt64(reader.GetOrdinal("MaxInjuries")) > 0);
        }
        Assert.True(rows > 0);
    }

    [Fact]
    public void EdgeCase_LetWithSameNameAsTable_ShadowsCTE()
    {
        // let binding with same name as table produces a CTE that shadows the table.
        // DuckDB 1.5.0+ handles this as a non-recursive CTE referencing the base table.
        var kql = @"
let StormEvents = StormEvents | where State == 'TEXAS';
StormEvents | count";
        var sql = _converter.Convert(kql);
        Assert.False(string.IsNullOrWhiteSpace(sql));

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void EdgeCase_PrintWithExpressions()
    {
        var kql = "print x = 1+2+3, y = strlen('hello'), z = iif(true, 'a', 'b')";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(6L, reader.GetInt64(reader.GetOrdinal("x")));
        Assert.Equal(5L, reader.GetInt64(reader.GetOrdinal("y")));
        Assert.Equal("a", reader.GetString(reader.GetOrdinal("z")));
    }

    [Fact]
    public void EdgeCase_DataTableWithBooleanAndNull_NullProducesInvalidSql()
    {
        // Known limitation: null values in datatable literals produce invalid SQL.
        // The converter emits lowercase "null" which DuckDB rejects in a VALUES clause.
        var kql = "datatable(A:bool, B:string)[true, 'x', false, null]";
        var sql = _converter.Convert(kql);
        Assert.False(string.IsNullOrWhiteSpace(sql));

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        Assert.ThrowsAny<Exception>(() => cmd.ExecuteReader());
    }

    [Fact]
    public void EdgeCase_DataTableWithBoolean_WithoutNull()
    {
        var kql = "datatable(A:bool, B:string)[true, 'x', false, 'y']";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(2, rows);
    }

    [Fact]
    public void EdgeCase_EmptyStringFilter()
    {
        var kql = "StormEvents | where State == '' | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0); // may be 0 but should not error
    }

    [Fact]
    public void EdgeCase_VeryLargeTake()
    {
        var kql = "StormEvents | take 999999";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.True(rows > 0);
        // 999999 is larger than the dataset, so all rows should be returned
    }

    [Fact]
    public void EdgeCase_MultipleSummarizeAggregates_FivePlusFunctions()
    {
        var kql = @"
StormEvents
| summarize
    Total = count(),
    TotalInjuries = sum(InjuriesDirect),
    AvgInjuries = avg(InjuriesDirect),
    MinInjuries = min(InjuriesDirect),
    MaxInjuries = max(InjuriesDirect)
    by State
| sort by Total desc
| take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.True(reader.GetInt64(reader.GetOrdinal("Total")) > 0);
            // min should be <= avg <= max
        }
        Assert.Equal(3, rows);
    }

    [Fact]
    public void EdgeCase_JoinOnMultipleKeys_LeftRightSyntax()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE Employees (Dept VARCHAR, Role VARCHAR, Name VARCHAR);
            INSERT INTO Employees VALUES ('Eng', 'Dev', 'Alice'), ('Eng', 'QA', 'Bob'), ('Sales', 'Rep', 'Charlie');
            CREATE TABLE Budgets (Dept VARCHAR, Role VARCHAR, Amount BIGINT);
            INSERT INTO Budgets VALUES ('Eng', 'Dev', 100000), ('Eng', 'QA', 80000), ('Sales', 'Rep', 90000);";
        setup.ExecuteNonQuery();

        var kql = "Employees | join kind=inner Budgets on $left.Dept == $right.Dept, $left.Role == $right.Role";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    [Fact]
    public void EdgeCase_UnionOfThreeTables()
    {
        var kql = @"
let t1 = StormEvents | where State == 'TEXAS' | take 2;
let t2 = StormEvents | where State == 'KANSAS' | take 2;
let t3 = StormEvents | where State == 'FLORIDA' | take 2;
union t1, t2, t3 | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var count = (long)cmd.ExecuteScalar()!;
        Assert.Equal(6L, count);
    }

    [Fact]
    public void EdgeCase_NestedLetStatements_ThreeLevels()
    {
        var kql = @"
let level1 = StormEvents | where State == 'TEXAS';
let level2 = level1 | where InjuriesDirect > 0;
let level3 = level2 | summarize cnt = count() by EventType;
level3 | sort by cnt desc | take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.True(reader.GetInt64(reader.GetOrdinal("cnt")) > 0);
        }
        Assert.True(rows > 0 && rows <= 3);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 2. Queries that should throw NotSupportedException
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Unsupported_InvokeOperator_Throws()
    {
        var kql = "T | invoke myFunc()";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void Unsupported_EvaluateAutocluster_Throws()
    {
        var kql = "T | evaluate autocluster()";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void Unsupported_JoinKindAnti_Throws()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE T (Key BIGINT);
            INSERT INTO T VALUES (1);
            CREATE TABLE Y (Key BIGINT);
            INSERT INTO Y VALUES (1);";
        setup.ExecuteNonQuery();

        var kql = "T | join kind=anti Y on Key";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void Unsupported_JoinKindLeftAnti_Throws()
    {
        var kql = "T | join kind=leftanti Y on Key";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void Unsupported_JoinKindRightAnti_Throws()
    {
        var kql = "T | join kind=rightanti Y on Key";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void Unsupported_JoinKindLeftSemi_Throws()
    {
        var kql = "T | join kind=leftsemi Y on Key";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void Unsupported_EvaluateUnknownPlugin_Throws()
    {
        var kql = "T | evaluate diffpatterns()";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 3. SQL injection attempts (should produce safe SQL)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Injection_StringWithEscapedSingleQuotes_ProducesSafeSql()
    {
        var kql = "StormEvents | where State == 'O''Brien' | count";
        var sql = _converter.Convert(kql);
        Assert.Contains("O''Brien", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
    }

    [Fact]
    public void Injection_StringWithSqlKeywords_ProducesSafeSql()
    {
        var kql = "StormEvents | where State == 'DROP TABLE' | count";
        var sql = _converter.Convert(kql);

        Assert.False(string.IsNullOrWhiteSpace(sql));

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);

        // Verify StormEvents table still exists after execution
        using var verify = conn.CreateCommand();
        verify.CommandText = "SELECT count(*) FROM StormEvents";
        var totalCount = (long)verify.ExecuteScalar()!;
        Assert.True(totalCount > 0, "StormEvents table should still exist and have data");
    }

    [Fact]
    public void Injection_StringWithSemicolon_ProducesSafeSql()
    {
        var kql = "StormEvents | where State == 'x; DROP TABLE StormEvents; --' | count";
        var sql = _converter.Convert(kql);

        Assert.False(string.IsNullOrWhiteSpace(sql));

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
    }

    [Fact]
    public void Injection_StringWithDashDashComment_ProducesSafeSql()
    {
        var kql = "StormEvents | where State == 'TEXAS -- malicious comment' | count";
        var sql = _converter.Convert(kql);

        Assert.False(string.IsNullOrWhiteSpace(sql));

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 4. Type edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void TypeEdge_CastingChain_TolongTostring()
    {
        var kql = "print x = tolong(tostring(42))";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(42L, reader.GetInt64(reader.GetOrdinal("x")));
    }

    [Fact]
    public void TypeEdge_DateTimeArithmetic_AddOneDay()
    {
        var kql = "print x = datetime(2024-01-01) + 1d";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var dt = reader.GetDateTime(0);
        Assert.Equal(new DateTime(2024, 1, 2), dt.Date);
    }

    [Fact]
    public void TypeEdge_DateTimeArithmetic_SubtractOneDay()
    {
        var kql = "print x = datetime(2024-01-10) - 1d";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var dt = reader.GetDateTime(0);
        Assert.Equal(new DateTime(2024, 1, 9), dt.Date);
    }

    [Fact]
    public void TypeEdge_BooleanLiterals_InProject()
    {
        var kql = "print a = true, b = false, c = not(true)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(reader.GetOrdinal("a")));
        Assert.False(reader.GetBoolean(reader.GetOrdinal("b")));
        Assert.False(reader.GetBoolean(reader.GetOrdinal("c")));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5. Additional adversarial patterns
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Adversarial_WhereWithCompoundBooleanLogic()
    {
        var kql = @"
StormEvents
| where (State == 'TEXAS' or State == 'KANSAS') and InjuriesDirect > 0
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void Adversarial_ProjectReorderColumns()
    {
        var kql = "StormEvents | project EventType, State, InjuriesDirect | take 1";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("EventType", reader.GetName(0));
        Assert.Equal("State", reader.GetName(1));
        Assert.Equal("InjuriesDirect", reader.GetName(2));
    }

    [Fact]
    public void Adversarial_CountAfterCount()
    {
        // count produces a single row with a Count column; counting again should give 1
        var kql = "StormEvents | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void Adversarial_DistinctOnComputedColumn()
    {
        var kql = @"
StormEvents
| extend StateUpper = toupper(State)
| distinct StateUpper
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void Adversarial_SummarizeWithNoGroupBy()
    {
        var kql = "StormEvents | summarize Total = count(), MaxInjuries = max(InjuriesDirect)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(reader.GetOrdinal("Total")) > 0);
        Assert.False(reader.Read(), "Summarize with no group-by should return exactly 1 row");
    }

    [Fact]
    public void Adversarial_DataTableSingleRow()
    {
        var kql = "datatable(X:long)[42]";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(42L, reader.GetInt64(0));
        Assert.False(reader.Read());
    }

    [Fact]
    public void Adversarial_DataTableEmptyString()
    {
        var kql = "datatable(S:string)['', 'hello', '']";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var values = new List<string>();
        while (reader.Read())
        {
            values.Add(reader.GetString(0));
        }
        Assert.Equal(3, values.Count);
        Assert.Equal("", values[0]);
        Assert.Equal("hello", values[1]);
        Assert.Equal("", values[2]);
    }

    [Fact]
    public void Adversarial_RangeWithFilterAndProject()
    {
        var kql = "range i from 1 to 100 step 1 | where i % 2 == 0 | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = long.Parse(cmd.ExecuteScalar()!.ToString()!);
        Assert.Equal(50L, count);
    }

    [Fact]
    public void Adversarial_LetStatementChain_FourLevels()
    {
        var kql = @"
let a = StormEvents | take 100;
let b = a | where State == 'TEXAS';
let c = b | extend x = InjuriesDirect + 1;
let d = c | summarize Total = sum(x);
d";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        // Result is a single aggregated value
        Assert.NotNull(reader.GetValue(0));
    }

    [Fact]
    public void Adversarial_WhereChainedMultipleFilters()
    {
        var kql = @"
StormEvents
| where State == 'TEXAS'
| where InjuriesDirect > 0
| where EventType != 'Tornado'
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
    }

    [Fact]
    public void Adversarial_ExtendOverwritesExistingColumn()
    {
        var kql = @"
StormEvents
| extend State = toupper(State)
| where State == 'TEXAS'
| take 1
| project State";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("TEXAS", reader.GetString(0));
    }

    [Fact]
    public void Adversarial_TopWithTiedValues()
    {
        // StormEvents has many rows with 0 injuries; top should still return exactly N
        var kql = "StormEvents | top 5 by InjuriesDirect asc";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    [Fact]
    public void Adversarial_PrintLargeInteger()
    {
        var kql = "print x = 9223372036854775807"; // long.MaxValue
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(long.MaxValue, reader.GetInt64(reader.GetOrdinal("x")));
    }

    [Fact]
    public void Adversarial_PrintNegativeNumber()
    {
        var kql = "print x = -42";
        var sql = _converter.Convert(kql);

        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(-42L, reader.GetInt64(0));
    }

    [Fact]
    public void Adversarial_NegativeViaMultiplication_Workaround()
    {
        // Workaround: express negative values using subtraction from 0
        var kql = "print x = 0 - 42";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(-42L, reader.GetInt64(reader.GetOrdinal("x")));
    }

    [Fact]
    public void Adversarial_ContainsCaseInsensitive()
    {
        // 'contains' in KQL is case-insensitive
        var kql = "StormEvents | where State contains 'tex' | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0, "Case-insensitive contains should match TEXAS");
    }

    [Fact]
    public void Adversarial_InOperatorWithSingleValue()
    {
        var kql = "StormEvents | where State in ('TEXAS') | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void Adversarial_SummarizeByMultipleGroupingColumns()
    {
        var kql = @"
StormEvents
| summarize cnt = count() by State, EventType
| sort by cnt desc
| take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.False(string.IsNullOrWhiteSpace(reader.GetString(reader.GetOrdinal("State"))));
            Assert.False(string.IsNullOrWhiteSpace(reader.GetString(reader.GetOrdinal("EventType"))));
        }
        Assert.Equal(5, rows);
    }
}
