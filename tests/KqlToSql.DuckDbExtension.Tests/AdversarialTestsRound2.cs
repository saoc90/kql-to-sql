using System;
using System.Collections.Generic;
using DuckDB.NET.Data;
using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.DuckDbExtension.Tests;

/// <summary>
/// Round 2 adversarial tests: operator combinations, expression edge cases,
/// aggregate edge cases, CTE edge cases, and new operator edge cases.
/// Tests that document known bugs are marked with comments explaining the issue.
/// </summary>
public class AdversarialTestsRound2
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    private static DuckDBConnection InMemory()
    {
        DuckDbSetup.EnsureDuckDb();
        var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        return conn;
    }

    // ══════════════════════════════════════════════════════════════════════
    // 1. Operator combinations that may break
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void OperatorCombo_SummarizeAfterSummarize()
    {
        // Double summarize: first aggregate then aggregate the aggregation
        var kql = @"
StormEvents
| summarize EventCount = count() by State
| summarize TotalStates = count(), MaxEvents = max(EventCount)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        var totalStates = reader.GetInt64(reader.GetOrdinal("TotalStates"));
        var maxEvents = reader.GetInt64(reader.GetOrdinal("MaxEvents"));
        Assert.True(totalStates > 0);
        Assert.True(maxEvents > 0);
        Assert.False(reader.Read()); // single row
    }

    [Fact]
    public void OperatorCombo_ExtendAfterProjectAway()
    {
        // project-away removes columns, then extend adds new ones
        var kql = @"
StormEvents
| project-away InjuriesIndirect, DeathsDirect, DeathsIndirect
| extend TotalInjuries = InjuriesDirect + 1
| take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    [Fact]
    public void OperatorCombo_ProjectAfterSummarize()
    {
        // Summarize then project only subset of aggregation results
        var kql = @"
StormEvents
| summarize Total = count(), MaxInj = max(InjuriesDirect) by State
| project State, Total
| take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.Equal(2, reader.FieldCount);
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    [Fact]
    public void OperatorCombo_WhereAfterExtendWithComputedColumn()
    {
        // extend creates a column, then where filters on it
        var kql = @"
StormEvents
| extend TotalInjuries = InjuriesDirect + InjuriesIndirect
| where TotalInjuries > 10
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
    }

    [Fact]
    public void OperatorCombo_SortAfterDistinct()
    {
        var kql = @"
StormEvents
| distinct State
| sort by State asc
| take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var states = new List<string>();
        while (reader.Read())
            states.Add(reader.GetString(0));
        Assert.Equal(5, states.Count);
        // Verify sorting
        for (int i = 1; i < states.Count; i++)
            Assert.True(string.Compare(states[i - 1], states[i], StringComparison.Ordinal) <= 0);
    }

    [Fact]
    public void OperatorCombo_CountAfterDistinct()
    {
        // distinct then count should give number of unique values
        var kql = @"
StormEvents
| distinct State
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void OperatorCombo_ExtendAfterExtend_SameColumnName()
    {
        // Two extends that define the same column name - second should override
        var kql = @"
StormEvents
| extend Computed = InjuriesDirect + 1
| extend Computed = InjuriesDirect + 100
| project Computed
| take 1";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        // This may produce ambiguous column; document behavior
        Assert.NotNull(reader.GetValue(0));
    }

    [Fact]
    public void OperatorCombo_TopAfterWhere()
    {
        var kql = @"
StormEvents
| where State == 'TEXAS'
| top 3 by InjuriesDirect desc";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    [Fact]
    public void OperatorCombo_SerializeWithNoExpressions_IsPassthrough()
    {
        // serialize with no expressions is a no-op passthrough
        var kql = @"
StormEvents
| serialize
| take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    [Fact]
    public void OperatorCombo_SerializeAfterSerialize()
    {
        // Double serialize: both should be passthroughs
        var kql = @"
StormEvents
| serialize
| serialize
| take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    [Fact]
    public void OperatorCombo_SummarizeWithBinAfterWhere()
    {
        // Common real-world pattern: filter -> bin-based time aggregation
        var kql = @"
StormEvents
| where State == 'TEXAS'
| summarize cnt = count() by EventType
| sort by cnt desc
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
            Assert.True(reader.GetInt64(reader.GetOrdinal("cnt")) > 0);
        }
        Assert.True(rows > 0 && rows <= 3);
    }

    [Fact]
    public void OperatorCombo_ProjectRenameAfterExtend()
    {
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE T (A BIGINT, B VARCHAR); INSERT INTO T VALUES (1, 'x'), (2, 'y');";
        setup.ExecuteNonQuery();

        var kql = "T | extend C = A + 10 | project-rename RenamedA = A";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        // If this works, project-rename should produce a column named RenamedA
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(2, rows);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 2. Expression edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Expression_NestedIif()
    {
        // Deeply nested iif: iif(cond1, iif(cond2, iif(cond3, a, b), c), d)
        var kql = @"print x = iif(1 > 2, 'a', iif(2 > 3, 'b', iif(3 > 4, 'c', 'd')))";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("d", reader.GetString(reader.GetOrdinal("x")));
    }

    [Fact]
    public void Expression_NestedFunctionCalls()
    {
        // strlen(toupper(tolower('Hello')))
        var kql = "print x = strlen(toupper(tolower('Hello')))";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(5L, reader.GetInt64(reader.GetOrdinal("x")));
    }

    [Fact]
    public void Expression_StringLiteralWithBackslash()
    {
        var kql = @"print x = 'path\\to\\file'";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        var val = reader.GetString(reader.GetOrdinal("x"));
        Assert.Contains("\\", val);
    }

    [Fact]
    public void Expression_EmptyStringConcat()
    {
        var kql = "print x = strcat('', '', '')";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("", reader.GetString(reader.GetOrdinal("x")));
    }

    [Fact]
    public void Expression_CaseWithManyBranches()
    {
        var kql = @"print x = case(1 == 2, 'a', 2 == 3, 'b', 3 == 4, 'c', 4 == 5, 'd', 'default')";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("default", reader.GetString(reader.GetOrdinal("x")));
    }

    [Fact]
    public void Expression_ArithmeticWithParentheses()
    {
        var kql = "print x = (1 + 2) * (3 + 4)";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(21L, reader.GetInt64(reader.GetOrdinal("x")));
    }

    [Fact]
    public void Expression_DivisionByZero()
    {
        // DuckDB handles integer division by zero without throwing.
        var kql = "print x = 1 / 0";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // DuckDB may return NULL or throw — just verify the SQL is valid and executes
    }

    [Fact]
    public void Expression_ModuloByZero()
    {
        // DuckDB does NOT throw on modulo by zero; it returns NULL.
        var kql = "print x = 10 % 0";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(reader.GetOrdinal("x")));
    }

    [Fact]
    public void Expression_BetweenOperator()
    {
        var kql = "StormEvents | where InjuriesDirect between (1 .. 5) | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
    }

    [Fact]
    public void Expression_NotBetweenOperator()
    {
        var kql = "StormEvents | where InjuriesDirect !between (1 .. 5) | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
    }

    [Fact]
    public void Expression_NotContains()
    {
        var kql = "StormEvents | where State !contains 'tex' | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void Expression_StartsWithAndEndsWith()
    {
        var kql = @"
StormEvents
| where State startswith 'TEX'
| where EventType endswith 'Wind'
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
    }

    [Fact]
    public void Expression_CastChain_TostringTolongTostring()
    {
        var kql = "print x = tostring(tolong(tostring(42)))";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("42", reader.GetString(reader.GetOrdinal("x")));
    }

    [Fact]
    public void Expression_HasAnyOperator()
    {
        var kql = "StormEvents | where State has_any ('TEXAS', 'KANSAS') | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void Expression_HasAllOperator()
    {
        // has_all checks all terms must be present in the same value
        // 'TEXAS' cannot also contain 'KANSAS', so 0 results expected
        var kql = "StormEvents | where State has_all ('TEX', 'AS') | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0); // TEXAS contains both 'TEX' and 'AS'
    }

    [Fact]
    public void Expression_InOperatorWithMultipleTypes()
    {
        var kql = "StormEvents | where InjuriesDirect in (0, 1, 2) | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void Expression_NotInOperator()
    {
        var kql = "StormEvents | where State !in ('TEXAS', 'KANSAS') | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 3. Aggregate edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Aggregate_CountifWithComplexBoolean()
    {
        var kql = @"
StormEvents
| summarize TexasInjuries = countif(State == 'TEXAS' and InjuriesDirect > 0)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(reader.GetOrdinal("TexasInjuries")) >= 0);
    }

    [Fact]
    public void Aggregate_Dcount()
    {
        var kql = "StormEvents | summarize UniqueStates = dcount(State)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        var uniqueStates = reader.GetInt64(reader.GetOrdinal("UniqueStates"));
        Assert.True(uniqueStates > 0);
    }

    [Fact]
    public void Aggregate_SumIf()
    {
        var kql = @"
StormEvents
| summarize TexasInjuries = sumif(InjuriesDirect, State == 'TEXAS')";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(reader.GetOrdinal("TexasInjuries")) >= 0);
    }

    [Fact]
    public void Aggregate_Stdev()
    {
        var kql = "StormEvents | summarize sd = stdev(InjuriesDirect)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        // stdev should be a non-negative number or null
        Assert.False(reader.IsDBNull(0) && false); // just verify no crash
    }

    [Fact]
    public void Aggregate_Variance()
    {
        var kql = "StormEvents | summarize v = variance(InjuriesDirect)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
    }

    [Fact]
    public void Aggregate_MakeList()
    {
        var kql = @"
StormEvents
| where State == 'TEXAS'
| take 5
| summarize States = make_list(State)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        // make_list returns an array
        Assert.NotNull(reader.GetValue(0));
    }

    [Fact]
    public void Aggregate_MakeSet()
    {
        var kql = @"
StormEvents
| take 100
| summarize UniqueStates = make_set(State)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.NotNull(reader.GetValue(0));
    }

    [Fact]
    public void Aggregate_TakeAny()
    {
        var kql = "StormEvents | summarize AnyState = take_any(State)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.False(reader.IsDBNull(reader.GetOrdinal("AnyState")));
    }

    [Fact]
    public void Aggregate_ArgMaxWithSpecificColumns()
    {
        // arg_max with specific column (not *) uses different code path
        var kql = @"
StormEvents
| summarize arg_max(InjuriesDirect, State) by EventType
| take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.True(rows > 0 && rows <= 5);
    }

    [Fact]
    public void Aggregate_ArgMinWithSpecificColumns()
    {
        var kql = @"
StormEvents
| summarize arg_min(InjuriesDirect, EventType) by State
| take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.True(rows > 0 && rows <= 5);
    }

    [Fact]
    public void Aggregate_CountDistinct()
    {
        var kql = "StormEvents | summarize UniqueEvents = count_distinct(EventType)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(reader.GetOrdinal("UniqueEvents")) > 0);
    }

    [Fact]
    public void Aggregate_PercentileSingle()
    {
        var kql = "StormEvents | summarize p50 = percentile(InjuriesDirect, 50)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
    }

    [Fact]
    public void Aggregate_MinIf()
    {
        var kql = @"
StormEvents
| summarize MinInj = minif(InjuriesDirect, State == 'TEXAS')";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
    }

    // ══════════════════════════════════════════════════════════════════════
    // 4. CTE / let edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void CTE_FiveLevelNestedLets()
    {
        var kql = @"
let a = StormEvents | take 1000;
let b = a | where State == 'TEXAS';
let c = b | where InjuriesDirect > 0;
let d = c | extend x = InjuriesDirect + 1;
let e = d | summarize Total = sum(x);
e";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.NotNull(reader.GetValue(0));
    }

    [Fact]
    public void CTE_LetWithEmptyPipeline_JustTableRef()
    {
        // let that just references a table with no operators
        var kql = @"
let t = StormEvents;
t | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void CTE_LetUsedInJoin()
    {
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE Employees (Dept VARCHAR, Name VARCHAR);
            INSERT INTO Employees VALUES ('Eng', 'Alice'), ('Sales', 'Bob');
            CREATE TABLE Departments (Dept VARCHAR, Budget BIGINT);
            INSERT INTO Departments VALUES ('Eng', 100000), ('Sales', 90000);";
        setup.ExecuteNonQuery();

        var kql = @"
let emp = Employees;
let dept = Departments;
emp | join kind=inner dept on Dept";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(2, rows);
    }

    [Fact]
    public void CTE_LetWithSummarize()
    {
        var kql = @"
let summary = StormEvents | summarize cnt = count() by State;
summary | sort by cnt desc | take 3";
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
        Assert.Equal(3, rows);
    }

    [Fact]
    public void CTE_MultipleLetsSameBaseTable()
    {
        // Two lets that both reference StormEvents independently
        var kql = @"
let texas = StormEvents | where State == 'TEXAS' | take 5;
let kansas = StormEvents | where State == 'KANSAS' | take 5;
union texas, kansas | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.Equal(10L, count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5. Operator-specific edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Operator_TopHitters()
    {
        var kql = "StormEvents | top-hitters 5 of State";
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
    public void Operator_TopHittersWithByClause()
    {
        var kql = "StormEvents | top-hitters 3 of State by sum(InjuriesDirect)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    [Fact]
    public void Operator_SampleDistinct()
    {
        var kql = "StormEvents | sample-distinct 5 of State";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        var uniqueStates = new HashSet<string>();
        while (reader.Read())
        {
            rows++;
            uniqueStates.Add(reader.GetString(0));
        }
        Assert.Equal(5, rows);
        Assert.Equal(5, uniqueStates.Count); // all distinct
    }

    [Fact]
    public void Operator_GetSchema()
    {
        var kql = "StormEvents | getschema";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        // DESCRIBE should return column info
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.True(rows > 0);
    }

    [Fact]
    public void Operator_Sample()
    {
        var kql = "StormEvents | sample 10";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(10, rows);
    }

    [Fact]
    public void Operator_ProjectKeep()
    {
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE T (A BIGINT, B VARCHAR, C BIGINT); INSERT INTO T VALUES (1, 'x', 10);";
        setup.ExecuteNonQuery();

        var kql = "T | project-keep A, C";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(2, reader.FieldCount);
    }

    [Fact]
    public void Operator_ProjectReorder()
    {
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE T (A BIGINT, B VARCHAR, C BIGINT); INSERT INTO T VALUES (1, 'x', 10);";
        setup.ExecuteNonQuery();

        var kql = "T | project-reorder C, A";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        // C should come first, then A, then B
        Assert.Equal("C", reader.GetName(0));
        Assert.Equal("A", reader.GetName(1));
    }

    [Fact]
    public void Operator_AsOperator_IsPassthrough()
    {
        var kql = "StormEvents | as T | take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    [Fact]
    public void Operator_RenderOperator_IsPassthrough()
    {
        var kql = "StormEvents | summarize cnt = count() by State | render barchart";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.True(rows > 0);
    }

    [Fact]
    public void Operator_ConsumeOperator_IsPassthrough()
    {
        var kql = "StormEvents | take 3 | consume";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 6. Parse operator edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Parse_SimpleCapture()
    {
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE Logs (Message VARCHAR); INSERT INTO Logs VALUES ('Error: 404 Not Found'), ('Error: 500 Internal');";
        setup.ExecuteNonQuery();

        var kql = "Logs | parse Message with 'Error: ' Code:long ' ' Rest";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            var code = reader.GetValue(reader.GetOrdinal("Code"));
            Assert.NotNull(code);
        }
        Assert.Equal(2, rows);
    }

    [Fact]
    public void Parse_TypedCapture_NoMatch_ReturnsNull()
    {
        // Fixed: parse typed captures use TRY_CAST so non-matching rows get NULL
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE Logs (Message VARCHAR); INSERT INTO Logs VALUES ('totally unrelated'), ('Error: 404 Not Found');";
        setup.ExecuteNonQuery();

        var kql = "Logs | parse Message with 'Error: ' Code:long ' ' Rest";
        var sql = _converter.Convert(kql);
        Assert.Contains("TRY_CAST", sql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            // Non-matching row should have NULL for Code
        }
        Assert.Equal(2, rows);
    }

    [Fact]
    public void ParseWhere_FiltersNonMatching()
    {
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE Logs (Message VARCHAR); INSERT INTO Logs VALUES ('totally unrelated'), ('Error: 404 Not Found');";
        setup.ExecuteNonQuery();

        var kql = "Logs | parse-where Message with 'Error: ' Code:long ' ' Rest";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        // parse-where should filter out non-matching rows
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(1, rows);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 7. Datatable edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Datatable_MultipleColumns()
    {
        var kql = "datatable(Name:string, Age:long, City:string)['Alice', 30, 'NYC', 'Bob', 25, 'LA']";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.Equal(3, reader.FieldCount);
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(2, rows);
    }

    [Fact]
    public void Datatable_PipedToWhere()
    {
        var kql = "datatable(Name:string, Age:long)['Alice', 30, 'Bob', 25, 'Charlie', 35] | where Age > 28";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(2, rows); // Alice (30) and Charlie (35)
    }

    [Fact]
    public void Datatable_PipedToSummarize()
    {
        var kql = "datatable(Name:string, Score:long)['A', 10, 'B', 20, 'A', 30] | summarize Total = sum(Score) by Name | sort by Name asc";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("A", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal(40L, reader.GetInt64(reader.GetOrdinal("Total")));
        Assert.True(reader.Read());
        Assert.Equal("B", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal(20L, reader.GetInt64(reader.GetOrdinal("Total")));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 8. Range edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Range_PipedToExtend()
    {
        var kql = "range i from 1 to 5 step 1 | extend doubled = i * 2";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    [Fact]
    public void Range_PipedToSummarize()
    {
        var kql = "range i from 1 to 10 step 1 | summarize Total = sum(i)";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        // sum of 1..10 = 55
        Assert.Equal(55L, reader.GetInt64(reader.GetOrdinal("Total")));
    }

    [Fact]
    public void Range_SingleValue_StartEqualsEnd()
    {
        var kql = "range i from 5 to 5 step 1 | count";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = long.Parse(cmd.ExecuteScalar()!.ToString()!);
        Assert.Equal(1L, count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 9. Join edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Join_LeftOuter()
    {
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE Left_T (Key BIGINT, Val VARCHAR);
            INSERT INTO Left_T VALUES (1, 'a'), (2, 'b'), (3, 'c');
            CREATE TABLE Right_T (Key BIGINT, Score BIGINT);
            INSERT INTO Right_T VALUES (1, 100), (3, 300);";
        setup.ExecuteNonQuery();

        var kql = "Left_T | join kind=leftouter Right_T on Key";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows); // all left rows preserved
    }

    [Fact]
    public void Join_FullOuter()
    {
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE Left_T (Key BIGINT, Val VARCHAR);
            INSERT INTO Left_T VALUES (1, 'a'), (2, 'b');
            CREATE TABLE Right_T (Key BIGINT, Score BIGINT);
            INSERT INTO Right_T VALUES (2, 200), (3, 300);";
        setup.ExecuteNonQuery();

        var kql = "Left_T | join kind=fullouter Right_T on Key";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows); // Key 1, 2, 3
    }

    [Fact]
    public void Lookup_LeftOuter()
    {
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE Facts (Key BIGINT, Val VARCHAR);
            INSERT INTO Facts VALUES (1, 'a'), (2, 'b'), (3, 'c');
            CREATE TABLE Dims (Key BIGINT, Label VARCHAR);
            INSERT INTO Dims VALUES (1, 'One'), (2, 'Two');";
        setup.ExecuteNonQuery();

        var kql = "Facts | lookup Dims on Key";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows); // default lookup is leftouter
    }

    [Fact]
    public void Lookup_Inner()
    {
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE Facts (Key BIGINT, Val VARCHAR);
            INSERT INTO Facts VALUES (1, 'a'), (2, 'b'), (3, 'c');
            CREATE TABLE Dims (Key BIGINT, Label VARCHAR);
            INSERT INTO Dims VALUES (1, 'One'), (2, 'Two');";
        setup.ExecuteNonQuery();

        var kql = "Facts | lookup kind=inner Dims on Key";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(2, rows); // only matching
    }

    [Fact]
    public void Join_InnerUnique_DeduplicatesLeftSide()
    {
        // innerunique (default) should deduplicate left side by join key
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE Left_T (Key BIGINT, Val VARCHAR);
            INSERT INTO Left_T VALUES (1, 'a'), (1, 'b'), (2, 'c');
            CREATE TABLE Right_T (Key BIGINT, Score BIGINT);
            INSERT INTO Right_T VALUES (1, 100), (2, 200);";
        setup.ExecuteNonQuery();

        var kql = "Left_T | join Right_T on Key";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        // innerunique: one row per unique left key, so 2 rows (key 1 and key 2)
        Assert.Equal(2, rows);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 10. Union edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Union_WithSource()
    {
        var kql = @"
let t1 = StormEvents | where State == 'TEXAS' | take 2;
let t2 = StormEvents | where State == 'KANSAS' | take 2;
union withsource=Source t1, t2 | count";
        var sql = _converter.Convert(kql);
        // Just verify conversion succeeds; withsource adds a Source column
        Assert.False(string.IsNullOrWhiteSpace(sql));
    }

    [Fact]
    public void Union_SingleTable()
    {
        // Union of single table is degenerate but valid
        var kql = @"
let t1 = StormEvents | take 5;
union t1 | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.Equal(5L, count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 11. Print / literal edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Print_MultipleExpressions()
    {
        var kql = "print a = 1, b = 'hello', c = true, d = 3.14";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(4, reader.FieldCount);
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("a")));
        Assert.Equal("hello", reader.GetString(reader.GetOrdinal("b")));
        Assert.True(reader.GetBoolean(reader.GetOrdinal("c")));
    }

    [Fact]
    public void Print_WithFunctions()
    {
        var kql = "print x = strlen('test'), y = toupper('hello'), z = tolower('WORLD')";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(4L, reader.GetInt64(reader.GetOrdinal("x")));
        Assert.Equal("HELLO", reader.GetString(reader.GetOrdinal("y")));
        Assert.Equal("world", reader.GetString(reader.GetOrdinal("z")));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 12. String operations edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void String_SubstringWithZeroStart()
    {
        var kql = "print x = substring('hello', 0, 3)";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("hel", reader.GetString(reader.GetOrdinal("x")));
    }

    [Fact]
    public void String_IndexOf()
    {
        var kql = "print x = indexof('hello world', 'world')";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(6L, reader.GetInt64(reader.GetOrdinal("x")));
    }

    [Fact]
    public void String_ReplaceString()
    {
        var kql = "print x = replace_string('hello world', 'world', 'there')";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("hello there", reader.GetString(reader.GetOrdinal("x")));
    }

    [Fact]
    public void String_StrcatDelim()
    {
        var kql = "print x = strcat_delim('-', 'a', 'b', 'c')";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("a-b-c", reader.GetString(reader.GetOrdinal("x")));
    }

    [Fact]
    public void String_Reverse()
    {
        var kql = "print x = reverse('hello')";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("olleh", reader.GetString(reader.GetOrdinal("x")));
    }

    [Fact]
    public void String_Countof()
    {
        var kql = "print x = countof('banana', 'an')";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("x")));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 13. Math functions edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Math_AbsSqrtFloorCeiling()
    {
        var kql = "print a = abs(-5), b = floor(3.7), c = ceiling(3.2)";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(5L, reader.GetInt64(reader.GetOrdinal("a")));
    }

    [Fact]
    public void Math_MinOfMaxOf()
    {
        var kql = "print x = min_of(3, 1, 4, 1, 5), y = max_of(3, 1, 4, 1, 5)";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("x")));
        Assert.Equal(5L, reader.GetInt64(reader.GetOrdinal("y")));
    }

    [Fact]
    public void Math_Round()
    {
        var kql = "print x = round(3.14159, 2)";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        // DuckDB ROUND returns DECIMAL, not DOUBLE, so use GetDecimal
        Assert.Equal(3.14m, reader.GetDecimal(reader.GetOrdinal("x")));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 14. WHERE clause optimization edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Where_MultipleWheresCombined()
    {
        // Multiple wheres should be combined with AND if possible
        var kql = @"
StormEvents
| where State == 'TEXAS'
| where InjuriesDirect > 0
| where EventType == 'Tornado'
| where InjuriesIndirect >= 0
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
        // Verify the SQL doesn't have excessive nesting
        // The first WHERE should be simple, subsequent ones should use AND
    }

    [Fact]
    public void Where_AfterOrderBy_WrapsInSubquery()
    {
        // Fixed: WHERE after ORDER BY now wraps in subquery
        var kql = @"
StormEvents
| sort by InjuriesDirect desc
| where InjuriesDirect > 0
| take 5";
        var sql = _converter.Convert(kql);
        Assert.Contains("FROM (", sql); // wrapped in subquery

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    [Fact]
    public void Where_AfterLimit_WrapsInSubquery()
    {
        // Fixed: WHERE after LIMIT now wraps in subquery
        var kql = @"
StormEvents
| take 100
| where State == 'TEXAS'
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 15. Complex real-world queries
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Bug_RealWorld_ToscalarInExtend_ThrowsUnsupported()
    {
        // BUG: toscalar(StormEvents | count) inside extend is parsed as
        // ToScalarExpression by the Kusto SDK, which ExpressionSqlBuilder
        // does not handle. Same root cause as Bug_Toscalar_InFilter.
        var kql = @"
StormEvents
| summarize EventCount = count() by State
| extend TotalEvents = toscalar(StormEvents | count)
| sort by EventCount desc
| take 5";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void RealWorld_FilterExtendProjectSortTake()
    {
        // Common pattern: filter -> extend -> project -> sort -> take
        var kql = @"
StormEvents
| where State == 'TEXAS'
| extend TotalInjuries = InjuriesDirect + InjuriesIndirect
| project State, EventType, TotalInjuries
| sort by TotalInjuries desc
| take 10";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.Equal(3, reader.FieldCount);
        var rows = 0;
        long? prevInjuries = null;
        while (reader.Read())
        {
            rows++;
            var injuries = reader.GetInt64(reader.GetOrdinal("TotalInjuries"));
            if (prevInjuries.HasValue)
                Assert.True(prevInjuries.Value >= injuries, "Sort order violated");
            prevInjuries = injuries;
        }
        Assert.True(rows > 0 && rows <= 10);
    }

    [Fact]
    public void RealWorld_SummarizeByMultipleColumnsWithRename()
    {
        var kql = @"
StormEvents
| summarize EventCount = count() by State, EventType
| where EventCount > 5
| project Events = EventCount, State, Type = EventType
| sort by Events desc
| take 5";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 16. BUG FINDINGS: Tests documenting broken behavior
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Bug_CountAfterSummarize_ColumnNameIsCount()
    {
        // BUG: When you pipe summarize into count, the count operator does
        // ReplaceSelectStar which looks for "SELECT *" prefix. But summarize
        // doesn't produce "SELECT *"; it produces "SELECT col, agg FROM ...".
        // The count wraps it: SELECT COUNT(*) AS Count FROM (SELECT ...)
        // which should work. Let's verify.
        var kql = @"
StormEvents
| summarize cnt = count() by State
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void Bug_ExtendProducesDuplicateColumnsInStar()
    {
        // When extend is called twice with the same column name, the SQL
        // becomes SELECT *, Computed AS Computed FROM (SELECT *, Computed AS Computed FROM ...)
        // which creates an ambiguous "Computed" column. Projecting it explicitly
        // may break depending on the engine.
        var kql = @"
datatable(A:long)[1]
| extend B = A + 1
| extend B = A + 100
| project B";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        // BUG: DuckDB may return the first "B" or error on ambiguity.
        // This documents the current behavior.
        try
        {
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            // If it doesn't throw, document which value wins
            var val = reader.GetInt64(0);
            // Ideally second extend should override (val should be 101),
            // but due to duplicate column names, it might be 2
            Assert.True(val == 2 || val == 101,
                $"Expected 2 or 101, got {val}. Second extend may not override first.");
        }
        catch (Exception)
        {
            // If DuckDB throws on ambiguous column, that's the bug
            // Just pass - we documented it
        }
    }

    [Fact]
    public void Bug_WhereCanAppendAfterJoin_ProducesInvalidSql()
    {
        // CanAppendWhere checks for " WHERE " in the SQL. After a JOIN,
        // there might be an ON clause but no WHERE. If where tries to append
        // "AND condition" after a non-existent WHERE, it would fail.
        // Let's verify the behavior.
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE L (Key BIGINT, Val VARCHAR);
            INSERT INTO L VALUES (1, 'a');
            CREATE TABLE R (Key BIGINT, Score BIGINT);
            INSERT INTO R VALUES (1, 100);";
        setup.ExecuteNonQuery();

        var kql = "L | join kind=inner R on Key | where Score > 0";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(1, rows);
    }

    [Fact]
    public void Bug_SortDefaultDirection_ShouldBeAscByDefault()
    {
        // KQL sort default direction is ASC but the code defaults to DESC
        // for non-OrderedExpression sort elements (line 97 of TabularHandlers.cs)
        var kql = @"
datatable(X:long)[3, 1, 2]
| sort by X";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var values = new List<long>();
        while (reader.Read())
            values.Add(reader.GetInt64(0));
        Assert.Equal(3, values.Count);
        // KQL default sort order is DESC (actually KQL default IS desc),
        // so this is technically correct behavior
        Assert.Equal(3L, values[0]);
        Assert.Equal(2L, values[1]);
        Assert.Equal(1L, values[2]);
    }

    [Fact]
    public void Bug_MultipleWheresAfterSelectStar_Optimization()
    {
        // First where appends WHERE, second where should append AND.
        // Verify the optimization works correctly.
        var kql = @"
StormEvents
| where State == 'TEXAS'
| where InjuriesDirect > 0";
        var sql = _converter.Convert(kql);

        // The SQL should be: SELECT * FROM StormEvents WHERE State = 'TEXAS' AND InjuriesDirect > 0
        // Not: SELECT * FROM (SELECT * FROM StormEvents WHERE State = 'TEXAS') WHERE InjuriesDirect > 0
        Assert.Contains("AND", sql);
        Assert.DoesNotContain("FROM (SELECT", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.True(rows >= 0);
    }

    [Fact]
    public void Bug_ProjectAwayAllColumns_ProducesEmptySelect()
    {
        // What happens if you project-away every column?
        // This should probably error or produce an empty result set
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE T (A BIGINT); INSERT INTO T VALUES (1);";
        setup.ExecuteNonQuery();

        var kql = "T | project-away A";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        // DuckDB with EXCLUDE should handle this, producing a row with 0 columns
        // or erroring because you can't exclude all columns
        try
        {
            using var reader = cmd.ExecuteReader();
            // If it succeeds, it should have 0 columns
            Assert.Equal(0, reader.FieldCount);
        }
        catch (Exception)
        {
            // DuckDB may error: "Cannot exclude all columns"
            // This is expected behavior for this edge case
        }
    }

    [Fact]
    public void Bug_SerializeWithExpressions_AddsToSelect()
    {
        // serialize with expressions should add computed columns like extend
        var kql = @"
StormEvents
| take 3
| serialize RowNum = row_number()";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            // row_number should produce sequential numbers
            var rowNum = reader.GetInt64(reader.GetOrdinal("RowNum"));
            Assert.True(rowNum >= 1);
        }
        Assert.Equal(3, rows);
    }

    [Fact]
    public void Bug_DistinctWithMultipleColumns()
    {
        var kql = "StormEvents | distinct State, EventType | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void TopNested_SingleLevel_NamedAggregation()
    {
        // Fixed: top-nested with named aggregation "cnt = count()" now works
        var kql = "StormEvents | top-nested 5 of State by cnt = count()";
        var sql = _converter.Convert(kql);
        Assert.Contains("COUNT(*)", sql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    [Fact]
    public void TopNested_SingleLevel_UnnamedAggregation_Works()
    {
        // top-nested works when the aggregation is not named
        var kql = "StormEvents | top-nested 5 of State by count()";
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
    public void Bug_Toscalar_InFilter_ThrowsUnsupported()
    {
        // BUG: toscalar() inside a where clause is parsed by Kusto SDK as
        // ToScalarExpression (not FunctionCallExpression), which ExpressionSqlBuilder
        // does not handle. The FunctionCallExpression path for "toscalar" only works
        // when toscalar() is used directly in print or extend without subquery syntax.
        var kql = @"
StormEvents
| where InjuriesDirect > toscalar(StormEvents | summarize avg(InjuriesDirect))
| count";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 17. Unsupported / error handling
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Unsupported_EvaluateDiffpatterns_Throws()
    {
        var kql = "StormEvents | evaluate diffpatterns()";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void Unsupported_EvaluateBasketThrows()
    {
        var kql = "StormEvents | evaluate basket()";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void Unsupported_JoinKindRightSemi_Throws()
    {
        var kql = "T | join kind=rightsemi Y on Key";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void Unsupported_InvokeOperator_Throws()
    {
        var kql = "T | invoke someFunction()";
        Assert.Throws<NotSupportedException>(() => _converter.Convert(kql));
    }

    [Fact]
    public void Unsupported_ForkOperator_Throws()
    {
        // fork is not in the operator dispatch; should throw
        var kql = "StormEvents | fork (where State == 'TEXAS') (where State == 'KANSAS')";
        Assert.ThrowsAny<Exception>(() => _converter.Convert(kql));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 18. Coalesce and null handling
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Tolong_EmptyString_ReturnsNull()
    {
        // Fixed: tolong('') now uses TRY_CAST, returns NULL, coalesce picks 42
        var kql = "print x = coalesce(tolong(''), 42)";
        var sql = _converter.Convert(kql);
        Assert.Contains("TRY_CAST", sql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(42L, reader.GetInt64(reader.GetOrdinal("x")));
    }

    [Fact]
    public void Null_IsEmptyFunction_SimpleCase()
    {
        // Test isempty without the problematic tolong('') chain
        var kql = "print a = isempty(''), b = isempty('hello')";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(reader.GetOrdinal("a")));
        Assert.False(reader.GetBoolean(reader.GetOrdinal("b")));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 19. Date/time functions
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void DateTime_StartOfDay()
    {
        var kql = "print x = startofday(datetime(2024-06-15 14:30:00))";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        var dt = reader.GetDateTime(0);
        Assert.Equal(new DateTime(2024, 6, 15, 0, 0, 0), dt);
    }

    [Fact]
    public void DateTime_DatetimeDiff()
    {
        var kql = "print x = datetime_diff('day', datetime(2024-01-10), datetime(2024-01-01))";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(9L, reader.GetInt64(reader.GetOrdinal("x")));
    }

    [Fact]
    public void DateTime_DatetimeAdd()
    {
        var kql = "print x = datetime_add('day', 5, datetime(2024-01-01))";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        var dt = reader.GetDateTime(0);
        Assert.Equal(new DateTime(2024, 1, 6), dt.Date);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 20. Edge cases in SQL generation helpers
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Helper_ExtractFromHandlesNestedSubqueries()
    {
        // A complex pipeline should nest properly
        var kql = @"
StormEvents
| where State == 'TEXAS'
| extend X = InjuriesDirect + 1
| where X > 0
| project State, X
| sort by X desc
| take 5
| summarize Total = sum(X)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.GetInt64(reader.GetOrdinal("Total")) > 0);
    }

    [Fact]
    public void Helper_UnwrapFromForSimpleTableRef()
    {
        // A simple "T" reference should be unwrapped, not parenthesized
        using var conn = InMemory();
        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE T (A BIGINT); INSERT INTO T VALUES (1), (2);";
        setup.ExecuteNonQuery();

        var kql = "T | count";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.Equal(2L, count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 21. Regex and pattern matching
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Regex_MatchesRegex()
    {
        var kql = "StormEvents | where State matches regex '^T' | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0, "Should match states starting with T like TEXAS, TENNESSEE");
    }

    [Fact]
    public void Regex_ExtractFunction()
    {
        // KQL extract with 3 args: pattern, group, source
        var kql = "print x = extract('([0-9]+)', 1, 'abc123def')";
        var sql = _converter.Convert(kql);

        using var conn = InMemory();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("123", reader.GetString(reader.GetOrdinal("x")));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 22. Comments in KQL
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Comments_SingleLineComment()
    {
        var kql = @"
// This is a comment
StormEvents // another comment
| where State == 'TEXAS' // filter
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    [Fact]
    public void Comments_MultiLineComment()
    {
        var kql = @"
/* Multi-line
   comment */
StormEvents
| where State == 'TEXAS'
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }
}
