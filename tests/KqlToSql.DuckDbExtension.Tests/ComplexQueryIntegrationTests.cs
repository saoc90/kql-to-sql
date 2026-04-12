using System;
using System.Collections.Generic;
using System.Linq;
using DuckDB.NET.Data;
using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.DuckDbExtension.Tests;

/// <summary>
/// Complex multi-operator integration tests that exercise chained pipelines,
/// joins of various kinds, CTEs, aggregations, and new operators against DuckDB.
/// </summary>
public class ComplexQueryIntegrationTests
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    // ── Multi-operator pipelines ──────────────────────────────────────────

    [Fact]
    public void Pipeline_WhereExtendSummarizeSortTake()
    {
        var kql = @"
StormEvents
| where State == 'TEXAS'
| extend TotalInjuries = InjuriesDirect + InjuriesIndirect
| summarize TotalInjuries = sum(TotalInjuries), EventCount = count() by EventType
| sort by TotalInjuries desc
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
            Assert.False(string.IsNullOrWhiteSpace(reader.GetString(reader.GetOrdinal("EventType"))));
            Assert.True(reader.GetInt64(reader.GetOrdinal("EventCount")) > 0);
        }
        Assert.Equal(3, rows);
    }

    [Fact]
    public void Pipeline_WhereProjectDistinctCount()
    {
        var kql = @"
StormEvents
| where InjuriesDirect > 0
| project State, EventType
| distinct State
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 1);
    }

    [Fact]
    public void Pipeline_TopWithExtend()
    {
        var kql = @"
StormEvents
| extend TotalInjuries = InjuriesDirect + InjuriesIndirect
| top 5 by TotalInjuries desc";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        long prev = long.MaxValue;
        while (reader.Read())
        {
            rows++;
            var val = reader.GetInt64(reader.GetOrdinal("TotalInjuries"));
            Assert.True(val <= prev, "Results should be sorted descending");
            prev = val;
        }
        Assert.Equal(5, rows);
    }

    // ── Let statements / CTEs ─────────────────────────────────────────────

    [Fact]
    public void CTE_LetWithMultipleReferences()
    {
        var kql = @"
let topStates = StormEvents | summarize cnt = count() by State | top 3 by cnt desc;
topStates | project State, cnt";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var states = new List<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(reader.GetOrdinal("State")));
        }
        Assert.Equal(3, states.Count);
        Assert.All(states, s => Assert.False(string.IsNullOrWhiteSpace(s)));
    }

    [Fact]
    public void CTE_MaterializedLetUsedTwice()
    {
        var kql = @"
let injured = materialize(StormEvents | where InjuriesDirect > 0);
let stateCount = injured | summarize cnt = count() by State | sort by cnt desc | take 3;
stateCount";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.True(rows > 0 && rows <= 3);
    }

    // ── Join kinds ────────────────────────────────────────────────────────

    [Fact]
    public void Join_InnerJoin_WithCTEs()
    {
        var kql = @"
let stateInjuries = StormEvents | summarize TotalInjuries = sum(InjuriesDirect) by State;
let stateCount = StormEvents | summarize EventCount = count() by State;
stateInjuries | join kind=inner stateCount on State | sort by TotalInjuries desc | take 5";
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
        }
        Assert.Equal(5, rows);
    }

    [Fact]
    public void Join_LeftOuterJoin_InMemory()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        SetupJoinTables(conn);

        var kql = "Orders | join kind=leftouter Customers on CustomerId";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var results = new List<(long OrderId, long CustomerId, string? Name)>();
        while (reader.Read())
        {
            results.Add((
                reader.GetInt64(reader.GetOrdinal("OrderId")),
                reader.GetInt64(reader.GetOrdinal("CustomerId")),
                reader.IsDBNull(reader.GetOrdinal("Name")) ? null : reader.GetString(reader.GetOrdinal("Name"))
            ));
        }

        Assert.Equal(4, results.Count);
        Assert.Contains(results, r => r.OrderId == 1 && r.Name == "Alice");
        Assert.Contains(results, r => r.OrderId == 2 && r.Name == "Bob");
        Assert.Contains(results, r => r.OrderId == 4 && r.Name == null); // no matching customer
    }

    [Fact]
    public void Join_RightOuterJoin_InMemory()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        SetupJoinTables(conn);

        var kql = "Orders | join kind=rightouter Customers on CustomerId";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var results = new List<(long? OrderId, string Name)>();
        while (reader.Read())
        {
            results.Add((
                reader.IsDBNull(reader.GetOrdinal("OrderId")) ? null : reader.GetInt64(reader.GetOrdinal("OrderId")),
                reader.GetString(reader.GetOrdinal("Name"))
            ));
        }

        Assert.Contains(results, r => r.Name == "Alice" && r.OrderId == 1);
        Assert.Contains(results, r => r.Name == "Charlie" && r.OrderId == null); // no matching order
    }

    [Fact]
    public void Join_FullOuterJoin_InMemory()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        SetupJoinTables(conn);

        var kql = "Orders | join kind=fullouter Customers on CustomerId";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        bool hasUnmatchedLeft = false;
        bool hasUnmatchedRight = false;
        while (reader.Read())
        {
            rows++;
            if (reader.IsDBNull(reader.GetOrdinal("Name")))
                hasUnmatchedLeft = true;
            if (reader.IsDBNull(reader.GetOrdinal("OrderId")))
                hasUnmatchedRight = true;
        }

        Assert.True(rows >= 4);
        Assert.True(hasUnmatchedLeft, "Should have unmatched left rows (order 4 has no customer)");
        Assert.True(hasUnmatchedRight, "Should have unmatched right rows (Charlie has no order)");
    }

    [Fact]
    public void Join_InnerUnique_DeduplicatesLeft()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE A (Key VARCHAR, Val1 BIGINT);
            INSERT INTO A VALUES ('x',1),('x',2),('y',3);
            CREATE TABLE B (Key VARCHAR, Val2 BIGINT);
            INSERT INTO B VALUES ('x',10),('y',20);";
        setup.ExecuteNonQuery();

        var kql = "A | join B on Key";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var results = new List<(string Key, long Val1, long Val2)>();
        while (reader.Read())
        {
            results.Add((
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2)
            ));
        }

        // innerunique: only one row from left per key
        Assert.Equal(2, results.Count);
        Assert.Single(results.Where(r => r.Key == "x"));
        Assert.Single(results.Where(r => r.Key == "y"));
    }

    // ── Lookup operator ───────────────────────────────────────────────────

    [Fact]
    public void Lookup_LeftOuter_InMemory()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        SetupJoinTables(conn);

        var kql = "Orders | lookup Customers on CustomerId";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var results = new List<(long OrderId, string? Name)>();
        while (reader.Read())
        {
            results.Add((
                reader.GetInt64(reader.GetOrdinal("OrderId")),
                reader.IsDBNull(reader.GetOrdinal("Name")) ? null : reader.GetString(reader.GetOrdinal("Name"))
            ));
        }

        Assert.Equal(4, results.Count);
        Assert.Contains(results, r => r.OrderId == 4 && r.Name == null);
        Assert.Contains(results, r => r.OrderId == 1 && r.Name == "Alice");
    }

    [Fact]
    public void Lookup_Inner_InMemory()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        SetupJoinTables(conn);

        var kql = "Orders | lookup kind=inner Customers on CustomerId";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;

        Assert.Equal(3, rows); // order 4 (CustomerId=99) has no match
    }

    // ── Sample operator ───────────────────────────────────────────────────

    [Fact]
    public void Sample_ReturnsRequestedRows()
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
    public void SampleDistinct_ReturnsUniqueValues()
    {
        var kql = "StormEvents | sample-distinct 5 of State";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var states = new HashSet<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(0));
        }
        Assert.Equal(5, states.Count);
    }

    // ── Serialize operator ────────────────────────────────────────────────

    [Fact]
    public void Serialize_WithRowNumber_ProducesSequentialNumbers()
    {
        var kql = "StormEvents | take 5 | serialize rn = row_number()";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rowNumbers = new List<long>();
        while (reader.Read())
        {
            rowNumbers.Add(reader.GetInt64(reader.GetOrdinal("rn")));
        }
        Assert.Equal(5, rowNumbers.Count);
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, rowNumbers.ToArray());
    }

    // ── Matches regex ─────────────────────────────────────────────────────

    [Fact]
    public void MatchesRegex_FiltersCorrectly()
    {
        var kql = "StormEvents | where EventType matches regex '^Tornado' | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0, "Should find events matching '^Tornado'");
    }

    [Fact]
    public void MatchesRegex_WithComplexPattern()
    {
        var kql = @"StormEvents | where EventType matches regex 'Wind|Tornado' | distinct EventType | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 2, "Should find multiple event types matching pattern");
    }

    // ── Parse operator ────────────────────────────────────────────────────

    [Fact]
    public void Parse_ExtractsColumnsFromDataTable()
    {
        var kql = @"
datatable(Text: string)['key=abc,val=123', 'key=def,val=456']
| parse Text with 'key=' key:string ',val=' val:long";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var results = new List<(string Key, long Val)>();
        while (reader.Read())
        {
            results.Add((
                reader.GetString(reader.GetOrdinal("key")),
                reader.GetInt64(reader.GetOrdinal("val"))
            ));
        }
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Key == "abc" && r.Val == 123);
        Assert.Contains(results, r => r.Key == "def" && r.Val == 456);
    }

    // ── Union operator ────────────────────────────────────────────────────

    [Fact]
    public void Union_WithPipedFilter()
    {
        var kql = @"
let texas = StormEvents | where State == 'TEXAS' | take 3;
let kansas = StormEvents | where State == 'KANSAS' | take 3;
union texas, kansas | summarize cnt = count() by State";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var results = new Dictionary<string, long>();
        while (reader.Read())
        {
            results[reader.GetString(0)] = reader.GetInt64(1);
        }
        Assert.Equal(2, results.Count);
        Assert.Equal(3L, results["TEXAS"]);
        Assert.Equal(3L, results["KANSAS"]);
    }

    // ── Render operator (no-op) ───────────────────────────────────────────

    [Fact]
    public void Render_PassesThrough()
    {
        var kql = "StormEvents | summarize cnt = count() by State | sort by cnt desc | take 5 | render barchart";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    // ── As operator (pass-through) ────────────────────────────────────────

    [Fact]
    public void As_PassesThrough()
    {
        var kql = "StormEvents | where State == 'TEXAS' | as texasEvents | count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count > 0);
    }

    // ── Aggregation variety ───────────────────────────────────────────────

    [Fact]
    public void Summarize_MultipleAggregates()
    {
        var kql = @"
StormEvents
| summarize EventCount = count(), AvgInjuries = avg(InjuriesDirect), MaxInjuries = max(InjuriesDirect) by State
| top 5 by EventCount desc";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.True(reader.GetInt64(reader.GetOrdinal("EventCount")) > 0);
        }
        Assert.Equal(5, rows);
    }

    [Fact]
    public void Summarize_CountIf()
    {
        var kql = @"
StormEvents
| summarize Total = count(), Injured = countif(InjuriesDirect > 0) by State
| where Injured > 0
| sort by Injured desc
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
            Assert.True(reader.GetInt64(reader.GetOrdinal("Injured")) > 0);
        }
        Assert.True(rows > 0 && rows <= 3);
    }

    [Fact]
    public void Summarize_MakeList()
    {
        var kql = @"
StormEvents
| where State == 'TEXAS'
| summarize EventTypes = make_set(EventType)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        // make_set returns a LIST in DuckDB
        var eventTypes = reader.GetValue(reader.GetOrdinal("EventTypes"));
        Assert.NotNull(eventTypes);
    }

    // ── Project variants ──────────────────────────────────────────────────

    [Fact]
    public void ProjectAway_RemovesColumns()
    {
        var kql = "StormEvents | take 1 | project-away Source, BeginLocation";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        for (int i = 0; i < reader.FieldCount; i++)
        {
            Assert.NotEqual("Source", reader.GetName(i));
            Assert.NotEqual("BeginLocation", reader.GetName(i));
        }
    }

    [Fact]
    public void ProjectRename_RenamesColumns()
    {
        var kql = "StormEvents | take 1 | project-rename StateName = State";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        var columns = Enumerable.Range(0, reader.FieldCount).Select(i => reader.GetName(i)).ToList();
        Assert.Contains("StateName", columns);
    }

    // ── MvExpand ──────────────────────────────────────────────────────────

    [Fact]
    public void MvExpand_UnnestsList()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE T (Id BIGINT, Tags VARCHAR[]); INSERT INTO T VALUES (1, ['a','b','c']), (2, ['x','y']);";
        setup.ExecuteNonQuery();

        var kql = "T | mv-expand Tags";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(5, rows);
    }

    // ── Complex end-to-end pipelines ──────────────────────────────────────

    [Fact]
    public void Complex_FilterExtendSummarizeJoinProject()
    {
        var kql = @"
let injuriesByState = StormEvents
    | extend TotalInjuries = InjuriesDirect + InjuriesIndirect
    | summarize TotalInjuries = sum(TotalInjuries), EventCount = count() by State;
let topStates = injuriesByState | top 5 by TotalInjuries desc;
topStates
| project State, TotalInjuries, EventCount";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.Equal(3, reader.FieldCount);
        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.False(string.IsNullOrWhiteSpace(reader.GetString(reader.GetOrdinal("State"))));
        }
        Assert.Equal(5, rows);
    }

    [Fact]
    public void Complex_MultiLetSummarizeJoin()
    {
        var kql = @"
let byType = StormEvents | summarize TypeCount = count() by EventType;
let byState = StormEvents | summarize StateCount = count() by State;
byType
| top 3 by TypeCount desc
| project EventType, TypeCount";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
            Assert.True(reader.GetInt64(reader.GetOrdinal("TypeCount")) > 0);
        }
        Assert.Equal(3, rows);
    }

    [Fact]
    public void Complex_NestedExtendWithFunctions()
    {
        var kql = @"
StormEvents
| where State == 'TEXAS'
| extend UpperType = toupper(EventType)
| extend InjuryRatio = iif(InjuriesDirect > 0, InjuriesDirect * 1.0 / (InjuriesDirect + InjuriesIndirect + 1), 0.0)
| project State, UpperType, InjuryRatio
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
            Assert.Equal("TEXAS", reader.GetString(reader.GetOrdinal("State")));
            var upperType = reader.GetString(reader.GetOrdinal("UpperType"));
            Assert.Equal(upperType, upperType.ToUpperInvariant());
        }
        Assert.Equal(5, rows);
    }

    [Fact]
    public void Complex_DataTableWithUnion()
    {
        var kql = @"
let extra = datatable(State: string, EventType: string, InjuriesDirect: long)
    ['TESTSTATE', 'TestEvent', 999];
StormEvents
| project State, EventType, InjuriesDirect
| take 2
| union extra
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.Equal(3L, count);
    }

    [Fact]
    public void Complex_PrintWithArithmetic()
    {
        var kql = "print result = 2 + 3 * 4, greeting = 'hello'";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(14L, reader.GetInt64(reader.GetOrdinal("result")));
        Assert.Equal("hello", reader.GetString(reader.GetOrdinal("greeting")));
    }

    [Fact]
    public void Complex_RangeWithSummarize()
    {
        var kql = "range x from 1 to 10 step 1 | summarize total = sum(x)";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar()!;
        var total = long.Parse(result.ToString()!); // DuckDB returns BigInteger for HUGEINT sums
        Assert.Equal(55L, total); // 1+2+...+10
    }

    [Fact]
    public void Complex_BetweenWithContains()
    {
        var kql = @"
StormEvents
| where InjuriesDirect between (1 .. 50)
| where EventType contains 'Tornado'
| count";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var count = (long)cmd.ExecuteScalar()!;
        Assert.True(count >= 0);
    }

    [Fact]
    public void Complex_InOperatorWithSummarize()
    {
        var kql = @"
StormEvents
| where State in ('TEXAS', 'KANSAS', 'NEBRASKA')
| summarize cnt = count() by State
| sort by cnt desc";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var states = new List<string>();
        while (reader.Read())
        {
            states.Add(reader.GetString(0));
        }
        Assert.True(states.Count <= 3);
        Assert.All(states, s => Assert.Contains(s, new[] { "TEXAS", "KANSAS", "NEBRASKA" }));
    }

    // ── Arg_max / arg_min ─────────────────────────────────────────────────

    [Fact]
    public void ArgMax_WithStar()
    {
        var kql = "StormEvents | summarize arg_max(InjuriesDirect, *) by State | sort by InjuriesDirect desc | take 3";
        var sql = _converter.Convert(kql);

        using var conn = StormEventsDatabase.GetConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows);
    }

    // ── Helper methods ────────────────────────────────────────────────────

    private static void SetupJoinTables(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE Customers (CustomerId BIGINT, Name VARCHAR);
            INSERT INTO Customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
            CREATE TABLE Orders (OrderId BIGINT, CustomerId BIGINT, Amount DOUBLE);
            INSERT INTO Orders VALUES (1, 1, 100.0), (2, 2, 200.0), (3, 2, 150.0), (4, 99, 50.0);";
        cmd.ExecuteNonQuery();
    }
}
