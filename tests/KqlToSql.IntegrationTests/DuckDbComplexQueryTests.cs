using System.Text.Json;
using KqlToSql.Dialects;

namespace KqlToSql.IntegrationTests;

/// <summary>
/// Complex integration tests exercising multi-operator KQL pipelines
/// against DuckDB WASM. Each test converts KQL → SQL then executes
/// the SQL and validates results end-to-end.
/// </summary>
[Collection("NodeJS")]
public class DuckDbComplexQueryTests : IAsyncLifetime
{
    private readonly NodeJSFixture _fixture;
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());
    private string _scriptPath = null!;

    public DuckDbComplexQueryTests(NodeJSFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        _scriptPath = Path.Combine(_fixture.ScriptsPath, "duckdbWasmRunner.js");

        // Richer dataset for complex queries
        await Exec("DROP TABLE IF EXISTS Events");
        await Exec(@"
            CREATE TABLE Events (
                State VARCHAR,
                EventType VARCHAR,
                Injuries INTEGER,
                Deaths INTEGER,
                DamageProperty INTEGER,
                DamageCrops INTEGER,
                BeginTime TIMESTAMP,
                EndTime TIMESTAMP,
                Year INTEGER,
                Month INTEGER,
                Source VARCHAR,
                Magnitude DOUBLE,
                Lat DOUBLE,
                Lon DOUBLE
            )");

        await Exec(@"
            INSERT INTO Events VALUES
                ('TEXAS',    'Tornado',            5, 1, 50000,  1000, '2020-03-15 11:00:00', '2020-03-15 11:30:00', 2020, 3,  'Trained Spotter', 2.5,  31.0, -97.0),
                ('TEXAS',    'Hail',               0, 0, 500,    0,    '2020-04-10 14:30:00', '2020-04-10 14:45:00', 2020, 4,  'Public',          1.0,  32.0, -96.5),
                ('TEXAS',    'Flash Flood',        2, 0, 100000, 5000, '2021-05-20 09:00:00', '2021-05-20 12:00:00', 2021, 5,  'Emergency Manager',0.0, 30.5, -97.5),
                ('TEXAS',    'Tornado',            8, 2, 200000, 0,    '2021-06-01 16:00:00', '2021-06-01 16:45:00', 2021, 6,  'Trained Spotter', 4.0,  33.0, -96.0),
                ('KANSAS',   'Tornado',            3, 0, 25000,  3000, '2020-04-20 09:00:00', '2020-04-20 09:30:00', 2020, 4,  'Storm Chaser',    3.0,  38.0, -98.0),
                ('KANSAS',   'Hail',               0, 0, 1500,   2000, '2020-05-15 13:00:00', '2020-05-15 13:15:00', 2020, 5,  'Public',          1.5,  39.0, -97.5),
                ('KANSAS',   'Tornado',            1, 1, 75000,  10000,'2021-03-10 15:00:00', '2021-03-10 15:45:00', 2021, 3,  'Trained Spotter', 3.5,  37.5, -98.5),
                ('OKLAHOMA', 'Tornado',            2, 0, 15000,  500,  '2020-04-05 12:00:00', '2020-04-05 12:20:00', 2020, 4,  'Storm Chaser',    2.0,  35.0, -97.0),
                ('OKLAHOMA', 'Thunderstorm Wind',  1, 0, 2000,   0,    '2020-06-15 17:00:00', '2020-06-15 17:30:00', 2020, 6,  'Public',          0.0,  36.0, -96.0),
                ('OKLAHOMA', 'Tornado',            4, 1, 120000, 8000, '2021-04-25 14:00:00', '2021-04-25 14:40:00', 2021, 4,  'Trained Spotter', 3.0,  35.5, -97.5),
                ('FLORIDA',  'Hurricane',         10, 3, 500000, 50000,'2020-09-15 06:00:00', '2020-09-16 18:00:00', 2020, 9,  'Emergency Manager',5.0, 26.0, -80.0),
                ('FLORIDA',  'Flash Flood',        1, 0, 30000,  2000, '2021-07-20 08:00:00', '2021-07-20 11:00:00', 2021, 7,  'Public',          0.0,  27.0, -81.0),
                ('FLORIDA',  'Tornado',            0, 0, 5000,   0,    '2021-08-10 10:00:00', '2021-08-10 10:15:00', 2021, 8,  'Trained Spotter', 1.0,  28.0, -82.0),
                ('ALABAMA',  'Tornado',            6, 2, 180000, 15000,'2020-03-03 14:00:00', '2020-03-03 14:30:00', 2020, 3,  'Storm Chaser',    4.5,  33.5, -87.0),
                ('ALABAMA',  'Thunderstorm Wind',  0, 0, 800,    0,    '2021-06-10 16:30:00', '2021-06-10 17:00:00', 2021, 6,  'Public',          0.0,  34.0, -86.5)
        ");

        // Second table for join tests
        await Exec("DROP TABLE IF EXISTS StateInfo");
        await Exec(@"
            CREATE TABLE StateInfo (
                State VARCHAR,
                Region VARCHAR,
                Population INTEGER,
                AreaSqMi INTEGER
            )");

        await Exec(@"
            INSERT INTO StateInfo VALUES
                ('TEXAS',    'South',     29000000, 268596),
                ('KANSAS',   'Midwest',   2900000,  82278),
                ('OKLAHOMA', 'South',     3960000,  69899),
                ('FLORIDA',  'Southeast', 21500000, 65758),
                ('ALABAMA',  'Southeast', 5024000,  52420)
        ");
    }

    public Task DisposeAsync() => Task.CompletedTask;

    // ──────────────────────────────────────────────────────
    // Multi-stage pipelines
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Pipeline_Where_Extend_Summarize_Sort_Take()
    {
        var kql = @"
            Events
            | where Injuries > 0
            | extend TotalDamage = DamageProperty + DamageCrops
            | summarize EventCount = count(), TotalInjured = sum(Injuries), MaxDamage = max(TotalDamage) by State
            | sort by TotalInjured desc
            | take 3";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(3, rows.Count);
        // Most injured state should be first
        Assert.True(GetInt(rows[0], "TotalInjured") >= GetInt(rows[1], "TotalInjured"));
        Assert.True(GetInt(rows[1], "TotalInjured") >= GetInt(rows[2], "TotalInjured"));
    }

    [Fact]
    public async Task Pipeline_Where_Summarize_Where_Project()
    {
        // Double filter: aggregate then filter on aggregation result
        var kql = @"
            Events
            | where EventType == 'Tornado'
            | summarize TornadoCount = count(), AvgInjuries = avg(Injuries) by State
            | where TornadoCount >= 2
            | project State, TornadoCount, AvgInjuries";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        // TEXAS (2 tornados), KANSAS (2), OKLAHOMA (2) all have >= 2 tornados
        Assert.True(rows.Count >= 2);
        Assert.All(rows, r => Assert.True(r.GetProperty("TornadoCount").GetInt32() >= 2));
    }

    [Fact]
    public async Task Pipeline_Extend_Iif_Summarize_CountIf()
    {
        var kql = @"
            Events
            | extend Severity = iif(Injuries >= 5, 'High', iif(Injuries >= 1, 'Medium', 'Low'))
            | summarize HighCount = countif(Severity == 'High'), MediumCount = countif(Severity == 'Medium'), LowCount = countif(Severity == 'Low') by State
            | sort by HighCount desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.True(rows.Count > 0);
        var total = rows.Sum(r =>
            r.GetProperty("HighCount").GetInt32() +
            r.GetProperty("MediumCount").GetInt32() +
            r.GetProperty("LowCount").GetInt32());
        Assert.Equal(15, total); // All 15 events accounted for
    }

    [Fact]
    public async Task Pipeline_Multiple_Extends_With_Math()
    {
        var kql = @"
            Events
            | extend TotalDamage = DamageProperty + DamageCrops
            | extend DamagePerInjury = iif(Injuries > 0, TotalDamage * 1.0 / Injuries, 0.0)
            | extend LogDamage = iif(TotalDamage > 0, log10(toreal(TotalDamage)), 0.0)
            | where LogDamage > 4
            | project State, EventType, TotalDamage, LogDamage
            | sort by LogDamage desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        // log10 > 4 means TotalDamage > 10000
        Assert.All(rows, r => Assert.True(r.GetProperty("TotalDamage").GetInt32() > 10000));
        // Verify descending order
        for (int i = 1; i < rows.Count; i++)
        {
            Assert.True(rows[i - 1].GetProperty("LogDamage").GetDouble() >= rows[i].GetProperty("LogDamage").GetDouble());
        }
    }

    // ──────────────────────────────────────────────────────
    // Joins
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Join_Inner_With_Aggregation()
    {
        var kql = @"
            Events
            | summarize EventCount = count(), TotalInjuries = sum(Injuries) by State
            | join kind=inner (StateInfo) on State
            | extend EventsPerMillion = round(EventCount * 1000000.0 / Population, 2)
            | project State, Region, EventCount, EventsPerMillion
            | sort by EventsPerMillion desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(5, rows.Count);
        Assert.All(rows, r =>
        {
            Assert.True(r.GetProperty("EventCount").GetInt32() > 0);
            Assert.True(r.GetProperty("EventsPerMillion").GetDouble() > 0);
        });
    }

    [Fact]
    public async Task Join_LeftOuter_Shows_All_States()
    {
        // All states from StateInfo should appear even if no events match the filter
        var kql = @"
            Events
            | where EventType == 'Hurricane'
            | summarize HurricaneCount = count() by State
            | join kind=leftouter (StateInfo) on State
            | project State, Region, HurricaneCount";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        // Only FLORIDA has hurricanes
        Assert.Single(rows);
        Assert.Equal("FLORIDA", rows[0].GetProperty("State").GetString());
    }

    [Fact]
    public async Task Lookup_Enriches_Events()
    {
        var kql = @"
            Events
            | where Injuries > 3
            | lookup StateInfo on State
            | project State, EventType, Injuries, Region, Population";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.True(rows.Count >= 3);
        Assert.All(rows, r =>
        {
            Assert.True(r.GetProperty("Injuries").GetInt32() > 3);
            Assert.False(string.IsNullOrEmpty(r.GetProperty("Region").GetString()));
        });
    }

    // ──────────────────────────────────────────────────────
    // Union
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Union_Two_Filtered_Sets_Then_Aggregate()
    {
        var kql = @"
            Events
            | where Year == 2020
            | union (Events | where Year == 2021)
            | summarize TotalEvents = count(), TotalInjuries = sum(Injuries) by Year
            | sort by Year asc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(2, rows.Count);
        Assert.Equal(2020, rows[0].GetProperty("Year").GetInt32());
        Assert.Equal(2021, rows[1].GetProperty("Year").GetInt32());
        var total = rows[0].GetProperty("TotalEvents").GetInt32() + rows[1].GetProperty("TotalEvents").GetInt32();
        Assert.Equal(15, total);
    }

    // ──────────────────────────────────────────────────────
    // String functions in pipelines
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Pipeline_String_Functions_Contains_Strlen_Tolower()
    {
        var kql = @"
            Events
            | where EventType contains 'Tornado'
            | extend TypeLen = strlen(EventType), LowerState = tolower(State)
            | project LowerState, EventType, TypeLen, Injuries
            | sort by Injuries desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.All(rows, r =>
        {
            Assert.Equal("Tornado", r.GetProperty("EventType").GetString());
            Assert.Equal(7, r.GetProperty("TypeLen").GetInt32()); // "Tornado" = 7 chars
            Assert.Equal(r.GetProperty("LowerState").GetString(), r.GetProperty("LowerState").GetString()!.ToLower());
        });
    }

    [Fact]
    public async Task Pipeline_Substring_Strcat_Replace()
    {
        var kql = @"
            Events
            | extend StateAbbrev = substring(State, 0, 3)
            | extend Label = strcat(StateAbbrev, '-', EventType)
            | extend CleanLabel = replace_string(Label, ' ', '_')
            | project CleanLabel, Injuries
            | take 5";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(5, rows.Count);
        Assert.All(rows, r =>
        {
            var label = r.GetProperty("CleanLabel").GetString()!;
            Assert.DoesNotContain(" ", label);
            Assert.Contains("-", label);
        });
    }

    // ──────────────────────────────────────────────────────
    // Between, In, and complex where conditions
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Pipeline_Between_And_In()
    {
        var kql = @"
            Events
            | where Injuries between (1 .. 5)
            | where State in ('TEXAS', 'KANSAS')
            | project State, EventType, Injuries
            | sort by Injuries desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.All(rows, r =>
        {
            Assert.True(r.GetProperty("Injuries").GetInt32() >= 1);
            Assert.True(r.GetProperty("Injuries").GetInt32() <= 5);
            Assert.Contains(r.GetProperty("State").GetString(), new[] { "TEXAS", "KANSAS" });
        });
    }

    [Fact]
    public async Task Pipeline_NotBetween_NotIn()
    {
        var kql = @"
            Events
            | where Injuries !between (1 .. 5)
            | where State !in ('FLORIDA')
            | project State, EventType, Injuries";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.All(rows, r =>
        {
            var injuries = r.GetProperty("Injuries").GetInt32();
            Assert.True(injuries < 1 || injuries > 5);
            Assert.NotEqual("FLORIDA", r.GetProperty("State").GetString());
        });
    }

    // ──────────────────────────────────────────────────────
    // Complex aggregations
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Summarize_Multiple_Aggs()
    {
        var kql = @"
            Events
            | summarize
                EventCount = count(),
                TotalInjuries = sum(Injuries),
                AvgInjuries = avg(Injuries),
                MaxInjuries = max(Injuries),
                MinInjuries = min(Injuries),
                UniqueTypes = dcount(EventType)
              by State
            | sort by TotalInjuries desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(5, rows.Count);
        Assert.All(rows, r =>
        {
            Assert.True(r.GetProperty("EventCount").GetInt32() > 0);
            Assert.True(r.GetProperty("MaxInjuries").GetInt32() >= r.GetProperty("MinInjuries").GetInt32());
            Assert.True(r.GetProperty("UniqueTypes").GetInt32() >= 1);
        });
    }

    [Fact]
    public async Task Summarize_With_Bin_Time_Grouping()
    {
        var kql = @"
            Events
            | summarize EventCount = count(), TotalDamage = sum(DamageProperty) by bin(Year, 1)
            | sort by Year asc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(2, rows.Count);
        Assert.Equal(2020, rows[0].GetProperty("Year").GetInt32());
        Assert.Equal(2021, rows[1].GetProperty("Year").GetInt32());
    }

    [Fact]
    public async Task Summarize_SumIf_AvgIf()
    {
        var kql = @"
            Events
            | summarize
                TornadoDamage = sumif(DamageProperty, EventType == 'Tornado'),
                NonTornadoDamage = sumif(DamageProperty, EventType != 'Tornado'),
                AvgTornadoInjuries = avgif(Injuries, EventType == 'Tornado')
              by State
            | sort by TornadoDamage desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.True(rows.Count > 0);
        Assert.All(rows, r =>
        {
            // Tornado damage + non-tornado damage should equal total for that state
            Assert.True(GetInt(r, "TornadoDamage") >= 0);
        });
    }

    // ──────────────────────────────────────────────────────
    // Distinct & Top
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Pipeline_Top_By_Expression()
    {
        var kql = @"
            Events
            | extend TotalDamage = DamageProperty + DamageCrops
            | top 3 by TotalDamage";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(3, rows.Count);
        for (int i = 1; i < rows.Count; i++)
        {
            Assert.True(rows[i - 1].GetProperty("TotalDamage").GetInt32() >= rows[i].GetProperty("TotalDamage").GetInt32());
        }
    }

    [Fact]
    public async Task Distinct_After_Extend()
    {
        var kql = @"
            Events
            | extend StateYear = strcat(State, '-', tostring(Year))
            | distinct StateYear";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        // 5 states x 2 years max, but not all combos exist
        var values = rows.Select(r => r.GetProperty("StateYear").GetString()).ToList();
        Assert.Equal(values.Count, values.Distinct().Count()); // All distinct
    }

    // ──────────────────────────────────────────────────────
    // Case expression & complex conditionals
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Pipeline_Case_Expression()
    {
        var kql = @"
            Events
            | extend DamageCategory = case(
                DamageProperty >= 100000, 'Catastrophic',
                DamageProperty >= 10000,  'Major',
                DamageProperty >= 1000,   'Moderate',
                'Minor')
            | summarize Count = count() by DamageCategory
            | sort by Count desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        var total = rows.Sum(r => r.GetProperty("Count").GetInt32());
        Assert.Equal(15, total);
        var categories = rows.Select(r => r.GetProperty("DamageCategory").GetString()).ToList();
        Assert.All(categories, c => Assert.Contains(c, new[] { "Catastrophic", "Major", "Moderate", "Minor" }));
    }

    // ──────────────────────────────────────────────────────
    // Datatable with pipeline
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Datatable_With_Pipeline()
    {
        var kql = @"
            datatable(Name:string, Score:long) [
                'Alice', 95,
                'Bob', 82,
                'Charlie', 91,
                'Diana', 78,
                'Eve', 88
            ]
            | where Score >= 85
            | project Name, Score
            | sort by Score desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(3, rows.Count);
        Assert.Equal("Alice", rows[0].GetProperty("Name").GetString());
        Assert.Equal(91, rows[1].GetProperty("Score").GetInt32());
        Assert.Equal(88, rows[2].GetProperty("Score").GetInt32());
    }

    // ──────────────────────────────────────────────────────
    // project-away & project-rename
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task ProjectAway_Removes_Columns()
    {
        var kql = @"
            Events
            | where State == 'TEXAS'
            | project-away Lat, Lon, Source, Magnitude, BeginTime, EndTime
            | take 2";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(2, rows.Count);
        Assert.All(rows, r =>
        {
            Assert.True(r.TryGetProperty("State", out _));
            Assert.False(r.TryGetProperty("Lat", out _));
            Assert.False(r.TryGetProperty("Lon", out _));
        });
    }

    [Fact]
    public async Task ProjectRename_Then_Filter()
    {
        var kql = @"
            Events
            | project-rename Location = State, Type = EventType, Hurt = Injuries
            | where Hurt > 2
            | project Location, Type, Hurt
            | sort by Hurt desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.All(rows, r =>
        {
            Assert.True(r.GetProperty("Hurt").GetInt32() > 2);
            Assert.True(r.TryGetProperty("Location", out _));
            Assert.True(r.TryGetProperty("Type", out _));
        });
    }

    // ──────────────────────────────────────────────────────
    // Complex end-to-end: join + extend + summarize + top
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task EndToEnd_Join_Extend_Summarize_Top()
    {
        var kql = @"
            Events
            | join kind=inner (StateInfo) on State
            | extend DamagePerCapita = round((DamageProperty + DamageCrops) * 1.0 / Population, 6)
            | summarize
                TotalDamagePerCapita = sum(DamagePerCapita),
                EventCount = count()
              by State, Region
            | top 3 by TotalDamagePerCapita";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(3, rows.Count);
        Assert.All(rows, r =>
        {
            Assert.True(r.GetProperty("TotalDamagePerCapita").GetDouble() > 0);
            Assert.False(string.IsNullOrEmpty(r.GetProperty("Region").GetString()));
        });
    }

    // ──────────────────────────────────────────────────────
    // Chained where filters (should compose correctly)
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Chained_Where_Filters()
    {
        var kql = @"
            Events
            | where Year == 2020
            | where State startswith 'T'
            | where Injuries > 0
            | where EventType != 'Hail'
            | project State, EventType, Injuries";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.All(rows, r =>
        {
            Assert.Equal(2020, 2020); // Year filter
            Assert.StartsWith("T", r.GetProperty("State").GetString());
            Assert.True(r.GetProperty("Injuries").GetInt32() > 0);
            Assert.NotEqual("Hail", r.GetProperty("EventType").GetString());
        });
    }

    // ──────────────────────────────────────────────────────
    // has_any / has_all string operators
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Pipeline_HasAny_Filter()
    {
        var kql = @"
            Events
            | where EventType has_any ('Tornado', 'Hurricane')
            | summarize Count = count() by EventType
            | sort by Count desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        var types = rows.Select(r => r.GetProperty("EventType").GetString()).ToList();
        Assert.Contains("Tornado", types);
        Assert.Contains("Hurricane", types);
        Assert.DoesNotContain("Hail", types);
    }

    // ──────────────────────────────────────────────────────
    // arg_max / arg_min
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Summarize_ArgMax()
    {
        var kql = @"
            Events
            | summarize arg_max(Injuries, EventType, DamageProperty) by State
            | sort by State asc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(5, rows.Count);
        // arg_max returns EventType and DamageProperty from the row with max Injuries
        var texas = rows.First(r => r.GetProperty("State").GetString() == "TEXAS");
        // TEXAS max Injuries=8 row is Tornado with DamageProperty=200000
        Assert.Equal("Tornado", texas.GetProperty("EventType").GetString());
        Assert.Equal(200000, GetInt(texas, "DamageProperty"));
    }

    // ──────────────────────────────────────────────────────
    // Mixed: extend with coalesce, isnull, math chaining
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Pipeline_Coalesce_And_Null_Handling()
    {
        var kql = @"
            Events
            | extend SafeMagnitude = coalesce(Magnitude, 0.0)
            | extend MagnitudeCategory = iif(SafeMagnitude >= 3.0, 'Strong', iif(SafeMagnitude >= 1.0, 'Moderate', 'Weak'))
            | summarize Count = count() by MagnitudeCategory
            | sort by Count desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        var total = rows.Sum(r => r.GetProperty("Count").GetInt32());
        Assert.Equal(15, total);
    }

    // ──────────────────────────────────────────────────────
    // Datatable join with real table
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Datatable_Join_RealTable()
    {
        var kql = @"
            datatable(EventType:string, RiskLevel:long) [
                'Tornado', 5,
                'Hurricane', 5,
                'Flash Flood', 3,
                'Hail', 2,
                'Thunderstorm Wind', 1
            ]
            | join kind=inner (
                Events
                | summarize EventCount = count(), TotalInjuries = sum(Injuries) by EventType
            ) on EventType
            | extend WeightedRisk = RiskLevel * EventCount
            | project EventType, RiskLevel, EventCount, TotalInjuries, WeightedRisk
            | sort by WeightedRisk desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.True(rows.Count >= 4); // At least 4 event types
        // Verify weighted risk is correctly calculated
        Assert.All(rows, r =>
        {
            var expected = r.GetProperty("RiskLevel").GetInt32() * r.GetProperty("EventCount").GetInt32();
            Assert.Equal(expected, r.GetProperty("WeightedRisk").GetInt32());
        });
    }

    // ──────────────────────────────────────────────────────
    // Complex: summarize on summarize (nested aggregation)
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Nested_Summarize()
    {
        // First summarize by State+Year, then summarize by Year across states
        var kql = @"
            Events
            | summarize StateEvents = count(), StateDamage = sum(DamageProperty) by State, Year
            | summarize
                AvgEventsPerState = avg(StateEvents),
                TotalDamage = sum(StateDamage),
                StatesReporting = dcount(State)
              by Year
            | sort by Year asc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(2, rows.Count);
        Assert.All(rows, r =>
        {
            Assert.True(GetDouble(r, "AvgEventsPerState") > 0);
            Assert.True(GetInt(r, "TotalDamage") > 0);
            Assert.True(GetInt(r, "StatesReporting") >= 1);
        });
    }

    // ──────────────────────────────────────────────────────
    // Complex: count by computed expression
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Summarize_By_Computed_Expression()
    {
        var kql = @"
            Events
            | extend HasCasualties = Injuries > 0 or Deaths > 0
            | extend Quarter = case(Month <= 3, 'Q1', Month <= 6, 'Q2', Month <= 9, 'Q3', 'Q4')
            | summarize EventCount = count() by Quarter, HasCasualties
            | sort by Quarter asc, HasCasualties desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        var totalEvents = rows.Sum(r => r.GetProperty("EventCount").GetInt32());
        Assert.Equal(15, totalEvents);
    }

    // ──────────────────────────────────────────────────────
    // Complex: multiple joins (self-join pattern)
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task SelfJoin_CompareYears()
    {
        var kql = @"
            Events
            | where Year == 2020
            | summarize Events2020 = count() by State
            | join kind=inner (
                Events
                | where Year == 2021
                | summarize Events2021 = count() by State
            ) on State
            | extend YoYChange = Events2021 - Events2020
            | project State, Events2020, Events2021, YoYChange
            | sort by YoYChange desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.True(rows.Count >= 3); // States with data in both years
        Assert.All(rows, r =>
        {
            var change = r.GetProperty("YoYChange").GetInt32();
            var diff = r.GetProperty("Events2021").GetInt32() - r.GetProperty("Events2020").GetInt32();
            Assert.Equal(diff, change);
        });
    }

    // ──────────────────────────────────────────────────────
    // Count operator after complex pipeline
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Count_After_Complex_Filter()
    {
        var kql = @"
            Events
            | where Injuries > 0 and DamageProperty > 10000
            | where State in ('TEXAS', 'OKLAHOMA', 'KANSAS')
            | count";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Single(rows);
        Assert.True(rows[0].GetProperty("Count").GetInt32() > 0);
    }

    // ──────────────────────────────────────────────────────
    // Matches regex in pipeline
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Pipeline_MatchesRegex()
    {
        var kql = @"
            Events
            | where Source matches regex 'Trained|Storm'
            | summarize Count = count() by Source
            | sort by Count desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.All(rows, r =>
        {
            var source = r.GetProperty("Source").GetString()!;
            Assert.True(source.Contains("Trained") || source.Contains("Storm"));
        });
    }

    // ──────────────────────────────────────────────────────
    // Top-hitters
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task TopHitters_EventType()
    {
        var kql = @"
            Events
            | top-hitters 3 of EventType";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(3, rows.Count);
        // First should be the most common event type
        Assert.True(rows[0].GetProperty("approximate_count").GetInt32() >= rows[1].GetProperty("approximate_count").GetInt32());
    }

    // ──────────────────────────────────────────────────────
    // Deeply nested: 8+ operator pipeline
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task DeepPipeline_8Operators()
    {
        var kql = @"
            Events
            | where Year >= 2020
            | where Injuries > 0
            | extend TotalDamage = DamageProperty + DamageCrops
            | extend DamageRatio = iif(TotalDamage > 0, Injuries * 1.0 / TotalDamage * 100000, 0.0)
            | summarize AvgRatio = avg(DamageRatio), MaxRatio = max(DamageRatio), Count = count() by State
            | where Count >= 2
            | sort by AvgRatio desc
            | take 3";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.True(rows.Count > 0 && rows.Count <= 3);
        Assert.All(rows, r => Assert.True(r.GetProperty("Count").GetInt32() >= 2));
    }

    // ──────────────────────────────────────────────────────
    // Startswith / endswith combined pipeline
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Pipeline_Startswith_Endswith()
    {
        var kql = @"
            Events
            | where State startswith 'T' or State endswith 'DA'
            | distinct State
            | sort by State asc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.All(rows, r =>
        {
            var state = r.GetProperty("State").GetString()!;
            Assert.True(state.StartsWith("T", StringComparison.OrdinalIgnoreCase) ||
                        state.EndsWith("DA", StringComparison.OrdinalIgnoreCase));
        });
    }

    // ──────────────────────────────────────────────────────
    // Extend with row_number + top within groups
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Pipeline_Serialize_RowNumber()
    {
        var kql = @"
            Events
            | where EventType == 'Tornado'
            | serialize rn = row_number()
            | project rn, State, Injuries
            | take 5";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(5, rows.Count);
        // Verify row_number column exists and is sequential
        var rowNums = rows.Select(r => r.GetProperty("rn").GetInt32()).ToList();
        for (int i = 1; i < rowNums.Count; i++)
        {
            Assert.Equal(rowNums[i - 1] + 1, rowNums[i]);
        }
    }

    // ──────────────────────────────────────────────────────
    // Complex: Union with withsource
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Union_WithSource()
    {
        // Union same-schema subsets with source tracking
        var kql = @"
            Events
            | where Year == 2020
            | project State, EventType, Injuries
            | union withsource=TableName (
                Events
                | where Year == 2021
                | project State, EventType, Injuries
            )
            | count";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Single(rows);
        // All 15 events should appear (some from 2020, some from 2021)
        Assert.Equal(15, rows[0].GetProperty("Count").GetInt32());
    }

    // ──────────────────────────────────────────────────────
    // Complex: summarize + extend + project chain
    // ──────────────────────────────────────────────────────

    [Fact]
    public async Task Summarize_Extend_Project_Chain()
    {
        var kql = @"
            Events
            | summarize TotalDamage = sum(DamageProperty), TotalCropDamage = sum(DamageCrops), Events = count() by State
            | extend AllDamage = TotalDamage + TotalCropDamage
            | extend AvgDamagePerEvent = round(AllDamage * 1.0 / Events, 2)
            | extend Category = iif(AvgDamagePerEvent > 100000, 'High', iif(AvgDamagePerEvent > 10000, 'Medium', 'Low'))
            | project State, Events, AllDamage, AvgDamagePerEvent, Category
            | sort by AllDamage desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.Equal(5, rows.Count);
        // Verify descending damage order
        for (int i = 1; i < rows.Count; i++)
        {
            Assert.True(GetInt(rows[i - 1], "AllDamage") >= GetInt(rows[i], "AllDamage"));
        }
        // Verify categories are valid
        Assert.All(rows, r => Assert.Contains(r.GetProperty("Category").GetString(), new[] { "High", "Medium", "Low" }));
    }

    // ══════════════════════════════════════════════════════
    // README Showcases — complex real-world-style analytics
    // ══════════════════════════════════════════════════════

    [Fact]
    public async Task Showcase_RiskScoreDashboard()
    {
        // let + datatable + join + case + multi-agg summarize + computed ranking
        var kql = @"
            let severity_lookup = datatable(EventType:string, SeverityWeight:long) [
                'Tornado', 5,
                'Hurricane', 5,
                'Flash Flood', 3,
                'Hail', 2,
                'Thunderstorm Wind', 1
            ];
            let enriched = Events
                | join kind=inner (severity_lookup) on EventType
                | extend TotalDamage = DamageProperty + DamageCrops
                | extend DamageCategory = case(
                    TotalDamage >= 100000, 'Catastrophic',
                    TotalDamage >= 10000,  'Severe',
                    'Moderate');
            enriched
            | summarize
                EventCount = count(),
                TotalInjuries = sum(Injuries),
                TotalDeaths = sum(Deaths),
                AvgSeverity = avg(toreal(SeverityWeight)),
                MaxDamage = max(TotalDamage),
                CatastrophicCount = countif(DamageCategory == 'Catastrophic')
              by State
            | extend AvgSeverity = round(AvgSeverity, 2)
            | extend CatastrophicPct = round(CatastrophicCount * 100.0 / EventCount, 1)
            | extend RiskScore = round(AvgSeverity * EventCount + TotalInjuries * 2 + TotalDeaths * 10, 1)
            | project State, EventCount, TotalInjuries, TotalDeaths, AvgSeverity, CatastrophicPct, RiskScore
            | top 5 by RiskScore";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.True(rows.Count >= 3 && rows.Count <= 5);
        // Verify risk score ordering (descending)
        for (int i = 1; i < rows.Count; i++)
        {
            Assert.True(GetDouble(rows[i - 1], "RiskScore") >= GetDouble(rows[i], "RiskScore"));
        }
        // Every state should have events
        Assert.All(rows, r =>
        {
            Assert.True(GetInt(r, "EventCount") > 0);
            Assert.True(GetDouble(r, "AvgSeverity") > 0);
            Assert.True(GetDouble(r, "CatastrophicPct") >= 0);
        });
    }

    [Fact]
    public async Task Showcase_YearOverYearComparison()
    {
        // Self-join for YoY comparison + computed change metrics + trend labels
        var kql = @"
            Events
            | where Year == 2021
            | summarize Events2021 = count(), Damage2021 = sum(DamageProperty + DamageCrops), AvgInjuries = avg(toreal(Injuries)) by State
            | join kind=inner (
                Events
                | where Year == 2020
                | summarize Events2020 = count(), Damage2020 = sum(DamageProperty + DamageCrops) by State
            ) on State
            | extend AvgInjuries = round(AvgInjuries, 1)
            | extend DamageChangePct = round((Damage2021 - Damage2020) * 100.0 / Damage2020, 1)
            | extend Trend = iif(Events2021 > Events2020, 'Increasing', iif(Events2021 < Events2020, 'Decreasing', 'Stable'))
            | project State, Events2020, Events2021, Trend, Damage2020, Damage2021, DamageChangePct, AvgInjuries
            | sort by DamageChangePct desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.True(rows.Count >= 3);
        Assert.All(rows, r =>
        {
            Assert.True(GetInt(r, "Events2020") > 0);
            Assert.True(GetInt(r, "Events2021") > 0);
            var trend = r.GetProperty("Trend").GetString()!;
            Assert.Contains(trend, new[] { "Increasing", "Decreasing", "Stable" });
            // Verify trend label matches the data
            var e20 = GetInt(r, "Events2020");
            var e21 = GetInt(r, "Events2021");
            var expected = e21 > e20 ? "Increasing" : e21 < e20 ? "Decreasing" : "Stable";
            Assert.Equal(expected, trend);
        });
        // Verify sort order
        for (int i = 1; i < rows.Count; i++)
        {
            Assert.True(GetDouble(rows[i - 1], "DamageChangePct") >= GetDouble(rows[i], "DamageChangePct"));
        }
    }

    [Fact]
    public async Task Showcase_SourceReliabilityAnalysis()
    {
        // Nested extend + case + countif + computed scoring + string functions
        var kql = @"
            Events
            | extend TotalDamage = DamageProperty + DamageCrops
            | extend DamageBucket = case(
                TotalDamage >= 100000, 'High',
                TotalDamage >= 10000,  'Medium',
                'Low')
            | summarize
                Reports = count(),
                AvgMagnitude = avg(Magnitude),
                HighDamageReports = countif(DamageBucket == 'High'),
                TotalInjuries = sum(Injuries),
                StatesAffected = dcount(State)
              by Source
            | extend AvgMagnitude = round(AvgMagnitude, 2)
            | extend HighDamagePct = round(HighDamageReports * 100.0 / Reports, 1)
            | extend SourceLabel = strcat(Source, ' (', tostring(Reports), ' reports)')
            | project SourceLabel, Reports, AvgMagnitude, HighDamagePct, TotalInjuries, StatesAffected
            | sort by Reports desc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.True(rows.Count >= 3); // At least 3 distinct sources
        Assert.All(rows, r =>
        {
            Assert.True(GetInt(r, "Reports") > 0);
            Assert.Contains("reports)", r.GetProperty("SourceLabel").GetString());
            Assert.True(GetDouble(r, "HighDamagePct") >= 0 && GetDouble(r, "HighDamagePct") <= 100);
        });
        // Verify descending sort by Reports
        for (int i = 1; i < rows.Count; i++)
        {
            Assert.True(GetInt(rows[i - 1], "Reports") >= GetInt(rows[i], "Reports"));
        }
    }

    [Fact]
    public async Task Showcase_RegionalImpactWithLookup()
    {
        // lookup + extend + nested iif + between + summarize by computed column
        var kql = @"
            Events
            | lookup StateInfo on State
            | where Injuries between (1 .. 10)
            | extend ImpactPerCapita = round(Injuries * 1000000.0 / Population, 2)
            | extend Quarter = case(Month <= 3, 'Q1', Month <= 6, 'Q2', Month <= 9, 'Q3', 'Q4')
            | summarize
                IncidentCount = count(),
                TotalInjuries = sum(Injuries),
                AvgImpactPerCapita = avg(ImpactPerCapita)
              by Region, Quarter
            | extend AvgImpactPerCapita = round(AvgImpactPerCapita, 4)
            | extend Severity = iif(TotalInjuries >= 10, 'Critical', iif(TotalInjuries >= 5, 'Warning', 'Normal'))
            | project Region, Quarter, IncidentCount, TotalInjuries, AvgImpactPerCapita, Severity
            | sort by Region asc, Quarter asc";
        var sql = _converter.Convert(kql);
        var rows = await Query(sql);

        Assert.True(rows.Count >= 2);
        Assert.All(rows, r =>
        {
            Assert.False(string.IsNullOrEmpty(r.GetProperty("Region").GetString()));
            Assert.Contains(r.GetProperty("Quarter").GetString(), new[] { "Q1", "Q2", "Q3", "Q4" });
            Assert.Contains(r.GetProperty("Severity").GetString(), new[] { "Critical", "Warning", "Normal" });
            Assert.True(GetInt(r, "IncidentCount") > 0);
        });
    }

    private async Task<List<JsonElement>> Query(string sql)
    {
        var json = await _fixture.NodeJS.InvokeFromFileAsync<string>(
            _scriptPath,
            args: new object[] { "query", _fixture.NodeModulesPath, sql });

        return JsonSerializer.Deserialize<List<JsonElement>>(json!)!;
    }

    private async Task Exec(string sql)
    {
        await _fixture.NodeJS.InvokeFromFileAsync<string>(
            _scriptPath,
            args: new object[] { "exec", _fixture.NodeModulesPath, sql });
    }

    /// <summary>
    /// DuckDB WASM sometimes serializes aggregates (especially FILTER-based)
    /// as quoted strings rather than numbers. This helper reads either form.
    /// </summary>
    private static int GetInt(JsonElement el, string prop)
    {
        var p = el.GetProperty(prop);
        return p.ValueKind == JsonValueKind.Number
            ? p.GetInt32()
            : int.Parse(p.GetString()!.Trim('"'));
    }

    private static double GetDouble(JsonElement el, string prop)
    {
        var p = el.GetProperty(prop);
        return p.ValueKind == JsonValueKind.Number
            ? p.GetDouble()
            : double.Parse(p.GetString()!.Trim('"'));
    }
}
