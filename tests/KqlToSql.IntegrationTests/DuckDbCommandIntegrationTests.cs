using System.Text.Json;
using KqlToSql.Dialects;

namespace KqlToSql.IntegrationTests;

/// <summary>
/// Integration tests that convert KQL management commands to SQL
/// and execute them against DuckDB WASM.
/// </summary>
[Collection("NodeJS")]
public class DuckDbCommandIntegrationTests : IAsyncLifetime
{
    private readonly NodeJSFixture _fixture;
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());
    private string _scriptPath = null!;

    public DuckDbCommandIntegrationTests(NodeJSFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        _scriptPath = Path.Combine(_fixture.ScriptsPath, "duckdbWasmRunner.js");

        // Seed a base table for tests
        await Exec("DROP TABLE IF EXISTS CmdTestEvents");
        await Exec(@"
            CREATE TABLE CmdTestEvents (
                State VARCHAR,
                EventType VARCHAR,
                Injuries INTEGER,
                DamageProperty INTEGER,
                Year INTEGER
            )");
        await Exec(@"
            INSERT INTO CmdTestEvents VALUES
                ('TEXAS', 'Tornado', 5, 50000, 2020),
                ('TEXAS', 'Hail', 0, 500, 2020),
                ('KANSAS', 'Tornado', 3, 25000, 2021),
                ('OKLAHOMA', 'Tornado', 2, 15000, 2021)
        ");
    }

    public Task DisposeAsync() => Task.CompletedTask;

    // ── .create table ───────────────────────────────────────

    [Fact]
    public async Task CreateTable_And_Insert()
    {
        await Exec("DROP TABLE IF EXISTS NewTbl");
        var sql = _converter.Convert(".create table NewTbl (Id:int, Name:string, Score:real)");
        await Exec(sql);
        await Exec("INSERT INTO NewTbl VALUES (1, 'Alice', 9.5)");
        var rows = await Query("SELECT * FROM NewTbl");
        Assert.Single(rows);
        Assert.Equal("Alice", rows[0].GetProperty("Name").GetString());
    }

    [Fact]
    public async Task CreateTable_BasedOn()
    {
        await Exec("DROP TABLE IF EXISTS ClonedEvents");
        var sql = _converter.Convert(".create table ClonedEvents based-on CmdTestEvents");
        await Exec(sql);
        var rows = await Query("SELECT * FROM ClonedEvents");
        Assert.Empty(rows); // schema only, no data
        // Verify schema matches by inserting
        await Exec("INSERT INTO ClonedEvents SELECT * FROM CmdTestEvents LIMIT 1");
        rows = await Query("SELECT * FROM ClonedEvents");
        Assert.Single(rows);
    }

    // ── .drop table ─────────────────────────────────────────

    [Fact]
    public async Task DropTable()
    {
        await Exec("CREATE TABLE DropMe (Id INTEGER)");
        var sql = _converter.Convert(".drop table DropMe");
        Assert.Equal("DROP TABLE DropMe", sql);
        await Exec(sql);
        // Table should be gone — querying it should fail
        await Assert.ThrowsAnyAsync<Exception>(() => Query("SELECT * FROM DropMe"));
    }

    [Fact]
    public async Task DropTable_IfExists_NoError()
    {
        var sql = _converter.Convert(".drop table NonExistent123 ifexists");
        Assert.Equal("DROP TABLE IF EXISTS NonExistent123", sql);
        await Exec(sql); // Should not throw
    }

    // ── .rename table ───────────────────────────────────────

    [Fact]
    public async Task RenameTable()
    {
        await Exec("DROP TABLE IF EXISTS RenSrc");
        await Exec("DROP TABLE IF EXISTS RenDst");
        await Exec("CREATE TABLE RenSrc (Val INTEGER)");
        await Exec("INSERT INTO RenSrc VALUES (42)");
        var sql = _converter.Convert(".rename table RenSrc to RenDst");
        await Exec(sql);
        var rows = await Query("SELECT * FROM RenDst");
        Assert.Single(rows);
        Assert.Equal(42, rows[0].GetProperty("Val").GetInt32());
    }

    // ── .alter-merge table ──────────────────────────────────

    [Fact]
    public async Task AlterMergeTable_AddColumns()
    {
        await Exec("DROP TABLE IF EXISTS MergeTbl");
        await Exec("CREATE TABLE MergeTbl (Id INTEGER)");
        var sql = _converter.Convert(".alter-merge table MergeTbl (Name:string, Score:real)");
        // Multiple statements
        foreach (var stmt in sql.Split(';', StringSplitOptions.RemoveEmptyEntries))
            await Exec(stmt.Trim());
        await Exec("INSERT INTO MergeTbl VALUES (1, 'Bob', 8.5)");
        var rows = await Query("SELECT * FROM MergeTbl");
        Assert.Single(rows);
        Assert.Equal("Bob", rows[0].GetProperty("Name").GetString());
    }

    // ── .clear table data ───────────────────────────────────

    [Fact]
    public async Task ClearTableData()
    {
        await Exec("DROP TABLE IF EXISTS ClearMe");
        await Exec("CREATE TABLE ClearMe (Id INTEGER)");
        await Exec("INSERT INTO ClearMe VALUES (1), (2), (3)");
        var sql = _converter.Convert(".clear table ClearMe data");
        await Exec(sql);
        var rows = await Query("SELECT COUNT(*) AS cnt FROM ClearMe");
        Assert.Equal(0, rows[0].GetProperty("cnt").GetInt32());
    }

    // ── .show tables ────────────────────────────────────────

    [Fact]
    public async Task ShowTables()
    {
        var sql = _converter.Convert(".show tables");
        var rows = await Query(sql);
        var names = rows.Select(r => r.GetProperty("table_name").GetString()).ToList();
        Assert.Contains("CmdTestEvents", names);
    }

    // ── .show table details ─────────────────────────────────

    [Fact]
    public async Task ShowTableDetails()
    {
        var sql = _converter.Convert(".show table CmdTestEvents details");
        var rows = await Query(sql);
        Assert.True(rows.Count > 0); // DESCRIBE returns column info
    }

    // ── Column commands ─────────────────────────────────────

    [Fact]
    public async Task AlterColumnType()
    {
        await Exec("DROP TABLE IF EXISTS ColTest");
        await Exec("CREATE TABLE ColTest (Val INTEGER)");
        var sql = _converter.Convert(".alter column ColTest.Val type=string");
        await Exec(sql);
        await Exec("INSERT INTO ColTest VALUES ('hello')");
        var rows = await Query("SELECT * FROM ColTest");
        Assert.Equal("hello", rows[0].GetProperty("Val").GetString());
    }

    [Fact]
    public async Task DropColumn()
    {
        await Exec("DROP TABLE IF EXISTS DropColTest");
        await Exec("CREATE TABLE DropColTest (A INTEGER, B VARCHAR, C INTEGER)");
        await Exec("INSERT INTO DropColTest VALUES (1, 'x', 2)");
        var sql = _converter.Convert(".drop column DropColTest.B");
        await Exec(sql);
        var rows = await Query("SELECT * FROM DropColTest");
        Assert.True(rows[0].TryGetProperty("A", out _));
        Assert.False(rows[0].TryGetProperty("B", out _));
        Assert.True(rows[0].TryGetProperty("C", out _));
    }

    [Fact]
    public async Task DropTableColumns()
    {
        await Exec("DROP TABLE IF EXISTS MultiDropCol");
        await Exec("CREATE TABLE MultiDropCol (A INTEGER, B VARCHAR, C INTEGER, D VARCHAR)");
        var sql = _converter.Convert(".drop table MultiDropCol columns (B, D)");
        foreach (var stmt in sql.Split(';', StringSplitOptions.RemoveEmptyEntries))
            await Exec(stmt.Trim());
        await Exec("INSERT INTO MultiDropCol VALUES (1, 2)");
        var rows = await Query("SELECT * FROM MultiDropCol");
        Assert.True(rows[0].TryGetProperty("A", out _));
        Assert.True(rows[0].TryGetProperty("C", out _));
        Assert.False(rows[0].TryGetProperty("B", out _));
        Assert.False(rows[0].TryGetProperty("D", out _));
    }

    [Fact]
    public async Task RenameColumn()
    {
        await Exec("DROP TABLE IF EXISTS RenColTest");
        await Exec("CREATE TABLE RenColTest (OldName INTEGER)");
        await Exec("INSERT INTO RenColTest VALUES (99)");
        var sql = _converter.Convert(".rename column RenColTest.OldName to NewName");
        await Exec(sql);
        var rows = await Query("SELECT NewName FROM RenColTest");
        Assert.Equal(99, rows[0].GetProperty("NewName").GetInt32());
    }

    // ── Function / View commands ────────────────────────────

    [Fact]
    public async Task CreateFunction_AsView()
    {
        await Exec("DROP VIEW IF EXISTS TornadoView");
        var sql = _converter.Convert(".create function TornadoView() { CmdTestEvents | where EventType == 'Tornado' }");
        await Exec(sql);
        var rows = await Query("SELECT * FROM TornadoView");
        Assert.All(rows, r => Assert.Equal("Tornado", r.GetProperty("EventType").GetString()));
    }

    [Fact]
    public async Task CreateOrAlterFunction()
    {
        await Exec("DROP VIEW IF EXISTS TexasView");
        var sql = _converter.Convert(".create-or-alter function TexasView() { CmdTestEvents | where State == 'TEXAS' }");
        await Exec(sql);
        var rows = await Query("SELECT * FROM TexasView");
        Assert.All(rows, r => Assert.Equal("TEXAS", r.GetProperty("State").GetString()));
        // Alter it
        sql = _converter.Convert(".create-or-alter function TexasView() { CmdTestEvents | where State == 'KANSAS' }");
        await Exec(sql);
        rows = await Query("SELECT * FROM TexasView");
        Assert.All(rows, r => Assert.Equal("KANSAS", r.GetProperty("State").GetString()));
    }

    [Fact]
    public async Task DropFunction()
    {
        await Exec("DROP VIEW IF EXISTS TempView");
        await Exec("CREATE VIEW TempView AS SELECT 1 AS val");
        var sql = _converter.Convert(".drop function TempView");
        await Exec(sql);
        await Assert.ThrowsAnyAsync<Exception>(() => Query("SELECT * FROM TempView"));
    }

    // ── .set / .append / .set-or-append / .set-or-replace ──

    [Fact]
    public async Task Set_CreatesTableFromQuery()
    {
        await Exec("DROP TABLE IF EXISTS SetResult");
        var sql = _converter.Convert(".set SetResult <| CmdTestEvents | where State == 'TEXAS' | project State, EventType");
        await Exec(sql);
        var rows = await Query("SELECT * FROM SetResult");
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal("TEXAS", r.GetProperty("State").GetString()));
    }

    [Fact]
    public async Task Append_InsertsIntoExisting()
    {
        await Exec("DROP TABLE IF EXISTS AppendTarget");
        await Exec("CREATE TABLE AppendTarget (State VARCHAR, EventType VARCHAR)");
        await Exec("INSERT INTO AppendTarget VALUES ('INITIAL', 'Test')");
        var sql = _converter.Convert(".append AppendTarget <| CmdTestEvents | where State == 'KANSAS' | project State, EventType");
        await Exec(sql);
        var rows = await Query("SELECT * FROM AppendTarget");
        Assert.Equal(2, rows.Count); // 1 initial + 1 from KANSAS
    }

    [Fact]
    public async Task SetOrReplace_DropsAndRecreates()
    {
        await Exec("DROP TABLE IF EXISTS ReplaceTarget");
        await Exec("CREATE TABLE ReplaceTarget (X INTEGER)");
        await Exec("INSERT INTO ReplaceTarget VALUES (999)");
        var sql = _converter.Convert(".set-or-replace ReplaceTarget <| CmdTestEvents | project State, Injuries");
        foreach (var stmt in sql.Split(';', StringSplitOptions.RemoveEmptyEntries))
            await Exec(stmt.Trim());
        var rows = await Query("SELECT * FROM ReplaceTarget");
        Assert.Equal(4, rows.Count);
        // Old column X should be gone, new columns State and Injuries should exist
        Assert.True(rows[0].TryGetProperty("State", out _));
    }

    // ── .purge / .delete ────────────────────────────────────

    [Fact]
    public async Task Purge_DeletesMatchingRows()
    {
        await Exec("DROP TABLE IF EXISTS PurgeTbl");
        await Exec("CREATE TABLE PurgeTbl AS SELECT * FROM CmdTestEvents");
        var sql = _converter.Convert(".purge table PurgeTbl records <| where State == 'TEXAS'");
        await Exec(sql);
        var rows = await Query("SELECT * FROM PurgeTbl");
        Assert.All(rows, r => Assert.NotEqual("TEXAS", r.GetProperty("State").GetString()));
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Delete_DeletesMatchingRows()
    {
        await Exec("DROP TABLE IF EXISTS DelTbl");
        await Exec("CREATE TABLE DelTbl AS SELECT * FROM CmdTestEvents");
        var sql = _converter.Convert(".delete table DelTbl records <| where Injuries > 2");
        await Exec(sql);
        var rows = await Query("SELECT * FROM DelTbl");
        Assert.All(rows, r => Assert.True(r.GetProperty("Injuries").GetInt32() <= 2));
    }

    // ── .view ───────────────────────────────────────────────

    [Fact]
    public async Task View_Command()
    {
        await Exec("DROP VIEW IF EXISTS DamageView");
        var sql = _converter.Convert(".view DamageView <| CmdTestEvents | where DamageProperty > 10000 | project State, DamageProperty");
        await Exec(sql);
        var rows = await Query("SELECT * FROM DamageView");
        Assert.All(rows, r => Assert.True(r.GetProperty("DamageProperty").GetInt32() > 10000));
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
}
