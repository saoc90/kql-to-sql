using System.Collections.Generic;
using System.Linq;
using DuckDB.NET.Data;
using KqlToSql;
using Xunit;

namespace KqlToSql.Tests.Operators;

public class JoinOperatorTests
{
    [Fact]
    public void Converts_Join_Default()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* RENAME (\"Key\" AS Key1) FROM (SELECT * FROM X QUALIFY ROW_NUMBER() OVER (PARTITION BY \"Key\") = 1) AS L INNER JOIN Y AS R ON L.\"Key\" IS NOT DISTINCT FROM R.\"Key\"", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string Key, long Value1, string Key1, long Value2)>();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetInt64(1), reader.GetString(2), reader.GetInt64(3)));
        }
        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 2 && r.Key1 == "b" && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 20);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 30);
    }

    [Fact]
    public void Converts_LeftOuterJoin()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join kind=leftouter Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* RENAME (\"Key\" AS Key1) FROM X AS L LEFT OUTER JOIN Y AS R ON L.\"Key\" IS NOT DISTINCT FROM R.\"Key\"", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string? Key, long? Value1, string? Key1, long? Value2)>();
        while (reader.Read())
        {
            results.Add((
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3)
            ));
        }
        Assert.Equal(5, results.Count);
        Assert.Contains(results, r => r.Key == "a" && r.Value1 == 1 && r.Key1 == null && r.Value2 == null);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 2 && r.Key1 == "b" && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 3 && r.Key1 == "b" && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 20);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 30);
    }

    [Fact]
    public void Converts_RightOuterJoin()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join kind=rightouter Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* RENAME (\"Key\" AS Key1) FROM X AS L RIGHT OUTER JOIN Y AS R ON L.\"Key\" IS NOT DISTINCT FROM R.\"Key\"", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string? Key, long? Value1, string? Key1, long Value2)>();
        while (reader.Read())
        {
            results.Add((
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetInt64(3)
            ));
        }
        Assert.Equal(5, results.Count);
        Assert.Contains(results, r => r.Value2 == 10 && r.Key == "b" && r.Value1 == 2 && r.Key1 == "b");
        Assert.Contains(results, r => r.Value2 == 10 && r.Key == "b" && r.Value1 == 3 && r.Key1 == "b");
        Assert.Contains(results, r => r.Value2 == 20 && r.Key == "c" && r.Value1 == 4 && r.Key1 == "c");
        Assert.Contains(results, r => r.Value2 == 30 && r.Key == "c" && r.Value1 == 4 && r.Key1 == "c");
        Assert.Contains(results, r => r.Value2 == 40 && r.Key == null && r.Value1 == null && r.Key1 == "d");
    }

    [Fact]
    public void Converts_FullOuterJoin()
    {
        DuckDbSetup.EnsureDuckDb();

        var converter = new KqlToSqlConverter();
        var kql = "X | join kind=fullouter Y on Key";
        var sql = converter.Convert(kql);
        Assert.Equal("SELECT L.*, R.* RENAME (\"Key\" AS Key1) FROM X AS L FULL OUTER JOIN Y AS R ON L.\"Key\" IS NOT DISTINCT FROM R.\"Key\"", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateJoinTables(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<(string? Key, long? Value1, string? Key1, long? Value2)>();
        while (reader.Read())
        {
            results.Add((
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3)
            ));
        }
        Assert.Equal(6, results.Count);
        Assert.Contains(results, r => r.Key == "a" && r.Value1 == 1 && r.Key1 == null && r.Value2 == null);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 2 && r.Key1 == "b" && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "b" && r.Value1 == 3 && r.Key1 == "b" && r.Value2 == 10);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 20);
        Assert.Contains(results, r => r.Key == "c" && r.Value1 == 4 && r.Key1 == "c" && r.Value2 == 30);
        Assert.Contains(results, r => r.Key == null && r.Value1 == null && r.Key1 == "d" && r.Value2 == 40);
    }

    [Fact]
    public void Converts_Join_DuplicateCols_Get_Suffixed_With_1()
    {
        // KQL: any R column that also exists on L keeps L's name and is re-emitted as <name>1.
        // Uses CTEs so the converter can enumerate both sides' output columns via AST.
        DuckDbSetup.EnsureDuckDb();
        var converter = new KqlToSqlConverter();
        var kql = "let A = X | project Key, Value; let B = Y | project Key, Value; A | join B on Key";
        var sql = converter.Convert(kql);

        // Live Kusto: R.Key becomes Key1 (not dropped) and R.Value becomes Value1.
        Assert.Contains("R.\"Key\" AS Key1", sql);
        Assert.Contains("R.Value AS Value1", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE X (Key VARCHAR, Value BIGINT); INSERT INTO X VALUES ('b', 2), ('c', 4);";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE TABLE Y (Key VARCHAR, Value BIGINT); INSERT INTO Y VALUES ('b', 10), ('c', 20);";
            cmd.ExecuteNonQuery();
        }
        using var runCmd = conn.CreateCommand();
        runCmd.CommandText = sql;
        using var reader = runCmd.ExecuteReader();
        // Columns: Key, Value, Key1, Value1
        var rows = new List<(string Key, long Value, string Key1, long Value1)>();
        while (reader.Read())
        {
            rows.Add((reader.GetString(0), reader.GetInt64(1), reader.GetString(2), reader.GetInt64(3)));
        }
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r.Key == "b" && r.Value == 2 && r.Key1 == "b" && r.Value1 == 10);
        Assert.Contains(rows, r => r.Key == "c" && r.Value == 4 && r.Key1 == "c" && r.Value1 == 20);
    }

    [Fact]
    public void Converts_Join_NonColliding_Cols_Not_Suffixed()
    {
        // If the R column name doesn't exist on L, no suffix is applied.
        var converter = new KqlToSqlConverter();
        var kql = "let A = X | project Key, LeftOnly; let B = Y | project Key, RightOnly; A | join B on Key";
        var sql = converter.Convert(kql);
        Assert.Contains("R.RightOnly", sql);
        Assert.DoesNotContain("AS RightOnly1", sql);
    }

    [Fact]
    public void Converts_Join_BareTable_WithRegisteredSchema_Suffixes_Duplicates()
    {
        // When a join side is a bare table reference whose schema was registered,
        // structural column enumeration resolves it and duplicates get the `1` suffix.
        var converter = new KqlToSqlConverter();
        converter.RegisterTableColumns("MyTable", new[] { "Key", "Value" });
        var kql = "let A = MyTable | project Key, Value; A | join MyTable on Key";
        var sql = converter.Convert(kql);
        Assert.Contains("R.Value AS Value1", sql);
    }

    [Fact]
    public void Converts_Join_WellKnown_Telemetry_Suffixes_Duplicates()
    {
        // Telemetry is pre-registered with the Bühler columns; joining two Telemetry-derived
        // CTEs should suffix overlapping columns (Timestamp) as Timestamp1.
        var converter = new KqlToSqlConverter();
        var kql = "let Left = Telemetry | where Timestamp > ago(1h); Left | join Telemetry on DeviceId";
        var sql = converter.Convert(kql);
        Assert.Contains("R.Timestamp AS Timestamp1", sql);
    }

    [Fact]
    public void Converts_Join_UserFunction_Body_Enumerates_Columns()
    {
        // When the join RHS is a call to a user-defined function, enumerate the body
        // so duplicate columns get the `1` suffix.
        var converter = new KqlToSqlConverter();
        var kql = "let GetItems = (p:string) { X | project Key, Value };"
                + " let A = X | project Key, Value;"
                + " A | join GetItems(\"foo\") on Key";
        var sql = converter.Convert(kql);
        Assert.Contains("R.Value AS Value1", sql);
    }

    [Fact]
    public void Converts_Join_ArgMax_Rhs_Enumerates_Columns_For_ProjectAway()
    {
        // RHS is a view whose summarize uses bare arg_max (no outer alias).
        // The column enumerator must emit Timestamp + V so that project-away Timestamp1
        // on the outer join resolves correctly (no "EXCLUDE list not found" error).
        var converter = new KqlToSqlConverter();
        var kql =
            "let B = view() { X | summarize arg_max(Timestamp, V=Energy) by jobIdent | project-away Timestamp | extend key=1 };"
            + " X | join kind=leftouter B on key";
        var sql = converter.Convert(kql);
        // The join must enumerate columns rather than falling back to EXCLUDE on unknown schema.
        // Check that Timestamp is not emitted as an EXCLUDE target (it doesn't exist on the R side
        // after project-away), and that the join clause is present.
        Assert.Contains("LEFT OUTER JOIN", sql);
        Assert.DoesNotContain("EXCLUDE (\"Timestamp\"", sql);
    }

    [Fact]
    public void Join_DatatableKeyCollision_RightSharedColumn_Suffixed_With_1()
    {
        // Bug: when both sides share a non-join column name (v), Kusto renames the RIGHT copy to v1.
        // The datatable schema must be enumerable so the suffix is applied (previously fell back to
        // a key-only RENAME that left an unbound `v1` / duplicate `v`). Oracle: cols k,v,k1,v1.
        DuckDbSetup.EnsureDuckDb();
        var converter = new KqlToSqlConverter();
        var kql = "let L=datatable(k:long,v:long)[1,10,2,20,3,30]; let R=datatable(k:long,v:long)[1,100,2,200]; L | join kind=leftouter R on k";
        var sql = converter.Convert(kql);
        Assert.Contains("R.v AS v1", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        Assert.Equal(new[] { "k", "v", "k1", "v1" },
            Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray());
        var rows = new List<(long k, long v, long? k1, long? v1)>();
        while (reader.Read())
            rows.Add((reader.GetInt64(0), reader.GetInt64(1),
                reader.IsDBNull(2) ? (long?)null : reader.GetInt64(2),
                reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3)));
        Assert.Equal(3, rows.Count);
        Assert.Contains(rows, r => r.k == 1 && r.v == 10 && r.k1 == 1 && r.v1 == 100);
        Assert.Contains(rows, r => r.k == 2 && r.v == 20 && r.k1 == 2 && r.v1 == 200);
        Assert.Contains(rows, r => r.k == 3 && r.v == 30 && r.k1 == null && r.v1 == null);
    }

    [Fact]
    public void Join_FullOuter_StringColumns_PaddedWith_EmptyString_NumericsStayNull()
    {
        // Bug: Kusto fills unmatched STRING cells with '' (not NULL) on the nullable side of an
        // outer join; numeric/other types stay NULL. Verified against the oracle.
        DuckDbSetup.EnsureDuckDb();
        var converter = new KqlToSqlConverter();
        var kql = "let L=datatable(k:long,lv:string)[1,\"a\",2,\"b\",3,\"c\"]; let R=datatable(k:long,rv:string)[2,\"x\",4,\"y\"]; L | join kind=fullouter R on k";
        var sql = converter.Convert(kql);
        Assert.Contains("COALESCE(L.lv, '')", sql);
        Assert.Contains("COALESCE(R.rv, '')", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var rows = new List<(long? k, string lv, long? k1, string rv)>();
        while (reader.Read())
            rows.Add((
                reader.IsDBNull(0) ? (long?)null : reader.GetInt64(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? (long?)null : reader.GetInt64(2),
                reader.GetString(3)));
        Assert.Equal(4, rows.Count);
        // left-only rows: rv padded to '' but the long key k1 stays NULL
        Assert.Contains(rows, r => r.k == 1 && r.lv == "a" && r.k1 == null && r.rv == "");
        Assert.Contains(rows, r => r.k == 3 && r.lv == "c" && r.k1 == null && r.rv == "");
        // matched row
        Assert.Contains(rows, r => r.k == 2 && r.lv == "b" && r.k1 == 2 && r.rv == "x");
        // right-only row: lv padded to '' but the long key k stays NULL
        Assert.Contains(rows, r => r.k == null && r.lv == "" && r.k1 == 4 && r.rv == "y");
    }

    [Fact]
    public void Join_NullKeys_Match_NullEqualsNull()
    {
        // Bug: Kusto treats null == null as equal in join keys; SQL `=` does not. The translator
        // must emit IS NOT DISTINCT FROM so a null key on both sides joins.
        DuckDbSetup.EnsureDuckDb();
        var converter = new KqlToSqlConverter();
        var kql = "let L=datatable(k:long,v:long)[1,10,long(null),20]; let R=datatable(k:long,w:long)[long(null),200,3,300]; L | join kind=inner R on k";
        var sql = converter.Convert(kql);
        Assert.Contains("IS NOT DISTINCT FROM", sql);

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var rows = new List<(long? k, long v, long? k1, long w)>();
        while (reader.Read())
            rows.Add((
                reader.IsDBNull(0) ? (long?)null : reader.GetInt64(0),
                reader.GetInt64(1),
                reader.IsDBNull(2) ? (long?)null : reader.GetInt64(2),
                reader.GetInt64(3)));
        Assert.Single(rows);
        Assert.Contains(rows, r => r.k == null && r.v == 20 && r.k1 == null && r.w == 200);
    }

    private static void CreateJoinTables(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE X (Key VARCHAR, Value1 BIGINT);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO X VALUES ('a',1),('b',2),('b',3),('c',4);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE TABLE Y (Key VARCHAR, Value2 BIGINT);";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Y VALUES ('b',10),('c',20),('c',30),('d',40);";
        cmd.ExecuteNonQuery();
    }
}
