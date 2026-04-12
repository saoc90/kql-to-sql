using System;
using System.Collections.Generic;
using DuckDB.NET.Data;
using KqlToSql;
using KqlToSql.Dialects;
using Xunit;

namespace KqlToSql.DuckDbExtension.Tests;

public class EvaluateIntegrationTests
{
    private readonly KqlToSqlConverter _converter = new(new DuckDbDialect());

    [Fact]
    public void Pivot_CountByState()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE Sales (Region VARCHAR, Product VARCHAR, Amount BIGINT);
            INSERT INTO Sales VALUES
                ('East', 'Widget', 100), ('East', 'Gadget', 200),
                ('West', 'Widget', 150), ('West', 'Gadget', 250),
                ('East', 'Widget', 50);";
        setup.ExecuteNonQuery();

        var kql = "Sales | evaluate pivot(Product, sum(Amount), Region)";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var results = new Dictionary<string, (long Widget, long Gadget)>();
        while (reader.Read())
        {
            var region = reader.GetString(reader.GetOrdinal("Region"));
            var widget = reader.GetInt64(reader.GetOrdinal("Widget"));
            var gadget = reader.GetInt64(reader.GetOrdinal("Gadget"));
            results[region] = (widget, gadget);
        }

        Assert.Equal(2, results.Count);
        Assert.Equal((150L, 200L), results["East"]);
        Assert.Equal((150L, 250L), results["West"]);
    }

    [Fact]
    public void Pivot_CountOnly()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE Events (State VARCHAR, EventType VARCHAR);
            INSERT INTO Events VALUES
                ('TX', 'Tornado'), ('TX', 'Flood'), ('TX', 'Tornado'),
                ('KS', 'Tornado'), ('KS', 'Hail');";
        setup.ExecuteNonQuery();

        var kql = "Events | evaluate pivot(EventType, count(), State)";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read()); // At least one row
        // Should have columns: State, Tornado, Flood, Hail
        var columns = new List<string>();
        for (int i = 0; i < reader.FieldCount; i++)
            columns.Add(reader.GetName(i));

        Assert.Contains("State", columns);
        Assert.Contains("Tornado", columns);
    }

    [Fact]
    public void Narrow_UnpivotsTable()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE T (Id BIGINT, A BIGINT, B BIGINT); INSERT INTO T VALUES (1, 10, 20);";
        setup.ExecuteNonQuery();

        var kql = "T | evaluate narrow()";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(3, rows); // Id, A, B → 3 rows
    }

    [Fact]
    public void EvaluatePivot_InPipeline()
    {
        DuckDbSetup.EnsureDuckDb();
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = @"
            CREATE TABLE Logs (Status VARCHAR, Server VARCHAR);
            INSERT INTO Logs VALUES
                ('200', 'A'), ('200', 'A'), ('200', 'B'),
                ('404', 'A'), ('500', 'B');";
        setup.ExecuteNonQuery();

        var kql = "Logs | evaluate pivot(Status, count(), Server)";
        var sql = _converter.Convert(kql);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        var columns = new List<string>();
        for (int i = 0; i < reader.FieldCount; i++)
            columns.Add(reader.GetName(i));
        Assert.Contains("Server", columns);
        Assert.Contains("200", columns);
    }
}
