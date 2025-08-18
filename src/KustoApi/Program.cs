using System.Text.Json;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using KqlToSql;
using KustoApi.Models;
using KustoApi.Services;

var app = Program.BuildApp(args);
app.Run();

public partial class Program
{
    public static WebApplication BuildApp(string[] args, X509Certificate2? httpsCertificate = null)
    {
        var builder = WebApplication.CreateBuilder(args);

        if (httpsCertificate != null)
        {
            builder.WebHost.ConfigureKestrel(o =>
            {
                o.ConfigureHttpsDefaults(opt => opt.ServerCertificate = httpsCertificate);
            });
        }

        builder.Services.AddSingleton<KqlToSqlConverter>();
        builder.Services.AddSingleton<StormEventsDatabase>();
        builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.PropertyNamingPolicy = null);

        var app = builder.Build();

        var queryHandlerV2 = (QueryRequest request, KqlToSqlConverter converter, StormEventsDatabase database) =>
        {
            try
            {
                using var conn = database.GetConnection();
                var sql = converter.Convert(request.Csl);
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                using var reader = cmd.ExecuteReader();

                var columns = new List<Column>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    columns.Add(new Column(reader.GetName(i), MapType(reader.GetFieldType(i))));
                }

                var rows = new List<object[]>();
                while (reader.Read())
                {
                    var values = new object[reader.FieldCount];
                    reader.GetValues(values);
                    rows.Add(values);
                }

                object[] frames =
                {
                    new DataSetHeader("v2.0", false),
                    new DataTable(0, "PrimaryResult", "PrimaryResult", columns, rows),
                    new DataSetCompletion(false, false, null)
                };

                return Results.Json(frames);
            }
            catch (Exception ex)
            {
                var error = new
                {
                    error = new
                    {
                        code = "General_BadRequest",
                        message = ex.Message
                    }
                };
                return Results.Json(error, statusCode: 400);
            }
        };

        var queryHandlerV1 = (QueryRequest request, KqlToSqlConverter converter, StormEventsDatabase database) =>
        {
            try
            {
                using var conn = database.GetConnection();
                var sql = converter.Convert(request.Csl);
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                using var reader = cmd.ExecuteReader();

                var columns = new List<Column>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    columns.Add(new Column(reader.GetName(i), MapType(reader.GetFieldType(i))));
                }

                var rows = new List<object[]>();
                while (reader.Read())
                {
                    var values = new object[reader.FieldCount];
                    reader.GetValues(values);
                    rows.Add(values);
                }

                var result = new
                {
                    Tables = new[]
                    {
                        new
                        {
                            TableName = "PrimaryResult",
                            Columns = columns,
                            Rows = rows
                        }
                    }
                };

                return Results.Json(result);
            }
            catch (Exception ex)
            {
                var error = new
                {
                    error = new
                    {
                        code = "General_BadRequest",
                        message = ex.Message
                    }
                };
                return Results.Json(error, statusCode: 400);
            }
        };

        app.MapPost("/v2/rest/query", queryHandlerV2);
        app.MapPost("/v1/rest/query", queryHandlerV1);

        app.MapPost("/v1/rest/mgmt", (QueryRequest request, StormEventsDatabase database) =>
        {
            using var conn = database.GetConnection();
            using var cmd = conn.CreateCommand();
            var csl = request.Csl.Trim();

            if (string.Equals(csl, ".show tables", StringComparison.OrdinalIgnoreCase))
            {
                cmd.CommandText = "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name";
                using var reader = cmd.ExecuteReader();
                var rows = new List<object[]>();
                while (reader.Read())
                {
                    rows.Add(new object[] { reader.GetString(0) });
                }

                var result = new
                {
                    Tables = new[]
                    {
                        new
                        {
                            TableName = "Tables",
                            Columns = new[] { new { ColumnName = "TableName", ColumnType = "string" } },
                            Rows = rows
                        }
                    }
                };

                return Results.Json(result);
            }
            else if (csl.StartsWith(".show table", StringComparison.OrdinalIgnoreCase) && csl.EndsWith("schema", StringComparison.OrdinalIgnoreCase))
            {
                var parts = csl.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var tableName = parts[2];
                cmd.CommandText = $"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='main' AND table_name='{tableName}' ORDER BY ordinal_position";
                using var reader = cmd.ExecuteReader();
                var rows = new List<object[]>();
                while (reader.Read())
                {
                    rows.Add(new object[] { reader.GetString(0), reader.GetString(1) });
                }

                var result = new
                {
                    Tables = new[]
                    {
                        new
                        {
                            TableName = "TableSchema",
                            Columns = new[]
                            {
                                new { ColumnName = "ColumnName", ColumnType = "string" },
                                new { ColumnName = "DataType", ColumnType = "string" }
                            },
                            Rows = rows
                        }
                    }
                };

                return Results.Json(result);
            }
            else if (csl.StartsWith(".create table", StringComparison.OrdinalIgnoreCase))
            {
                var match = Regex.Match(csl, @"\.create\s+table\s+(\w+)\s*\((.+)\)", RegexOptions.IgnoreCase);
                if (!match.Success)
                {
                    var error = new { error = new { code = "BadRequest", message = "Invalid create table syntax" } };
                    return Results.Json(error, statusCode: 400);
                }

                var tableName = match.Groups[1].Value;
                var cols = match.Groups[2].Value.Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(c => c.Split(':', StringSplitOptions.RemoveEmptyEntries))
                    .Select(p => $"{p[0].Trim()} {MapKustoType(p[1].Trim())}");

                cmd.CommandText = $"CREATE TABLE {tableName} ({string.Join(",", cols)})";
                cmd.ExecuteNonQuery();

                var result = new { Tables = Array.Empty<object>() };
                return Results.Json(result);
            }
            else if (csl.StartsWith(".ingest inline into table", StringComparison.OrdinalIgnoreCase))
            {
                var prefix = ".ingest inline into table";
                var after = csl[prefix.Length..].Trim();
                var split = after.Split("<|", 2, StringSplitOptions.None);
                var tableName = split[0].Trim();
                var data = split.Length > 1 ? split[1] : string.Empty;

                var rows = data.Replace("\r", string.Empty).Split('\n', StringSplitOptions.RemoveEmptyEntries);
                foreach (var row in rows)
                {
                    var fields = row.Split(',', StringSplitOptions.None).Select(f => f.Trim()).ToArray();
                    var values = string.Join(",", fields.Select(SqlLiteral));
                    cmd.CommandText = $"INSERT INTO {tableName} VALUES ({values})";
                    cmd.ExecuteNonQuery();
                }

                var result = new
                {
                    Tables = new[]
                    {
                        new
                        {
                            TableName = "IngestionStatus",
                            Columns = new[] { new { ColumnName = "Status", ColumnType = "string" } },
                            Rows = new[] { new object[] { "Completed" } }
                        }
                    }
                };

                return Results.Json(result);
            }
            else
            {
                var error = new { error = new { code = "BadRequest", message = "Unsupported command" } };
                return Results.Json(error, statusCode: 400);
            }
        });

        app.MapGet("/v1/rest/metadata/{db}", (string db, StormEventsDatabase database) =>
        {
            using var conn = database.GetConnection();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema='main' ORDER BY table_name, ordinal_position";
            using var reader = cmd.ExecuteReader();

            var rows = new List<object[]>();
            while (reader.Read())
            {
                rows.Add(new object[] { reader.GetString(0), reader.GetString(1), reader.GetString(2) });
            }

            object[] frames =
            {
                new DataSetHeader("v2.0", false),
                new DataTable(0, "PrimaryResult", "Metadata", new List<Column>
                {
                    new("TableName", "string"),
                    new("ColumnName", "string"),
                    new("DataType", "string")
                }, rows),
                new DataSetCompletion(false, false, null)
            };

            return Results.Json(frames);
        });

        app.MapGet("/v1/rest/auth/metadata", () =>
        {
            var metadata = new
            {
                AzureAD = new
                {
                    LoginEndpoint = "https://login.microsoftonline.com",
                    Audience = "https://kusto.fake",
                    TenantId = "common"
                }
            };
            return Results.Json(metadata);
        });

        return app;
    }

    static string MapType(Type type) => Type.GetTypeCode(type) switch
    {
        TypeCode.Int32 => "int",
        TypeCode.Int64 => "long",
        TypeCode.Double => "real",
        TypeCode.Single => "real",
        TypeCode.String => "string",
        TypeCode.Boolean => "bool",
        TypeCode.DateTime => "datetime",
        _ => "dynamic",
    };

    static string MapKustoType(string type) => type.ToLower() switch
    {
        "string" => "TEXT",
        "int" => "INTEGER",
        "long" => "BIGINT",
        "datetime" => "TIMESTAMP",
        "bool" => "BOOLEAN",
        "real" => "DOUBLE",
        "double" => "DOUBLE",
        _ => "TEXT",
    };

    static string SqlLiteral(string value) => int.TryParse(value, out _) || double.TryParse(value, out _)
        ? value
        : $"'{value.Replace("'", "''")}'";
}
