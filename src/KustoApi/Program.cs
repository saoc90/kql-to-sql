using System.Text.Json;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json.Serialization;
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
        builder.Services.AddCors(options =>
        {
            options.AddPolicy("MyAllowAllHeadersPolicy",
                policy =>
                {
                    policy
                        .AllowAnyMethod()
                        .AllowAnyOrigin()
                        .AllowAnyHeader();
                });
        });
        builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.PropertyNamingPolicy = null);

        var app = builder.Build();
        app.UseCors("MyAllowAllHeadersPolicy");

        app.MapPost("/v2/rest/query", (QueryRequest request, KqlToSqlConverter converter, StormEventsDatabase database) =>
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

                // Detect progressive mode request
                var progressive = false;
                if (request.Properties is JsonElement props && props.ValueKind == JsonValueKind.Object)
                {
                    if (props.TryGetProperty("Options", out var options) && options.ValueKind == JsonValueKind.Object)
                    {
                        if (options.TryGetProperty("results_progressive_enabled", out var prog) && prog.ValueKind == JsonValueKind.True)
                        {
                            progressive = true;
                        }
                    }
                }

                if (progressive)
                {
                    var frames = new object[]
                    {
                        new { FrameType = "DataSetHeader", IsProgressive = true, Version = "v2.0", IsFragmented = false, ErrorReportingPlacement = "InData" },
                        new
                        {
                            FrameType = "TableHeader",
                            TableId = 1,
                            TableKind = "PrimaryResult",
                            TableName = "PrimaryResult",
                            Columns = columns.Select(c => new { c.ColumnName, c.ColumnType }).ToList()
                        },
                        new
                        {
                            FrameType = "TableFragment",
                            TableId = 1,
                            FieldCount = columns.Count,
                            TableFragmentType = "DataAppend",
                            Rows = rows
                        },
                        new { FrameType = "TableCompletion", TableId = 1, RowCount = rows.Count },
                        new { FrameType = "DataSetCompletion", HasErrors = false, Cancelled = false }
                    };
                    return Results.Json(frames, contentType: "application/json; charset=utf-8");
                }
                else
                {
                    // Build QueryProperties frame (TableId 0)
                    var queryProps = new
                    {
                        FrameType = "DataTable",
                        TableId = 0,
                        TableKind = "QueryProperties",
                        TableName = "@ExtendedProperties",
                        Columns = new[]
                        {
                            new { ColumnName = "TableId", ColumnType = "int" },
                            new { ColumnName = "Key", ColumnType = "string" },
                            new { ColumnName = "Value", ColumnType = "dynamic" }
                        },
                        Rows = new object[]
                        {
                            new object[]
                            {
                                1,
                                "Visualization",
                                "{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}"
                            }
                        }
                    };

                    // Build PrimaryResult (TableId 1)
                    var primary = new
                    {
                        FrameType = "DataTable",
                        TableId = 1,
                        TableKind = "PrimaryResult",
                        TableName = "PrimaryResult",
                        Columns = columns,
                        Rows = rows
                    };

                    // Build QueryCompletionInformation (TableId 2)
                    var qci = new
                    {
                        FrameType = "DataTable",
                        TableId = 2,
                        TableKind = "QueryCompletionInformation",
                        TableName = "QueryCompletionInformation",
                        Columns = new[]
                        {
                            new { ColumnName = "Timestamp", ColumnType = "datetime" },
                            new { ColumnName = "ClientRequestId", ColumnType = "string" },
                            new { ColumnName = "ActivityId", ColumnType = "guid" },
                            new { ColumnName = "SubActivityId", ColumnType = "guid" },
                            new { ColumnName = "ParentActivityId", ColumnType = "guid" },
                            new { ColumnName = "Level", ColumnType = "int" },
                            new { ColumnName = "LevelName", ColumnType = "string" },
                            new { ColumnName = "StatusCode", ColumnType = "int" },
                            new { ColumnName = "StatusCodeName", ColumnType = "string" },
                            new { ColumnName = "EventType", ColumnType = "int" },
                            new { ColumnName = "EventTypeName", ColumnType = "string" },
                            new { ColumnName = "Payload", ColumnType = "string" }
                        },
                        Rows = new object[]
                        {
                            new object[]
                            {
                                DateTime.UtcNow.ToString("o"),
                                $"Kusto.Web.KWE.Query;{Guid.NewGuid()};{Guid.NewGuid()}",
                                Guid.NewGuid().ToString(),
                                Guid.NewGuid().ToString(),
                                Guid.NewGuid().ToString(),
                                4,
                                "Info",
                                0,
                                "S_OK (0)",
                                4,
                                "QueryInfo",
                                "{\"Count\":2,\"Text\":\"Query completed successfully\"}"
                            },
                            new object[]
                            {
                                DateTime.UtcNow.ToString("o"),
                                $"Kusto.Web.KWE.Query;{Guid.NewGuid()};{Guid.NewGuid()}",
                                Guid.NewGuid().ToString(),
                                Guid.NewGuid().ToString(),
                                Guid.NewGuid().ToString(),
                                5,
                                "WorkloadGroup",
                                0,
                                "S_OK (0)",
                                0,
                                "QueryResourceConsumption",
                                "{\"Count\":1,\"Text\":\"default\"}"
                            }
                        }
                    };

                    object[] frames =
                    {
                        new { FrameType = "DataSetHeader", IsProgressive = false, Version = "v2.0", IsFragmented = false, ErrorReportingPlacement = "InData" },
                        queryProps,
                        primary,
                        qci,
                        new { FrameType = "DataSetCompletion", HasErrors = false, Cancelled = false }
                    };

                    return Results.Json(frames, contentType: "application/json; charset=utf-8");
                }
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
                return Results.Json(error, statusCode: 400, contentType: "application/json; charset=utf-8");
            }
        });

        app.MapPost("/v1/rest/query", (QueryRequest request, KqlToSqlConverter converter, StormEventsDatabase database) =>
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

                return Results.Json(result, contentType: "application/json; charset=utf-8");
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
                return Results.Json(error, statusCode: 400, contentType: "application/json; charset=utf-8");
            }
        });

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
            else if(csl.StartsWith(".create table", StringComparison.InvariantCultureIgnoreCase) && csl.Contains("if not exists", StringComparison.InvariantCultureIgnoreCase))
            {
                var match = Regex.Match(csl, @"\.create\s+table\s+(\w+)\s*\((.+)\)\s*if not exists", RegexOptions.IgnoreCase);
                if (!match.Success)
                {
                    var error = new { error = new { code = "BadRequest", message = "Invalid create table syntax" } };
                    return Results.Json(error, statusCode: 400);
                }

                var tableName = match.Groups[1].Value;
                var cols = match.Groups[2].Value.Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(c => c.Split(':', StringSplitOptions.RemoveEmptyEntries))
                    .Select(p => $"{p[0].Trim()} {MapKustoType(p[1].Trim())}");

                cmd.CommandText = $"CREATE TABLE IF NOT EXISTS {tableName} ({string.Join(",", cols)})";
                cmd.ExecuteNonQuery();

                var result = new { Tables = Array.Empty<object>() };
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
            else if (csl.StartsWith(".show cluster monitoring", StringComparison.OrdinalIgnoreCase))
            {
                var result = new
                {
                    Tables = new[]
                    {
                        new
                        {
                            TableName = "ClusterMonitoring",
                            Columns = new[]
                            {
                                new { ColumnName = "KustoAccount", ColumnType = "string" },
                                new { ColumnName = "ClusterAlias", ColumnType = "string" },
                                new { ColumnName = "GenevaMonitoringAccount", ColumnType = "string" },
                                new { ColumnName = "DataCenter", ColumnType = "string" },
                                new { ColumnName = "CloudName", ColumnType = "string" },
                                new { ColumnName = "CloudResourceId", ColumnType = "string" },
                                new { ColumnName = "VirtualClusterName", ColumnType = "string" }
                            },
                            Rows = new[]
                            {
                                new object[] { "N/A", "N/A", "N/A", "N/A", "DevPublicCloud", "N/A", "N/A" }
                            }
                        }
                    }
                };

                return Results.Json(result);
            }
            else if (csl.StartsWith(".show databases", StringComparison.InvariantCultureIgnoreCase) &&
                     csl.Contains("as json", StringComparison.InvariantCultureIgnoreCase))
            {
                var dbName = string.IsNullOrWhiteSpace(request.Db) ? "NetDefaultDB" : request.Db;

                // Collect schema information from DuckDB
                cmd.CommandText = "SELECT table_name, column_name, data_type, ordinal_position FROM information_schema.columns WHERE table_schema='main' ORDER BY table_name, ordinal_position";
                using var reader = cmd.ExecuteReader();

                var tables = new Dictionary<string, List<object>>(StringComparer.OrdinalIgnoreCase);
                while (reader.Read())
                {
                    var table = reader.GetString(0);
                    var column = reader.GetString(1);
                    var duckType = reader.GetString(2);

                    var (systemType, cslType) = MapDuckTypeToSystemAndCsl(duckType);

                    if (!tables.TryGetValue(table, out var cols))
                    {
                        cols = new List<object>();
                        tables[table] = cols;
                    }

                    cols.Add(new
                    {
                        Name = column,
                        Type = systemType,
                        CslType = cslType
                    });
                }

                // Build Tables object map
                var tablesMap = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var kvp in tables)
                {
                    tablesMap[kvp.Key] = new
                    {
                        Name = kvp.Key,
                        OrderedColumns = kvp.Value
                    };
                }

                var dbObject = new
                {
                    Name = dbName,
                    Tables = tablesMap,
                    MajorVersion = 2,
                    MinorVersion = 0,
                    Functions = new { },
                    DatabaseAccessMode = "ReadWrite",
                    ExternalTables = new { },
                    MaterializedViews = new { },
                    EntityGroups = new { },
                    Graphs = new { },
                    StoredQueryResults = new { }
                };

                var result = new
                {
                    Databases = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                    {
                        [dbName] = dbObject
                    }
                };

                // Return the JSON object directly, as expected by the client/tests
                return Results.Json(result);
            }
            else if (string.Equals(csl, ".show databases", StringComparison.OrdinalIgnoreCase))
            {
                var dbName = string.IsNullOrWhiteSpace(request.Db) ? "NetDefaultDB" : request.Db;
                var row = new object[]
                {
                    dbName,                                                   // DatabaseName
                    $"/kusto/tmp/Kusto.Personal/{dbName}",                  // PersistentStorage
                    "v1.0",                                                // Version
                    "TRUE",                                                // IsCurrent
                    "ReadWrite",                                           // DatabaseAccessMode
                    string.Empty,                                            // PrettyName
                    string.Empty,                                            // ReservedSlot1
                    Guid.NewGuid().ToString(),                               // DatabaseId
                    string.Empty,                                            // InTransitionTo
                    string.Empty                                             // SuspensionState
                };

                var result = new
                {
                    Tables = new[]
                    {
                        new
                        {
                            TableName = "Databases",
                            Columns = new[]
                            {
                                new { ColumnName = "DatabaseName", ColumnType = "string" },
                                new { ColumnName = "PersistentStorage", ColumnType = "string" },
                                new { ColumnName = "Version", ColumnType = "string" },
                                new { ColumnName = "IsCurrent", ColumnType = "string" },
                                new { ColumnName = "DatabaseAccessMode", ColumnType = "string" },
                                new { ColumnName = "PrettyName", ColumnType = "string" },
                                new { ColumnName = "ReservedSlot1", ColumnType = "string" },
                                new { ColumnName = "DatabaseId", ColumnType = "string" },
                                new { ColumnName = "InTransitionTo", ColumnType = "string" },
                                new { ColumnName = "SuspensionState", ColumnType = "string" }
                            },
                            Rows = new[] { row }
                        }
                    }
                };

                return Results.Json(result);
            }
            else if (csl.StartsWith(".show version", StringComparison.OrdinalIgnoreCase))
            {
                var result = new
                {
                    Tables = new[]
                    {
                        new
                        {
                            TableName = "Version",
                            Columns = new[]
                            {
                                new { ColumnName = "BuildVersion", ColumnType = "string" },
                                new { ColumnName = "BuildTime", ColumnType = "string" },
                                new { ColumnName = "ServiceType", ColumnType = "string" },
                                new { ColumnName = "ProductVersion", ColumnType = "string" },
                                new { ColumnName = "ServiceOffering", ColumnType = "string" }
                            },
                            Rows = new[]
                            {
                                new object[]
                                {
                                    "1.0.9318.23388",
                                    "2025-07-06 12:59:36.0000000",
                                    "Engine",
                                    "2025.07.06.1253-2528-4be5c04-master",
                                    string.Empty
                                }
                            }
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

        app.MapGet("/v1/rest/ping", (HttpContext ctx) =>
        {
            var ip = ctx.Connection.RemoteIpAddress;
            var clientIp = ip == null ? "::1" : ip.MapToIPv6().ToString();
            var result = new
            {
                ApplicationHealthState = "Healthy",
                ClientAddress = clientIp
            };
            return Results.Json(result);
        });

        app.MapGet("/v1/rest/auth/metadata", () =>
        {
            var metadata = new
            {
                AzureAD = (object?)null,
                dSTS = (object?)null,
                AzureSettings = new
                {
                    CloudName = (string?)null,
                    AzureRegion = (string?)null,
                    Classification = "External"
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

    private static (string SystemType, string CslType) MapDuckTypeToSystemAndCsl(string duckType)
    {
        var t = duckType.Trim().ToLowerInvariant();
        // Strip parameters like VARCHAR(100)
        var baseType = t.Split('(')[0];
        return baseType switch
        {
            "boolean" or "bool" => ("System.Boolean", "bool"),
            "tinyint" or "utinyint" or "smallint" or "usmallint" or "integer" or "int" => ("System.Int32", "int"),
            "bigint" or "ubigint" => ("System.Int64", "long"),
            "real" or "float" => ("System.Single", "real"),
            "double" or "double precision" or "decimal" or "numeric" => ("System.Double", "real"),
            "timestamp" or "datetime" or "timestamptz" => ("System.DateTime", "datetime"),
            "date" => ("System.DateTime", "datetime"),
            "time" => ("System.String", "string"),
            "uuid" => ("System.String", "string"),
            "blob" or "binary" => ("System.String", "string"),
            _ => ("System.String", "string"), // varchar, text, json, map, list, etc.
        };
    }
}
