using System;
using System.Collections.Generic;

namespace KustoApi.Models;

public record DataSetHeader(string Version, bool IsProgressive);

public record Column(string ColumnName, string ColumnType);

public record DataTable(int TableId, string TableKind, string TableName, List<Column> Columns, List<object[]> Rows);

public record DataSetCompletion(bool HasErrors, bool Cancelled, object? OneApiErrors);

