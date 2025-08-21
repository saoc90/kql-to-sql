namespace KustoApi.Models;

public class TableResult
{
    public IEnumerable<Table> Table { get; set; } = [];
}

public class Table
{
    public string TableName { get; set; } = string.Empty;
    public List<Column> Columns { get; set; } = new List<Column>();
    public List<List<object>> Rows { get; set; } = new List<List<object>>();
}
