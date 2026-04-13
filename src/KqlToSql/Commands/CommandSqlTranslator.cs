using System;

namespace KqlToSql.Commands;

public class CommandSqlTranslator
{
    private readonly ExternalTableCommandHandler _externalTable;
    private readonly TableCommandHandler _table;
    private readonly FunctionCommandHandler _function;
    private readonly DataCommandHandler _data;
    private readonly DatabaseCommandHandler _database;

    public CommandSqlTranslator(KqlToSqlConverter converter)
    {
        _externalTable = new ExternalTableCommandHandler();
        _table = new TableCommandHandler(converter);
        _function = new FunctionCommandHandler(converter);
        _data = new DataCommandHandler(converter);
        _database = new DatabaseCommandHandler(converter);
    }

    public string Translate(string kqlText)
    {
        var text = kqlText.Trim();

        // Order matters: more specific prefixes first within each handler.
        // External tables before tables, as ".create external table" starts with ".create".
        if (_externalTable.TryTranslate(text, out var sql)) return sql;
        if (_table.TryTranslate(text, out sql)) return sql;
        if (_function.TryTranslate(text, out sql)) return sql;
        if (_data.TryTranslate(text, out sql)) return sql;
        if (_database.TryTranslate(text, out sql)) return sql;

        throw new NotSupportedException("Unsupported command");
    }
}
