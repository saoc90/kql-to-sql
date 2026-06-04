namespace KqlToSql.Fuzzer;

public sealed record SeedColumn(string Name, string KqlType);

public sealed record Seed(string Name, string Kql, IReadOnlyList<SeedColumn> Columns)
{
    public IEnumerable<SeedColumn> Of(params TypeClassFilter[] filters) =>
        Columns.Where(c => filters.Length == 0 || filters.Any(f => f(c.KqlType)));
}

public delegate bool TypeClassFilter(string kqlType);

/// <summary>
/// Canonical seed tables expressed as self-contained <c>datatable(...)</c> literals. Because the
/// data lives in the query text, Kustainer and the translator+DuckDB receive byte-identical input,
/// eliminating the data-parity problem for the bulk of the corpus.
/// </summary>
public static class Seeds
{
    public static readonly Seed Num = new("S_num",
        "datatable(i:int, l:long, r:real)[ " +
        "-5,-5,-5.5, " +
        "0,0,0.0, " +
        "3,2147483648,0.1, " +
        "7,100,3.25, " +
        "42,-9999999999,100000.5 ]",
        new[] { new SeedColumn("i", "int"), new SeedColumn("l", "long"), new SeedColumn("r", "real") });

    public static readonly Seed NumNull = new("S_numnull",
        "datatable(i:int, l:long, r:real)[ 1,1,1.5, int(null),long(null),real(null), 2,2,2.5, -3,-3,-3.5 ]",
        new[] { new SeedColumn("i", "int"), new SeedColumn("l", "long"), new SeedColumn("r", "real") });

    public static readonly Seed Str = new("S_str",
        "datatable(s:string, tag:string)[ " +
        "\"alpha\",\"x\", " +
        "\"Beta\",\"x\", " +
        "\"café\",\"y\", " +
        "\"\",\"z\", " +
        "\" spaced \",\"y\", " +
        "\"O'Brien\",\"x\", " +
        "\"where\",\"kw\", " +
        "\"MiXeD\",\"y\" ]",
        new[] { new SeedColumn("s", "string"), new SeedColumn("tag", "string") });

    // Note: KQL datatable has no literal null-string ("" is the closest); null strings are exercised
    // via computed expressions (iif/tostring(dynamic(null))) in the adversarial tier instead.
    public static readonly Seed StrNull = new("S_strnull",
        "datatable(s:string, tag:string)[ \"a\",\"x\", \"\",\"y\", \"c\",\"z\", \"b\",\"x\" ]",
        new[] { new SeedColumn("s", "string"), new SeedColumn("tag", "string") });

    public static readonly Seed Dt = new("S_dt",
        "datatable(t:datetime, span:timespan)[ " +
        "datetime(2007-01-01),1d, " +
        "datetime(1950-06-15 13:45:00),2h, " +
        "datetime(2030-12-31 23:59:59),-1h, " +
        "datetime(2020-02-29 00:00:01),30m ]",
        new[] { new SeedColumn("t", "datetime"), new SeedColumn("span", "timespan") });

    public static readonly Seed Dyn = new("S_dyn",
        "datatable(d:dynamic)[ " +
        "dynamic({\"a\":1,\"b\":[1,2,3]}), " +
        "dynamic([10,20,30]), " +
        "dynamic({\"nested\":{\"x\":\"y\"}}), " +
        "dynamic(null) ]",
        new[] { new SeedColumn("d", "dynamic") });

    public static readonly Seed Bool = new("S_bool",
        "datatable(b:bool, n:int)[ true,1, false,2, true,3, false,4 ]",
        new[] { new SeedColumn("b", "bool"), new SeedColumn("n", "int") });

    public static readonly Seed Dup = new("S_dup",
        "datatable(k:long, v:long)[ 1,10, 1,20, 2,30, 2,30, 3,40 ]",
        new[] { new SeedColumn("k", "long"), new SeedColumn("v", "long") });

    public static readonly Seed Empty = new("S_empty",
        "datatable(a:long, b:string)[]",
        new[] { new SeedColumn("a", "long"), new SeedColumn("b", "string") });

    public static readonly Seed Wide = new("S_wide",
        "datatable(id:long, name:string, score:real, active:bool, ts:datetime, cat:string)[ " +
        "1,\"a\",1.5,true,datetime(2020-01-01),\"p\", " +
        "2,\"b\",2.5,false,datetime(2021-06-01),\"q\", " +
        "3,\"a\",3.5,true,datetime(2022-12-31),\"p\", " +
        "4,\"c\",4.5,false,datetime(2023-03-15),\"q\" ]",
        new[]
        {
            new SeedColumn("id", "long"), new SeedColumn("name", "string"), new SeedColumn("score", "real"),
            new SeedColumn("active", "bool"), new SeedColumn("ts", "datetime"), new SeedColumn("cat", "string"),
        });

    public static readonly IReadOnlyList<Seed> All = new[]
    {
        Num, NumNull, Str, StrNull, Dt, Dyn, Bool, Dup, Empty, Wide,
    };

    public static Seed ByName(string name) => All.First(s => s.Name == name);

    // Type-class filters for the generator.
    public static readonly TypeClassFilter Numeric = t => t is "int" or "long" or "real" or "decimal";
    public static readonly TypeClassFilter Stringy = t => t is "string";
    public static readonly TypeClassFilter Temporal = t => t is "datetime" or "timespan";
    public static readonly TypeClassFilter Dynamicish = t => t is "dynamic";
}
