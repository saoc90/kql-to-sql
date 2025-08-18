using System.Text.Json;

namespace KustoApi.Models;

public record QueryRequest(string Db, string Csl, JsonElement? Properties);

