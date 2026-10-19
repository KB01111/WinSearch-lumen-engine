using System.Text.Json;
using System.Text.Json.Serialization;

namespace WinSearch.Core.Protocol;

public sealed class ProtocolCodeEnumConverter : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert) => typeToConvert == typeof(SearchDisposition)
        || typeToConvert == typeof(SemanticPhase) || typeToConvert == typeof(SemanticIssue) || typeToConvert == typeof(FileKind);
    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        => typeToConvert == typeof(SearchDisposition) ? new ClosedCodes<SearchDisposition>()
            : typeToConvert == typeof(SemanticPhase) ? new ClosedCodes<SemanticPhase>()
            : typeToConvert == typeof(SemanticIssue) ? new ClosedCodes<SemanticIssue>()
            : new ClosedCodes<FileKind>();

    private sealed class ClosedCodes<T> : JsonConverter<T> where T : struct, Enum
    {
        private static readonly Dictionary<string, T> Codes = Enum.GetValues<T>()
            .ToDictionary(value => JsonNamingPolicy.SnakeCaseLower.ConvertName(value.ToString()), StringComparer.Ordinal);
        public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => reader.TokenType == JsonTokenType.String && Codes.TryGetValue(reader.GetString()!, out var value)
                ? value : throw new JsonException("Invalid protocol code");
        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        {
            var code = JsonNamingPolicy.SnakeCaseLower.ConvertName(value.ToString());
            if (!Codes.ContainsKey(code)) throw new JsonException("Invalid protocol code");
            writer.WriteStringValue(code);
        }
    }
}

public enum SearchDisposition { Complete, Superseded, Canceled, Failed }

public enum SemanticPhase { Disabled, Ready, Degraded }

public enum SemanticIssue { ModelUnavailable, NoVectors, EmbeddingFailed, RetrievalFailed, IndexChanged }

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class SemanticOutcome
{
    public SemanticPhase? Phase { get; set; }
    public SemanticIssue? Reason { get; set; }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class SearchOutcome
{
    public SearchDisposition? Disposition { get; set; }
    public SemanticOutcome? Semantic { get; set; }
    public bool ValidFor(bool done, bool emptyBatch, bool requestedSemantic)
    {
        if (!done) return false;
        return Disposition switch {
            SearchDisposition.Complete => Semantic?.Phase switch {
                SemanticPhase.Disabled => Semantic.Reason is null,
                SemanticPhase.Ready => requestedSemantic && Semantic.Reason is null,
                SemanticPhase.Degraded => requestedSemantic && Semantic.Reason is SemanticIssue.ModelUnavailable
                    or SemanticIssue.NoVectors or SemanticIssue.EmbeddingFailed or SemanticIssue.RetrievalFailed or SemanticIssue.IndexChanged,
                _ => false },
            SearchDisposition.Superseded or SearchDisposition.Canceled or SearchDisposition.Failed => emptyBatch && Semantic is null,
            _ => false };
    }
}
