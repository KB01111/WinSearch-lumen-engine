using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace WinSearch.Core.Protocol;

/// <summary>
/// Wire format shared with the Rust engine: length-prefixed JSON frames whose
/// payloads use snake_case names and string discriminators ("type", "kind"),
/// exactly matching the serde representation in ws-protocol.
/// </summary>
public static class ProtocolJson
{
    /// <summary>Must match PROTOCOL_VERSION in ws-protocol.</summary>
    public const uint Version = 1;

    public static readonly JsonSerializerOptions Options = CreateOptions();

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            PropertyNameCaseInsensitive = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            TypeInfoResolver = new DefaultJsonTypeInfoResolver { Modifiers = { typeInfo => {
                if (typeInfo.Type == typeof(SearchQuery) || typeInfo.Type == typeof(RelatedQuery))
                    typeInfo.Properties.Single(property => property.Name == "file_kinds").ShouldSerialize
                        = (_, value) => value is List<FileKind> { Count: > 0 };
            } } },
        };
        options.Converters.Add(new ProtocolCodeEnumConverter());
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower));
        return options;
    }
}
