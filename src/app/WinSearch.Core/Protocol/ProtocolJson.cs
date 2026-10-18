using System.Text.Json;
using System.Text.Json.Serialization;

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
        };
        options.Converters.Add(new OutcomeEnumConverter());
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower));
        return options;
    }
}
