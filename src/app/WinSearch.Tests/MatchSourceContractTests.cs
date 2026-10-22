using System.Text.Json;
using WinSearch.Core.Protocol;
using Xunit;

namespace WinSearch.Tests;

public class MatchSourceContractTests
{
    [Theory]
    [InlineData("filename")]
    [InlineData("content")]
    [InlineData("semantic")]
    [InlineData("related")]
    public void ClosedMatchSourceRoundTrips(string code)
    {
        var result = JsonSerializer.Deserialize<SearchResult>("{\"match_source\":\"" + code + "\"}", ProtocolJson.Options)!;
        using var encoded = JsonDocument.Parse(JsonSerializer.Serialize(result, ProtocolJson.Options));
        Assert.Equal(code, encoded.RootElement.GetProperty("match_source").GetString());
    }

    [Theory]
    [InlineData("\"guessed\"")]
    [InlineData("\"Filename\"")]
    [InlineData("0")]
    public void RejectsUnknownAndAliasedMatchSources(string code)
        => Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<SearchResult>("{\"match_source\":" + code + "}", ProtocolJson.Options));

    [Fact]
    public void LegacyAbsenceStaysAbsent()
    {
        var result = JsonSerializer.Deserialize<SearchResult>("{}", ProtocolJson.Options)!;
        Assert.Null(result.MatchSource);
        using var encoded = JsonDocument.Parse(JsonSerializer.Serialize(result, ProtocolJson.Options));
        Assert.False(encoded.RootElement.TryGetProperty("match_source", out _));
    }
}
