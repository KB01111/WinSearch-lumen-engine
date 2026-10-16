using System.Text.Json;
using WinSearch.Core.Protocol;
using Xunit;

namespace WinSearch.Tests;

public class IndexingContractTests
{
    [Fact]
    public void ExactMatchMomentsAreTypedAndOldResultsKeepDefaults()
    {
        var oldResult = JsonSerializer.Deserialize<SearchResult>("{\"id\":\"e1\",\"title\":\"old.txt\"}", ProtocolJson.Options)!;
        Assert.Null(oldResult.Excerpt);
        Assert.Empty(oldResult.MatchingMoments);
        var result = new SearchResult { Id = "e2", Excerpt = "invoice token", Location = new LineLocation { Line = 2 },
            MatchingMoments = [new() { Excerpt = "page token", Location = new PageLocation { Page = 9 } },
                new() { Excerpt = "time token", Location = new TimestampLocation { Seconds = 42, EndSeconds = 48 } }] };
        var json = JsonSerializer.Serialize(result, ProtocolJson.Options);
        Assert.Contains("matching_moments", json);
        var copy = JsonSerializer.Deserialize<SearchResult>(json, ProtocolJson.Options)!;
        Assert.Equal((uint)2, Assert.IsType<LineLocation>(copy.Location).Line);
        Assert.Equal((uint)9, Assert.IsType<PageLocation>(copy.MatchingMoments[0].Location).Page);
        Assert.Equal(42, Assert.IsType<TimestampLocation>(copy.MatchingMoments[1].Location).Seconds);
        Assert.Equal("invoice token", copy.Excerpt);
    }

    [Fact]
    public void IndexingContractUsesSnakeCaseAndKeepsDefaults()
    {
        ClientMessage message = new SetIndexingConfigMessage { Id = 42, Config = new IndexingConfig {
            Roots = [@"C:\Data"], CodeEnabled = true, CodeRoots = [@"D:\Code"],
            ExcludedPaths = [@"C:\Data\Skip"], ExcludedExtensions = ["pdf"],
            Mode = IndexingMode.Scheduled, HoursStart = 1320, HoursEnd = 360 } };
        var json = JsonSerializer.Serialize(message, ProtocolJson.Options);
        Assert.Contains("\"type\":\"set_indexing_config\"", json);
        Assert.Contains("\"code_enabled\":true", json);
        Assert.Contains("\"mode\":\"scheduled\"", json);
        var copy = Assert.IsType<SetIndexingConfigMessage>(JsonSerializer.Deserialize<ClientMessage>(json, ProtocolJson.Options));
        Assert.Equal((ushort)1320, copy.Config.HoursStart);
        Assert.Equal(message is SetIndexingConfigMessage m ? m.Config.Roots : [], copy.Config.Roots);
        var defaults = JsonSerializer.Deserialize<IndexingConfig>("{}", ProtocolJson.Options)!;
        Assert.Empty(defaults.Roots);
        Assert.False(defaults.CodeEnabled);
        Assert.True(defaults.RespectBatterySaver);
        Assert.Equal((ushort)60, defaults.HoursStart);
    }
}
