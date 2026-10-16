using System.Text.Json;
using WinSearch.Core.Protocol;
using Xunit;

namespace WinSearch.Tests;

public class IndexingContractTests
{
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
