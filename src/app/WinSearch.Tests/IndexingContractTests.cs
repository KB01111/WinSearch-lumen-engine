using System.Text.Json;
using WinSearch.Core.Protocol;
using Xunit;

namespace WinSearch.Tests;

public class IndexingContractTests
{
    [Fact]
    public void SearchOutcomesValidateTerminalStateAndRetainLegacyUnknown()
    {
        var legacy = JsonSerializer.Deserialize<ResultsMessage>("""{"id":1,"batch":[],"done":true}""", ProtocolJson.Options)!;
        Assert.Null(legacy.Outcome);
        var ready = new SearchOutcome { Disposition = SearchDisposition.Complete, Semantic = new() { Phase = SemanticPhase.Ready } };
        Assert.True(ready.ValidFor(true, true, true));
        Assert.False(ready.ValidFor(false, true, true));
        Assert.False(ready.ValidFor(true, true, false));
        foreach (var disposition in new[] { SearchDisposition.Superseded, SearchDisposition.Canceled, SearchDisposition.Failed })
        {
            var outcome = new SearchOutcome { Disposition = disposition };
            Assert.True(outcome.ValidFor(true, true, true));
            Assert.False(outcome.ValidFor(true, false, true));
            outcome.Semantic = new() { Phase = SemanticPhase.Disabled };
            Assert.False(outcome.ValidFor(true, true, true));
        }
        foreach (var semantic in new SemanticOutcome[] { new() { Phase = SemanticPhase.Degraded },
            new() { Phase = SemanticPhase.Disabled, Reason = SemanticIssue.NoVectors },
            new() { Phase = SemanticPhase.Ready, Reason = SemanticIssue.EmbeddingFailed }, new() })
            Assert.False(new SearchOutcome { Disposition = SearchDisposition.Complete, Semantic = semantic }.ValidFor(true, true, true));
        Assert.False(new SearchOutcome().ValidFor(true, true, true));
    }

    [Fact]
    public void SearchOutcomeEnumsRejectNumericUnknownAndPrivateReasonValues()
    {
        foreach (var json in new[] {
            """{"disposition":0,"semantic":{"phase":"disabled"}}""",
            """{"disposition":"complete","semantic":{"phase":0}}""",
            """{"disposition":"COMPLETE","semantic":{"phase":"ready"}}""",
            """{"disposition":"complete","semantic":{"phase":"Ready"}}""",
            """{"disposition":"complete","semantic":{"phase":"degraded","reason":"ModelUnavailable"}}""",
            """{"disposition":"complete","semantic":{"phase":"degraded","reason":"private worker exception"}}""",
            """{"disposition":"complete","semantic":{"phase":"ready"},"path":"C:/private"}""" })
            Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<SearchOutcome>(json, ProtocolJson.Options));
    }

    [Fact]
    public void BoundReferencesAndSourceHashesRoundTripWithoutChangingHitIds()
    {
        const string hitJson = """{"id":"code:42:1:true:needle","kind":"code","entry_reference":{"result_id":"c42","source_revision":"proof"}}""";
        var hit = JsonSerializer.Deserialize<SearchResult>(hitJson, ProtocolJson.Options)!;
        using var encodedHit = JsonDocument.Parse(JsonSerializer.Serialize(hit, ProtocolJson.Options));
        Assert.Equal("code:42:1:true:needle", encodedHit.RootElement.GetProperty("id").GetString());
        Assert.Equal("c42", encodedHit.RootElement.GetProperty("entry_reference").GetProperty("result_id").GetString());
        const string metadataJson = """{"result_id":"c42","kind":"code","index_revision":7,"source_revision":"proof","content_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""";
        var entry = JsonSerializer.Deserialize<ResolvedEntry>(metadataJson, ProtocolJson.Options)!;
        using var encodedEntry = JsonDocument.Parse(JsonSerializer.Serialize(entry, ProtocolJson.Options));
        Assert.Equal(new string('a', 64), encodedEntry.RootElement.GetProperty("content_sha256").GetString());
        using var legacyHit = JsonDocument.Parse(JsonSerializer.Serialize(new SearchResult(), ProtocolJson.Options));
        using var legacyEntry = JsonDocument.Parse(JsonSerializer.Serialize(new ResolvedEntry(), ProtocolJson.Options));
        Assert.False(legacyHit.RootElement.TryGetProperty("entry_reference", out _));
        Assert.False(legacyEntry.RootElement.TryGetProperty("content_sha256", out _));
    }

    [Fact]
    public void PreviewCodecPreservesSourceLocationAndUnknownFullCoverage()
    {
        const string json = """
            {"type":"entry_preview","id":17,"preview":{
              "entry":{"result_id":"e12","root_path":"C:/Docs","path":"C:/Docs/source.pdf",
                "name":"source.pdf","kind":"document","size_bytes":19,"modified_unix_seconds":1,
                "index_revision":3,"source_revision":"current-proof"},
              "state":"ready","parts":[{"kind":"pdf","text":"actual eighth page","location":{"kind":"page","page":8}}],
              "truncated":false,"extraction":[{"kind":"pdf","complete_within_limit":true,
                "source_units":null,"indexed_units":8,"capped":null}]}}
            """;
        var message = JsonSerializer.Deserialize<ServerMessage>(json, ProtocolJson.Options)!;
        using var encoded = JsonDocument.Parse(JsonSerializer.Serialize(message, ProtocolJson.Options));
        var preview = encoded.RootElement.GetProperty("preview");
        Assert.Equal("current-proof", preview.GetProperty("entry").GetProperty("source_revision").GetString());
        Assert.Equal(8, preview.GetProperty("parts")[0].GetProperty("location").GetProperty("page").GetInt32());
        var extraction = preview.GetProperty("extraction")[0];
        Assert.True(extraction.GetProperty("complete_within_limit").GetBoolean());
        Assert.True(!extraction.TryGetProperty("source_units", out var source) || source.ValueKind == JsonValueKind.Null);
        Assert.True(!extraction.TryGetProperty("capped", out var capped) || capped.ValueKind == JsonValueKind.Null);
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<ClientMessage>(
            """{"type":"preview_entry","id":1,"request":{"reference":{"result_id":"e12"},"path":"C:/outside"}}""", ProtocolJson.Options));
    }

    [Fact]
    public void NativeResolvedEntryRetainsCurrentSourceAndContentRevisionSeparately()
    {
        const string json = """
            {"type":"resolved_entry","id":41,"entry":{"result_id":"e23","root_path":"C:/fixture",
            "path":"C:/fixture/report.pdf","name":"report.pdf","kind":"document","size_bytes":1234,
            "modified_unix_seconds":1700000000,"index_revision":7,"source_revision":"opaque-native-source-proof"}}
            """;
        var message = JsonSerializer.Deserialize<ServerMessage>(json, ProtocolJson.Options)!;
        using var encoded = JsonDocument.Parse(JsonSerializer.Serialize(message, ProtocolJson.Options));
        Assert.Equal("resolved_entry", encoded.RootElement.GetProperty("type").GetString());
        var entry = encoded.RootElement.GetProperty("entry");
        Assert.Equal("opaque-native-source-proof", entry.GetProperty("source_revision").GetString());
        Assert.Equal(7, entry.GetProperty("index_revision").GetInt64());
        Assert.Equal(1234UL, entry.GetProperty("size_bytes").GetUInt64());
    }

    [Fact]
    public void EntryReferenceAcceptsIndexedIdentityAndRejectsCallerPaths()
    {
        const string valid = """{"type":"resolve_entry","id":1,"reference":{"result_id":"e23"}}""";
        var request = Assert.IsType<ResolveEntryMessage>(JsonSerializer.Deserialize<ClientMessage>(valid, ProtocolJson.Options));
        Assert.Equal("e23", request.Reference.ResultId);
        const string invalid = """{"type":"resolve_entry","id":1,"reference":{"result_id":"e23","path":"C:/outside/secret.txt"}}""";
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<ClientMessage>(invalid, ProtocolJson.Options));
    }

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
