using System.Text.Json;
using WinSearch.Core.Protocol;
using Xunit;

namespace WinSearch.Tests;

public class FileKindContractTests
{
    [Theory]
    [InlineData("PDF", false, FileKind.Pdf)]
    [InlineData("md", false, FileKind.Document)]
    [InlineData("ods", false, FileKind.Spreadsheet)]
    [InlineData("pptx", false, FileKind.Presentation)]
    [InlineData("kts", false, FileKind.Source)]
    [InlineData("png", false, FileKind.Image)]
    [InlineData("mkv", false, FileKind.Video)]
    [InlineData("wav", false, FileKind.Audio)]
    [InlineData("7Z", false, FileKind.Archive)]
    [InlineData("ps1", false, FileKind.Executable)]
    [InlineData("GLTF", false, FileKind.Model)]
    [InlineData("ipynb", false, FileKind.Unknown)]
    [InlineData("", false, FileKind.Unknown)]
    [InlineData("KT", false, FileKind.Unknown)]
    [InlineData("pdf", true, FileKind.Folder)]
    public void FileKindClassificationPreservesSourceExtensionsAndDirectoryPrecedence(string extension, bool directory, FileKind expected)
        => Assert.Equal(expected, FileKinds.ForExtension(extension, directory));

    [Theory]
    [InlineData("C:/Docs/.json", FileKind.Unknown, true)]
    [InlineData("C:/Docs/.config.json", FileKind.Source, true)]
    [InlineData("C:/Docs/.json", FileKind.Source, false)]
    [InlineData("", FileKind.Unknown, false)]
    public void FileKindReplyValidationPreservesDotfileAndEmptyPathSemantics(string path, FileKind selected, bool expected)
        => Assert.Equal(expected, FileKinds.Matches([selected], new() { Kind = ResultKind.File, Path = path }));

    [Fact]
    public void FileKindSelectionRoundTripsAllClosedCodesAndPreservesLegacyQueries()
    {
        const string kinds = """["folder","pdf","document","spreadsheet","presentation","source","image","video","audio","archive","executable","model","unknown"]""";
        var query = JsonSerializer.Deserialize<SearchQuery>("{\"text\":\"needle\",\"file_kinds\":" + kinds + "}", ProtocolJson.Options)!;
        using var encoded = JsonDocument.Parse(JsonSerializer.Serialize(query, ProtocolJson.Options));
        Assert.True(encoded.RootElement.TryGetProperty("file_kinds", out var actual));
        Assert.Equal(kinds, actual.GetRawText());
        var legacy = JsonSerializer.Deserialize<SearchQuery>("""{"text":"needle"}""", ProtocolJson.Options)!;
        using var encodedLegacy = JsonDocument.Parse(JsonSerializer.Serialize(legacy, ProtocolJson.Options));
        Assert.False(encodedLegacy.RootElement.TryGetProperty("file_kinds", out _));
        using var encodedRelated = JsonDocument.Parse(JsonSerializer.Serialize(new RelatedQuery(), ProtocolJson.Options));
        Assert.False(encodedRelated.RootElement.TryGetProperty("file_kinds", out _));
    }

    [Theory]
    [InlineData("[\"PDF\"]")]
    [InlineData("[\"notebook\"]")]
    [InlineData("[1]")]
    [InlineData("[\"1\"]")]
    public void FileKindSelectionRejectsUnknownCaseAliasedAndNumericCodes(string kinds)
    {
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<SearchQuery>(
            "{\"file_kinds\":" + kinds + "}", ProtocolJson.Options));
    }
}
