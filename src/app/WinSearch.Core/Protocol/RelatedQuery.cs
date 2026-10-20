using System.Text.Json.Serialization;

namespace WinSearch.Core.Protocol;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class RelatedQuery
{
    public EntryReference Source { get; set; } = new();
    public uint Limit { get; set; } = 20;
    public List<ResultKind> Categories { get; set; } = new();
    public List<string> Extensions { get; set; } = new();
    public List<FileKind> FileKinds { get; set; } = new();
}
