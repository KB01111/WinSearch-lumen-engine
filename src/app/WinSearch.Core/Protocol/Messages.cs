using System.Text.Json.Serialization;

namespace WinSearch.Core.Protocol;

// ---------------------------------------------------------------------------
// Client -> engine
// ---------------------------------------------------------------------------

[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(HelloMessage), "hello")]
[JsonDerivedType(typeof(PingMessage), "ping")]
[JsonDerivedType(typeof(ResolveEntryMessage), "resolve_entry")]
[JsonDerivedType(typeof(PreviewEntryMessage), "preview_entry")]
[JsonDerivedType(typeof(SearchMessage), "search")]
[JsonDerivedType(typeof(CancelMessage), "cancel")]
[JsonDerivedType(typeof(GetStatusMessage), "get_status")]
[JsonDerivedType(typeof(ModelDownloadStartMessage), "model_download_start")]
[JsonDerivedType(typeof(ModelDownloadCancelMessage), "model_download_cancel")]
[JsonDerivedType(typeof(ModelImportMessage), "model_import")]
[JsonDerivedType(typeof(ModelDeleteMessage), "model_delete")]
[JsonDerivedType(typeof(GetModelStatusMessage), "get_model_status")]
[JsonDerivedType(typeof(GetConfigMessage), "get_config")]
[JsonDerivedType(typeof(SetConfigMessage), "set_config")]
[JsonDerivedType(typeof(GetIndexingConfigMessage), "get_indexing_config")]
[JsonDerivedType(typeof(SetIndexingConfigMessage), "set_indexing_config")]
public abstract class ClientMessage;

public sealed class HelloMessage : ClientMessage
{
    public uint ProtocolVersion { get; set; } = ProtocolJson.Version;
    public string Client { get; set; } = "";
    public string ClientVersion { get; set; } = "";
}

public sealed class PingMessage : ClientMessage
{
    public ulong Id { get; set; }
}

public sealed class ResolveEntryMessage : ClientMessage
{
    public ulong Id { get; set; }
    public EntryReference Reference { get; set; } = new();
}

public sealed class PreviewEntryMessage : ClientMessage
{
    public ulong Id { get; set; }
    public PreviewRequest Request { get; set; } = new();
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class PreviewRequest
{
    public EntryReference Reference { get; set; } = new();
    public MatchLocation? Location { get; set; }
}

public enum PreviewState { Ready, NotIndexed, NoCachedText, LocationUnavailable }

public sealed class PreviewTextPart
{
    public string Kind { get; set; } = "";
    public string Text { get; set; } = "";
    public MatchLocation? Location { get; set; }
}

public sealed class PreviewExtraction
{
    public string Kind { get; set; } = "";
    public bool CompleteWithinLimit { get; set; }
    public ulong? SourceUnits { get; set; }
    public ulong? IndexedUnits { get; set; }
    public bool? Capped { get; set; }
}

public sealed class EntryPreview
{
    public ResolvedEntry Entry { get; set; } = new();
    public PreviewState State { get; set; }
    public List<PreviewTextPart> Parts { get; set; } = new();
    public bool Truncated { get; set; }
    public List<PreviewExtraction> Extraction { get; set; } = new();
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class EntryReference
{
    public string ResultId { get; set; } = "";
    public string? SourceRevision { get; set; }
}

public sealed class ResolvedEntry
{
    public string ResultId { get; set; } = "";
    public string RootPath { get; set; } = "";
    public string Path { get; set; } = "";
    public string Name { get; set; } = "";
    public ResultKind Kind { get; set; }
    public ulong SizeBytes { get; set; }
    public long? ModifiedUnixSeconds { get; set; }
    public long? IndexRevision { get; set; }
    /// <summary>Opaque metadata/identity and policy proof, never a content hash.</summary>
    public string SourceRevision { get; set; } = "";
    public string? ContentSha256 { get; set; }
}

public sealed class SearchMessage : ClientMessage
{
    public ulong Id { get; set; }
    public SearchQuery Query { get; set; } = new();
}

public sealed class CancelMessage : ClientMessage
{
    public ulong Id { get; set; }
}

public sealed class GetStatusMessage : ClientMessage
{
    public ulong Id { get; set; }
}

public sealed class ModelDownloadStartMessage : ClientMessage
{
    public ulong Id { get; set; }
}

public sealed class ModelDownloadCancelMessage : ClientMessage
{
    public ulong Id { get; set; }
}

public sealed class ModelImportMessage : ClientMessage
{
    public ulong Id { get; set; }
    public string Path { get; set; } = "";
}

public sealed class ModelDeleteMessage : ClientMessage
{
    public ulong Id { get; set; }
}

public sealed class GetModelStatusMessage : ClientMessage
{
    public ulong Id { get; set; }
}

public sealed class GetConfigMessage : ClientMessage
{
    public ulong Id { get; set; }
}

public sealed class SetConfigMessage : ClientMessage
{
    public ulong Id { get; set; }
    public EngineConfig Config { get; set; } = new();
}

public sealed class GetIndexingConfigMessage : ClientMessage { public ulong Id { get; set; } }
public sealed class SetIndexingConfigMessage : ClientMessage
{
    public ulong Id { get; set; }
    public IndexingConfig Config { get; set; } = new();
}

// ---------------------------------------------------------------------------
// Engine -> client
// ---------------------------------------------------------------------------

[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(HelloAckMessage), "hello_ack")]
[JsonDerivedType(typeof(PongMessage), "pong")]
[JsonDerivedType(typeof(ResolvedEntryMessage), "resolved_entry")]
[JsonDerivedType(typeof(EntryPreviewMessage), "entry_preview")]
[JsonDerivedType(typeof(ResultsMessage), "results")]
[JsonDerivedType(typeof(StatusMessage), "status")]
[JsonDerivedType(typeof(ModelStatusMessage), "model_status")]
[JsonDerivedType(typeof(ConfigMessage), "config")]
[JsonDerivedType(typeof(IndexingConfigMessage), "indexing_config")]
[JsonDerivedType(typeof(ErrorMessage), "error")]
public abstract class ServerMessage;

public sealed class HelloAckMessage : ServerMessage
{
    public uint ProtocolVersion { get; set; }
    public string EngineVersion { get; set; } = "";
    public List<string> Features { get; set; } = new();
}

public sealed class PongMessage : ServerMessage
{
    public ulong Id { get; set; }
}

public sealed class ResolvedEntryMessage : ServerMessage
{
    public ulong Id { get; set; }
    public ResolvedEntry Entry { get; set; } = new();
}

public sealed class EntryPreviewMessage : ServerMessage
{
    public ulong Id { get; set; }
    public EntryPreview Preview { get; set; } = new();
}

public sealed class ResultsMessage : ServerMessage
{
    public ulong Id { get; set; }
    public List<SearchResult> Batch { get; set; } = new();
    public bool Done { get; set; }
}

public sealed class StatusMessage : ServerMessage
{
    /// <summary>Set when answering GetStatus; null for pushed updates.</summary>
    public ulong? Id { get; set; }
    public EngineStatus Status { get; set; } = new();
}

public sealed class ModelStatusMessage : ServerMessage
{
    /// <summary>Set when answering a model request; null for pushed progress updates.</summary>
    public ulong? Id { get; set; }
    public ModelStatus Status { get; set; } = new();
}

public sealed class ConfigMessage : ServerMessage
{
    /// <summary>Set when answering GetConfig/SetConfig.</summary>
    public ulong? Id { get; set; }
    public EngineConfig Config { get; set; } = new();
}

public sealed class IndexingConfigMessage : ServerMessage
{
    public ulong? Id { get; set; }
    public IndexingConfig Config { get; set; } = new();
}

public enum IndexingMode { Always, Scheduled, Manual }
public sealed class IndexingConfig
{
    public List<string> Roots { get; set; } = new();
    public List<string> ExcludedPaths { get; set; } = new();
    public List<string> ExcludedExtensions { get; set; } = new();
    public bool CodeEnabled { get; set; }
    public List<string> CodeRoots { get; set; } = new();
    public bool Paused { get; set; }
    public IndexingMode Mode { get; set; } = IndexingMode.Always;
    public bool IndexOnBattery { get; set; } = true;
    public bool RespectBatterySaver { get; set; } = true;
    public ushort HoursStart { get; set; } = 60;
    public ushort HoursEnd { get; set; } = 420;
}

public sealed class ErrorMessage : ServerMessage
{
    public ulong? Id { get; set; }
    public ErrorCode Code { get; set; }
    public string Message { get; set; } = "";
}

// ---------------------------------------------------------------------------
// Shared models
// ---------------------------------------------------------------------------

public enum SearchMode
{
    Auto,
    Apps,
    Files,
    Code,
}

public enum ResultKind
{
    Application,
    File,
    Folder,
    Image,
    Screenshot,
    Document,
    Audio,
    Video,
    Code,
    Calculator,
    Settings,
    Action,
}

public enum ErrorCode
{
    ProtocolVersionMismatch,
    InvalidRequest,
    Internal,
    NotReady,
    Cancelled,
    Unsupported,
}

public enum EngineRunState
{
    Idle,
    Indexing,
    Paused,
    Error,
}

public sealed class SearchQuery
{
    public string Text { get; set; } = "";
    public SearchMode Mode { get; set; } = SearchMode.Auto;
    public uint Limit { get; set; } = 20;
    public bool IncludeSemantic { get; set; }
    public List<ResultKind> Categories { get; set; } = new();
}

[JsonPolymorphic(TypeDiscriminatorPropertyName = "kind")]
[JsonDerivedType(typeof(PageLocation), "page")]
[JsonDerivedType(typeof(TimestampLocation), "timestamp")]
[JsonDerivedType(typeof(LineLocation), "line")]
[JsonDerivedType(typeof(NotebookLocation), "notebook")]
public abstract class MatchLocation;

public sealed class PageLocation : MatchLocation
{
    public uint Page { get; set; }
}

public sealed class TimestampLocation : MatchLocation
{
    public double Seconds { get; set; }
    public double EndSeconds { get; set; }
}

public sealed class LineLocation : MatchLocation
{
    public uint Line { get; set; }
}

public sealed class NotebookLocation : MatchLocation
{
    public uint CellOrdinal { get; set; }
    public uint Line { get; set; }
    public uint EndLine { get; set; }
}

public sealed class SearchResult
{
    public EntryReference? EntryReference { get; set; }
    public string Id { get; set; } = "";
    public ResultKind Kind { get; set; }
    public string Title { get; set; } = "";
    public string Subtitle { get; set; } = "";
    public string MatchExplanation { get; set; } = "";
    public double Score { get; set; }
    public string? Path { get; set; }
    public MatchLocation? Location { get; set; }
    public string? ThumbnailPath { get; set; }
    public string? Excerpt { get; set; }
    public List<MatchingMoment> MatchingMoments { get; set; } = new();
}

public sealed class MatchingMoment
{
    public string Excerpt { get; set; } = "";
    public MatchLocation? Location { get; set; }
    public string? ThumbnailPath { get; set; }
}

public sealed class EngineStatus
{
    public EngineRunState State { get; set; }
    public string CurrentActivity { get; set; } = "";
    public ulong FilesDiscovered { get; set; }
    public ulong FilesIndexed { get; set; }
    public ulong SegmentsIndexed { get; set; }
    public ulong QueueDepth { get; set; }
    public bool ModelLoaded { get; set; }
    public ulong IndexSizeBytes { get; set; }
}

public enum ModelState
{
    NotDownloaded,
    Downloading,
    Paused,
    Ready,
    Error,
}

public sealed class ModelStatus
{
    public ModelState State { get; set; }
    public ulong Bytes { get; set; }
    public ulong TotalBytes { get; set; }
    public string? Error { get; set; }

    /// <summary>Fraction downloaded in [0, 1]; 0.0 when <see cref="TotalBytes"/> is zero.</summary>
    public double Fraction => TotalBytes == 0 ? 0 : (double)Bytes / TotalBytes;
}

/// <summary>
/// Engine search policy (mirrors the Rust EngineConfig verbatim).
/// PreferredBackend is a plain string on the wire (canonical
/// "auto|cuda|vulkan|cpu"); the engine refuses unknown spellings with
/// invalid_request at set_config time.
/// </summary>
public sealed class EngineConfig
{
    public bool SemanticEnabled { get; set; } = true;
    public string PreferredBackend { get; set; } = "auto";
}

/// <summary>One streamed chunk of search results (client-side only).</summary>
public sealed class SearchBatch
{
    public required IReadOnlyList<SearchResult> Results { get; init; }
    public bool Done { get; init; }
}

/// <summary>Raised when the engine rejects a request.</summary>
public sealed class EngineException : Exception
{
    public ErrorCode Code { get; }

    public EngineException(ErrorCode code, string message) : base(message)
    {
        Code = code;
    }
}
