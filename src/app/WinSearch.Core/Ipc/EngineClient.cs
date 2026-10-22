using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text;
using System.Threading.Channels;
using WinSearch.Core.Protocol;

namespace WinSearch.Core.Ipc;

/// <summary>
/// Contract for engine IPC operations consumed by view models, allowing
/// deterministic in-memory unit tests alongside the named-pipe <see cref="EngineClient"/>.
/// </summary>
public interface IEngineClient : IAsyncDisposable
{
    Task<IndexReset> ResetIndexAsync(ResetRequest request, CancellationToken cancellationToken = default)
        => throw new EngineException(ErrorCode.Unsupported, "index reset is unavailable");
    event EventHandler<EngineStatus>? StatusChanged;
    event EventHandler<ModelStatus>? ModelStatusChanged;
    event EventHandler<EngineConfig>? ConfigChanged;
    event EventHandler<IndexingConfig>? IndexingConfigChanged;
    Task<IndexingConfig> GetIndexingConfigAsync(CancellationToken cancellationToken = default);
    Task<IndexingConfig> SetIndexingConfigAsync(IndexingConfig config, CancellationToken cancellationToken = default);
    event EventHandler? Disconnected;
    event EventHandler? Reconnected;

    Task<EngineStatus> GetStatusAsync(CancellationToken cancellationToken = default);
    Task<ResolvedEntry> ResolveEntryAsync(EntryReference reference, CancellationToken cancellationToken = default);
    Task<EntryPreview> PreviewTextAsync(PreviewRequest request, CancellationToken cancellationToken = default);
    Task<ModelStatus> StartModelDownloadAsync(CancellationToken cancellationToken = default);
    Task<ModelStatus> StartModelDownloadAsync(bool includeNpu, CancellationToken cancellationToken = default)
        => includeNpu ? throw new EngineException(ErrorCode.Unsupported, "optional NPU bundle installation is unavailable") : StartModelDownloadAsync(cancellationToken);
    Task<ModelStatus> CancelModelDownloadAsync(CancellationToken cancellationToken = default);
    Task<ModelStatus> ImportModelAsync(string path, CancellationToken cancellationToken = default);
    Task<ModelStatus> ImportModelAsync(string path, bool includeNpu, CancellationToken cancellationToken = default)
        => includeNpu ? throw new EngineException(ErrorCode.Unsupported, "optional NPU bundle import is unavailable") : ImportModelAsync(path, cancellationToken);
    Task<ModelStatus> DeleteModelAsync(CancellationToken cancellationToken = default);
    Task<ModelStatus> GetModelStatusAsync(CancellationToken cancellationToken = default);
    Task<EngineConfig> GetConfigAsync(CancellationToken cancellationToken = default);
    Task<EngineConfig> SetConfigAsync(EngineConfig config, CancellationToken cancellationToken = default);
    IAsyncEnumerable<SearchBatch> SearchAsync(SearchQuery query, CancellationToken cancellationToken = default);
    IAsyncEnumerable<SearchBatch> RelatedAsync(RelatedQuery query, CancellationToken cancellationToken = default)
        => throw new EngineException(ErrorCode.Unsupported, "Related search is unavailable");
}

/// <summary>
/// Async client for the engine's named pipe. Supports request/response with
/// correlation ids, streamed search batches, per-request cancellation, pushed
/// status updates, and automatic reconnection (with engine restart) when the
/// connection drops.
/// </summary>
public sealed class EngineClient : IEngineClient
{
    private const int MaxFrameBytes = 16 * 1024 * 1024;
    private const int HandshakeTimeoutMs = 5000;
    private const int ConnectTimeoutMs = 15000;

    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly SemaphoreSlim _previewSlots = new(2, 2);
    private readonly ConcurrentDictionary<ulong, TaskCompletionSource<ServerMessage>> _unary = new();
    private readonly ConcurrentDictionary<ulong, Channel<SearchBatch>> _streams = new();
    private readonly ConcurrentDictionary<ulong, SearchReceiveBudget> _streamBudgets = new();
    private readonly string _clientName;

    private NamedPipeClientStream? _pipe;
    private CancellationTokenSource _lifetime = new();
    private Task? _readLoop;
    private TaskCompletionSource<HelloAckMessage>? _helloAck;
    private long _nextRequestId;
    private string[] _features = [];
    private bool _disposed;
    private bool _oneShot;
    private int _resetState;

    public async Task<IndexReset> ResetIndexAsync(ResetRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (!request.IsValid()) throw new EngineException(ErrorCode.InvalidRequest, "invalid reset namespace");
        if (!_features.Contains("index.reset", StringComparer.Ordinal))
            throw new EngineException(ErrorCode.Unsupported, "index reset is unavailable");
        if (Interlocked.CompareExchange(ref _resetState, 1, 0) != 0)
            throw new EngineException(ErrorCode.NotReady, "reset already active or uncertain");
        var snapshot = new ResetRequest { ExpectedIndexId = request.ExpectedIndexId, ExpectedCodeIndexId = request.ExpectedCodeIndexId };
        try
        {
            var reset = await UnaryAsync(id => new ResetIndexMessage { Id = id, Request = snapshot }, message =>
            {
                var reset = ((IndexResetMessage)message).Reset;
                if (!reset.IsValid() || reset.IndexId == snapshot.ExpectedIndexId || reset.CodeIndexId == snapshot.ExpectedCodeIndexId)
                    throw new IOException("invalid reset acknowledgment");
                return reset;
            }, cancellationToken);
            Volatile.Write(ref _resetState, 0);
            return reset;
        }
        catch (EngineException error) when (error.Code != ErrorCode.Internal)
        {
            Volatile.Write(ref _resetState, 0);
            throw;
        }
        catch
        {
            Volatile.Write(ref _resetState, 2);
            throw;
        }
    }

    private sealed class SearchReceiveBudget(long maximum)
    {
        private int _frames;
        private long _results;
        private long _bytes;

        public bool Admit(ResultsMessage message, int bytes)
        {
            _frames++;
            _results += message.Batch?.Count ?? 0;
            _bytes += bytes;
            return message.Batch is not null && _frames <= 64 && _results <= maximum && _bytes <= 64L * 1024 * 1024;
        }
    }

    public string PipeName { get; }
    public string ClientVersion { get; }
    public bool IsConnected => _pipe?.IsConnected == true;

    /// <summary>Raised for every pushed or request-scoped status snapshot.</summary>
    public event EventHandler<EngineStatus>? StatusChanged;

    /// <summary>Raised for every pushed or request-scoped model status (download progress, completion).</summary>
    public event EventHandler<ModelStatus>? ModelStatusChanged;

    /// <summary>Raised when the engine search policy arrives (Get/SetConfig answers).</summary>
    public event EventHandler<EngineConfig>? ConfigChanged;
    public event EventHandler<IndexingConfig>? IndexingConfigChanged;

    public Task<IndexingConfig> GetIndexingConfigAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(static id => new GetIndexingConfigMessage { Id = id }, static m => ((IndexingConfigMessage)m).Config, cancellationToken);
    public Task<IndexingConfig> SetIndexingConfigAsync(IndexingConfig config, CancellationToken cancellationToken = default)
    {
        if (config.RootPolicies.Count > 0 && !_features.Contains("index.root_policies", StringComparer.Ordinal))
            throw new EngineException(ErrorCode.Unsupported, "engine does not support per-folder source policies");
        return UnaryAsync(id => new SetIndexingConfigMessage { Id = id, Config = config }, static m => ((IndexingConfigMessage)m).Config, cancellationToken);
    }

    /// <summary>Raised when the pipe drops and reconnect attempts begin.</summary>
    public event EventHandler? Disconnected;

    /// <summary>Raised after a successful reconnect.</summary>
    public event EventHandler? Reconnected;

    public EngineClient(string pipeName, string clientName = "winsearch-ui", string clientVersion = "0.1.0")
    {
        PipeName = pipeName;
        _clientName = clientName;
        ClientVersion = clientVersion;
    }

    /// <summary>
    /// Connects to the engine, starting it first if no pipe answers. Performs
    /// the protocol handshake and begins the read loop.
    /// </summary>
    public async Task<HelloAckMessage> ConnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(ConnectTimeoutMs);

        var engineStarted = false;
        while (true)
        {
            timeout.Token.ThrowIfCancellationRequested();
            try
            {
                return await ConnectOnceAsync(timeout.Token);
            }
            catch (TimeoutException) when (!timeout.Token.IsCancellationRequested)
            {
                // Pipe not present yet: make sure the engine process exists.
            }
            catch (IOException) when (!timeout.Token.IsCancellationRequested)
            {
            }

            if (!engineStarted)
            {
                EngineLocator.StartEngine();
                engineStarted = true;
            }
            await Task.Delay(250, timeout.Token);
        }
    }

    private async Task<HelloAckMessage> ConnectOnceAsync(CancellationToken cancellationToken)
    {
        var pipe = new NamedPipeClientStream(
            ".", PipeName, PipeDirection.InOut, PipeOptions.Asynchronous);

        using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        connectCts.CancelAfter(1000);
        try
        {
            await pipe.ConnectAsync(connectCts.Token);
        }
        catch (OperationCanceledException)
        {
            await pipe.DisposeAsync();
            throw new TimeoutException($"pipe {PipeName} not available");
        }

        _pipe = pipe;
        _features = [];
        _lifetime.Dispose();
        _lifetime = new CancellationTokenSource();
        _helloAck = new TaskCompletionSource<HelloAckMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _readLoop = Task.Run(() => ReadLoopAsync(pipe, _lifetime.Token));

        await WriteAsync(new HelloMessage
        {
            ProtocolVersion = ProtocolJson.Version,
            Client = _clientName,
            ClientVersion = ClientVersion,
        }, cancellationToken);

        var ackTask = _helloAck.Task;
        var completed = await Task.WhenAny(ackTask, Task.Delay(HandshakeTimeoutMs, cancellationToken));
        if (completed != ackTask)
        {
            throw new TimeoutException("engine handshake timed out");
        }

        var ack = await ackTask;
        if (ack.ProtocolVersion != ProtocolJson.Version)
        {
            throw new EngineException(
                ErrorCode.ProtocolVersionMismatch,
                $"engine speaks protocol v{ack.ProtocolVersion}, client speaks v{ProtocolJson.Version}");
        }
        _features = ack.Features.ToArray();
        Reconnected?.Invoke(this, EventArgs.Empty);
        return ack;
    }

    /// <summary>Requests a status snapshot; pushed updates also arrive via <see cref="StatusChanged"/>.</summary>
    public Task<EngineStatus> GetStatusAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(static id => new GetStatusMessage { Id = id }, static m => ((StatusMessage)m).Status, cancellationToken);

    public Task<ResolvedEntry> ResolveEntryAsync(EntryReference reference, CancellationToken cancellationToken = default)
    {
        if (!ReferenceValid(reference))
            return Task.FromException<ResolvedEntry>(new EngineException(ErrorCode.InvalidRequest, "invalid indexed entry reference"));
        var feature = reference.ResultId[0] == 'c' ? "entry.resolve.code" : "entry.resolve";
        if (IsConnected && !_features.Contains(feature, StringComparer.Ordinal))
            return Task.FromException<ResolvedEntry>(new EngineException(ErrorCode.Unsupported, "engine does not support entry resolution"));
        var resultId = reference.ResultId;
        var sourceRevision = reference.SourceRevision;
        return UnaryAsync(id => new ResolveEntryMessage { Id = id,
                Reference = new() { ResultId = resultId, SourceRevision = sourceRevision } }, m =>
            m is ResolvedEntryMessage resolved && ResolvedEntryMatches(resolved.Entry, resultId, sourceRevision)
                ? resolved.Entry : throw new EngineException(ErrorCode.InvalidRequest, "entry resolution identity changed"), cancellationToken);
    }

    public async Task<EntryPreview> PreviewTextAsync(PreviewRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (request?.Reference is null || !ReferenceValid(request.Reference))
            throw new EngineException(ErrorCode.InvalidRequest, "invalid preview request");
        var code = request.Reference.ResultId[0] == 'c';
        if (!(code ? CodePreviewLocationValid(request.Location) : PreviewLocationValid(request.Location)))
            throw new EngineException(ErrorCode.InvalidRequest, "invalid preview location for this entry");
        var feature = code ? "entry.preview.code" : "entry.preview.text";
        var snapshot = new PreviewRequest { Reference = new() { ResultId = request.Reference.ResultId, SourceRevision = request.Reference.SourceRevision },
            Location = request.Location switch {
                null => null, PageLocation page => new PageLocation { Page = page.Page },
                LineLocation line => new LineLocation { Line = line.Line },
                NotebookLocation cell => new NotebookLocation { CellOrdinal = cell.CellOrdinal, Line = cell.Line, EndLine = cell.EndLine },
                TimestampLocation time => new TimestampLocation { Seconds = time.Seconds, EndSeconds = time.EndSeconds },
                _ => throw new EngineException(ErrorCode.InvalidRequest, "unsupported preview location") } };
        if (IsConnected && !_features.Contains(feature, StringComparer.Ordinal))
            throw new EngineException(ErrorCode.Unsupported, "engine does not support cached text previews");
        using var total = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        total.CancelAfter(TimeSpan.FromSeconds(35));
        await _previewSlots.WaitAsync(total.Token);
        try
        {
            // An owned read connection can be retired on partial I/O without corrupting UI/control traffic.
            await using var owned = new EngineClient(PipeName, _clientName, ClientVersion) { _oneShot = true };
            var info = await owned.ConnectOnceAsync(total.Token);
            if (!info.Features.Contains(feature, StringComparer.Ordinal))
                throw new EngineException(ErrorCode.Unsupported, "engine does not support cached text previews");
            return await owned.UnaryAsync(id => new PreviewEntryMessage { Id = id, Request = snapshot }, message =>
                message is EntryPreviewMessage reply && PreviewMatches(reply.Preview, snapshot)
                    ? reply.Preview : throw new EngineException(ErrorCode.InvalidRequest, "preview identity, location, state or budget changed"), total.Token);
        }
        catch (Exception) when (total.IsCancellationRequested) { throw new OperationCanceledException(total.Token); }
        finally { _previewSlots.Release(); }
    }

    private static bool PreviewLocationValid(MatchLocation? location) => location switch {
        null => true,
        PageLocation page => page.Page is > 0 and <= 5000,
        LineLocation line => line.Line is > 0 and <= 64 * 1024 * 1024,
        TimestampLocation time => double.IsFinite(time.Seconds) && double.IsFinite(time.EndSeconds)
            && time.Seconds >= 0 && time.EndSeconds >= time.Seconds && time.EndSeconds <= 366 * 86400,
        _ => false };

    private static bool CodePreviewLocationValid(MatchLocation? location) => location switch {
        null => true,
        LineLocation line => line.Line is > 0 and <= 64 * 1024 * 1024,
        NotebookLocation cell => cell.Line > 0 && cell.EndLine >= cell.Line && cell.EndLine <= 64 * 1024 * 1024,
        _ => false };

    private static int PreviewLineCount(string text) => Math.Max(1, text.Count(c => c == '\n') + (text.EndsWith('\n') ? 0 : 1));

    private static bool PreviewMatches(EntryPreview? preview, PreviewRequest expected)
    {
        var code = expected.Reference.ResultId[0] == 'c';
        bool Known(string? kind) => code ? kind == "code" : kind is "doc" or "pdf" or "ocr" or "image" or "audio" or "video" or "transcript";
        if (preview?.Parts is null || preview.Extraction is null || !Enum.IsDefined(preview.State)
            || !ResolvedEntryMatches(preview.Entry, expected.Reference.ResultId, expected.Reference.SourceRevision)
            || preview.Parts.Count > 64 || preview.Extraction.Count > 8) return false;
        var ready = preview.State == PreviewState.Ready;
        if (ready == (preview.Parts.Count == 0) || (ready && preview.Entry.IndexRevision is null)
            || (preview.State == PreviewState.NotIndexed && preview.Entry.IndexRevision is not null)
            || (preview.State == PreviewState.LocationUnavailable && expected.Location is null)) return false;
        var bytes = 0;
        foreach (var part in preview.Parts)
        {
            if (part is null || string.IsNullOrEmpty(part.Text) || !Known(part.Kind)
                || !(code ? part.Location is not null && CodePreviewLocationValid(part.Location) : PreviewLocationValid(part.Location))) return false;
            if (part.Location is NotebookLocation range && (ulong)(range.EndLine - range.Line) >= (ulong)PreviewLineCount(part.Text)) return false;
            bytes += Encoding.UTF8.GetByteCount(part.Text);
            if (bytes > 65536) return false;
            var matches = (expected.Location, part.Location) switch {
                (null, _) => true,
                (PageLocation first, PageLocation actual) => first.Page == actual.Page,
                (LineLocation first, LineLocation actual) => first.Line >= actual.Line
                    && (ulong)(first.Line - actual.Line) < (ulong)PreviewLineCount(part.Text),
                (NotebookLocation first, NotebookLocation actual) => first.CellOrdinal == actual.CellOrdinal
                    && first.Line >= actual.Line && first.EndLine <= actual.EndLine,
                (TimestampLocation first, TimestampLocation actual) => first.Seconds == actual.Seconds && first.EndSeconds == actual.EndSeconds,
                _ => false };
            if (!matches) return false;
        }
        return preview.Extraction.All(item => item is not null && Known(item.Kind)
            && !(item.IndexedUnits is { } indexed && item.SourceUnits is { } source && indexed > source));
    }

    private static bool ReferenceValid(EntryReference? reference, bool bound = false)
    {
        if (reference is null || string.IsNullOrEmpty(reference.ResultId) || reference.ResultId[0] is not ('e' or 'c')
            || !long.TryParse(reference.ResultId.AsSpan(1), System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture, out var id) || id <= 0
            || reference.ResultId[1..] != id.ToString(System.Globalization.CultureInfo.InvariantCulture)) return false;
        return reference.SourceRevision is { } proof
            ? !string.IsNullOrWhiteSpace(proof) && Encoding.UTF8.GetByteCount(proof) <= 65536 : !bound;
    }

    private static bool ReferenceKindMatches(EntryReference reference, ResultKind kind) => reference.ResultId[0] == 'c'
        ? kind == ResultKind.Code
        : kind is ResultKind.File or ResultKind.Folder or ResultKind.Image or ResultKind.Screenshot
            or ResultKind.Document or ResultKind.Audio or ResultKind.Video;

    private static bool ResolvedEntryMatches(ResolvedEntry? entry, string resultId, string? sourceRevision)
    {
        var reference = new EntryReference { ResultId = resultId, SourceRevision = sourceRevision };
        if (entry is null || entry.RootPath is null || entry.Path is null
            || !ReferenceValid(reference) || !ReferenceKindMatches(reference, entry.Kind)
            || entry.ResultId != resultId || string.IsNullOrWhiteSpace(entry.SourceRevision)
            || Encoding.UTF8.GetByteCount(entry.SourceRevision) > 65536
            || (entry.ContentSha256 is { } hash && (entry.IndexRevision is null || hash.Length != 64
                || hash.Any(c => !(c is >= '0' and <= '9' or >= 'a' and <= 'f'))))
            || string.IsNullOrWhiteSpace(entry.Name) || entry.IndexRevision is <= 0
            || (sourceRevision is not null && entry.SourceRevision != sourceRevision)
            || !Enum.IsDefined(entry.Kind))
            return false;
        var root = entry.RootPath.Replace('/', '\\');
        var path = entry.Path.Replace('/', '\\');
        static bool LocalDisk(string path)
        {
            if (path.StartsWith(@"\\?\", StringComparison.Ordinal)) path = path[4..];
            return path.Length >= 3 && char.IsAsciiLetter(path[0]) && path[1] == ':' && path[2] == '\\'
                && !path.AsSpan(2).Contains(':') && !path.Contains('\0');
        }
        if (!LocalDisk(root) || !LocalDisk(path)) return false;
        try
        {
            var relative = Path.GetRelativePath(root, path);
            return !Path.IsPathRooted(relative) && relative != ".."
                && !relative.StartsWith(@"..\", StringComparison.Ordinal);
        }
        catch (Exception error) when (error is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return false;
        }
    }

    /// <summary>Starts (or resumes) the embedding-model download; progress arrives via <see cref="ModelStatusChanged"/>.</summary>
    public Task<ModelStatus> StartModelDownloadAsync(CancellationToken cancellationToken = default)
        => StartModelDownloadAsync(false, cancellationToken);

    public Task<ModelStatus> StartModelDownloadAsync(bool includeNpu, CancellationToken cancellationToken = default)
    {
        RequireNpuBundle(includeNpu);
        return UnaryAsync(id => new ModelDownloadStartMessage { Id = id, IncludeNpu = includeNpu }, static m => ((ModelStatusMessage)m).Status, cancellationToken);
    }

    /// <summary>Cancels the in-flight model download; partial files stay for resume.</summary>
    public Task<ModelStatus> CancelModelDownloadAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(static id => new ModelDownloadCancelMessage { Id = id }, static m => ((ModelStatusMessage)m).Status, cancellationToken);

    /// <summary>Imports model files from a local file or directory.</summary>
    public Task<ModelStatus> ImportModelAsync(string path, CancellationToken cancellationToken = default)
        => ImportModelAsync(path, false, cancellationToken);

    public Task<ModelStatus> ImportModelAsync(string path, bool includeNpu, CancellationToken cancellationToken = default)
    {
        RequireNpuBundle(includeNpu);
        return UnaryAsync(id => new ModelImportMessage { Id = id, Path = path, IncludeNpu = includeNpu }, static m => ((ModelStatusMessage)m).Status, cancellationToken);
    }

    private void RequireNpuBundle(bool includeNpu)
    {
        if (includeNpu && !_features.Contains("model.npu_bundle", StringComparer.Ordinal))
            throw new EngineException(ErrorCode.Unsupported, "engine does not support optional NPU bundle installation");
    }

    /// <summary>Deletes the downloaded model files.</summary>
    public Task<ModelStatus> DeleteModelAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(static id => new ModelDeleteMessage { Id = id }, static m => ((ModelStatusMessage)m).Status, cancellationToken);

    /// <summary>Requests a model status snapshot; pushed updates also arrive via <see cref="ModelStatusChanged"/>.</summary>
    public Task<ModelStatus> GetModelStatusAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(static id => new GetModelStatusMessage { Id = id }, static m => ((ModelStatusMessage)m).Status, cancellationToken);

    /// <summary>Requests the engine search policy; answers also arrive via <see cref="ConfigChanged"/>.</summary>
    public Task<EngineConfig> GetConfigAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(static id => new GetConfigMessage { Id = id }, static m => ((ConfigMessage)m).Config, cancellationToken);

    /// <summary>Persists the engine search policy; the stored copy arrives via <see cref="ConfigChanged"/>.</summary>
    public Task<EngineConfig> SetConfigAsync(EngineConfig config, CancellationToken cancellationToken = default)
        => UnaryAsync(id => new SetConfigMessage { Id = id, Config = config }, static m => ((ConfigMessage)m).Config, cancellationToken);

    /// <summary>
    /// Runs a search and streams result batches. Cancelling <paramref name="cancellationToken"/>
    /// sends a Cancel to the engine so obsolete semantic work stops server-side too.
    /// </summary>
    public async IAsyncEnumerable<SearchBatch> SearchAsync(
        SearchQuery query,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (query is null || query.Text is null || query.Text.Contains('\0') || Encoding.UTF8.GetByteCount(query.Text) > 64 * 1024
            || query.Limit > 10000 || !Enum.IsDefined(query.Mode) || query.Categories is null || query.Categories.Count > 32
            || query.Categories.Any(kind => !Enum.IsDefined(kind)) || !SearchExtensionsValid(query.Extensions) || !FileKinds.Valid(query.FileKinds))
            throw new EngineException(ErrorCode.InvalidRequest, "invalid search request budget or filters");
        var snapshot = new SearchQuery { Text = query.Text, Mode = query.Mode, Limit = query.Limit,
            IncludeSemantic = query.IncludeSemantic, Categories = query.Categories.ToList(), Extensions = query.Extensions.ToList(), FileKinds = query.FileKinds.ToList() };
        if (snapshot.Limit > 4096 && !_features.Contains("search.limits", StringComparer.Ordinal))
            throw new EngineException(ErrorCode.Unsupported, "engine does not support search.limits");
        if (snapshot.Extensions.Count > 0 && IsConnected && !_features.Contains("search.filters", StringComparer.Ordinal))
            throw new EngineException(ErrorCode.Unsupported, "engine does not support search filters");
        if (snapshot.FileKinds.Count > 0 && IsConnected && !_features.Contains("search.file_kinds", StringComparer.Ordinal))
            throw new EngineException(ErrorCode.Unsupported, "engine does not support search.file_kinds");
        await foreach (var batch in SearchStreamAsync(id => new SearchMessage { Id = id, Query = snapshot }, snapshot, null, cancellationToken))
            yield return batch;
    }

    public async IAsyncEnumerable<SearchBatch> RelatedAsync(RelatedQuery query,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (query is null || !ReferenceValid(query.Source, bound: true) || query.Limit is < 1 or > 10000
            || query.Categories is null || query.Categories.Count > 32 || query.Categories.Any(kind => !Enum.IsDefined(kind))
            || !SearchExtensionsValid(query.Extensions) || !FileKinds.Valid(query.FileKinds))
            throw new EngineException(ErrorCode.InvalidRequest, "invalid Related source or request budget");
        var snapshot = new RelatedQuery { Source = new() { ResultId = query.Source.ResultId, SourceRevision = query.Source.SourceRevision },
            Limit = query.Limit, Categories = query.Categories.ToList(), Extensions = query.Extensions.ToList(), FileKinds = query.FileKinds.ToList() };
        var code = snapshot.Source.ResultId[0] == 'c';
        foreach (var (required, feature) in new[] { (true, "search.related"), (true, "search.semantic"),
            (true, "search.outcomes"), (code, "code"), (snapshot.Extensions.Count > 0, "search.filters"), (snapshot.FileKinds.Count > 0, "search.file_kinds") })
            if (required && !_features.Contains(feature, StringComparer.Ordinal))
                throw new EngineException(ErrorCode.Unsupported, $"engine does not support {feature}");
        var context = new SearchQuery { Mode = code ? SearchMode.Code : SearchMode.Files, IncludeSemantic = true,
            Limit = snapshot.Limit, Categories = snapshot.Categories, Extensions = snapshot.Extensions, FileKinds = snapshot.FileKinds };
        await foreach (var batch in SearchStreamAsync(id => new RelatedMessage { Id = id, Query = snapshot }, context,
            snapshot.Source.ResultId, cancellationToken)) yield return batch;
    }

    private async IAsyncEnumerable<SearchBatch> SearchStreamAsync(Func<ulong, ClientMessage> request, SearchQuery snapshot,
        string? relatedSource, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var id = NextRequestId();
        var mode = snapshot.Mode;
        var requireOutcome = relatedSource is not null || _features.Contains("search.outcomes", StringComparer.Ordinal);
        var maxResults = relatedSource is not null || _features.Contains("search.limits", StringComparer.Ordinal)
            ? Math.Max(1u, snapshot.Limit) : 4096u;
        var channel = relatedSource is not null ? Channel.CreateBounded<SearchBatch>(new BoundedChannelOptions(2)
        { SingleReader = true, SingleWriter = true }) : Channel.CreateUnbounded<SearchBatch>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
        });
        _streams[id] = channel;
        _streamBudgets[id] = new SearchReceiveBudget(maxResults);
        var frames = 0;
        long results = 0;
        var terminal = false;

        using var registration = cancellationToken.Register(() =>
        {
            // Fire-and-forget: best effort server-side cancellation.
            _ = TryCancelAsync(id);
        });

        try
        {
            await WriteAsync(request(id), cancellationToken);
            await foreach (var batch in channel.Reader.ReadAllAsync(cancellationToken))
            {
                frames++;
                results += batch.Results?.Count ?? 0;
                if (frames > 64 || results > maxResults)
                    throw new EngineException(ErrorCode.InvalidRequest, "search exceeded its result budget");
                if ((batch.Done && requireOutcome && batch.Outcome is null) || (batch.Outcome is { } outcome
                    && !outcome.ValidFor(batch.Done, batch.Results is { Count: 0 }, snapshot.IncludeSemantic)))
                    throw new EngineException(ErrorCode.InvalidRequest, "search returned an invalid or missing terminal outcome");
                if (relatedSource is not null && ((batch.Outcome is { Disposition: SearchDisposition.Complete,
                    Semantic.Phase: SemanticPhase.Disabled }) || batch.Results is null || batch.Results.Any(hit => hit is null
                    || hit.EntryReference is null || hit.EntryReference.ResultId == relatedSource
                    || (snapshot.Categories.Count > 0 && !snapshot.Categories.Contains(hit.Kind)))))
                    throw new EngineException(ErrorCode.InvalidRequest, "Related returned its source, an unbound result or ordinary fallback state");
                if (batch.Results is null || batch.Results.Any(hit => hit is null || (snapshot.Extensions.Count > 0 && hit.Kind == ResultKind.Folder)
                    || !SearchExtensionMatches(snapshot.Extensions, hit.Path) || !FileKinds.Matches(snapshot.FileKinds,hit) || (hit.EntryReference is { } reference
                    && (!ReferenceValid(reference, bound: true) || !ReferenceKindMatches(reference, hit.Kind)
                        || ((reference.ResultId[0] == 'c') != (mode == SearchMode.Code))))))
                    throw new EngineException(ErrorCode.InvalidRequest, "search returned an invalid reference or filtered result");
                terminal = batch.Done;
                yield return batch;
            }
        }
        finally
        {
            _streams.TryRemove(id, out _);
            _streamBudgets.TryRemove(id, out _);
            if (relatedSource is not null && !terminal) _ = TryCancelAsync(id);
        }
    }

    private static bool SearchExtensionsValid(IReadOnlyList<string>? extensions)
    {
        if (extensions is null || extensions.Count > 64) return false;
        foreach (var value in extensions)
        {
            if (value is null) return false;
            var extension = value.StartsWith(".", StringComparison.Ordinal) ? value[1..] : value;
            if (extension.Length is < 1 or > 32 || !extension.All(character => character is >= 'a' and <= 'z'
                or >= 'A' and <= 'Z' or >= '0' and <= '9' or '+' or '-' or '_')) return false;
        }
        return true;
    }

    private static bool SearchExtensionMatches(IReadOnlyList<string> extensions, string? path)
    {
        if (extensions.Count == 0) return true;
        if (path is null) return false;
        var extension = System.IO.Path.GetExtension(path);
        return extension.Length > 1 && extensions.Any(value => string.Equals(
            value.StartsWith(".", StringComparison.Ordinal) ? value[1..] : value, extension[1..], StringComparison.OrdinalIgnoreCase));
    }

    private async Task TryCancelAsync(ulong id)
    {
        try
        {
            await WriteAsync(new CancelMessage { Id = id }, CancellationToken.None);
        }
        catch
        {
            // Connection may already be gone; nothing to do.
        }
    }

    private async Task<T> UnaryAsync<T>(
        Func<ulong, ClientMessage> factory,
        Func<ServerMessage, T> selector,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var id = NextRequestId();
        var tcs = new TaskCompletionSource<ServerMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _unary[id] = tcs;
        await using var registration = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken)).ConfigureAwait(false);
        try
        {
            await WriteAsync(factory(id), cancellationToken);
            var message = await tcs.Task;
            return selector(message);
        }
        finally { _unary.TryRemove(id, out _); }
    }

    private ulong NextRequestId() => (ulong)Interlocked.Increment(ref _nextRequestId);

    private async Task WriteAsync(ClientMessage message, CancellationToken cancellationToken)
    {
        var pipe = _pipe ?? throw new IOException("not connected");
        // Serialize against the base type so the "type" discriminator is
        // written (serde needs it to route the message).
        var payload = JsonSerializer.SerializeToUtf8Bytes(message, typeof(ClientMessage), ProtocolJson.Options);
        if (payload.Length > MaxFrameBytes)
        {
            throw new InvalidOperationException("frame exceeds maximum size");
        }

        var frame = new byte[4 + payload.Length];
        BitConverter.TryWriteBytes(frame.AsSpan(0, 4), payload.Length);
        payload.CopyTo(frame.AsSpan(4));

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await pipe.WriteAsync(frame, cancellationToken);
            await pipe.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task ReadLoopAsync(NamedPipeClientStream pipe, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var frame = await ReadFrameAsync(pipe, cancellationToken);
                if (frame.Message is not { } message)
                {
                    break;
                }
                Dispatch(message, frame.Bytes);
            }
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception)
        {
            // Fall through to disconnect handling.
        }

        if (!cancellationToken.IsCancellationRequested && !_disposed)
        {
            HandleDisconnect();
        }
    }

    private async Task<(ServerMessage? Message, int Bytes)> ReadFrameAsync(NamedPipeClientStream pipe, CancellationToken cancellationToken)
    {
        var lengthBuffer = new byte[4];
        var read = await ReadAtLeastAsync(pipe, lengthBuffer, cancellationToken);
        if (read == 0)
        {
            return (null, 0); // clean disconnect
        }
        if (read < 4)
        {
            throw new IOException("stream ended mid-frame");
        }

        var length = BitConverter.ToInt32(lengthBuffer);
        if (length is < 0 or > MaxFrameBytes)
        {
            throw new IOException($"frame of {length} bytes is invalid");
        }

        var payload = new byte[length];
        if (await ReadAtLeastAsync(pipe, payload, cancellationToken) < length)
        {
            throw new IOException("stream ended mid-frame");
        }

        return ((ServerMessage?)JsonSerializer.Deserialize(payload, typeof(ServerMessage), ProtocolJson.Options), length + 4);
    }

    /// <summary>Reads until the buffer is full; returns 0 only for a clean EOF before any byte.</summary>
    private static async Task<int> ReadAtLeastAsync(
        Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        var filled = 0;
        while (filled < buffer.Length)
        {
            var n = await stream.ReadAsync(buffer.AsMemory(filled), cancellationToken);
            if (n == 0)
            {
                return filled;
            }
            filled += n;
        }
        return filled;
    }

    private void Dispatch(ServerMessage message, int bytes)
    {
        switch (message)
        {
            case IndexResetMessage reset:
                if (_unary.TryRemove(reset.Id, out var resetTcs)) resetTcs.TrySetResult(reset);
                break;
            case ResolvedEntryMessage resolved:
                if (_unary.TryRemove(resolved.Id, out var resolvedTcs))
                    resolvedTcs.TrySetResult(resolved);
                break;
            case EntryPreviewMessage preview:
                if (_unary.TryRemove(preview.Id, out var previewTcs))
                    previewTcs.TrySetResult(preview);
                break;
            case IndexingConfigMessage indexing:
                if (indexing.Id is { } indexingId && _unary.TryRemove(indexingId, out var indexingTcs))
                    indexingTcs.TrySetResult(indexing);
                IndexingConfigChanged?.Invoke(this, indexing.Config);
                break;
            case HelloAckMessage ack:
                _helloAck?.TrySetResult(ack);
                break;

            case PongMessage pong:
                if (_unary.TryRemove(pong.Id, out var pongTcs))
                {
                    pongTcs.TrySetResult(pong);
                }
                break;

            case ResultsMessage results:
                if (_streams.TryGetValue(results.Id, out var channel))
                {
                    if (!_streamBudgets.TryGetValue(results.Id, out var budget) || !budget.Admit(results, bytes))
                    {
                        channel.Writer.TryComplete(new EngineException(ErrorCode.InvalidRequest, "search stream exceeded its receive budget"));
                        _streams.TryRemove(results.Id, out _);
                        _streamBudgets.TryRemove(results.Id, out _);
                        _ = TryCancelAsync(results.Id);
                        break;
                    }
                    if (!channel.Writer.TryWrite(new SearchBatch
                    {
                        Results = results.Batch,
                        Done = results.Done,
                        Outcome = results.Outcome,
                    })) channel.Writer.TryComplete(new EngineException(ErrorCode.InvalidRequest, "search stream exceeded its pending frame budget"));
                    if (results.Done)
                    {
                        channel.Writer.TryComplete();
                    }
                }
                break;

            case StatusMessage status:
                if (status.Id is { } statusId && _unary.TryRemove(statusId, out var statusTcs))
                {
                    statusTcs.TrySetResult(status);
                }
                StatusChanged?.Invoke(this, status.Status);
                break;

            case ModelStatusMessage modelStatus:
                if (modelStatus.Id is { } modelStatusId && _unary.TryRemove(modelStatusId, out var modelStatusTcs))
                {
                    modelStatusTcs.TrySetResult(modelStatus);
                }
                ModelStatusChanged?.Invoke(this, modelStatus.Status);
                break;

            case ConfigMessage config:
                if (config.Id is { } configId && _unary.TryRemove(configId, out var configTcs))
                {
                    configTcs.TrySetResult(config);
                }
                ConfigChanged?.Invoke(this, config.Config);
                break;

            case ErrorMessage error:
                var exception = new EngineException(error.Code, error.Message);
                if (error.Id is { } errorId)
                {
                    if (_unary.TryRemove(errorId, out var errorTcs))
                    {
                        errorTcs.TrySetException(exception);
                    }
                    if (_streams.TryRemove(errorId, out var errorChannel))
                    {
                        _streamBudgets.TryRemove(errorId, out _);
                        errorChannel.Writer.TryComplete(exception);
                    }
                }
                else
                {
                    _helloAck?.TrySetException(exception);
                }
                break;
        }
    }

    private void HandleDisconnect()
    {
        FailPending(new IOException("engine connection lost"));
        if (_oneShot) return;
        Disconnected?.Invoke(this, EventArgs.Empty);
        _ = ReconnectLoopAsync();
    }

    private void FailPending(Exception exception)
    {
        foreach (var key in _unary.Keys)
        {
            if (_unary.TryRemove(key, out var tcs))
            {
                tcs.TrySetException(exception);
            }
        }
        foreach (var key in _streams.Keys)
        {
            if (_streams.TryRemove(key, out var channel))
            {
                _streamBudgets.TryRemove(key, out _);
                channel.Writer.TryComplete(exception);
            }
        }
    }

    private async Task ReconnectLoopAsync()
    {
        var engineStarted = false;
        while (!_disposed)
        {
            try
            {
                await Task.Delay(500);
                await ConnectAsync(_lifetime.Token);
                return;
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception)
            {
                if (!engineStarted)
                {
                    EngineLocator.StartEngine();
                    engineStarted = true;
                }
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _resetState) == 2)
            throw new EngineException(ErrorCode.NotReady, "reset outcome unknown; restart the owned engine and create a new client");
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        _lifetime.Cancel();
        FailPending(new ObjectDisposedException(nameof(EngineClient)));
        if (_pipe is not null)
        {
            await _pipe.DisposeAsync();
        }
        _writeLock.Dispose();
        _lifetime.Dispose();
    }
}
