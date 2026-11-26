using System.Collections;
using System.IO.Pipes;
using System.Reflection;
using System.Text.Json;
using WinSearch.Core.Ipc;
using WinSearch.Core.Protocol;
using Xunit;

namespace WinSearch.Tests;

public class IndexingPipeTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    public async Task ResetRequiresCapabilityAndNeverRetriesAnUnknownMutation(bool supported, bool acknowledge)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var requestCancellation = new CancellationTokenSource();
        var token = timeout.Token;
        var pipe = "winsearch-reset-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token);
            await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = supported ? ["index.reset"] : [] }, token);
            if (supported)
            {
                var request = Assert.IsType<ResetIndexMessage>(await ReadAsync(server, token));
                Assert.Equal(new string('a', 32), request.Request.ExpectedIndexId);
                if (!acknowledge) { requestCancellation.Cancel(); return; }
                await WriteAsync(server, new IndexResetMessage { Id = request.Id, Reset = new() { IndexId = new string('c', 32), CodeIndexId = new string('d', 32) } }, token);
            }
            var status = Assert.IsType<GetStatusMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new StatusMessage { Id = status.Id, Status = new() }, token);
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        await client.ConnectAsync(token);
        var request = new ResetRequest { ExpectedIndexId = new string('a', 32), ExpectedCodeIndexId = new string('b', 32) };
        if (!supported)
            Assert.Equal(ErrorCode.Unsupported, (await Assert.ThrowsAsync<EngineException>(() => client.ResetIndexAsync(request, token))).Code);
        else if (acknowledge)
            Assert.Equal(new string('c', 32), (await client.ResetIndexAsync(request, token)).IndexId);
        else
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ResetIndexAsync(request, requestCancellation.Token));
            Assert.Equal(ErrorCode.NotReady, (await Assert.ThrowsAsync<EngineException>(() => client.GetStatusAsync(token))).Code);
        }
        if (!supported || acknowledge) await client.GetStatusAsync(token);
        await serving;
    }
    [Fact]
    public void DiagnosticsRemainUnknownForLegacyAndRoundTripTypedReasons()
    {
        var legacy = JsonSerializer.Deserialize<EngineStatus>("{}", ProtocolJson.Options)!;
        Assert.Null(legacy.Embedding);
        Assert.Null(JsonSerializer.Deserialize<ModelStatus>("{}", ProtocolJson.Options)!.NpuBundle);
        const string json = "{\"embedding\":{\"requested_backend\":\"npu\",\"text_backend\":\"npu+cpu\",\"image_backend\":\"cpu\",\"audio_backend\":\"cpu\",\"npu\":\"quarantined\"}}";
        var status = JsonSerializer.Deserialize<EngineStatus>(json, ProtocolJson.Options)!;
        Assert.Equal(NpuState.Quarantined, status.Embedding!.Npu);
        Assert.Equal("npu+cpu", status.Embedding.TextBackend);
        Assert.Equal("cpu", status.Embedding.ImageBackend);
        Assert.Equal("cpu", status.Embedding.AudioBackend);
        Assert.Equal(NpuState.Quarantined, JsonSerializer.Deserialize<EngineStatus>(
            JsonSerializer.Serialize(status, ProtocolJson.Options), ProtocolJson.Options)!.Embedding!.Npu);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OptionalNpuRequestsRequireCapabilityBeforeAnyMutation(bool import)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = timeout.Token;
        var pipe = "winsearch-npu-legacy-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1 }, token);
            var request = Assert.IsType<GetStatusMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new StatusMessage { Id = request.Id, Status = new() }, token);
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        await client.ConnectAsync(token);
        var error = await Assert.ThrowsAsync<EngineException>(() => import
            ? client.ImportModelAsync("C:/models", true, token)
            : client.StartModelDownloadAsync(true, token));
        Assert.Equal(ErrorCode.Unsupported, error.Code);
        await client.GetStatusAsync(token);
        await serving;
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task ModelRequestsCarryExplicitNpuChoiceAndIndependentProgress(bool import, bool includeNpu)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = timeout.Token;
        var pipe = "winsearch-npu-choice-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token);
            await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["model.npu_bundle"] }, token);
            var request = await ReadAsync(server, token);
            ulong id;
            if (import)
            {
                var message = Assert.IsType<ModelImportMessage>(request);
                Assert.Equal(includeNpu, message.IncludeNpu);
                Assert.Equal("C:/models", message.Path);
                id = message.Id;
            }
            else
            {
                var message = Assert.IsType<ModelDownloadStartMessage>(request);
                Assert.Equal(includeNpu, message.IncludeNpu);
                id = message.Id;
            }
            await WriteAsync(server, new ModelStatusMessage { Id = id, Status = new ModelStatus
            {
                State = ModelState.Ready, Bytes = 4, TotalBytes = 4,
                NpuBundle = new ModelComponentStatus { State = ModelState.Downloading, Bytes = 10, TotalBytes = 1_085_278_625 },
            } }, token);
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        await client.ConnectAsync(token);
        var status = import
            ? includeNpu ? await client.ImportModelAsync("C:/models", true, token) : await client.ImportModelAsync("C:/models", token)
            : includeNpu ? await client.StartModelDownloadAsync(true, token) : await client.StartModelDownloadAsync(token);
        Assert.Equal(ModelState.Ready, status.State);
        Assert.Equal(4UL, status.TotalBytes);
        Assert.Equal(ModelState.Downloading, status.NpuBundle!.State);
        Assert.Equal(1_085_278_625UL, status.NpuBundle.TotalBytes);
        await serving;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FileKindSelectionRequiresCapabilityBeforeDispatch(bool related)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-kind-legacy-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task<bool> Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["search.exact", "search.related", "search.semantic", "search.outcomes"] }, token);
            try {
                var message = await ReadAsync(server, token);
                var id = message is RelatedMessage rel ? rel.Id : Assert.IsType<SearchMessage>(message).Id;
                await WriteAsync(server, new ResultsMessage { Id = id, Done = true, Outcome = new() {
                    Disposition = SearchDisposition.Complete, Semantic = new() { Phase = related ? SemanticPhase.Ready : SemanticPhase.Disabled } } }, token);
                return true;
            }
            catch (EndOfStreamException) { return false; }
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        var failure = await Record.ExceptionAsync(async () => {
            var stream = related ? client.RelatedAsync(new() { Source = new() { ResultId = "e1", SourceRevision = "proof" }, FileKinds = [FileKind.Pdf] }, token)
                : client.SearchAsync(new() { FileKinds = [FileKind.Pdf] }, token);
            await foreach (var _ in stream) { }
        });
        await client.DisposeAsync(); Assert.False(await serving);
        Assert.Equal(ErrorCode.Unsupported, Assert.IsType<EngineException>(failure).Code);
    }

    [Theory]
    [InlineData("C:/Docs/book.XLSX", ResultKind.Document, true)]
    [InlineData("C:/Docs/book.pdf", ResultKind.Document, false)]
    [InlineData("C:/Docs/folder.xlsx", ResultKind.Folder, false)]
    [InlineData("C:/Docs/book.xlsx", (ResultKind)999, false)]
    [InlineData(null, ResultKind.File, false)]
    public async Task FileKindSelectionRejectsUnselectedRepliesAndPreservesTheWireSelector(string? path, ResultKind kind, bool allowed)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-kind-reply-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task<SearchQuery> Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["search.exact", "search.file_kinds"] }, token);
            var message = Assert.IsType<SearchMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new ResultsMessage { Id = message.Id, Done = true,
                Batch = [new SearchResult { Id = "e2", Path = path, Kind = kind }] }, token);
            return message.Query;
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        var failure = await Record.ExceptionAsync(async () => {
            await foreach (var _ in client.SearchAsync(new() { FileKinds = [FileKind.Spreadsheet] }, token)) { }
        });
        Assert.Equal(new[] { FileKind.Spreadsheet }, (await serving).FileKinds);
        if (allowed) Assert.Null(failure);
        else Assert.Equal(ErrorCode.InvalidRequest, Assert.IsType<EngineException>(failure).Code);
    }

    [Fact]
    public async Task FileKindSelectionRejectsInvalidListsBeforeConnecting()
    {
        await using var client = new EngineClient("unused-kinds-" + Guid.NewGuid());
        foreach (var kinds in new List<FileKind>[] { null!, Enumerable.Repeat(FileKind.Pdf, 14).ToList(), [(FileKind)999] })
        {
            var failure = await Assert.ThrowsAsync<EngineException>(async () => {
                await foreach (var _ in client.SearchAsync(new() { FileKinds = kinds })) { }
            });
            Assert.Equal(ErrorCode.InvalidRequest, failure.Code);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FileKindSelectionSnapshotsCallerListsDuringProgressiveSearch(bool related)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-kind-snapshot-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1,
                Features = ["search.exact", "search.related", "search.semantic", "search.outcomes", "search.file_kinds"] }, token);
            var message = await ReadAsync(server, token);
            var id = message is RelatedMessage rel ? rel.Id : Assert.IsType<SearchMessage>(message).Id;
            Assert.Equal(new[] { FileKind.Spreadsheet }, message is RelatedMessage r ? r.Query.FileKinds : ((SearchMessage)message).Query.FileKinds);
            var hit = new SearchResult { Id = "e2", Kind = ResultKind.Document, Path = "C:/Docs/book.xlsx",
                EntryReference = new() { ResultId = "e2", SourceRevision = "proof" } };
            await WriteAsync(server, new ResultsMessage { Id = id, Done = false, Batch = [hit] }, token);
            await resume.Task.WaitAsync(token);
            await WriteAsync(server, new ResultsMessage { Id = id, Done = true, Batch = [hit], Outcome = new() {
                Disposition = SearchDisposition.Complete, Semantic = new() { Phase = related ? SemanticPhase.Ready : SemanticPhase.Disabled } } }, token);
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        var kinds = new List<FileKind> { FileKind.Spreadsheet };
        var stream = related ? client.RelatedAsync(new() { Source = new() { ResultId = "e1", SourceRevision = "seed-proof" }, FileKinds = kinds }, token)
            : client.SearchAsync(new() { FileKinds = kinds }, token);
        await using var iterator = stream.GetAsyncEnumerator(token);
        Assert.True(await iterator.MoveNextAsync()); Assert.False(iterator.Current.Done);
        kinds[0] = FileKind.Pdf; resume.SetResult();
        Assert.True(await iterator.MoveNextAsync()); Assert.True(iterator.Current.Done);
        Assert.False(await iterator.MoveNextAsync()); await serving;
    }

    [Fact]
    public async Task RelatedRejectsUnboundSourceBeforeConnecting()
    {
        await using var client = new EngineClient("unused-related-" + Guid.NewGuid());
        var failure = await Assert.ThrowsAsync<EngineException>(async () => {
            await foreach (var _ in client.RelatedAsync(new() { Source = new() { ResultId = "e1" } })) { }
        });
        Assert.Equal(ErrorCode.InvalidRequest, failure.Code);
    }

    [Theory]
    [InlineData(0)] [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)] [InlineData(5)]
    [InlineData(6)] [InlineData(7)] [InlineData(8)] [InlineData(9)] [InlineData(10)]
    public async Task RelatedIsDistinctCapabilityGatedAndRejectsSourceOrFallbackReplies(int scenario)
    {
        var pipe = "winsearch-related-" + Guid.NewGuid().ToString("N");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        await using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            var features = new List<string> { "search.exact", "search.filters" };
            if (scenario != 6) features.Add("search.semantic");
            if (scenario != 4) features.Add("search.related");
            if (scenario != 5) features.Add("search.outcomes");
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = features }, token);
            if (scenario is >= 4 and <= 7) return;
            var request = Assert.IsType<RelatedMessage>(await ReadAsync(server, token));
            Assert.Equal("e1", request.Query.Source.ResultId); Assert.Equal("source-proof", request.Query.Source.SourceRevision);
            var target = new SearchResult { Id = "s2", Kind = ResultKind.Document, Path = "C:/Docs/report.txt",
                EntryReference = scenario == 3 ? null : new() { ResultId = scenario == 1 ? "e1" : "e2", SourceRevision = "target-proof" } };
            await WriteAsync(server, new ResultsMessage { Id = request.Id, Done = true,
                Outcome = new() { Disposition = SearchDisposition.Complete, Semantic = new() {
                    Phase = scenario == 2 ? SemanticPhase.Disabled : SemanticPhase.Ready } },
                Batch = scenario == 10 ? [target, target] : [target] }, token);
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        SearchBatch? observed = null;
        var failure = await Record.ExceptionAsync(async () => {
            await foreach (var batch in client.RelatedAsync(new() { Source = new() { ResultId = scenario == 7 ? "c1" : "e1", SourceRevision = "source-proof" },
                Limit = scenario == 10 ? 1u : 20u, Categories = scenario == 8 ? [ResultKind.Audio] : [],
                Extensions = scenario == 9 ? ["pdf"] : [] }, token)) observed = batch;
        });
        if (scenario == 0) { Assert.Null(failure); Assert.NotNull(observed); }
        else Assert.Equal(scenario is >= 4 and <= 7 ? ErrorCode.Unsupported : ErrorCode.InvalidRequest, Assert.IsType<EngineException>(failure).Code);
        await serving;
    }

    [Theory]
    [InlineData(0)] [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)] [InlineData(5)] [InlineData(6)]
    [InlineData(7)] [InlineData(8)] [InlineData(9)] [InlineData(10)] [InlineData(11)] [InlineData(12)] [InlineData(13)]
    public async Task SearchOutcomesAreRequiredAndValidatedBeforeDelivery(int scenario)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-outcomes-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        var requested = scenario is 3 or 7 or 9 or 10 or 11;
        SearchOutcome Complete(SemanticPhase phase, SemanticIssue? reason = null) => new() {
            Disposition = SearchDisposition.Complete, Semantic = new() { Phase = phase, Reason = reason } };
        var outcome = scenario switch {
            0 => null,
            1 or 8 => Complete(SemanticPhase.Disabled),
            2 => new SearchOutcome { Disposition = SearchDisposition.Complete },
            3 => Complete(SemanticPhase.Ready, SemanticIssue.EmbeddingFailed),
            4 or 9 => Complete(SemanticPhase.Ready),
            5 or 11 => new SearchOutcome { Disposition = SearchDisposition.Superseded },
            6 => new SearchOutcome { Disposition = SearchDisposition.Canceled, Semantic = new() { Phase = SemanticPhase.Disabled } },
            7 => Complete(SemanticPhase.Degraded),
            10 => Complete(SemanticPhase.Degraded, SemanticIssue.ModelUnavailable),
            12 => new SearchOutcome { Disposition = SearchDisposition.Canceled },
            _ => new SearchOutcome { Disposition = SearchDisposition.Failed } };
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1,
                Features = ["search.exact", "search.semantic", "search.outcomes"] }, token);
            var search = Assert.IsType<SearchMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new ResultsMessage { Id = search.Id, Done = scenario != 1, Outcome = outcome,
                Batch = scenario == 5 ? [new SearchResult()] : [] }, token);
            if (scenario == 1) await WriteAsync(server, new ResultsMessage { Id = search.Id, Done = true,
                Outcome = Complete(SemanticPhase.Disabled) }, token);
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        SearchOutcome? observed = null;
        var failure = await Record.ExceptionAsync(async () => {
            await foreach (var batch in client.SearchAsync(new() { IncludeSemantic = requested }, token)) observed = batch.Outcome;
        });
        await serving;
        if (scenario < 8) Assert.Equal(ErrorCode.InvalidRequest, Assert.IsType<EngineException>(failure).Code);
        else { Assert.Null(failure); Assert.NotNull(observed); Assert.Equal(outcome!.Disposition, observed.Disposition); }
    }

    [Theory]
    [InlineData("")] [InlineData(".")] [InlineData("..pdf")] [InlineData("*.pdf")]
    [InlineData("a/b")] [InlineData("a\\b")] [InlineData("å")]
    public async Task ExtensionFilterRejectsInvalidInputBeforeConnecting(string extension)
    {
        await using var client = new EngineClient("unused-filter-" + Guid.NewGuid());
        var error = await Assert.ThrowsAsync<EngineException>(async () => {
            await foreach (var _ in client.SearchAsync(new() { Extensions = [extension] })) { }
        });
        Assert.Equal(ErrorCode.InvalidRequest, error.Code);
    }

    [Fact]
    public async Task ExtensionFilterRejectsOversizedListsAndComponentsBeforeConnecting()
    {
        await using var client = new EngineClient("unused-filter-budget-" + Guid.NewGuid());
        foreach (var extensions in new List<string>[] { Enumerable.Repeat("pdf", 65).ToList(), [new string('a', 33)] })
        {
            var error = await Assert.ThrowsAsync<EngineException>(async () => {
                await foreach (var _ in client.SearchAsync(new() { Extensions = extensions })) { }
            });
            Assert.Equal(ErrorCode.InvalidRequest, error.Code);
        }
    }

    [Fact]
    public async Task ExtensionFilterRequiresThePeerCapabilityBeforeSending()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-filter-legacy-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task<bool> Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["search.exact"] }, token);
            try {
                var query = Assert.IsType<SearchMessage>(await ReadAsync(server, token));
                await WriteAsync(server, new ResultsMessage { Id = query.Id, Done = true }, token);
                return true;
            }
            catch (EndOfStreamException) { return false; }
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        var failure = await Record.ExceptionAsync(async () => {
            await foreach (var _ in client.SearchAsync(new() { Extensions = ["pdf"] }, token)) { }
        });
        await client.DisposeAsync();
        Assert.False(await serving);
        Assert.Equal(ErrorCode.Unsupported, Assert.IsType<EngineException>(failure).Code);
    }

    [Theory]
    [InlineData("C:/Docs/image.png", ResultKind.Image, false)]
    [InlineData("C:/Docs/source.PDF", ResultKind.Document, true)]
    [InlineData(null, ResultKind.File, false)]
    [InlineData("C:/Docs/folder.pdf", ResultKind.Folder, false)]
    public async Task ExtensionFilterRejectsUnselectedResultsFromThePeer(string? path, ResultKind kind, bool allowed)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-filter-reply-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["search.exact", "search.filters"] }, token);
            var search = Assert.IsType<SearchMessage>(await ReadAsync(server, token));
            Assert.Equal(new[] { ".PDF" }, search.Query.Extensions);
            await WriteAsync(server, new ResultsMessage { Id = search.Id, Done = true,
                Batch = [new SearchResult { Id = "e1", Path = path, Kind = kind }] }, token);
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        var failure = await Record.ExceptionAsync(async () => {
            await foreach (var _ in client.SearchAsync(new() { Extensions = [".PDF"] }, token)) { }
        });
        await serving;
        if (allowed) Assert.Null(failure);
        else Assert.Equal(ErrorCode.InvalidRequest, Assert.IsType<EngineException>(failure).Code);
    }

    [Fact]
    public async Task CodePreviewRequiresItsOwnCapabilityBeforeSending()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-code-preview-legacy-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["entry.preview.text"] }, token);
            Assert.Equal(0, await server.ReadAsync(new byte[1], token));
        }
        var serving = Serve(); await using var client = new EngineClient(pipe);
        Exception? error;
        try {
            error = await Record.ExceptionAsync(() => client.PreviewTextAsync(new() {
                Reference = new() { ResultId = "c23", SourceRevision = "proof" }, Location = new LineLocation { Line = 1 } }, token));
            if (error is EngineException { Code: ErrorCode.Unsupported }) await serving;
        }
        finally { timeout.Cancel(); try { await serving; } catch (OperationCanceledException) { } }
        Assert.Equal(ErrorCode.Unsupported, Assert.IsType<EngineException>(error).Code);
    }

    [Fact]
    public async Task CodePreviewRejectsInvalidCoordinatesBeforeConnecting()
    {
        await using var client = new EngineClient("unused-" + Guid.NewGuid());
        foreach (var (id, location) in new (string, MatchLocation)[] {
            ("c23", new PageLocation { Page = 1 }), ("c23", new TimestampLocation { Seconds = 0, EndSeconds = 1 }),
            ("c23", new NotebookLocation { CellOrdinal = 0, Line = 0, EndLine = 1 }),
            ("c23", new NotebookLocation { CellOrdinal = 0, Line = 3, EndLine = 2 }),
            ("c23", new NotebookLocation { CellOrdinal = 0, Line = 1, EndLine = 64 * 1024 * 1024 + 1 }),
            ("e23", new NotebookLocation { CellOrdinal = 0, Line = 1, EndLine = 1 }) })
        {
            var error = await Assert.ThrowsAsync<EngineException>(() => client.PreviewTextAsync(new() {
                Reference = new() { ResultId = id }, Location = location }));
            Assert.Equal(ErrorCode.InvalidRequest, error.Code);
        }
    }

    [Theory]
    [InlineData(0)] [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)] [InlineData(5)]
    [InlineData(6)] [InlineData(7)] [InlineData(8)] [InlineData(9)] [InlineData(10)] [InlineData(11)] [InlineData(12)]
    public async Task CodePreviewCopiesCellLocationsAndRejectsMalformedReplies(int scenario)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-code-preview-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        var hello = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = false;
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token); hello.TrySetResult();
            await release.Task.WaitAsync(token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["entry.preview.code"] }, token);
            var request = Assert.IsType<PreviewEntryMessage>(await ReadAsync(server, token)); received = true;
            Assert.Equal("c23", request.Request.Reference.ResultId);
            if (scenario == 1) Assert.Equal((uint)2, Assert.IsType<NotebookLocation>(request.Request.Location).CellOrdinal);
            var preview = new EntryPreview { Entry = new() { ResultId = "c23", RootPath = @"C:\fixture", Path = @"C:\fixture\code.rs",
                    Name = "code.rs", Kind = ResultKind.Code, IndexRevision = 1, SourceRevision = "proof", ContentSha256 = new string('a', 64) },
                State = PreviewState.Ready, Parts = [new() { Kind = "code", Text = "first\nsecond", Location = scenario == 0
                    ? new LineLocation { Line = 1 } : new NotebookLocation { CellOrdinal = 2, Line = 1, EndLine = 2 } }],
                Extraction = [new() { Kind = "code", CompleteWithinLimit = true }] };
            switch (scenario)
            {
                case 2: preview.Entry.ResultId = "e23"; break;
                case 3: preview.Parts[0].Location = new NotebookLocation { CellOrdinal = 3, Line = 1, EndLine = 2 }; break;
                case 4: preview.Parts[0].Location = new NotebookLocation { CellOrdinal = 2, Line = 1, EndLine = 3 }; break;
                case 5: preview.Parts[0].Location = new LineLocation { Line = 1 }; break;
                case 6: preview.Parts[0].Kind = "doc"; break;
                case 7: preview.Extraction[0].Kind = "doc"; break;
                case 8: preview.Parts[0].Text = string.Concat(Enumerable.Repeat("🌿", 17000)); break;
                case 9: preview.Entry.IndexRevision = null; break;
                case 10: preview.Entry.ContentSha256 = new string('A', 64); break;
                case 11: preview.Parts[0].Text = "first\n"; break;
                case 12: preview.Parts[0].Location = null; break;
            }
            await WriteAsync(server, new EntryPreviewMessage { Id = request.Id, Preview = preview }, token);
        }
        var serving = Serve(); await using var client = new EngineClient(pipe);
        var request = new PreviewRequest { Reference = new() { ResultId = "c23", SourceRevision = "proof" }, Location = scenario == 0
            ? new LineLocation { Line = 2 } : new NotebookLocation { CellOrdinal = 2, Line = 2, EndLine = 2 } };
        var pending = client.PreviewTextAsync(request, token); EntryPreview? reply = null; Exception? error;
        try
        {
            if (scenario == 1 && await Task.WhenAny(pending, hello.Task) == hello.Task)
            {
                request.Reference.ResultId = "c999"; Assert.IsType<NotebookLocation>(request.Location).CellOrdinal = 99;
            }
            release.TrySetResult(); error = await Record.ExceptionAsync(async () => { reply = await pending; });
        }
        finally { timeout.Cancel(); try { await serving; } catch (OperationCanceledException) { } }
        Assert.True(received, "the code preview response must be validated after a real request");
        if (scenario <= 1) { Assert.Null(error); Assert.Equal(PreviewState.Ready, Assert.IsType<EntryPreview>(reply).State); }
        else Assert.Equal(ErrorCode.InvalidRequest, Assert.IsType<EngineException>(error).Code);
    }

    [Fact]
    public async Task CodePreviewOrdinaryTrailingTerminatorDoesNotClaimAnotherLine()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-preview-newline-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["entry.preview.text"] }, token);
            var request = Assert.IsType<PreviewEntryMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new EntryPreviewMessage { Id = request.Id, Preview = new() {
                Entry = new() { ResultId = "e23", RootPath = @"C:\fixture", Path = @"C:\fixture\text.txt", Name = "text.txt",
                    Kind = ResultKind.Document, IndexRevision = 1, SourceRevision = "proof" }, State = PreviewState.Ready,
                Parts = [new() { Kind = "doc", Text = "first\nsecond\n", Location = new LineLocation { Line = 1 } }] } }, token);
        }
        var serving = Serve(); await using var client = new EngineClient(pipe);
        var error = await Record.ExceptionAsync(() => client.PreviewTextAsync(new() { Reference = new() {
            ResultId = "e23", SourceRevision = "proof" }, Location = new LineLocation { Line = 3 } }, token));
        await serving; Assert.Equal(ErrorCode.InvalidRequest, Assert.IsType<EngineException>(error).Code);
    }

    [Fact]
    public async Task BoundReferencesRejectInvalidInputsWithoutConnecting()
    {
        await using var client = new EngineClient("unused-" + Guid.NewGuid());
        foreach (var reference in new[] { "e0", "e01", "e+1", "c-1", "s42", "code:42:1", "e9223372036854775808" }
            .Select(id => new EntryReference { ResultId = id }).Concat([
                new() { ResultId = "c42", SourceRevision = " " },
                new() { ResultId = "e42", SourceRevision = string.Concat(Enumerable.Repeat("🌿", 17000)) }]))
        {
            var error = await Assert.ThrowsAsync<EngineException>(() => client.ResolveEntryAsync(reference));
            Assert.Equal(ErrorCode.InvalidRequest, error.Code);
        }
    }

    [Fact]
    public async Task BoundReferencesRequireCodeCapabilityAndKeepControlTrafficUsable()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-code-legacy-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["entry.resolve"] }, token);
            var request = await ReadAsync(server, token);
            if (request is ResolveEntryMessage unexpected)
            {
                await WriteAsync(server, new ResolvedEntryMessage { Id = unexpected.Id, Entry = new() {
                    ResultId = "c42", Kind = ResultKind.Code, Name = "code.rs", RootPath = @"C:\repo",
                    Path = @"C:\repo\code.rs", SourceRevision = "proof" } }, token);
                request = await ReadAsync(server, token);
            }
            var status = Assert.IsType<GetStatusMessage>(request);
            await WriteAsync(server, new StatusMessage { Id = status.Id, Status = new() }, token);
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        var error = await Record.ExceptionAsync(() => client.ResolveEntryAsync(new() { ResultId = "c42" }, token));
        await client.GetStatusAsync(token); await serving;
        Assert.Equal(ErrorCode.Unsupported, Assert.IsType<EngineException>(error).Code);
        Assert.Equal(0, Pending(client, "_unary"));
    }

    [Theory]
    [InlineData(0)] [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)] [InlineData(5)]
    public async Task BoundReferencesValidateCodeMetadataHashAndDomain(int scenario)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-code-metadata-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["entry.resolve", "entry.resolve.code"] }, token);
            var request = Assert.IsType<ResolveEntryMessage>(await ReadAsync(server, token));
            var entry = new { result_id = "c42", kind = scenario == 3 ? "file" : "code", name = "code.rs",
                root_path = @"C:\repo", path = @"C:\repo\code.rs", size_bytes = 7,
                source_revision = scenario == 4 ? new string('x', 65537) : "proof",
                index_revision = scenario == 2 ? (long?)null : 7,
                content_sha256 = scenario == 1 ? new string('A', 64) : scenario == 5 ? "incorrect" : new string('a', 64) };
            await WriteRawAsync(server, new { type = "resolved_entry", id = request.Id, entry }, token);
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        var result = client.ResolveEntryAsync(new() { ResultId = "c42", SourceRevision = scenario == 4 ? null : "proof" }, token);
        if (scenario == 0)
        {
            var entry = await result;
            using var json = JsonDocument.Parse(JsonSerializer.Serialize(entry, ProtocolJson.Options));
            Assert.Equal(new string('a', 64), json.RootElement.GetProperty("content_sha256").GetString());
        }
        else Assert.Equal(ErrorCode.InvalidRequest, (await Assert.ThrowsAsync<EngineException>(() => result)).Code);
        await serving;
    }

    [Theory]
    [InlineData(0)] [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)]
    [InlineData(5)] [InlineData(6)] [InlineData(7)] [InlineData(8)] [InlineData(9)] [InlineData(10)]
    public async Task BoundReferencesValidateStreamDomainsAndKeepLegacyResults(int scenario)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-bound-stream-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["search.entry_references"] }, token);
            var request = Assert.IsType<SearchMessage>(await ReadAsync(server, token));
            object? reference = scenario == 0 ? null : new { result_id = scenario == 2 ? "e042" : scenario is 6 or 8 or 9 ? "c42" : "e42",
                source_revision = scenario == 3 ? null : scenario == 4 ? " " : scenario == 5 ? new string('x', 65537) : "proof" };
            var hit = new { id = "query-specific-hit", kind = scenario == 7 ? "application" : scenario >= 8 ? "code" : "file", entry_reference = reference };
            await WriteRawAsync(server, new { type = "results", id = request.Id, batch = new[] { hit }, done = true }, token);
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        async Task<SearchBatch> ReadBatch()
        {
            await foreach (var batch in client.SearchAsync(new() { Mode = scenario >= 9 ? SearchMode.Code : SearchMode.Auto }, token)) return batch;
            throw new Xunit.Sdk.XunitException("missing batch");
        }
        if (scenario is 0 or 1 or 9) Assert.Equal("query-specific-hit", Assert.Single((await ReadBatch()).Results).Id);
        else Assert.Equal(ErrorCode.InvalidRequest, (await Assert.ThrowsAsync<EngineException>(ReadBatch)).Code);
        Assert.Equal(0, Pending(client, "_streams")); await serving;
    }

    private static async Task WriteRawAsync(Stream stream, object message, CancellationToken token)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(message);
        await stream.WriteAsync(BitConverter.GetBytes(payload.Length), token);
        await stream.WriteAsync(payload, token); await stream.FlushAsync(token);
    }

    [Fact]
    public async Task PreviewRequiresCapabilityBeforeSendingAndClosesTheLegacyConnection()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var pipe = "winsearch-preview-legacy-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(timeout.Token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, timeout.Token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1 }, timeout.Token);
            Assert.Equal(0, await server.ReadAsync(new byte[1], timeout.Token));
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        try
        {
            var error = await Assert.ThrowsAsync<EngineException>(() => client.PreviewTextAsync(
                new() { Reference = new() { ResultId = "e23" } }, timeout.Token));
            Assert.Equal(ErrorCode.Unsupported, error.Code);
            await serving;
        }
        finally
        {
            timeout.Cancel();
            try { await serving; } catch (OperationCanceledException) { }
        }
    }

    [Fact]
    public async Task PreviewRejectsInvalidNumericLocationsBeforeConnecting()
    {
        await using var client = new EngineClient("unused-preview-" + Guid.NewGuid());
        foreach (var location in new MatchLocation[] {
            new PageLocation { Page = 0 }, new PageLocation { Page = 5001 },
            new LineLocation { Line = 0 }, new LineLocation { Line = 64 * 1024 * 1024 + 1 },
            new TimestampLocation { Seconds = double.NaN, EndSeconds = 0 },
            new TimestampLocation { Seconds = 1, EndSeconds = 0 },
            new TimestampLocation { Seconds = 0, EndSeconds = double.PositiveInfinity } })
        {
            var error = await Assert.ThrowsAsync<EngineException>(() => client.PreviewTextAsync(
                new() { Reference = new() { ResultId = "e23" }, Location = location }));
            Assert.Equal(ErrorCode.InvalidRequest, error.Code);
        }
    }

    [Fact]
    public async Task PreviewCancellationClosesItsOwnedPipeAndReleasesTheReadSlot()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        var pipe = "winsearch-preview-cancel-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(timeout.Token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, timeout.Token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["entry.preview.text"] }, timeout.Token);
            Assert.IsType<PreviewEntryMessage>(await ReadAsync(server, timeout.Token));
            received.TrySetResult();
            Assert.Equal(0, await server.ReadAsync(new byte[1], timeout.Token));
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        try
        {
            var pending = client.PreviewTextAsync(new() { Reference = new() { ResultId = "e23" } }, cancel.Token);
            await received.Task.WaitAsync(timeout.Token);
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
            await serving;
            var slots = (SemaphoreSlim)typeof(EngineClient).GetField("_previewSlots", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)!;
            Assert.Equal(2, slots.CurrentCount);
        }
        finally
        {
            timeout.Cancel();
            try { await serving; } catch (OperationCanceledException) { }
        }
    }

    [Fact]
    public async Task PreviewSnapshotsArgumentsRejectsMalformedRepliesAndClosesItsOwnedConnection()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = timeout.Token;
        var pipe = "winsearch-preview-" + Guid.NewGuid();
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task Serve()
        {
            for (var index = 0; index < 11; index++)
            {
                using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
                await server.WaitForConnectionAsync(token);
                Assert.IsType<HelloMessage>(await ReadAsync(server, token));
                await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["entry.preview.text"] }, token);
                var request = Assert.IsType<PreviewEntryMessage>(await ReadAsync(server, token));
                Assert.Equal("e23", request.Request.Reference.ResultId);
                Assert.Equal("proof", request.Request.Reference.SourceRevision);
                Assert.Equal((uint)8, Assert.IsType<PageLocation>(request.Request.Location).Page);
                if (index == 0) { received.TrySetResult(); await release.Task.WaitAsync(token); }
                var preview = new EntryPreview {
                    Entry = new() { ResultId = "e23", RootPath = @"C:\fixture", Path = @"C:\fixture\report.pdf",
                        Name = "report.pdf", Kind = ResultKind.Document, SourceRevision = "proof", IndexRevision = 3 },
                    State = PreviewState.Ready, Parts = [new() { Kind = "pdf", Text = "actual page eight", Location = new PageLocation { Page = 8 } }] };
                switch (index)
                {
                    case 1: preview.Entry.ResultId = "e24"; break;
                    case 2: preview.Parts[0].Text = new string('界', 30000); break;
                    case 3: preview.Parts[0].Location = new PageLocation { Page = 9 }; break;
                    case 4: preview.State = PreviewState.NoCachedText; break;
                    case 5: preview.Parts = null!; break;
                    case 6: preview.Entry = null!; break;
                    case 7: preview.Parts = Enumerable.Repeat(preview.Parts[0], 65).ToList(); break;
                    case 8: preview.Parts[0].Kind = "C:/must-not-open"; break;
                    case 9: preview.Entry.SourceRevision = "stale"; break;
                    case 10: preview = null!; break;
                }
                await WriteAsync(server, new EntryPreviewMessage { Id = request.Id, Preview = preview }, token);
                Assert.Equal(0, await server.ReadAsync(new byte[1], token));
            }
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        try
        {
            var request = new PreviewRequest { Reference = new() { ResultId = "e23", SourceRevision = "proof" }, Location = new PageLocation { Page = 8 } };
            var pending = client.PreviewTextAsync(request, token);
            if (await Task.WhenAny(pending, received.Task).WaitAsync(token) == pending) await pending;
            await received.Task.WaitAsync(token);
            request.Reference.ResultId = "e99";
            ((PageLocation)request.Location).Page = 99;
            release.TrySetResult();
            Assert.Equal("actual page eight", (await pending).Parts[0].Text);
            for (var index = 1; index < 11; index++)
                await Assert.ThrowsAsync<EngineException>(() => client.PreviewTextAsync(new() {
                    Reference = new() { ResultId = "e23", SourceRevision = "proof" }, Location = new PageLocation { Page = 8 } }, token));
            await serving;
        }
        finally
        {
            timeout.Cancel();
            try { await serving; } catch (OperationCanceledException) { }
        }
    }

    private static async Task<ClientMessage> ReadAsync(Stream stream, CancellationToken token)
    {
        var header = new byte[4]; await stream.ReadExactlyAsync(header, token);
        var payload = new byte[BitConverter.ToInt32(header)]; await stream.ReadExactlyAsync(payload, token);
        return JsonSerializer.Deserialize<ClientMessage>(payload, ProtocolJson.Options)!;
    }
    private static async Task WriteAsync(Stream stream, ServerMessage message, CancellationToken token)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(message, ProtocolJson.Options);
        await stream.WriteAsync(BitConverter.GetBytes(payload.Length), token);
        await stream.WriteAsync(payload, token);
        await stream.FlushAsync(token);
    }
    private static int Pending(EngineClient client, string field) =>
        ((IDictionary)typeof(EngineClient).GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)!).Count;

    [Fact]
    public async Task EntryResolutionDispatchesMetadataAndRejectsAnotherIdentity()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = timeout.Token;
        var pipe = "winsearch-entry-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["entry.resolve"] }, token);
            var request = Assert.IsType<ResolveEntryMessage>(await ReadAsync(server, token));
            Assert.Equal("e23", request.Reference.ResultId);
            Assert.Equal("opaque-native-proof", request.Reference.SourceRevision);
            received.TrySetResult();
            await release.Task.WaitAsync(token);
            await WriteAsync(server, new ResolvedEntryMessage { Id = request.Id,
                Entry = new() { ResultId = "e23", SourceRevision = "opaque-native-proof", SizeBytes = 1234,
                    RootPath = @"C:\fixture", Path = @"C:\fixture\report.pdf", Name = "report.pdf", Kind = ResultKind.Document } }, token);
            request = Assert.IsType<ResolveEntryMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new ResolvedEntryMessage { Id = request.Id,
                Entry = new() { ResultId = "e24", SourceRevision = "opaque-native-proof" } }, token);
            request = Assert.IsType<ResolveEntryMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new ResolvedEntryMessage { Id = request.Id,
                Entry = new() { ResultId = "e23", SourceRevision = "opaque-native-proof",
                    RootPath = @"C:\fixture", Path = @"C:\outside\secret.txt", Name = "secret.txt", Kind = ResultKind.Document } }, token);
            request = Assert.IsType<ResolveEntryMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new ResolvedEntryMessage { Id = request.Id,
                Entry = new() { ResultId = "e23", RootPath = @"C:\fixture", Path = @"C:\fixture\report.pdf",
                    Name = "report.pdf", Kind = ResultKind.Document } }, token);
            foreach (var entry in new object?[] { null, new { result_id = "e23", source_revision = "proof",
                root_path = (string?)null, path = @"C:\fixture\report.pdf", name = "report.pdf", kind = "document" } })
            {
                request = Assert.IsType<ResolveEntryMessage>(await ReadAsync(server, token));
                var malformed = JsonSerializer.SerializeToUtf8Bytes(new { type = "resolved_entry", id = request.Id, entry });
                await server.WriteAsync(BitConverter.GetBytes(malformed.Length), token);
                await server.WriteAsync(malformed, token);
                await server.FlushAsync(token);
            }
            request = Assert.IsType<ResolveEntryMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new ResolvedEntryMessage { Id = request.Id,
                Entry = new() { ResultId = "e23", RootPath = @"C:\fixture", Path = @"C:\fixture\report.pdf",
                    Name = "report.pdf", Kind = ResultKind.Document, SourceRevision = "other-proof" } }, token);
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        await client.ConnectAsync(token);
        var reference = new EntryReference { ResultId = "e23", SourceRevision = "opaque-native-proof" };
        var resolving = client.ResolveEntryAsync(reference, token);
        await received.Task.WaitAsync(token);
        reference.ResultId = "e24";
        reference.SourceRevision = "different-proof";
        release.TrySetResult();
        var entry = await resolving;
        Assert.Equal("e23", entry.ResultId);
        Assert.Equal(1234UL, entry.SizeBytes);
        await Assert.ThrowsAsync<EngineException>(() => client.ResolveEntryAsync(new() { ResultId = "e23" }, token));
        await Assert.ThrowsAsync<EngineException>(() => client.ResolveEntryAsync(new() { ResultId = "e23" }, token));
        await Assert.ThrowsAsync<EngineException>(() => client.ResolveEntryAsync(new() { ResultId = "e23" }, token));
        await Assert.ThrowsAsync<EngineException>(() => client.ResolveEntryAsync(new() { ResultId = "e23" }, token));
        await Assert.ThrowsAsync<EngineException>(() => client.ResolveEntryAsync(new() { ResultId = "e23" }, token));
        await Assert.ThrowsAsync<EngineException>(() => client.ResolveEntryAsync(new()
            { ResultId = "e23", SourceRevision = "opaque-native-proof" }, token));
        Assert.Equal(0, Pending(client, "_unary"));
        await serving;
    }

    [Fact]
    public async Task SearchLimitsDisposeClearsBudgetsWhileTheConsumerIsPaused()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)); var token = timeout.Token;
        var pipe = "winsearch-search-dispose-budget-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token); await ReadAsync(server, token);
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["search.exact", "search.limits"] }, token);
            var request = Assert.IsType<SearchMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new ResultsMessage { Id = request.Id, Batch = [new()] }, token);
        }
        var serving = Serve(); await using var client = new EngineClient(pipe); await client.ConnectAsync(token);
        await using var iterator = client.SearchAsync(new() { Text = "fixture", Limit = 2 }, token).GetAsyncEnumerator(token);
        Assert.True(await iterator.MoveNextAsync()); await serving;
        Assert.Equal(1, Pending(client, "_streamBudgets"));
        await client.DisposeAsync();
        Assert.Equal(0, Pending(client, "_streams"));
        Assert.Equal(0, Pending(client, "_streamBudgets"));
    }

    [Fact]
    public async Task SearchLimitsBoundAggregateBytesBeforeQueuedFramesGrowWithoutLimit()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = timeout.Token;
        var pipe = "winsearch-search-byte-limit-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["search.exact", "search.limits"] }, token);
            var request = Assert.IsType<SearchMessage>(await ReadAsync(server, token));
            var text = new string('x', 9 * 1024 * 1024);
            for (var i = 0; i < 8; i++)
                await WriteAsync(server, new ResultsMessage { Id = request.Id, Batch = [new() { Id = $"large-{i}", Title = text }], Done = i == 7 }, token);
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        await client.ConnectAsync(token);
        await Assert.ThrowsAsync<EngineException>(async () => { await foreach (var _ in client.SearchAsync(new() { Text = "fixture", Limit = 20 }, token)) { } });
        Assert.Equal(0, Pending(client, "_streams"));
        await serving;
    }

    [Fact]
    public async Task SearchLimitsCapablePeerMustHonorTheRequestedTotal()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = timeout.Token;
        var pipe = "winsearch-search-limit-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1, Features = ["search.exact", "search.limits"] }, token);
            var request = Assert.IsType<SearchMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new ResultsMessage { Id = request.Id, Batch = [new(), new()], Done = true }, token);
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        await client.ConnectAsync(token);
        await Assert.ThrowsAsync<EngineException>(async () => { await foreach (var _ in client.SearchAsync(new() { Text = "fixture", Limit = 1 }, token)) { } });
        Assert.Equal(0, Pending(client, "_streams"));
        await serving;
    }

    [Theory]
    [InlineData(10001u)]
    [InlineData(5000u)]
    public async Task SearchLimitsRejectInvalidOrUnsupportedLargeRequests(uint limit)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = timeout.Token;
        var pipe = "winsearch-search-limit-legacy-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1 }, token);
            var request = await ReadAsync(server, token);
            if (request is SearchMessage unexpected)
            {
                await WriteAsync(server, new ResultsMessage { Id = unexpected.Id, Done = true }, token);
                return;
            }
            var status = Assert.IsType<GetStatusMessage>(request);
            await WriteAsync(server, new StatusMessage { Id = status.Id, Status = new() }, token);
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        await client.ConnectAsync(token);
        await Assert.ThrowsAsync<EngineException>(async () => { await foreach (var _ in client.SearchAsync(new() { Text = "fixture", Limit = limit }, token)) { } });
        await client.GetStatusAsync(token);
        Assert.Equal(0, Pending(client, "_streams"));
        await serving;
    }

    [Fact]
    public async Task RootPoliciesRequireCapabilityBeforeSendingConfiguration()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = timeout.Token;
        var pipe = "winsearch-root-policy-legacy-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1 }, token);
            var request = await ReadAsync(server, token);
            if (request is SetIndexingConfigMessage unexpected)
            {
                await WriteAsync(server, new IndexingConfigMessage { Id = unexpected.Id, Config = unexpected.Config }, token);
                return;
            }
            var status = Assert.IsType<GetStatusMessage>(request);
            await WriteAsync(server, new StatusMessage { Id = status.Id, Status = new() }, token);
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        await client.ConnectAsync(token);
        var config = JsonSerializer.Deserialize<IndexingConfig>("""{"roots":["C:/source"],"root_policies":[{"root":"C:/source","max_file_size_bytes":1024,"exclusions":[]}]}""", ProtocolJson.Options)!;
        await Assert.ThrowsAsync<EngineException>(() => client.SetIndexingConfigAsync(config, token));
        await client.GetStatusAsync(token);
        Assert.Equal(0, Pending(client, "_unary"));
        await serving;
    }

    [Fact]
    public async Task EntryResolutionRequiresTheAdvertisedEngineCapability()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = timeout.Token;
        var pipe = "winsearch-entry-legacy-" + Guid.NewGuid();
        using var server = new NamedPipeServerStream(pipe, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        async Task Serve()
        {
            await server.WaitForConnectionAsync(token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1 }, token);
            var request = await ReadAsync(server, token);
            if (request is ResolveEntryMessage unexpected)
            {
                await WriteAsync(server, new ResolvedEntryMessage { Id = unexpected.Id, Entry = new()
                { ResultId = "e1", RootPath = @"C:\fixture", Path = @"C:\fixture\file.txt",
                    Name = "file.txt", Kind = ResultKind.File, SourceRevision = "proof" } }, token);
                return;
            }
            var status = Assert.IsType<GetStatusMessage>(request);
            await WriteAsync(server, new StatusMessage { Id = status.Id, Status = new() }, token);
        }
        var serving = Serve();
        await using var client = new EngineClient(pipe);
        await client.ConnectAsync(token);
        await Assert.ThrowsAsync<EngineException>(() => client.ResolveEntryAsync(new() { ResultId = "e1" }, token));
        await client.GetStatusAsync(token);
        Assert.Equal(0, Pending(client, "_unary"));
        await serving;
    }

    [Fact]
    public async Task FailedWritesReleaseUnaryAndSearchWaiters()
    {
        await using var client = new EngineClient("unused-" + Guid.NewGuid());
        await Assert.ThrowsAsync<IOException>(() => client.GetIndexingConfigAsync());
        Assert.Equal(0, Pending(client, "_unary"));
        await Assert.ThrowsAsync<IOException>(async () => { await foreach (var _ in client.SearchAsync(new())) { } });
        Assert.Equal(0, Pending(client, "_streams"));
    }

    [Fact]
    public async Task ConcurrentPipeClientsDispatchFullPolicyPushesAndCancellation()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15)); var token = timeout.Token;
        var pipe = "winsearch-dotnet-policy-" + Guid.NewGuid();
        using var server1 = new NamedPipeServerStream(pipe, PipeDirection.InOut, 2, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        using var server2 = new NamedPipeServerStream(pipe, PipeDirection.InOut, 2, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        var received = new System.Collections.Concurrent.ConcurrentDictionary<string, TaskCompletionSource>();
        async Task Serve(NamedPipeServerStream server)
        {
            await server.WaitForConnectionAsync(token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1 }, token);
            var set = Assert.IsType<SetIndexingConfigMessage>(await ReadAsync(server, token));
            var gotRequest = received.GetOrAdd(set.Config.Roots[0], _ => new(TaskCreationOptions.RunContinuationsAsynchronously));
            Assert.Equal(IndexingMode.Scheduled, set.Config.Mode);
            Assert.Equal((ushort)1320, set.Config.HoursStart);
            await WriteAsync(server, new IndexingConfigMessage { Id = set.Id, Config = set.Config }, token);
            await WriteAsync(server, new IndexingConfigMessage { Config = set.Config }, token);
            Assert.IsType<GetIndexingConfigMessage>(await ReadAsync(server, token));
            gotRequest.TrySetResult();
            // Deliberately leave one unary unanswered; cancellation must release it.
            await Task.Delay(100, token);
        }
        var servers = Task.WhenAll(Serve(server1), Serve(server2));
        async Task Exercise(string root)
        {
            await using var client = new EngineClient(pipe);
            var pushed = new TaskCompletionSource<IndexingConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.IndexingConfigChanged += (_, config) => pushed.TrySetResult(config);
            await client.ConnectAsync(token);
            var config = new IndexingConfig { Roots = [root], CodeEnabled = true, CodeRoots = [@"D:\Code"],
                ExcludedPaths = [root + @"\Skip"], ExcludedExtensions = ["pdf"], Mode = IndexingMode.Scheduled, HoursStart = 1320, HoursEnd = 360 };
            var copy = await client.SetIndexingConfigAsync(config, token);
            Assert.Equal(root, Assert.Single(copy.Roots)); Assert.Equal((ushort)360, copy.HoursEnd);
            Assert.Equal(root, Assert.Single((await pushed.Task.WaitAsync(token)).Roots));
            using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(token);
            var request = client.GetIndexingConfigAsync(cancelled.Token);
            await received.GetOrAdd(root, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).Task.WaitAsync(token);
            cancelled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => request);
            Assert.Equal(0, Pending(client, "_unary"));
        }
        await Task.WhenAll(Exercise(@"C:\A"), Exercise(@"C:\B"));
        await servers;
    }
}
