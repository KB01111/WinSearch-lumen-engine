using System.Runtime.CompilerServices;
using System.Threading.Channels;
using WinSearch.Core.Protocol;
using WinSearch.Core.Settings;
using WinSearch.ViewModels;
using Xunit;

namespace WinSearch.Tests;

public sealed class MainOverlayViewModelTests
{
    [Fact]
    public async Task SemanticStreaming_PreservesExactOrder_AppendsSemantic_AndTracksIsSemanticPending()
    {
        var channel = Channel.CreateUnbounded<SearchBatch>();
        var fake = new FakeEngineClient
        {
            CurrentConfig = new EngineConfig { SemanticEnabled = true, PreferredBackend = "auto" },
            CurrentModelStatus = new ModelStatus { State = ModelState.Ready },
            SearchHandler = (_, ct) => ReadBatchesAsync(channel.Reader, ct),
        };

        await using var vm = new MainOverlayViewModel(fake, new AppSettings());
        await vm.InitializeAsync();

        var searchTask = vm.RunSearchAsync("meeting");

        // Emit exact batch (Done = false because semantic stage follows)
        channel.Writer.TryWrite(new SearchBatch
        {
            Results =
            [
                new SearchResult
                {
                    Id = "e1",
                    Kind = ResultKind.Document,
                    Title = "meeting-notes.txt",
                    Subtitle = @"C:\Docs\meeting-notes.txt",
                    MatchExplanation = "Exact prefix",
                    Score = 0.92,
                },
                new SearchResult
                {
                    Id = "e2",
                    Kind = ResultKind.Document,
                    Title = "meeting-agenda.txt",
                    Subtitle = @"C:\Docs\meeting-agenda.txt",
                    MatchExplanation = "Filename token",
                    Score = 0.55,
                },
            ],
            Done = false,
        });

        await WaitUntilAsync(() => vm.Results.Count == 2);

        // While waiting for semantic stage, IsSemanticPending must be true and exact results untouched
        Assert.True(vm.IsSemanticPending);
        Assert.False(vm.IsSearching);
        Assert.Equal(["e1", "e2"], vm.Results.Select(r => r.Result.Id).ToArray());

        // Emit semantic batch (Done = true) with higher score than e2 — must append without reordering!
        channel.Writer.TryWrite(new SearchBatch
        {
            Results =
            [
                new SearchResult
                {
                    Id = "s99",
                    Kind = ResultKind.Document,
                    Title = "banquet-hall.txt",
                    Subtitle = @"C:\Docs\banquet-hall.txt",
                    MatchExplanation = "semantic name match · similarity 0.88",
                    Score = 0.89,
                },
            ],
            Done = true,
        });
        channel.Writer.TryComplete();

        await searchTask;

        Assert.False(vm.IsSemanticPending);
        Assert.False(vm.IsSearching);
        Assert.Equal(["e1", "e2", "s99"], vm.Results.Select(r => r.Result.Id).ToArray());
        Assert.Equal("semantic name match · similarity 0.88", vm.Results[2].Explanation);
        Assert.Equal("3 results", vm.StatusText);
    }

    [Fact]
    public async Task MissingModelHint_ShowsWhenModelNotReady_AndHidesWhenReadyOrCodeOrDisabled()
    {
        var fake = new FakeEngineClient
        {
            CurrentConfig = new EngineConfig { SemanticEnabled = true, PreferredBackend = "auto" },
            CurrentModelStatus = new ModelStatus { State = ModelState.NotDownloaded },
        };

        await using var vm = new MainOverlayViewModel(fake, new AppSettings());
        await vm.InitializeAsync();

        // Empty query -> no hint
        Assert.Equal("", vm.SemanticHintText);

        // Normal query when model is missing -> exact hint shown
        vm.QueryText = "budget report";
        Assert.Equal(MainOverlayViewModel.MissingModelHintText, vm.SemanticHintText);
        Assert.Equal("Semantic search needs the AI model — Settings → AI model", vm.SemanticHintText);

        // code: query -> no semantic hint
        vm.QueryText = "code: retry backoff";
        Assert.Equal("", vm.SemanticHintText);

        // Back to normal query -> hint returns
        vm.QueryText = "budget report";
        Assert.Equal(MainOverlayViewModel.MissingModelHintText, vm.SemanticHintText);

        // Model becomes Ready -> hint disappears live
        fake.PushModelStatus(new ModelStatus { State = ModelState.Ready, Bytes = 100, TotalBytes = 100 });
        Assert.Equal("", vm.SemanticHintText);

        // Model deleted -> hint reappears even if a status snapshot reports ModelLoaded = true
        fake.PushModelStatus(new ModelStatus { State = ModelState.NotDownloaded });
        fake.PushStatus(new EngineStatus { State = EngineRunState.Idle, ModelLoaded = true });
        Assert.Equal(MainOverlayViewModel.MissingModelHintText, vm.SemanticHintText);

        // User disables semantic search in Settings -> hint disappears and SearchQuery.IncludeSemantic is false
        fake.PushConfig(new EngineConfig { SemanticEnabled = false, PreferredBackend = "auto" });
        Assert.Equal("", vm.SemanticHintText);

        // Typing arms HasPendingSearch; SearchNowAsync flushes immediately and clears HasPendingSearch
        vm.QueryText = "immediate query";
        Assert.True(vm.HasPendingSearch);
        await vm.SearchNowAsync();
        Assert.False(vm.HasPendingSearch);
        Assert.False(fake.SearchCalls.Last().IncludeSemantic);
    }

    [Fact]
    public async Task SupersededSearch_ClearsIsSemanticPending_AndDiscardsStaleBatches()
    {
        var firstChannel = Channel.CreateUnbounded<SearchBatch>();
        var secondChannel = Channel.CreateUnbounded<SearchBatch>();
        var callIndex = 0;

        var fake = new FakeEngineClient
        {
            CurrentConfig = new EngineConfig { SemanticEnabled = true, PreferredBackend = "auto" },
            CurrentModelStatus = new ModelStatus { State = ModelState.Ready },
            SearchHandler = (_, ct) =>
            {
                var current = Interlocked.Increment(ref callIndex);
                return ReadBatchesAsync(current == 1 ? firstChannel.Reader : secondChannel.Reader, ct);
            },
        };

        await using var vm = new MainOverlayViewModel(fake, new AppSettings());
        await vm.InitializeAsync();

        var firstSearch = vm.RunSearchAsync("first");
        firstChannel.Writer.TryWrite(new SearchBatch
        {
            Results = [new SearchResult { Id = "e1", Title = "first.txt", Score = 0.9 }],
            Done = false,
        });

        await WaitUntilAsync(() => vm.IsSemanticPending);
        Assert.True(vm.IsSemanticPending);

        // Start a second search before the first semantic batch arrives
        var secondSearch = vm.RunSearchAsync("second");
        secondChannel.Writer.TryWrite(new SearchBatch
        {
            Results = [new SearchResult { Id = "e2", Title = "second.txt", Score = 0.95 }],
            Done = true,
        });
        secondChannel.Writer.TryComplete();

        await secondSearch;

        // Complete the stale first channel afterward — must be ignored
        firstChannel.Writer.TryWrite(new SearchBatch
        {
            Results = [new SearchResult { Id = "s1", Title = "stale-semantic.txt", Score = 0.85 }],
            Done = true,
        });
        firstChannel.Writer.TryComplete();
        await firstSearch;

        Assert.False(vm.IsSemanticPending);
        Assert.Single(vm.Results);
        Assert.Equal("e2", vm.Results[0].Result.Id);
    }

    [Fact]
    public async Task FlushOrWaitForSearchAsync_DoesNotMissInFlightTaskWhenDebounceStartsRunning()
    {
        var channel = Channel.CreateUnbounded<SearchBatch>();
        var flushStarted = new TaskCompletionSource<Task>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task? flushTask = null;
        MainOverlayViewModel? vmRef = null;

        var fake = new FakeEngineClient
        {
            CurrentConfig = new EngineConfig { SemanticEnabled = false, PreferredBackend = "auto" },
            SearchHandler = (_, ct) =>
            {
                // Called synchronously inside RunSearchCoreAsync before its first await.
                // Start FlushOrWaitForSearchAsync right at this suspension point to verify
                // it never observes HasPendingSearch == false before _activeSearchTask is set.
                flushTask = Task.Run(() =>
                {
                    var pendingFlush = vmRef!.FlushOrWaitForSearchAsync();
                    flushStarted.SetResult(pendingFlush);
                    return pendingFlush;
                });
                return ReadBatchesAsync(channel.Reader, ct);
            },
        };

        await using var vm = new MainOverlayViewModel(fake, new AppSettings());
        vmRef = vm;

        vm.QueryText = "race";
        var runTask = vm.RunSearchAsync("race");

        Assert.NotNull(flushTask);
        var pendingFlush = await flushStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.False(pendingFlush.IsCompleted);
        Assert.False(flushTask!.IsCompleted);

        channel.Writer.TryWrite(new SearchBatch
        {
            Results = [new SearchResult { Id = "r1", Title = "race.txt", Score = 1.0 }],
            Done = true,
        });
        channel.Writer.TryComplete();

        await Task.WhenAll(runTask, flushTask);
        Assert.Single(vm.Results);
        Assert.Equal("r1", vm.Results[0].Result.Id);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FlushOrWaitForSearchAsync_FollowsQueryChangesWhileWaiting(bool debounceElapsed)
    {
        var first = Channel.CreateUnbounded<SearchBatch>();
        var second = Channel.CreateUnbounded<SearchBatch>();
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fake = new FakeEngineClient
        {
            SearchHandler = (q, ct) =>
            {
                if (q.Text == "second") secondStarted.TrySetResult();
                return ReadBatchesAsync(q.Text == "first" ? first.Reader : second.Reader, ct);
            },
        };
        await using var vm = new MainOverlayViewModel(fake, new AppSettings());
        vm.QueryText = "first";
        var search = vm.SearchNowAsync();
        var flush = vm.FlushOrWaitForSearchAsync();
        vm.QueryText = "second";
        var replacement = debounceElapsed ? vm.SearchNowAsync() : Task.CompletedTask;
        first.Writer.TryComplete();
        await search.WaitAsync(TimeSpan.FromSeconds(2));
        await secondStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.False(flush.IsCompletedSuccessfully);
        second.Writer.TryWrite(new SearchBatch
        {
            Results = [new SearchResult { Id = "second", Title = "second.txt" }],
            Done = true,
        });
        second.Writer.TryComplete();
        await Task.WhenAll(flush, replacement).WaitAsync(TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => vm.Results.Count == 1);
        Assert.Equal("second", Assert.Single(vm.Results).Result.Id);
    }

    [Fact]
    public async Task SupersededDebounceCallback_DoesNotStartOrCancelSearch()
    {
        var channel = Channel.CreateUnbounded<SearchBatch>();
        var fake = new FakeEngineClient
        {
            SearchHandler = (_, ct) => ReadBatchesAsync(channel.Reader, ct),
        };
        await using var vm = new MainOverlayViewModel(fake, new AppSettings());

        const System.Reflection.BindingFlags flags =
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic;
        var stateLock = typeof(MainOverlayViewModel).GetField("_searchStateLock", flags)!.GetValue(vm)!;
        var generationField = typeof(MainOverlayViewModel).GetField("_debounceGeneration", flags)!;
        var timerField = typeof(MainOverlayViewModel).GetField("_debounceTimer", flags)!;
        var callback = typeof(MainOverlayViewModel).GetMethod("OnDebounceElapsed", flags)!;
        Task searchTask;

        // Hold the state lock to control callback delivery without timer delays.
        lock (stateLock)
        {
            vm.QueryText = "old";
            var staleGeneration = generationField.GetValue(vm);
            vm.QueryText = "current";
            var currentTimer = timerField.GetValue(vm);

            callback.Invoke(vm, [staleGeneration]);
            Assert.Empty(fake.SearchCalls);
            Assert.True(vm.HasPendingSearch);
            Assert.Same(currentTimer, timerField.GetValue(vm));

            searchTask = vm.SearchNowAsync();
            callback.Invoke(vm, [staleGeneration]);
            Assert.Equal("current", Assert.Single(fake.SearchCalls).Text);
            Assert.False(vm.HasPendingSearch);
        }

        channel.Writer.TryComplete();
        await searchTask.WaitAsync(TimeSpan.FromSeconds(2));
    }

    private static async IAsyncEnumerable<SearchBatch> ReadBatchesAsync(
        ChannelReader<SearchBatch> reader,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var batch in reader.ReadAllAsync(cancellationToken))
        {
            yield return batch;
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 2000)
    {
        var start = Environment.TickCount64;
        while (!condition())
        {
            if (Environment.TickCount64 - start > timeoutMs)
            {
                throw new TimeoutException("Condition was not met within timeout.");
            }
            await Task.Delay(10);
        }
    }
}
