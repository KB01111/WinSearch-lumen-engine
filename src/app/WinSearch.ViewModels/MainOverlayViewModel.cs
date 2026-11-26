using System.Collections.ObjectModel;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using WinSearch.Core.Ipc;
using WinSearch.Core.Protocol;
using WinSearch.Core.Settings;

namespace WinSearch.ViewModels;

/// <summary>
/// State and behaviour of the search overlay: progressive search with
/// debounce, cancellation of superseded queries, and stable result updates.
/// Exact matches stream in first; semantic batches append later without
/// disturbing the user's selection (the view keeps selection by result id).
/// </summary>
public sealed partial class MainOverlayViewModel : ObservableObject, IAsyncDisposable
{
    public const string MissingModelHintText =
        "Semantic search needs the AI model — Settings → AI model";

    private const int DebounceMs = 120;

    private readonly IEngineClient _client;
    private readonly AppSettings _settings;
    private readonly SynchronizationContext? _ui;
    private readonly object _searchStateLock = new();
    private CancellationTokenSource? _searchCts;
    private Timer? _debounceTimer;
    private long _debounceGeneration;
    private bool _debouncePending;
    private Task? _activeSearchTask;
    private long _searchSequence;
    private string _pendingQuery = "";
    private bool _semanticEnabled = true;
    private ModelState _modelState = ModelState.NotDownloaded;
    private bool _disposed;

    public MainOverlayViewModel(IEngineClient client, AppSettings settings)
    {
        _client = client;
        _settings = settings;
        _ui = SynchronizationContext.Current;
        _client.StatusChanged += OnStatusChanged;
        _client.ModelStatusChanged += OnModelStatusChanged;
        _client.ConfigChanged += OnConfigChanged;
        _client.Reconnected += OnReconnected;
    }

    public ObservableCollection<ResultItemViewModel> Results { get; } = new();

    [ObservableProperty]
    private string _queryText = "";

    [ObservableProperty]
    private string _statusText = "";

    [ObservableProperty]
    private string _modeLabel = "All";

    [ObservableProperty]
    private bool _isSearching;

    /// <summary>True when semantic retrieval is running on top of exact results.</summary>
    [ObservableProperty]
    private bool _isSemanticPending;

    /// <summary>
    /// Hint shown when semantic search is enabled in engine policy, the user is
    /// running a non-code search, and the AI model is not ready.
    /// </summary>
    [ObservableProperty]
    private string _semanticHintText = "";

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var config = await _client.GetConfigAsync(cancellationToken);
            Dispatch(() => ApplyConfig(config));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Connection may still be establishing; Reconnected refreshes state.
        }

        try
        {
            var modelStatus = await _client.GetModelStatusAsync(cancellationToken);
            Dispatch(() => ApplyModelStatus(modelStatus));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
        }
    }

    public void ApplyConfig(EngineConfig config)
    {
        _semanticEnabled = config.SemanticEnabled;
        UpdateSemanticHint();
    }

    public void ApplyModelStatus(ModelStatus status)
    {
        _modelState = status.State;
        UpdateSemanticHint();
    }

    private void UpdateSemanticHint()
    {
        var query = QueryText;
        var isCode = query.StartsWith("code:", StringComparison.OrdinalIgnoreCase);
        var hasQuery = !string.IsNullOrWhiteSpace(query);

        if (hasQuery && !isCode && _semanticEnabled && _modelState != ModelState.Ready)
        {
            SemanticHintText = MissingModelHintText;
        }
        else
        {
            SemanticHintText = "";
        }
    }

    partial void OnQueryTextChanged(string value)
    {
        lock (_searchStateLock)
        {
            _pendingQuery = value;
            Volatile.Write(ref _debouncePending, true);
            _debounceTimer?.Dispose();
            var generation = ++_debounceGeneration;
            _debounceTimer = new Timer(
                _ => OnDebounceElapsed(generation),
                null,
                DebounceMs,
                Timeout.Infinite);
        }
        UpdateSemanticHint();
    }

    private void OnDebounceElapsed(long generation)
    {
        lock (_searchStateLock)
        {
            if (generation != _debounceGeneration || !_debouncePending)
            {
                return;
            }
            _ = RunSearchAsync(_pendingQuery);
        }
    }

    /// <summary>True when a query change is waiting on the debounce timer.</summary>
    public bool HasPendingSearch => Volatile.Read(ref _debouncePending);

    /// <summary>Flushes any pending debounced query or awaits an in-flight search.</summary>
    public async Task FlushOrWaitForSearchAsync()
    {
        while (true)
        {
            Task? toAwait;
            lock (_searchStateLock)
            {
                toAwait = _debouncePending ? RunSearchAsync(QueryText) : _activeSearchTask;
            }
            if (toAwait is not null)
            {
                await toAwait;
            }
            lock (_searchStateLock)
            {
                if (!_debouncePending && ReferenceEquals(toAwait, _activeSearchTask))
                {
                    return;
                }
            }
        }
    }

    /// <summary>Runs immediately (Enter / mode switch), bypassing the debounce.</summary>
    [RelayCommand]
    public Task SearchNowAsync() => RunSearchAsync(QueryText);

    public Task RunSearchAsync(string query)
    {
        lock (_searchStateLock)
        {
            ++_debounceGeneration;
            _debounceTimer?.Dispose();
            _debounceTimer = null;
            var task = RunSearchCoreAsync(query);
            _activeSearchTask = task;
            Volatile.Write(ref _debouncePending, false);
            return task;
        }
    }

    private async Task RunSearchCoreAsync(string query)
    {
        if (_disposed)
        {
            return;
        }

        _searchCts?.Cancel();
        _searchCts?.Dispose();
        _searchCts = new CancellationTokenSource();
        var ct = _searchCts.Token;
        var sequence = Interlocked.Increment(ref _searchSequence);

        if (string.IsNullOrWhiteSpace(query))
        {
            Dispatch(() =>
            {
                if (sequence != Interlocked.Read(ref _searchSequence))
                {
                    return;
                }
                Results.Clear();
                IsSearching = false;
                IsSemanticPending = false;
                StatusText = "";
                UpdateSemanticHint();
            });
            return;
        }

        var mode = query.StartsWith("code:", StringComparison.OrdinalIgnoreCase)
            ? SearchMode.Code
            : SearchMode.Auto;
        var effectiveText = mode == SearchMode.Code ? query["code:".Length..].Trim() : query;
        var includeSemantic = mode != SearchMode.Code && _semanticEnabled;

        Dispatch(() =>
        {
            if (sequence != Interlocked.Read(ref _searchSequence))
            {
                return;
            }
            Results.Clear();
            IsSearching = true;
            IsSemanticPending = false;
            ModeLabel = mode == SearchMode.Code ? "Code" : "All";
            StatusText = "Searching…";
            UpdateSemanticHint();
        });

        var searchQuery = new SearchQuery
        {
            Text = effectiveText,
            Mode = mode,
            Limit = 20,
            // Semantic batches append after the exact results; engine policy
            // and model readiness gate the second stage server-side.
            IncludeSemantic = includeSemantic,
        };

        try
        {
            var seen = 0;
            await foreach (var batch in _client.SearchAsync(searchQuery, ct))
            {
                if (sequence != Interlocked.Read(ref _searchSequence))
                {
                    return; // superseded by a newer query
                }

                var items = batch.Results.Select(r => new ResultItemViewModel(r)).ToList();
                seen += items.Count;
                var done = batch.Done;
                var pendingSemantic = includeSemantic && !done;
                Dispatch(() =>
                {
                    if (sequence != Interlocked.Read(ref _searchSequence))
                    {
                        return;
                    }
                    // Append-only: never clear or reorder already-displayed exact results.
                    foreach (var item in items)
                    {
                        Results.Add(item);
                    }
                    IsSearching = false;
                    IsSemanticPending = pendingSemantic;
                    StatusText = seen == 0
                        ? (pendingSemantic ? "Searching semantic matches…" : "No matches")
                        : $"{seen} result{(seen == 1 ? "" : "s")}";
                });
            }

            if (sequence == Interlocked.Read(ref _searchSequence))
            {
                Dispatch(() =>
                {
                    if (sequence != Interlocked.Read(ref _searchSequence))
                    {
                        return;
                    }
                    IsSearching = false;
                    IsSemanticPending = false;
                });
            }
        }
        catch (OperationCanceledException)
        {
            // Superseded query: if no newer query took over, clear pending flags.
            if (sequence == Interlocked.Read(ref _searchSequence))
            {
                Dispatch(() =>
                {
                    if (sequence != Interlocked.Read(ref _searchSequence))
                    {
                        return;
                    }
                    IsSearching = false;
                    IsSemanticPending = false;
                });
            }
        }
        catch (EngineException ex)
        {
            Dispatch(() =>
            {
                if (sequence != Interlocked.Read(ref _searchSequence))
                {
                    return;
                }
                IsSearching = false;
                IsSemanticPending = false;
                StatusText = ex.Code == ErrorCode.NotReady ? "Search is not ready yet" : ex.Message;
            });
        }
        catch (IOException)
        {
            Dispatch(() =>
            {
                if (sequence != Interlocked.Read(ref _searchSequence))
                {
                    return;
                }
                IsSearching = false;
                IsSemanticPending = false;
                StatusText = "Lost connection to the search engine";
            });
        }
    }

    private void OnStatusChanged(object? sender, EngineStatus status)
    {
        Dispatch(() =>
        {
            if (!IsSearching && !IsSemanticPending && string.IsNullOrWhiteSpace(QueryText))
            {
                StatusText = status.State switch
                {
                    EngineRunState.Indexing =>
                        $"Indexing… {status.FilesIndexed:N0} of {status.FilesDiscovered:N0} files",
                    EngineRunState.Paused => "Indexing paused",
                    EngineRunState.Error => "Indexing error — see logs",
                    _ when status.FilesIndexed > 0 => $"{status.FilesIndexed:N0} files indexed",
                    _ => "",
                };
            }
        });
    }

    private void OnModelStatusChanged(object? sender, ModelStatus status)
    {
        Dispatch(() => ApplyModelStatus(status));
    }

    private void OnConfigChanged(object? sender, EngineConfig config)
    {
        Dispatch(() => ApplyConfig(config));
    }

    private void OnReconnected(object? sender, EventArgs e)
    {
        _ = InitializeAsync();
    }

    public Task RefreshStatusAsync(CancellationToken cancellationToken = default)
        => _client.GetStatusAsync(cancellationToken);

    private void Dispatch(Action action)
    {
        if (_ui is null || SynchronizationContext.Current == _ui)
        {
            action();
        }
        else
        {
            _ui.Post(_ => action(), null);
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }
        _disposed = true;
        _client.StatusChanged -= OnStatusChanged;
        _client.ModelStatusChanged -= OnModelStatusChanged;
        _client.ConfigChanged -= OnConfigChanged;
        _client.Reconnected -= OnReconnected;
        _debounceTimer?.Dispose();
        _searchCts?.Cancel();
        _searchCts?.Dispose();
        return ValueTask.CompletedTask;
    }
}
