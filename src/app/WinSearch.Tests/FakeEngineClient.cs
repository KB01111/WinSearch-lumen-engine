using System.Runtime.CompilerServices;
using System.Threading.Channels;
using WinSearch.Core.Ipc;
using WinSearch.Core.Protocol;

namespace WinSearch.Tests;

internal sealed class FakeEngineClient : IEngineClient
{
    public event EventHandler<EngineStatus>? StatusChanged;
    public event EventHandler<ModelStatus>? ModelStatusChanged;
    public event EventHandler<EngineConfig>? ConfigChanged;
    public event EventHandler<IndexingConfig>? IndexingConfigChanged;
    public IndexingConfig CurrentIndexingConfig { get; set; } = new();
    public List<IndexingConfig> SetIndexingConfigCalls { get; } = new();
    public Task<IndexingConfig> GetIndexingConfigAsync(CancellationToken cancellationToken = default) => Task.FromResult(CurrentIndexingConfig);
    public Task<IndexingConfig> SetIndexingConfigAsync(IndexingConfig config, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        CurrentIndexingConfig = config;
        SetIndexingConfigCalls.Add(config);
        IndexingConfigChanged?.Invoke(this, config);
        return Task.FromResult(config);
    }
    public event EventHandler? Disconnected;
    public event EventHandler? Reconnected;

    public EngineStatus CurrentStatus { get; set; } = new();
    public ModelStatus CurrentModelStatus { get; set; } = new() { State = ModelState.NotDownloaded };
    public EngineConfig CurrentConfig { get; set; } = new();

    public List<EngineConfig> SetConfigCalls { get; } = new();
    public List<string> ImportCalls { get; } = new();
    public List<SearchQuery> SearchCalls { get; } = new();
    public int DownloadStartCalls { get; private set; }
    public int DownloadCancelCalls { get; private set; }
    public int DeleteModelCalls { get; private set; }

    public Func<SearchQuery, CancellationToken, IAsyncEnumerable<SearchBatch>>? SearchHandler { get; set; }
    public Func<CancellationToken, Task<ModelStatus>>? GetModelStatusHandler { get; set; }
    public Func<CancellationToken, Task<EngineConfig>>? GetConfigHandler { get; set; }
    public Func<EngineConfig, CancellationToken, Task<EngineConfig>>? SetConfigHandler { get; set; }

    public void PushStatus(EngineStatus status)
    {
        CurrentStatus = status;
        StatusChanged?.Invoke(this, status);
    }

    public void PushModelStatus(ModelStatus status)
    {
        CurrentModelStatus = status;
        ModelStatusChanged?.Invoke(this, status);
    }

    public void PushConfig(EngineConfig config)
    {
        CurrentConfig = config;
        ConfigChanged?.Invoke(this, config);
    }

    public void RaiseDisconnected() => Disconnected?.Invoke(this, EventArgs.Empty);

    public void RaiseReconnected() => Reconnected?.Invoke(this, EventArgs.Empty);

    public Task<EngineStatus> GetStatusAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(CurrentStatus);

    public Task<ModelStatus> StartModelDownloadAsync(CancellationToken cancellationToken = default)
    {
        DownloadStartCalls++;
        CurrentModelStatus = new ModelStatus
        {
            State = ModelState.Downloading,
            Bytes = CurrentModelStatus.Bytes,
            TotalBytes = CurrentModelStatus.TotalBytes == 0 ? 864_676_480UL : CurrentModelStatus.TotalBytes,
        };
        ModelStatusChanged?.Invoke(this, CurrentModelStatus);
        return Task.FromResult(CurrentModelStatus);
    }

    public Task<ModelStatus> CancelModelDownloadAsync(CancellationToken cancellationToken = default)
    {
        DownloadCancelCalls++;
        CurrentModelStatus = new ModelStatus
        {
            State = ModelState.Paused,
            Bytes = CurrentModelStatus.Bytes,
            TotalBytes = CurrentModelStatus.TotalBytes,
        };
        ModelStatusChanged?.Invoke(this, CurrentModelStatus);
        return Task.FromResult(CurrentModelStatus);
    }

    public Task<ModelStatus> ImportModelAsync(string path, CancellationToken cancellationToken = default)
    {
        ImportCalls.Add(path);
        CurrentModelStatus = new ModelStatus
        {
            State = ModelState.Ready,
            Bytes = 864_676_480UL,
            TotalBytes = 864_676_480UL,
        };
        ModelStatusChanged?.Invoke(this, CurrentModelStatus);
        return Task.FromResult(CurrentModelStatus);
    }

    public Task<ModelStatus> DeleteModelAsync(CancellationToken cancellationToken = default)
    {
        DeleteModelCalls++;
        CurrentModelStatus = new ModelStatus
        {
            State = ModelState.NotDownloaded,
            Bytes = 0,
            TotalBytes = 864_676_480UL,
        };
        ModelStatusChanged?.Invoke(this, CurrentModelStatus);
        return Task.FromResult(CurrentModelStatus);
    }

    public Task<ModelStatus> GetModelStatusAsync(CancellationToken cancellationToken = default)
        => GetModelStatusHandler is not null
            ? GetModelStatusHandler(cancellationToken)
            : Task.FromResult(CurrentModelStatus);

    public Task<EngineConfig> GetConfigAsync(CancellationToken cancellationToken = default)
        => GetConfigHandler is not null
            ? GetConfigHandler(cancellationToken)
            : Task.FromResult(CurrentConfig);

    public Task<EngineConfig> SetConfigAsync(EngineConfig config, CancellationToken cancellationToken = default)
    {
        SetConfigCalls.Add(new EngineConfig
        {
            SemanticEnabled = config.SemanticEnabled,
            PreferredBackend = config.PreferredBackend,
        });
        if (SetConfigHandler is not null)
        {
            return SetConfigHandler(config, cancellationToken);
        }
        CurrentConfig = new EngineConfig
        {
            SemanticEnabled = config.SemanticEnabled,
            PreferredBackend = config.PreferredBackend,
        };
        ConfigChanged?.Invoke(this, CurrentConfig);
        return Task.FromResult(CurrentConfig);
    }

    public IAsyncEnumerable<SearchBatch> SearchAsync(SearchQuery query, CancellationToken cancellationToken = default)
    {
        SearchCalls.Add(query);
        if (SearchHandler is not null)
        {
            return SearchHandler(query, cancellationToken);
        }
        return EmptyStreamAsync(cancellationToken);
    }

    private static async IAsyncEnumerable<SearchBatch> EmptyStreamAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.Yield();
        cancellationToken.ThrowIfCancellationRequested();
        yield return new SearchBatch
        {
            Results = Array.Empty<SearchResult>(),
            Done = true,
        };
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
