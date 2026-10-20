using System.Globalization;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using WinSearch.Core.Ipc;
using WinSearch.Core.Protocol;
using WinSearch.Core.Settings;

namespace WinSearch.ViewModels;

/// <summary>
/// Settings page state: theme, global shortcut, search behaviour, and AI model
/// management (download/pause/resume/import/delete, semantic toggle, backend selection).
/// </summary>
public sealed partial class SettingsViewModel : ObservableObject, IDisposable
{
    public static IReadOnlyList<AppTheme> ThemeOptions { get; } =
        Enum.GetValues<AppTheme>();

    public static IReadOnlyList<string> BackendOptions { get; } =
        ["auto", "npu", "cuda", "vulkan", "cpu"];

    private readonly AppSettings _settings;
    private readonly IEngineClient? _client;
    private readonly SynchronizationContext? _ui;
    private readonly Func<DateTimeOffset> _clock;

    private bool _suppressConfigPush;
    private EngineConfig _lastAppliedConfig = new();
    private ulong _lastProgressBytes;
    private DateTimeOffset? _lastProgressTime;
    private double _smoothedBytesPerSec;
    private bool _disposed;

    public SettingsViewModel(
        AppSettings settings,
        IEngineClient? client = null,
        Func<DateTimeOffset>? clock = null)
    {
        _settings = settings;
        _client = client;
        _ui = SynchronizationContext.Current;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);

        _selectedTheme = settings.Theme;
        _rememberLastQuery = settings.RememberLastQuery;

        if (_client is not null)
        {
            _client.ModelStatusChanged += OnModelStatusChanged;
            _client.ConfigChanged += OnConfigChanged;
            _client.Reconnected += OnReconnected;
        }
    }

    [ObservableProperty]
    private AppTheme _selectedTheme;

    [ObservableProperty]
    private bool _rememberLastQuery;

    [ObservableProperty]
    private string _hotkeyDisplay = "";

    [ObservableProperty]
    private string _hotkeyStatus = "";

    // ---- AI Model & Semantic Config ----

    [ObservableProperty]
    private ModelState _modelState = ModelState.NotDownloaded;

    [ObservableProperty]
    private ulong _modelBytes;

    [ObservableProperty]
    private ulong _modelTotalBytes;

    [ObservableProperty]
    private double _modelProgressPercent;

    [ObservableProperty]
    private string _modelProgressText = "0 MB / 865 MB";

    [ObservableProperty]
    private string _downloadSpeedText = "";

    [ObservableProperty]
    private string _modelStatusSummary = "AI model not downloaded (865 MB)";

    [ObservableProperty]
    private string _modelErrorText = "";

    [ObservableProperty]
    private bool _canStartOrResumeDownload = true;

    [ObservableProperty]
    private bool _canPauseDownload;

    [ObservableProperty]
    private bool _canImportModel = true;

    [ObservableProperty]
    private bool _canDeleteModel;

    [ObservableProperty]
    private string _primaryModelActionLabel = "Download";

    [ObservableProperty]
    private bool _enableSemanticSearch = true;

    [ObservableProperty]
    private string _selectedBackend = "auto";

    [ObservableProperty]
    private string _detectedBackendDisplay = "Auto (AMD NPU > CUDA > Vulkan > CPU)";

    public void Initialize()
    {
        HotkeyDisplay = _settings.HotkeyDisplay;
        ApplyModelStatus(new ModelStatus { State = ModelState.NotDownloaded });
        UpdateBackendDisplay();
    }

    /// <summary>Loads initial model status and engine config over IPC.</summary>
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        Dispatch(Initialize);
        await RefreshFromEngineAsync(cancellationToken);
    }

    private async Task RefreshFromEngineAsync(CancellationToken cancellationToken = default)
    {
        if (_client is null || _disposed)
        {
            return;
        }

        try
        {
            var status = await _client.GetModelStatusAsync(cancellationToken);
            Dispatch(() =>
            {
                if (!_disposed)
                {
                    ApplyModelStatus(status);
                }
            });
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Dispatch(() =>
            {
                if (!_disposed)
                {
                    ModelErrorText = ex.Message;
                }
            });
        }

        try
        {
            var config = await _client.GetConfigAsync(cancellationToken);
            Dispatch(() =>
            {
                if (!_disposed)
                {
                    ApplyConfig(config);
                }
            });
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Dispatch(() =>
            {
                if (!_disposed)
                {
                    ModelErrorText = ex.Message;
                }
            });
        }
    }

    partial void OnSelectedThemeChanged(AppTheme value)
    {
        _settings.Theme = value;
        _settings.Save();
    }

    partial void OnRememberLastQueryChanged(bool value)
    {
        _settings.RememberLastQuery = value;
        _settings.Save();
    }

    partial void OnEnableSemanticSearchChanged(bool value)
    {
        if (!_suppressConfigPush)
        {
            _ = PushConfigAsync();
        }
    }

    partial void OnSelectedBackendChanged(string value)
    {
        UpdateBackendDisplay();
        if (!_suppressConfigPush)
        {
            _ = PushConfigAsync();
        }
    }

    private async Task PushConfigAsync()
    {
        if (_client is null)
        {
            return;
        }

        try
        {
            var updated = await _client.SetConfigAsync(new EngineConfig
            {
                SemanticEnabled = EnableSemanticSearch,
                PreferredBackend = SelectedBackend,
            });
            Dispatch(() => ApplyConfig(updated));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var fallback = _lastAppliedConfig;
            try
            {
                fallback = await _client.GetConfigAsync();
            }
            catch
            {
                // Fall back to the last applied config if GetConfigAsync also fails.
            }

            Dispatch(() =>
            {
                ApplyConfig(fallback);
                ModelErrorText = $"Settings sync failed: {ex.Message}";
            });
        }
    }

    public void ApplyConfig(EngineConfig config)
    {
        var normalizedBackend = string.IsNullOrWhiteSpace(config.PreferredBackend)
            ? "auto"
            : config.PreferredBackend.Trim().ToLowerInvariant();
        if (!BackendOptions.Contains(normalizedBackend))
        {
            normalizedBackend = "auto";
        }

        _lastAppliedConfig = new EngineConfig
        {
            SemanticEnabled = config.SemanticEnabled,
            PreferredBackend = normalizedBackend,
        };

        _suppressConfigPush = true;
        try
        {
            EnableSemanticSearch = config.SemanticEnabled;
            SelectedBackend = normalizedBackend;
            UpdateBackendDisplay();
        }
        finally
        {
            _suppressConfigPush = false;
        }
    }

    public void ApplyModelStatus(ModelStatus status)
    {
        var now = _clock();

        if (status.State == ModelState.Downloading)
        {
            if (_lastProgressTime is { } prevTime && status.Bytes >= _lastProgressBytes)
            {
                var elapsedSec = (now - prevTime).TotalSeconds;
                if (elapsedSec > 0.01)
                {
                    var instantSpeed = (status.Bytes - _lastProgressBytes) / elapsedSec;
                    _smoothedBytesPerSec = _smoothedBytesPerSec <= 0
                        ? instantSpeed
                        : (_smoothedBytesPerSec * 0.6) + (instantSpeed * 0.4);
                }
            }
            else if (_lastProgressTime is null || status.Bytes < _lastProgressBytes)
            {
                _smoothedBytesPerSec = 0;
            }

            _lastProgressBytes = status.Bytes;
            _lastProgressTime = now;
        }
        else
        {
            _lastProgressBytes = status.Bytes;
            _lastProgressTime = null;
            _smoothedBytesPerSec = 0;
        }

        ModelState = status.State;
        ModelBytes = status.Bytes;
        ModelTotalBytes = status.TotalBytes;

        var fraction = status.State == ModelState.Ready && status.TotalBytes == 0
            ? 1.0
            : Math.Clamp(status.Fraction, 0.0, 1.0);
        ModelProgressPercent = Math.Round(fraction * 100.0, 1);

        var pctString = ModelProgressPercent.ToString("0.#", CultureInfo.InvariantCulture);
        ModelProgressText = status.TotalBytes > 0
            ? $"{FormatBytes(status.Bytes)} / {FormatBytes(status.TotalBytes)} ({pctString}%)"
            : status.State == ModelState.Ready
                ? "Ready"
                : "0 MB / 865 MB";

        DownloadSpeedText = status.State == ModelState.Downloading && _smoothedBytesPerSec > 0
            ? FormatSpeed(_smoothedBytesPerSec)
            : "";

        ModelErrorText = status.State == ModelState.Error
            ? (status.Error ?? "Model operation failed.")
            : "";

        CanStartOrResumeDownload = status.State is ModelState.NotDownloaded or ModelState.Paused or ModelState.Error;
        CanPauseDownload = status.State == ModelState.Downloading;
        CanImportModel = status.State != ModelState.Downloading;
        CanDeleteModel = status.State is ModelState.Ready or ModelState.Paused or ModelState.Error;

        PrimaryModelActionLabel = status.State switch
        {
            ModelState.Paused => "Resume",
            ModelState.Error => "Retry",
            _ => "Download",
        };

        ModelStatusSummary = status.State switch
        {
            ModelState.NotDownloaded => "AI model not downloaded (~865 MB)",
            ModelState.Downloading => string.IsNullOrEmpty(DownloadSpeedText)
                ? $"Downloading AI model… {ModelProgressText}"
                : $"Downloading AI model… {ModelProgressText} · {DownloadSpeedText}",
            ModelState.Paused => $"Download paused ({ModelProgressText})",
            ModelState.Ready => status.TotalBytes > 0
                ? $"AI model ready ({FormatBytes(status.TotalBytes)})"
                : "AI model ready",
            ModelState.Error => $"AI model error: {ModelErrorText}",
            _ => "",
        };
    }

    private void UpdateBackendDisplay()
    {
        DetectedBackendDisplay = SelectedBackend switch
        {
            "npu" => "AMD NPU (Ryzen AI — falls back to GPU, then CPU, if unavailable)",
            "cuda" => "CUDA (NVIDIA GPU — falls back to CPU if unavailable)",
            "vulkan" => "Vulkan (GPU — falls back to CPU if unavailable)",
            "cpu" => "CPU only",
            _ => "Auto (AMD NPU > CUDA > Vulkan > CPU)",
        };
    }

    public static string FormatBytes(ulong bytes)
    {
        const double mb = 1024.0 * 1024.0;
        const double gb = 1024.0 * 1024.0 * 1024.0;
        if (bytes >= (ulong)gb)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{bytes / gb:0.00} GB");
        }
        return string.Create(CultureInfo.InvariantCulture, $"{bytes / mb:0.0} MB");
    }

    public static string FormatSpeed(double bytesPerSecond)
    {
        const double kb = 1024.0;
        const double mb = 1024.0 * 1024.0;
        if (bytesPerSecond >= mb)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{bytesPerSecond / mb:0.0} MB/s");
        }
        if (bytesPerSecond >= kb)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{bytesPerSecond / kb:0} KB/s");
        }
        return string.Create(CultureInfo.InvariantCulture, $"{bytesPerSecond:0} B/s");
    }

    [RelayCommand]
    public async Task StartOrResumeDownloadAsync(CancellationToken cancellationToken = default)
    {
        if (_client is null)
        {
            return;
        }
        try
        {
            ModelErrorText = "";
            var status = await _client.StartModelDownloadAsync(cancellationToken);
            Dispatch(() => ApplyModelStatus(status));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Dispatch(() => ModelErrorText = ex.Message);
        }
    }

    [RelayCommand]
    public async Task PauseDownloadAsync(CancellationToken cancellationToken = default)
    {
        if (_client is null)
        {
            return;
        }
        try
        {
            var status = await _client.CancelModelDownloadAsync(cancellationToken);
            Dispatch(() => ApplyModelStatus(status));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Dispatch(() => ModelErrorText = ex.Message);
        }
    }

    public async Task ImportModelFromPathAsync(string path, CancellationToken cancellationToken = default)
    {
        if (_client is null || string.IsNullOrWhiteSpace(path))
        {
            return;
        }
        try
        {
            ModelErrorText = "";
            var status = await _client.ImportModelAsync(path, cancellationToken);
            Dispatch(() => ApplyModelStatus(status));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Dispatch(() => ModelErrorText = ex.Message);
        }
    }

    [RelayCommand]
    public async Task DeleteModelAsync(CancellationToken cancellationToken = default)
    {
        if (_client is null)
        {
            return;
        }
        try
        {
            ModelErrorText = "";
            var status = await _client.DeleteModelAsync(cancellationToken);
            Dispatch(() => ApplyModelStatus(status));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Dispatch(() => ModelErrorText = ex.Message);
        }
    }

    /// <summary>
    /// Called by the view with a captured key chord. Returns true when the new
    /// hotkey was saved; the view is responsible for re-registering it and
    /// reporting conflicts via <see cref="HotkeyStatus"/>.
    /// </summary>
    public bool TrySetHotkey(uint modifiers, uint key)
    {
        _settings.HotkeyModifiers = modifiers;
        _settings.HotkeyKey = key;
        _settings.Save();
        HotkeyDisplay = _settings.HotkeyDisplay;
        HotkeyStatus = "";
        return true;
    }

    [RelayCommand]
    private void ResetHotkey()
    {
        TrySetHotkey(0x1, 0x20); // Alt + Space
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
        if (!_disposed)
        {
            _ = RefreshFromEngineAsync();
        }
    }

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

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        if (_client is not null)
        {
            _client.ModelStatusChanged -= OnModelStatusChanged;
            _client.ConfigChanged -= OnConfigChanged;
            _client.Reconnected -= OnReconnected;
        }
    }
}
