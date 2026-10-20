using Microsoft.UI.Xaml;
using WinSearch.App.Interop;
using WinSearch.Core.Ipc;
using WinSearch.Core.Protocol;
using WinSearch.Core.Settings;
using WinSearch.ViewModels;

namespace WinSearch.App;

/// <summary>
/// Application composition root: single-instance guard, engine connection,
/// global hotkey, tray icon, and the reusable overlay window.
/// </summary>
public partial class App : Application
{
    private static string InstanceMutexName => EnginePipe.ObjectNameForCurrentUser(@"Local\WinSearch.App.v1");
    private static string ShowSignalName => EnginePipe.ObjectNameForCurrentUser(@"Local\WinSearch.ShowOverlay");

    private Mutex? _instanceMutex;
    private EventWaitHandle? _showSignal;
    private Thread? _activationThread;
    private bool _exiting;

    private AppSettings _settings = new();
    private MessageWindow? _messageWindow;
    private HotkeyManager? _hotkeyManager;
    private TrayIcon? _trayIcon;
    private EngineClient? _engineClient;
    private MainOverlayViewModel? _overlayViewModel;
    private OverlayWindow? _overlay;
    private SettingsWindow? _settingsWindow;

    public App()
    {
        InitializeComponent();
    }

    public static App CurrentApp => (App)Current;

    public EngineClient Engine =>
        _engineClient ?? throw new InvalidOperationException("engine client not initialized");

    public AppSettings Settings => _settings;

    protected override void OnLaunched(LaunchActivatedEventArgs args)
    {
        _ = AppSettings.SettingsDirectory;
        _instanceMutex = new Mutex(true, InstanceMutexName, out var isFirstInstance);
        if (!isFirstInstance)
        {
            // A second launch (e.g. from a shortcut) shows the running instance.
            try
            {
                using var signal = EventWaitHandle.OpenExisting(ShowSignalName);
                signal.Set();
            }
            catch (WaitHandleCannotBeOpenedException)
            {
                // First instance is shutting down; nothing to do.
            }
            Exit();
            return;
        }

        _settings = AppSettings.Load();

        _messageWindow = MessageWindow.Instance;
        _hotkeyManager = new HotkeyManager(_messageWindow, _settings);
        _hotkeyManager.Pressed += OnHotkeyPressed;

        _trayIcon = new TrayIcon(_messageWindow, "WinSearch");
        _trayIcon.OpenRequested += ShowOverlay;
        _trayIcon.SettingsRequested += ShowSettings;
        _trayIcon.ExitRequested += Shutdown;

        _engineClient = new EngineClient(EnginePipe.NameForCurrentUser());
        _engineClient.StatusChanged += OnEngineStatus;

        _overlayViewModel = new MainOverlayViewModel(_engineClient, _settings);
        _overlay = new OverlayWindow(_overlayViewModel, _settings);

        RegisterHotkeyWithFallback();

        _showSignal = new EventWaitHandle(false, EventResetMode.AutoReset, ShowSignalName);
        _activationThread = new Thread(ListenForActivation)
        {
            IsBackground = true,
            Name = "WinSearch.ActivationListener",
        };
        _activationThread.Start();

        _ = ConnectEngineAsync();

        // The overlay is created hidden and reused for every invocation — it is
        // never rebuilt, so the hotkey path is just "show and focus".
        ShowOverlay();
    }

    private void RegisterHotkeyWithFallback()
    {
        if (_hotkeyManager is null || _settings is null)
        {
            return;
        }

        if (_hotkeyManager.TryRegisterCurrent())
        {
            return;
        }

        // The preferred chord is taken (e.g. by another launcher): fall back to
        // the first free alternative and persist it. Onboarding (Phase 6) will
        // surface this clearly; Settings always shows the active chord.
        foreach (var (modifiers, key) in new (uint, uint)[]
                 {
                     (NativeMethods.MOD_CONTROL | NativeMethods.MOD_ALT, 0x20), // Ctrl+Alt+Space
                     (NativeMethods.MOD_CONTROL | NativeMethods.MOD_ALT, 0x53), // Ctrl+Alt+S
                     (NativeMethods.MOD_WIN, 0x20),                             // Win+Space
                 })
        {
            if (_hotkeyManager.TryRegister(modifiers, key))
            {
                _settings.HotkeyModifiers = modifiers;
                _settings.HotkeyKey = key;
                _settings.Save();
                return;
            }
        }
    }

    private void OnHotkeyPressed()
    {
        if (_overlay?.IsVisible == true)
        {
            _overlay.HideOverlay();
        }
        else
        {
            ShowOverlay();
        }
    }

    private void ShowOverlay()
    {
        _overlay?.ShowOverlay();
    }

    private void ShowSettings()
    {
        if (_settingsWindow is null)
        {
            _settingsWindow = new SettingsWindow(_settings, _hotkeyManager!, _engineClient);
            _settingsWindow.Closed += (_, _) => _settingsWindow = null;
        }
        _settingsWindow.Activate();
    }

    private async Task ConnectEngineAsync()
    {
        if (_engineClient is null || _overlay is null)
        {
            return;
        }
        try
        {
            await _engineClient.ConnectAsync();
            if (_overlayViewModel is not null)
            {
                await _overlayViewModel.InitializeAsync();
            }
        }
        catch (Exception ex)
        {
            _overlay.SetConnectionMessage($"Cannot reach the search engine: {ex.Message}");
        }
    }

    private void OnEngineStatus(object? sender, EngineStatus status)
    {
        if (_trayIcon is not null)
        {
            var detail = status.State switch
            {
                EngineRunState.Indexing =>
                    $"WinSearch — indexing {status.FilesIndexed:N0}/{status.FilesDiscovered:N0} files",
                EngineRunState.Paused => "WinSearch — indexing paused",
                _ => "WinSearch",
            };
            // Tray updates arrive off-thread; Shell_NotifyIconW is safe to call
            // from any thread once the icon exists.
            _trayIcon.UpdateTooltip(detail);
        }
    }

    private void ListenForActivation()
    {
        while (!_exiting && _showSignal is not null)
        {
            try
            {
                _showSignal.WaitOne();
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            if (_exiting)
            {
                return;
            }
            _overlay?.DispatcherQueue.TryEnqueue(() => ShowOverlay());
        }
    }

    private void Shutdown()
    {
        if (_exiting)
        {
            return;
        }
        _exiting = true;

        _hotkeyManager?.Dispose();
        _trayIcon?.Dispose();
        _messageWindow?.Dispose();
        if (_engineClient is not null)
        {
            _engineClient.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(2));
        }
        _showSignal?.Dispose();
        _instanceMutex?.ReleaseMutex();
        _instanceMutex?.Dispose();
        Exit();
    }
}
