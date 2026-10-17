using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Windows.Storage.Pickers;
using Windows.System;
using WinSearch.Core.Ipc;
using WinSearch.Core.Settings;
using WinSearch.ViewModels;

namespace WinSearch.App;

/// <summary>
/// Settings window. Covers General (theme, shortcut, search behaviour) and AI
/// model management (download/pause/resume/import/delete, semantic toggle, backend).
/// </summary>
internal sealed partial class SettingsWindow : Window
{
    private readonly AppSettings _settings;
    private readonly HotkeyManager _hotkeyManager;
    private readonly SettingsViewModel _viewModel;
    private bool _capturingHotkey;
    private bool _syncingFromViewModel;

    public SettingsWindow(AppSettings settings, HotkeyManager hotkeyManager, IEngineClient? engineClient = null)
    {
        _settings = settings;
        _hotkeyManager = hotkeyManager;
        _viewModel = new SettingsViewModel(settings, engineClient);
        InitializeComponent();

        Title = "WinSearch Settings";
        SystemBackdrop = new Microsoft.UI.Xaml.Media.DesktopAcrylicBackdrop();
        RootGrid.RequestedTheme = _settings.Theme switch
        {
            AppTheme.Light => ElementTheme.Light,
            AppTheme.Dark => ElementTheme.Dark,
            _ => ElementTheme.Default,
        };

        _viewModel.PropertyChanged += OnViewModelPropertyChanged;
        _viewModel.Initialize();
        HotkeyText.Text = _viewModel.HotkeyDisplay;

        ThemeCombo.ItemsSource = SettingsViewModel.ThemeOptions;
        ThemeCombo.SelectedItem = _viewModel.SelectedTheme;
        RememberQueryToggle.IsOn = _viewModel.RememberLastQuery;
        CodeEditorCombo.ItemsSource = Enum.GetValues<CodeEditor>();
        CodeEditorCombo.SelectedItem = _settings.PreferredCodeEditor;

        BackendCombo.ItemsSource = SettingsViewModel.BackendOptions;
        SyncAiControlsFromViewModel();

        RootGrid.KeyDown += OnRootKeyDown;
        Closed += OnWindowClosed;

        _ = _viewModel.InitializeAsync();
    }

    private void OnWindowClosed(object sender, WindowEventArgs args)
    {
        _viewModel.PropertyChanged -= OnViewModelPropertyChanged;
        _viewModel.Dispose();
    }

    private void OnCodeEditorChanged(object sender, SelectionChangedEventArgs e)
    {
        if (CodeEditorCombo.SelectedItem is CodeEditor editor)
        {
            _settings.PreferredCodeEditor = editor;
            _settings.Save();
        }
    }

    private void OnViewModelPropertyChanged(object? sender, System.ComponentModel.PropertyChangedEventArgs e)
    {
        SyncAiControlsFromViewModel();
    }

    private void SyncAiControlsFromViewModel()
    {
        _syncingFromViewModel = true;
        try
        {
            ModelStatusSummaryText.Text = _viewModel.ModelStatusSummary;
            ModelProgressBar.Value = _viewModel.ModelProgressPercent;
            ModelProgressDetailText.Text = _viewModel.ModelProgressText;
            ModelSpeedText.Text = _viewModel.DownloadSpeedText;

            DownloadModelButton.Content = _viewModel.PrimaryModelActionLabel;
            DownloadModelButton.IsEnabled = _viewModel.CanStartOrResumeDownload;
            PauseModelButton.IsEnabled = _viewModel.CanPauseDownload;
            ImportModelButton.IsEnabled = _viewModel.CanImportModel;
            DeleteModelButton.IsEnabled = _viewModel.CanDeleteModel;

            ModelErrorTextBlock.Text = _viewModel.ModelErrorText;
            ModelErrorTextBlock.Visibility = string.IsNullOrEmpty(_viewModel.ModelErrorText)
                ? Visibility.Collapsed
                : Visibility.Visible;

            if (SemanticSearchToggle.IsOn != _viewModel.EnableSemanticSearch)
            {
                SemanticSearchToggle.IsOn = _viewModel.EnableSemanticSearch;
            }

            if (BackendCombo.SelectedItem as string != _viewModel.SelectedBackend)
            {
                BackendCombo.SelectedItem = _viewModel.SelectedBackend;
            }

            DetectedBackendText.Text = _viewModel.DetectedBackendDisplay;
        }
        finally
        {
            _syncingFromViewModel = false;
        }
    }

    private void OnThemeChanged(object sender, SelectionChangedEventArgs e)
    {
        if (ThemeCombo.SelectedItem is AppTheme theme)
        {
            _viewModel.SelectedTheme = theme;
            RootGrid.RequestedTheme = theme switch
            {
                AppTheme.Light => ElementTheme.Light,
                AppTheme.Dark => ElementTheme.Dark,
                _ => ElementTheme.Default,
            };
        }
    }

    private void OnChangeHotkeyClick(object sender, RoutedEventArgs e)
    {
        _capturingHotkey = true;
        ChangeHotkeyButton.Content = "Press keys…";
        HotkeyStatusText.Text = "Press the new combination (Esc to cancel).";
    }

    private void OnResetHotkeyClick(object sender, RoutedEventArgs e)
    {
        _capturingHotkey = false;
        ChangeHotkeyButton.Content = "Change";
        ApplyHotkey(0x1, 0x20); // Alt + Space
    }

    private void OnRootKeyDown(object sender, KeyRoutedEventArgs e)
    {
        if (!_capturingHotkey)
        {
            return;
        }
        e.Handled = true;

        if (e.Key == VirtualKey.Escape)
        {
            _capturingHotkey = false;
            ChangeHotkeyButton.Content = "Change";
            HotkeyStatusText.Text = "Capture cancelled.";
            return;
        }

        uint modifiers = 0;
        var keyboard = Microsoft.UI.Input.InputKeyboardSource.GetKeyStateForCurrentThread;
        if ((keyboard(VirtualKey.LeftWindows) & Windows.UI.Core.CoreVirtualKeyStates.Down) != 0
            || (keyboard(VirtualKey.RightWindows) & Windows.UI.Core.CoreVirtualKeyStates.Down) != 0)
        {
            modifiers |= NativeMethods_MOD_WIN;
        }
        if ((keyboard(VirtualKey.Control) & Windows.UI.Core.CoreVirtualKeyStates.Down) != 0)
        {
            modifiers |= NativeMethods_MOD_CONTROL;
        }
        if ((keyboard(VirtualKey.Menu) & Windows.UI.Core.CoreVirtualKeyStates.Down) != 0)
        {
            modifiers |= NativeMethods_MOD_ALT;
        }
        if ((keyboard(VirtualKey.Shift) & Windows.UI.Core.CoreVirtualKeyStates.Down) != 0)
        {
            modifiers |= NativeMethods_MOD_SHIFT;
        }

        // Modifier-only presses are not a valid chord yet.
        if (e.Key is VirtualKey.Control or VirtualKey.Menu or VirtualKey.Shift
            or VirtualKey.LeftWindows or VirtualKey.RightWindows)
        {
            return;
        }

        _capturingHotkey = false;
        ChangeHotkeyButton.Content = "Change";
        ApplyHotkey(modifiers, (uint)e.Key);
    }

    private void ApplyHotkey(uint modifiers, uint key)
    {
        if (_hotkeyManager.TryRegister(modifiers, key))
        {
            _viewModel.TrySetHotkey(modifiers, key);
            HotkeyText.Text = _viewModel.HotkeyDisplay;
            HotkeyStatusText.Text = "Shortcut active.";
        }
        else
        {
            // Re-register the previous chord so the launcher keeps working.
            _hotkeyManager.TryRegisterCurrent();
            HotkeyStatusText.Text =
                $"{AppSettings.FormatHotkey(modifiers, key)} is already used by another application. Choose a different combination.";
        }
    }

    private void OnRememberQueryToggled(object sender, RoutedEventArgs e)
    {
        if (_syncingFromViewModel)
        {
            return;
        }
        _viewModel.RememberLastQuery = RememberQueryToggle.IsOn;
    }

    private void OnSemanticSearchToggled(object sender, RoutedEventArgs e)
    {
        if (_syncingFromViewModel)
        {
            return;
        }
        _viewModel.EnableSemanticSearch = SemanticSearchToggle.IsOn;
    }

    private void OnBackendChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_syncingFromViewModel)
        {
            return;
        }
        if (BackendCombo.SelectedItem is string backend)
        {
            _viewModel.SelectedBackend = backend;
        }
    }

    private async void OnDownloadModelClick(object sender, RoutedEventArgs e)
    {
        await _viewModel.StartOrResumeDownloadAsync();
    }

    private async void OnPauseModelClick(object sender, RoutedEventArgs e)
    {
        await _viewModel.PauseDownloadAsync();
    }

    private async void OnImportModelClick(object sender, RoutedEventArgs e)
    {
        var picker = new FileOpenPicker();
        var hwnd = OverlayPlacement.WindowNativeHandle(this);
        if (hwnd != IntPtr.Zero)
        {
            WinRT.Interop.InitializeWithWindow.Initialize(picker, hwnd);
        }
        picker.FileTypeFilter.Add(".gguf");

        var file = await picker.PickSingleFileAsync();
        if (file is not null && !string.IsNullOrWhiteSpace(file.Path))
        {
            await _viewModel.ImportModelFromPathAsync(file.Path);
        }
    }

    private async void OnDeleteModelClick(object sender, RoutedEventArgs e)
    {
        await _viewModel.DeleteModelAsync();
    }

    private const uint NativeMethods_MOD_ALT = 0x1;
    private const uint NativeMethods_MOD_CONTROL = 0x2;
    private const uint NativeMethods_MOD_SHIFT = 0x4;
    private const uint NativeMethods_MOD_WIN = 0x8;
}
