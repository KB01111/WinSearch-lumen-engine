using System.Numerics;
using Microsoft.UI.Composition;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Hosting;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Media;
using Windows.System;
using WinSearch.App.Interop;
using WinSearch.Core.Protocol;
using WinSearch.Core.Settings;
using WinSearch.ViewModels;

namespace WinSearch.App;

/// <summary>
/// The floating search overlay: created once, shown and hidden instantly for
/// every hotkey press. Handles keyboard navigation, animations, theme, and
/// monitor-aware placement.
/// </summary>
public sealed partial class OverlayWindow : Window
{
    private readonly MainOverlayViewModel _viewModel;
    private readonly AppSettings _settings;
    private IntPtr _previousForeground;
    private bool _visible;

    public bool IsVisible => _visible;

    public OverlayWindow(MainOverlayViewModel viewModel, AppSettings settings)
    {
        _viewModel = viewModel;
        _settings = settings;
        InitializeComponent();

        Title = "WinSearch";
        SystemBackdrop = new DesktopAcrylicBackdrop();
        OverlayPlacement.ApplyChrome(this);

        ResultsList.ItemsSource = _viewModel.Results;
        _viewModel.PropertyChanged += OnViewModelPropertyChanged;
        SearchBox.TextChanged += OnSearchBoxTextChanged;
        ApplyTheme();

        Activated += OnWindowActivated;

        if (_settings.RememberLastQuery)
        {
            SearchBox.Text = _settings.LastQuery;
            _viewModel.QueryText = _settings.LastQuery;
        }
    }

    private void OnSearchBoxTextChanged(object sender, TextChangedEventArgs e)
    {
        if (_viewModel.QueryText != SearchBox.Text)
        {
            _viewModel.QueryText = SearchBox.Text;
        }
    }

    private void OnViewModelPropertyChanged(object? sender, System.ComponentModel.PropertyChangedEventArgs e)
    {
        switch (e.PropertyName)
        {
            case nameof(MainOverlayViewModel.StatusText):
                StatusText.Text = _viewModel.StatusText;
                break;
            case nameof(MainOverlayViewModel.ModeLabel):
                ModeIndicator.Text = _viewModel.ModeLabel;
                break;
            case nameof(MainOverlayViewModel.IsSemanticPending):
                SemanticPendingPanel.Visibility = _viewModel.IsSemanticPending
                    ? Visibility.Visible
                    : Visibility.Collapsed;
                break;
            case nameof(MainOverlayViewModel.SemanticHintText):
                SemanticHintText.Text = _viewModel.SemanticHintText;
                SemanticHintText.Visibility = string.IsNullOrEmpty(_viewModel.SemanticHintText)
                    ? Visibility.Collapsed
                    : Visibility.Visible;
                break;
        }
    }

    public void SetConnectionMessage(string message) => StatusText.Text = message;

    private void ApplyTheme()
    {
        RootGrid.RequestedTheme = _settings.Theme switch
        {
            AppTheme.Light => ElementTheme.Light,
            AppTheme.Dark => ElementTheme.Dark,
            _ => ElementTheme.Default,
        };
    }

    /// <summary>Shows the overlay on the monitor of the active window.</summary>
    public void ShowOverlay()
    {
        _previousForeground = NativeMethods.GetForegroundWindow();
        OverlayPlacement.PlaceOnActiveMonitor(this);

        if (!_visible)
        {
            AppWindow.Show();
            _visible = true;
            AnimateIn();
        }

        Activate();
        SearchBox.Focus(FocusState.Programmatic);
        if (!string.IsNullOrEmpty(SearchBox.Text))
        {
            SearchBox.SelectAll();
        }
    }

    /// <summary>Hides instantly and restores focus to the previous window.</summary>
    public void HideOverlay()
    {
        if (!_visible)
        {
            return;
        }

        if (_settings.RememberLastQuery)
        {
            _settings.LastQuery = SearchBox.Text;
            _settings.Save();
        }

        _visible = false;
        AnimateOut(() =>
        {
            AppWindow.Hide();
        });

        if (_previousForeground != IntPtr.Zero)
        {
            NativeMethods.SetForegroundWindow(_previousForeground);
        }
    }

    private void OnWindowActivated(object sender, WindowActivatedEventArgs args)
    {
        // Clicking away dismisses the overlay like a true spotlight panel.
        if (args.WindowActivationState == WindowActivationState.Deactivated && _visible)
        {
            HideOverlay();
        }
    }

    private async void OnSearchBoxKeyDown(object sender, KeyRoutedEventArgs e)
    {
        var ctrlDown = (Microsoft.UI.Input.InputKeyboardSource.GetKeyStateForCurrentThread(VirtualKey.Control)
                        & Windows.UI.Core.CoreVirtualKeyStates.Down) != 0;

        switch (e.Key)
        {
            case VirtualKey.Escape:
                if (!string.IsNullOrEmpty(SearchBox.Text))
                {
                    SearchBox.Text = "";
                    _viewModel.QueryText = "";
                    _ = _viewModel.SearchNowAsync();
                }
                else
                {
                    HideOverlay();
                }
                e.Handled = true;
                break;

            case VirtualKey.Enter when ctrlDown:
                // Ctrl+Enter: reveal the result in its folder instead of opening it.
                e.Handled = true;
                await FlushPendingQueryAsync();
                RevealSelectedResult();
                break;

            case VirtualKey.Enter:
                e.Handled = true;
                await FlushPendingQueryAsync();
                OpenSelectedResult();
                break;

            case VirtualKey.C when ctrlDown && string.IsNullOrEmpty(SearchBox.SelectedText):
                // Ctrl+C with no text selection copies the selected result's path.
                if (ResultsList.SelectedItem is ResultItemViewModel copyItem)
                {
                    ResultActions.CopyPath(copyItem.Result);
                    e.Handled = true;
                }
                break;

            case VirtualKey.Down:
                MoveSelection(1);
                e.Handled = true;
                break;

            case VirtualKey.Up:
                MoveSelection(-1);
                e.Handled = true;
                break;

            case VirtualKey.Space when ctrlDown:
                // Ctrl+Space reserved for preview (Phase 6).
                break;
        }
    }

    private void MoveSelection(int delta)
    {
        if (_viewModel.Results.Count == 0)
        {
            return;
        }
        var index = ResultsList.SelectedIndex;
        if (index < 0)
        {
            index = delta > 0 ? 0 : _viewModel.Results.Count - 1;
        }
        else
        {
            index = Math.Clamp(index + delta, 0, _viewModel.Results.Count - 1);
        }
        ResultsList.SelectedIndex = index;
        ResultsList.ScrollIntoView(_viewModel.Results[index]);
    }

    private void OnResultClick(object sender, ItemClickEventArgs e)
    {
        if (e.ClickedItem is ResultItemViewModel item)
        {
            var explanation = ResultActions.Open(item.Result);
            if (explanation is null) HideOverlay();
            else _viewModel.StatusText = explanation;
        }
    }

    private async Task FlushPendingQueryAsync()
    {
        do
        {
            _viewModel.QueryText = SearchBox.Text;
            await _viewModel.FlushOrWaitForSearchAsync();
        }
        while (_viewModel.QueryText != SearchBox.Text || _viewModel.HasPendingSearch);

        if (ResultsList.SelectedIndex < 0 && _viewModel.Results.Count > 0)
        {
            ResultsList.SelectedIndex = 0;
        }
    }

    private void RevealSelectedResult()
    {
        var item = ResultsList.SelectedItem as ResultItemViewModel
            ?? (_viewModel.Results.Count > 0 ? _viewModel.Results[0] : null);
        if (item is not null)
        {
            ResultActions.RevealInExplorer(item.Result);
            HideOverlay();
        }
    }

    private void OpenSelectedResult()
    {
        var item = ResultsList.SelectedItem as ResultItemViewModel
            ?? (_viewModel.Results.Count > 0 ? _viewModel.Results[0] : null);
        if (item is not null)
        {
            var explanation = ResultActions.Open(item.Result);
            if (explanation is null) HideOverlay();
            else _viewModel.StatusText = explanation;
        }
    }

    // ---- animations (native Composition; reduced-motion aware) ----

    private bool AnimationsEnabled { get; } =
        new Windows.UI.ViewManagement.UISettings().AnimationsEnabled;

    private void AnimateIn()
    {
        var visual = ElementCompositionPreview.GetElementVisual(RootGrid);
        if (!AnimationsEnabled)
        {
            visual.Opacity = 1f;
            visual.Offset = Vector3.Zero;
            return;
        }

        var compositor = visual.Compositor;
        visual.Opacity = 0f;
        visual.Offset = new Vector3(0, 16, 0);

        var easing = compositor.CreateCubicBezierEasingFunction(
            new Vector2(0.1f, 0.9f), new Vector2(0.2f, 1f));

        var opacity = compositor.CreateScalarKeyFrameAnimation();
        opacity.InsertKeyFrame(1f, 1f, easing);
        opacity.Duration = TimeSpan.FromMilliseconds(160);

        var offset = compositor.CreateVector3KeyFrameAnimation();
        offset.InsertKeyFrame(1f, Vector3.Zero, easing);
        offset.Duration = TimeSpan.FromMilliseconds(160);

        visual.StartAnimation("Opacity", opacity);
        visual.StartAnimation("Offset", offset);
    }

    private void AnimateOut(Action onCompleted)
    {
        var visual = ElementCompositionPreview.GetElementVisual(RootGrid);
        if (!AnimationsEnabled)
        {
            visual.Opacity = 1f;
            onCompleted();
            return;
        }

        var compositor = visual.Compositor;
        var easing = compositor.CreateCubicBezierEasingFunction(
            new Vector2(0.9f, 0.1f), new Vector2(1f, 0.2f));

        var opacity = compositor.CreateScalarKeyFrameAnimation();
        opacity.InsertKeyFrame(1f, 0f, easing);
        opacity.Duration = TimeSpan.FromMilliseconds(110);

        var offset = compositor.CreateVector3KeyFrameAnimation();
        offset.InsertKeyFrame(1f, new Vector3(0, 10, 0), easing);
        offset.Duration = TimeSpan.FromMilliseconds(110);

        var batch = compositor.CreateScopedBatch(CompositionBatchTypes.Animation);
        visual.StartAnimation("Opacity", opacity);
        visual.StartAnimation("Offset", offset);
        batch.End();
        batch.Completed += (_, _) =>
        {
            visual.Opacity = 1f;
            visual.Offset = Vector3.Zero;
            onCompleted();
        };
    }
}
