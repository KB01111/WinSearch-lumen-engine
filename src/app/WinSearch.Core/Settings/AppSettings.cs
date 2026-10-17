using System.Text.Json;

namespace WinSearch.Core.Settings;

public enum CodeEditor { Associated, VSCode, Cursor, VisualStudio, JetBrains, Sublime, Zed, NotepadPlusPlus }

public enum AppTheme
{
    System,
    Light,
    Dark,
}

/// <summary>
/// Frontend settings (theme, hotkey, search behaviour) stored in
/// <c>%LOCALAPPDATA%\WinSearch\settings.json</c>. Engine-side policy (folders,
/// schedules, semantic enablement, preferred backend) travels over IPC and
/// lives in the engine database.
/// </summary>
public sealed class AppSettings
{
    public CodeEditor PreferredCodeEditor { get; set; } = CodeEditor.Associated;
    public AppTheme Theme { get; set; } = AppTheme.System;

    /// <summary>RegisterHotKey modifier flags (MOD_ALT = 1, MOD_CONTROL = 2, MOD_SHIFT = 4, MOD_WIN = 8).</summary>
    public uint HotkeyModifiers { get; set; } = 0x1;

    /// <summary>Virtual-key code of the hotkey (VK_SPACE = 0x20).</summary>
    public uint HotkeyKey { get; set; } = 0x20;

    public bool RememberLastQuery { get; set; }
    public string LastQuery { get; set; } = "";

    public static string SettingsDirectory
    {
        get
        {
            var value = Environment.GetEnvironmentVariable("WINSEARCH_DATA_DIR");
            if (value is null) return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "WinSearch");
            if (!Path.IsPathFullyQualified(value)) throw new InvalidOperationException("WINSEARCH_DATA_DIR must be an absolute path");
            return value;
        }
    }

    public static string SettingsPath => Path.Combine(SettingsDirectory, "settings.json");

    public static AppSettings Load()
    {
        var settingsPath = SettingsPath;
        try
        {
            if (File.Exists(settingsPath))
            {
                var json = File.ReadAllText(settingsPath);
                return JsonSerializer.Deserialize<AppSettings>(json) ?? new AppSettings();
            }
        }
        catch
        {
            // Unreadable settings fall back to defaults; never block startup.
        }
        return new AppSettings();
    }

    public void Save()
    {
        Directory.CreateDirectory(SettingsDirectory);
        var json = JsonSerializer.Serialize(this, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(SettingsPath, json);
    }

    /// <summary>Human-readable hotkey, e.g. "Alt + Space".</summary>
    public static string FormatHotkey(uint modifiers, uint key)
    {
        var parts = new List<string>();
        if ((modifiers & 0x8) != 0) parts.Add("Win");
        if ((modifiers & 0x2) != 0) parts.Add("Ctrl");
        if ((modifiers & 0x1) != 0) parts.Add("Alt");
        if ((modifiers & 0x4) != 0) parts.Add("Shift");
        parts.Add(KeyName(key));
        return string.Join(" + ", parts);
    }

    public string HotkeyDisplay => FormatHotkey(HotkeyModifiers, HotkeyKey);

    private static string KeyName(uint vk) => vk switch
    {
        0x20 => "Space",
        0x0D => "Enter",
        0x09 => "Tab",
        0x1B => "Esc",
        >= 0x70 and <= 0x7B => $"F{vk - 0x6F}",
        >= 0x30 and <= 0x39 => ((char)vk).ToString(),
        >= 0x41 and <= 0x5A => ((char)vk).ToString(),
        0x60 => "Num0", 0x61 => "Num1", 0x62 => "Num2", 0x63 => "Num3", 0x64 => "Num4",
        0x65 => "Num5", 0x66 => "Num6", 0x67 => "Num7", 0x68 => "Num8", 0x69 => "Num9",
        _ => $"0x{vk:X2}",
    };
}
