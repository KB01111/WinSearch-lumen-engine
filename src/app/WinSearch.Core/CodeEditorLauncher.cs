using System.Diagnostics;
using Microsoft.Win32;
using WinSearch.Core.Protocol;
using WinSearch.Core.Settings;

namespace WinSearch.Core;

public static class CodeEditorLauncher
{
    public static ProcessStartInfo StartInfo(CodeEditor editor, string executable, string file, MatchLocation? location)
    {
        var info = new ProcessStartInfo(executable) { UseShellExecute = false };
        uint? line = location is LineLocation value && value.Line > 0 && !Path.GetExtension(file).Equals(".ipynb", StringComparison.OrdinalIgnoreCase) ? value.Line : null;
        string[] arguments = editor switch
        {
            CodeEditor.VSCode or CodeEditor.Cursor when line.HasValue => ["--goto", $"{file}:{line}:1"],
            CodeEditor.VisualStudio when line.HasValue => ["/Edit", file, "/Command", $"Edit.Goto {line}"],
            CodeEditor.VisualStudio => ["/Edit", file],
            CodeEditor.JetBrains when line.HasValue => ["--line", line.Value.ToString(System.Globalization.CultureInfo.InvariantCulture), file],
            CodeEditor.Sublime or CodeEditor.Zed when line.HasValue => [$"{file}:{line}:1"],
            CodeEditor.NotepadPlusPlus when line.HasValue => [$"-n{line}", file],
            _ => [file],
        };
        foreach (var argument in arguments) info.ArgumentList.Add(argument);
        return info;
    }

    public static string? Detect(CodeEditor editor)
    {
        if (editor == CodeEditor.Associated) return null;
        var local = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        var programs = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles);
        var programs32 = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86);
        var names = editor switch
        {
            CodeEditor.VSCode => new[] { "Code.exe" },
            CodeEditor.Cursor => ["Cursor.exe"],
            CodeEditor.VisualStudio => ["devenv.exe"],
            CodeEditor.JetBrains => ["rider64.exe", "idea64.exe", "pycharm64.exe", "webstorm64.exe", "clion64.exe", "goland64.exe", "rustrover64.exe"],
            CodeEditor.Sublime => ["sublime_text.exe"],
            CodeEditor.Zed => ["zed.exe"],
            CodeEditor.NotepadPlusPlus => ["notepad++.exe"],
            _ => Array.Empty<string>(),
        };
        foreach (var hive in new[] { Registry.CurrentUser, Registry.LocalMachine })
        foreach (var name in names)
        {
            using var key = hive.OpenSubKey(@"Software\Microsoft\Windows\CurrentVersion\App Paths\" + name);
            if (key?.GetValue("") is string path && File.Exists(path.Trim('"'))) return path.Trim('"');
        }
        var candidates = editor switch
        {
            CodeEditor.VSCode => new[] { Path.Combine(local, "Programs", "Microsoft VS Code", "Code.exe"), Path.Combine(programs, "Microsoft VS Code", "Code.exe") },
            CodeEditor.Cursor => [Path.Combine(local, "Programs", "cursor", "Cursor.exe"), Path.Combine(programs, "Cursor", "Cursor.exe")],
            CodeEditor.VisualStudio => new[] { "2022", "2026", "18" }.SelectMany(year => new[] { "Community", "Professional", "Enterprise" }.Select(edition => Path.Combine(programs, "Microsoft Visual Studio", year, edition, "Common7", "IDE", "devenv.exe"))).ToArray(),
            CodeEditor.Sublime => [Path.Combine(programs, "Sublime Text", "sublime_text.exe"), Path.Combine(programs, "Sublime Text 3", "sublime_text.exe")],
            CodeEditor.Zed => [Path.Combine(local, "Programs", "Zed", "zed.exe"), Path.Combine(local, "Zed", "zed.exe")],
            CodeEditor.NotepadPlusPlus => [Path.Combine(programs, "Notepad++", "notepad++.exe"), Path.Combine(programs32, "Notepad++", "notepad++.exe")],
            CodeEditor.JetBrains => JetBrainsCandidates(programs, local, names),
            _ => Array.Empty<string>(),
        };
        return candidates.FirstOrDefault(File.Exists);
    }

    private static string[] JetBrainsCandidates(string programs, string local, string[] names)
    {
        var candidates = new List<string>();
        foreach (var root in new[] { Path.Combine(programs, "JetBrains"), Path.Combine(local, "Programs"), Path.Combine(local, "JetBrains", "Toolbox", "apps") })
        {
            if (!Directory.Exists(root)) continue;
            try
            {
                foreach (var app in Directory.EnumerateDirectories(root))
                foreach (var name in names) candidates.Add(Path.Combine(app, "bin", name));
            }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }
        return candidates.ToArray();
    }
}
