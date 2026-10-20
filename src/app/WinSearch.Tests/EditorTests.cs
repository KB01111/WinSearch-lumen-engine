using WinSearch.Core.Settings;
using WinSearch.Core.Protocol;
using WinSearch.Core;
using Xunit;

namespace WinSearch.Tests;

public sealed class EditorTests
{
    public static IEnumerable<object[]> Arguments()
    {
        const string file = @"C:\space dir\源 code.rs";
        yield return [CodeEditor.VSCode, new[] { "--goto", file + ":17:1" }];
        yield return [CodeEditor.Cursor, new[] { "--goto", file + ":17:1" }];
        yield return [CodeEditor.VisualStudio, new[] { "/Edit", file, "/Command", "Edit.Goto 17" }];
        yield return [CodeEditor.JetBrains, new[] { "--line", "17", file }];
        yield return [CodeEditor.Sublime, new[] { file + ":17:1" }];
        yield return [CodeEditor.Zed, new[] { file + ":17:1" }];
        yield return [CodeEditor.NotepadPlusPlus, new[] { "-n17", file }];
    }

    [Theory, MemberData(nameof(Arguments))]
    public void GeneratedEditorMatrixUsesIndividualArguments(CodeEditor editor, string[] expected)
    {
        var start = CodeEditorLauncher.StartInfo(editor, @"C:\Editor Dir\editor.exe", @"C:\space dir\源 code.rs", new LineLocation { Line = 17 });
        Assert.False(start.UseShellExecute);
        Assert.Equal(expected, start.ArgumentList.ToArray());
        Assert.Empty(start.Arguments);
    }

    [Theory, MemberData(nameof(Arguments))]
    public void NotebookNeverUsesVirtualLineAsJsonLine(CodeEditor editor, string[] unused)
    {
        _ = unused;
        const string file = @"C:\space dir\研究.ipynb";
        var start = CodeEditorLauncher.StartInfo(editor, "editor.exe", file, new NotebookLocation { CellOrdinal = 9, Line = 42, EndLine = 43 });
        Assert.Equal(editor == CodeEditor.VisualStudio ? new[] { "/Edit", file } : new[] { file }, start.ArgumentList.ToArray());
    }
}
