using WinSearch.Core.Protocol;

namespace WinSearch.ViewModels;

/// <summary>Display wrapper over one search result.</summary>
public sealed class ResultItemViewModel
{
    public ResultItemViewModel(SearchResult result)
    {
        Result = result;
    }

    public SearchResult Result { get; }
    public string Title => Result.Title;
    public string Subtitle => Result.Subtitle;
    public string Explanation => Result.MatchExplanation;
    public string KindLabel => Result.Kind switch
    {
        ResultKind.Application => "Application",
        ResultKind.Folder => "Folder",
        ResultKind.Image => "Image",
        ResultKind.Screenshot => "Screenshot",
        ResultKind.Document => "Document",
        ResultKind.Audio => "Audio",
        ResultKind.Video => "Video",
        ResultKind.Code => "Code",
        ResultKind.Calculator => "Calculator",
        ResultKind.Settings => "Setting",
        ResultKind.Action => "Action",
        _ => "File",
    };

    public string LocationLabel => Result.Location switch
    {
        PageLocation page => $"page {page.Page}",
        TimestampLocation ts => $"{FormatTimestamp(ts.Seconds)} – {FormatTimestamp(ts.EndSeconds)}",
        LineLocation line => $"line {line.Line}",
        NotebookLocation notebook => $"cell {notebook.CellOrdinal + 1}, line {notebook.Line}",
        _ => "",
    };

    public string IconGlyph => Result.Kind switch
    {
        ResultKind.Application => "\uE7B8",
        ResultKind.Folder => "\uE8B7",
        ResultKind.Image => "\uEB9F",
        ResultKind.Screenshot => "\uE722",
        ResultKind.Audio => "\uEC4F",
        ResultKind.Video => "\uE714",
        ResultKind.Code => "\uE943",
        ResultKind.Calculator => "\uE940",
        ResultKind.Settings => "\uE713",
        ResultKind.Action => "\uE712",
        ResultKind.Document => "\uE8A5",
        _ => "\uE8A5",
    };

    public static string FormatTimestamp(double seconds)
    {
        var time = TimeSpan.FromSeconds(Math.Max(0, seconds));
        return time.TotalHours >= 1
            ? time.ToString(@"h\:mm\:ss")
            : time.ToString(@"m\:ss");
    }
}
