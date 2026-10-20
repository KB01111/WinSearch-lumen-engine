namespace WinSearch.Core.Protocol;

public enum FileKind
{
    Folder, Pdf, Document, Spreadsheet, Presentation, Source,
    Image, Video, Audio, Archive, Executable, Model, Unknown,
}

public static class FileKinds
{
    public static bool Valid(IReadOnlyList<FileKind>? kinds)
        => kinds is { Count: <= 13 } && kinds.All(Enum.IsDefined);

    public static FileKind ForExtension(string extension, bool isDirectory)
    {
        if (isDirectory) return FileKind.Folder;
        if (extension.Any(character => character > 127)) return FileKind.Unknown;
        return extension.ToLowerInvariant() switch {
            "pdf" => FileKind.Pdf,
            "doc" or "docx" or "odt" or "rtf" or "txt" or "md" => FileKind.Document,
            "csv" or "ods" or "xls" or "xlsx" => FileKind.Spreadsheet,
            "odp" or "ppt" or "pptx" => FileKind.Presentation,
            "c" or "cc" or "cpp" or "cs" or "css" or "go" or "h" or "hpp" or "html" or "java" or "js" or "jsx"
                or "json" or "kt" or "kts" or "lua" or "php" or "py" or "rb" or "rs" or "scss" or "sh" or "sql"
                or "swift" or "toml" or "ts" or "tsx" or "vue" or "xml" or "yaml" or "yml" => FileKind.Source,
            "avif" or "bmp" or "gif" or "ico" or "jpeg" or "jpg" or "png" or "webp" => FileKind.Image,
            "avi" or "m4v" or "mkv" or "mov" or "mp4" or "webm" or "wmv" => FileKind.Video,
            "aac" or "flac" or "m4a" or "mp3" or "ogg" or "wav" or "wma" => FileKind.Audio,
            "7z" or "bz2" or "gz" or "rar" or "tar" or "xz" or "zip" => FileKind.Archive,
            "bat" or "cmd" or "com" or "exe" or "msi" or "ps1" => FileKind.Executable,
            "fbx" or "gltf" or "glb" or "obj" or "stl" => FileKind.Model,
            _ => FileKind.Unknown,
        };
    }

    public static bool Matches(IReadOnlyList<FileKind> kinds, SearchResult hit)
    {
        if (kinds.Count == 0) return true;
        if (!Enum.IsDefined(hit.Kind) || string.IsNullOrWhiteSpace(hit.Path)
            || hit.Kind is ResultKind.Calculator or ResultKind.Settings or ResultKind.Action) return false;
        var name = Path.GetFileName(hit.Path);
        var dot = name.LastIndexOf('.');
        return kinds.Contains(ForExtension(dot > 0 ? name[(dot + 1)..] : "", hit.Kind == ResultKind.Folder));
    }
}
