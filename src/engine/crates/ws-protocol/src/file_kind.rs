use serde::{Deserialize, Serialize};

/// Source classification, independent of retrieval/result domains.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    Folder,
    Pdf,
    Document,
    Spreadsheet,
    Presentation,
    Source,
    Image,
    Video,
    Audio,
    Archive,
    Executable,
    Model,
    Unknown,
}

pub fn file_kind_for_extension(extension: &str, is_directory: bool) -> FileKind {
    if is_directory {
        return FileKind::Folder;
    }
    match extension.to_ascii_lowercase().as_str() {
        "pdf" => FileKind::Pdf,
        "doc" | "docx" | "odt" | "rtf" | "txt" | "md" => FileKind::Document,
        "csv" | "ods" | "xls" | "xlsx" => FileKind::Spreadsheet,
        "odp" | "ppt" | "pptx" => FileKind::Presentation,
        "c" | "cc" | "cpp" | "cs" | "css" | "go" | "h" | "hpp" | "html" | "java" | "js" | "jsx"
        | "json" | "kt" | "kts" | "lua" | "php" | "py" | "rb" | "rs" | "scss" | "sh" | "sql"
        | "swift" | "toml" | "ts" | "tsx" | "vue" | "xml" | "yaml" | "yml" => FileKind::Source,
        "avif" | "bmp" | "gif" | "ico" | "jpeg" | "jpg" | "png" | "webp" => FileKind::Image,
        "avi" | "m4v" | "mkv" | "mov" | "mp4" | "webm" | "wmv" => FileKind::Video,
        "aac" | "flac" | "m4a" | "mp3" | "ogg" | "wav" | "wma" => FileKind::Audio,
        "7z" | "bz2" | "gz" | "rar" | "tar" | "xz" | "zip" => FileKind::Archive,
        "bat" | "cmd" | "com" | "exe" | "msi" | "ps1" => FileKind::Executable,
        "fbx" | "gltf" | "glb" | "obj" | "stl" => FileKind::Model,
        _ => FileKind::Unknown,
    }
}

pub fn valid_search_file_kinds(kinds: &[FileKind]) -> bool {
    kinds.len() <= 13
}

pub fn search_file_kind_allowed(kinds: &[FileKind], extension: &str, is_directory: bool) -> bool {
    kinds.is_empty() || kinds.contains(&file_kind_for_extension(extension, is_directory))
}

pub fn search_file_kind_result_allowed(kinds: &[FileKind], hit: &crate::SearchResult) -> bool {
    kinds.is_empty()
        || (!matches!(
            hit.kind,
            crate::ResultKind::Calculator | crate::ResultKind::Settings | crate::ResultKind::Action
        ) && hit.path.as_deref().is_some_and(|path| {
            !path.trim().is_empty()
                && search_file_kind_allowed(
                    kinds,
                    std::path::Path::new(path)
                        .extension()
                        .and_then(|value| value.to_str())
                        .unwrap_or(""),
                    hit.kind == crate::ResultKind::Folder,
                )
        }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifications_preserve_lumen_extensions_and_directory_precedence() {
        for (expected, extensions) in [
            (FileKind::Pdf,"pdf"), (FileKind::Document,"doc docx odt rtf txt md"),
            (FileKind::Spreadsheet,"csv ods xls xlsx"), (FileKind::Presentation,"odp ppt pptx"),
            (FileKind::Source,"c cc cpp cs css go h hpp html java js jsx json kt kts lua php py rb rs scss sh sql swift toml ts tsx vue xml yaml yml"),
            (FileKind::Image,"avif bmp gif ico jpeg jpg png webp"), (FileKind::Video,"avi m4v mkv mov mp4 webm wmv"),
            (FileKind::Audio,"aac flac m4a mp3 ogg wav wma"), (FileKind::Archive,"7z bz2 gz rar tar xz zip"),
            (FileKind::Executable,"bat cmd com exe msi ps1"), (FileKind::Model,"fbx gltf glb obj stl"),
            (FileKind::Unknown,"ipynb unknown KT"),
        ] {
            for extension in extensions.split_whitespace() {
                assert_eq!(file_kind_for_extension(extension,false),expected,"{extension}");
                assert_eq!(file_kind_for_extension(&extension.to_ascii_uppercase(),false),expected,"{extension}");
                assert_eq!(file_kind_for_extension(extension,true),FileKind::Folder);
            }
        }
        assert_eq!(file_kind_for_extension("", false), FileKind::Unknown);
        assert!(search_file_kind_allowed(&[], "pdf", false));
        assert!(search_file_kind_allowed(
            &[FileKind::Pdf, FileKind::Document],
            "txt",
            false
        ));
        assert!(!search_file_kind_allowed(&[FileKind::Pdf], "pdf", true));
        assert!(valid_search_file_kinds(&[FileKind::Pdf; 13]));
        assert!(!valid_search_file_kinds(&[FileKind::Pdf; 14]));
    }
}
