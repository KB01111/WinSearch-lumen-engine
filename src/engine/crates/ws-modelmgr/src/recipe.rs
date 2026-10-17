//! Application inputs for the pinned EmbeddingGemma 2 model. Raw runtime
//! callers and native reference inputs deliberately do not use this module.

pub fn search_query(text: &str) -> String {
    format!("task: search result | query: {}", text.trim())
}

pub fn code_query(text: &str) -> String {
    format!("task: code retrieval | query: {}", text.trim())
}

/// Development measurements favor the original filename over document prompts
/// or stripping/normalizing extensions. Keep meaningful Unicode and punctuation.
pub fn filename(name: &str) -> &str {
    name
}

pub fn document(title: &str, text: &str) -> String {
    let title = if title.trim().is_empty() {
        "none"
    } else {
        title
    };
    format!("title: {title} | text: {text}")
}

pub fn code_document(filename: &str, text: &str) -> String {
    document(filename, text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_tasks_share_document_format_and_preserve_content() {
        assert_eq!(
            search_query("  books  "),
            "task: search result | query: books"
        );
        assert_eq!(
            code_query("  fn open  "),
            "task: code retrieval | query: fn open"
        );
        assert_eq!(
            document("", "\nExact 原文\n"),
            "title: none | text: \nExact 原文\n"
        );
        assert_eq!(
            code_document("cache.rs", "fn evict() {}"),
            "title: cache.rs | text: fn evict() {}"
        );
        assert_eq!(filename("家庭菜園-water.v2.xlsx"), "家庭菜園-water.v2.xlsx");
    }
}
