use crate::SearchQuery;

pub const MAX_SEARCH_RESULTS: u32 = 10_000;
pub const MAX_SEARCH_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_SEARCH_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SEARCH_RESULT_FRAMES: usize = 64;
pub const MAX_SEARCH_BATCH_ITEMS: usize = 512;
pub const MAX_SEARCH_BATCH_BYTES: usize = 8 * 1024 * 1024;

pub fn valid_search_query(query: &SearchQuery) -> bool {
    query.limit <= MAX_SEARCH_RESULTS
        && query.text.len() <= MAX_SEARCH_TEXT_BYTES
        && !query.text.contains('\0')
        && query.categories.len() <= 32
        && crate::valid_search_extensions(&query.extensions)
        && crate::valid_search_file_kinds(&query.file_kinds)
}
