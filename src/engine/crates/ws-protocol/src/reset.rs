use serde::{Deserialize, Serialize};

pub fn valid_index_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Compare-and-reset: stale or repeated requests must not clear a newer index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResetRequest {
    pub expected_index_id: String,
    pub expected_code_index_id: String,
}
impl ResetRequest {
    pub fn is_valid(&self) -> bool {
        valid_index_id(&self.expected_index_id) && valid_index_id(&self.expected_code_index_id)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IndexReset {
    pub index_id: String,
    pub code_index_id: String,
}
impl IndexReset {
    pub fn is_valid(&self) -> bool {
        valid_index_id(&self.index_id)
            && valid_index_id(&self.code_index_id)
            && self.index_id != self.code_index_id
    }
}
