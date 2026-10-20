use std::io::{self, Write};
use ws_protocol::{FrameError, MAX_SEARCH_RESPONSE_BYTES, MAX_SEARCH_RESULT_FRAMES};

#[derive(Default, Clone, Copy)]
pub struct SearchUsage {
    pub results: usize,
    pub frames: usize,
    pub bytes: usize,
}

impl SearchUsage {
    pub fn allows(&self, results: usize, bytes: usize, limit: u32) -> bool {
        self.results.saturating_add(results) <= limit.max(1) as usize
            && self.frames < MAX_SEARCH_RESULT_FRAMES - 2
            && self.bytes.saturating_add(bytes) <= MAX_SEARCH_RESPONSE_BYTES - 1024
    }
}

pub fn serialized_size(value: &ws_protocol::ServerMessage) -> Result<usize, FrameError> {
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("response size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter(0);
    serde_json::to_writer(&mut count, value)?;
    Ok(count.0 + 4)
}
