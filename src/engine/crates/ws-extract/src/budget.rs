//! Per-file extraction budgets: byte limits, segment caps, and wall-clock deadlines.
//!
//! Untrusted files on disk can be huge, slow to decode, or structured to
//! explode into millions of chunks. Every [`crate::Extractor`] receives an
//! [`ExtractBudget`] and must consult it before reading the file, inside loops
//! via [`ExtractBudget::check_deadline`], and when emitting segments.

use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::ExtractError;

/// Default maximum bytes an extractor may read from one file (32 MiB).
pub const DEFAULT_MAX_FILE_BYTES: u64 = 32 * 1024 * 1024;
/// Streaming media is bounded by decoded windows, not an in-memory source read.
pub const DEFAULT_MAX_MEDIA_FILE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
/// Default wall-clock budget per extraction batch (also enforced by the parent).
pub const DEFAULT_EXTRACT_TIMEOUT: Duration = Duration::from_secs(120);
/// Default cap on segments emitted by one file extraction.
pub const DEFAULT_MAX_SEGMENTS: usize = 256;

/// Resource limits for extracting a single file.
#[derive(Debug, Clone)]
pub struct ExtractBudget {
    /// Maximum input file size in bytes (and maximum bytes read).
    pub max_bytes: u64,
    /// Wall-clock deadline after which extraction must abort.
    pub deadline: Instant,
    /// Maximum number of segments to keep per file.
    pub max_segments: usize,
}

impl Default for ExtractBudget {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_FILE_BYTES,
            DEFAULT_EXTRACT_TIMEOUT,
            DEFAULT_MAX_SEGMENTS,
        )
    }
}

impl ExtractBudget {
    pub fn for_modality(kind: &str) -> Self {
        Self {
            max_bytes: if matches!(kind, "audio" | "video") {
                DEFAULT_MAX_MEDIA_FILE_BYTES
            } else {
                DEFAULT_MAX_FILE_BYTES
            },
            ..Self::default()
        }
    }
    /// Creates a budget starting now with `timeout` duration.
    pub fn new(max_bytes: u64, timeout: Duration, max_segments: usize) -> Self {
        Self {
            max_bytes,
            deadline: Instant::now() + timeout,
            max_segments,
        }
    }

    /// Creates a budget with a specific `timeout` and default byte/segment limits.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::new(DEFAULT_MAX_FILE_BYTES, timeout, DEFAULT_MAX_SEGMENTS)
    }

    /// Creates a budget with a specific `max_bytes` cap and default timeout/segment limits.
    pub fn with_max_bytes(max_bytes: u64) -> Self {
        Self::new(max_bytes, DEFAULT_EXTRACT_TIMEOUT, DEFAULT_MAX_SEGMENTS)
    }

    /// Creates a budget with a specific `max_segments` cap and default byte/timeout limits.
    pub fn with_max_segments(max_segments: usize) -> Self {
        Self::new(
            DEFAULT_MAX_FILE_BYTES,
            DEFAULT_EXTRACT_TIMEOUT,
            max_segments,
        )
    }

    /// Returns `Err(ExtractError::DeadlineExceeded)` if `Instant::now() >= self.deadline`.
    pub fn check_deadline(&self) -> Result<(), ExtractError> {
        if Instant::now() >= self.deadline {
            Err(ExtractError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }

    /// Returns `Err(ExtractError::FileTooLarge)` if `size > self.max_bytes`.
    pub fn check_size(&self, size: u64) -> Result<(), ExtractError> {
        if size > self.max_bytes {
            Err(ExtractError::FileTooLarge {
                size,
                max_bytes: self.max_bytes,
            })
        } else {
            Ok(())
        }
    }

    /// Checks whether `count` segments is still within `max_segments`.
    pub fn check_segments(&self, count: usize) -> Result<(), ExtractError> {
        if count > self.max_segments {
            Err(ExtractError::SegmentLimitExceeded {
                count,
                max_segments: self.max_segments,
            })
        } else {
            Ok(())
        }
    }

    /// Reads up to `max_bytes` from `path`, checking the deadline and file
    /// size before and after reading. If the file exceeds `max_bytes`, returns
    /// [`ExtractError::FileTooLarge`] without reading it into memory.
    pub fn read_bounded(&self, path: &Path) -> Result<Vec<u8>, ExtractError> {
        self.check_deadline()?;
        let meta = std::fs::metadata(path)?;
        self.check_size(meta.len())?;

        let file = std::fs::File::open(path)?;
        let cap = self.max_bytes.saturating_add(1);
        let mut buf = Vec::with_capacity((meta.len() as usize).min(64 * 1024));
        file.take(cap).read_to_end(&mut buf)?;
        self.check_size(buf.len() as u64)?;
        self.check_deadline()?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_deadline_aborts() {
        let budget = ExtractBudget {
            max_bytes: 1024,
            deadline: Instant::now() - Duration::from_millis(10),
            max_segments: 10,
        };
        assert!(matches!(
            budget.check_deadline(),
            Err(ExtractError::DeadlineExceeded)
        ));
    }

    #[test]
    fn future_deadline_passes() {
        let budget = ExtractBudget::with_timeout(Duration::from_secs(5));
        assert!(budget.check_deadline().is_ok());
    }

    #[test]
    fn size_and_segment_checks_enforce_limits() {
        let budget = ExtractBudget::new(100, Duration::from_secs(5), 3);
        assert!(budget.check_size(100).is_ok());
        assert!(matches!(
            budget.check_size(101),
            Err(ExtractError::FileTooLarge {
                size: 101,
                max_bytes: 100
            })
        ));
        assert!(budget.check_segments(3).is_ok());
        assert!(matches!(
            budget.check_segments(4),
            Err(ExtractError::SegmentLimitExceeded {
                count: 4,
                max_segments: 3
            })
        ));
    }
}
