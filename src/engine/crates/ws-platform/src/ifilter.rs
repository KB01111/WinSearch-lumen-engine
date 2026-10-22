//! Windows IFilter COM extraction helper for legacy/unknown document formats
//! (e.g. `.doc`, `.xls`, `.ppt`, or third-party formats with registered shell
//! filters).
//!
//! Uses `LoadIFilter` (`query.dll`) + `IFilter::Init` + `GetChunk` / `GetText`
//! on a dedicated helper thread waited on via `mpsc::Receiver::recv_timeout`
//! to stream plain text chunks with a strict character cap and wall-clock
//! deadline so buggy or hung third-party COM filters cannot stall extraction
//! even if a single COM call blocks indefinitely.
//! If no filter is registered for the file extension (`E_FAIL` /
//! `FILTER_E_UNKNOWNFORMAT` / `REGDB_E_CLASSNOTREG`), returns
//! [`IFilterOutcome::Unsupported`] so the caller degrades cleanly to
//! exact-name-only indexing without generating garbage embeddings.

use std::ffi::c_void;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Instant;

use windows::core::{IUnknown, Interface, HRESULT, PCWSTR, PWSTR};
use windows::Win32::Foundation::{S_FALSE, S_OK};
use windows::Win32::Storage::IndexServer::{
    IFilter, LoadIFilter, CHUNK_TEXT, FILTER_E_EMBEDDING_UNAVAILABLE, FILTER_E_END_OF_CHUNKS,
    FILTER_E_LINK_UNAVAILABLE, FILTER_E_NO_MORE_TEXT, FILTER_E_NO_TEXT, FILTER_E_NO_VALUES,
    FILTER_S_LAST_TEXT, IFILTER_INIT_APPLY_INDEX_ATTRIBUTES, IFILTER_INIT_CANON_HYPHENS,
    IFILTER_INIT_CANON_PARAGRAPHS, IFILTER_INIT_CANON_SPACES, IFILTER_INIT_HARD_LINE_BREAKS,
    STAT_CHUNK,
};
use windows::Win32::System::Com::{CoInitializeEx, CoUninitialize, COINIT_MULTITHREADED};

/// Outcome of attempting to extract text from a file via Windows IFilter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IFilterOutcome {
    /// Text was extracted via a registered IFilter.
    Extracted(String),
    /// No IFilter is registered for this file format or the format is unrecognized.
    Unsupported,
    /// Deadline expired while streaming chunks from the IFilter.
    DeadlineExceeded,
    /// An IFilter was loaded, but failed during initialization or streaming.
    Failed(String),
}

struct ComGuard {
    should_uninit: bool,
}

impl ComGuard {
    fn init() -> Self {
        // S_OK (0) or S_FALSE (1) means this thread now holds a COM init ref;
        // RPC_E_CHANGED_MODE means STA is already initialized on this thread,
        // which still allows in-proc IFilter calls without uninitializing.
        let hr = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) };
        let should_uninit = hr == S_OK || hr == S_FALSE;
        Self { should_uninit }
    }
}

impl Drop for ComGuard {
    fn drop(&mut self) {
        if self.should_uninit {
            unsafe {
                CoUninitialize();
            }
        }
    }
}

/// Extracts plain text from `path` via Windows `LoadIFilter`, collecting at
/// most `max_chars` characters and aborting if `Instant::now() >= deadline`.
///
/// The COM filter runs on a dedicated helper thread waited on with
/// `recv_timeout` so a third-party `IFilter` that blocks inside `LoadIFilter`,
/// `Init`, `GetChunk`, or `GetText` cannot stall the caller past `deadline`.
pub fn extract_text_via_ifilter(
    path: &Path,
    max_chars: usize,
    deadline: Instant,
) -> IFilterOutcome {
    if Instant::now() >= deadline {
        return IFilterOutcome::DeadlineExceeded;
    }
    if !path.is_file() {
        return IFilterOutcome::Unsupported;
    }

    let path_buf = path.to_path_buf();
    run_with_deadline(deadline, move || {
        extract_text_via_ifilter_blocking(&path_buf, max_chars, deadline)
    })
}

fn run_with_deadline<F>(deadline: Instant, work: F) -> IFilterOutcome
where
    F: FnOnce() -> IFilterOutcome + Send + 'static,
{
    let Some(timeout) = deadline.checked_duration_since(Instant::now()) else {
        return IFilterOutcome::DeadlineExceeded;
    };
    if timeout.is_zero() {
        return IFilterOutcome::DeadlineExceeded;
    }

    let (tx, rx) = mpsc::channel();
    let spawn_res = std::thread::Builder::new()
        .name("ws-ifilter".into())
        .spawn(move || {
            let outcome = work();
            let _ = tx.send(outcome);
        });
    if let Err(e) = spawn_res {
        return IFilterOutcome::Failed(format!("failed to spawn IFilter worker thread: {e}"));
    }

    match rx.recv_timeout(timeout) {
        Ok(outcome) => outcome,
        Err(RecvTimeoutError::Timeout) => IFilterOutcome::DeadlineExceeded,
        Err(RecvTimeoutError::Disconnected) => {
            IFilterOutcome::Failed("IFilter worker thread terminated unexpectedly".into())
        }
    }
}

fn extract_text_via_ifilter_blocking(
    path: &Path,
    max_chars: usize,
    deadline: Instant,
) -> IFilterOutcome {
    if Instant::now() >= deadline {
        return IFilterOutcome::DeadlineExceeded;
    }
    if !path.is_file() {
        return IFilterOutcome::Unsupported;
    }

    let _com = ComGuard::init();

    let wide_path: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();

    let mut raw_unk: *mut c_void = std::ptr::null_mut();
    let load_res =
        unsafe { LoadIFilter(PCWSTR(wide_path.as_ptr()), None::<&IUnknown>, &mut raw_unk) };
    if load_res.is_err() || raw_unk.is_null() {
        return IFilterOutcome::Unsupported;
    }

    let unk: IUnknown = unsafe { IUnknown::from_raw(raw_unk) };
    let filter: IFilter = match unk.cast() {
        Ok(f) => f,
        Err(_) => return IFilterOutcome::Unsupported,
    };

    if Instant::now() >= deadline {
        return IFilterOutcome::DeadlineExceeded;
    }

    let init_flags = (IFILTER_INIT_CANON_PARAGRAPHS.0
        | IFILTER_INIT_HARD_LINE_BREAKS.0
        | IFILTER_INIT_CANON_HYPHENS.0
        | IFILTER_INIT_CANON_SPACES.0
        | IFILTER_INIT_APPLY_INDEX_ATTRIBUTES.0) as u32;
    let mut flags_out = 0u32;
    let init_hr = HRESULT(unsafe { filter.Init(init_flags, &[], &mut flags_out) });
    if init_hr.is_err() {
        // Password-protected, corrupt, or unsupported sub-format.
        return IFilterOutcome::Failed(format!("IFilter::Init failed: {init_hr:?}"));
    }

    let mut out = String::new();
    let mut buf = vec![0u16; 4096];

    loop {
        if Instant::now() >= deadline {
            return IFilterOutcome::DeadlineExceeded;
        }
        if out.len() >= max_chars {
            break;
        }

        let mut stat = STAT_CHUNK::default();
        let chunk_hr = HRESULT(unsafe { filter.GetChunk(&mut stat) });
        if chunk_hr == FILTER_E_END_OF_CHUNKS {
            break;
        }
        if chunk_hr == FILTER_E_NO_TEXT
            || chunk_hr == FILTER_E_NO_VALUES
            || chunk_hr == FILTER_E_EMBEDDING_UNAVAILABLE
            || chunk_hr == FILTER_E_LINK_UNAVAILABLE
        {
            continue;
        }
        if chunk_hr.is_err() {
            return IFilterOutcome::Failed(format!("IFilter::GetChunk failed: {chunk_hr:?}"));
        }

        if (stat.flags.0 & CHUNK_TEXT.0) == 0 {
            continue;
        }

        if !out.is_empty() && !out.ends_with('\n') && out.chars().count() < max_chars {
            out.push('\n');
        }

        loop {
            if Instant::now() >= deadline {
                return IFilterOutcome::DeadlineExceeded;
            }
            let current_chars = out.chars().count();
            if current_chars >= max_chars {
                break;
            }
            let mut cwc = (buf.len() - 1) as u32;
            let text_hr = HRESULT(unsafe { filter.GetText(&mut cwc, PWSTR(buf.as_mut_ptr())) });
            if text_hr == S_OK || text_hr == FILTER_S_LAST_TEXT || text_hr == S_FALSE {
                let count = (cwc as usize).min(buf.len());
                if count > 0 {
                    let decoded = String::from_utf16_lossy(&buf[..count]);
                    let remaining = max_chars.saturating_sub(current_chars);
                    out.extend(decoded.chars().take(remaining));
                }
                if text_hr == FILTER_S_LAST_TEXT || text_hr == S_FALSE || count == 0 {
                    break;
                }
            } else if text_hr == FILTER_E_NO_MORE_TEXT || text_hr == FILTER_E_NO_TEXT {
                break;
            } else {
                return IFilterOutcome::Failed(format!("IFilter::GetText failed: {text_hr:?}"));
            }
        }
    }

    let cleaned: String = out
        .chars()
        .filter(|&c| c != '\0')
        .collect::<String>()
        .trim()
        .to_string();

    IFilterOutcome::Extracted(cleaned)
}

/// Alias for [`extract_text_via_ifilter`].
pub use extract_text_via_ifilter as extract_text_ifilter;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn expired_deadline_returns_deadline_exceeded() {
        let past = Instant::now() - Duration::from_millis(10);
        let res = extract_text_via_ifilter(Path::new("nonexistent.doc"), 1024, past);
        assert_eq!(res, IFilterOutcome::DeadlineExceeded);
    }

    #[test]
    fn missing_file_returns_unsupported() {
        let deadline = Instant::now() + Duration::from_secs(5);
        let res = extract_text_via_ifilter(
            Path::new(r"C:\nonexistent-winsearch-ifilter-test.doc"),
            1024,
            deadline,
        );
        assert_eq!(res, IFilterOutcome::Unsupported);
    }

    #[test]
    fn hung_worker_times_out_at_deadline_without_stalling_caller() {
        let start = Instant::now();
        let deadline = start + Duration::from_millis(50);
        let outcome = run_with_deadline(deadline, || {
            std::thread::sleep(Duration::from_secs(5));
            IFilterOutcome::Extracted("never".into())
        });
        assert_eq!(outcome, IFilterOutcome::DeadlineExceeded);
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "caller must return promptly on deadline timeout"
        );
    }
}
