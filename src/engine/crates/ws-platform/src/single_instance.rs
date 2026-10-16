//! Single-instance guard backed by a named mutex.

use windows::Win32::Foundation::{CloseHandle, GetLastError, ERROR_ALREADY_EXISTS, HANDLE};
use windows::Win32::System::Threading::CreateMutexW;

const ENGINE_MUTEX_NAME: &str = "Local\\WinSearch.Engine.v1";
const EMBED_MUTEX_NAME: &str = "Local\\WinSearch.Embed.v1";

/// Holds a named mutex for the process lifetime. `acquire` returns `None` when
/// another instance already holds it.
#[derive(Debug)]
pub struct SingleInstanceGuard {
    handle: HANDLE,
}

// The mutex handle is owned exclusively by this guard and only ever closed
// once at drop.
unsafe impl Send for SingleInstanceGuard {}

impl SingleInstanceGuard {
    pub fn acquire_engine() -> Option<Self> {
        Self::acquire_isolated(ENGINE_MUTEX_NAME)
    }

    /// The embed worker's single-instance guard: exactly one
    /// `winsearch-embed` serves the per-user pipe at a time.
    pub fn acquire_embed() -> Option<Self> {
        Self::acquire_isolated(EMBED_MUTEX_NAME)
    }

    fn acquire_isolated(base: &str) -> Option<Self> {
        let name =
            crate::identity::isolated_object_name(base).expect("invalid WINSEARCH_INSTANCE_ID");
        let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
        Self::acquire(windows::core::PCWSTR(wide.as_ptr()))
    }

    pub fn acquire(name: windows::core::PCWSTR) -> Option<Self> {
        unsafe {
            let handle = match CreateMutexW(None, false, name) {
                Ok(h) => h,
                Err(_) => return None,
            };
            if GetLastError() == ERROR_ALREADY_EXISTS {
                let _ = CloseHandle(handle);
                return None;
            }
            Some(Self { handle })
        }
    }
}

impl Drop for SingleInstanceGuard {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embed_mutex_is_single_instance() {
        let first = SingleInstanceGuard::acquire_embed();
        assert!(first.is_some(), "could not acquire embed mutex");
        // Held here: a second worker must refuse.
        assert!(SingleInstanceGuard::acquire_embed().is_none());
        drop(first);
        // Released: acquirable again.
        assert!(SingleInstanceGuard::acquire_embed().is_some());
    }
}
