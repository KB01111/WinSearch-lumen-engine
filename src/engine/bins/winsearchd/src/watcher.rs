//! Filesystem watching via `ReadDirectoryChangesW`, one watcher thread per
//! root. Events arrive coalesced per path; renames are paired from the
//! OLD_NAME/NEW_NAME records Windows emits back-to-back.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use windows::core::PCWSTR;
use windows::Win32::Foundation::{CloseHandle, HANDLE};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, ReadDirectoryChangesW, FILE_ACTION_ADDED, FILE_ACTION_MODIFIED,
    FILE_ACTION_REMOVED, FILE_ACTION_RENAMED_NEW_NAME, FILE_ACTION_RENAMED_OLD_NAME,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_LIST_DIRECTORY, FILE_NOTIFY_CHANGE_CREATION,
    FILE_NOTIFY_CHANGE_DIR_NAME, FILE_NOTIFY_CHANGE_FILE_NAME, FILE_NOTIFY_CHANGE_LAST_WRITE,
    FILE_NOTIFY_CHANGE_SIZE, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
};
use windows::Win32::System::IO::CancelIoEx;

/// One coalesced filesystem change under a watched root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    Created(PathBuf),
    Deleted(PathBuf),
    Modified(PathBuf),
    /// A rename/move pair as reported by the OS (identity-preserving).
    Renamed {
        from: PathBuf,
        to: PathBuf,
    },
    /// The notification buffer overflowed: the subtree needs a rescan.
    Overflow,
}

/// Handle for a running watcher thread.
pub struct Watcher {
    root: PathBuf,
    handle: Arc<OwnedHandle>,
    thread: Option<JoinHandle<()>>,
}

impl Watcher {
    /// Stops watching and joins the watcher thread (same as dropping).
    #[allow(dead_code)] // exercised by tests; Drop does the same in production
    pub fn stop(mut self) {
        self.shutdown();
    }

    /// True while the watcher thread is still running.
    pub fn is_alive(&self) -> bool {
        self.thread.as_ref().is_some_and(|t| !t.is_finished())
    }

    /// True when this watcher covers `root`.
    pub fn watches(&self, root: &Path) -> bool {
        self.root == root
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Watcher {
    fn shutdown(&mut self) {
        if let Some(thread) = self.thread.take() {
            // Closing the handle unblocks a ReadDirectoryChangesW that is
            // currently waiting (and any that start after), which CancelIoEx
            // alone cannot guarantee — it only cancels requests that are
            // already pending.
            unsafe {
                let _ = CancelIoEx(self.handle.raw(), None);
            }
            self.handle.close();
            let _ = thread.join();
        }
    }
}

/// HANDLE wrapper closed exactly once, shareable across the watcher thread
/// and the stop call.
struct OwnedHandle(HANDLE, AtomicBool);
unsafe impl Send for OwnedHandle {}
unsafe impl Sync for OwnedHandle {}

impl OwnedHandle {
    fn raw(&self) -> HANDLE {
        self.0
    }

    fn close(&self) {
        if !self.1.swap(true, Ordering::SeqCst) {
            unsafe {
                let _ = CloseHandle(self.0);
            }
        }
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        self.close();
    }
}

const NOTIFY_BUFFER_BYTES: usize = 64 * 1024;

/// Starts watching `root` (recursively). Events are delivered on a dedicated
/// thread through `on_event`.
pub fn spawn<F>(root: &Path, on_event: F) -> io::Result<Watcher>
where
    F: Fn(WatchEvent) + Send + 'static,
{
    let wide: Vec<u16> = root
        .to_string_lossy()
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();

    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            FILE_LIST_DIRECTORY.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            None,
        )
        .map_err(|e| io::Error::from_raw_os_error(e.code().0))?
    };
    let handle = Arc::new(OwnedHandle(handle, AtomicBool::new(false)));
    let thread_handle = Arc::clone(&handle);
    let root_buf = root.to_path_buf();
    let thread_root = root_buf.clone();

    let thread = std::thread::Builder::new()
        .name("winsearch-watcher".into())
        .spawn(move || watch_loop(&thread_root, thread_handle, &on_event))?;

    Ok(Watcher {
        root: root_buf,
        handle,
        thread: Some(thread),
    })
}

fn watch_loop(root: &Path, handle: Arc<OwnedHandle>, on_event: &(dyn Fn(WatchEvent) + Send)) {
    let mut buffer = vec![0u8; NOTIFY_BUFFER_BYTES];
    let filter = FILE_NOTIFY_CHANGE_FILE_NAME
        | FILE_NOTIFY_CHANGE_DIR_NAME
        | FILE_NOTIFY_CHANGE_LAST_WRITE
        | FILE_NOTIFY_CHANGE_SIZE
        | FILE_NOTIFY_CHANGE_CREATION;

    let mut pending_rename: Option<PathBuf> = None;

    loop {
        let mut returned = 0u32;
        let result = unsafe {
            ReadDirectoryChangesW(
                handle.raw(),
                buffer.as_mut_ptr().cast(),
                buffer.len() as u32,
                true,
                filter,
                Some(&mut returned),
                None,
                None,
            )
        };
        if result.is_err() {
            // Cancelled (shutdown) or the directory went away.
            return;
        }
        if returned == 0 {
            // Buffer overflow: contents unreliable, subtree must be rescanned.
            pending_rename = None;
            on_event(WatchEvent::Overflow);
            continue;
        }
        for event in
            parse_notifications_with_state(&buffer[..returned as usize], &mut pending_rename)
        {
            let full_event = match event {
                WatchEvent::Created(p) => WatchEvent::Created(root.join(p)),
                WatchEvent::Deleted(p) => WatchEvent::Deleted(root.join(p)),
                WatchEvent::Modified(p) => WatchEvent::Modified(root.join(p)),
                WatchEvent::Renamed { from, to } => WatchEvent::Renamed {
                    from: root.join(from),
                    to: root.join(to),
                },
                WatchEvent::Overflow => WatchEvent::Overflow,
            };
            on_event(full_event);
        }
    }
}

const ACTION_ADDED: u32 = FILE_ACTION_ADDED.0;
const ACTION_REMOVED: u32 = FILE_ACTION_REMOVED.0;
const ACTION_MODIFIED: u32 = FILE_ACTION_MODIFIED.0;
const ACTION_RENAMED_OLD: u32 = FILE_ACTION_RENAMED_OLD_NAME.0;
const ACTION_RENAMED_NEW: u32 = FILE_ACTION_RENAMED_NEW_NAME.0;

/// Parses a `FILE_NOTIFY_INFORMATION` stream into paired events.
#[cfg(test)]
fn parse_notifications(data: &[u8]) -> Vec<WatchEvent> {
    let mut pending = None;
    let mut out = parse_notifications_with_state(data, &mut pending);
    if let Some(unmatched) = pending.take() {
        out.push(WatchEvent::Deleted(unmatched));
    }
    out
}

fn parse_notifications_with_state(
    mut data: &[u8],
    pending_rename: &mut Option<PathBuf>,
) -> Vec<WatchEvent> {
    let mut events = Vec::new();

    loop {
        if data.len() < 12 {
            break;
        }
        let next = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let action = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let name_len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        if data.len() < 12 + name_len {
            break;
        }
        let name_bytes = &data[12..12 + name_len];
        let wide: Vec<u16> = name_bytes
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        let name = String::from_utf16_lossy(&wide);
        let path = PathBuf::from(name);

        if action != ACTION_RENAMED_NEW {
            if let Some(unmatched_old) = pending_rename.take() {
                events.push(WatchEvent::Deleted(unmatched_old));
            }
        }

        match action {
            ACTION_ADDED => events.push(WatchEvent::Created(path)),
            ACTION_REMOVED => events.push(WatchEvent::Deleted(path)),
            ACTION_MODIFIED => events.push(WatchEvent::Modified(path)),
            ACTION_RENAMED_OLD => {
                *pending_rename = Some(path);
            }
            ACTION_RENAMED_NEW => match pending_rename.take() {
                Some(from) => events.push(WatchEvent::Renamed { from, to: path }),
                None => events.push(WatchEvent::Created(path)),
            },
            _ => {}
        }

        if next == 0 {
            break;
        }
        data = &data[next..];
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::mpsc;
    use std::time::Duration;

    struct TempTree(PathBuf);

    impl TempTree {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "winsearch-watch-test-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn parses_notify_stream_with_rename_pair() {
        // Two records: RENAMED_OLD_NAME("a.txt") followed by RENAMED_NEW_NAME("b.txt").
        fn record(next: u32, action: u32, name: &str) -> Vec<u8> {
            let wide: Vec<u16> = name.encode_utf16().collect();
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&next.to_le_bytes());
            bytes.extend_from_slice(&action.to_le_bytes());
            bytes.extend_from_slice(&((wide.len() * 2) as u32).to_le_bytes());
            for u in wide {
                bytes.extend_from_slice(&u.to_le_bytes());
            }
            bytes
        }

        let r1 = record(24, FILE_ACTION_RENAMED_OLD_NAME.0, "a.txt");
        let r2 = record(0, FILE_ACTION_RENAMED_NEW_NAME.0, "b.txt");
        let mut stream = r1.clone();
        stream.extend_from_slice(&r2);
        // First record's "next" must point at the second record's start.
        let mut stream = stream;
        let first_next = r1.len() as u32;
        stream[0..4].copy_from_slice(&first_next.to_le_bytes());

        let events = parse_notifications(&stream);
        assert_eq!(
            events,
            vec![WatchEvent::Renamed {
                from: PathBuf::from("a.txt"),
                to: PathBuf::from("b.txt"),
            }]
        );
    }

    #[test]
    fn watches_create_modify_rename_delete() {
        let tree = TempTree::new();
        let (tx, rx) = mpsc::channel();
        let watcher = spawn(&tree.0, move |ev| {
            let _ = tx.send(ev);
        })
        .unwrap();

        // Give the watcher thread a moment to enter the wait.
        std::thread::sleep(Duration::from_millis(100));

        let file = tree.0.join("hello.txt");
        fs::write(&file, b"one").unwrap();
        fs::write(&file, b"two").unwrap();
        let renamed = tree.0.join("renamed.txt");
        fs::rename(&file, &renamed).unwrap();
        fs::remove_file(&renamed).unwrap();

        let mut saw_create = false;
        let mut saw_rename = false;
        let mut saw_delete = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline && !(saw_create && saw_rename && saw_delete) {
            match rx.recv_timeout(Duration::from_millis(500)) {
                Ok(WatchEvent::Created(ref p)) if p == &tree.0.join("hello.txt") => {
                    saw_create = true
                }
                Ok(WatchEvent::Renamed { ref from, ref to })
                    if from == &tree.0.join("hello.txt") && to == &tree.0.join("renamed.txt") =>
                {
                    saw_rename = true
                }
                Ok(WatchEvent::Deleted(ref p)) if p == &tree.0.join("renamed.txt") => {
                    saw_delete = true
                }
                Ok(_) => {}
                Err(_) => {}
            }
        }

        watcher.stop();
        assert!(saw_create, "no create event");
        assert!(saw_rename, "no rename event");
        assert!(saw_delete, "no delete event");
    }
}
