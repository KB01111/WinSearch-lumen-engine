//! One-shot local directory notifications for bounded query-pass reuse.
//! These fences never replace fresh source/grant validation before delivery.
use std::os::windows::{
    fs::{MetadataExt, OpenOptionsExt},
    io::{AsRawHandle, FromRawHandle, OwnedHandle},
};
use std::{
    cell::UnsafeCell,
    fs::File,
    path::{Component, Path, Prefix},
};
use windows::{
    core::{HRESULT, PCWSTR},
    Win32::{
        Foundation::{ERROR_IO_PENDING, HANDLE, WAIT_TIMEOUT},
        Storage::FileSystem::{
            GetDriveTypeW, ReadDirectoryChangesW, FILE_FLAG_BACKUP_SEMANTICS,
            FILE_FLAG_OPEN_NO_RECALL, FILE_FLAG_OPEN_REPARSE_POINT, FILE_FLAG_OVERLAPPED,
            FILE_LIST_DIRECTORY, FILE_NOTIFY_CHANGE_ATTRIBUTES, FILE_NOTIFY_CHANGE_CREATION,
            FILE_NOTIFY_CHANGE_DIR_NAME, FILE_NOTIFY_CHANGE_FILE_NAME,
            FILE_NOTIFY_CHANGE_LAST_WRITE, FILE_NOTIFY_CHANGE_SECURITY, FILE_NOTIFY_CHANGE_SIZE,
        },
        System::{
            Threading::{CreateEventW, WaitForSingleObject},
            IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED},
        },
    },
};

struct PendingChange {
    buffer: [u32; 1024],
    overlapped: OVERLAPPED,
}
pub struct DirectoryChangeGuard {
    file: File,
    event: OwnedHandle,
    pending: Box<UnsafeCell<PendingChange>>,
    armed: bool,
}
// Only the kernel touches the pinned pending buffer. Moving the owner does
// not move that buffer; Drop always cancels/drains before freeing it.
unsafe impl Send for DirectoryChangeGuard {}
impl DirectoryChangeGuard {
    pub fn open(path: &Path, recursive: bool) -> Option<Self> {
        let letter = match path.components().next()? {
            Component::Prefix(prefix) => match prefix.kind() {
                Prefix::Disk(letter) | Prefix::VerbatimDisk(letter) => letter,
                _ => return None,
            },
            _ => return None,
        };
        let volume = [letter as u16, b':' as u16, b'\\' as u16, 0];
        // Removable/network/unknown providers retain the uncached live path.
        if unsafe { GetDriveTypeW(PCWSTR(volume.as_ptr())) } != 3
            || !crate::content::metadata_path_allowed(path, false)
        {
            return None;
        }
        let file = std::fs::OpenOptions::new()
            .access_mode(FILE_LIST_DIRECTORY.0)
            .share_mode(7)
            .custom_flags(
                (FILE_FLAG_BACKUP_SEMANTICS
                    | FILE_FLAG_OPEN_NO_RECALL
                    | FILE_FLAG_OPEN_REPARSE_POINT
                    | FILE_FLAG_OVERLAPPED)
                    .0,
            )
            .open(path)
            .ok()?;
        let metadata = file.metadata().ok()?;
        // Directory/volume recall flags are not file residency. This handle
        // monitors metadata only; live content opens keep their stricter checks.
        if !metadata.is_dir() || metadata.file_attributes() & 0x400 != 0 {
            return None;
        }
        let event =
            unsafe { OwnedHandle::from_raw_handle(CreateEventW(None, true, false, None).ok()?.0) };
        let mut guard = Self {
            file,
            event,
            pending: Box::new(UnsafeCell::new(PendingChange {
                buffer: [0; 1024],
                overlapped: OVERLAPPED::default(),
            })),
            armed: false,
        };
        let filter = FILE_NOTIFY_CHANGE_ATTRIBUTES
            | FILE_NOTIFY_CHANGE_CREATION
            | FILE_NOTIFY_CHANGE_DIR_NAME
            | FILE_NOTIFY_CHANGE_FILE_NAME
            | FILE_NOTIFY_CHANGE_LAST_WRITE
            | FILE_NOTIFY_CHANGE_SECURITY
            | FILE_NOTIFY_CHANGE_SIZE;
        unsafe {
            let pending = guard.pending.get();
            (*pending).overlapped.hEvent = HANDLE(guard.event.as_raw_handle());
            let result = ReadDirectoryChangesW(
                HANDLE(guard.file.as_raw_handle()),
                std::ptr::addr_of_mut!((*pending).buffer).cast(),
                4096,
                recursive,
                filter,
                None,
                Some(std::ptr::addr_of_mut!((*pending).overlapped)),
                None,
            );
            if result.is_err_and(|error| error.code() != HRESULT::from_win32(ERROR_IO_PENDING.0)) {
                return None;
            }
        }
        guard.armed = true;
        Some(guard)
    }
    pub fn intact(&self) -> bool {
        unsafe { WaitForSingleObject(HANDLE(self.event.as_raw_handle()), 0) == WAIT_TIMEOUT }
    }
}
impl Drop for DirectoryChangeGuard {
    fn drop(&mut self) {
        if self.armed {
            unsafe {
                let handle = HANDLE(self.file.as_raw_handle());
                let overlap = std::ptr::addr_of!((*self.pending.get()).overlapped);
                let _ = CancelIoEx(handle, Some(overlap));
                let mut returned = 0;
                let _ = GetOverlappedResult(handle, overlap, &mut returned, true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        io::Write,
        path::PathBuf,
        time::{Duration, Instant},
    };

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "winsearch-directory-guard-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn changed(guard: &DirectoryChangeGuard) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while guard.intact() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !guard.intact(),
            "a changed/overflowed notification must never authorize reuse"
        );
    }
    #[test]
    fn recursive_write_and_restored_mtime_break_the_fence() {
        let fixture = Fixture::new();
        fs::create_dir(fixture.0.join("child")).unwrap();
        let path = fixture.0.join("child/source.txt");
        fs::write(&path, b"before").unwrap();
        let modified = fs::metadata(&path).unwrap().modified().unwrap();
        let guard = DirectoryChangeGuard::open(&fixture.0, true).expect("fixed local directory");
        assert!(guard.intact());
        let mut writer = fs::OpenOptions::new().write(true).open(&path).unwrap();
        writer.write_all(b"after!").unwrap();
        writer
            .set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
        drop(writer);
        changed(&guard);
    }
    #[test]
    fn parent_notification_observes_directory_replacement_and_drop_drains_io() {
        let fixture = Fixture::new();
        fs::create_dir(fixture.0.join("selected")).unwrap();
        let guard = DirectoryChangeGuard::open(&fixture.0, false).expect("fixed local parent");
        fs::rename(fixture.0.join("selected"), fixture.0.join("renamed")).unwrap();
        changed(&guard);
        drop(guard);
        let started = Instant::now();
        for _ in 0..16 {
            let guard = DirectoryChangeGuard::open(&fixture.0, false).unwrap();
            assert!(guard.intact());
            drop(guard);
        }
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "idle cancellation must drain promptly"
        );
    }
    #[test]
    fn unsupported_or_missing_paths_cannot_authorize_reuse() {
        assert!(
            DirectoryChangeGuard::open(Path::new(r"\\invalid\share\directory"), true).is_none()
        );
        let fixture = Fixture::new();
        assert!(DirectoryChangeGuard::open(&fixture.0.join("missing"), true).is_none());
    }
}
