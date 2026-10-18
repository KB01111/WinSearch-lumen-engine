//! Read-cache break notifications for source parsing in an owned worker process.
use std::os::windows::{
    fs::{MetadataExt, OpenOptionsExt},
    io::{AsRawHandle, FromRawHandle, OwnedHandle},
};
use std::{cell::UnsafeCell, fs::File, path::Path};
use windows::{
    Win32::{
        Foundation::{ERROR_IO_PENDING, HANDLE, WAIT_TIMEOUT},
        Storage::FileSystem::{
            FILE_FLAG_OPEN_NO_RECALL, FILE_FLAG_OPEN_REPARSE_POINT, FILE_FLAG_OVERLAPPED,
        },
        System::{
            IO::{CancelIoEx, DeviceIoControl, GetOverlappedResult, OVERLAPPED},
            Ioctl::{
                FSCTL_REQUEST_OPLOCK, OPLOCK_LEVEL_CACHE_READ, REQUEST_OPLOCK_CURRENT_VERSION,
                REQUEST_OPLOCK_INPUT_BUFFER, REQUEST_OPLOCK_INPUT_FLAG_REQUEST,
                REQUEST_OPLOCK_OUTPUT_BUFFER,
            },
            Threading::{CreateEventW, WaitForSingleObject},
        },
    },
    core::HRESULT,
};

struct PendingRead {
    input: REQUEST_OPLOCK_INPUT_BUFFER,
    output: REQUEST_OPLOCK_OUTPUT_BUFFER,
    overlapped: OVERLAPPED,
}

pub struct SourceReadGuard {
    file: File,
    event: OwnedHandle,
    // The kernel owns mutable access until completion; moving this owner cannot move its buffers.
    pending: Box<UnsafeCell<PendingRead>>,
    armed: bool,
}

impl SourceReadGuard {
    pub fn open(path: &Path) -> Option<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(
                (FILE_FLAG_OPEN_NO_RECALL | FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_OVERLAPPED).0,
            )
            .open(path)
            .ok()?;
        let metadata = file.metadata().ok()?;
        if !metadata.is_file()
            || !crate::content::content_attributes_allowed(metadata.file_attributes())
        {
            return None;
        }
        let event =
            unsafe { OwnedHandle::from_raw_handle(CreateEventW(None, true, false, None).ok()?.0) };
        let mut guard = Self {
            file,
            event,
            pending: Box::new(UnsafeCell::new(PendingRead {
                input: REQUEST_OPLOCK_INPUT_BUFFER {
                    StructureVersion: REQUEST_OPLOCK_CURRENT_VERSION as u16,
                    StructureLength: std::mem::size_of::<REQUEST_OPLOCK_INPUT_BUFFER>() as u16,
                    RequestedOplockLevel: OPLOCK_LEVEL_CACHE_READ,
                    Flags: REQUEST_OPLOCK_INPUT_FLAG_REQUEST,
                },
                output: REQUEST_OPLOCK_OUTPUT_BUFFER::default(),
                overlapped: OVERLAPPED::default(),
            })),
            armed: false,
        };
        unsafe {
            let pending = guard.pending.get();
            (*pending).overlapped.hEvent = HANDLE(guard.event.as_raw_handle());
            let result = DeviceIoControl(
                HANDLE(guard.file.as_raw_handle()),
                FSCTL_REQUEST_OPLOCK,
                Some(std::ptr::addr_of!((*pending).input).cast()),
                std::mem::size_of::<REQUEST_OPLOCK_INPUT_BUFFER>() as u32,
                Some(std::ptr::addr_of_mut!((*pending).output).cast()),
                std::mem::size_of::<REQUEST_OPLOCK_OUTPUT_BUFFER>() as u32,
                None,
                Some(std::ptr::addr_of_mut!((*pending).overlapped)),
            );
            if result.is_err_and(|error| error.code() != HRESULT::from_win32(ERROR_IO_PENDING.0)) {
                return None;
            }
        }
        guard.armed = true;
        Some(guard)
    }
    pub fn identity(&self) -> Option<(u32, u64)> {
        crate::identity::opened_file_identity(&self.file)
    }
    pub fn intact(&self) -> bool {
        unsafe { WaitForSingleObject(HANDLE(self.event.as_raw_handle()), 0) == WAIT_TIMEOUT }
    }
}

impl Drop for SourceReadGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        unsafe {
            let overlap = std::ptr::addr_of!((*self.pending.get()).overlapped);
            let handle = HANDLE(self.file.as_raw_handle());
            let _ = CancelIoEx(handle, Some(overlap));
            let mut returned = 0;
            // Storage must outlive cancellation. The extraction owner bounds this worker's lifetime.
            let _ = GetOverlappedResult(handle, overlap, &mut returned, true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "winsearch-read-guard-{}-{}.txt",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            std::io::Write::write_all(&mut file, b"abc").unwrap();
            Self(path)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_file(&self.0).unwrap();
        }
    }

    #[test]
    fn read_guard_observes_change_restore_without_blocking_reads_or_writes() {
        let fixture = Fixture::new();
        let modified = fs::metadata(&fixture.0).unwrap().modified().unwrap();
        let guard = SourceReadGuard::open(&fixture.0)
            .expect("local fixture must support read notifications");
        assert_eq!(guard.identity(), crate::identity::file_identity(&fixture.0));
        assert!(guard.intact());
        assert_eq!(fs::read(&fixture.0).unwrap(), b"abc");
        assert!(guard.intact());
        fs::write(&fixture.0, b"abd").unwrap();
        fs::write(&fixture.0, b"abc").unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&fixture.0)
            .unwrap()
            .set_modified(modified)
            .unwrap();
        assert_eq!(
            fs::metadata(&fixture.0).unwrap().modified().unwrap(),
            modified
        );
        assert!(
            !guard.intact(),
            "restoring bytes and mtime cannot restore a broken read notification"
        );
    }

    #[test]
    fn read_guard_drop_cancels_pending_notifications_and_releases_handles() {
        let fixture = Fixture::new();
        drop(SourceReadGuard::open(&fixture.0).unwrap());
        let handles = || {
            let mut count = 0;
            unsafe {
                windows::Win32::System::Threading::GetProcessHandleCount(
                    windows::Win32::System::Threading::GetCurrentProcess(),
                    &mut count,
                )
                .unwrap();
            }
            count
        };
        let before = handles();
        for _ in 0..100 {
            let guard = SourceReadGuard::open(&fixture.0).unwrap();
            assert!(guard.intact());
            drop(guard);
        }
        assert_eq!(
            handles(),
            before,
            "canceling pending notifications must release their handles"
        );
        fs::write(&fixture.0, b"after cancellation").unwrap();
        assert_eq!(fs::read(&fixture.0).unwrap(), b"after cancellation");
    }
}
