//! Fixed-size, best-effort logs. Storage failure must never stop search.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

pub(crate) const LOG_FILE_BYTES: u64 = 8 * 1024 * 1024;
const LOG_FILES: usize = 4;
const MAX_EVENT_BYTES: usize = 8 * 1024;
const TRUNCATED: &[u8] = b"[event truncated]\n";

/// Cap formatting output before the nonblocking queue allocates its messages.
pub(crate) struct LimitedWriter<W: Write> {
    writer: W,
    remaining: usize,
    truncated: bool,
}
impl<W: Write> LimitedWriter<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            remaining: MAX_EVENT_BYTES - TRUNCATED.len(),
            truncated: false,
        }
    }
}
impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.remaining.min(bytes.len());
        self.writer.write_all(&bytes[..count])?;
        self.remaining -= count;
        self.truncated |= count < bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
impl<W: Write> Drop for LimitedWriter<W> {
    fn drop(&mut self) {
        if self.truncated {
            let _ = self.writer.write_all(TRUNCATED);
        }
    }
}

pub(crate) struct BoundedFile {
    directory: PathBuf,
    file: Option<File>,
    bytes: u64,
    limit: u64,
}
fn regular_file(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
        Ok(metadata) => {
            #[cfg(windows)]
            {
                use std::os::windows::fs::MetadataExt;
                if metadata.file_attributes() & 0x400 != 0 {
                    return Err(io::Error::other("log path is a reparse point"));
                }
            }
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(io::Error::other("log path is not a plain file"));
            }
            Ok(true)
        }
    }
}
impl BoundedFile {
    pub(crate) fn open(directory: &Path, limit: u64) -> io::Result<Self> {
        if limit == 0 {
            return Err(io::Error::other("invalid log limit"));
        }
        fs::create_dir_all(directory)?;
        let mut writer = Self {
            directory: directory.to_path_buf(),
            file: None,
            bytes: 0,
            limit,
        };
        // Retire only the old engine's exact daily-file naming pattern. Other
        // logs and unknown names in the directory are never retention targets.
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(date) = name
                .to_str()
                .and_then(|name| name.strip_prefix("winsearchd.log."))
            {
                if date.len() == 10
                    && date.bytes().enumerate().all(|(i, b)| {
                        if i == 4 || i == 7 {
                            b == b'-'
                        } else {
                            b.is_ascii_digit()
                        }
                    })
                    && regular_file(&entry.path())?
                {
                    fs::remove_file(entry.path())?;
                }
            }
        }
        for number in 0..LOG_FILES {
            let path = writer.path(number);
            if regular_file(&path)? && fs::metadata(&path)?.len() > limit {
                fs::remove_file(path)?;
            }
        }
        writer.open_current()?;
        Ok(writer)
    }
    fn path(&self, number: usize) -> PathBuf {
        self.directory.join(format!("winsearchd.log.{number}"))
    }
    fn open_current(&mut self) -> io::Result<()> {
        let path = self.path(0);
        regular_file(&path)?;
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(1).custom_flags(0x00200000);
        }
        let file = options.open(&path)?;
        regular_file(&path)?;
        self.bytes = file.metadata()?.len();
        self.file = Some(file);
        Ok(())
    }
    fn rotate(&mut self) -> io::Result<()> {
        self.file = None;
        let last = self.path(LOG_FILES - 1);
        if regular_file(&last)? {
            fs::remove_file(last)?;
        }
        for number in (0..LOG_FILES - 1).rev() {
            let source = self.path(number);
            if regular_file(&source)? {
                let destination = self.path(number + 1);
                regular_file(&destination)?;
                fs::rename(source, destination)?;
            }
        }
        self.open_current()
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.bytes.saturating_add(bytes.len() as u64) > self.limit {
            self.rotate()?;
        }
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::other("logging disabled"))?
            .write_all(bytes)?;
        self.bytes += bytes.len() as u64;
        Ok(())
    }
}
impl Write for BoundedFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.file.is_some() {
            let count = bytes.len().min(self.limit as usize);
            if self.append(&bytes[..count]).is_err() {
                self.file = None;
            }
        }
        // Logging is best effort, including low disk and rotation/sharing errors.
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        io::Write,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "winsearch-logs-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&root).unwrap();
            Self(root)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
    #[test]
    fn retention_caps_file_count_and_bytes_and_preserves_unrelated_files() {
        let fixture = Fixture::new();
        fs::write(fixture.0.join("unrelated.log"), b"preserve").unwrap();
        fs::write(
            fixture.0.join("winsearchd.log.2026-10-20"),
            b"legacy daily log",
        )
        .unwrap();
        let mut writer = BoundedFile::open(&fixture.0, 32).unwrap();
        for _ in 0..100 {
            writer.write_all(b"12345678").unwrap();
        }
        drop(writer);
        let mut count = 0;
        let mut bytes = 0;
        for item in fs::read_dir(&fixture.0).unwrap() {
            let item = item.unwrap();
            if item
                .file_name()
                .to_string_lossy()
                .starts_with("winsearchd.log.")
            {
                count += 1;
                let size = item.metadata().unwrap().len();
                assert!(size <= 32);
                bytes += size;
            }
        }
        assert_eq!(count, LOG_FILES);
        assert!(bytes <= LOG_FILES as u64 * 32);
        assert_eq!(
            fs::read(fixture.0.join("unrelated.log")).unwrap(),
            b"preserve"
        );
    }
    #[test]
    fn oversized_event_is_capped_before_the_queue_and_rotation_failure_disables_writes() {
        let mut bytes = Vec::new();
        {
            let mut event = LimitedWriter::new(&mut bytes);
            event.write_all(&vec![b'x'; MAX_EVENT_BYTES * 4]).unwrap();
            event.write_all(b"further text").unwrap();
        }
        assert!(bytes.len() <= MAX_EVENT_BYTES);
        assert!(bytes.ends_with(b"[event truncated]\n"));
        let fixture = Fixture::new();
        let mut writer = BoundedFile::open(&fixture.0, 8).unwrap();
        writer.write_all(b"12345678").unwrap();
        fs::create_dir(fixture.0.join("winsearchd.log.1")).unwrap();
        writer.write_all(b"new event").unwrap();
        assert!(
            writer.file.is_none(),
            "failed rotation must disable logging"
        );
        writer
            .write_all(b"search continues without a usable log sink")
            .unwrap();
    }
}
