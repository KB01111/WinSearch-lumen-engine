//! Bounded complete-source byte digests for ordinary extraction provenance.
use crate::{ExtractBudget, ExtractError};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

pub const MAX_SOURCE_HASH_BYTES: u64 = 64 * 1024 * 1024;
const FILE_FLAG_OPEN_NO_RECALL: u32 = 0x00100000;
const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x00200000;

#[derive(PartialEq, Eq)]
struct SourceDigest {
    sha256: String,
    identity: (u32, u64),
    modified: std::time::SystemTime,
    change_time: i64,
}

pub fn with_source_sha256(
    request: &crate::worker::ExtractRequest,
    budget: &ExtractBudget,
    extract: impl FnOnce() -> Result<crate::worker::ExtractResponse, ExtractError>,
) -> Result<crate::worker::ExtractResponse, ExtractError> {
    if request.kind == "code" {
        return extract();
    }
    let before = source_digest(&request.path, budget)?;
    let guard = ws_platform::source_guard::SourceReadGuard::open(&request.path);
    if guard.as_ref().is_some_and(|guard| {
        !guard.intact()
            || before
                .as_ref()
                .is_some_and(|source| guard.identity() != Some(source.identity))
    }) {
        return Err(ExtractError::SourceChanged);
    }
    let mut response = extract()?;
    response.source_sha256 = if matches!(
        response.output.status,
        crate::ExtractionStatus::Extracted | crate::ExtractionStatus::ExactOnly
    ) {
        let after = source_digest(&request.path, budget)?;
        if after != before || guard.as_ref().is_some_and(|guard| !guard.intact()) {
            return Err(ExtractError::SourceChanged);
        }
        after
            .filter(|_| guard.is_some())
            .map(|source| source.sha256)
    } else {
        None
    };
    Ok(response)
}

pub fn source_sha256(path: &Path, budget: &ExtractBudget) -> Result<Option<String>, ExtractError> {
    budget.check_deadline()?;
    if !path.is_absolute() || !ws_platform::content::local_content_allowed(path) {
        return Err(ExtractError::Decode(
            "source is not admitted for byte provenance".into(),
        ));
    }
    let guard = ws_platform::source_guard::SourceReadGuard::open(path);
    let source = source_digest(path, budget)?;
    if guard.as_ref().is_some_and(|guard| {
        !guard.intact()
            || source
                .as_ref()
                .is_some_and(|source| guard.identity() != Some(source.identity))
    }) {
        return Err(ExtractError::SourceChanged);
    }
    Ok(source
        .filter(|_| guard.is_some())
        .map(|source| source.sha256))
}

fn source_digest(
    path: &Path,
    budget: &ExtractBudget,
) -> Result<Option<SourceDigest>, ExtractError> {
    budget.check_deadline()?;
    let changed = || ExtractError::SourceChanged;
    if !path.is_absolute() || !ws_platform::content::local_content_allowed(path) {
        return Err(ExtractError::Decode(
            "source is not admitted for byte provenance".into(),
        ));
    }
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(changed());
    }
    budget.check_size(metadata.len())?;
    let Some(identity) = ws_platform::identity::file_identity(path) else {
        return Ok(None);
    };
    if metadata.len() > MAX_SOURCE_HASH_BYTES {
        return Ok(None);
    }
    let snapshot = (metadata.len(), metadata.modified()?);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_NO_RECALL | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .map_err(|error| {
            ExtractError::Io(std::io::Error::new(
                error.kind(),
                format!("source unreadable: {error}"),
            ))
        })?;
    let opened = file.metadata()?;
    if ws_platform::identity::opened_file_identity(&file) != Some(identity)
        || !ws_platform::content::content_attributes_allowed(opened.file_attributes())
        || (opened.len(), opened.modified()?) != snapshot
    {
        return Err(changed());
    }
    let Some(change_time) = ws_platform::identity::opened_file_change_time(&file) else {
        return Ok(None);
    };
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    let mut total = 0;
    loop {
        budget.check_deadline()?;
        let capacity = (snapshot.0 - total + 1).min(buffer.len() as u64) as usize;
        let count = file.read(&mut buffer[..capacity])?;
        if count == 0 {
            break;
        }
        total += count as u64;
        budget.check_size(total)?;
        if total > snapshot.0 {
            return Err(changed());
        }
        hash.update(&buffer[..count]);
    }
    let current = file.metadata()?;
    if total != snapshot.0
        || ws_platform::identity::opened_file_change_time(&file) != Some(change_time)
        || (current.len(), current.modified()?) != snapshot
        || !ws_platform::content::content_attributes_allowed(current.file_attributes())
        || ws_platform::identity::file_identity(path) != Some(identity)
        || !ws_platform::content::local_content_allowed(path)
    {
        return Err(changed());
    }
    budget.check_deadline()?;
    Ok(Some(SourceDigest {
        sha256: format!("{:x}", hash.finalize()),
        identity,
        modified: snapshot.1,
        change_time,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    struct Fixture(PathBuf);
    impl Fixture {
        fn new(bytes: &[u8]) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let directory = std::env::temp_dir().join(format!(
                "winsearch-source-hash-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&directory).unwrap();
            let fixture = Self(directory);
            fs::write(fixture.path(), bytes).unwrap();
            fixture
        }
        fn path(&self) -> PathBuf {
            self.0.join("source.bin")
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn ordinary_source_hash_is_actual_complete_bytes_including_bom_and_crlf() {
        let long = vec![b'a'; 100_000];
        for (bytes, expected) in [
            (
                &b""[..],
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                &b"abc"[..],
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            (
                &b"\xef\xbb\xbfa\r\nb\r\n"[..],
                "ef7385f30109f20b5bb2d2b82376d31c0fc64b33feefeb8efac373469fc9dca6",
            ),
            (
                &long[..],
                "6d1cf22d7cc09b085dfc25ee1a1f3ae0265804c607bc2074ad253bcc82fd81ee",
            ),
        ] {
            let fixture = Fixture::new(bytes);
            assert_eq!(
                source_sha256(&fixture.path(), &ExtractBudget::default())
                    .unwrap()
                    .as_deref(),
                Some(expected)
            );
        }
    }

    #[test]
    fn ordinary_source_hash_changes_for_same_length_binary_bytes() {
        let fixture = Fixture::new(b"\0\x01\xff");
        let first = source_sha256(&fixture.path(), &ExtractBudget::default())
            .unwrap()
            .expect("binary bytes have provenance");
        fs::write(fixture.path(), b"\0\x02\xff").unwrap();
        let second = source_sha256(&fixture.path(), &ExtractBudget::default())
            .unwrap()
            .unwrap();
        assert_ne!(first, second);
        assert!(
            ws_protocol::valid_content_sha256(&first) && ws_protocol::valid_content_sha256(&second)
        );
    }

    #[test]
    fn ordinary_source_hash_preserves_input_deadline_and_path_admission() {
        let fixture = Fixture::new(b"abc");
        assert!(matches!(
            source_sha256(&fixture.path(), &ExtractBudget::with_max_bytes(2)),
            Err(ExtractError::FileTooLarge { .. })
        ));
        let expired = ExtractBudget::with_timeout(Duration::ZERO);
        assert!(matches!(
            source_sha256(&fixture.path(), &expired),
            Err(ExtractError::DeadlineExceeded)
        ));
        assert!(
            source_sha256(
                &fixture
                    .0
                    .join("..")
                    .join(fixture.0.file_name().unwrap())
                    .join("source.bin"),
                &ExtractBudget::default()
            )
            .is_err(),
            "lexically escaping paths are denied even if they resolve to this fixture"
        );
        assert!(source_sha256(&fixture.0, &ExtractBudget::default()).is_err());
    }

    #[test]
    fn ordinary_source_hash_large_streaming_media_remains_unknown_without_relaxing_budget() {
        let fixture = Fixture::new(b"");
        fs::OpenOptions::new()
            .write(true)
            .open(fixture.path())
            .unwrap()
            .set_len(MAX_SOURCE_HASH_BYTES + 1)
            .unwrap();
        assert_eq!(
            source_sha256(&fixture.path(), &ExtractBudget::for_modality("video")).unwrap(),
            None
        );
        assert!(matches!(
            source_sha256(&fixture.path(), &ExtractBudget::default()),
            Err(ExtractError::FileTooLarge { .. })
        ));
    }

    fn request(fixture: &Fixture) -> crate::worker::ExtractRequest {
        crate::worker::ExtractRequest {
            path: fixture.path(),
            kind: "doc".into(),
            scratch_dir: fixture.0.clone(),
            cursor: 0,
            max_units: 30,
            media_plan: None,
            video_selection: None,
        }
    }

    fn response() -> crate::worker::ExtractResponse {
        crate::worker::ExtractResponse {
            source_sha256: None,
            output: crate::ExtractOutput::extracted(vec![]),
            next_cursor: None,
            total_units: Some(0),
            diagnostics: vec![],
            media: None,
            video_selection: None,
            code: None,
        }
    }

    #[test]
    fn ordinary_source_hash_attests_successful_parse_and_rejects_source_bytes_changed_by_parser() {
        let fixture = Fixture::new(b"abc");
        let budget = ExtractBudget::default();
        let result = with_source_sha256(&request(&fixture), &budget, || Ok(response())).unwrap();
        assert_eq!(
            result.source_sha256.as_deref(),
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
        let changed = with_source_sha256(&request(&fixture), &budget, || {
            fs::write(fixture.path(), b"abd")?;
            Ok(response())
        });
        assert!(
            matches!(changed, Err(ExtractError::SourceChanged)),
            "a parse cannot publish text under a digest of different source bytes"
        );
    }

    #[test]
    fn ordinary_source_hash_rejects_parse_time_change_and_restore_with_original_mtime() {
        let fixture = Fixture::new(b"abc");
        let modified = fs::metadata(fixture.path()).unwrap().modified().unwrap();
        let result = with_source_sha256(&request(&fixture), &ExtractBudget::default(), || {
            fs::write(fixture.path(), b"abd")?;
            let temporary = fs::read_to_string(fixture.path())?;
            fs::write(fixture.path(), b"abc")?;
            fs::OpenOptions::new()
                .write(true)
                .open(fixture.path())?
                .set_modified(modified)?;
            let mut output = response();
            output
                .output
                .segments
                .push(crate::SegmentDraft::text("doc", temporary));
            Ok(output)
        });
        assert_eq!(fs::read(fixture.path()).unwrap(), b"abc");
        assert_eq!(
            fs::metadata(fixture.path()).unwrap().modified().unwrap(),
            modified
        );
        assert!(
            matches!(result, Err(ExtractError::SourceChanged)),
            "parsed transient bytes must not inherit the restored file's digest: {result:?}"
        );
    }

    #[test]
    fn ordinary_source_hash_rejects_noncanonical_failed_and_code_worker_claims() {
        let fixture = Fixture::new(b"abc");
        let request = request(&fixture);
        let legacy = response();
        assert!(
            serde_json::to_value(&legacy)
                .unwrap()
                .get("source_sha256")
                .is_none()
        );
        crate::worker::validate_response(&request, &legacy).unwrap();
        let mut valid = response();
        valid.source_sha256 = Some("a".repeat(64));
        crate::worker::validate_response(&request, &valid).unwrap();
        for invalid in [
            String::new(),
            "a".repeat(63),
            "A".repeat(64),
            "g".repeat(64),
        ] {
            let mut reply = valid.clone();
            reply.source_sha256 = Some(invalid);
            assert!(crate::worker::validate_response(&request, &reply).is_err());
        }
        for status in [
            crate::ExtractionStatus::Unsupported,
            crate::ExtractionStatus::SourceChanged,
            crate::ExtractionStatus::Failed("fixture failure".into()),
        ] {
            let mut reply = valid.clone();
            reply.output.status = status;
            assert!(crate::worker::validate_response(&request, &reply).is_err());
        }
        let mut code_request = request.clone();
        code_request.kind = "code".into();
        assert!(crate::worker::validate_response(&code_request, &valid).is_err());
    }
}
