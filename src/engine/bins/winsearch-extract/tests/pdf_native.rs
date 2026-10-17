use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use ws_extract::worker::{
    encode_request, validate_response, ExtractRequest, ExtractResponse, WorkerHello,
};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../..")
        .canonicalize()
        .unwrap()
}
fn run(name: &str, cursor: u64, dependency: Option<&Path>) -> ExtractResponse {
    let repo = root();
    let scratch = repo
        .join("artifacts/pdf-native")
        .join(format!("{}-{name}-{cursor}", std::process::id()));
    std::fs::create_dir_all(&scratch).unwrap();
    let req = ExtractRequest {
        path: repo.join("artifacts/pdf-fixtures").join(name),
        kind: "pdf".into(),
        scratch_dir: scratch,
        cursor,
        max_units: 30,
        media_plan: None,
        video_selection: None,
    };
    assert!(
        req.path.is_file(),
        "generate fixtures with tools/generate-pdf-fixtures.py"
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_winsearch-extract"));
    command.env(
        "WINSEARCH_TEST_PDFIUM_DIR",
        dependency
            .map(Path::to_path_buf)
            .unwrap_or_else(|| repo.join("artifacts/dependencies/pdfium-8086/bin")),
    );
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&encode_request(&req).unwrap())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let mut bytes = out.stdout.as_slice();
    let _: WorkerHello = ws_protocol::read_message(&mut bytes).unwrap().unwrap();
    let response = ws_protocol::read_message(&mut bytes).unwrap().unwrap();
    validate_response(&req, &response).unwrap();
    response
}

#[test]
fn pdf_native_forty_pages_unicode_and_continuation() {
    let first = run("fyrtio-Å界.pdf", 0, None);
    assert_eq!(first.total_units, Some(40));
    assert_eq!(
        first.output.segments.len(),
        30,
        "first batch must represent 30 actual pages"
    );
    for (i, s) in first.output.segments.iter().enumerate() {
        assert_eq!(s.page, Some(i as u32 + 1));
        assert!(s.text.contains(&format!("PDF_MARKER_PAGE_{:02}", i + 1)));
    }
    let second = run(
        "fyrtio-Å界.pdf",
        first.next_cursor.expect("continuation"),
        None,
    );
    assert_eq!(second.output.segments.len(), 10);
    assert_eq!(second.output.segments[4].page, Some(35));
    assert!(second.output.segments[4]
        .text
        .contains("PDF_MARKER_PAGE_35"));
    assert_eq!(second.next_cursor, None);
    let a = std::fs::read(first.output.segments[0].thumbnail_hint.as_ref().unwrap()).unwrap();
    let b = std::fs::read(second.output.segments[4].thumbnail_hint.as_ref().unwrap()).unwrap();
    assert_ne!(a, b);
}

#[test]
fn pdf_native_actual_scanned_invoice_ocr_and_rendered_visual() {
    let response = run("scanned.pdf", 0, None);
    let text = response
        .output
        .segments
        .iter()
        .filter(|s| s.kind == "ocr")
        .map(|s| s.text.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    println!("actual native OCR: {text:?}");
    assert!(
        text.contains("SCANNED") && text.contains("INVOICE") && text.contains("42"),
        "native installed OCR must recognize raster: {text:?}"
    );
    let visual = response
        .output
        .segments
        .iter()
        .find(|s| s.kind == "pdf_visual")
        .expect("rendered visual draft");
    assert_eq!(visual.page, Some(1));
    let image =
        ws_platform::wic::decode_image_wic(visual.thumbnail_hint.as_ref().unwrap(), 2048).unwrap();
    assert!(image.bgra8.chunks_exact(4).any(|p| p[0] < 100));
    assert!(image.bgra8.chunks_exact(4).any(|p| p[0] > 240));
}

#[test]
fn pdf_native_blank_failures_limits_and_large_page_continuation() {
    let blank = run("empty.pdf", 0, None);
    assert_eq!(blank.total_units, Some(1));
    assert_eq!(blank.output.segments.len(), 1);
    assert!(blank.output.segments[0].text.is_empty());
    assert_eq!(blank.next_cursor, None);
    for (name, message) in [
        ("protected.pdf", "password protected"),
        ("corrupt.pdf", "corrupt"),
        ("pathological-size.pdf", "dimensions"),
        ("pathological-text.pdf", "text exceeds"),
    ] {
        let out = run(name, 0, None);
        assert!(
            matches!(&out.output.status,ws_extract::ExtractionStatus::Failed(s) if s.contains(message)),
            "{name}: {:?}",
            out.output.status
        );
        assert!(out.output.segments.is_empty());
    }
    let missing = run("empty.pdf", 0, Some(&root().join("artifacts/no-pdfium")));
    assert!(
        matches!(&missing.output.status,ws_extract::ExtractionStatus::Failed(s) if s.contains("dependency"))
    );
    let first = run("huge-text.pdf", 0, None);
    assert_eq!(first.total_units, Some(1));
    assert_eq!(first.output.segments.len(), 256);
    assert_eq!(first.next_cursor, Some(256));
    let second = run("huge-text.pdf", 256, None);
    assert_eq!(second.next_cursor, None);
    assert!(second
        .output
        .segments
        .last()
        .unwrap()
        .text
        .contains("finalhugepageneedle"));
}

#[test]
fn pdf_native_source_page_limit_is_diagnostic_and_blank_pages_continue() {
    let out = run("over-page-limit.pdf", 0, None);
    assert_eq!(out.total_units, Some(5000));
    assert_eq!(out.output.segments.len(), 30);
    assert_eq!(out.next_cursor, Some(30 << 32));
    assert!(out
        .diagnostics
        .iter()
        .any(|d| d.contains("source_pages=5001") && d.contains("indexed_pages=5000")));
    let last = run("over-page-limit.pdf", 4999 << 32, None);
    assert_eq!(last.output.segments[0].page, Some(5000));
    assert_eq!(last.next_cursor, None);
    assert_eq!(last.total_units, Some(5000));
}

#[test]
fn pdf_native_unreadable_and_numeric_exact_only_are_explicit() {
    use std::os::windows::fs::OpenOptionsExt;
    let path = root().join("artifacts/pdf-fixtures/locked.pdf");
    std::fs::copy(root().join("artifacts/pdf-fixtures/empty.pdf"), &path).unwrap();
    let _lock = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&path)
        .unwrap();
    let unreadable = run("locked.pdf", 0, None);
    assert!(
        matches!(&unreadable.output.status,ws_extract::ExtractionStatus::Failed(s) if s.contains("unreadable")),
        "{:?}",
        unreadable.output.status
    );
    let numeric = run("numeric.pdf", 0, None);
    assert!(numeric.output.segments[0].text.contains("98017"));
    assert!(!numeric.output.segments[0].embed);
    assert_eq!(numeric.output.segments[0].page, Some(1));
}
