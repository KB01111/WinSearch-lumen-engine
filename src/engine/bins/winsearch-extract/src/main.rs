//! One batch per process. Stdin is the assignment gate: the daemon sends it
//! only after assigning this process to its memory-limited Windows job.
use ws_extract::worker::{read_request, ExtractResponse, WorkerHello, SUPPORTED_KINDS};
use ws_extract::{
    DocumentExtractor, ExtractBudget, ExtractOutput, ExtractionStatus, Extractor, ImageExtractor,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("extract worker: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut stdout = std::io::stdout().lock();
    ws_protocol::write_message(
        &mut stdout,
        &WorkerHello {
            supported_kinds: SUPPORTED_KINDS.iter().map(|s| s.to_string()).collect(),
        },
    )
    .map_err(|e| e.to_string())?;
    let request = read_request(&mut std::io::stdin().lock())?;
    let output = if !SUPPORTED_KINDS.contains(&request.kind.as_str()) || request.cursor != 0 {
        ExtractOutput::unsupported()
    } else if !request.scratch_dir.is_absolute()
        || !ws_platform::content::local_content_allowed(&request.path)
        || !ws_platform::content::local_content_allowed(&request.scratch_dir)
    {
        ExtractOutput {
            status: ExtractionStatus::Failed("content policy changed".into()),
            segments: Vec::new(),
        }
    } else {
        let budget = ExtractBudget::new(
            ws_extract::DEFAULT_MAX_FILE_BYTES,
            ws_extract::DEFAULT_EXTRACT_TIMEOUT,
            request.max_units as usize,
        );
        let result = match request.kind.as_str() {
            "doc" | "pdf" => DocumentExtractor.extract(&request.path, &budget),
            "image" => ImageExtractor::default()
                .with_thumb_dir(request.scratch_dir.clone())
                .extract(&request.path, &budget),
            _ => unreachable!(),
        };
        result.unwrap_or_else(|error| ExtractOutput {
            status: ExtractionStatus::Failed(error.to_string()),
            segments: Vec::new(),
        })
    };
    // Legacy extractors truncate at their segment cap. A capped result does
    // not establish the file's total number of units until batching lands.
    let total_units = (matches!(
        &output.status,
        ExtractionStatus::Extracted | ExtractionStatus::ExactOnly
    ) && output.segments.len() < request.max_units as usize)
        .then_some(output.segments.len() as u64);
    ws_protocol::write_message(
        &mut stdout,
        &ExtractResponse {
            output,
            next_cursor: None,
            total_units,
        },
    )
    .map_err(|e| e.to_string())
}
