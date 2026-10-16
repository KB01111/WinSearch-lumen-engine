//! One batch per process. Stdin is the assignment gate: the daemon sends it
//! only after assigning this process to its memory-limited Windows job.
use ws_extract::worker::{read_request, WorkerHello, SUPPORTED_KINDS};
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
    let response = if !SUPPORTED_KINDS.contains(&request.kind.as_str()) {
        ws_extract::worker::ExtractResponse {
            output: ExtractOutput::unsupported(),
            next_cursor: None,
            total_units: None,
        }
    } else if !request.scratch_dir.is_absolute()
        || !ws_platform::content::local_content_allowed(&request.path)
        || !ws_platform::content::local_content_allowed(&request.scratch_dir)
    {
        ws_extract::worker::ExtractResponse {
            output: ExtractOutput {
                status: ExtractionStatus::Failed("content policy changed".into()),
                segments: Vec::new(),
            },
            next_cursor: None,
            total_units: None,
        }
    } else {
        let budget = ExtractBudget::new(
            ws_extract::DEFAULT_MAX_FILE_BYTES,
            ws_extract::DEFAULT_EXTRACT_TIMEOUT,
            request.max_units as usize,
        );
        let result = match request.kind.as_str() {
            "doc" | "pdf" => {
                DocumentExtractor.extract_batch(&request.path, &budget, request.cursor)
            }
            "image" => ImageExtractor::default()
                .with_thumb_dir(request.scratch_dir.clone())
                .extract_batch(&request.path, &budget, request.cursor),
            _ => unreachable!(),
        };
        result.unwrap_or_else(|error| ws_extract::worker::ExtractResponse {
            output: ExtractOutput {
                status: ExtractionStatus::Failed(error.to_string()),
                segments: Vec::new(),
            },
            next_cursor: None,
            total_units: None,
        })
    };
    ws_protocol::write_message(&mut stdout, &response).map_err(|e| e.to_string())
}
