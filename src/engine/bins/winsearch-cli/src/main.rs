//! `winsearch-cli` — command-line interface for WinSearch status, live search,
//! headless retrieval evaluation (`eval <suite>`), and embedding model
//! management (`model download|import|verify|status`).

pub mod eval;
pub mod ipc;

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use ws_modelmgr::download::verify_file;
use ws_modelmgr::{ModelManager, ModelState, EXPECTED_FILES};
use ws_platform::paths;
use ws_protocol::{SearchMode, SearchQuery};

use crate::ipc::{DaemonClient, IpcError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliCommand {
    Help,
    Version,
    Status,
    Search {
        query: String,
        mode: SearchMode,
        semantic: bool,
        limit: u32,
    },
    Eval {
        suite: String,
    },
    Model(ModelSubcommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelSubcommand {
    Status,
    Verify,
    Download,
    Import { path: PathBuf },
}

pub fn parse_args(args: &[String]) -> anyhow::Result<CliCommand> {
    if args.is_empty() {
        return Ok(CliCommand::Help);
    }
    match args[0].as_str() {
        "-h" | "--help" | "help" => Ok(CliCommand::Help),
        "-V" | "--version" | "version" => Ok(CliCommand::Version),
        "status" => Ok(CliCommand::Status),
        "search" => parse_search_args(&args[1..]),
        "eval" => {
            let suite = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "names".to_string());
            Ok(CliCommand::Eval { suite })
        }
        "model" => parse_model_args(&args[1..]),
        other => anyhow::bail!("unknown subcommand {:?} (run with --help for usage)", other),
    }
}

fn parse_search_args(args: &[String]) -> anyhow::Result<CliCommand> {
    let mut mode = SearchMode::Auto;
    let mut semantic = false;
    let mut limit = 20u32;
    let mut query_parts: Vec<String> = Vec::new();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" | "-m" => {
                let val = args
                    .get(i + 1)
                    .ok_or_else(|| anyhow::anyhow!("--mode requires a value (all|files|apps|code)"))?;
                mode = parse_search_mode(val)?;
                i += 2;
            }
            "--semantic" | "-s" => {
                semantic = true;
                i += 1;
            }
            "--no-semantic" => {
                semantic = false;
                i += 1;
            }
            "--limit" | "-n" => {
                let val = args
                    .get(i + 1)
                    .ok_or_else(|| anyhow::anyhow!("--limit requires a positive integer"))?;
                limit = val
                    .parse::<u32>()
                    .map_err(|_| anyhow::anyhow!("invalid --limit value {:?}", val))?
                    .max(1);
                i += 2;
            }
            arg if arg.starts_with("--mode=") => {
                let val = arg.trim_start_matches("--mode=");
                mode = parse_search_mode(val)?;
                i += 1;
            }
            arg => {
                query_parts.push(arg.to_string());
                i += 1;
            }
        }
    }

    let query = query_parts.join(" ").trim().to_string();
    if query.is_empty() {
        anyhow::bail!("`search` requires a query string (e.g. `winsearch-cli search \"budget\" --mode files`)");
    }

    Ok(CliCommand::Search {
        query,
        mode,
        semantic,
        limit,
    })
}

fn parse_search_mode(s: &str) -> anyhow::Result<SearchMode> {
    match s.to_ascii_lowercase().as_str() {
        "auto" | "all" => Ok(SearchMode::Auto),
        "files" | "file" => Ok(SearchMode::Files),
        "apps" | "app" => Ok(SearchMode::Apps),
        "code" => Ok(SearchMode::Code),
        other => anyhow::bail!(
            "invalid search mode {:?} (expected all|auto|files|apps|code)",
            other
        ),
    }
}

fn parse_model_args(args: &[String]) -> anyhow::Result<CliCommand> {
    let sub = args.first().map(|s| s.as_str()).unwrap_or("status");
    match sub {
        "status" => Ok(CliCommand::Model(ModelSubcommand::Status)),
        "verify" => Ok(CliCommand::Model(ModelSubcommand::Verify)),
        "download" => Ok(CliCommand::Model(ModelSubcommand::Download)),
        "import" => {
            let path = args
                .get(1)
                .ok_or_else(|| anyhow::anyhow!("`model import` requires a <path> argument"))?;
            Ok(CliCommand::Model(ModelSubcommand::Import {
                path: PathBuf::from(path),
            }))
        }
        other => anyhow::bail!(
            "unknown `model` subcommand {:?} (expected download|import|verify|status)",
            other
        ),
    }
}

fn print_help() {
    println!(
        "winsearch-cli {} — WinSearch command-line interface

USAGE:
    winsearch-cli <SUBCOMMAND> [OPTIONS]

SUBCOMMANDS:
    status
        Connect to running winsearchd daemon and print engine/model/config status
        (falls back to local on-disk diagnostic when daemon is not running).

    search <query> [--mode <all|files|apps|code>] [--semantic] [--limit <N>]
        Run a search against the running winsearchd daemon over named-pipe IPC.

    eval [suite]
        Run headless retrieval evaluation against a fixture index (default: `names`).
        Computes Recall@5, MRR, and negative-control FP rate; exits 0 when gates pass.

    model status
        Show current model state and byte counters.
    model verify
        Verify on-disk model files against the pinned manifest sizes and SHA-256 hashes.
    model download
        Download (or resume) EmbeddingGemma 2 GGUF files into the model directory.
    model import <path>
        Import and verify model file(s) from a local file or directory path.",
        ipc::CLI_VERSION
    );
}

fn resolve_model_dir() -> PathBuf {
    std::env::var_os("WS_MODEL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(paths::model_dir)
}

fn cmd_status() -> anyhow::Result<()> {
    match DaemonClient::connect() {
        Ok(mut client) => {
            let status = client.get_status()?;
            let model = client.get_model_status().ok();
            let config = client.get_config().ok();
            println!("daemon:           connected ({})", client.pipe_name());
            println!("engine_version:   {}", client.engine_version);
            println!("features:         {}", client.features.join(", "));
            println!("state:            {:?}", status.state);
            println!(
                "activity:         {}",
                if status.current_activity.is_empty() {
                    "idle"
                } else {
                    &status.current_activity
                }
            );
            println!("files_discovered: {}", status.files_discovered);
            println!("files_indexed:    {}", status.files_indexed);
            println!("segments_indexed: {}", status.segments_indexed);
            println!("queue_depth:      {}", status.queue_depth);
            println!("model_loaded:     {}", status.model_loaded);
            println!(
                "index_size:       {:.2} MiB ({} bytes)",
                status.index_size_bytes as f64 / 1_048_576.0,
                status.index_size_bytes
            );
            if let Some(m) = model {
                println!(
                    "model_state:      {:?} ({}/{} bytes)",
                    m.state, m.bytes, m.total_bytes
                );
            }
            if let Some(c) = config {
                println!(
                    "config:           semantic_enabled={}, preferred_backend={}",
                    c.semantic_enabled, c.preferred_backend
                );
            }
            Ok(())
        }
        Err(IpcError::NotConnected { pipe, source }) => {
            let mgr = ModelManager::new(resolve_model_dir());
            let m = mgr.status();
            println!("daemon:           disconnected (pipe {pipe}: {source})");
            println!("index_db_path:    {}", paths::index_db_path().display());
            println!("model_dir:        {}", mgr.dir().display());
            println!(
                "model_state:      {:?} ({}/{} bytes)",
                m.state, m.bytes, m.total_bytes
            );
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

fn cmd_search(query: String, mode: SearchMode, semantic: bool, limit: u32) -> anyhow::Result<()> {
    let mut client = DaemonClient::connect()?;
    let results = client.search(SearchQuery {
        text: query.clone(),
        mode,
        limit,
        include_semantic: semantic,
        categories: Vec::new(),
    })?;

    println!(
        "search {:?} (mode={:?}, semantic={}) -> {} hit(s):",
        query,
        mode,
        semantic,
        results.len()
    );
    for (idx, r) in results.iter().enumerate() {
        println!(
            "  {:>2}. [{:?}] {} (score {:.3}) — {} [{}]",
            idx + 1,
            r.kind,
            r.title,
            r.score,
            r.match_explanation,
            r.subtitle
        );
    }
    Ok(())
}

fn cmd_eval(suite_name: &str) -> anyhow::Result<()> {
    let path = eval::resolve_suite_path(suite_name)?;
    let suite = eval::load_suite(&path)?;
    let report = eval::run_eval_suite(&suite)?;

    println!("=== WinSearch Retrieval Eval: {} ===", report.suite_name);
    println!("suite file:        {}", path.display());
    println!("embedder:          {}", report.embedder_label);
    println!("vector pipeline:   {}", ws_modelmgr::manifest::VECTOR_PIPELINE_VERSION);
    println!("semantic floor:    {:.4}", ws_index::retrieval::MIN_SEMANTIC_COSINE);
    println!("fixture entries:   {}", suite.entries.len());
    println!(
        "queries:           {} total ({} positive, {} negative control)",
        suite.queries.len(),
        report.positive_queries,
        report.negative_queries
    );
    println!();

    for outcome in &report.outcomes {
        if outcome.expect_empty {
            let status = if outcome.false_positive { "FAIL (FP)" } else { "PASS (empty)" };
            println!(
                "  [{status}] [{}] {:?} -> {} hit(s)",
                outcome.category,
                outcome.query,
                outcome.top_hits.len()
            );
            if outcome.false_positive {
                for (name, score, explanation) in &outcome.top_hits {
                    println!("      {name} (score {score:.4}) | {explanation}");
                }
            }
        } else {
            let status = if outcome.hit_in_top_5 { "PASS" } else { "MISS" };
            let rank_str = outcome
                .first_relevant_rank
                .map(|r| format!("rank #{r}"))
                .unwrap_or_else(|| "not found".to_string());
            let top1 = outcome
                .top_hits
                .first()
                .map(|(n, s, exp)| format!("{n} ({s:.2}, {exp})"))
                .unwrap_or_else(|| "<none>".to_string());
            println!(
                "  [{status}] [{}] {:?} -> {rank_str} (RR={:.2}) | top1: {top1}",
                outcome.category, outcome.query, outcome.reciprocal_rank
            );
        }
        for hit in &outcome.ranked_hits {
            for location in &hit.matches {
                println!("      {}: page={:?} time_ms={:?}..{:?} line={:?}",
                    hit.name, location.page, location.start_ms, location.end_ms, location.line);
            }
            if let Some(code) = &hit.code {
                println!("      {}: symbol={} kind={} definition={} virtual_lines={}..{} cell={:?}",
                    hit.path,code.symbol,code.kind,code.is_definition,code.start_line,code.end_line,code.notebook);
            }
        }
    }

    println!();
    println!("Per modality/category (n/a means no queries of that type):");
    for (modality, metrics) in &report.modalities {
        let metric = |value: Option<f64>| value.map(|n|format!("{n:.3}")).unwrap_or_else(||"n/a".into());
        println!("  {modality}: positive={} negative={} Recall@5={} MRR={} FP={}",
            metrics.positive, metrics.negative, metric(metrics.recall()), metric(metrics.mrr()), metric(metrics.fp_rate()));
    }
    println!();
    println!(
        "Recall@5:          {:.3}  (threshold >= {:.3})",
        report.recall_at_5, report.thresholds.min_recall_at_5
    );
    println!(
        "MRR:               {:.3}  (threshold >= {:.3})",
        report.mrr, report.thresholds.min_mrr
    );
    println!(
        "Negative FP rate:  {:.3}  (threshold <= {:.3})",
        report.false_positive_rate, report.thresholds.max_false_positive_rate
    );

    if report.passed() {
        println!("Result:            PASS");
        Ok(())
    } else {
        anyhow::bail!(
            "eval suite {:?} failed thresholds (Recall@5={:.3}, MRR={:.3}, FP={:.3})",
            report.suite_name,
            report.recall_at_5,
            report.mrr,
            report.false_positive_rate
        )
    }
}

fn cmd_model(sub: ModelSubcommand) -> anyhow::Result<()> {
    let pipe = std::env::var("WS_PIPE")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            ws_platform::identity::pipe_name()
                .unwrap_or_else(|_| r"\\.\pipe\winsearch-local".to_string())
        });
    cmd_model_with_pipe(sub, &pipe, resolve_model_dir())
}

fn cmd_model_with_pipe(
    sub: ModelSubcommand,
    pipe_name: &str,
    model_dir: PathBuf,
) -> anyhow::Result<()> {
    let mgr = ModelManager::new(model_dir.clone());

    match sub {
        ModelSubcommand::Status => {
            if let Ok(mut client) = DaemonClient::connect_pipe(pipe_name) {
                if let Ok(status) = client.get_model_status() {
                    println!("source:      daemon ({})", client.pipe_name());
                    println!("model_dir:   {}", model_dir.display());
                    println!("state:       {:?}", status.state);
                    println!("bytes:       {} / {}", status.bytes, status.total_bytes);
                    if let Some(err) = status.error {
                        println!("error:       {err}");
                    }
                    return Ok(());
                }
            }
            let status = mgr.status();
            println!("source:      local filesystem");
            println!("model_dir:   {}", model_dir.display());
            println!("state:       {:?}", status.state);
            println!("bytes:       {} / {}", status.bytes, status.total_bytes);
            if let Some(err) = status.error {
                println!("error:       {err}");
            }
            Ok(())
        }
        ModelSubcommand::Verify => {
            println!("verifying model files in {}...", model_dir.display());
            let mut all_ok = true;
            for expected in &EXPECTED_FILES {
                let file_path = model_dir.join(expected.name);
                if !file_path.is_file() {
                    println!("  [MISSING] {} (expected {} bytes)", expected.name, expected.size);
                    all_ok = false;
                    continue;
                }
                match verify_file(&file_path, expected) {
                    Ok(()) => {
                        println!(
                            "  [OK]      {} ({} bytes, sha256={})",
                            expected.name,
                            expected.size,
                            expected.sha256.unwrap_or("unpinned")
                        );
                    }
                    Err(e) => {
                        println!("  [INVALID] {}: {e}", expected.name);
                        all_ok = false;
                    }
                }
            }
            if all_ok {
                println!("model verification: READY");
                Ok(())
            } else {
                anyhow::bail!(
                    "model verification failed in {} (run `winsearch-cli model download` or `model import <path>`)",
                    model_dir.display()
                )
            }
        }
        ModelSubcommand::Download => match DaemonClient::connect_pipe(pipe_name) {
            Ok(mut client) => {
                println!(
                    "downloading EmbeddingGemma 2 via daemon ({})...",
                    client.pipe_name()
                );
                let mut last_pct = 0u64;
                let status = client.start_model_download(|s| {
                    if s.total_bytes > 0 {
                        let pct = (s.bytes * 100) / s.total_bytes;
                        if pct >= last_pct + 10 || s.bytes == s.total_bytes {
                            last_pct = pct;
                            println!("  progress: {pct}% ({}/{} bytes)", s.bytes, s.total_bytes);
                        }
                    }
                })?;
                match status.state {
                    ModelState::Ready => {
                        println!("download complete: {:?}", status.state);
                        Ok(())
                    }
                    other => {
                        let detail = status.error.unwrap_or_else(|| format!("state={other:?}"));
                        anyhow::bail!("daemon model download did not reach Ready: {detail}")
                    }
                }
            }
            Err(IpcError::NotConnected { .. }) => {
                println!("downloading EmbeddingGemma 2 into {}...", model_dir.display());
                let cancel = AtomicBool::new(false);
                let last_pct = std::sync::Mutex::new(0u64);
                let progress = |done: u64, total: u64| {
                    if total == 0 {
                        return;
                    }
                    let pct = (done * 100) / total;
                    let mut guard = last_pct.lock().unwrap_or_else(|p| p.into_inner());
                    if pct >= *guard + 10 || done == total {
                        *guard = pct;
                        println!("  progress: {pct}% ({done}/{total} bytes)");
                    }
                };
                mgr.download_all(&progress, &cancel)?;
                let status = mgr.status();
                println!("download complete: {:?}", status.state);
                Ok(())
            }
            Err(e) => Err(e.into()),
        },
        ModelSubcommand::Import { path } => match DaemonClient::connect_pipe(pipe_name) {
            Ok(mut client) => {
                println!(
                    "importing model from {} via daemon ({})...",
                    path.display(),
                    client.pipe_name()
                );
                let status = client.import_model(path.to_string_lossy().into_owned(), |s| {
                    if s.total_bytes > 0 && s.state == ModelState::Downloading {
                        println!("  progress: {}/{} bytes", s.bytes, s.total_bytes);
                    }
                })?;
                match status.state {
                    ModelState::Ready => {
                        println!(
                            "import complete: state={:?} ({}/{} bytes)",
                            status.state, status.bytes, status.total_bytes
                        );
                        Ok(())
                    }
                    other => {
                        let detail = status.error.unwrap_or_else(|| format!("state={other:?}"));
                        anyhow::bail!("daemon model import did not reach Ready: {detail}")
                    }
                }
            }
            Err(IpcError::NotConnected { .. }) => {
                println!(
                    "importing model from {} into {}...",
                    path.display(),
                    model_dir.display()
                );
                let cancel = AtomicBool::new(false);
                let status = mgr.import(&path, &cancel)?;
                println!(
                    "import complete: state={:?} ({}/{} bytes)",
                    status.state, status.bytes, status.total_bytes
                );
                if status.state != ModelState::Ready {
                    println!(
                        "note: partial import ({} of {} total bytes present); import remaining manifest file(s) to reach Ready",
                        status.bytes, status.total_bytes
                    );
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        },
    }
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match parse_args(&args)? {
        CliCommand::Help => {
            print_help();
            Ok(())
        }
        CliCommand::Version => {
            println!("winsearch-cli {}", ipc::CLI_VERSION);
            Ok(())
        }
        CliCommand::Status => cmd_status(),
        CliCommand::Search {
            query,
            mode,
            semantic,
            limit,
        } => cmd_search(query, mode, semantic, limit),
        CliCommand::Eval { suite } => cmd_eval(&suite),
        CliCommand::Model(sub) => cmd_model(sub),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use ws_protocol::{
        read_message, write_message, ClientMessage, EngineRunState, EngineStatus, ResultKind,
        SearchResult, ServerMessage, PROTOCOL_VERSION,
    };

    fn vec_args(slice: &[&str]) -> Vec<String> {
        slice.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn cli_arg_parsing_covers_all_subcommands() {
        assert_eq!(parse_args(&vec_args(&["status"])).unwrap(), CliCommand::Status);
        assert_eq!(
            parse_args(&vec_args(&["eval", "names"])).unwrap(),
            CliCommand::Eval {
                suite: "names".into()
            }
        );
        assert_eq!(
            parse_args(&vec_args(&["eval"])).unwrap(),
            CliCommand::Eval {
                suite: "names".into()
            }
        );
        assert_eq!(
            parse_args(&vec_args(&[
                "search",
                "budget",
                "spreadsheet",
                "--mode",
                "files",
                "--semantic",
                "--limit",
                "10"
            ]))
            .unwrap(),
            CliCommand::Search {
                query: "budget spreadsheet".into(),
                mode: SearchMode::Files,
                semantic: true,
                limit: 10,
            }
        );
        assert_eq!(
            parse_args(&vec_args(&["model", "verify"])).unwrap(),
            CliCommand::Model(ModelSubcommand::Verify)
        );
        assert_eq!(
            parse_args(&vec_args(&["model", "download"])).unwrap(),
            CliCommand::Model(ModelSubcommand::Download)
        );
        assert_eq!(
            parse_args(&vec_args(&["model", "import", "C:\\tmp\\models"])).unwrap(),
            CliCommand::Model(ModelSubcommand::Import {
                path: PathBuf::from("C:\\tmp\\models")
            })
        );
    }

    #[test]
    fn ipc_client_round_trips_status_and_search_over_real_pipe() {
        let pipe = format!(r"\\.\pipe\winsearch-cli-test-{}", std::process::id());
        let server_pipe = pipe.clone();

        std::thread::spawn(move || {
            let _ = ws_platform::pipe::serve(&server_pipe, |mut stream| {
                let hello: Option<ClientMessage> = read_message(&mut stream).unwrap();
                assert!(matches!(hello, Some(ClientMessage::Hello { .. })));
                write_message(
                    &mut stream,
                    &ServerMessage::HelloAck {
                        protocol_version: PROTOCOL_VERSION,
                        engine_version: "0.1.0-test".into(),
                        features: vec!["search.exact".into(), "search.semantic".into()],
                    },
                )
                .unwrap();
                // Push initial status like real winsearchd does.
                write_message(
                    &mut stream,
                    &ServerMessage::Status {
                        id: None,
                        status: EngineStatus {
                            state: EngineRunState::Idle,
                            files_indexed: 42,
                            segments_indexed: 42,
                            model_loaded: true,
                            ..Default::default()
                        },
                    },
                )
                .unwrap();

                while let Ok(Some(msg)) = read_message::<_, ClientMessage>(&mut stream) {
                    match msg {
                        ClientMessage::GetStatus { id } => {
                            write_message(
                                &mut stream,
                                &ServerMessage::Status {
                                    id: Some(id),
                                    status: EngineStatus {
                                        state: EngineRunState::Idle,
                                        files_indexed: 42,
                                        segments_indexed: 42,
                                        model_loaded: true,
                                        ..Default::default()
                                    },
                                },
                            )
                            .unwrap();
                        }
                        ClientMessage::Search { id, query } => {
                            write_message(
                                &mut stream,
                                &ServerMessage::Results {
                                    id,
                                    batch: vec![SearchResult {
                                        id: "e1".into(),
                                        kind: ResultKind::Document,
                                        title: format!("hit-for-{}", query.text),
                                        subtitle: "C:\\Docs\\hit.pdf".into(),
                                        match_explanation: "Exact name match".into(),
                                        score: 0.95,
                                        ..Default::default()
                                    }],
                                    done: false,
                                },
                            )
                            .unwrap();
                            write_message(
                                &mut stream,
                                &ServerMessage::Results {
                                    id,
                                    batch: vec![SearchResult {
                                        id: "s2".into(),
                                        kind: ResultKind::Document,
                                        title: "semantic-hit.xlsx".into(),
                                        subtitle: "C:\\Docs\\semantic-hit.xlsx".into(),
                                        match_explanation:
                                            "semantic name match · similarity 0.84".into(),
                                        score: 0.83,
                                        ..Default::default()
                                    }],
                                    done: true,
                                },
                            )
                            .unwrap();
                        }
                        _ => break,
                    }
                }
            });
        });

        let mut client = None;
        for _ in 0..100 {
            if let Ok(c) = DaemonClient::connect_pipe(&pipe) {
                client = Some(c);
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let mut client = client.expect("connect to test pipe");
        let status = client.get_status().expect("get_status");
        assert_eq!(status.files_indexed, 42);
        assert_eq!(status.segments_indexed, 42);
        assert!(status.model_loaded);

        let hits = client
            .search(SearchQuery {
                text: "budget".into(),
                mode: SearchMode::Files,
                limit: 10,
                include_semantic: true,
                categories: Vec::new(),
            })
            .expect("search");
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].title, "hit-for-budget");
        assert_eq!(hits[1].title, "semantic-hit.xlsx");
    }

    #[test]
    fn cmd_model_routes_download_and_import_through_connected_daemon_pipe() {
        use std::sync::{Arc, Mutex};
        use ws_protocol::ModelStatus;

        let pipe = format!(
            r"\\.\pipe\winsearch-cli-model-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        );
        let server_pipe = pipe.clone();
        let seen: Arc<Mutex<Vec<ClientMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_server = Arc::clone(&seen);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();

        std::thread::spawn(move || {
            let _ = ready_tx.send(());
            let _ = ws_platform::pipe::serve(&server_pipe, move |mut stream| {
                let hello: Option<ClientMessage> = read_message(&mut stream).unwrap();
                assert!(matches!(hello, Some(ClientMessage::Hello { .. })));
                write_message(
                    &mut stream,
                    &ServerMessage::HelloAck {
                        protocol_version: PROTOCOL_VERSION,
                        engine_version: "0.1.0-test".into(),
                        features: vec!["search.exact".into(), "search.semantic".into()],
                    },
                )
                .unwrap();

                if let Ok(Some(msg)) = read_message::<_, ClientMessage>(&mut stream) {
                    seen_server.lock().unwrap().push(msg.clone());
                    match msg {
                        ClientMessage::ModelDownloadStart { id } => {
                            write_message(
                                &mut stream,
                                &ServerMessage::ModelStatus {
                                    id: Some(id),
                                    status: ModelStatus {
                                        state: ModelState::Downloading,
                                        bytes: 0,
                                        total_bytes: 100,
                                        error: None,
                                    },
                                },
                            )
                            .unwrap();
                            write_message(
                                &mut stream,
                                &ServerMessage::ModelStatus {
                                    id: None,
                                    status: ModelStatus {
                                        state: ModelState::Downloading,
                                        bytes: 50,
                                        total_bytes: 100,
                                        error: None,
                                    },
                                },
                            )
                            .unwrap();
                            write_message(
                                &mut stream,
                                &ServerMessage::ModelStatus {
                                    id: None,
                                    status: ModelStatus {
                                        state: ModelState::Ready,
                                        bytes: 100,
                                        total_bytes: 100,
                                        error: None,
                                    },
                                },
                            )
                            .unwrap();
                        }
                        ClientMessage::ModelImport { id, .. } => {
                            write_message(
                                &mut stream,
                                &ServerMessage::ModelStatus {
                                    id: Some(id),
                                    status: ModelStatus {
                                        state: ModelState::Downloading,
                                        bytes: 0,
                                        total_bytes: 100,
                                        error: None,
                                    },
                                },
                            )
                            .unwrap();
                            write_message(
                                &mut stream,
                                &ServerMessage::ModelStatus {
                                    id: None,
                                    status: ModelStatus {
                                        state: ModelState::Ready,
                                        bytes: 100,
                                        total_bytes: 100,
                                        error: None,
                                    },
                                },
                            )
                            .unwrap();
                        }
                        _ => {}
                    }
                }
            });
        });

        ready_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(50));

        let dummy_dir = std::env::temp_dir().join(format!(
            "winsearch-cli-model-test-dir-{}",
            std::process::id()
        ));
        cmd_model_with_pipe(ModelSubcommand::Download, &pipe, dummy_dir.clone())
            .expect("daemon model download");
        cmd_model_with_pipe(
            ModelSubcommand::Import {
                path: PathBuf::from(r"C:\fake\models"),
            },
            &pipe,
            dummy_dir,
        )
        .expect("daemon model import");

        let recorded = seen.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        assert!(matches!(
            recorded[0],
            ClientMessage::ModelDownloadStart { .. }
        ));
        assert!(matches!(
            &recorded[1],
            ClientMessage::ModelImport { path, .. } if path == r"C:\fake\models"
        ));
    }
}
