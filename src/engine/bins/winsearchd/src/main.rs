//! `winsearchd` — the WinSearch indexing and search engine.
//!
//! Runs independently of the UI: it keeps indexing and answering searches over
//! the per-user named pipe even when no frontend is connected.

mod catalog;
mod code;
mod crawl;
mod embed_client;
mod engine;
mod extract;
mod extract_client;
mod indexer;
mod model;
mod policy;
mod server;
mod watcher;

use std::sync::Arc;

use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use ws_index::IndexStore;
use ws_platform::identity;
use ws_platform::paths;
use ws_platform::pipe;
use ws_platform::SingleInstanceGuard;

use engine::{EngineService, SharedEngine};
use indexer::Indexer;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    #[cfg(debug_assertions)]
    if args.iter().any(|a| a == "--test-stdin-gate") {
        // Native acceptance assigns the daemon before eager catalog children.
        let mut gate = [0u8];
        std::io::Read::read_exact(&mut std::io::stdin(), &mut gate)?;
        anyhow::ensure!(gate == [1], "test startup gate not released");
    }
    if args.iter().any(|a| a == "--version") {
        println!("winsearchd {}", server::ENGINE_VERSION);
        return Ok(());
    }
    if let Some(pos) = args.iter().position(|a| a == "--bench-crawl") {
        let count: usize = args
            .get(pos + 1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(20_000);
        return run_crawl_bench(count);
    }

    paths::ensure_layout()?;
    identity::instance_suffix()?;
    init_logging();
    info!(version = server::ENGINE_VERSION, "winsearchd starting");

    // Only one engine per session; the pipe name is per-user but a second
    // engine would contend on the database anyway.
    let _guard = match SingleInstanceGuard::acquire_engine() {
        Some(g) => g,
        None => {
            info!("another winsearchd instance is already running; exiting");
            return Ok(());
        }
    };

    let store = IndexStore::open(&paths::index_db_path())?;
    let indexer = if args.iter().any(|arg| arg == "--defer-root-admission") {
        Indexer::open_deferred(store)?
    } else {
        Indexer::open(store, &[])?
    };
    indexer.start_background()?;

    let pipe_name = identity::pipe_name()?;
    // The embed worker is supervisor-owned and lazy: constructing it spawns
    // nothing (first embed request starts the worker, idle timeout stops it).
    let embed = embed_client::EmbedSupervisor::new();
    let engine = EngineService::new_with_embed(Arc::clone(&indexer), embed);
    info!(
        bin = %engine.embed_supervisor().worker_bin().display(),
        pipe = %engine.embed_supervisor().pipe_name(),
        "embed supervisor wired to engine"
    );
    // Semantic pipeline (P3.5): one background thread drains
    // `extract_jobs(kind='name')` in 128-name batches through the bounded
    // supervisor call (120 s watchdog); crash-safe via claim/release +
    // idempotent segment upserts.
    indexer.start_embed_scheduler(engine.embed_supervisor());
    // Content extraction worker pool (P4.1): two background threads drain
    // non-name modality jobs (`doc`, `pdf`, `image`, `audio`, `video`) in
    // priority order (small + recent files first) under per-file budgets.
    indexer.start_extract_workers(engine.embed_supervisor(), engine.extraction_registry());
    engine.start_code_scheduler()?;
    spawn_status_pusher(Arc::clone(&engine));
    // Model progress/completion pushes fan out to every connected client.
    {
        let weak = Arc::downgrade(&engine);
        engine.model_service().set_notifier(Arc::new(move |msg| {
            if let Some(engine) = weak.upgrade() {
                engine.model_lifecycle_update(&msg);
            }
        }));
    }
    let handler_engine = Arc::clone(&engine);

    info!(pipe = %pipe_name, "serving IPC");
    pipe::serve(&pipe_name, move |stream| {
        server::handle_connection(stream, Arc::clone(&handler_engine))
    })?;

    Ok(())
}

/// Crawl-pipeline benchmark: synthetic tree on disk through the real
/// scan → identity → upsert path. Prints the numbers quoted in
/// `docs/benchmarks.md`. Run: `winsearchd --bench-crawl [files]`.
fn run_crawl_bench(file_count: usize) -> anyhow::Result<()> {
    use std::time::Instant;

    let dir = std::env::temp_dir().join("winsearch-bench-crawl");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let t_build = Instant::now();
    for i in 0..file_count {
        let sub = dir.join(format!("dir{:04}", i / 200));
        std::fs::create_dir_all(&sub)?;
        std::fs::write(sub.join(format!("file-{:06}.txt", i)), b"bench")?;
    }
    let build = t_build.elapsed();

    let db_dir = std::env::temp_dir().join("winsearch-bench-crawl-db");
    let _ = std::fs::remove_dir_all(&db_dir);
    let mut store = IndexStore::open(&db_dir.join("bench.db"))?;
    let root_id = store.ensure_root(&dir.to_string_lossy())?;

    let run_pass = |store: &mut IndexStore| -> anyhow::Result<(std::time::Duration, usize)> {
        let gen = store.begin_generation(root_id)?;
        let started = Instant::now();
        let mut queue = vec![dir.clone()];
        let mut entries = 0usize;
        while let Some(d) = queue.pop() {
            let scan = crawl::scan_directory(root_id, &d.to_string_lossy(), None, 0)?;
            entries += scan.entries.len();
            queue.extend(scan.subdirs.iter().map(std::path::PathBuf::from));
            store.apply_batch(root_id, gen, &scan.entries)?;
        }
        Ok((started.elapsed(), entries))
    };

    let (full, entries) = run_pass(&mut store)?;
    let (incremental, _) = run_pass(&mut store)?;
    let stats = store.stats()?;

    println!("winsearch crawl-pipeline benchmark");
    println!(
        "files: {file_count}, dirs: {}, cpus: {}",
        file_count / 200,
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(0)
    );
    println!();
    println!("tree creation:               {build:>10.2?}");
    println!(
        "full crawl (scan+identity+upsert, {entries} entries): {full:>8.2?}  ({:.0} files/s)",
        file_count as f64 / full.as_secs_f64()
    );
    println!(
        "incremental crawl (unchanged): {incremental:>10.2?}  ({:.0} files/s)",
        file_count as f64 / incremental.as_secs_f64()
    );
    println!(
        "index size: {:.1} MiB",
        stats.index_size_bytes as f64 / 1_048_576.0
    );

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&db_dir);
    Ok(())
}

/// Pushes an engine-status update to connected clients whenever the indexing
/// picture changes (new files indexed, queue drained, ...).
fn spawn_status_pusher(engine: SharedEngine) {
    std::thread::Builder::new()
        .name("winsearch-status".into())
        .spawn(move || {
            let mut last = engine.status();
            loop {
                std::thread::sleep(std::time::Duration::from_secs(2));
                let current = engine.status();
                if current != last {
                    last = current;
                    engine.set_status(|_| {});
                }
            }
        })
        .ok();
}

fn init_logging() {
    let logs_dir = paths::logs_dir();
    if let Err(e) = std::fs::create_dir_all(&logs_dir) {
        eprintln!("warning: cannot create log directory: {e}");
        return;
    }
    let appender = tracing_appender::rolling::daily(&logs_dir, "winsearchd.log");
    let (writer, guard) = tracing_appender::non_blocking(appender);
    // Keep the guard alive for the process lifetime.
    Box::leak(Box::new(guard));

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("winsearchd=info,ws_platform=info,ws_protocol=warn,ws_index=warn")
    });

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_ansi(false)
        .init();

    warn!(path = %logs_dir.display(), "file logging active");
}
