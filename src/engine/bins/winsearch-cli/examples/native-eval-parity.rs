//! Compare the existing offline ranker with public results on an isolated native
//! index snapshot. Invoked by tools/test-search-parity.py, never by the daemon.
#[allow(dead_code)]
#[path = "../src/eval.rs"]
mod eval;

use std::path::PathBuf;

use eval::EvalEmbedder;
use serde::Deserialize;
use serde_json::{json, Value};
use ws_index::{model::SearchRequest, IndexStore};
use ws_protocol::{MatchLocation, SearchMode, SearchQuery, SearchResult};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    index: PathBuf,
    code_index: PathBuf,
    model: Option<PathBuf>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    query: SearchQuery,
    expected: Vec<SearchResult>,
}

fn offline_identity(hit: &eval::RankedEvalHit) -> Value {
    let location = if let Some(code) = &hit.code {
        Some(if let Some(cell) = code.notebook {
            MatchLocation::Notebook {
                cell_ordinal: cell.cell_ordinal,
                line: cell.start_line,
                end_line: cell.end_line,
            }
        } else {
            MatchLocation::Line {
                line: code.start_line,
            }
        })
    } else {
        hit.matches.first().and_then(content_location)
    };
    let id = if let Some(code) = &hit.code {
        format!(
            "code:{}:{}:{}:{}",
            hit.entry_id, code.start_line, code.is_definition, code.symbol
        )
    } else {
        let prefix = if hit.explanation.starts_with("semantic ") {
            's'
        } else {
            'e'
        };
        format!("{prefix}{}", hit.entry_id)
    };
    let moments: Vec<_> = if hit.code.is_some() {
        Vec::new()
    } else {
        hit.matches.iter().filter_map(content_location).collect()
    };
    json!({"id":id,"path":hit.path,"location":location,"moments":moments})
}

fn content_location(value: &ws_index::model::ContentMatch) -> Option<MatchLocation> {
    if let Some(page) = value.page {
        return Some(MatchLocation::Page { page });
    }
    if let Some(start) = value.start_ms {
        return Some(MatchLocation::Timestamp {
            seconds: start as f64 / 1000.0,
            end_seconds: value.end_ms.unwrap_or(start) as f64 / 1000.0,
        });
    }
    value.line.map(|line| MatchLocation::Line { line })
}

fn native_identity(hit: &SearchResult) -> Value {
    let moments: Vec<_> = hit
        .matching_moments
        .iter()
        .filter_map(|moment| moment.location)
        .collect();
    json!({"id":hit.id,"path":hit.path,"location":hit.location,"moments":moments})
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let input = PathBuf::from(
        args.next()
            .ok_or_else(|| anyhow::anyhow!("snapshot manifest required"))?,
    );
    let output = PathBuf::from(
        args.next()
            .ok_or_else(|| anyhow::anyhow!("result path required"))?,
    );
    anyhow::ensure!(args.next().is_none(), "unexpected arguments");
    let snapshot: Snapshot = serde_json::from_slice(&std::fs::read(input)?)?;
    anyhow::ensure!(!snapshot.cases.is_empty(), "no native cases supplied");
    let store = IndexStore::open(&snapshot.index)?;
    let code_store = ws_code::CodeStore::open(&snapshot.code_index)?;
    let embedder: Box<dyn EvalEmbedder> = match snapshot.model {
        Some(path) => {
            anyhow::ensure!(path.is_file(), "requested real model does not exist");
            Box::new(eval::WorkerModelEmbedder::new(path))
        }
        None => {
            anyhow::ensure!(
                snapshot
                    .cases
                    .iter()
                    .all(|case| !case.query.include_semantic),
                "semantic snapshot comparison requires the real model"
            );
            Box::new(eval::ConceptStubEmbedder)
        }
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    let mut results = Vec::new();
    for case in snapshot.cases {
        anyhow::ensure!(
            case.query.categories.is_empty(),
            "snapshot category parity is not implemented"
        );
        let hits = if case.query.mode == SearchMode::Code {
            eval::execute_code_query(
                &code_store,
                embedder.as_ref(),
                &case.query.text,
                case.query.include_semantic,
                case.query.limit as usize,
                now,
            )?
        } else {
            anyhow::ensure!(
                matches!(case.query.mode, SearchMode::Files | SearchMode::Apps),
                "snapshot comparison covers indexed file/app/code providers only"
            );
            eval::execute_hybrid_request(
                &store,
                embedder.as_ref(),
                &SearchRequest {
                    text: case.query.text.clone(),
                    apps_only: case.query.mode == SearchMode::Apps,
                    files_only: case.query.mode == SearchMode::Files,
                    limit: case.query.limit as usize,
                    ..Default::default()
                },
                case.query.include_semantic,
                now,
            )?
        };
        let offline: Vec<_> = hits.iter().map(offline_identity).collect();
        let native: Vec<_> = case.expected.iter().map(native_identity).collect();
        results.push(json!({"name":case.name,"query":case.query,
            "equal":offline==native,"offline":offline,"native":native}));
    }
    let equal = results.iter().all(|case| case["equal"] == true);
    std::fs::write(
        output,
        serde_json::to_vec_pretty(&json!({
            "scope":"shared offline query functions versus public daemon results on native DB snapshots; no re-extraction",
            "embedder":embedder.label(),"equal":equal,"cases":results
        }))?,
    )?;
    anyhow::ensure!(
        equal,
        "offline/native ordered identity or source location differs; see result file"
    );
    Ok(())
}
