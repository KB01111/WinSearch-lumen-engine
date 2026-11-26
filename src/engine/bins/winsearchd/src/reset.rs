//! One admission/drain boundary shared by ordinary and code work.
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(crate) struct WorkGate {
    state: Mutex<(bool, usize)>,
    drained: Condvar,
}
pub(crate) struct WorkTicket(Arc<WorkGate>);
impl WorkGate {
    pub(crate) fn enter(self: &Arc<Self>) -> Option<WorkTicket> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.0 {
            return None;
        }
        state.1 += 1;
        Some(WorkTicket(self.clone()))
    }
    pub(crate) fn close(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.0 {
            return false;
        }
        state.0 = true;
        true
    }
    pub(crate) fn active_count(&self) -> usize {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).1
    }
    pub(crate) fn drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        while state.1 != 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            state = self
                .drained
                .wait_timeout(state, remaining)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        true
    }
    pub(crate) fn reopen(&self) {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).0 = false;
    }
}
impl Drop for WorkTicket {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        state.1 -= 1;
        if state.1 == 0 {
            self.0.drained.notify_all();
        }
    }
}

const INTENT: &str = "index_reset_intent";

pub(crate) fn recover(
    ordinary: &mut ws_index::IndexStore,
    code: &mut ws_code::CodeStore,
) -> anyhow::Result<()> {
    if let Some(json) = ordinary.meta_get(INTENT)? {
        let target: ws_protocol::IndexReset = serde_json::from_str(&json)?;
        anyhow::ensure!(target.is_valid(), "invalid pending reset intent");
        ordinary.reset_to(&target.index_id)?;
        code.reset_to(&target.code_index_id)?;
        ordinary.finish_index_reset()?;
    }
    Ok(())
}

pub(crate) fn execute(
    ordinary: &mut ws_index::IndexStore,
    code: &mut ws_code::CodeStore,
) -> anyhow::Result<ws_protocol::IndexReset> {
    let target = ws_protocol::IndexReset {
        index_id: new_id()?,
        code_index_id: new_id()?,
    };
    ordinary.begin_index_reset(&serde_json::to_string(&target)?)?;
    recover(ordinary, code)?;
    Ok(target)
}
fn new_id() -> anyhow::Result<String> {
    // Database IDs use the existing 32 lowercase hexadecimal wire convention.
    Ok(format!(
        "{:032x}",
        unsafe { windows::Win32::System::Com::CoCreateGuid() }
            .map_err(|_| anyhow::anyhow!("cannot generate reset namespace"))?
            .to_u128()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn uuid() -> String {
        new_id().unwrap()
    }
    #[test]
    fn closed_gate_rejects_new_work_and_waits_for_actual_retirement() {
        let gate = Arc::new(WorkGate::default());
        let ticket = gate.enter().unwrap();
        assert!(gate.close());
        assert!(!gate.close());
        assert!(gate.enter().is_none());
        assert!(!gate.drain(Duration::from_millis(10)));
        drop(ticket);
        assert!(gate.drain(Duration::from_millis(10)));
        gate.reopen();
        assert!(gate.enter().is_some());
    }
    #[test]
    fn reset_recovers_after_either_database_commit_and_preserves_other_profiles() {
        for stage in 0..=2 {
            let root = std::env::temp_dir().join(format!("winsearch-reset-{}-{stage}", uuid()));
            std::fs::create_dir_all(&root).unwrap();
            let mut ordinary = ws_index::IndexStore::open(&root.join("index.db")).unwrap();
            let mut code = ws_code::CodeStore::open(&root.join("code.db")).unwrap();
            let mut other = ws_index::IndexStore::open_in_memory().unwrap();
            let other_id = other.database_id().to_owned();
            other.meta_set("host_pin", "preserved").unwrap();
            ordinary
                .meta_set("indexing_config", "{\"roots\":[]}")
                .unwrap();
            let old = ordinary.database_id().to_owned();
            let target = ws_protocol::IndexReset {
                index_id: uuid(),
                code_index_id: uuid(),
            };
            ordinary
                .begin_index_reset(&serde_json::to_string(&target).unwrap())
                .unwrap();
            if stage >= 1 {
                ordinary.reset_to(&target.index_id).unwrap();
            }
            if stage >= 2 {
                code.reset_to(&target.code_index_id).unwrap();
            }
            drop(ordinary);
            drop(code);
            let mut ordinary = ws_index::IndexStore::open(&root.join("index.db")).unwrap();
            let mut code = ws_code::CodeStore::open(&root.join("code.db")).unwrap();
            recover(&mut ordinary, &mut code).unwrap();
            assert_eq!(ordinary.database_id(), target.index_id);
            assert_ne!(ordinary.database_id(), old);
            assert_eq!(code.database_id(), target.code_index_id);
            assert!(ordinary.meta_get(INTENT).unwrap().is_none());
            assert_eq!(
                ordinary.meta_get("indexing_config").unwrap().as_deref(),
                Some("{\"roots\":[]}")
            );
            assert_eq!(other.database_id(), other_id);
            assert_eq!(
                other.meta_get("host_pin").unwrap().as_deref(),
                Some("preserved")
            );
            recover(&mut ordinary, &mut code).unwrap();
            assert_eq!(ordinary.database_id(), target.index_id);
            drop(ordinary);
            drop(code);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
