//! Bounded reuse inside one lexical planning pass only. Delivery is never cached.
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

type Key = (String, Option<String>, bool);
const MAX_KEYS: usize = 4096;
const MAX_KEY_BYTES: usize = 2 * 1024 * 1024;

struct Pass {
    current: Box<dyn Fn() -> bool + Send + Sync>,
    entries: HashMap<Key, bool>,
    bytes: usize,
}
#[derive(Default)]
struct State {
    active: AtomicBool,
    pass: Mutex<Option<Pass>>,
}
#[derive(Clone, Default)]
pub(crate) struct QueryCache(Arc<State>);
pub(crate) struct Scope<'a> {
    cache: &'a QueryCache,
}
impl Drop for Scope<'_> {
    fn drop(&mut self) {
        self.cache.0.active.store(false, Ordering::Release);
        *self.cache.0.pass.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }
}
impl QueryCache {
    pub(crate) fn begin(
        &self,
        current: Box<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Scope<'_>, &'static str> {
        let mut pass = self.0.pass.lock().unwrap_or_else(|p| p.into_inner());
        if pass.is_some() {
            return Err("nested source-validation cache scope");
        }
        *pass = Some(Pass {
            current,
            entries: HashMap::new(),
            bytes: 0,
        });
        self.0.active.store(true, Ordering::Release);
        Ok(Scope { cache: self })
    }
    pub(crate) fn evaluate(&self, key: &Key, live: impl FnOnce() -> bool) -> bool {
        if !self.0.active.load(Ordering::Acquire) {
            return live();
        }
        {
            let mut pass = self.0.pass.lock().unwrap_or_else(|p| p.into_inner());
            if pass.as_ref().is_some_and(|pass| !(pass.current)()) {
                *pass = None;
                self.0.active.store(false, Ordering::Release);
            }
            if let Some(value) = pass.as_ref().and_then(|pass| pass.entries.get(key)) {
                return *value;
            }
        }
        let value = live();
        let mut pass = self.0.pass.lock().unwrap_or_else(|p| p.into_inner());
        if pass.as_ref().is_some_and(|pass| !(pass.current)()) {
            *pass = None;
            self.0.active.store(false, Ordering::Release);
        }
        if let Some(pass) = pass.as_mut() {
            let bytes = key
                .0
                .len()
                .saturating_add(key.1.as_ref().map_or(0, String::len));
            if pass.entries.len() < MAX_KEYS && bytes <= MAX_KEY_BYTES.saturating_sub(pass.bytes) {
                pass.entries.insert(key.clone(), value);
                pass.bytes += bytes;
            }
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_broken_fence_stays_uncached_and_panic_cleanup_removes_the_scope() {
        let cache = QueryCache::default();
        let intact = Arc::new(AtomicBool::new(true));
        let fence = intact.clone();
        let key = ("fixture".into(), None, false);
        let scope = cache
            .begin(Box::new(move || fence.load(Ordering::SeqCst)))
            .unwrap();
        assert!(cache.evaluate(&key, || true));
        intact.store(false, Ordering::SeqCst);
        assert!(!cache.evaluate(&key, || false));
        intact.store(true, Ordering::SeqCst);
        assert!(
            !cache.evaluate(&key, || false),
            "a broken pass cannot become reusable again"
        );
        drop(scope);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _scope = cache.begin(Box::new(|| true)).unwrap();
            cache.evaluate(&key, || panic!("controlled guard failure"));
        }));
        assert!(!cache.evaluate(&key, || false));
        assert!(cache.begin(Box::new(|| true)).is_ok());
    }
    #[test]
    fn key_memory_is_bounded_without_changing_live_admission() {
        let cache = QueryCache::default();
        let _scope = cache.begin(Box::new(|| true)).unwrap();
        let large = ("x".repeat(MAX_KEY_BYTES + 1), None, false);
        assert!(cache.evaluate(&large, || true));
        assert!(!cache.evaluate(&large, || false));
        for index in 0..MAX_KEYS + 1 {
            assert!(cache.evaluate(&(index.to_string(), None, false), || true));
        }
        assert!(!cache.evaluate(&(MAX_KEYS.to_string(), None, false), || false));
    }

    #[test]
    fn a_fence_broken_during_a_live_check_cannot_resurrect_earlier_results() {
        let cache = QueryCache::default();
        let intact = Arc::new(AtomicBool::new(true));
        let fence = intact.clone();
        let _scope = cache
            .begin(Box::new(move || fence.load(Ordering::SeqCst)))
            .unwrap();
        let prior = ("prior".into(), None, false);
        assert!(cache.evaluate(&prior, || true));
        cache.evaluate(&("later".into(), None, false), || {
            intact.store(false, Ordering::SeqCst);
            true
        });
        intact.store(true, Ordering::SeqCst);
        assert!(!cache.evaluate(&prior, || false));
    }
}
