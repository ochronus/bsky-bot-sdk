//! A pluggable key/value store for the pieces a bot needs to remember across
//! restarts.
//!
//! The SDK persists two kinds of state through a [`Store`]:
//!
//! - the **notification watermark**, so a restart resumes exactly where it left
//!   off instead of re-skipping whatever backlog exists at startup, and
//! - an **idempotency set** and any **conversation state** your handlers keep,
//!   reached through [`Context::store`](crate::Context::store) and the
//!   [`remember`](crate::Context::remember) /
//!   [`is_remembered`](crate::Context::is_remembered) helpers.
//!
//! Attach one with [`BotBuilder::store`](crate::BotBuilder::store). Two backends
//! ship with the crate — [`MemoryStore`] (process-local, for tests and ephemeral
//! bots) and [`FileStore`] (a JSON file, for a single-process bot) — and the trait
//! is small enough to back with SQLite, Redis, or anything else you like.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};

/// The boxed future a [`Store`] method returns. Boxing keeps `Store` object-safe,
/// so a bot can hold any backend as `Arc<dyn Store>`.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// A minimal async key/value store.
///
/// Keys and values are strings; a value is typically a small JSON blob your
/// handler (de)serializes. Implementations must be `Send + Sync` so the bot can
/// share one across its concurrent loops.
///
/// ```
/// use bsky_bot_sdk::store::{MemoryStore, Store};
///
/// # async fn demo() -> bsky_bot_sdk::Result<()> {
/// let store = MemoryStore::default();
/// store.save("greeted:alice", "1").await?;
/// assert_eq!(store.load("greeted:alice").await?, Some("1".to_string()));
/// store.remove("greeted:alice").await?;
/// assert_eq!(store.load("greeted:alice").await?, None);
/// # Ok(())
/// # }
/// ```
pub trait Store: Send + Sync {
    /// Load the value stored at `key`, or `None` if there is none.
    fn load<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<String>>;

    /// Store `value` at `key`, replacing any existing value.
    fn save<'a>(&'a self, key: &'a str, value: &'a str) -> StoreFuture<'a, ()>;

    /// Remove `key`. Removing an absent key is not an error.
    fn remove<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()>;
}

// A blanket impl so `Arc<dyn Store>`/`Arc<S>` are themselves `Store`, letting the
// SDK hold a shared handle without another layer of indirection.
impl<S: Store + ?Sized> Store for Arc<S> {
    fn load<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<String>> {
        (**self).load(key)
    }
    fn save<'a>(&'a self, key: &'a str, value: &'a str) -> StoreFuture<'a, ()> {
        (**self).save(key, value)
    }
    fn remove<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        (**self).remove(key)
    }
}

/// A process-local, in-memory [`Store`] backed by a shared map.
///
/// Cloning shares the same underlying map (via [`Arc`]), so you can keep a clone
/// to inspect after handing one to [`BotBuilder::store`](crate::BotBuilder::store)
/// — handy in tests. State is lost when the process exits.
#[derive(Clone, Default)]
pub struct MemoryStore {
    map: Arc<Mutex<HashMap<String, String>>>,
}

impl MemoryStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of keys currently held (for tests/inspection).
    pub fn len(&self) -> usize {
        self.map.lock().expect("store mutex").len()
    }

    /// Whether the store holds no keys.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Store for MemoryStore {
    fn load<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<String>> {
        Box::pin(async move { Ok(self.map.lock().expect("store mutex").get(key).cloned()) })
    }

    fn save<'a>(&'a self, key: &'a str, value: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.map
                .lock()
                .expect("store mutex")
                .insert(key.to_string(), value.to_string());
            Ok(())
        })
    }

    fn remove<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.map.lock().expect("store mutex").remove(key);
            Ok(())
        })
    }
}

/// A [`Store`] backed by a single JSON file, holding the whole map.
///
/// The map is cached in memory and rewritten on every `save`/`remove`, so it suits
/// a single-process bot with low-frequency state (the watermark, small idempotency
/// sets). Writes are synchronous filesystem writes; for high write rates or
/// multi-process deployments, back [`Store`] with a real database instead.
pub struct FileStore {
    path: PathBuf,
    cache: Mutex<HashMap<String, String>>,
}

impl FileStore {
    /// Open (or create) a file-backed store at `path`, loading any existing map.
    ///
    /// A file that exists but does not parse as a JSON object is treated as empty
    /// rather than failing, so a corrupt state file never bricks startup.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let cache = if path.exists() {
            let bytes = std::fs::read(&path)?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            HashMap::new()
        };
        Ok(Self {
            path,
            cache: Mutex::new(cache),
        })
    }

    /// Write the current map to disk (called while the cache lock is held, so no
    /// `await` happens across it).
    fn persist(&self, map: &HashMap<String, String>) -> Result<()> {
        let json = serde_json::to_vec_pretty(map)?;
        std::fs::write(&self.path, json).map_err(Error::from)
    }
}

impl Store for FileStore {
    fn load<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<String>> {
        Box::pin(async move { Ok(self.cache.lock().expect("store mutex").get(key).cloned()) })
    }

    fn save<'a>(&'a self, key: &'a str, value: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let mut map = self.cache.lock().expect("store mutex");
            map.insert(key.to_string(), value.to_string());
            self.persist(&map)
        })
    }

    fn remove<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let mut map = self.cache.lock().expect("store mutex");
            map.remove(key);
            self.persist(&map)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_store_round_trips_and_removes() {
        let store = MemoryStore::new();
        assert_eq!(store.load("k").await.unwrap(), None, "absent key is None");

        store.save("k", "v1").await.unwrap();
        assert_eq!(store.load("k").await.unwrap(), Some("v1".into()));

        // Save overwrites.
        store.save("k", "v2").await.unwrap();
        assert_eq!(store.load("k").await.unwrap(), Some("v2".into()));

        store.remove("k").await.unwrap();
        assert_eq!(store.load("k").await.unwrap(), None, "removed key is None");
        // Removing again is not an error.
        store.remove("k").await.unwrap();
    }

    #[tokio::test]
    async fn memory_store_clone_shares_state() {
        let a = MemoryStore::new();
        let b = a.clone();
        a.save("shared", "yes").await.unwrap();
        assert_eq!(
            b.load("shared").await.unwrap(),
            Some("yes".into()),
            "a clone observes writes through the shared map",
        );
        assert_eq!(b.len(), 1);
    }

    #[tokio::test]
    async fn file_store_persists_across_reopen() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "bsky_bot_sdk_store_test_{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        {
            let store = FileStore::new(&path).expect("open store");
            store
                .save("watermark", "2026-07-23T00:00:00.000Z")
                .await
                .unwrap();
            store.save("temp", "x").await.unwrap();
            store.remove("temp").await.unwrap();
        }

        // Reopen: the saved key survives, the removed one does not.
        let reopened = FileStore::new(&path).expect("reopen store");
        assert_eq!(
            reopened.load("watermark").await.unwrap(),
            Some("2026-07-23T00:00:00.000Z".into()),
            "saved state survives a reopen",
        );
        assert_eq!(
            reopened.load("temp").await.unwrap(),
            None,
            "removed state stays removed after reopen",
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn file_store_tolerates_a_corrupt_file() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("bsky_bot_sdk_corrupt_{}.json", std::process::id()));
        std::fs::write(&path, b"not json at all").unwrap();

        let store = FileStore::new(&path).expect("a corrupt file must not fail open");
        assert_eq!(store.load("anything").await.unwrap(), None);
        // And it becomes usable, overwriting the garbage.
        store.save("k", "v").await.unwrap();
        assert_eq!(store.load("k").await.unwrap(), Some("v".into()));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn works_behind_a_dyn_arc() {
        // The blanket impl lets the SDK hold `Arc<dyn Store>`.
        let store: Arc<dyn Store> = Arc::new(MemoryStore::new());
        store.save("k", "v").await.unwrap();
        assert_eq!(store.load("k").await.unwrap(), Some("v".into()));
    }
}
