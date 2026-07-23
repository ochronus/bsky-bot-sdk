//! Real-time ingestion from the Bluesky [Jetstream] firehose.
//!
//! Where the notification loop only sees events about *the bot's own* account,
//! Jetstream is a live view of the *whole network*: every create/update/delete
//! commit, plus identity and account events. This module connects to a public
//! Jetstream instance over a WebSocket, decodes its lightweight JSON events, and
//! dispatches them to stream handlers registered on the
//! [`BotBuilder`](crate::BotBuilder) — the same dispatch shape as notification
//! handlers, so a bot can mix both.
//!
//! Register handlers with [`on_firehose`](crate::BotBuilder::on_firehose),
//! [`on_keyword`](crate::BotBuilder::on_keyword), or
//! [`on_hashtag`](crate::BotBuilder::on_hashtag). Keyword and hashtag handlers
//! automatically subscribe to `app.bsky.feed.post`; a firehose handler receives
//! whatever collections you configure (or the entire network if you configure
//! none).
//!
//! ```no_run
//! use bsky_bot_sdk::Bot;
//!
//! # async fn demo() -> bsky_bot_sdk::Result<()> {
//! Bot::builder()
//!     .credentials("mybot.bsky.social", "app-password")
//!     .on_keyword("rustlang", |_ctx, event| async move {
//!         if let Some(text) = event.text() {
//!             println!("{} posted: {text}", event.did());
//!         }
//!         Ok(())
//!     })
//!     .build()
//!     .await?
//!     .run()
//!     .await
//! # }
//! ```
//!
//! Compression (`zstd`) is not yet supported; the client requests the
//! uncompressed JSON stream.
//!
//! [Jetstream]: https://docs.bsky.app/blog/jetstream

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use atrium_api::app::bsky::feed::post;
use atrium_api::com::atproto::repo::strong_ref;
use atrium_api::types::string::Cid;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::handler::BoxFuture;

/// The default public Jetstream instance (US-East).
pub const DEFAULT_JETSTREAM_ENDPOINT: &str = "wss://jetstream2.us-east.bsky.network/subscribe";

/// The `app.bsky.feed.post` collection NSID, subscribed to implicitly by keyword
/// and hashtag handlers.
const POST_COLLECTION: &str = "app.bsky.feed.post";

// ---------------------------------------------------------------------------
// Event model
// ---------------------------------------------------------------------------

/// The kind of a Jetstream event.
///
/// Unknown / future kinds are preserved via [`StreamKind::Other`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamKind {
    /// A repository commit: a record was created, updated, or deleted.
    Commit,
    /// An identity update (handle or DID-document change).
    Identity,
    /// An account status change (e.g. active → deactivated or takendown).
    Account,
    /// Any other / future kind, carrying the raw string.
    Other(String),
}

impl StreamKind {
    fn parse(kind: &str) -> Self {
        match kind {
            "commit" => Self::Commit,
            "identity" => Self::Identity,
            "account" => Self::Account,
            other => Self::Other(other.to_string()),
        }
    }
}

impl core::fmt::Display for StreamKind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Commit => "commit",
            Self::Identity => "identity",
            Self::Account => "account",
            Self::Other(s) => s.as_str(),
        })
    }
}

/// The operation carried by a commit event.
///
/// Unknown / future operations are preserved via [`CommitOp::Other`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommitOp {
    /// A new record was created.
    Create,
    /// An existing record was replaced.
    Update,
    /// A record was deleted (no `record`/`cid` is present).
    Delete,
    /// Any other / future operation, carrying the raw string.
    Other(String),
}

impl CommitOp {
    fn parse(op: &str) -> Self {
        match op {
            "create" => Self::Create,
            "update" => Self::Update,
            "delete" => Self::Delete,
            other => Self::Other(other.to_string()),
        }
    }

    /// Whether this operation carries record contents (create or update).
    fn is_write(&self) -> bool {
        matches!(self, Self::Create | Self::Update)
    }
}

impl core::fmt::Display for CommitOp {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Other(s) => s.as_str(),
        })
    }
}

/// The `commit` payload of a Jetstream commit event.
#[derive(Debug, Clone, Deserialize)]
pub struct RawCommit {
    /// The repository revision this commit produced.
    #[serde(default)]
    pub rev: Option<String>,
    /// The operation string: `create`, `update`, or `delete`.
    pub operation: String,
    /// The collection NSID (e.g. `app.bsky.feed.post`).
    pub collection: String,
    /// The record key.
    pub rkey: String,
    /// The record contents. Absent for deletes.
    #[serde(default)]
    pub record: Option<serde_json::Value>,
    /// The record CID, as a string. Absent for deletes.
    #[serde(default)]
    pub cid: Option<String>,
}

/// The raw, deserialized Jetstream event.
///
/// Most callers use the ergonomic accessors on [`StreamEvent`] instead of
/// reading these fields directly, but the raw form is always available via
/// [`StreamEvent::raw`].
#[derive(Debug, Clone, Deserialize)]
pub struct RawStreamEvent {
    /// The DID of the repository this event concerns.
    pub did: String,
    /// The event's cursor timestamp, in unix microseconds.
    pub time_us: u64,
    /// The event kind string: `commit`, `identity`, or `account`.
    pub kind: String,
    /// The commit payload, present when `kind == "commit"`.
    #[serde(default)]
    pub commit: Option<RawCommit>,
    /// The identity payload, present when `kind == "identity"`.
    #[serde(default)]
    pub identity: Option<serde_json::Value>,
    /// The account payload, present when `kind == "account"`.
    #[serde(default)]
    pub account: Option<serde_json::Value>,
}

/// An ergonomic wrapper over a single [Jetstream] event.
///
/// Mirrors [`Notification`](crate::Notification): typed accessors for the
/// commonly needed fields, with the raw event always reachable via
/// [`raw`](StreamEvent::raw). Cheap to clone (the inner event is `Arc`-shared),
/// so it can be handed to several handlers and moved into spawned tasks.
///
/// [Jetstream]: https://docs.bsky.app/blog/jetstream
#[derive(Debug, Clone)]
pub struct StreamEvent {
    inner: Arc<RawStreamEvent>,
}

impl StreamEvent {
    /// Wrap a raw event.
    pub fn from_raw(raw: RawStreamEvent) -> Self {
        Self {
            inner: Arc::new(raw),
        }
    }

    /// Borrow the underlying raw event.
    pub fn raw(&self) -> &RawStreamEvent {
        &self.inner
    }

    /// The DID of the repository this event concerns.
    pub fn did(&self) -> &str {
        &self.inner.did
    }

    /// The event's cursor timestamp, in unix microseconds.
    pub fn time_us(&self) -> u64 {
        self.inner.time_us
    }

    /// The typed kind of this event.
    pub fn kind(&self) -> StreamKind {
        StreamKind::parse(&self.inner.kind)
    }

    /// The commit payload, if this is a commit event.
    fn commit(&self) -> Option<&RawCommit> {
        self.inner.commit.as_ref()
    }

    /// The commit operation, if this is a commit event.
    pub fn operation(&self) -> Option<CommitOp> {
        self.commit().map(|c| CommitOp::parse(&c.operation))
    }

    /// The collection NSID this commit is in, if this is a commit event.
    pub fn collection(&self) -> Option<&str> {
        self.commit().map(|c| c.collection.as_str())
    }

    /// The record key of this commit, if this is a commit event.
    pub fn rkey(&self) -> Option<&str> {
        self.commit().map(|c| c.rkey.as_str())
    }

    /// The record CID string of this commit, if present (absent on deletes).
    pub fn cid(&self) -> Option<&str> {
        self.commit().and_then(|c| c.cid.as_deref())
    }

    /// The raw record contents of this commit, if present (absent on deletes).
    pub fn record(&self) -> Option<&serde_json::Value> {
        self.commit().and_then(|c| c.record.as_ref())
    }

    /// The AT-URI of the record this commit concerns
    /// (`at://<did>/<collection>/<rkey>`), if this is a commit event.
    pub fn uri(&self) -> Option<String> {
        self.commit()
            .map(|c| format!("at://{}/{}/{}", self.inner.did, c.collection, c.rkey))
    }

    /// Whether this event is a commit in the `app.bsky.feed.post` collection.
    pub fn is_post(&self) -> bool {
        self.collection() == Some(POST_COLLECTION)
    }

    /// Attempt to decode this commit's record as an `app.bsky.feed.post`.
    ///
    /// Returns `None` for deletes, non-post collections, or a record that does
    /// not decode as a post. Fallible by design: never panics on a mismatch.
    pub fn as_post(&self) -> Option<post::RecordData> {
        let record = self.record()?;
        serde_json::from_value(record.clone()).ok()
    }

    /// The plain text of this commit's record, if it is a post.
    pub fn text(&self) -> Option<String> {
        self.as_post().map(|p| p.text)
    }

    /// A [`strong_ref`] (`uri` + `cid`) pointing at the record this commit
    /// created — the thing you would like, repost, or reply to.
    ///
    /// Returns `None` when there is no CID (e.g. a delete) or the CID is
    /// unparseable.
    pub fn strong_ref(&self) -> Option<strong_ref::Main> {
        let commit = self.commit()?;
        let cid_str = commit.cid.as_deref()?;
        // Reuse `Cid`'s own `Deserialize` impl rather than depending on a
        // particular `FromStr` surface.
        let cid: Cid =
            serde_json::from_value(serde_json::Value::String(cid_str.to_string())).ok()?;
        let uri = format!(
            "at://{}/{}/{}",
            self.inner.did, commit.collection, commit.rkey
        );
        Some(strong_ref::MainData { cid, uri }.into())
    }

    /// The hashtags on this commit's post, lowercased and without the leading
    /// `#`. Combines the record's `tags` field with any `#tag` tokens found in
    /// the text. Empty for non-posts.
    pub fn hashtags(&self) -> Vec<String> {
        self.as_post()
            .map(|p| extract_hashtags(&p))
            .unwrap_or_default()
    }
}

/// Extract hashtags from a post: the record's structured `tags` plus any `#tag`
/// tokens scanned from the text. Returned lowercased and without the `#`.
fn extract_hashtags(post: &post::RecordData) -> Vec<String> {
    let mut tags: Vec<String> = Vec::new();
    if let Some(structured) = &post.tags {
        tags.extend(structured.iter().map(|t| t.to_lowercase()));
    }
    tags.extend(scan_hashtags(&post.text));
    tags
}

/// Scan free text for `#hashtag` tokens. A tag starts at a `#` that is not
/// preceded by an alphanumeric (so `a#b` is not a tag) and runs over following
/// alphanumeric or `_` characters. Returned lowercased, without the `#`.
fn scan_hashtags(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '#' && (i == 0 || !chars[i - 1].is_alphanumeric()) {
            let start = i + 1;
            let mut j = start;
            while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            if j > start {
                out.push(chars[start..j].iter().collect::<String>().to_lowercase());
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

type StreamHandlerFn = Arc<dyn Fn(Context, StreamEvent) -> BoxFuture<Result<()>> + Send + Sync>;

type StreamErrorHandlerFn = Arc<dyn Fn(Context, StreamEvent, Error) -> BoxFuture<()> + Send + Sync>;

/// Erase a concrete async stream handler into a [`StreamHandlerFn`].
pub(crate) fn boxed_stream_handler<F, Fut>(handler: F) -> StreamHandlerFn
where
    F: Fn(Context, StreamEvent) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    Arc::new(move |ctx, event| Box::pin(handler(ctx, event)))
}

/// Erase a concrete async stream error handler into a [`StreamErrorHandlerFn`].
pub(crate) fn boxed_stream_error_handler<F, Fut>(handler: F) -> StreamErrorHandlerFn
where
    F: Fn(Context, StreamEvent, Error) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Arc::new(move |ctx, event, err| Box::pin(handler(ctx, event, err)))
}

/// Decides whether a registered handler should fire for a given event.
#[derive(Clone)]
pub(crate) enum Matcher {
    /// Match every event (subject to the configured collection subscription).
    Firehose,
    /// Match post create/update commits whose text contains any keyword
    /// (case-insensitive). Keywords are stored lowercased.
    Keyword(Vec<String>),
    /// Match post create/update commits carrying any of these hashtags. Tags are
    /// stored lowercased, without the leading `#`.
    Hashtag(Vec<String>),
}

impl Matcher {
    fn matches(&self, event: &StreamEvent) -> bool {
        match self {
            Matcher::Firehose => true,
            Matcher::Keyword(keywords) => {
                if !Self::is_post_write(event) {
                    return false;
                }
                let Some(text) = event.text() else {
                    return false;
                };
                let lower = text.to_lowercase();
                keywords.iter().any(|k| lower.contains(k))
            }
            Matcher::Hashtag(tags) => {
                if !Self::is_post_write(event) {
                    return false;
                }
                let found = event.hashtags();
                tags.iter().any(|t| found.iter().any(|f| f == t))
            }
        }
    }

    /// True when the event is a create/update commit on a post — the only shape
    /// keyword/hashtag matching applies to.
    fn is_post_write(event: &StreamEvent) -> bool {
        event.is_post() && event.operation().is_some_and(|op| op.is_write())
    }
}

/// The registry of stream (Jetstream) handlers for a bot.
#[derive(Default, Clone)]
pub(crate) struct StreamHandlers {
    handlers: Vec<(Matcher, StreamHandlerFn)>,
    on_error: Option<StreamErrorHandlerFn>,
}

impl StreamHandlers {
    pub(crate) fn push(&mut self, matcher: Matcher, handler: StreamHandlerFn) {
        self.handlers.push((matcher, handler));
    }

    pub(crate) fn set_error(&mut self, handler: StreamErrorHandlerFn) {
        self.on_error = Some(handler);
    }

    /// True when no stream handlers are registered.
    pub(crate) fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// Collections implied by the registered matchers. Keyword and hashtag
    /// handlers imply `app.bsky.feed.post`; a firehose handler implies nothing
    /// specific (it takes whatever the subscription is configured for).
    pub(crate) fn implied_collections(&self) -> BTreeSet<String> {
        let mut set = BTreeSet::new();
        for (matcher, _) in &self.handlers {
            if matches!(matcher, Matcher::Keyword(_) | Matcher::Hashtag(_)) {
                set.insert(POST_COLLECTION.to_string());
            }
        }
        set
    }

    /// Whether any registered handler is an unfiltered firehose handler.
    pub(crate) fn has_firehose(&self) -> bool {
        self.handlers
            .iter()
            .any(|(m, _)| matches!(m, Matcher::Firehose))
    }

    /// Dispatch one event to every handler whose matcher applies. A handler
    /// returning `Err` is routed to the stream error handler (or logged) and
    /// never stops the others.
    pub(crate) async fn dispatch(&self, ctx: Context, event: StreamEvent) {
        for (matcher, handler) in &self.handlers {
            if !matcher.matches(&event) {
                continue;
            }
            if let Err(err) = handler(ctx.clone(), event.clone()).await {
                match &self.on_error {
                    Some(on_error) => on_error(ctx.clone(), event.clone(), err).await,
                    None => tracing::error!(
                        did = %event.did(),
                        kind = %event.kind(),
                        error = %err,
                        "stream handler returned an error",
                    ),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Exponential-backoff-with-jitter parameters for stream reconnection.
#[derive(Debug, Clone)]
pub struct Backoff {
    /// The base delay before the first reconnect attempt.
    pub initial: Duration,
    /// The maximum delay between attempts (the cap on exponential growth).
    pub max: Duration,
    /// The multiplier applied to the delay after each failed attempt.
    pub factor: f64,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            factor: 2.0,
        }
    }
}

impl Backoff {
    /// The base (un-jittered) delay for a zero-based `attempt`, capped at `max`.
    pub(crate) fn base_delay(&self, attempt: u32) -> Duration {
        let grown = self.initial.as_secs_f64() * self.factor.powi(attempt as i32);
        Duration::from_secs_f64(grown.min(self.max.as_secs_f64()))
    }

    /// The delay for `attempt` with jitter applied. `jitter` in `[0, 1)` scales
    /// the base delay into `[50%, 100%]`, so retries spread out rather than
    /// stampeding in lock-step.
    pub(crate) fn delay_with_jitter(&self, attempt: u32, jitter: f64) -> Duration {
        let base = self.base_delay(attempt).as_secs_f64();
        Duration::from_secs_f64(base * (0.5 + 0.5 * jitter.clamp(0.0, 1.0)))
    }
}

/// Configuration for the Jetstream real-time ingestion connection.
///
/// Tweak via the `jetstream_*` builder methods on
/// [`BotBuilder`](crate::BotBuilder), or construct directly.
#[derive(Debug, Clone)]
pub struct JetstreamConfig {
    /// The `wss://…/subscribe` endpoint to connect to.
    pub endpoint: String,
    /// Explicit collection NSIDs to subscribe to. Keyword and hashtag handlers
    /// add `app.bsky.feed.post` on top of these automatically.
    pub collections: BTreeSet<String>,
    /// Restrict the stream to these repository DIDs. Empty means all repos.
    pub dids: Vec<String>,
    /// Optional starting cursor (unix microseconds). `None` starts at the live
    /// tail.
    pub cursor: Option<u64>,
    /// Maximum server payload size in bytes. `None` or `0` means no limit.
    pub max_message_size: Option<u64>,
    /// Reconnect backoff bounds.
    pub reconnect: Backoff,
    /// On reconnect, rewind the cursor by this much for gapless playback (the
    /// Jetstream docs recommend a small negative buffer).
    pub cursor_rewind: Duration,
}

impl Default for JetstreamConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_JETSTREAM_ENDPOINT.to_string(),
            collections: BTreeSet::new(),
            dids: Vec::new(),
            cursor: None,
            max_message_size: None,
            reconnect: Backoff::default(),
            cursor_rewind: Duration::from_secs(3),
        }
    }
}

/// Build a Jetstream subscribe URL from an endpoint and the effective filters.
///
/// Collection NSIDs, DIDs, and numeric values are all made of query-safe
/// characters (`:`, `.`, `-`, `*`, digits, letters), so no percent-encoding is
/// required.
fn build_subscribe_url(
    endpoint: &str,
    collections: &BTreeSet<String>,
    dids: &[String],
    cursor: Option<u64>,
    max_message_size: Option<u64>,
) -> String {
    let mut params: Vec<String> = Vec::new();
    for collection in collections {
        params.push(format!("wantedCollections={collection}"));
    }
    for did in dids {
        params.push(format!("wantedDids={did}"));
    }
    if let Some(cursor) = cursor {
        params.push(format!("cursor={cursor}"));
    }
    if let Some(max) = max_message_size
        && max > 0
    {
        params.push(format!("maxMessageSizeBytes={max}"));
    }
    if params.is_empty() {
        endpoint.to_string()
    } else {
        format!("{endpoint}?{}", params.join("&"))
    }
}

/// A sub-second jitter unit in `[0, 1)`, derived from the system clock so we do
/// not need a random-number-generator dependency just to spread out reconnects.
pub(crate) fn jitter_unit() -> f64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos) / 1_000_000_000.0
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// The outcome of a single connect-and-stream attempt.
enum StreamOutcome {
    /// Shutdown was signalled; the runner should stop entirely.
    Shutdown,
    /// The connection ended (close, error, or failed connect). `got_data` is
    /// true if at least one event was received, which resets the backoff.
    Ended { got_data: bool },
}

/// Drives the Jetstream WebSocket connection: connect, read + dispatch events,
/// and reconnect with backoff until shutdown. Mirrors the scheduler's
/// cooperative-shutdown model so `run_until` can drive it alongside the
/// notification loop.
pub(crate) struct StreamRunner {
    config: JetstreamConfig,
    handlers: StreamHandlers,
}

impl StreamRunner {
    pub(crate) fn new(config: JetstreamConfig, handlers: StreamHandlers) -> Self {
        Self { config, handlers }
    }

    /// The effective wanted collections: explicit config plus whatever the
    /// registered handlers imply.
    fn wanted_collections(&self) -> BTreeSet<String> {
        let mut collections = self.config.collections.clone();
        collections.extend(self.handlers.implied_collections());
        collections
    }

    /// Build the subscribe URL for a given resume cursor.
    fn build_url(&self, cursor: Option<u64>) -> String {
        let rewound = cursor.map(|c| {
            let rewind = self
                .config
                .cursor_rewind
                .as_micros()
                .min(u128::from(u64::MAX)) as u64;
            c.saturating_sub(rewind)
        });
        build_subscribe_url(
            &self.config.endpoint,
            &self.wanted_collections(),
            &self.config.dids,
            rewound,
            self.config.max_message_size,
        )
    }

    /// Reconnect loop. Returns when `shutdown` flips to `true`.
    pub(crate) async fn run(self, ctx: Context, mut shutdown: watch::Receiver<bool>) {
        if self.wanted_collections().is_empty() && self.handlers.has_firehose() {
            tracing::warn!(
                "jetstream firehose has no collection filter; subscribing to the entire network \
                 (high volume). Set jetstream_collections(...) to narrow it.",
            );
        }

        let mut cursor = self.config.cursor;
        let mut attempt: u32 = 0;

        loop {
            if *shutdown.borrow() {
                break;
            }

            let url = self.build_url(cursor);
            tracing::info!(url = %url, "connecting to jetstream");

            match self
                .connect_and_stream(&url, &ctx, &mut shutdown, &mut cursor)
                .await
            {
                StreamOutcome::Shutdown => break,
                StreamOutcome::Ended { got_data } => {
                    if got_data {
                        attempt = 0;
                    }
                }
            }

            if *shutdown.borrow() {
                break;
            }

            let delay = self
                .config
                .reconnect
                .delay_with_jitter(attempt, jitter_unit());
            attempt = attempt.saturating_add(1);
            tracing::warn!(
                delay_ms = delay.as_millis() as u64,
                attempt,
                "jetstream disconnected; reconnecting after backoff",
            );
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.changed() => break,
            }
        }

        tracing::info!("jetstream ingestion stopped");
    }

    /// Connect once and pump messages until the socket closes, errors, or
    /// shutdown is signalled.
    async fn connect_and_stream(
        &self,
        url: &str,
        ctx: &Context,
        shutdown: &mut watch::Receiver<bool>,
        cursor: &mut Option<u64>,
    ) -> StreamOutcome {
        let connect = tokio::select! {
            _ = shutdown.changed() => return StreamOutcome::Shutdown,
            result = connect_async(url) => result,
        };
        let mut ws = match connect {
            Ok((ws, _response)) => ws,
            Err(err) => {
                tracing::warn!(error = %err, "jetstream connection failed");
                return StreamOutcome::Ended { got_data: false };
            }
        };
        tracing::info!("jetstream connected");

        let mut got_data = false;
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    let _ = ws.close(None).await;
                    return StreamOutcome::Shutdown;
                }
                message = ws.next() => match message {
                    None => return StreamOutcome::Ended { got_data },
                    Some(Err(err)) => {
                        tracing::warn!(error = %err, "jetstream read error");
                        return StreamOutcome::Ended { got_data };
                    }
                    Some(Ok(Message::Text(text))) => {
                        got_data = true;
                        self.handle_text(text.as_str(), ctx, cursor).await;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        // Keep the connection alive. tungstenite also queues an
                        // automatic pong, but replying explicitly is harmless and
                        // robust across versions.
                        let _ = ws.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Close(_))) => return StreamOutcome::Ended { got_data },
                    Some(Ok(Message::Binary(_))) => {
                        // Compression is not requested, so binary frames are
                        // unexpected; ignore rather than mis-decode.
                        tracing::debug!("ignoring unexpected binary jetstream frame");
                    }
                    Some(Ok(_)) => {} // Pong / raw Frame: nothing to do.
                }
            }
        }
    }

    /// Parse one JSON event, advance the cursor, and dispatch it.
    async fn handle_text(&self, text: &str, ctx: &Context, cursor: &mut Option<u64>) {
        match serde_json::from_str::<RawStreamEvent>(text) {
            Ok(raw) => {
                *cursor = Some(raw.time_us);
                self.handlers
                    .dispatch(ctx.clone(), StreamEvent::from_raw(raw))
                    .await;
            }
            Err(err) => tracing::debug!(error = %err, "failed to parse jetstream event"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::BotIdentity;
    use crate::ratelimit::WriteBudget;
    use std::sync::Mutex;

    // --- fixtures ----------------------------------------------------------

    fn event_from_json(value: serde_json::Value) -> StreamEvent {
        let raw: RawStreamEvent = serde_json::from_value(value).expect("valid jetstream fixture");
        StreamEvent::from_raw(raw)
    }

    fn post_commit(op: &str, text: &str, tags: Option<Vec<&str>>) -> serde_json::Value {
        let mut record = serde_json::json!({
            "$type": "app.bsky.feed.post",
            "text": text,
            "createdAt": "2026-07-22T10:00:00.000Z",
        });
        if let Some(tags) = tags {
            record["tags"] = serde_json::json!(tags);
        }
        serde_json::json!({
            "did": "did:plc:alice000000000000000000",
            "time_us": 1_725_911_162_329_308u64,
            "kind": "commit",
            "commit": {
                "rev": "3l3qo2vutsw2b",
                "operation": op,
                "collection": "app.bsky.feed.post",
                "rkey": "3l3qo2vuowo2b",
                "record": record,
                "cid": "bafyreidwaivazkwu67xztlmuobx35hs2lnfh3kolmgfmucldvhd3sgzcqi",
            }
        })
    }

    // --- parsing / accessors ----------------------------------------------

    #[test]
    fn parses_a_like_commit_create() {
        let event = event_from_json(serde_json::json!({
            "did": "did:plc:eygmaihciaxprqvxpfvl6flk",
            "time_us": 1_725_911_162_329_308u64,
            "kind": "commit",
            "commit": {
                "rev": "3l3qo2vutsw2b",
                "operation": "create",
                "collection": "app.bsky.feed.like",
                "rkey": "3l3qo2vuowo2b",
                "record": { "$type": "app.bsky.feed.like" },
                "cid": "bafyreidwaivazkwu67xztlmuobx35hs2lnfh3kolmgfmucldvhd3sgzcqi",
            }
        }));
        assert_eq!(event.did(), "did:plc:eygmaihciaxprqvxpfvl6flk");
        assert_eq!(event.time_us(), 1_725_911_162_329_308);
        assert_eq!(event.kind(), StreamKind::Commit);
        assert_eq!(event.operation(), Some(CommitOp::Create));
        assert_eq!(event.collection(), Some("app.bsky.feed.like"));
        assert_eq!(event.rkey(), Some("3l3qo2vuowo2b"));
        assert!(event.cid().is_some());
        assert!(event.record().is_some());
        assert_eq!(
            event.uri().as_deref(),
            Some("at://did:plc:eygmaihciaxprqvxpfvl6flk/app.bsky.feed.like/3l3qo2vuowo2b"),
        );
        assert!(!event.is_post());
        // A like is not a post, so post decoding yields nothing.
        assert!(event.as_post().is_none());
    }

    #[test]
    fn parses_a_delete_commit_without_record_or_cid() {
        let event = event_from_json(serde_json::json!({
            "did": "did:plc:rfov6bpyztcnedeyyzgfq42k",
            "time_us": 1_725_516_666_833_633u64,
            "kind": "commit",
            "commit": {
                "rev": "3l3f6nzl3cv2s",
                "operation": "delete",
                "collection": "app.bsky.graph.follow",
                "rkey": "3l3dn7tku762u",
            }
        }));
        assert_eq!(event.operation(), Some(CommitOp::Delete));
        assert_eq!(event.cid(), None, "deletes carry no cid");
        assert!(event.record().is_none(), "deletes carry no record");
        // The URI is still constructible from did/collection/rkey.
        assert_eq!(
            event.uri().as_deref(),
            Some("at://did:plc:rfov6bpyztcnedeyyzgfq42k/app.bsky.graph.follow/3l3dn7tku762u"),
        );
        // But without a cid there is no strong ref.
        assert!(event.strong_ref().is_none());
    }

    #[test]
    fn parses_identity_and_account_events() {
        let identity = event_from_json(serde_json::json!({
            "did": "did:plc:ufbl4k27gp6kzas5glhz7fim",
            "time_us": 1_725_516_665_234_703u64,
            "kind": "identity",
            "identity": { "did": "did:plc:ufbl4k27gp6kzas5glhz7fim", "handle": "x.bsky.social" }
        }));
        assert_eq!(identity.kind(), StreamKind::Identity);
        assert_eq!(identity.operation(), None, "identity has no commit op");
        assert!(identity.uri().is_none());

        let account = event_from_json(serde_json::json!({
            "did": "did:plc:ufbl4k27gp6kzas5glhz7fim",
            "time_us": 1_725_516_665_333_808u64,
            "kind": "account",
            "account": { "active": true, "did": "did:plc:ufbl4k27gp6kzas5glhz7fim" }
        }));
        assert_eq!(account.kind(), StreamKind::Account);
    }

    #[test]
    fn post_commit_decodes_text_and_strong_ref() {
        let event = event_from_json(post_commit("create", "hello #rustlang world", None));
        assert!(event.is_post());
        assert_eq!(event.text().as_deref(), Some("hello #rustlang world"));
        let post = event.as_post().expect("record decodes as a post");
        assert_eq!(post.text, "hello #rustlang world");

        let strong = event.strong_ref().expect("post has a strong ref");
        assert_eq!(
            strong.uri,
            "at://did:plc:alice000000000000000000/app.bsky.feed.post/3l3qo2vuowo2b",
        );
    }

    // --- hashtag scanning --------------------------------------------------

    #[test]
    fn scan_hashtags_extracts_tokens_and_ignores_mid_word_hash() {
        let tags = scan_hashtags("Loving #RustLang and #atproto, but not a#b or bare #");
        assert_eq!(tags, vec!["rustlang".to_string(), "atproto".to_string()]);
    }

    #[test]
    fn hashtags_combine_structured_tags_and_text() {
        let event = event_from_json(post_commit(
            "create",
            "a #inline tag",
            Some(vec!["Structured"]),
        ));
        let tags = event.hashtags();
        assert!(tags.contains(&"structured".to_string()), "from tags field");
        assert!(tags.contains(&"inline".to_string()), "from text scan");
    }

    // --- matchers ----------------------------------------------------------

    #[test]
    fn keyword_matcher_is_case_insensitive() {
        let matcher = Matcher::Keyword(vec!["rustlang".to_string()]);
        assert!(matcher.matches(&event_from_json(post_commit(
            "create",
            "I love RUSTLANG!",
            None
        ))));
        assert!(!matcher.matches(&event_from_json(post_commit(
            "create",
            "I love golang",
            None
        ))));
    }

    #[test]
    fn keyword_matcher_ignores_deletes_and_non_posts() {
        let matcher = Matcher::Keyword(vec!["hello".to_string()]);
        // A delete has no record/text, so it cannot match even if it "would".
        let delete = event_from_json(serde_json::json!({
            "did": "did:plc:alice000000000000000000",
            "time_us": 1u64,
            "kind": "commit",
            "commit": {
                "operation": "delete",
                "collection": "app.bsky.feed.post",
                "rkey": "abc",
            }
        }));
        assert!(!matcher.matches(&delete), "keyword must ignore delete ops");

        // A non-post collection is never a keyword match.
        let like = event_from_json(serde_json::json!({
            "did": "did:plc:alice000000000000000000",
            "time_us": 1u64,
            "kind": "commit",
            "commit": {
                "operation": "create",
                "collection": "app.bsky.feed.like",
                "rkey": "abc",
                "record": { "$type": "app.bsky.feed.like" },
                "cid": "bafyreidwaivazkwu67xztlmuobx35hs2lnfh3kolmgfmucldvhd3sgzcqi",
            }
        }));
        assert!(
            !matcher.matches(&like),
            "keyword must ignore non-post collections"
        );
    }

    #[test]
    fn hashtag_matcher_matches_from_text_and_field() {
        let matcher = Matcher::Hashtag(vec!["rustlang".to_string()]);
        assert!(
            matcher.matches(&event_from_json(post_commit(
                "create",
                "hi #RustLang",
                None
            ))),
            "matches a hashtag typed in the text",
        );
        assert!(
            matcher.matches(&event_from_json(post_commit(
                "create",
                "no inline",
                Some(vec!["rustlang"])
            ))),
            "matches a structured tag",
        );
        assert!(
            !matcher.matches(&event_from_json(post_commit("create", "hi #golang", None))),
            "does not match a different hashtag",
        );
    }

    #[test]
    fn firehose_matcher_matches_every_kind() {
        let matcher = Matcher::Firehose;
        assert!(matcher.matches(&event_from_json(post_commit("create", "anything", None))));
        assert!(matcher.matches(&event_from_json(serde_json::json!({
            "did": "did:plc:x0000000000000000000000000",
            "time_us": 1u64,
            "kind": "identity",
            "identity": {}
        }))));
    }

    // --- subscribe URL -----------------------------------------------------

    #[test]
    fn url_includes_collections_dids_cursor_and_size() {
        let mut collections = BTreeSet::new();
        collections.insert("app.bsky.feed.post".to_string());
        collections.insert("app.bsky.graph.follow".to_string());
        let url = build_subscribe_url(
            "wss://example/subscribe",
            &collections,
            &["did:plc:abc".to_string()],
            Some(1_725_519_626_134_432),
            Some(1_000_000),
        );
        // BTreeSet keeps collections in a deterministic (sorted) order.
        assert_eq!(
            url,
            "wss://example/subscribe?wantedCollections=app.bsky.feed.post\
             &wantedCollections=app.bsky.graph.follow\
             &wantedDids=did:plc:abc&cursor=1725519626134432&maxMessageSizeBytes=1000000",
        );
    }

    #[test]
    fn url_without_filters_is_the_bare_endpoint() {
        let url = build_subscribe_url(
            "wss://example/subscribe",
            &BTreeSet::new(),
            &[],
            None,
            Some(0),
        );
        assert_eq!(
            url, "wss://example/subscribe",
            "zero size and no filters => bare URL"
        );
    }

    // --- implied collections ----------------------------------------------

    #[test]
    fn keyword_and_hashtag_handlers_imply_the_post_collection() {
        let mut handlers = StreamHandlers::default();
        handlers.push(
            Matcher::Keyword(vec!["x".to_string()]),
            boxed_stream_handler(|_c, _e| async move { Ok(()) }),
        );
        let implied = handlers.implied_collections();
        assert_eq!(implied.len(), 1);
        assert!(implied.contains("app.bsky.feed.post"));
        assert!(!handlers.has_firehose());
    }

    #[test]
    fn firehose_handler_implies_no_collection_but_is_flagged() {
        let mut handlers = StreamHandlers::default();
        handlers.push(
            Matcher::Firehose,
            boxed_stream_handler(|_c, _e| async move { Ok(()) }),
        );
        assert!(handlers.implied_collections().is_empty());
        assert!(handlers.has_firehose());
    }

    // --- backoff -----------------------------------------------------------

    #[test]
    fn backoff_grows_exponentially_and_caps_at_max() {
        let backoff = Backoff {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            factor: 2.0,
        };
        assert_eq!(backoff.base_delay(0), Duration::from_millis(500));
        assert_eq!(backoff.base_delay(1), Duration::from_secs(1));
        assert_eq!(backoff.base_delay(2), Duration::from_secs(2));
        // 500ms * 2^10 = 512s, capped to 30s.
        assert_eq!(backoff.base_delay(10), Duration::from_secs(30));
    }

    #[test]
    fn jitter_stays_within_half_to_full_of_the_base() {
        let backoff = Backoff::default();
        let base = backoff.base_delay(3);
        for unit in [0.0, 0.25, 0.5, 0.9999] {
            let jittered = backoff.delay_with_jitter(3, unit);
            assert!(
                jittered >= base / 2 && jittered <= base,
                "jittered {jittered:?} must lie in [50%, 100%] of {base:?}",
            );
        }
    }

    #[test]
    fn runner_rewinds_the_cursor_on_resume() {
        let config = JetstreamConfig {
            endpoint: "wss://example/subscribe".to_string(),
            cursor_rewind: Duration::from_secs(3),
            ..Default::default()
        };
        let runner = StreamRunner::new(config, StreamHandlers::default());
        // 3 seconds == 3_000_000 microseconds of rewind.
        let url = runner.build_url(Some(10_000_000));
        assert!(
            url.ends_with("cursor=7000000"),
            "expected rewound cursor, got {url}"
        );
    }

    // --- dispatch ----------------------------------------------------------

    async fn test_context() -> Context {
        let agent = bsky_sdk::BskyAgent::builder()
            .build()
            .await
            .expect("build agent");
        let identity = Arc::new(BotIdentity::new(
            "did:plc:bot00000000000000000000000"
                .parse()
                .expect("valid did"),
            "bot.test".parse().expect("valid handle"),
        ));
        Context::new(agent, identity, WriteBudget::new(None))
    }

    #[tokio::test]
    async fn dispatch_only_runs_matching_handlers() {
        let log = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut handlers = StreamHandlers::default();

        let kw_log = Arc::clone(&log);
        handlers.push(
            Matcher::Keyword(vec!["rust".to_string()]),
            boxed_stream_handler(move |_c, _e| {
                let log = Arc::clone(&kw_log);
                async move {
                    log.lock().unwrap().push("keyword".to_string());
                    Ok(())
                }
            }),
        );
        let fh_log = Arc::clone(&log);
        handlers.push(
            Matcher::Firehose,
            boxed_stream_handler(move |_c, _e| {
                let log = Arc::clone(&fh_log);
                async move {
                    log.lock().unwrap().push("firehose".to_string());
                    Ok(())
                }
            }),
        );

        let ctx = test_context().await;
        // A matching post fires both keyword and firehose.
        handlers
            .dispatch(
                ctx.clone(),
                event_from_json(post_commit("create", "I love rust", None)),
            )
            .await;
        assert_eq!(&*log.lock().unwrap(), &["keyword", "firehose"]);

        // A non-matching post fires only firehose.
        log.lock().unwrap().clear();
        handlers
            .dispatch(
                ctx,
                event_from_json(post_commit("create", "I love go", None)),
            )
            .await;
        assert_eq!(&*log.lock().unwrap(), &["firehose"]);
    }

    #[tokio::test]
    async fn dispatch_routes_errors_and_keeps_going() {
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let ran_second = Arc::new(Mutex::new(false));
        let mut handlers = StreamHandlers::default();

        handlers.push(
            Matcher::Firehose,
            boxed_stream_handler(|_c, _e| async move { Err(Error::invalid_input("boom")) }),
        );
        let ran = Arc::clone(&ran_second);
        handlers.push(
            Matcher::Firehose,
            boxed_stream_handler(move |_c, _e| {
                let ran = Arc::clone(&ran);
                async move {
                    *ran.lock().unwrap() = true;
                    Ok(())
                }
            }),
        );
        let err_seen = Arc::clone(&seen);
        handlers.set_error(boxed_stream_error_handler(move |_c, _e, err| {
            let seen = Arc::clone(&err_seen);
            async move {
                seen.lock().unwrap().push(err.to_string());
            }
        }));

        let ctx = test_context().await;
        handlers
            .dispatch(ctx, event_from_json(post_commit("create", "hi", None)))
            .await;

        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "the error handler fires once"
        );
        assert!(seen.lock().unwrap()[0].contains("boom"));
        assert!(
            *ran_second.lock().unwrap(),
            "a failing handler must not skip the next handler",
        );
    }

    // --- live network (ignored by default) --------------------------------

    /// End-to-end check against the real public Jetstream. Ignored in normal
    /// runs (needs network and is inherently timing-dependent); run explicitly:
    ///
    /// ```bash
    /// cargo test --lib stream::tests::live -- --ignored --nocapture
    /// ```
    ///
    /// Auth is not required to *read* Jetstream, so this drives the runner with
    /// an unauthenticated context — exactly the real ingestion path, minus the
    /// SDK login the public `Bot` builder performs.
    #[tokio::test]
    #[ignore = "hits the live Jetstream network"]
    async fn live_stream_receives_and_matches_real_events() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let ctx = test_context().await;
        let posts = Arc::new(AtomicUsize::new(0));
        let keyword_hits = Arc::new(AtomicUsize::new(0));
        let refs_ok = Arc::new(AtomicUsize::new(0));

        let mut handlers = StreamHandlers::default();

        let posts_c = Arc::clone(&posts);
        let refs_c = Arc::clone(&refs_ok);
        handlers.push(
            Matcher::Firehose,
            boxed_stream_handler(move |_c, event| {
                let posts = Arc::clone(&posts_c);
                let refs_ok = Arc::clone(&refs_c);
                async move {
                    if event.is_post() && event.operation() == Some(CommitOp::Create) {
                        posts.fetch_add(1, Ordering::Relaxed);
                        // A live create must yield a usable strong ref + uri.
                        if event.strong_ref().is_some() && event.uri().is_some() {
                            refs_ok.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(())
                }
            }),
        );
        let kw_c = Arc::clone(&keyword_hits);
        handlers.push(
            Matcher::Keyword(vec!["the".to_string()]),
            boxed_stream_handler(move |_c, _e| {
                let kw = Arc::clone(&kw_c);
                async move {
                    kw.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            }),
        );

        let mut config = JetstreamConfig::default();
        config.collections.insert(POST_COLLECTION.to_string());
        let runner = StreamRunner::new(config, handlers);

        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(runner.run(ctx, rx));
        tokio::time::sleep(Duration::from_secs(8)).await;
        let _ = tx.send(true);
        let _ = handle.await;

        let seen = posts.load(Ordering::Relaxed);
        let hits = keyword_hits.load(Ordering::Relaxed);
        let refs = refs_ok.load(Ordering::Relaxed);
        eprintln!(
            "live jetstream: {seen} post creates, {hits} keyword matches, {refs} strong refs"
        );
        assert!(seen > 0, "expected live post commits from Jetstream");
        assert!(hits > 0, "expected the keyword matcher to match live posts");
        assert!(
            refs > 0,
            "expected strong refs to build from live post commits"
        );
    }
}
