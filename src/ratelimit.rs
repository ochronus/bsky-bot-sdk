//! A small token-bucket rate limiter used to keep bots within Bluesky's
//! points-based write limits.
//!
//! Bluesky enforces a points-based budget on repository writes: creating a
//! record costs 3 points, updating costs 2, and deleting costs 1, with a rolling
//! budget of 5000 points per hour. The [`RateLimiter`] here models that budget as
//! a classic token bucket so a busy bot slows itself down gracefully instead of
//! being hard-rejected by the server.
//!
//! The limiter is intentionally generic: [`RateLimiter::new`] takes a raw
//! `capacity` and `refill_per_sec`, while [`RateLimiter::from_config`] wires it up
//! to Bluesky's defaults via [`RateLimitConfig`].

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use atrium_api::xrpc::http::{HeaderMap, Request, Response};
use atrium_api::xrpc::{HttpClient, XrpcClient};
use atrium_xrpc_client::reqwest::ReqwestClient;
use tokio::sync::Mutex;

/// Configuration describing Bluesky's points-based write budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitConfig {
    /// Total points replenished per hour (Bluesky default: 5000).
    pub points_per_hour: u32,
    /// Point cost of creating a record (Bluesky default: 3).
    pub create_cost: u32,
    /// Point cost of updating a record (Bluesky default: 2).
    pub update_cost: u32,
    /// Point cost of deleting a record (Bluesky default: 1).
    pub delete_cost: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            points_per_hour: 5000,
            create_cost: 3,
            update_cost: 2,
            delete_cost: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// Server-reported limits (`RateLimit-*` response headers)
// ---------------------------------------------------------------------------

/// A snapshot of Bluesky's rate-limit state as it last told us, via the
/// `RateLimit-Limit` / `RateLimit-Remaining` / `RateLimit-Reset` response
/// headers.
///
/// The client-side [`RateLimiter`] is an *estimate*; this is the server's truth.
/// Read it with [`Context::server_rate_limit`](crate::Context::server_rate_limit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitStatus {
    /// The ceiling for the current window, if the server reported one.
    pub limit: Option<u64>,
    /// Requests remaining in the current window, if reported.
    pub remaining: Option<u64>,
    /// Unix timestamp (seconds) at which the window resets, if reported.
    pub reset_unix: Option<u64>,
}

/// The server's most recently observed rate-limit state, shared (via [`Arc`])
/// between the [`RateLimitClient`] that records it and the [`WriteBudget`] that
/// honors it. Interior-mutable via atomics; `-1` / `0` mean "not yet observed".
#[derive(Debug)]
pub(crate) struct ServerRateLimit {
    limit: AtomicI64,
    remaining: AtomicI64,
    reset: AtomicI64,
}

impl Default for ServerRateLimit {
    fn default() -> Self {
        Self {
            limit: AtomicI64::new(-1),
            remaining: AtomicI64::new(-1),
            reset: AtomicI64::new(0),
        }
    }
}

impl ServerRateLimit {
    /// Record any `RateLimit-*` headers present on a response.
    fn observe(&self, headers: &HeaderMap) {
        if let Some(v) = header_i64(headers, "ratelimit-limit") {
            self.limit.store(v, Ordering::Relaxed);
        }
        if let Some(v) = header_i64(headers, "ratelimit-remaining") {
            self.remaining.store(v, Ordering::Relaxed);
        }
        if let Some(v) = header_i64(headers, "ratelimit-reset") {
            self.reset.store(v, Ordering::Relaxed);
        }
    }

    /// A public snapshot, or `None` if the server has not reported limits yet.
    fn status(&self) -> Option<RateLimitStatus> {
        let limit = self.limit.load(Ordering::Relaxed);
        let remaining = self.remaining.load(Ordering::Relaxed);
        let reset = self.reset.load(Ordering::Relaxed);
        if limit < 0 && remaining < 0 && reset == 0 {
            return None;
        }
        Some(RateLimitStatus {
            limit: u64::try_from(limit).ok(),
            remaining: u64::try_from(remaining).ok(),
            reset_unix: u64::try_from(reset).ok().filter(|&r| r > 0),
        })
    }

    /// If the server last reported *zero* remaining, the [`Duration`] to wait
    /// until its reset (capped at one hour); otherwise `None`.
    ///
    /// Because this is a single last-writer-wins snapshot across all routes, the
    /// exhausted limit may belong to a different route than the imminent call —
    /// so at worst this waits when it needn't, erring toward politeness.
    fn wait_until_reset(&self) -> Option<Duration> {
        if self.remaining.load(Ordering::Relaxed) != 0 {
            return None;
        }
        let reset = self.reset.load(Ordering::Relaxed);
        if reset <= 0 {
            return None;
        }
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
        let secs = reset - now;
        if secs <= 0 {
            return None;
        }
        Some(Duration::from_secs(secs.min(3600) as u64))
    }
}

/// Parse a header value as an `i64`, if present and well-formed.
fn header_i64(headers: &HeaderMap, name: &str) -> Option<i64> {
    headers.get(name)?.to_str().ok()?.trim().parse().ok()
}

/// The transport a [`RateLimitClient`] delegates to: the real reqwest client in
/// production, or an in-process mock in tests (see [`crate::testkit`]).
#[derive(Clone)]
enum Transport {
    Real(ReqwestClient),
    Mock(Arc<crate::testkit::MockTransport>),
}

/// An XRPC client that wraps the reqwest-backed [`ReqwestClient`] and records
/// Bluesky's `RateLimit-*` response headers as a side effect of every response,
/// so a bot can honor — and inspect — the server's real limits.
///
/// This is the client the SDK installs on its agent; you rarely name it directly,
/// but it appears in the type of [`Bot::agent`](crate::Bot::agent). In tests it
/// can instead front an in-process mock ([`crate::testkit::MockBot`]).
#[derive(Clone)]
pub struct RateLimitClient {
    inner: Transport,
    limits: Arc<ServerRateLimit>,
}

impl RateLimitClient {
    pub(crate) fn new(inner: ReqwestClient, limits: Arc<ServerRateLimit>) -> Self {
        Self {
            inner: Transport::Real(inner),
            limits,
        }
    }

    /// Back the client with an in-process mock transport instead of the network.
    pub(crate) fn mock(
        mock: Arc<crate::testkit::MockTransport>,
        limits: Arc<ServerRateLimit>,
    ) -> Self {
        Self {
            inner: Transport::Mock(mock),
            limits,
        }
    }
}

impl HttpClient for RateLimitClient {
    async fn send_http(
        &self,
        request: Request<Vec<u8>>,
    ) -> core::result::Result<Response<Vec<u8>>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        let response = match &self.inner {
            Transport::Real(client) => client.send_http(request).await?,
            Transport::Mock(mock) => mock.respond(request),
        };
        self.limits.observe(response.headers());
        Ok(response)
    }
}

impl XrpcClient for RateLimitClient {
    fn base_uri(&self) -> String {
        match &self.inner {
            Transport::Real(client) => client.base_uri(),
            Transport::Mock(_) => "http://mock.invalid".to_string(),
        }
    }
}

/// The mutable state of a token bucket. Kept separate from [`RateLimiter`] so its
/// arithmetic can be unit-tested without any async runtime or real clock.
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
    last: Instant,
}

impl Bucket {
    /// Add tokens accrued since `last`, saturating at `capacity`.
    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            self.last = now;
        }
    }

    /// Try to remove `cost` tokens. On success returns `Ok(())`; otherwise returns
    /// `Err(wait_secs)` — the number of seconds until enough tokens accrue.
    fn try_take(&mut self, cost: f64) -> core::result::Result<(), f64> {
        if self.tokens >= cost {
            self.tokens -= cost;
            Ok(())
        } else if self.refill_per_sec > 0.0 {
            Err((cost - self.tokens) / self.refill_per_sec)
        } else {
            Err(f64::INFINITY)
        }
    }
}

/// A token-bucket rate limiter shared across all writes performed by a bot.
#[derive(Debug)]
pub struct RateLimiter {
    bucket: Mutex<Bucket>,
}

impl RateLimiter {
    /// Create a limiter with an explicit `capacity` (burst size, in points) and
    /// `refill_per_sec` (points regenerated per second). The bucket starts full.
    pub fn new(capacity: f64, refill_per_sec: f64) -> Self {
        Self {
            bucket: Mutex::new(Bucket {
                tokens: capacity,
                capacity,
                refill_per_sec,
                last: Instant::now(),
            }),
        }
    }

    /// Build a limiter from a [`RateLimitConfig`], using its hourly budget as the
    /// bucket capacity and spreading refill evenly across the hour.
    pub fn from_config(cfg: &RateLimitConfig) -> Self {
        let capacity = cfg.points_per_hour.max(1) as f64;
        Self::new(capacity, capacity / 3600.0)
    }

    /// Acquire `cost` points, sleeping (asynchronously) until they are available.
    ///
    /// This never rejects; it simply back-pressures the caller until the budget
    /// allows the write.
    pub async fn acquire(&self, cost: f64) {
        loop {
            let wait = {
                let mut bucket = self.bucket.lock().await;
                bucket.refill(Instant::now());
                match bucket.try_take(cost) {
                    Ok(()) => return,
                    Err(wait) => wait,
                }
            };
            // Clamp so a pathological config can't sleep forever in one shot; we
            // re-check on the next loop iteration regardless.
            let wait = wait.clamp(0.01, 3600.0);
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
        }
    }
}

/// A bot's shared write budget: an optional [`RateLimiter`] paired with the point
/// costs of the operations charged against it.
///
/// Keeping the limiter and its per-operation costs together (rather than as loose
/// values threaded through [`Context`](crate::Context)) makes "no limiter means no
/// limiting" a single, testable place. Cheap to clone — the limiter is shared via
/// [`Arc`].
#[derive(Clone)]
pub(crate) struct WriteBudget {
    limiter: Option<Arc<RateLimiter>>,
    /// The server's reported limits, honored before each write when present.
    server: Option<Arc<ServerRateLimit>>,
    create_cost: f64,
    delete_cost: f64,
}

impl WriteBudget {
    /// Build a budget from an optional config. `None` disables rate limiting.
    pub(crate) fn new(config: Option<&RateLimitConfig>) -> Self {
        match config {
            Some(cfg) => Self {
                limiter: Some(Arc::new(RateLimiter::from_config(cfg))),
                server: None,
                create_cost: f64::from(cfg.create_cost),
                delete_cost: f64::from(cfg.delete_cost),
            },
            None => Self {
                limiter: None,
                server: None,
                create_cost: 0.0,
                delete_cost: 0.0,
            },
        }
    }

    /// Attach the shared server-rate-limit snapshot, so writes wait when the
    /// server says the current window is exhausted.
    pub(crate) fn with_server(mut self, server: Arc<ServerRateLimit>) -> Self {
        self.server = Some(server);
        self
    }

    /// The server's last-reported rate-limit status, if any.
    pub(crate) fn server_status(&self) -> Option<RateLimitStatus> {
        self.server.as_ref().and_then(|s| s.status())
    }

    /// If the server reported the window exhausted, wait until it resets. This
    /// pre-empts a 429 rather than absorbing one.
    async fn await_server_reset(&self) {
        if let Some(server) = &self.server
            && let Some(wait) = server.wait_until_reset()
        {
            tracing::warn!(
                secs = wait.as_secs(),
                "server rate limit exhausted; waiting until it resets"
            );
            tokio::time::sleep(wait).await;
        }
    }

    /// Wait until a record creation is within budget (no-op if unlimited).
    pub(crate) async fn charge_create(&self) {
        self.await_server_reset().await;
        if let Some(limiter) = &self.limiter {
            limiter.acquire(self.create_cost).await;
        }
    }

    /// Wait until a record deletion is within budget (no-op if unlimited).
    pub(crate) async fn charge_delete(&self) {
        self.await_server_reset().await;
        if let Some(limiter) = &self.limiter {
            limiter.acquire(self.delete_cost).await;
        }
    }
}

/// Build a session-less `BskyAgent<RateLimitClient>` for tests. No network I/O
/// happens without a session, so this is offline. Shared by the crate's other
/// test modules, which need a context backed by the same client type production
/// uses.
#[cfg(test)]
pub(crate) async fn test_agent() -> bsky_sdk::BskyAgent<RateLimitClient> {
    let limits = Arc::new(ServerRateLimit::default());
    bsky_sdk::BskyAgent::builder()
        .client(RateLimitClient::new(
            ReqwestClient::new("https://bsky.social"),
            limits,
        ))
        .build()
        .await
        .expect("build test agent")
}

#[cfg(test)]
mod tests {
    use super::*;
    use atrium_api::xrpc::http::{HeaderName, HeaderValue};

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                HeaderName::from_static(name),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn observe_parses_ratelimit_headers() {
        let server = ServerRateLimit::default();
        assert_eq!(server.status(), None, "nothing observed yet");

        server.observe(&headers(&[
            ("ratelimit-limit", "3000"),
            ("ratelimit-remaining", "2999"),
            ("ratelimit-reset", "1700000000"),
        ]));
        let status = server.status().expect("observed");
        assert_eq!(status.limit, Some(3000));
        assert_eq!(status.remaining, Some(2999));
        assert_eq!(status.reset_unix, Some(1_700_000_000));
    }

    #[test]
    fn observe_ignores_absent_or_malformed_headers() {
        let server = ServerRateLimit::default();
        // Only remaining present; a garbage reset must be ignored, not zero it.
        server.observe(&headers(&[
            ("ratelimit-remaining", "10"),
            ("ratelimit-reset", "not-a-number"),
        ]));
        let status = server.status().expect("remaining was observed");
        assert_eq!(status.remaining, Some(10));
        assert_eq!(status.limit, None, "limit was never sent");
        assert_eq!(status.reset_unix, None, "malformed reset must be ignored");
    }

    #[test]
    fn wait_until_reset_only_when_exhausted_with_a_future_reset() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Exhausted, reset 100s out → wait ~100s.
        let exhausted = ServerRateLimit::default();
        exhausted.observe(&headers(&[
            ("ratelimit-remaining", "0"),
            ("ratelimit-reset", &(now + 100).to_string()),
        ]));
        let wait = exhausted
            .wait_until_reset()
            .expect("should wait when exhausted");
        assert!(
            wait.as_secs() >= 95 && wait.as_secs() <= 100,
            "expected ~100s, got {wait:?}"
        );

        // Not exhausted → never wait.
        let ok = ServerRateLimit::default();
        ok.observe(&headers(&[("ratelimit-remaining", "5")]));
        assert!(ok.wait_until_reset().is_none(), "remaining>0 must not wait");

        // Exhausted but reset already passed → don't wait.
        let stale = ServerRateLimit::default();
        stale.observe(&headers(&[
            ("ratelimit-remaining", "0"),
            ("ratelimit-reset", &(now - 10).to_string()),
        ]));
        assert!(
            stale.wait_until_reset().is_none(),
            "a past reset must not wait"
        );
    }

    #[test]
    fn wait_until_reset_is_capped_at_one_hour() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let server = ServerRateLimit::default();
        server.observe(&headers(&[
            ("ratelimit-remaining", "0"),
            ("ratelimit-reset", &(now + 100_000).to_string()),
        ]));
        assert_eq!(
            server.wait_until_reset().map(|d| d.as_secs()),
            Some(3600),
            "a far-future reset must be capped at one hour"
        );
    }

    #[test]
    fn bucket_refills_linearly_and_caps_at_capacity() {
        let start = Instant::now();
        let mut bucket = Bucket {
            tokens: 0.0,
            capacity: 10.0,
            refill_per_sec: 1.0,
            last: start,
        };

        bucket.refill(start + Duration::from_secs(5));
        assert!(
            (bucket.tokens - 5.0).abs() < 1e-9,
            "5s at 1/s should yield 5 tokens"
        );

        // Refilling far into the future must not exceed capacity.
        bucket.refill(start + Duration::from_secs(1_000));
        assert!(
            (bucket.tokens - 10.0).abs() < 1e-9,
            "tokens must saturate at capacity"
        );
    }

    #[test]
    fn try_take_succeeds_when_enough_tokens() {
        let mut bucket = Bucket {
            tokens: 3.0,
            capacity: 10.0,
            refill_per_sec: 1.0,
            last: Instant::now(),
        };
        assert!(bucket.try_take(3.0).is_ok());
        assert!((bucket.tokens - 0.0).abs() < 1e-9);
    }

    #[test]
    fn try_take_reports_wait_when_short() {
        let mut bucket = Bucket {
            tokens: 1.0,
            capacity: 10.0,
            refill_per_sec: 2.0,
            last: Instant::now(),
        };
        // Need 5, have 1 → short by 4 points, refilling at 2/s → 2 seconds.
        let wait = bucket
            .try_take(5.0)
            .expect_err("should not have enough tokens");
        assert!((wait - 2.0).abs() < 1e-9, "expected 2s wait, got {wait}");
        // A failed take must not consume any tokens.
        assert!((bucket.tokens - 1.0).abs() < 1e-9);
    }

    #[test]
    fn zero_refill_reports_infinite_wait_when_empty() {
        let mut bucket = Bucket {
            tokens: 0.0,
            capacity: 10.0,
            refill_per_sec: 0.0,
            last: Instant::now(),
        };
        let wait = bucket
            .try_take(1.0)
            .expect_err("empty bucket cannot satisfy take");
        assert!(wait.is_infinite());
    }

    #[test]
    fn from_config_uses_hourly_budget_as_capacity() {
        let cfg = RateLimitConfig::default();
        let limiter = RateLimiter::from_config(&cfg);
        let bucket = limiter.bucket.blocking_lock();
        assert!((bucket.capacity - 5000.0).abs() < 1e-9);
        assert!((bucket.refill_per_sec - 5000.0 / 3600.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn unlimited_budget_never_blocks() {
        let budget = WriteBudget::new(None);
        // With no limiter, charging is a no-op and must return immediately even
        // when called far more often than any real budget would allow.
        for _ in 0..10_000 {
            budget.charge_create().await;
            budget.charge_delete().await;
        }
    }

    #[tokio::test]
    async fn configured_budget_allows_initial_writes_immediately() {
        let budget = WriteBudget::new(Some(&RateLimitConfig::default()));
        // The bucket starts full, so the first write must not block.
        budget.charge_create().await;
    }
}
