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
use std::time::{Duration, Instant};

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
    create_cost: f64,
    delete_cost: f64,
}

impl WriteBudget {
    /// Build a budget from an optional config. `None` disables rate limiting.
    pub(crate) fn new(config: Option<&RateLimitConfig>) -> Self {
        match config {
            Some(cfg) => Self {
                limiter: Some(Arc::new(RateLimiter::from_config(cfg))),
                create_cost: f64::from(cfg.create_cost),
                delete_cost: f64::from(cfg.delete_cost),
            },
            None => Self {
                limiter: None,
                create_cost: 0.0,
                delete_cost: 0.0,
            },
        }
    }

    /// Wait until a record creation is within budget (no-op if unlimited).
    pub(crate) async fn charge_create(&self) {
        if let Some(limiter) = &self.limiter {
            limiter.acquire(self.create_cost).await;
        }
    }

    /// Wait until a record deletion is within budget (no-op if unlimited).
    pub(crate) async fn charge_delete(&self) {
        if let Some(limiter) = &self.limiter {
            limiter.acquire(self.delete_cost).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
