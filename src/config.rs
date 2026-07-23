//! Runtime configuration for a [`Bot`](crate::Bot).

use std::path::PathBuf;
use std::time::Duration;

use crate::event::NotificationReason;
use crate::ratelimit::RateLimitConfig;
use crate::retry::RetryPolicy;

/// The default Bluesky PDS entryway.
pub const DEFAULT_SERVICE: &str = "https://bsky.social";

/// Configuration controlling how a bot authenticates and polls for work.
///
/// Construct with [`BotConfig::default`] and tweak fields, or drive it through
/// [`BotBuilder`](crate::BotBuilder), which writes into a `BotConfig` under the
/// hood.
#[derive(Debug, Clone)]
pub struct BotConfig {
    /// XRPC service endpoint (PDS/entryway). Defaults to [`DEFAULT_SERVICE`].
    pub service: String,
    /// How long to wait between notification polls.
    pub poll_interval: Duration,
    /// How many notifications to request per poll (clamped to `1..=100`).
    pub notification_limit: u8,
    /// If set, only fetch notifications with these reasons. `None` fetches all.
    pub reasons: Option<Vec<NotificationReason>>,
    /// Whether to process notifications that already existed when the bot started.
    ///
    /// Defaults to `false` so a freshly-started bot does not reply to a backlog of
    /// old mentions. Set to `true` to drain the backlog on startup.
    pub process_backlog: bool,
    /// Whether to call `updateSeen` after processing a batch, marking those
    /// notifications read on the server.
    pub mark_seen: bool,
    /// Optional path used to persist and resume the login session, avoiding a
    /// fresh `createSession` on every start.
    pub session_path: Option<PathBuf>,
    /// Optional client-side write rate limiting. `Some(_)` (the default) keeps the
    /// bot within Bluesky's points budget; `None` disables limiting entirely.
    pub rate_limit: Option<RateLimitConfig>,
    /// How transient failures (network blips, 5xx, throttling) on idempotent
    /// reads and the poll loops are retried. Defaults to a few quick tries so a
    /// blip is ridden out within a poll interval. Record *writes* are never
    /// auto-retried (that could double-post).
    pub retry: RetryPolicy,
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            service: DEFAULT_SERVICE.to_string(),
            poll_interval: Duration::from_secs(15),
            notification_limit: 50,
            reasons: None,
            process_backlog: false,
            mark_seen: true,
            session_path: None,
            rate_limit: Some(RateLimitConfig::default()),
            retry: RetryPolicy::default(),
        }
    }
}

impl BotConfig {
    /// The notification limit, clamped to the API-valid `1..=100` range.
    pub(crate) fn clamped_limit(&self) -> u8 {
        self.notification_limit.clamp(1, 100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane_for_a_polite_bot() {
        let cfg = BotConfig::default();
        assert_eq!(cfg.service, DEFAULT_SERVICE);
        assert_eq!(cfg.poll_interval, Duration::from_secs(15));
        assert!(
            !cfg.process_backlog,
            "bots must not spam an old backlog by default"
        );
        assert!(cfg.mark_seen);
        assert!(
            cfg.rate_limit.is_some(),
            "rate limiting on by default keeps bots within budget"
        );
        assert_eq!(
            cfg.retry.max_retries, 3,
            "transient reads should retry a few times by default"
        );
    }

    #[test]
    fn limit_is_clamped_to_api_bounds() {
        let mut cfg = BotConfig {
            notification_limit: 0,
            ..Default::default()
        };
        assert_eq!(cfg.clamped_limit(), 1, "0 is below the API minimum of 1");
        cfg.notification_limit = 200;
        assert_eq!(
            cfg.clamped_limit(),
            100,
            "200 is above the API maximum of 100"
        );
        cfg.notification_limit = 42;
        assert_eq!(cfg.clamped_limit(), 42);
    }
}
