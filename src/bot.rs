//! The [`Bot`] runtime and its [`BotBuilder`].

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use atrium_api::app::bsky::notification::{list_notifications, update_seen};
use atrium_api::types::LimitedNonZeroU8;
use atrium_api::types::string::Datetime;
use bsky_sdk::BskyAgent;
use bsky_sdk::agent::config::{Config, FileStore};

use crate::config::BotConfig;
use crate::context::{BotIdentity, Context};
use crate::dedup::Dedup;
use crate::error::{Error, Result};
use crate::event::{Notification, NotificationReason};
use crate::handler::{Handlers, boxed_error_handler, boxed_handler};
use crate::ratelimit::{RateLimitConfig, WriteBudget};

/// Generate the `on_<reason>` convenience builders. They all share the same
/// handler bounds and simply forward to [`BotBuilder::on`], so expressing them
/// once keeps the six of them from drifting apart.
macro_rules! reason_handlers {
    ($($(#[$doc:meta])* $method:ident => $reason:expr;)+) => {
        $(
            $(#[$doc])*
            pub fn $method<F, Fut>(self, handler: F) -> Self
            where
                F: Fn(Context, Notification) -> Fut + Send + Sync + 'static,
                Fut: Future<Output = Result<()>> + Send + 'static,
            {
                self.on($reason, handler)
            }
        )+
    };
}

/// Builder for a [`Bot`]: configure credentials, polling behaviour, and handlers,
/// then [`build`](BotBuilder::build) to authenticate and obtain a runnable bot.
///
/// ```no_run
/// use bsky_bot_sdk::Bot;
///
/// # async fn demo() -> bsky_bot_sdk::Result<()> {
/// let bot = Bot::builder()
///     .credentials("mybot.bsky.social", "app-password")
///     .session_file("session.json")
///     .on_mention(|ctx, notif| async move {
///         ctx.reply_to(&notif, "👋 hello!").await?;
///         Ok(())
///     })
///     .build()
///     .await?;
/// bot.run().await
/// # }
/// ```
#[derive(Default)]
#[must_use = "a BotBuilder does nothing until `.build()` is awaited"]
pub struct BotBuilder {
    identifier: Option<String>,
    password: Option<String>,
    config: BotConfig,
    handlers: Handlers,
}

impl BotBuilder {
    /// Start a new builder with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    // --- credentials -------------------------------------------------------

    /// Set the account identifier (handle, DID, or email).
    pub fn identifier(mut self, identifier: impl Into<String>) -> Self {
        self.identifier = Some(identifier.into());
        self
    }

    /// Set the app password.
    ///
    /// Always use an app password, never your main account password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set both identifier and password at once.
    pub fn credentials(self, identifier: impl Into<String>, password: impl Into<String>) -> Self {
        self.identifier(identifier).password(password)
    }

    /// Read credentials from the environment: `BSKY_IDENTIFIER` and
    /// `BSKY_APP_PASSWORD` (falling back to `BSKY_PASSWORD`).
    pub fn from_env(mut self) -> Result<Self> {
        let identifier = std::env::var("BSKY_IDENTIFIER").map_err(|_| Error::MissingCredentials)?;
        let password = std::env::var("BSKY_APP_PASSWORD")
            .or_else(|_| std::env::var("BSKY_PASSWORD"))
            .map_err(|_| Error::MissingCredentials)?;
        self.identifier = Some(identifier);
        self.password = Some(password);
        Ok(self)
    }

    // --- configuration -----------------------------------------------------

    /// Override the XRPC service endpoint (default `https://bsky.social`).
    pub fn service(mut self, service: impl Into<String>) -> Self {
        self.config.service = service.into();
        self
    }

    /// Set the interval between notification polls (default 15s).
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.config.poll_interval = interval;
        self
    }

    /// Set how many notifications to fetch per poll (clamped to `1..=100`).
    pub fn notification_limit(mut self, limit: u8) -> Self {
        self.config.notification_limit = limit;
        self
    }

    /// Restrict polling to a set of reasons. By default all reasons are fetched.
    pub fn reasons(mut self, reasons: impl IntoIterator<Item = NotificationReason>) -> Self {
        self.config.reasons = Some(reasons.into_iter().collect());
        self
    }

    /// Process the backlog of notifications that existed before startup.
    ///
    /// Off by default, so a restarting bot does not reply to old mentions again.
    pub fn process_backlog(mut self, process: bool) -> Self {
        self.config.process_backlog = process;
        self
    }

    /// Whether to mark notifications seen after processing (default `true`).
    pub fn mark_seen(mut self, mark: bool) -> Self {
        self.config.mark_seen = mark;
        self
    }

    /// Persist and resume the login session at `path` (JSON).
    pub fn session_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.session_path = Some(path.into());
        self
    }

    /// Configure client-side write rate limiting. Pass `None` to disable.
    pub fn rate_limit(mut self, config: Option<RateLimitConfig>) -> Self {
        self.config.rate_limit = config;
        self
    }

    /// Replace the entire [`BotConfig`] at once.
    pub fn config(mut self, config: BotConfig) -> Self {
        self.config = config;
        self
    }

    // --- handlers ----------------------------------------------------------

    /// Register a handler for a specific notification reason.
    pub fn on<F, Fut>(mut self, reason: NotificationReason, handler: F) -> Self
    where
        F: Fn(Context, Notification) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.handlers.register(reason, boxed_handler(handler));
        self
    }

    /// Register a catch-all handler invoked for every notification (after any
    /// reason-specific handlers).
    pub fn on_any<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Context, Notification) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.handlers.register_any(boxed_handler(handler));
        self
    }

    reason_handlers! {
        /// Register a handler for mentions.
        on_mention => NotificationReason::Mention;
        /// Register a handler for replies.
        on_reply => NotificationReason::Reply;
        /// Register a handler for follows (e.g. to follow back).
        on_follow => NotificationReason::Follow;
        /// Register a handler for likes.
        on_like => NotificationReason::Like;
        /// Register a handler for reposts.
        on_repost => NotificationReason::Repost;
        /// Register a handler for quote posts.
        on_quote => NotificationReason::Quote;
    }

    /// Register an error handler invoked whenever a handler returns `Err`.
    ///
    /// Without one, handler errors are logged via `tracing` at error level.
    pub fn on_error<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Context, Notification, Error) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.handlers.set_error(boxed_error_handler(handler));
        self
    }

    // --- build -------------------------------------------------------------

    /// Authenticate (resuming a saved session if possible, otherwise logging in)
    /// and produce a runnable [`Bot`].
    pub async fn build(self) -> Result<Bot> {
        // 1. Load a persisted session/config if one exists.
        let persisted = match &self.config.session_path {
            Some(path) if path.exists() => Config::load(&FileStore::new(path)).await.ok(),
            _ => None,
        };
        let endpoint = persisted
            .as_ref()
            .map(|c| c.endpoint.clone())
            .unwrap_or_else(|| self.config.service.clone());

        // 2. Build the agent aimed at the resolved endpoint (no session yet, so a
        //    stale saved session can't fail the whole build).
        let agent = BskyAgent::builder()
            .config(Config {
                endpoint,
                ..Default::default()
            })
            .build()
            .await?;

        // 3. Authenticate: try to resume, else log in with credentials.
        let mut authenticated = false;
        if let Some(session) = persisted.and_then(|c| c.session) {
            if agent.resume_session(session).await.is_ok() {
                authenticated = true;
                tracing::debug!("resumed persisted session");
            } else {
                tracing::info!("persisted session could not be resumed; logging in fresh");
            }
        }
        if !authenticated {
            let identifier = self.identifier.as_ref().ok_or(Error::MissingCredentials)?;
            let password = self.password.as_ref().ok_or(Error::MissingCredentials)?;
            agent.login(identifier, password).await?;
        }

        // 4. Persist the (possibly refreshed) session. Best-effort.
        if let Some(path) = &self.config.session_path {
            if let Err(err) = agent.to_config().await.save(&FileStore::new(path)).await {
                tracing::warn!(error = %err, "failed to persist session file");
            }
        }

        // 5. Resolve the bot's own identity.
        let session = agent.get_session().await.ok_or(Error::NotAuthenticated)?;
        let identity = Arc::new(BotIdentity::new(
            session.data.did.clone(),
            session.data.handle.clone(),
        ));

        // 6. Assemble the write budget (rate limiter + per-operation costs).
        let budget = WriteBudget::new(self.config.rate_limit.as_ref());

        let context = Context::new(agent, identity, budget);
        Ok(Bot {
            context,
            config: self.config,
            handlers: self.handlers,
        })
    }
}

/// A configured, authenticated bot ready to poll for notifications and dispatch
/// them to handlers.
pub struct Bot {
    context: Context,
    config: BotConfig,
    handlers: Handlers,
}

impl Bot {
    /// Create a [`BotBuilder`].
    pub fn builder() -> BotBuilder {
        BotBuilder::new()
    }

    /// The shared [`Context`] (also handed to every handler).
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// The authenticated agent.
    pub fn agent(&self) -> &BskyAgent {
        self.context.agent()
    }

    /// The bot's own identity (DID + handle).
    pub fn identity(&self) -> &BotIdentity {
        self.context.me()
    }

    /// The effective configuration.
    pub fn config(&self) -> &BotConfig {
        &self.config
    }

    /// Run until `Ctrl-C` (SIGINT) is received.
    pub async fn run(self) -> Result<()> {
        self.run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }

    /// Run until the provided `shutdown` future resolves.
    ///
    /// Returns [`Error::NoHandlers`] immediately if no handlers were registered.
    pub async fn run_until<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        if self.handlers.is_empty() {
            return Err(Error::NoHandlers);
        }

        let mut dedup = Dedup::new();

        // Unless asked to drain it, skip whatever backlog exists at startup.
        if !self.config.process_backlog {
            match self.fetch().await {
                Ok(notifs) => {
                    tracing::info!(
                        count = notifs.len(),
                        "priming watermark; skipping existing backlog"
                    );
                    dedup.prime(&notifs);
                }
                Err(err) => {
                    tracing::warn!(error = %err, "initial fetch failed; backlog not primed");
                }
            }
        }

        tokio::pin!(shutdown);
        let mut ticker = tokio::time::interval(self.config.poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        tracing::info!(
            handle = %self.context.handle(),
            did = %self.context.did(),
            interval_secs = self.config.poll_interval.as_secs(),
            "bot started",
        );

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("shutdown signal received; stopping");
                    break;
                }
                _ = ticker.tick() => {
                    match self.poll_and_dispatch(&mut dedup).await {
                        Ok(0) => {}
                        Ok(n) => tracing::debug!(processed = n, "handled notifications"),
                        Err(err) => tracing::error!(error = %err, "poll cycle failed"),
                    }
                }
            }
        }

        Ok(())
    }

    /// Perform one poll cycle against the given [`Dedup`]: fetch notifications,
    /// dispatch the fresh ones oldest-first, and (optionally) mark them seen.
    ///
    /// Returns the number of notifications dispatched. Exposed for callers that
    /// want to drive the loop themselves rather than using [`run`](Bot::run).
    pub async fn poll_and_dispatch(&self, dedup: &mut Dedup) -> Result<usize> {
        let notifs = self.fetch().await?;
        let fresh = dedup.take_new_sorted(notifs);
        let count = fresh.len();

        for notif in fresh {
            self.handlers.dispatch(self.context.clone(), notif).await;
        }

        if count > 0 && self.config.mark_seen {
            if let Err(err) = self.mark_seen().await {
                tracing::warn!(error = %err, "failed to mark notifications seen");
            }
        }

        Ok(count)
    }

    /// Fetch the current page of notifications, applying any reason filter.
    async fn fetch(&self) -> Result<Vec<Notification>> {
        let limit = LimitedNonZeroU8::<100>::try_from(self.config.clamped_limit()).ok();
        let reasons = self.config.reasons.as_ref().map(|rs| {
            rs.iter()
                .map(|r| r.as_reason().to_string())
                .collect::<Vec<_>>()
        });

        let params = list_notifications::ParametersData {
            cursor: None,
            limit,
            priority: None,
            reasons,
            seen_at: None,
        };
        let output = self
            .context
            .agent()
            .api
            .app
            .bsky
            .notification
            .list_notifications(params.into())
            .await?;

        Ok(output
            .data
            .notifications
            .into_iter()
            .map(Notification::new)
            .collect())
    }

    /// Mark notifications seen as of now.
    async fn mark_seen(&self) -> Result<()> {
        let input = update_seen::InputData {
            seen_at: Datetime::now(),
        };
        self.context
            .agent()
            .api
            .app
            .bsky
            .notification
            .update_seen(input.into())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_without_credentials_or_session_fails_with_missing_credentials() {
        let result = Bot::builder()
            .on_mention(|_ctx, _n| async move { Ok(()) })
            .build()
            .await;
        assert!(matches!(result, Err(Error::MissingCredentials)));
    }

    #[test]
    fn builder_collects_config_and_handlers() {
        let builder = Bot::builder()
            .service("https://example.com")
            .poll_interval(Duration::from_secs(30))
            .notification_limit(10)
            .reasons([NotificationReason::Mention, NotificationReason::Follow])
            .process_backlog(true)
            .mark_seen(false)
            .on_mention(|_c, _n| async move { Ok(()) })
            .on_follow(|_c, _n| async move { Ok(()) });

        assert_eq!(builder.config.service, "https://example.com");
        assert_eq!(builder.config.poll_interval, Duration::from_secs(30));
        assert_eq!(builder.config.notification_limit, 10);
        assert!(builder.config.process_backlog);
        assert!(!builder.config.mark_seen);
        assert!(!builder.handlers.is_empty());
        assert_eq!(
            builder.config.reasons.as_ref().map(Vec::len),
            Some(2),
            "both reason filters should be recorded",
        );
    }

    #[tokio::test]
    #[allow(unsafe_code)] // edition 2024 marks env mutation unsafe; fine in a test
    async fn from_env_errors_when_unset() {
        // Ensure the vars are absent for this check.
        unsafe {
            std::env::remove_var("BSKY_IDENTIFIER");
            std::env::remove_var("BSKY_APP_PASSWORD");
            std::env::remove_var("BSKY_PASSWORD");
        }
        let result = Bot::builder().from_env();
        assert!(matches!(result, Err(Error::MissingCredentials)));
    }
}
