//! The [`Bot`] runtime and its [`BotBuilder`].

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
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
use crate::dm::{
    DirectMessage, DmConfig, DmHandlers, DmRunner, boxed_dm_error_handler, boxed_dm_handler,
};
use crate::error::{Error, Result};
use crate::event::{Notification, NotificationReason};
use crate::handler::{Handlers, boxed_error_handler, boxed_handler};
use crate::ratelimit::{RateLimitConfig, WriteBudget};
use crate::schedule::{Schedule, Scheduler, boxed_task};
use crate::stream::{
    JetstreamConfig, Matcher, StreamEvent, StreamHandlers, StreamRunner,
    boxed_stream_error_handler, boxed_stream_handler,
};

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
    scheduler: Scheduler,
    stream_config: JetstreamConfig,
    stream_handlers: StreamHandlers,
    dm_config: DmConfig,
    dm_handlers: DmHandlers,
    /// The first error from a fallible scheduling call (e.g. a bad cron
    /// expression), surfaced from [`build`](BotBuilder::build) so the builder
    /// chain stays fluent.
    schedule_error: Option<Error>,
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

    // --- scheduling --------------------------------------------------------

    /// Run `task` every `interval`, starting one `interval` after the bot starts.
    ///
    /// The task receives the shared [`Context`], so it can post, reply, or reach
    /// the raw agent just like a notification handler. Register as many as you
    /// like; they run concurrently with the notification loop.
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use bsky_bot_sdk::Bot;
    /// # fn demo(b: bsky_bot_sdk::BotBuilder) -> bsky_bot_sdk::BotBuilder {
    /// b.every(Duration::from_secs(3600), |ctx| async move {
    ///     ctx.post("hourly heartbeat").await?;
    ///     Ok(())
    /// })
    /// # }
    /// ```
    pub fn every<F, Fut>(mut self, interval: Duration, task: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.scheduler
            .push(Schedule::every(interval), boxed_task(task));
        self
    }

    /// Run `task` on a cron schedule evaluated in **UTC**.
    ///
    /// Accepts 5-field (`min hour dom mon dow`) and 6-field
    /// (`sec min hour dom mon dow`) expressions plus `@daily`-style macros. An
    /// invalid expression is remembered and returned from
    /// [`build`](BotBuilder::build), keeping the builder chain fluent.
    ///
    /// ```
    /// # use bsky_bot_sdk::Bot;
    /// # fn demo(b: bsky_bot_sdk::BotBuilder) -> bsky_bot_sdk::BotBuilder {
    /// b.cron("0 12 * * *", |ctx| async move {  // 12:00 UTC every day
    ///     ctx.post("daily digest").await?;
    ///     Ok(())
    /// })
    /// # }
    /// ```
    pub fn cron<F, Fut>(mut self, expr: &str, task: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        match Schedule::cron(expr) {
            Ok(schedule) => self.scheduler.push(schedule, boxed_task(task)),
            Err(err) => self.record_schedule_error(err),
        }
        self
    }

    /// Like [`cron`](Self::cron), but the expression is evaluated in the host's
    /// local timezone instead of UTC.
    pub fn cron_local<F, Fut>(mut self, expr: &str, task: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        match Schedule::cron_local(expr) {
            Ok(schedule) => self.scheduler.push(schedule, boxed_task(task)),
            Err(err) => self.record_schedule_error(err),
        }
        self
    }

    /// Run `task` on a pre-built [`Schedule`] (for a custom [`Tz`](crate::Tz) or a
    /// schedule parsed from a string).
    ///
    /// ```
    /// # use bsky_bot_sdk::{Bot, Schedule};
    /// # fn demo(b: bsky_bot_sdk::BotBuilder) -> bsky_bot_sdk::Result<bsky_bot_sdk::BotBuilder> {
    /// let schedule: Schedule = "@every 15m".parse()?;
    /// Ok(b.schedule(schedule, |ctx| async move {
    ///     ctx.post("every 15 minutes").await?;
    ///     Ok(())
    /// }))
    /// # }
    /// ```
    pub fn schedule<F, Fut>(mut self, schedule: Schedule, task: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.scheduler.push(schedule, boxed_task(task));
        self
    }

    /// Remember the first scheduling error so it can be surfaced from `build`.
    fn record_schedule_error(&mut self, err: Error) {
        if self.schedule_error.is_none() {
            self.schedule_error = Some(err);
        }
    }

    // --- real-time stream (Jetstream) --------------------------------------

    /// React to *every* event on the network via the [Jetstream] firehose.
    ///
    /// The handler receives a [`StreamEvent`] for each commit (and identity /
    /// account event) in the collections the stream is subscribed to. With no
    /// [`jetstream_collections`](Self::jetstream_collections) filter this is the
    /// entire network — very high volume — so most bots should scope it.
    ///
    /// Runs concurrently with the notification loop and any schedules; a bot may
    /// run with *only* stream handlers.
    ///
    /// [Jetstream]: https://docs.bsky.app/blog/jetstream
    pub fn on_firehose<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Context, StreamEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.stream_handlers
            .push(Matcher::Firehose, boxed_stream_handler(handler));
        self
    }

    /// React to network posts whose text contains `keyword` (case-insensitive).
    ///
    /// Implicitly subscribes the stream to `app.bsky.feed.post`.
    ///
    /// ```
    /// # use bsky_bot_sdk::Bot;
    /// # fn demo(b: bsky_bot_sdk::BotBuilder) -> bsky_bot_sdk::BotBuilder {
    /// b.on_keyword("rustlang", |ctx, event| async move {
    ///     if let Some(subject) = event.strong_ref() {
    ///         ctx.like_ref(subject).await?;
    ///     }
    ///     Ok(())
    /// })
    /// # }
    /// ```
    pub fn on_keyword<F, Fut>(self, keyword: impl Into<String>, handler: F) -> Self
    where
        F: Fn(Context, StreamEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_keywords([keyword.into()], handler)
    }

    /// React to network posts whose text contains *any* of `keywords`
    /// (case-insensitive). Implicitly subscribes to `app.bsky.feed.post`.
    pub fn on_keywords<F, Fut, I, S>(mut self, keywords: I, handler: F) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
        F: Fn(Context, StreamEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let keywords: Vec<String> = keywords
            .into_iter()
            .map(|k| k.into().to_lowercase())
            .collect();
        self.stream_handlers
            .push(Matcher::Keyword(keywords), boxed_stream_handler(handler));
        self
    }

    /// React to network posts carrying the hashtag `tag` (with or without a
    /// leading `#`, case-insensitive). Matches both `#tag` tokens in the post
    /// text and structured record tags. Implicitly subscribes to
    /// `app.bsky.feed.post`.
    pub fn on_hashtag<F, Fut>(mut self, tag: impl Into<String>, handler: F) -> Self
    where
        F: Fn(Context, StreamEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let tag = tag.into();
        let tag = tag.strip_prefix('#').unwrap_or(&tag).to_lowercase();
        self.stream_handlers
            .push(Matcher::Hashtag(vec![tag]), boxed_stream_handler(handler));
        self
    }

    /// Register an error handler for stream handlers, mirroring
    /// [`on_error`](Self::on_error). Without one, stream handler errors are
    /// logged via `tracing`.
    pub fn on_stream_error<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Context, StreamEvent, Error) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.stream_handlers
            .set_error(boxed_stream_error_handler(handler));
        self
    }

    /// Override the Jetstream endpoint (default
    /// [`DEFAULT_JETSTREAM_ENDPOINT`](crate::DEFAULT_JETSTREAM_ENDPOINT)).
    pub fn jetstream_endpoint(mut self, url: impl Into<String>) -> Self {
        self.stream_config.endpoint = url.into();
        self
    }

    /// Add explicit collection NSIDs to the Jetstream subscription (e.g.
    /// `app.bsky.graph.follow`, or a prefix like `app.bsky.*`). Combined with
    /// whatever keyword/hashtag handlers imply.
    pub fn jetstream_collections<I, S>(mut self, collections: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.stream_config
            .collections
            .extend(collections.into_iter().map(Into::into));
        self
    }

    /// Restrict the Jetstream subscription to specific repository DIDs.
    pub fn jetstream_dids<I, S>(mut self, dids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.stream_config
            .dids
            .extend(dids.into_iter().map(Into::into));
        self
    }

    /// Start the Jetstream stream from an explicit cursor (unix microseconds)
    /// instead of the live tail — for replaying missed events after downtime.
    pub fn jetstream_cursor(mut self, time_us: u64) -> Self {
        self.stream_config.cursor = Some(time_us);
        self
    }

    /// Replace the entire [`JetstreamConfig`] at once.
    pub fn jetstream_config(mut self, config: JetstreamConfig) -> Self {
        self.stream_config = config;
        self
    }

    // --- direct messages (chat.bsky.convo) ---------------------------------

    /// React to incoming direct messages.
    ///
    /// The handler receives a [`DirectMessage`] for each new message in any of
    /// the bot's conversations. Runs concurrently with the notification loop, the
    /// scheduler, and the Jetstream stream (a bot may run with *only* message
    /// handlers). Messages the bot itself sent are never delivered, so an echo
    /// handler cannot loop.
    ///
    /// **Requires an app password with direct-message access** (a per-app-password
    /// opt-in in the Bluesky settings); without it the server rejects chat calls.
    /// To receive messages from accounts the bot does not follow, the bot's
    /// `chat.bsky.actor.declaration` must also allow them (`allowIncoming = "all"`);
    /// see [`DirectMessage`] and the crate README for how to open the inbox.
    ///
    /// ```
    /// # use bsky_bot_sdk::Bot;
    /// # fn demo(b: bsky_bot_sdk::BotBuilder) -> bsky_bot_sdk::BotBuilder {
    /// b.on_message(|ctx, dm| async move {
    ///     ctx.send_dm_to_convo(dm.convo_id(), format!("you said: {}", dm.text()))
    ///         .await?;
    ///     Ok(())
    /// })
    /// # }
    /// ```
    pub fn on_message<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Context, DirectMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.dm_handlers.push(boxed_dm_handler(handler));
        self
    }

    /// Register an error handler for message handlers, mirroring
    /// [`on_error`](Self::on_error). Without one, message handler errors are
    /// logged via `tracing`.
    pub fn on_message_error<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Context, DirectMessage, Error) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.dm_handlers.set_error(boxed_dm_error_handler(handler));
        self
    }

    /// Set the interval between direct-message polls (default 5s).
    pub fn dm_poll_interval(mut self, interval: Duration) -> Self {
        self.dm_config.poll_interval = interval;
        self
    }

    /// Process the backlog of direct messages that existed before startup.
    ///
    /// Off by default, so a restarting bot does not re-answer an old conversation
    /// backlog (mirrors [`process_backlog`](Self::process_backlog) for
    /// notifications).
    pub fn process_dm_backlog(mut self, process: bool) -> Self {
        self.dm_config.process_backlog = process;
        self
    }

    /// Replace the entire [`DmConfig`] at once.
    pub fn dm_config(mut self, config: DmConfig) -> Self {
        self.dm_config = config;
        self
    }

    // --- build -------------------------------------------------------------

    /// Authenticate (resuming a saved session if possible, otherwise logging in)
    /// and produce a runnable [`Bot`].
    ///
    /// # Errors
    /// Returns any error deferred from a scheduling call (e.g. an invalid cron
    /// expression passed to [`cron`](BotBuilder::cron)) before attempting to
    /// authenticate.
    pub async fn build(self) -> Result<Bot> {
        // 0. Surface any deferred scheduling error before doing network work.
        if let Some(err) = self.schedule_error {
            return Err(err);
        }

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
        if let Some(path) = &self.config.session_path
            && let Err(err) = agent.to_config().await.save(&FileStore::new(path)).await
        {
            tracing::warn!(error = %err, "failed to persist session file");
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
            scheduler: self.scheduler,
            stream_config: self.stream_config,
            stream_handlers: self.stream_handlers,
            dm_config: self.dm_config,
            dm_handlers: self.dm_handlers,
        })
    }
}

/// A configured, authenticated bot ready to poll for notifications and dispatch
/// them to handlers, plus any scheduled jobs.
pub struct Bot {
    context: Context,
    config: BotConfig,
    handlers: Handlers,
    scheduler: Scheduler,
    stream_config: JetstreamConfig,
    stream_handlers: StreamHandlers,
    dm_config: DmConfig,
    dm_handlers: DmHandlers,
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
    /// Drives the notification loop (when any handlers are registered), the
    /// [Jetstream] real-time stream (when any stream handlers are registered),
    /// and every scheduled job (see [`every`](BotBuilder::every) /
    /// [`cron`](BotBuilder::cron)) concurrently, stopping all of them cleanly on
    /// shutdown.
    ///
    /// Returns [`Error::NoHandlers`] immediately if no notification handler, no
    /// stream handler, no message handler, and no scheduled job were registered —
    /// the bot would have nothing to do.
    ///
    /// [Jetstream]: https://docs.bsky.app/blog/jetstream
    pub async fn run_until<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        let has_stream = !self.stream_handlers.is_empty();
        let has_dm = !self.dm_handlers.is_empty();
        if self.handlers.is_empty() && self.scheduler.is_empty() && !has_stream && !has_dm {
            return Err(Error::NoHandlers);
        }

        // Spawn each scheduled job and the stream runner on their own tasks. All
        // are cancelled cooperatively via a shared watch channel, so in-flight
        // work runs to completion before it stops.
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut background: Vec<tokio::task::JoinHandle<()>> = self
            .scheduler
            .jobs()
            .iter()
            .map(|job| tokio::spawn(job.clone().run(self.context.clone(), shutdown_rx.clone())))
            .collect();
        if !self.scheduler.is_empty() {
            tracing::info!(jobs = self.scheduler.len(), "scheduler started");
        }
        if has_stream {
            let runner =
                StreamRunner::new(self.stream_config.clone(), self.stream_handlers.clone());
            background.push(tokio::spawn(
                runner.run(self.context.clone(), shutdown_rx.clone()),
            ));
            tracing::info!("jetstream ingestion started");
        }
        if has_dm {
            let runner = DmRunner::new(self.dm_config.clone(), self.dm_handlers.clone());
            background.push(tokio::spawn(
                runner.run(self.context.clone(), shutdown_rx.clone()),
            ));
            tracing::info!("dm ingestion started");
        }

        tokio::pin!(shutdown);

        if self.handlers.is_empty() {
            // No notification handlers: nothing to poll, so just wait for
            // shutdown while the background tasks (stream / dm / schedules) run.
            tracing::info!(
                handle = %self.context.handle(),
                did = %self.context.did(),
                "bot started (background tasks only)",
            );
            shutdown.await;
            tracing::info!("shutdown signal received; stopping");
        } else {
            self.run_notification_loop(shutdown).await;
        }

        // Tell background tasks to stop and wait for them to wind down.
        let _ = shutdown_tx.send(true);
        for handle in background {
            let _ = handle.await;
        }

        Ok(())
    }

    /// The notification polling loop: prime the watermark, then poll and dispatch
    /// on each tick until `shutdown` resolves.
    async fn run_notification_loop<F>(&self, mut shutdown: Pin<&mut F>)
    where
        F: Future<Output = ()>,
    {
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

        if count > 0
            && self.config.mark_seen
            && let Err(err) = self.mark_seen().await
        {
            tracing::warn!(error = %err, "failed to mark notifications seen");
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

    #[tokio::test]
    async fn build_defers_invalid_cron_error_until_build() {
        // The fluent `.cron(...)` call cannot return a Result, so a bad
        // expression must surface from `build()`.
        let result = Bot::builder()
            .cron("total nonsense", |_ctx| async move { Ok(()) })
            .build()
            .await;
        assert!(
            matches!(result, Err(Error::InvalidInput(_))),
            "an invalid cron expression should fail the build with InvalidInput",
        );
    }

    /// Build a `Bot` without any network I/O (an agent with no session performs
    /// none), for exercising the run loop's start-up guards.
    async fn offline_bot(
        with_schedule: bool,
        with_handler: bool,
        with_stream: bool,
        with_dm: bool,
    ) -> Bot {
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
        let context = Context::new(agent, identity, WriteBudget::new(None));

        let mut handlers = Handlers::default();
        if with_handler {
            handlers.register_any(boxed_handler(|_c, _n| async move { Ok(()) }));
        }
        let mut scheduler = Scheduler::default();
        if with_schedule {
            scheduler.push(
                Schedule::every(Duration::from_secs(3600)),
                boxed_task(|_ctx| async move { Ok(()) }),
            );
        }
        let mut stream_handlers = StreamHandlers::default();
        if with_stream {
            stream_handlers.push(
                Matcher::Firehose,
                boxed_stream_handler(|_c, _e| async move { Ok(()) }),
            );
        }
        let mut dm_handlers = DmHandlers::default();
        if with_dm {
            dm_handlers.push(boxed_dm_handler(|_c, _m| async move { Ok(()) }));
        }

        Bot {
            context,
            config: BotConfig::default(),
            handlers,
            scheduler,
            stream_config: JetstreamConfig::default(),
            stream_handlers,
            dm_config: DmConfig::default(),
            dm_handlers,
        }
    }

    #[tokio::test]
    async fn run_until_errors_when_there_is_nothing_to_do() {
        let bot = offline_bot(false, false, false, false).await;
        let result = bot.run_until(async {}).await;
        assert!(
            matches!(result, Err(Error::NoHandlers)),
            "a bot with no handlers, schedules, stream, or dm should refuse to run",
        );
    }

    #[tokio::test]
    async fn run_until_with_only_a_schedule_runs_and_stops_cleanly() {
        let bot = offline_bot(true, false, false, false).await;
        // Immediate shutdown. With no handlers there is no polling (no network);
        // the scheduled job is spawned and then cancelled cooperatively.
        let result = bot.run_until(async {}).await;
        assert!(
            result.is_ok(),
            "a schedule-only bot must run without erroring as NoHandlers: {result:?}",
        );
    }

    #[tokio::test]
    async fn run_until_with_only_a_stream_runs_and_stops_cleanly() {
        let bot = offline_bot(false, false, true, false).await;
        // Immediate shutdown on the current-thread test runtime: the spawned
        // stream runner is only polled once the main task awaits the join, by
        // which point the shutdown flag is already set — so it exits at the top
        // of its loop before attempting any network connection.
        let result = bot.run_until(async {}).await;
        assert!(
            result.is_ok(),
            "a stream-only bot must run without erroring as NoHandlers: {result:?}",
        );
    }

    #[tokio::test]
    async fn run_until_with_only_a_dm_handler_runs_and_stops_cleanly() {
        let bot = offline_bot(false, false, false, true).await;
        // Same reasoning as the stream-only case: the spawned DM runner sees the
        // shutdown flag already set and exits before any network call.
        let result = bot.run_until(async {}).await;
        assert!(
            result.is_ok(),
            "a dm-only bot must run without erroring as NoHandlers: {result:?}",
        );
    }

    #[test]
    fn builder_registers_message_handler_and_dm_config() {
        let builder = Bot::builder()
            .dm_poll_interval(Duration::from_secs(9))
            .process_dm_backlog(true)
            .on_message(|_c, _m| async move { Ok(()) });

        assert!(
            !builder.dm_handlers.is_empty(),
            "on_message should register"
        );
        assert_eq!(builder.dm_config.poll_interval, Duration::from_secs(9));
        assert!(builder.dm_config.process_backlog);
    }
}
