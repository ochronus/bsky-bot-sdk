//! Direct messages (`chat.bsky.convo`): an `on_message` event loop plus the
//! wrapper, handler registry, and runner that drive it.
//!
//! Where the notification loop reacts to public interactions (mentions, likes,
//! follows) and Jetstream reacts to the whole network, this module reacts to the
//! bot's **private** conversations. Bluesky's chat lives behind a separate
//! service (`api.bsky.chat`, reached via the `atproto-proxy` header) with its own
//! `chat.bsky.convo.*` XRPC methods. The runner polls
//! [`chat.bsky.convo.getLog`](https://docs.bsky.app/docs/api/chat-bsky-convo-get-log)
//! — the incremental, cursor-based event log for your conversations — and
//! dispatches each newly-created message to the handlers registered with
//! [`on_message`](crate::BotBuilder::on_message). Send a reply with
//! [`ctx.send_dm`](crate::Context::send_dm) or, when you already have the
//! conversation, [`ctx.send_dm_to_convo`](crate::Context::send_dm_to_convo).
//!
//! ```no_run
//! use bsky_bot_sdk::Bot;
//!
//! # async fn demo() -> bsky_bot_sdk::Result<()> {
//! Bot::builder()
//!     .credentials("mybot.bsky.social", "app-password")
//!     .on_message(|ctx, dm| async move {
//!         // Echo the message back into the same conversation.
//!         ctx.send_dm_to_convo(dm.convo_id(), format!("you said: {}", dm.text()))
//!             .await?;
//!         Ok(())
//!     })
//!     .build()
//!     .await?
//!     .run()
//!     .await
//! # }
//! ```
//!
//! Messages the bot itself sent are never dispatched to `on_message`, so an echo
//! handler like the one above cannot loop.
//!
//! # Two settings gate direct messages
//!
//! **1. The app password needs DM access.** Direct-message access is a
//! per-app-password opt-in in the Bluesky settings (Settings → Privacy and
//! security → App passwords). A password without it will see chat calls rejected
//! by the server.
//!
//! **2. The bot's inbox must allow the sender.** Who may open a conversation with
//! an account is controlled by that account's `chat.bsky.actor.declaration`
//! record ([`DmAccess`]: `Everyone`, `Following`, or `Nobody`). The default blocks
//! people the bot does not follow, so a bot that should receive DMs from *anyone*
//! must publish [`DmAccess::Everyone`] once. Do it declaratively on the builder
//! with [`accept_dms_from`](crate::BotBuilder::accept_dms_from):
//!
//! ```
//! # use bsky_bot_sdk::Bot;
//! # use bsky_bot_sdk::DmAccess;
//! # fn demo(b: bsky_bot_sdk::BotBuilder) -> bsky_bot_sdk::BotBuilder {
//! b.accept_dms_from(DmAccess::Everyone)
//!     .on_message(|ctx, dm| async move {
//!         ctx.send_dm_to_convo(dm.convo_id(), "hi!").await?;
//!         Ok(())
//!     })
//! # }
//! ```
//!
//! …or at runtime with [`set_dm_access`](crate::Context::set_dm_access).
//!
//! Note that a recipient's own inbox setting also gates *sending*: a
//! [`send_dm`](crate::Context::send_dm) to someone who restricts incoming messages
//! fails with a `MessagesDisabled` error from the server — expected, not a bug.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use atrium_api::chat::bsky::convo::defs::{LogCreateMessageMessageRefs, MessageView};
use atrium_api::chat::bsky::convo::get_log;
use atrium_api::types::Union;
use atrium_api::types::string::Datetime;
use tokio::sync::watch;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::handler::BoxFuture;

/// The `chat.bsky.convo.defs#messageView` returned inside conversation logs and
/// from [`sendMessage`](crate::Context::send_dm), re-exported so callers can
/// reach the full record via [`DirectMessage::raw`] without a separate
/// `atrium-api` navigation.
pub type RawMessage = MessageView;

// ---------------------------------------------------------------------------
// Event model
// ---------------------------------------------------------------------------

/// An ergonomic wrapper over a single direct message.
///
/// Mirrors [`Notification`](crate::Notification) and
/// [`StreamEvent`](crate::StreamEvent): typed accessors for the commonly needed
/// fields, with the raw [`MessageView`] always reachable via
/// [`raw`](DirectMessage::raw). Cheap to clone, so it can be handed to several
/// handlers and moved into spawned tasks.
///
/// The [`convo_id`](DirectMessage::convo_id) identifies the conversation the
/// message belongs to — pass it to
/// [`ctx.send_dm_to_convo`](crate::Context::send_dm_to_convo) to reply without a
/// second lookup.
#[derive(Debug, Clone)]
pub struct DirectMessage {
    message: MessageView,
    convo_id: String,
}

impl DirectMessage {
    /// Wrap a raw message together with the id of the conversation it belongs to.
    pub(crate) fn new(message: MessageView, convo_id: impl Into<String>) -> Self {
        Self {
            message,
            convo_id: convo_id.into(),
        }
    }

    /// Borrow the underlying raw [`MessageView`].
    pub fn raw(&self) -> &MessageView {
        &self.message
    }

    /// The id of the conversation this message belongs to. Pass it to
    /// [`ctx.send_dm_to_convo`](crate::Context::send_dm_to_convo) to reply.
    pub fn convo_id(&self) -> &str {
        &self.convo_id
    }

    /// The message's own id (unique within the conversation).
    pub fn id(&self) -> &str {
        &self.message.id
    }

    /// The conversation revision this message produced.
    pub fn rev(&self) -> &str {
        &self.message.rev
    }

    /// The DID of the account that sent this message.
    pub fn sender_did(&self) -> &str {
        self.message.sender.did.as_str()
    }

    /// The plain text of the message.
    pub fn text(&self) -> &str {
        &self.message.text
    }

    /// When the message was sent. Useful for ordering.
    pub fn sent_at(&self) -> &Datetime {
        &self.message.sent_at
    }

    /// Whether this message was sent by the account with the given DID — used to
    /// skip the bot's own messages so an echo handler cannot loop.
    pub(crate) fn is_from(&self, did: &str) -> bool {
        self.sender_did() == did
    }
}

/// Extract a [`DirectMessage`] from a single `getLog` item, keeping only
/// `logCreateMessage` entries that carry a live (non-deleted) message view.
fn message_from_log(item: &Union<get_log::OutputLogsItem>) -> Option<DirectMessage> {
    let Union::Refs(get_log::OutputLogsItem::ChatBskyConvoDefsLogCreateMessage(log)) = item else {
        return None;
    };
    let Union::Refs(LogCreateMessageMessageRefs::MessageView(view)) = &log.message else {
        // A deleted message (or an unknown future ref) carries no text to react to.
        return None;
    };
    Some(DirectMessage::new((**view).clone(), log.convo_id.clone()))
}

/// The dispatchable messages in a page of log items: newly-created messages that
/// were **not** sent by `skip_did`. Skipping the bot's own DID here is what keeps
/// an echo handler from reacting to its own reply and looping forever.
fn dispatchable_messages(
    items: &[Union<get_log::OutputLogsItem>],
    skip_did: &str,
) -> Vec<DirectMessage> {
    items
        .iter()
        .filter_map(message_from_log)
        .filter(|dm| !dm.is_from(skip_did))
        .collect()
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

type DmHandlerFn = Arc<dyn Fn(Context, DirectMessage) -> BoxFuture<Result<()>> + Send + Sync>;

type DmErrorHandlerFn = Arc<dyn Fn(Context, DirectMessage, Error) -> BoxFuture<()> + Send + Sync>;

/// Erase a concrete async message handler into a [`DmHandlerFn`].
pub(crate) fn boxed_dm_handler<F, Fut>(handler: F) -> DmHandlerFn
where
    F: Fn(Context, DirectMessage) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    Arc::new(move |ctx, dm| Box::pin(handler(ctx, dm)))
}

/// Erase a concrete async message error handler into a [`DmErrorHandlerFn`].
pub(crate) fn boxed_dm_error_handler<F, Fut>(handler: F) -> DmErrorHandlerFn
where
    F: Fn(Context, DirectMessage, Error) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Arc::new(move |ctx, dm, err| Box::pin(handler(ctx, dm, err)))
}

/// The registry of direct-message handlers for a bot.
#[derive(Default, Clone)]
pub(crate) struct DmHandlers {
    handlers: Vec<DmHandlerFn>,
    on_error: Option<DmErrorHandlerFn>,
}

impl DmHandlers {
    pub(crate) fn push(&mut self, handler: DmHandlerFn) {
        self.handlers.push(handler);
    }

    pub(crate) fn set_error(&mut self, handler: DmErrorHandlerFn) {
        self.on_error = Some(handler);
    }

    /// True when no message handlers are registered.
    pub(crate) fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// Dispatch one message to every registered handler (in registration order).
    /// A handler returning `Err` is routed to the error handler (or logged) and
    /// never stops the others.
    pub(crate) async fn dispatch(&self, ctx: Context, dm: DirectMessage) {
        for handler in &self.handlers {
            if let Err(err) = handler(ctx.clone(), dm.clone()).await {
                match &self.on_error {
                    Some(on_error) => on_error(ctx.clone(), dm.clone(), err).await,
                    None => tracing::error!(
                        convo = %dm.convo_id(),
                        sender = %dm.sender_did(),
                        error = %err,
                        "message handler returned an error",
                    ),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Inbox access
// ---------------------------------------------------------------------------

/// Who may start a direct-message conversation with the bot.
///
/// Mirrors the `allowIncoming` field of the `chat.bsky.actor.declaration` record.
/// Apply it with [`accept_dms_from`](crate::BotBuilder::accept_dms_from) on the
/// builder, or [`set_dm_access`](crate::Context::set_dm_access) at runtime. The
/// account default blocks accounts the bot does not follow, so a bot that should
/// receive DMs from anyone needs [`DmAccess::Everyone`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmAccess {
    /// Anyone may open a conversation with the bot.
    Everyone,
    /// Only accounts the bot follows may open a conversation with it.
    Following,
    /// No one may open a new conversation with the bot.
    Nobody,
}

impl DmAccess {
    /// The wire value for the `allowIncoming` field.
    pub(crate) fn as_wire(self) -> &'static str {
        match self {
            DmAccess::Everyone => "all",
            DmAccess::Following => "following",
            DmAccess::Nobody => "none",
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the direct-message poll loop.
///
/// Tweak via the `dm_*` builder methods on [`BotBuilder`](crate::BotBuilder), or
/// construct directly.
#[derive(Debug, Clone)]
pub struct DmConfig {
    /// How long to wait between `getLog` polls (default 5s). Chat feels more
    /// conversational than the notification loop, so it polls a little faster.
    pub poll_interval: Duration,
    /// Whether to dispatch messages that already existed when the bot started.
    ///
    /// Defaults to `false` so a restarting bot does not re-answer an old
    /// conversation backlog — matching the notification loop's
    /// [`process_backlog`](crate::BotConfig::process_backlog) default.
    pub process_backlog: bool,
}

impl Default for DmConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            process_backlog: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// A safety cap on how many `getLog` pages we walk in a single drain, so a
/// pathological (never-empty) server response cannot spin forever. A real
/// conversation log is far smaller than this.
const MAX_LOG_PAGES: u32 = 10_000;

/// The result of establishing the starting cursor.
enum PrimeOutcome {
    /// Shutdown was signalled during priming.
    Shutdown,
    /// Ready to poll from this cursor (`None` = from the beginning of the log).
    Ready(Option<String>),
}

/// Drives the direct-message poll loop: establish a cursor, then poll
/// `chat.bsky.convo.getLog` on an interval and dispatch new messages until
/// shutdown. Mirrors the [`StreamRunner`](crate::stream) cooperative-shutdown
/// model so [`run_until`](crate::Bot::run_until) can drive it alongside the
/// notification loop, the scheduler, and the Jetstream stream.
pub(crate) struct DmRunner {
    config: DmConfig,
    handlers: DmHandlers,
}

impl DmRunner {
    pub(crate) fn new(config: DmConfig, handlers: DmHandlers) -> Self {
        Self { config, handlers }
    }

    /// Poll until `shutdown` flips to `true`.
    pub(crate) async fn run(self, ctx: Context, mut shutdown: watch::Receiver<bool>) {
        let mut cursor = match self.prime(&ctx, &mut shutdown).await {
            PrimeOutcome::Shutdown => {
                tracing::info!("dm ingestion stopped");
                return;
            }
            PrimeOutcome::Ready(cursor) => cursor,
        };

        tracing::info!(
            interval_secs = self.config.poll_interval.as_secs(),
            "dm ingestion started",
        );

        let mut ticker = tokio::time::interval(self.config.poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = ticker.tick() => {
                    if let Err(err) = self.poll(&ctx, &mut cursor, &shutdown).await {
                        tracing::warn!(error = %err, "dm poll cycle failed");
                    }
                }
            }
            if *shutdown.borrow() {
                break;
            }
        }

        tracing::info!("dm ingestion stopped");
    }

    /// Establish the starting cursor. When `process_backlog` is set we start from
    /// the beginning of the log (`None`) and let the first poll dispatch it;
    /// otherwise we walk the log to its live tail, discarding what we find, so the
    /// bot only reacts to messages that arrive from now on.
    async fn prime(&self, ctx: &Context, shutdown: &mut watch::Receiver<bool>) -> PrimeOutcome {
        if self.config.process_backlog {
            return PrimeOutcome::Ready(None);
        }

        let mut cursor: Option<String> = None;
        let mut pages = 0u32;
        loop {
            if *shutdown.borrow() {
                return PrimeOutcome::Shutdown;
            }
            let out = tokio::select! {
                _ = shutdown.changed() => return PrimeOutcome::Shutdown,
                res = ctx.fetch_convo_log(cursor.clone()) => match res {
                    Ok(out) => out,
                    Err(err) => {
                        // If we can't drain the backlog, fall back to whatever
                        // cursor we reached rather than replaying everything.
                        tracing::warn!(
                            error = %err,
                            "priming dm cursor failed; starting from the furthest cursor reached",
                        );
                        return PrimeOutcome::Ready(cursor);
                    }
                }
            };
            if let Some(next) = &out.data.cursor {
                cursor = Some(next.clone());
            }
            pages += 1;
            if out.data.logs.is_empty() {
                let skipped = pages.saturating_sub(1);
                tracing::info!(
                    pages = skipped,
                    "primed dm cursor; skipping existing backlog"
                );
                return PrimeOutcome::Ready(cursor);
            }
            if pages >= MAX_LOG_PAGES {
                tracing::warn!(
                    pages,
                    "dm backlog exceeded the prime page cap; some old messages may be replayed",
                );
                return PrimeOutcome::Ready(cursor);
            }
        }
    }

    /// Drain every currently-available page from `cursor`, dispatching each new
    /// message (skipping the bot's own), and advance `cursor` past them.
    async fn poll(
        &self,
        ctx: &Context,
        cursor: &mut Option<String>,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<()> {
        let mut pages = 0u32;
        loop {
            let out = ctx.fetch_convo_log(cursor.clone()).await?;
            // Dispatch new messages from other people; our own are filtered out so
            // an echo handler cannot loop.
            for dm in dispatchable_messages(&out.data.logs, ctx.did()) {
                self.handlers.dispatch(ctx.clone(), dm).await;
            }
            // Adopt the server's cursor so the next request resumes past what we
            // just handled; the log never re-serves events before its cursor.
            if let Some(next) = &out.data.cursor {
                *cursor = Some(next.clone());
            }
            pages += 1;
            if out.data.logs.is_empty() || pages >= MAX_LOG_PAGES || *shutdown.borrow() {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::BotIdentity;
    use crate::ratelimit::WriteBudget;
    use std::sync::Mutex;

    // --- fixtures ----------------------------------------------------------

    const ALICE: &str = "did:plc:alice000000000000000000";
    const BOT: &str = "did:plc:bot00000000000000000000000";

    fn message_view(sender: &str, text: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "msg-1",
            "rev": "rev-1",
            "sender": { "did": sender },
            "sentAt": "2026-07-23T10:00:00.000Z",
            "text": text,
        })
    }

    fn log_create(convo_id: &str, sender: &str, text: &str) -> serde_json::Value {
        serde_json::json!({
            "$type": "chat.bsky.convo.defs#logCreateMessage",
            "convoId": convo_id,
            "rev": "rev-1",
            "message": {
                "$type": "chat.bsky.convo.defs#messageView",
                "id": "msg-1",
                "rev": "rev-1",
                "sender": { "did": sender },
                "sentAt": "2026-07-23T10:00:00.000Z",
                "text": text,
            },
        })
    }

    fn direct_message(sender: &str, text: &str) -> DirectMessage {
        let view: MessageView =
            serde_json::from_value(message_view(sender, text)).expect("valid message fixture");
        DirectMessage::new(view, "convo-1")
    }

    fn log_item(value: serde_json::Value) -> Union<get_log::OutputLogsItem> {
        serde_json::from_value(value).expect("valid log item fixture")
    }

    async fn test_context() -> Context {
        let agent = bsky_sdk::BskyAgent::builder()
            .build()
            .await
            .expect("build agent");
        let identity = Arc::new(BotIdentity::new(
            BOT.parse().expect("valid did"),
            "bot.test".parse().expect("valid handle"),
        ));
        Context::new(agent, identity, WriteBudget::new(None))
    }

    // --- accessors ---------------------------------------------------------

    #[test]
    fn accessors_expose_message_fields() {
        let dm = direct_message(ALICE, "hello bot");
        assert_eq!(dm.convo_id(), "convo-1");
        assert_eq!(dm.id(), "msg-1");
        assert_eq!(dm.rev(), "rev-1");
        assert_eq!(dm.sender_did(), ALICE);
        assert_eq!(dm.text(), "hello bot");
        assert_eq!(dm.raw().text, "hello bot");
    }

    #[test]
    fn is_from_matches_the_sender_did() {
        let dm = direct_message(ALICE, "hi");
        assert!(dm.is_from(ALICE), "sender's own DID must match");
        assert!(!dm.is_from(BOT), "a different DID must not match");
    }

    // --- log parsing -------------------------------------------------------

    #[test]
    fn message_from_log_decodes_a_create_message() {
        let item = log_item(log_create("convo-9", ALICE, "yo"));
        let dm = message_from_log(&item).expect("logCreateMessage yields a DirectMessage");
        assert_eq!(dm.convo_id(), "convo-9");
        assert_eq!(dm.sender_did(), ALICE);
        assert_eq!(dm.text(), "yo");
    }

    #[test]
    fn message_from_log_ignores_non_create_entries() {
        // A logReadMessage is not a new message, so it must not be dispatched.
        let read = log_item(serde_json::json!({
            "$type": "chat.bsky.convo.defs#logReadMessage",
            "convoId": "convo-1",
            "rev": "rev-1",
            "message": {
                "$type": "chat.bsky.convo.defs#messageView",
                "id": "msg-1",
                "rev": "rev-1",
                "sender": { "did": ALICE },
                "sentAt": "2026-07-23T10:00:00.000Z",
                "text": "already seen",
            },
        }));
        assert!(
            message_from_log(&read).is_none(),
            "read logs are not messages"
        );

        let begin = log_item(serde_json::json!({
            "$type": "chat.bsky.convo.defs#logBeginConvo",
            "convoId": "convo-1",
            "rev": "rev-1",
        }));
        assert!(
            message_from_log(&begin).is_none(),
            "begin logs are not messages"
        );
    }

    #[test]
    fn dispatchable_messages_skips_own_and_keeps_others() {
        // A page mixing a message from Alice, one the bot itself sent, and a
        // non-create (read) log. Only Alice's message should be dispatched.
        let page = vec![
            log_item(log_create("convo-1", ALICE, "from alice")),
            log_item(log_create("convo-1", BOT, "from me")),
            log_item(serde_json::json!({
                "$type": "chat.bsky.convo.defs#logReadMessage",
                "convoId": "convo-1",
                "rev": "rev-1",
                "message": {
                    "$type": "chat.bsky.convo.defs#messageView",
                    "id": "msg-1",
                    "rev": "rev-1",
                    "sender": { "did": ALICE },
                    "sentAt": "2026-07-23T10:00:00.000Z",
                    "text": "read",
                },
            })),
        ];

        let dispatched = dispatchable_messages(&page, BOT);
        assert_eq!(
            dispatched.len(),
            1,
            "only the non-own, newly-created message is dispatchable",
        );
        assert_eq!(dispatched[0].sender_did(), ALICE);
        assert_eq!(dispatched[0].text(), "from alice");

        // If we do NOT skip our own DID, the bot's own message leaks through —
        // this is the assertion that fails if the loop-prevention filter is
        // dropped.
        let unfiltered = dispatchable_messages(&page, "did:plc:nobody00000000000000000");
        assert_eq!(
            unfiltered.len(),
            2,
            "without the self-filter, the bot's own message would be dispatched",
        );
    }

    #[test]
    fn message_from_log_ignores_deleted_message_views() {
        let deleted = log_item(serde_json::json!({
            "$type": "chat.bsky.convo.defs#logCreateMessage",
            "convoId": "convo-1",
            "rev": "rev-1",
            "message": {
                "$type": "chat.bsky.convo.defs#deletedMessageView",
                "id": "msg-1",
                "rev": "rev-1",
                "sender": { "did": ALICE },
                "sentAt": "2026-07-23T10:00:00.000Z",
            },
        }));
        assert!(
            message_from_log(&deleted).is_none(),
            "a deleted message view carries no text to react to",
        );
    }

    // --- dispatch ----------------------------------------------------------

    #[tokio::test]
    async fn dispatch_runs_every_handler_in_order() {
        let log = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut handlers = DmHandlers::default();

        let first = Arc::clone(&log);
        handlers.push(boxed_dm_handler(move |_c, dm| {
            let log = Arc::clone(&first);
            async move {
                log.lock().unwrap().push(format!("a:{}", dm.text()));
                Ok(())
            }
        }));
        let second = Arc::clone(&log);
        handlers.push(boxed_dm_handler(move |_c, _dm| {
            let log = Arc::clone(&second);
            async move {
                log.lock().unwrap().push("b".to_string());
                Ok(())
            }
        }));

        let ctx = test_context().await;
        handlers.dispatch(ctx, direct_message(ALICE, "hi")).await;
        assert_eq!(&*log.lock().unwrap(), &["a:hi", "b"]);
    }

    #[tokio::test]
    async fn dispatch_routes_errors_and_keeps_going() {
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let ran_second = Arc::new(Mutex::new(false));
        let mut handlers = DmHandlers::default();

        handlers.push(boxed_dm_handler(|_c, _dm| async move {
            Err(Error::invalid_input("boom"))
        }));
        let ran = Arc::clone(&ran_second);
        handlers.push(boxed_dm_handler(move |_c, _dm| {
            let ran = Arc::clone(&ran);
            async move {
                *ran.lock().unwrap() = true;
                Ok(())
            }
        }));
        let err_seen = Arc::clone(&seen);
        handlers.set_error(boxed_dm_error_handler(move |_c, _dm, err| {
            let seen = Arc::clone(&err_seen);
            async move {
                seen.lock().unwrap().push(err.to_string());
            }
        }));

        let ctx = test_context().await;
        handlers.dispatch(ctx, direct_message(ALICE, "hi")).await;

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

    #[test]
    fn config_default_is_a_polite_non_replaying_bot() {
        let cfg = DmConfig::default();
        assert_eq!(cfg.poll_interval, Duration::from_secs(5));
        assert!(
            !cfg.process_backlog,
            "a restarting bot must not re-answer an old backlog by default",
        );
    }

    #[test]
    fn dm_access_maps_to_the_lexicon_wire_values() {
        // These strings are the `allowIncoming` enum in chat.bsky.actor.declaration;
        // if a mapping drifts, the server would silently apply the wrong policy.
        assert_eq!(DmAccess::Everyone.as_wire(), "all");
        assert_eq!(DmAccess::Following.as_wire(), "following");
        assert_eq!(DmAccess::Nobody.as_wire(), "none");
    }
}
