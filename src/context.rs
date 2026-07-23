//! The [`Context`] handed to every handler: a cheap-to-clone handle bundling the
//! authenticated agent, the bot's own identity, and ready-made action helpers.

use std::sync::Arc;

use atrium_api::agent::AtprotoServiceType;
use atrium_api::agent::bluesky::BSKY_CHAT_DID;
use atrium_api::app::bsky::actor::defs::{ProfileView, ViewerState};
use atrium_api::app::bsky::actor::{get_profile, profile};
use atrium_api::app::bsky::feed::defs::FeedViewPost;
use atrium_api::app::bsky::feed::{
    get_author_feed, get_timeline, like, post, postgate, repost, threadgate,
};
use atrium_api::app::bsky::graph::{
    block, defs as graph_defs, follow, get_followers, get_follows, list, listitem, mute_actor,
    unmute_actor,
};
use atrium_api::chat::bsky::actor::declaration;
use atrium_api::chat::bsky::convo::defs::{MessageInput, MessageInputData};
use atrium_api::chat::bsky::convo::{get_convo_for_members, get_log, send_message};
use atrium_api::com::atproto::repo::{
    create_record, delete_record, get_record, put_record, strong_ref,
};
use atrium_api::types::LimitedNonZeroU8;
use atrium_api::types::string::{AtIdentifier, Datetime, Did, Handle, Nsid, RecordKey};
use atrium_api::types::{BlobRef, Union};
use atrium_api::xrpc::error::XrpcErrorKind;
use bsky_sdk::BskyAgent;
use bsky_sdk::record::Record;
use bsky_sdk::rich_text::RichText;

use crate::dm::{DirectMessage, DmAccess};
use crate::embed::PostBuilder;
use crate::error::{Error, Result};
use crate::event::Notification;
use crate::ratelimit::{RateLimitClient, RateLimitStatus, WriteBudget};
use crate::read::{Page, Paginated, paginate};
use crate::retry::{RetryPolicy, retry};
use crate::self_label::{has_bot_label, set_bot_label};
use crate::store::Store;
use crate::thread::ThreadBuilder;

/// The DID of the Bluesky chat service, reached via the `atproto-proxy` header.
/// Parsed from the `atrium-api` constant; the value is a fixed, valid DID.
fn chat_service_did() -> Result<Did> {
    BSKY_CHAT_DID
        .parse()
        .map_err(|_| Error::invalid_input(format!("invalid chat service DID: {BSKY_CHAT_DID}")))
}

/// The literal record key of the singleton profile record.
fn profile_rkey() -> Result<RecordKey> {
    "self"
        .parse()
        .map_err(|_| Error::invalid_input("invalid record key for profile"))
}

/// A profile record with no fields set (used when an account has no
/// `app.bsky.actor.profile` record yet, e.g. a brand-new bot account).
fn empty_profile() -> profile::RecordData {
    profile::RecordData {
        avatar: None,
        banner: None,
        created_at: Some(Datetime::now()),
        description: None,
        display_name: None,
        joined_via_starter_pack: None,
        labels: None,
        pinned_post: None,
        pronouns: None,
        website: None,
    }
}

/// Whether an `atrium` XRPC error is a `getRecord` "record not found" — i.e. the
/// repo simply has no record at that key, as opposed to a transport, auth, or
/// other server error (which callers must *not* mistake for "no record").
///
/// Matches both the lexicon-typed `Custom` form and, for PDSes that return the
/// error untyped, the `Undefined` form carrying the `RecordNotFound` name.
fn is_record_not_found(err: &atrium_api::xrpc::Error<get_record::Error>) -> bool {
    match err {
        atrium_api::xrpc::Error::XrpcResponse(resp) => match &resp.error {
            Some(XrpcErrorKind::Custom(get_record::Error::RecordNotFound(_))) => true,
            Some(XrpcErrorKind::Undefined(body)) => body.error.as_deref() == Some("RecordNotFound"),
            _ => false,
        },
        _ => false,
    }
}

/// Parse a DID string into a typed [`Did`].
fn parse_did(did: &str) -> Result<Did> {
    did.parse()
        .map_err(|_| Error::invalid_input(format!("invalid DID: {did}")))
}

/// Parse a handle-or-DID string into an [`AtIdentifier`], the form the actor-lookup
/// endpoints (`getProfile`, `muteActor`, …) accept.
fn at_identifier(actor: &str) -> Result<AtIdentifier> {
    actor
        .parse()
        .map_err(|_| Error::invalid_input(format!("invalid handle or DID: {actor}")))
}

/// The per-page fetch size for the paginating read helpers: the API maximum, so a
/// full list is walked in as few round-trips as possible.
fn page_limit() -> Option<LimitedNonZeroU8<100>> {
    LimitedNonZeroU8::<100>::try_from(100).ok()
}

/// Extract the record key (the final path segment) from an `at://…` URI, e.g.
/// `3k2a…` from `at://did:plc:x/app.bsky.feed.post/3k2a…`. A thread-gate or
/// post-gate shares the record key of the post it governs.
fn rkey_of(at_uri: &str) -> Result<RecordKey> {
    at_uri
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::invalid_input(format!("no record key in AT-URI: {at_uri}")))?
        .parse()
        .map_err(|_| Error::invalid_input(format!("invalid record key in AT-URI: {at_uri}")))
}

/// Who may reply to a post, expressed as one allow-rule of a thread-gate.
///
/// A post with **no** thread-gate is open to everyone; a post whose gate lists
/// one or more [`ReplyGate`] rules is limited to the union of those audiences; a
/// gate with an **empty** rule set (see [`Context::disable_replies`]) is closed to
/// everyone but the author. Pass rules to [`Context::set_reply_gate`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplyGate {
    /// Only accounts mentioned in the post may reply.
    Mentioned,
    /// Only accounts the post's author follows may reply.
    Following,
    /// Only accounts that follow the post's author may reply.
    Followers,
    /// Only members of the given list (an `at://…/app.bsky.graph.list/…` URI) may
    /// reply.
    List(String),
}

impl ReplyGate {
    /// Convert to the atrium thread-gate allow-rule union member.
    fn to_allow_item(&self) -> Union<threadgate::RecordAllowItem> {
        use threadgate::{
            FollowerRuleData, FollowingRuleData, ListRuleData, MentionRuleData, RecordAllowItem,
        };
        let item = match self {
            ReplyGate::Mentioned => {
                RecordAllowItem::MentionRule(Box::new(MentionRuleData {}.into()))
            }
            ReplyGate::Following => {
                RecordAllowItem::FollowingRule(Box::new(FollowingRuleData {}.into()))
            }
            ReplyGate::Followers => {
                RecordAllowItem::FollowerRule(Box::new(FollowerRuleData {}.into()))
            }
            ReplyGate::List(uri) => {
                RecordAllowItem::ListRule(Box::new(ListRuleData { list: uri.clone() }.into()))
            }
        };
        Union::Refs(item)
    }
}

/// The bot's own account identity, resolved at login.
#[derive(Debug, Clone)]
pub struct BotIdentity {
    did: Did,
    handle: Handle,
}

impl BotIdentity {
    pub(crate) fn new(did: Did, handle: Handle) -> Self {
        Self { did, handle }
    }

    /// The bot's DID.
    pub fn did(&self) -> &str {
        self.did.as_str()
    }

    /// The bot's typed DID, for calls that need an `AtIdentifier`.
    pub(crate) fn did_typed(&self) -> &Did {
        &self.did
    }

    /// The bot's handle (e.g. `mybot.bsky.social`).
    pub fn handle(&self) -> &str {
        self.handle.as_str()
    }
}

/// Everything a handler needs to react to a notification.
///
/// A `Context` is cheap to clone (it holds `Arc`/`Arc`-backed handles) and is
/// `Send + Sync`, so it can be freely moved into spawned tasks. The action
/// helpers ([`post`](Context::post), [`reply_to`](Context::reply_to),
/// [`like`](Context::like), …) transparently detect rich-text facets and respect
/// the configured write rate limit.
#[derive(Clone)]
pub struct Context {
    agent: BskyAgent<RateLimitClient>,
    identity: Arc<BotIdentity>,
    budget: WriteBudget,
    retry: RetryPolicy,
    store: Option<Arc<dyn Store>>,
}

impl Context {
    pub(crate) fn new(
        agent: BskyAgent<RateLimitClient>,
        identity: Arc<BotIdentity>,
        budget: WriteBudget,
    ) -> Self {
        Self {
            agent,
            identity,
            budget,
            retry: RetryPolicy::default(),
            store: None,
        }
    }

    /// Override the retry policy applied to this context's idempotent reads.
    pub(crate) fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Attach a persistence backend for the watermark, idempotency set, and any
    /// handler-owned state.
    pub(crate) fn with_store(mut self, store: Option<Arc<dyn Store>>) -> Self {
        self.store = store;
        self
    }

    /// The authenticated agent, for calls not covered by the helpers below.
    ///
    /// Its client is a [`RateLimitClient`], which records the server's
    /// `RateLimit-*` headers; every helper method is generic over the client, so
    /// this reads exactly like a plain `BskyAgent`.
    pub fn agent(&self) -> &BskyAgent<RateLimitClient> {
        &self.agent
    }

    /// The bot's own identity (DID + handle).
    pub fn me(&self) -> &BotIdentity {
        &self.identity
    }

    /// The server's most recently reported rate-limit status (from Bluesky's
    /// `RateLimit-*` response headers), or `None` if it has not reported any yet.
    ///
    /// This is the server's truth, as opposed to the client-side estimate the
    /// [`RateLimiter`](crate::RateLimiter) maintains. Writes issued through this
    /// context already wait when the server reports the window exhausted.
    pub fn server_rate_limit(&self) -> Option<RateLimitStatus> {
        self.budget.server_status()
    }

    /// The bot's DID.
    pub fn did(&self) -> &str {
        self.identity.did()
    }

    /// The bot's handle.
    pub fn handle(&self) -> &str {
        self.identity.handle()
    }

    // --- persistence -------------------------------------------------------

    /// The persistence backend, if one was configured with
    /// [`BotBuilder::store`](crate::BotBuilder::store).
    ///
    /// Use it to keep conversation state or any per-user/per-thread data across
    /// restarts (`ctx.store()?.save("dialog:{did}", &json).await?`). For the common
    /// "have I already acted on this?" case, prefer the
    /// [`remember`](Context::remember) / [`is_remembered`](Context::is_remembered)
    /// helpers below.
    pub fn store(&self) -> Option<&Arc<dyn Store>> {
        self.store.as_ref()
    }

    /// Record `key` in the idempotency set, so [`is_remembered`](Context::is_remembered)
    /// returns `true` for it on a later call (surviving restarts if the store is
    /// persistent). A no-op with no store configured.
    ///
    /// Use it to make outbound actions idempotent — remember a notification's URI
    /// after replying so a restart mid-batch can't double-reply.
    pub async fn remember(&self, key: impl AsRef<str>) -> Result<()> {
        if let Some(store) = &self.store {
            store.save(key.as_ref(), "1").await?;
        }
        Ok(())
    }

    /// Whether `key` was previously [`remember`](Context::remember)ed. Always
    /// `false` when no store is configured (so the action simply proceeds).
    pub async fn is_remembered(&self, key: impl AsRef<str>) -> Result<bool> {
        match &self.store {
            Some(store) => Ok(store.load(key.as_ref()).await?.is_some()),
            None => Ok(false),
        }
    }

    /// Drop `key` from the idempotency set. A no-op with no store configured.
    pub async fn forget(&self, key: impl AsRef<str>) -> Result<()> {
        if let Some(store) = &self.store {
            store.remove(key.as_ref()).await?;
        }
        Ok(())
    }

    // --- posting -----------------------------------------------------------

    /// Build a post record, auto-detecting mentions/links/tags as facets.
    pub(crate) async fn build_post(
        &self,
        text: impl AsRef<str>,
        reply: Option<post::ReplyRef>,
    ) -> Result<post::RecordData> {
        let rich = RichText::new_with_detect_facets(text).await?;
        Ok(post::RecordData {
            created_at: Datetime::now(),
            embed: None,
            entities: None,
            facets: rich.facets,
            labels: None,
            langs: None,
            reply,
            tags: None,
            text: rich.text,
        })
    }

    /// Publish a new top-level post. Facets (mentions, links, hashtags) are
    /// detected automatically.
    pub async fn post(&self, text: impl AsRef<str>) -> Result<create_record::Output> {
        let record = self.build_post(text, None).await?;
        self.post_record(record).await
    }

    /// Publish a fully-formed post record (for embeds, self-labels, custom langs,
    /// and other advanced cases). No facet detection is performed.
    pub async fn post_record(&self, record: post::RecordData) -> Result<create_record::Output> {
        self.budget.charge_create().await;
        Ok(record.create(&self.agent).await?)
    }

    /// Start composing a post with rich media and embeds.
    ///
    /// Returns a fluent [`PostBuilder`]: chain [`image`](PostBuilder::image)
    /// (alt text required), [`video`](PostBuilder::video),
    /// [`link_card`](PostBuilder::link_card), [`quote`](PostBuilder::quote), …
    /// then call [`send`](PostBuilder::send). For a plain text post,
    /// [`post`](Context::post) is the one-liner.
    ///
    /// ```no_run
    /// # use bsky_bot_sdk::prelude::*;
    /// # async fn f(ctx: Context) -> Result<()> {
    /// ctx.compose()
    ///     .text("first post with a picture!")
    ///     .image(std::fs::read("photo.jpg")?, "A sunset over the ocean")
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn compose(&self) -> PostBuilder {
        PostBuilder::new(self.clone())
    }

    /// Start composing a multi-post thread.
    ///
    /// Returns a fluent [`ThreadBuilder`]: add pieces with
    /// [`text`](ThreadBuilder::text) / [`texts`](ThreadBuilder::texts), optionally
    /// [`reply_to`](ThreadBuilder::reply_to) a notification or
    /// [`numbered`](ThreadBuilder::numbered) the posts, then call
    /// [`send`](ThreadBuilder::send). Text over
    /// [`MAX_POST_GRAPHEMES`](crate::MAX_POST_GRAPHEMES) is split, at word
    /// boundaries, into as many posts as it needs.
    ///
    /// ```no_run
    /// # use bsky_bot_sdk::prelude::*;
    /// # async fn f(ctx: Context) -> Result<()> {
    /// ctx.thread()
    ///     .text("A long story that won't fit in one post …")
    ///     .numbered()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn thread(&self) -> ThreadBuilder {
        ThreadBuilder::new(self.clone())
    }

    /// Upload raw bytes as a blob to the bot's own PDS, returning a blob ref you
    /// can place into a custom record. Works on any PDS.
    ///
    /// Most callers want [`compose`](Context::compose) instead, which uploads and
    /// embeds media for you (and stamps the correct MIME type). Use this directly
    /// only for advanced records the builder does not cover.
    ///
    /// Blob uploads are governed by a separate server limit and are *not* charged
    /// against the client-side points budget (which models repo writes); only the
    /// final `createRecord` is.
    pub async fn upload_blob(&self, bytes: impl Into<Vec<u8>>) -> Result<BlobRef> {
        let out = self
            .agent
            .api
            .com
            .atproto
            .repo
            .upload_blob(bytes.into())
            .await?;
        Ok(out.data.blob)
    }

    // --- replies -----------------------------------------------------------

    /// Reply to a notification, threading correctly.
    ///
    /// The reply's `parent` is the notifying record; its `root` is taken from the
    /// parent post's own thread root when present, so replies to a deep thread
    /// stay attached to the original root rather than starting a new one.
    pub async fn reply_to(
        &self,
        notif: &Notification,
        text: impl AsRef<str>,
    ) -> Result<create_record::Output> {
        let parent = notif.subject_ref();
        let root = notif
            .as_post()
            .and_then(|p| p.reply.map(|r| r.root.clone()))
            .unwrap_or_else(|| parent.clone());
        self.reply(parent, root, text).await
    }

    /// Reply with explicit `parent` and `root` strong refs.
    pub async fn reply(
        &self,
        parent: strong_ref::Main,
        root: strong_ref::Main,
        text: impl AsRef<str>,
    ) -> Result<create_record::Output> {
        let reply = post::ReplyRefData { parent, root }.into();
        let record = self.build_post(text, Some(reply)).await?;
        self.post_record(record).await
    }

    // --- reactions ---------------------------------------------------------

    /// Like the record that generated a notification (e.g. the post that mentioned
    /// you).
    pub async fn like(&self, notif: &Notification) -> Result<create_record::Output> {
        self.like_ref(notif.subject_ref()).await
    }

    /// Like an arbitrary subject by strong ref.
    pub async fn like_ref(&self, subject: strong_ref::Main) -> Result<create_record::Output> {
        let record = like::RecordData {
            created_at: Datetime::now(),
            subject,
            via: None,
        };
        self.budget.charge_create().await;
        Ok(record.create(&self.agent).await?)
    }

    /// Repost the record that generated a notification.
    pub async fn repost(&self, notif: &Notification) -> Result<create_record::Output> {
        self.repost_ref(notif.subject_ref()).await
    }

    /// Repost an arbitrary subject by strong ref.
    pub async fn repost_ref(&self, subject: strong_ref::Main) -> Result<create_record::Output> {
        let record = repost::RecordData {
            created_at: Datetime::now(),
            subject,
            via: None,
        };
        self.budget.charge_create().await;
        Ok(record.create(&self.agent).await?)
    }

    // --- graph -------------------------------------------------------------

    /// Follow the actor who triggered a notification (e.g. follow-back).
    pub async fn follow_back(&self, notif: &Notification) -> Result<create_record::Output> {
        self.follow_did(notif.author().did.clone()).await
    }

    /// Follow an actor by DID string.
    pub async fn follow(&self, did: impl AsRef<str>) -> Result<create_record::Output> {
        self.follow_did(parse_did(did.as_ref())?).await
    }

    /// Follow an actor by typed DID.
    pub async fn follow_did(&self, subject: Did) -> Result<create_record::Output> {
        let record = follow::RecordData {
            created_at: Datetime::now(),
            subject,
        };
        self.budget.charge_create().await;
        Ok(record.create(&self.agent).await?)
    }

    /// Unfollow an actor (by handle or DID) by deleting the bot's follow record.
    ///
    /// Returns `true` if the bot was following the actor and the follow was
    /// deleted, or `false` if it was not following them (a no-op). The follow
    /// record's URI is discovered via `getProfile` (`viewer.following`).
    pub async fn unfollow(&self, actor: impl AsRef<str>) -> Result<bool> {
        match self
            .viewer_for(actor.as_ref())
            .await?
            .and_then(|v| v.data.following)
        {
            Some(uri) => {
                self.delete(uri).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    // --- moderation --------------------------------------------------------

    /// Mute an actor (by handle or DID).
    ///
    /// Muting is a private, server-side preference: the muted account is never
    /// notified, and — unlike a block — no public record is written, so this is
    /// **not** charged against the client-side write budget.
    pub async fn mute(&self, actor: impl AsRef<str>) -> Result<()> {
        let actor = at_identifier(actor.as_ref())?;
        self.agent
            .api
            .app
            .bsky
            .graph
            .mute_actor(mute_actor::InputData { actor }.into())
            .await?;
        Ok(())
    }

    /// Remove a mute previously set with [`mute`](Context::mute).
    pub async fn unmute(&self, actor: impl AsRef<str>) -> Result<()> {
        let actor = at_identifier(actor.as_ref())?;
        self.agent
            .api
            .app
            .bsky
            .graph
            .unmute_actor(unmute_actor::InputData { actor }.into())
            .await?;
        Ok(())
    }

    /// Block an actor by DID.
    ///
    /// Unlike a mute, a block is a public `app.bsky.graph.block` record (and is
    /// charged as a create). Use [`unblock`](Context::unblock) to lift it.
    pub async fn block(&self, did: impl AsRef<str>) -> Result<create_record::Output> {
        let record = block::RecordData {
            created_at: Datetime::now(),
            subject: parse_did(did.as_ref())?,
        };
        self.budget.charge_create().await;
        Ok(record.create(&self.agent).await?)
    }

    /// Unblock an actor (by handle or DID) by deleting the bot's block record.
    ///
    /// Returns `true` if a block existed and was removed, `false` otherwise. The
    /// block record's URI is discovered via `getProfile` (`viewer.blocking`).
    pub async fn unblock(&self, actor: impl AsRef<str>) -> Result<bool> {
        match self
            .viewer_for(actor.as_ref())
            .await?
            .and_then(|v| v.data.blocking)
        {
            Some(uri) => {
                self.delete(uri).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Fetch an actor's viewer-relationship state (the bot's follow/block/mute
    /// relationship to them), or `None` if the profile carries no viewer state.
    ///
    /// A read, so transient failures are retried per the configured retry policy.
    async fn viewer_for(&self, actor: &str) -> Result<Option<ViewerState>> {
        let ident = at_identifier(actor)?;
        retry(&self.retry, || async {
            let out = self
                .agent
                .api
                .app
                .bsky
                .actor
                .get_profile(
                    get_profile::ParametersData {
                        actor: ident.clone(),
                    }
                    .into(),
                )
                .await?;
            Ok(out.data.viewer)
        })
        .await
    }

    // --- deletion ----------------------------------------------------------

    /// Delete one of the bot's own records by AT-URI.
    pub async fn delete(&self, at_uri: impl AsRef<str>) -> Result<delete_record::Output> {
        self.budget.charge_delete().await;
        Ok(self.agent.delete_record(at_uri).await?)
    }

    // --- direct messages (chat.bsky.convo) ---------------------------------

    /// Send a direct message to an actor by DID.
    ///
    /// Resolves (or creates) the one-to-one conversation with `did`, then sends
    /// `text`, auto-detecting rich-text facets (mentions, links, hashtags) just
    /// like [`post`](Context::post). Returns the sent message.
    ///
    /// If you are already handling a message and only want to reply, prefer
    /// [`send_dm_to_convo`](Context::send_dm_to_convo) with the incoming
    /// [`DirectMessage::convo_id`](crate::DirectMessage::convo_id) — it skips the
    /// conversation lookup.
    ///
    /// **Requires an app password with direct-message access**, enabled per
    /// app-password in the Bluesky settings.
    ///
    /// ```no_run
    /// # use bsky_bot_sdk::prelude::*;
    /// # async fn f(ctx: Context) -> Result<()> {
    /// ctx.send_dm("did:plc:somebody000000000000000", "👋 hi from my bot!")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_dm(
        &self,
        did: impl AsRef<str>,
        text: impl AsRef<str>,
    ) -> Result<DirectMessage> {
        let convo_id = self.convo_id_for(did).await?;
        self.send_dm_to_convo(convo_id, text).await
    }

    /// Send a direct message into an existing conversation by id.
    ///
    /// The efficient way to reply from an [`on_message`](crate::BotBuilder::on_message)
    /// handler: pass the [`DirectMessage::convo_id`](crate::DirectMessage::convo_id)
    /// you were handed. Facets are detected automatically. Returns the sent
    /// message.
    pub async fn send_dm_to_convo(
        &self,
        convo_id: impl Into<String>,
        text: impl AsRef<str>,
    ) -> Result<DirectMessage> {
        let convo_id = convo_id.into();
        let message = self.build_message(text).await?;
        self.budget.charge_create().await;
        let output = self
            .agent
            .api_with_proxy(chat_service_did()?, AtprotoServiceType::BskyChat)
            .chat
            .bsky
            .convo
            .send_message(
                send_message::InputData {
                    convo_id: convo_id.clone(),
                    message,
                }
                .into(),
            )
            .await?;
        Ok(DirectMessage::new(output, convo_id))
    }

    /// Resolve the id of the one-to-one conversation with an actor DID, creating
    /// it if it does not yet exist.
    pub async fn convo_id_for(&self, did: impl AsRef<str>) -> Result<String> {
        let did = did.as_ref();
        let did: Did = did
            .parse()
            .map_err(|_| Error::invalid_input(format!("invalid DID: {did}")))?;
        retry(&self.retry, || async {
            let output = self
                .agent
                .api_with_proxy(chat_service_did()?, AtprotoServiceType::BskyChat)
                .chat
                .bsky
                .convo
                .get_convo_for_members(
                    get_convo_for_members::ParametersData {
                        members: vec![did.clone()],
                    }
                    .into(),
                )
                .await?;
            Ok(output.data.convo.id.clone())
        })
        .await
    }

    /// Build a chat message record, auto-detecting mentions/links/tags as facets.
    async fn build_message(&self, text: impl AsRef<str>) -> Result<MessageInput> {
        let rich = RichText::new_with_detect_facets(text).await?;
        Ok(MessageInputData {
            embed: None,
            facets: rich.facets,
            text: rich.text,
        }
        .into())
    }

    /// Set who may open a direct-message conversation with the bot, by publishing
    /// the bot's `chat.bsky.actor.declaration` record.
    ///
    /// The account default blocks people the bot does not follow, so a bot that
    /// should receive DMs from anyone must call this once (or use
    /// [`accept_dms_from`](crate::BotBuilder::accept_dms_from) on the builder, which
    /// applies it on startup). Writing the record is idempotent.
    ///
    /// ```no_run
    /// # use bsky_bot_sdk::prelude::*;
    /// # async fn f(ctx: Context) -> Result<()> {
    /// ctx.set_dm_access(DmAccess::Everyone).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_dm_access(&self, access: DmAccess) -> Result<()> {
        // The declaration is a singleton record keyed by the literal rkey "self".
        let rkey: RecordKey = "self"
            .parse()
            .map_err(|_| Error::invalid_input("invalid record key for chat declaration"))?;
        self.budget.charge_create().await;
        declaration::RecordData {
            allow_incoming: access.as_wire().to_string(),
        }
        .put(&self.agent, rkey)
        .await?;
        Ok(())
    }

    // --- self-labeling -----------------------------------------------------

    /// Declare the bot account **automated** (`automated = true`) or clear that
    /// declaration (`automated = false`) by adding or removing the `bot`
    /// self-label on its profile.
    ///
    /// Bluesky's bot guidelines recommend automated accounts self-label so people
    /// and moderation tooling can recognize them; it's a cheap signal that also
    /// lowers the chance of being mistaken for spam. The label is written into the
    /// account's `app.bsky.actor.profile` record, **preserving** the display name,
    /// description, avatar, and every other self-label already there.
    ///
    /// The write is idempotent and skipped entirely when the profile is already in
    /// the requested state, so it is safe to call on every startup. Prefer the
    /// declarative [`automated_label`](crate::BotBuilder::automated_label) on the
    /// builder for the common "set it once at boot" case.
    ///
    /// ```no_run
    /// # use bsky_bot_sdk::prelude::*;
    /// # async fn f(ctx: Context) -> Result<()> {
    /// ctx.set_automated_label(true).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_automated_label(&self, automated: bool) -> Result<()> {
        let existing = self.fetch_profile_record().await?;
        let currently = existing
            .as_ref()
            .map(|r| has_bot_label(&r.labels))
            .unwrap_or(false);
        // Already in the desired state (including "no profile, and none wanted") —
        // don't rewrite the record or spend a write against the budget.
        if currently == automated {
            return Ok(());
        }

        let mut record = existing.unwrap_or_else(empty_profile);
        record.labels = set_bot_label(record.labels, automated);

        self.budget.charge_create().await;
        record.put(&self.agent, profile_rkey()?).await?;
        Ok(())
    }

    // --- profile edits -----------------------------------------------------

    /// Read-modify-write the bot's own `app.bsky.actor.profile` record.
    ///
    /// Fetches the current profile (or an empty one for a brand-new account),
    /// hands it to `mutate` to change in place, and writes it back — **preserving**
    /// every field `mutate` leaves untouched (display name, avatar, self-labels,
    /// …). This is the primitive behind [`set_display_name`](Context::set_display_name),
    /// [`set_description`](Context::set_description), and
    /// [`set_avatar`](Context::set_avatar); reach for it directly to change several
    /// fields in one write, or to touch a field without a dedicated helper.
    ///
    /// ```no_run
    /// # use bsky_bot_sdk::prelude::*;
    /// # async fn f(ctx: Context) -> Result<()> {
    /// ctx.update_profile(|p| {
    ///     p.display_name = Some("My Bot".into());
    ///     p.description = Some("Beep boop.".into());
    /// })
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_profile<F>(&self, mutate: F) -> Result<()>
    where
        F: FnOnce(&mut profile::RecordData),
    {
        let mut record = self
            .fetch_profile_record()
            .await?
            .unwrap_or_else(empty_profile);
        mutate(&mut record);
        self.budget.charge_create().await;
        record.put(&self.agent, profile_rkey()?).await?;
        Ok(())
    }

    /// Set the bot's profile display name, preserving the rest of the profile.
    pub async fn set_display_name(&self, name: impl Into<String>) -> Result<()> {
        let name = name.into();
        self.update_profile(move |p| p.display_name = Some(name))
            .await
    }

    /// Set the bot's profile description (bio), preserving the rest of the profile.
    pub async fn set_description(&self, description: impl Into<String>) -> Result<()> {
        let description = description.into();
        self.update_profile(move |p| p.description = Some(description))
            .await
    }

    /// Set the bot's profile avatar from raw image bytes, preserving the rest of
    /// the profile. The bytes are uploaded as a blob to the bot's own PDS first.
    pub async fn set_avatar(&self, bytes: impl Into<Vec<u8>>) -> Result<()> {
        let blob = self.upload_blob(bytes).await?;
        self.update_profile(move |p| p.avatar = Some(blob)).await
    }

    // --- reply & quote controls -------------------------------------------

    /// Limit who may reply to one of the bot's posts, by writing an
    /// `app.bsky.feed.threadgate` for it.
    ///
    /// `post_uri` must be an `at://…/app.bsky.feed.post/…` URI of a post the bot
    /// authored. Each [`ReplyGate`] rule widens the allowed audience (the union of
    /// all rules may reply); passing **no** rules closes the thread to everyone but
    /// the bot — the same as [`disable_replies`](Context::disable_replies). Writing
    /// the gate is idempotent (it shares the post's record key), so calling it again
    /// replaces the previous rules.
    ///
    /// ```no_run
    /// # use bsky_bot_sdk::prelude::*;
    /// # async fn f(ctx: Context, post_uri: &str) -> Result<()> {
    /// // Only people the bot follows, or people it @-mentioned, may reply.
    /// ctx.set_reply_gate(post_uri, [ReplyGate::Following, ReplyGate::Mentioned])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_reply_gate(
        &self,
        post_uri: impl AsRef<str>,
        rules: impl IntoIterator<Item = ReplyGate>,
    ) -> Result<put_record::Output> {
        let post_uri = post_uri.as_ref();
        let rkey = rkey_of(post_uri)?;
        let allow: Vec<Union<threadgate::RecordAllowItem>> =
            rules.into_iter().map(|r| r.to_allow_item()).collect();
        let record = threadgate::RecordData {
            allow: Some(allow),
            created_at: Datetime::now(),
            hidden_replies: None,
            post: post_uri.to_string(),
        };
        self.budget.charge_create().await;
        Ok(record.put(&self.agent, rkey).await?)
    }

    /// Close replies on one of the bot's posts entirely (no one but the bot may
    /// reply). Shorthand for [`set_reply_gate`](Context::set_reply_gate) with no
    /// rules.
    pub async fn disable_replies(&self, post_uri: impl AsRef<str>) -> Result<put_record::Output> {
        self.set_reply_gate(post_uri, []).await
    }

    /// Remove the thread-gate from one of the bot's posts, re-opening replies to
    /// everyone. Returns the delete output; deleting an absent gate is a no-op on
    /// the server.
    pub async fn remove_reply_gate(
        &self,
        post_uri: impl AsRef<str>,
    ) -> Result<delete_record::Output> {
        let rkey = rkey_of(post_uri.as_ref())?;
        let uri = format!(
            "at://{}/app.bsky.feed.threadgate/{}",
            self.did(),
            rkey.as_str()
        );
        self.delete(uri).await
    }

    /// Disable quote-posts of one of the bot's posts, by writing an
    /// `app.bsky.feed.postgate` that turns off embedding. Existing quotes are
    /// detached. Use [`allow_quotes`](Context::allow_quotes) to re-enable.
    pub async fn disable_quotes(&self, post_uri: impl AsRef<str>) -> Result<put_record::Output> {
        let post_uri = post_uri.as_ref();
        let rkey = rkey_of(post_uri)?;
        let disable = Union::Refs(postgate::RecordEmbeddingRulesItem::DisableRule(Box::new(
            postgate::DisableRuleData {}.into(),
        )));
        let record = postgate::RecordData {
            created_at: Datetime::now(),
            detached_embedding_uris: None,
            embedding_rules: Some(vec![disable]),
            post: post_uri.to_string(),
        };
        self.budget.charge_create().await;
        Ok(record.put(&self.agent, rkey).await?)
    }

    /// Re-enable quote-posts of one of the bot's posts by removing its post-gate.
    pub async fn allow_quotes(&self, post_uri: impl AsRef<str>) -> Result<delete_record::Output> {
        let rkey = rkey_of(post_uri.as_ref())?;
        let uri = format!(
            "at://{}/app.bsky.feed.postgate/{}",
            self.did(),
            rkey.as_str()
        );
        self.delete(uri).await
    }

    // --- lists -------------------------------------------------------------

    /// Create a curation list and return its `createRecord` output (whose `uri` is
    /// the list's AT-URI, for [`add_to_list`](Context::add_to_list)).
    pub async fn create_list(
        &self,
        name: impl Into<String>,
        description: Option<String>,
    ) -> Result<create_record::Output> {
        let record = list::RecordData {
            avatar: None,
            created_at: Datetime::now(),
            description,
            description_facets: None,
            labels: None,
            name: name.into(),
            purpose: graph_defs::CURATELIST.to_string(),
        };
        self.budget.charge_create().await;
        Ok(record.create(&self.agent).await?)
    }

    /// Add an actor (by DID) to one of the bot's lists (by list AT-URI).
    pub async fn add_to_list(
        &self,
        list_uri: impl Into<String>,
        did: impl AsRef<str>,
    ) -> Result<create_record::Output> {
        let record = listitem::RecordData {
            created_at: Datetime::now(),
            list: list_uri.into(),
            subject: parse_did(did.as_ref())?,
        };
        self.budget.charge_create().await;
        Ok(record.create(&self.agent).await?)
    }

    // --- reads (transparently paginated) -----------------------------------

    /// Stream the bot's home timeline, newest first.
    ///
    /// Returns a [`Paginated`] stream that fetches each page lazily as you consume
    /// it. The timeline is effectively unbounded, so cap it with
    /// [`Paginated::take`] rather than [`Paginated::collect_all`].
    ///
    /// ```no_run
    /// # use bsky_bot_sdk::prelude::*;
    /// # async fn f(ctx: Context) -> Result<()> {
    /// let mut feed = ctx.timeline().take(50);
    /// while let Some(item) = feed.next().await {
    ///     let post = item?;
    ///     println!("{}: {:?}", post.post.author.handle.as_str(), post.post.cid);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn timeline(&self) -> Paginated<FeedViewPost> {
        let ctx = self.clone();
        paginate(move |cursor| {
            let ctx = ctx.clone();
            async move {
                retry(&ctx.retry, || async {
                    let out = ctx
                        .agent
                        .api
                        .app
                        .bsky
                        .feed
                        .get_timeline(
                            get_timeline::ParametersData {
                                algorithm: None,
                                cursor: cursor.clone(),
                                limit: page_limit(),
                            }
                            .into(),
                        )
                        .await?;
                    Ok(Page {
                        items: out.data.feed,
                        cursor: out.data.cursor,
                    })
                })
                .await
            }
        })
    }

    /// Stream an actor's posts (their author feed), newest first, by handle or DID.
    ///
    /// Like [`timeline`](Context::timeline), an author feed can be long; bound it
    /// with [`Paginated::take`].
    pub fn user_posts(&self, actor: impl AsRef<str>) -> Paginated<FeedViewPost> {
        let actor = match at_identifier(actor.as_ref()) {
            Ok(actor) => actor,
            Err(err) => return Paginated::once_err(err),
        };
        let ctx = self.clone();
        paginate(move |cursor| {
            let ctx = ctx.clone();
            let actor = actor.clone();
            async move {
                retry(&ctx.retry, || async {
                    let out = ctx
                        .agent
                        .api
                        .app
                        .bsky
                        .feed
                        .get_author_feed(
                            get_author_feed::ParametersData {
                                actor: actor.clone(),
                                cursor: cursor.clone(),
                                filter: None,
                                include_pins: None,
                                limit: page_limit(),
                            }
                            .into(),
                        )
                        .await?;
                    Ok(Page {
                        items: out.data.feed,
                        cursor: out.data.cursor,
                    })
                })
                .await
            }
        })
    }

    /// Stream the accounts that follow an actor (by handle or DID). Bounded, so
    /// [`Paginated::collect_all`] is safe.
    pub fn followers(&self, actor: impl AsRef<str>) -> Paginated<ProfileView> {
        let actor = match at_identifier(actor.as_ref()) {
            Ok(actor) => actor,
            Err(err) => return Paginated::once_err(err),
        };
        let ctx = self.clone();
        paginate(move |cursor| {
            let ctx = ctx.clone();
            let actor = actor.clone();
            async move {
                retry(&ctx.retry, || async {
                    let out = ctx
                        .agent
                        .api
                        .app
                        .bsky
                        .graph
                        .get_followers(
                            get_followers::ParametersData {
                                actor: actor.clone(),
                                cursor: cursor.clone(),
                                limit: page_limit(),
                            }
                            .into(),
                        )
                        .await?;
                    Ok(Page {
                        items: out.data.followers,
                        cursor: out.data.cursor,
                    })
                })
                .await
            }
        })
    }

    /// Stream the accounts an actor follows (by handle or DID). Bounded, so
    /// [`Paginated::collect_all`] is safe.
    pub fn following(&self, actor: impl AsRef<str>) -> Paginated<ProfileView> {
        let actor = match at_identifier(actor.as_ref()) {
            Ok(actor) => actor,
            Err(err) => return Paginated::once_err(err),
        };
        let ctx = self.clone();
        paginate(move |cursor| {
            let ctx = ctx.clone();
            let actor = actor.clone();
            async move {
                retry(&ctx.retry, || async {
                    let out = ctx
                        .agent
                        .api
                        .app
                        .bsky
                        .graph
                        .get_follows(
                            get_follows::ParametersData {
                                actor: actor.clone(),
                                cursor: cursor.clone(),
                                limit: page_limit(),
                            }
                            .into(),
                        )
                        .await?;
                    Ok(Page {
                        items: out.data.follows,
                        cursor: out.data.cursor,
                    })
                })
                .await
            }
        })
    }

    /// Stream the bot's own followers. Shorthand for [`followers`](Context::followers)
    /// of the bot itself.
    pub fn my_followers(&self) -> Paginated<ProfileView> {
        self.followers(self.did())
    }

    /// Stream the accounts the bot itself follows. Shorthand for
    /// [`following`](Context::following) of the bot itself.
    pub fn my_following(&self) -> Paginated<ProfileView> {
        self.following(self.did())
    }

    /// Fetch the account's own `app.bsky.actor.profile` record, or `None` if it
    /// has none yet. A missing record is distinguished from other errors so a
    /// transport/auth failure never masquerades as "no profile" (which would risk
    /// overwriting a real profile with an empty one).
    async fn fetch_profile_record(&self) -> Result<Option<profile::RecordData>> {
        let collection: Nsid = "app.bsky.actor.profile"
            .parse()
            .map_err(|_| Error::invalid_input("invalid profile collection NSID"))?;
        let repo = self.identity.did_typed().clone();
        // Retry transient failures; a `RecordNotFound` is mapped to `Ok(None)`
        // *inside* the retried closure so it is never retried (it is not
        // transient) and never confused with a real error.
        let output = retry(&self.retry, || async {
            let params = get_record::ParametersData {
                cid: None,
                collection: collection.clone(),
                repo: repo.clone().into(),
                rkey: profile_rkey()?,
            };
            match self
                .agent
                .api
                .com
                .atproto
                .repo
                .get_record(params.into())
                .await
            {
                Ok(output) => Ok(Some(output)),
                Err(err) if is_record_not_found(&err) => Ok(None),
                Err(err) => Err(Error::from(err)),
            }
        })
        .await?;
        let Some(output) = output else {
            return Ok(None);
        };
        let value = serde_json::to_value(&output.data.value)?;
        let record = serde_json::from_value(value)
            .map_err(|e| Error::InvalidRecord(format!("profile record: {e}")))?;
        Ok(Some(record))
    }

    /// Fetch one page of the conversation-event log from `cursor`, used by the
    /// direct-message poll loop. Exposed to the crate's DM runner.
    pub(crate) async fn fetch_convo_log(&self, cursor: Option<String>) -> Result<get_log::Output> {
        retry(&self.retry, || async {
            let output = self
                .agent
                .api_with_proxy(chat_service_did()?, AtprotoServiceType::BskyChat)
                .chat
                .bsky
                .convo
                .get_log(
                    get_log::ParametersData {
                        cursor: cursor.clone(),
                    }
                    .into(),
                )
                .await?;
            Ok(output)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atrium_api::xrpc::error::{ErrorResponseBody, XrpcError};
    use atrium_api::xrpc::http::StatusCode;

    fn xrpc_400(
        error: Option<XrpcErrorKind<get_record::Error>>,
    ) -> atrium_api::xrpc::Error<get_record::Error> {
        atrium_api::xrpc::Error::XrpcResponse(XrpcError {
            status: StatusCode::BAD_REQUEST,
            error,
        })
    }

    #[test]
    fn record_not_found_is_detected_in_the_typed_custom_form() {
        let err = xrpc_400(Some(XrpcErrorKind::Custom(
            get_record::Error::RecordNotFound(Some("could not locate record".into())),
        )));
        assert!(is_record_not_found(&err));
    }

    #[test]
    fn record_not_found_is_detected_in_the_untyped_undefined_form() {
        // Some PDSes return the error un-typed; the name still identifies it.
        let err = xrpc_400(Some(XrpcErrorKind::Undefined(ErrorResponseBody {
            error: Some("RecordNotFound".into()),
            message: Some("could not locate record".into()),
        })));
        assert!(is_record_not_found(&err));
    }

    #[test]
    fn other_server_errors_are_not_treated_as_record_not_found() {
        // The safety-critical case: a transient/auth error must NOT read as
        // "no record", or the caller would overwrite a real profile with an
        // empty one.
        let auth = xrpc_400(Some(XrpcErrorKind::Undefined(ErrorResponseBody {
            error: Some("AuthenticationRequired".into()),
            message: None,
        })));
        assert!(!is_record_not_found(&auth));

        // A response with no structured error body is likewise not not-found.
        let bare = xrpc_400(None);
        assert!(!is_record_not_found(&bare));
    }

    #[test]
    fn rkey_of_extracts_the_final_path_segment() {
        let rkey = rkey_of("at://did:plc:alice000000000000000000/app.bsky.feed.post/3kabc123xyz")
            .expect("valid at-uri");
        assert_eq!(rkey.as_str(), "3kabc123xyz");
    }

    #[test]
    fn rkey_of_rejects_a_uri_with_no_record_key() {
        // Trailing slash → empty final segment.
        assert!(rkey_of("at://did:plc:x/app.bsky.feed.post/").is_err());
        // An outright empty string has no key either.
        assert!(rkey_of("").is_err());
        // A segment containing a space is not a valid record key.
        assert!(rkey_of("at://did:plc:x/coll/has space").is_err());
    }

    #[test]
    fn parse_did_accepts_dids_and_rejects_handles() {
        assert!(parse_did("did:plc:alice000000000000000000").is_ok());
        // A handle is a valid actor identifier but not a DID.
        assert!(parse_did("alice.test").is_err());
        assert!(parse_did("").is_err());
    }

    #[test]
    fn at_identifier_accepts_both_handles_and_dids() {
        assert!(at_identifier("alice.test").is_ok());
        assert!(at_identifier("did:plc:alice000000000000000000").is_ok());
        // Whitespace is never a valid identifier.
        assert!(at_identifier("not valid").is_err());
    }

    async fn store_context(store: Option<Arc<dyn Store>>) -> Context {
        let agent = crate::ratelimit::test_agent().await;
        let identity = Arc::new(BotIdentity::new(
            "did:plc:bot00000000000000000000000"
                .parse()
                .expect("valid did"),
            "bot.test".parse().expect("valid handle"),
        ));
        Context::new(agent, identity, crate::ratelimit::WriteBudget::new(None)).with_store(store)
    }

    #[tokio::test]
    async fn remember_is_remembered_and_forget_use_the_store() {
        let store = crate::store::MemoryStore::new();
        let ctx = store_context(Some(Arc::new(store.clone()))).await;

        assert!(
            !ctx.is_remembered("at://x/1").await.unwrap(),
            "a fresh key is not remembered"
        );
        ctx.remember("at://x/1").await.unwrap();
        assert!(
            ctx.is_remembered("at://x/1").await.unwrap(),
            "after remember, the key is remembered"
        );
        assert_eq!(store.len(), 1, "the write reached the store");

        ctx.forget("at://x/1").await.unwrap();
        assert!(
            !ctx.is_remembered("at://x/1").await.unwrap(),
            "after forget, the key is gone"
        );
    }

    #[tokio::test]
    async fn idempotency_helpers_are_noops_without_a_store() {
        let ctx = store_context(None).await;
        // remember/forget must not error, and is_remembered is always false.
        ctx.remember("k").await.unwrap();
        assert!(
            !ctx.is_remembered("k").await.unwrap(),
            "with no store, nothing is ever remembered (so the action just proceeds)",
        );
        ctx.forget("k").await.unwrap();
        assert!(ctx.store().is_none(), "no store was configured");
    }

    #[test]
    fn reply_gate_maps_each_rule_to_its_lexicon_type() {
        let cases = [
            (ReplyGate::Mentioned, "app.bsky.feed.threadgate#mentionRule"),
            (
                ReplyGate::Following,
                "app.bsky.feed.threadgate#followingRule",
            ),
            (
                ReplyGate::Followers,
                "app.bsky.feed.threadgate#followerRule",
            ),
        ];
        for (gate, expected_type) in cases {
            let value = serde_json::to_value(gate.to_allow_item()).expect("serialize allow item");
            assert_eq!(
                value.get("$type").and_then(|v| v.as_str()),
                Some(expected_type),
            );
        }

        // The list rule additionally carries the list URI.
        let list = ReplyGate::List("at://did:plc:x/app.bsky.graph.list/abc".into());
        let value = serde_json::to_value(list.to_allow_item()).expect("serialize list rule");
        assert_eq!(
            value.get("$type").and_then(|v| v.as_str()),
            Some("app.bsky.feed.threadgate#listRule"),
        );
        assert_eq!(
            value.get("list").and_then(|v| v.as_str()),
            Some("at://did:plc:x/app.bsky.graph.list/abc"),
        );
    }
}
