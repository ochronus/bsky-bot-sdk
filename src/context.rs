//! The [`Context`] handed to every handler: a cheap-to-clone handle bundling the
//! authenticated agent, the bot's own identity, and ready-made action helpers.

use std::sync::Arc;

use atrium_api::agent::AtprotoServiceType;
use atrium_api::agent::bluesky::BSKY_CHAT_DID;
use atrium_api::app::bsky::feed::{like, post, repost};
use atrium_api::app::bsky::graph::follow;
use atrium_api::chat::bsky::actor::declaration;
use atrium_api::chat::bsky::convo::defs::{MessageInput, MessageInputData};
use atrium_api::chat::bsky::convo::{get_convo_for_members, get_log, send_message};
use atrium_api::com::atproto::repo::{create_record, delete_record, strong_ref};
use atrium_api::types::BlobRef;
use atrium_api::types::string::{Datetime, Did, Handle, RecordKey};
use bsky_sdk::BskyAgent;
use bsky_sdk::record::Record;
use bsky_sdk::rich_text::RichText;

use crate::dm::{DirectMessage, DmAccess};
use crate::embed::PostBuilder;
use crate::error::{Error, Result};
use crate::event::Notification;
use crate::ratelimit::WriteBudget;
use crate::thread::ThreadBuilder;

/// The DID of the Bluesky chat service, reached via the `atproto-proxy` header.
/// Parsed from the `atrium-api` constant; the value is a fixed, valid DID.
fn chat_service_did() -> Result<Did> {
    BSKY_CHAT_DID
        .parse()
        .map_err(|_| Error::invalid_input(format!("invalid chat service DID: {BSKY_CHAT_DID}")))
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
    agent: BskyAgent,
    identity: Arc<BotIdentity>,
    budget: WriteBudget,
}

impl Context {
    pub(crate) fn new(agent: BskyAgent, identity: Arc<BotIdentity>, budget: WriteBudget) -> Self {
        Self {
            agent,
            identity,
            budget,
        }
    }

    /// The authenticated agent, for calls not covered by the helpers below.
    pub fn agent(&self) -> &BskyAgent {
        &self.agent
    }

    /// The bot's own identity (DID + handle).
    pub fn me(&self) -> &BotIdentity {
        &self.identity
    }

    /// The bot's DID.
    pub fn did(&self) -> &str {
        self.identity.did()
    }

    /// The bot's handle.
    pub fn handle(&self) -> &str {
        self.identity.handle()
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
        let did = did.as_ref();
        let did: Did = did
            .parse()
            .map_err(|_| Error::invalid_input(format!("invalid DID: {did}")))?;
        self.follow_did(did).await
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
        let output = self
            .agent
            .api_with_proxy(chat_service_did()?, AtprotoServiceType::BskyChat)
            .chat
            .bsky
            .convo
            .get_convo_for_members(
                get_convo_for_members::ParametersData { members: vec![did] }.into(),
            )
            .await?;
        Ok(output.data.convo.id.clone())
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

    /// Fetch one page of the conversation-event log from `cursor`, used by the
    /// direct-message poll loop. Exposed to the crate's DM runner.
    pub(crate) async fn fetch_convo_log(&self, cursor: Option<String>) -> Result<get_log::Output> {
        let output = self
            .agent
            .api_with_proxy(chat_service_did()?, AtprotoServiceType::BskyChat)
            .chat
            .bsky
            .convo
            .get_log(get_log::ParametersData { cursor }.into())
            .await?;
        Ok(output)
    }
}
