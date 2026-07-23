//! The [`Context`] handed to every handler: a cheap-to-clone handle bundling the
//! authenticated agent, the bot's own identity, and ready-made action helpers.

use std::sync::Arc;

use atrium_api::agent::AtprotoServiceType;
use atrium_api::agent::bluesky::BSKY_CHAT_DID;
use atrium_api::app::bsky::actor::profile;
use atrium_api::app::bsky::feed::{like, post, repost};
use atrium_api::app::bsky::graph::follow;
use atrium_api::chat::bsky::actor::declaration;
use atrium_api::chat::bsky::convo::defs::{MessageInput, MessageInputData};
use atrium_api::chat::bsky::convo::{get_convo_for_members, get_log, send_message};
use atrium_api::com::atproto::repo::{create_record, delete_record, get_record, strong_ref};
use atrium_api::types::BlobRef;
use atrium_api::types::string::{Datetime, Did, Handle, Nsid, RecordKey};
use atrium_api::xrpc::error::XrpcErrorKind;
use bsky_sdk::BskyAgent;
use bsky_sdk::record::Record;
use bsky_sdk::rich_text::RichText;

use crate::dm::{DirectMessage, DmAccess};
use crate::embed::PostBuilder;
use crate::error::{Error, Result};
use crate::event::Notification;
use crate::ratelimit::WriteBudget;
use crate::self_label::{has_bot_label, set_bot_label};
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

    /// Fetch the account's own `app.bsky.actor.profile` record, or `None` if it
    /// has none yet. A missing record is distinguished from other errors so a
    /// transport/auth failure never masquerades as "no profile" (which would risk
    /// overwriting a real profile with an empty one).
    async fn fetch_profile_record(&self) -> Result<Option<profile::RecordData>> {
        let collection: Nsid = "app.bsky.actor.profile"
            .parse()
            .map_err(|_| Error::invalid_input("invalid profile collection NSID"))?;
        let params = get_record::ParametersData {
            cid: None,
            collection,
            repo: self.identity.did_typed().clone().into(),
            rkey: profile_rkey()?,
        };
        let output = match self
            .agent
            .api
            .com
            .atproto
            .repo
            .get_record(params.into())
            .await
        {
            Ok(output) => output,
            Err(err) if is_record_not_found(&err) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let value = serde_json::to_value(&output.data.value)?;
        let record = serde_json::from_value(value)
            .map_err(|e| Error::InvalidRecord(format!("profile record: {e}")))?;
        Ok(Some(record))
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
}
