//! The [`Context`] handed to every handler: a cheap-to-clone handle bundling the
//! authenticated agent, the bot's own identity, and ready-made action helpers.

use std::sync::Arc;

use atrium_api::app::bsky::feed::{like, post, repost};
use atrium_api::app::bsky::graph::follow;
use atrium_api::com::atproto::repo::{create_record, delete_record, strong_ref};
use atrium_api::types::string::{Datetime, Did, Handle};
use bsky_sdk::BskyAgent;
use bsky_sdk::record::Record;
use bsky_sdk::rich_text::RichText;

use crate::error::{Error, Result};
use crate::event::Notification;
use crate::ratelimit::WriteBudget;

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
    async fn build_post(
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
}
