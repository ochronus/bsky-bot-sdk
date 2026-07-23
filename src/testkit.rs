//! An in-process test harness for unit-testing bot logic without a network.
//!
//! Most bot frameworks make you either hit the real network or hand-roll HTTP
//! mocks to test a handler. This module gives you a real [`Context`] whose XRPC
//! calls are served *in-process* by canned responses — no sockets, no
//! credentials — while every request the handler makes is recorded so you can
//! assert on it. Because the [`Context`] is the exact type your handlers already
//! take, you call them directly:
//!
//! ```
//! use bsky_bot_sdk::prelude::*;
//! use bsky_bot_sdk::testkit::MockBot;
//!
//! // The handler you want to test — the same signature `on_mention` takes.
//! async fn greet(ctx: Context, notif: Notification) -> Result<()> {
//!     ctx.reply_to(&notif, "👋 thanks for the mention!").await?;
//!     Ok(())
//! }
//!
//! # async fn test() {
//! let bot = MockBot::new().await;
//! greet(bot.context(), bot.mention("alice.test", "hey @mockbot"))
//!     .await
//!     .unwrap();
//!
//! // Assert on what the handler did — no network was touched.
//! assert_eq!(bot.posts(), vec!["👋 thanks for the mention!"]);
//! # }
//! ```
//!
//! ## What is and isn't mocked
//!
//! Writes (`createRecord` / `putRecord` / `deleteRecord`), `updateSeen`,
//! `uploadBlob`, and chat `sendMessage` all get canned success responses and are
//! recorded; `getRecord` returns [`RecordNotFound`](MockBot::set_profile_record)
//! by default. One thing escapes the mock: when a post's text contains an
//! **`@mention`**, the SDK resolves that handle to a DID through Bluesky's *public*
//! API on a separate client — a real network call. So keep asserted post text free
//! of live `@mentions` (or use handles that actually resolve); everything else is
//! fully offline.

use std::sync::{Arc, Mutex};

use atrium_api::xrpc::http::{Request, Response};
use serde_json::{Value, json};

use crate::context::{BotIdentity, Context};
use crate::dm::DirectMessage;
use crate::event::Notification;
use crate::ratelimit::{RateLimitClient, ServerRateLimit, WriteBudget};
use crate::stream::{RawStreamEvent, StreamEvent};

/// A placeholder CID used throughout the mock responses. A valid base32 CID so it
/// round-trips through atrium's typed `Cid`.
const FAKE_CID: &str = "bafyreiclp443lavogvhj3d2ob2cxbfuscni2k5jk7bebjzg7khl3esabwq";
const MOCK_DID: &str = "did:plc:mockbot0000000000000000";
const MOCK_HANDLE: &str = "mockbot.test";
const FIXED_TIME: &str = "2026-01-01T00:00:00.000Z";

/// Build a `did:plc:`-shaped identifier from a seed (e.g. a handle), so distinct
/// fixture authors get distinct, valid-looking DIDs.
fn mock_did(seed: &str) -> String {
    let mut s: String = seed.chars().filter(char::is_ascii_alphanumeric).collect();
    s.truncate(24);
    while s.len() < 24 {
        s.push('0');
    }
    format!("did:plc:{}", s.to_lowercase())
}

// ---------------------------------------------------------------------------
// Recorded requests
// ---------------------------------------------------------------------------

/// A single XRPC request the bot made during a test, captured by [`MockBot`].
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    /// The XRPC method NSID, e.g. `com.atproto.repo.createRecord`.
    pub nsid: String,
    /// The HTTP method (`GET` / `POST`).
    pub method: String,
    /// The raw request body (JSON for writes; empty for reads).
    pub body: Vec<u8>,
    /// The URL query string, if any. Reads (`GET`) carry their parameters here,
    /// e.g. `actor=alice.test&limit=100`.
    pub query: Option<String>,
}

impl RecordedRequest {
    /// The request body parsed as JSON, if it is a non-empty JSON body.
    pub fn json(&self) -> Option<Value> {
        serde_json::from_slice(&self.body).ok()
    }

    /// For a `createRecord` / `putRecord`, the `collection` NSID of the record.
    pub fn collection(&self) -> Option<String> {
        self.json()?.get("collection")?.as_str().map(str::to_string)
    }

    /// For a `createRecord` / `putRecord`, the record payload.
    pub fn record(&self) -> Option<Value> {
        self.json()?.get("record").cloned()
    }

    /// If this request created an `app.bsky.feed.post`, its text.
    pub fn post_text(&self) -> Option<String> {
        if self.collection().as_deref() != Some("app.bsky.feed.post") {
            return None;
        }
        self.record()?.get("text")?.as_str().map(str::to_string)
    }

    /// Whether the request's query string contains `key=value` (a read parameter).
    pub fn has_query(&self, key: &str, value: &str) -> bool {
        let needle = format!("{key}={value}");
        self.query
            .as_deref()
            .is_some_and(|q| q.split('&').any(|pair| pair == needle))
    }
}

// ---------------------------------------------------------------------------
// Mock transport
// ---------------------------------------------------------------------------

/// The in-process transport backing [`MockBot`]: it records every request and
/// returns a canned success response, so no socket is ever opened.
pub(crate) struct MockTransport {
    records: Mutex<Vec<RecordedRequest>>,
    /// The `value` returned by `getRecord`; `None` responds `RecordNotFound`.
    get_record: Mutex<Option<Value>>,
    /// The `ProfileViewDetailed` returned by `getProfile`; `None` responds with a
    /// minimal profile carrying no viewer relationship.
    profile_view: Mutex<Option<Value>>,
    /// Canned full responses for read endpoints, keyed by NSID. A read endpoint
    /// without an entry returns an empty (but well-formed) page.
    read_responses: Mutex<std::collections::HashMap<String, Value>>,
}

impl MockTransport {
    pub(crate) fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            get_record: Mutex::new(None),
            profile_view: Mutex::new(None),
            read_responses: Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn set_get_record(&self, value: Option<Value>) {
        *self.get_record.lock().expect("mock lock") = value;
    }

    fn set_profile_view(&self, value: Option<Value>) {
        *self.profile_view.lock().expect("mock lock") = value;
    }

    fn set_read_response(&self, nsid: &str, value: Value) {
        self.read_responses
            .lock()
            .expect("mock lock")
            .insert(nsid.to_string(), value);
    }

    fn read_response(&self, nsid: &str) -> Option<Value> {
        self.read_responses
            .lock()
            .expect("mock lock")
            .get(nsid)
            .cloned()
    }

    fn records(&self) -> Vec<RecordedRequest> {
        self.records.lock().expect("mock lock").clone()
    }

    /// Record the request and return the canned response for its NSID.
    pub(crate) fn respond(&self, request: Request<Vec<u8>>) -> Response<Vec<u8>> {
        let method = request.method().to_string();
        let path = request.uri().path().to_string();
        let query = request.uri().query().map(str::to_string);
        let nsid = path.strip_prefix("/xrpc/").unwrap_or(&path).to_string();
        self.records
            .lock()
            .expect("mock lock")
            .push(RecordedRequest {
                nsid: nsid.clone(),
                method,
                body: request.body().clone(),
                query,
            });

        match nsid.as_str() {
            "com.atproto.server.createSession" => ok(json!({
                "accessJwt": "mock-access-jwt",
                "refreshJwt": "mock-refresh-jwt",
                "handle": MOCK_HANDLE,
                "did": MOCK_DID,
                "active": true,
            })),
            "com.atproto.repo.createRecord" | "com.atproto.repo.putRecord" => ok(json!({
                "cid": FAKE_CID,
                "uri": format!("at://{MOCK_DID}/mock/mock"),
            })),
            "com.atproto.repo.deleteRecord" | "com.atproto.notification.updateSeen" => {
                ok(json!({}))
            }
            "com.atproto.repo.uploadBlob" => ok(json!({
                "blob": {
                    "$type": "blob",
                    "ref": { "$link": FAKE_CID },
                    "mimeType": "application/octet-stream",
                    "size": 1,
                }
            })),
            "com.atproto.repo.getRecord" => {
                match self.get_record.lock().expect("mock lock").clone() {
                    Some(value) => ok(json!({
                        "uri": format!("at://{MOCK_DID}/app.bsky.actor.profile/self"),
                        "cid": FAKE_CID,
                        "value": value,
                    })),
                    None => err(400, "RecordNotFound", "mock: no such record"),
                }
            }
            // Procedures whose lexicon output is unit (`()`): atrium maps these
            // from a *non-JSON* (bytes) response, so an empty body — not `{}` — is
            // what a real server returns and what decodes to `Ok(())`.
            "app.bsky.graph.muteActor" | "app.bsky.graph.unmuteActor" => ok_empty(),
            "app.bsky.actor.getProfile" => {
                match self.profile_view.lock().expect("mock lock").clone() {
                    Some(value) => ok(value),
                    // A minimal, valid `ProfileViewDetailed` with no viewer state,
                    // so `unfollow` / `unblock` see "no relationship".
                    None => ok(json!({ "did": MOCK_DID, "handle": MOCK_HANDLE })),
                }
            }
            // Paginating reads (roadmap #8). Default to a well-formed empty page so
            // a stream terminates cleanly; override with `set_read_response`.
            "app.bsky.feed.getTimeline" | "app.bsky.feed.getAuthorFeed" => ok(self
                .read_response(&nsid)
                .unwrap_or_else(|| json!({ "feed": [] }))),
            "app.bsky.graph.getFollowers" => ok(self
                .read_response(&nsid)
                .unwrap_or_else(|| json!({ "followers": [], "subject": minimal_profile() }))),
            "app.bsky.graph.getFollows" => ok(self
                .read_response(&nsid)
                .unwrap_or_else(|| json!({ "follows": [], "subject": minimal_profile() }))),
            "chat.bsky.convo.sendMessage" => ok(message_view(MOCK_DID, "mock")),
            "chat.bsky.convo.getLog" => ok(json!({ "logs": [] })),
            // A catch-all empty object. Typed reads may not decode from this, but
            // handlers under unit test rarely call arbitrary reads.
            _ => ok(json!({})),
        }
    }
}

/// A 200 response carrying `value` as JSON.
fn ok(value: Value) -> Response<Vec<u8>> {
    Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&value).expect("serialize mock body"))
        .expect("build mock response")
}

/// A 200 response with an empty, non-JSON body — the shape atrium decodes into a
/// unit (`()`) output (e.g. `muteActor`).
fn ok_empty() -> Response<Vec<u8>> {
    Response::builder()
        .status(200)
        .body(Vec::new())
        .expect("build empty mock response")
}

/// A non-2xx response carrying a standard XRPC error body.
fn err(status: u16, error: &str, message: &str) -> Response<Vec<u8>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(
            serde_json::to_vec(&json!({ "error": error, "message": message }))
                .expect("serialize mock error"),
        )
        .expect("build mock error response")
}

/// A minimal, valid `app.bsky.actor.defs#profileView` (just the required fields).
fn minimal_profile() -> Value {
    json!({ "did": MOCK_DID, "handle": MOCK_HANDLE })
}

/// A `chat.bsky.convo.defs#messageView` JSON value.
fn message_view(sender_did: &str, text: &str) -> Value {
    json!({
        "id": "mock-msg",
        "rev": "mock-rev",
        "sender": { "did": sender_did },
        "sentAt": FIXED_TIME,
        "text": text,
    })
}

// ---------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------

/// A test harness: a real, "logged-in" [`Context`] whose network calls are served
/// in-process, plus input fixtures and assertions on what the handler did.
///
/// See the [module docs](crate::testkit) for the end-to-end pattern.
pub struct MockBot {
    context: Context,
    transport: Arc<MockTransport>,
}

impl MockBot {
    /// Create a harness with a ready-to-use [`Context`]. No network, no
    /// credentials: a canned session is installed so action helpers work.
    pub async fn new() -> Self {
        let transport = Arc::new(MockTransport::new());
        let limits = Arc::new(ServerRateLimit::default());
        let agent = bsky_sdk::BskyAgent::builder()
            .client(RateLimitClient::mock(Arc::clone(&transport), limits))
            .build()
            .await
            .expect("build mock agent");
        agent
            .login(MOCK_HANDLE, "mock-app-password")
            .await
            .expect("mock login");
        let session = agent.get_session().await.expect("mock session");
        let identity = Arc::new(BotIdentity::new(
            session.data.did.clone(),
            session.data.handle.clone(),
        ));
        let context = Context::new(agent, identity, WriteBudget::new(None));
        Self { context, transport }
    }

    /// The [`Context`] to hand to your handler. Cheap to clone.
    pub fn context(&self) -> Context {
        self.context.clone()
    }

    /// The mock bot's own handle (the "logged-in" account).
    pub fn handle(&self) -> &str {
        self.context.handle()
    }

    /// Set the record `getRecord` returns (its `value`). By default `getRecord`
    /// responds `RecordNotFound`. Useful for exercising
    /// [`set_automated_label`](Context::set_automated_label) and other
    /// read-modify-write helpers.
    pub fn set_profile_record(&self, value: Value) {
        self.transport.set_get_record(Some(value));
    }

    /// Set the `ProfileViewDetailed` that `getProfile` returns. Use this to give a
    /// fixture a viewer relationship (e.g. `viewer.following` / `viewer.blocking`)
    /// so [`unfollow`](Context::unfollow) / [`unblock`](Context::unblock) find a
    /// record to delete. By default `getProfile` returns a bare profile with no
    /// viewer state (so those helpers report "no relationship").
    pub fn set_profile_view(&self, value: Value) {
        self.transport.set_profile_view(Some(value));
    }

    /// Set the full response a paginating read endpoint returns, by NSID (e.g.
    /// `app.bsky.graph.getFollowers` → `{ "followers": [...], "cursor": ... }`).
    /// Without an override, read endpoints return an empty page so streams end
    /// immediately. Used to exercise [`followers`](Context::followers),
    /// [`timeline`](Context::timeline), and the other paginated reads.
    pub fn set_read_response(&self, nsid: &str, value: Value) {
        self.transport.set_read_response(nsid, value);
    }

    // --- assertions --------------------------------------------------------

    /// Every XRPC request the bot made, in order.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.transport.records()
    }

    /// The requests that created a record via `createRecord`.
    pub fn created(&self) -> Vec<RecordedRequest> {
        self.requests()
            .into_iter()
            .filter(|r| r.nsid == "com.atproto.repo.createRecord")
            .collect()
    }

    /// The records created in a given collection (e.g. `app.bsky.graph.follow`),
    /// as their record JSON.
    pub fn created_in(&self, collection: &str) -> Vec<Value> {
        self.created()
            .into_iter()
            .filter(|r| r.collection().as_deref() == Some(collection))
            .filter_map(|r| r.record())
            .collect()
    }

    /// The text of every `app.bsky.feed.post` the bot created (posts and replies),
    /// in order.
    pub fn posts(&self) -> Vec<String> {
        self.created()
            .iter()
            .filter_map(RecordedRequest::post_text)
            .collect()
    }

    // --- input fixtures ----------------------------------------------------

    /// A mention notification from `author` (a handle) carrying `text`.
    pub fn mention(&self, author: &str, text: &str) -> Notification {
        self.notification("mention", author, post_record(text))
    }

    /// A reply notification from `author` carrying `text`.
    pub fn reply(&self, author: &str, text: &str) -> Notification {
        self.notification("reply", author, post_record(text))
    }

    /// A follow notification from `author`.
    pub fn follow(&self, author: &str) -> Notification {
        self.notification(
            "follow",
            author,
            json!({
                "$type": "app.bsky.graph.follow",
                "subject": MOCK_DID,
                "createdAt": FIXED_TIME,
            }),
        )
    }

    /// A like notification from `author`.
    pub fn like(&self, author: &str) -> Notification {
        self.notification("like", author, post_record("liked post"))
    }

    /// A repost notification from `author`.
    pub fn repost(&self, author: &str) -> Notification {
        self.notification("repost", author, post_record("reposted post"))
    }

    /// A quote-post notification from `author` carrying `text`.
    pub fn quote(&self, author: &str, text: &str) -> Notification {
        self.notification("quote", author, post_record(text))
    }

    /// A direct message from `sender_did` in conversation `convo_id`.
    ///
    /// Reply to it from your handler with
    /// [`ctx.send_dm_to_convo`](Context::send_dm_to_convo); the sent message is
    /// recorded as a `chat.bsky.convo.sendMessage` request.
    pub fn direct_message(&self, sender_did: &str, convo_id: &str, text: &str) -> DirectMessage {
        let view = serde_json::from_value(message_view(sender_did, text))
            .expect("valid message view fixture");
        DirectMessage::new(view, convo_id.to_string())
    }

    /// A network post [`StreamEvent`] (a Jetstream `app.bsky.feed.post` create)
    /// from `did` carrying `text`.
    pub fn stream_post(&self, did: &str, text: &str) -> StreamEvent {
        let raw: RawStreamEvent = serde_json::from_value(json!({
            "did": did,
            "time_us": 1_700_000_000_000_000u64,
            "kind": "commit",
            "commit": {
                "rev": "mock-rev",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "mock",
                "record": post_record(text),
                "cid": FAKE_CID,
            },
        }))
        .expect("valid stream event fixture");
        StreamEvent::from_raw(raw)
    }

    /// Build a notification fixture with the given reason, author, and record.
    fn notification(&self, reason: &str, author: &str, record: Value) -> Notification {
        let did = mock_did(author);
        let value = json!({
            "author": { "did": did, "handle": author },
            "cid": FAKE_CID,
            "indexedAt": FIXED_TIME,
            "isRead": false,
            "reason": reason,
            "record": record,
            "uri": format!("at://{did}/app.bsky.feed.post/mock"),
        });
        Notification::new(serde_json::from_value(value).expect("valid notification fixture"))
    }
}

/// An `app.bsky.feed.post` record JSON with the given text.
fn post_record(text: &str) -> Value {
    json!({
        "$type": "app.bsky.feed.post",
        "text": text,
        "createdAt": FIXED_TIME,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[tokio::test]
    async fn reply_handler_posts_the_reply_and_touches_no_network() {
        // Reads the author to prove it's available, but keeps the *posted* text
        // free of live @mentions (those resolve over the public API).
        async fn greet(ctx: Context, notif: Notification) -> Result<()> {
            assert_eq!(notif.author_handle(), "alice.test");
            ctx.reply_to(&notif, "thanks for the shout-out!").await?;
            Ok(())
        }

        let bot = MockBot::new().await;
        greet(bot.context(), bot.mention("alice.test", "hey @mockbot"))
            .await
            .expect("handler ok");

        assert_eq!(bot.posts(), vec!["thanks for the shout-out!"]);
        // The reply hit exactly one createRecord for a post.
        assert_eq!(bot.created().len(), 1);
        assert_eq!(
            bot.created()[0].collection().as_deref(),
            Some("app.bsky.feed.post")
        );
    }

    #[tokio::test]
    async fn follow_back_creates_a_follow_record() {
        async fn follow_back(ctx: Context, notif: Notification) -> Result<()> {
            ctx.follow_back(&notif).await?;
            Ok(())
        }

        let bot = MockBot::new().await;
        follow_back(bot.context(), bot.follow("bob.test"))
            .await
            .expect("handler ok");

        let follows = bot.created_in("app.bsky.graph.follow");
        assert_eq!(follows.len(), 1, "exactly one follow should be created");
        assert!(
            bot.posts().is_empty(),
            "a follow-back must not create any post"
        );
    }

    #[tokio::test]
    async fn a_handler_that_does_nothing_records_nothing() {
        async fn ignore(_ctx: Context, _n: Notification) -> Result<()> {
            Ok(())
        }
        let bot = MockBot::new().await;
        ignore(bot.context(), bot.mention("carol.test", "hi"))
            .await
            .expect("handler ok");
        assert!(
            bot.created().is_empty(),
            "a no-op handler must make no createRecord calls"
        );
    }

    #[tokio::test]
    async fn dm_reply_is_recorded_as_send_message() {
        async fn echo(ctx: Context, dm: DirectMessage) -> Result<()> {
            ctx.send_dm_to_convo(dm.convo_id(), format!("echo: {}", dm.text()))
                .await?;
            Ok(())
        }
        let bot = MockBot::new().await;
        echo(
            bot.context(),
            bot.direct_message("did:plc:sender000000000000000000", "convo-1", "hello"),
        )
        .await
        .expect("handler ok");

        let sends: Vec<_> = bot
            .requests()
            .into_iter()
            .filter(|r| r.nsid == "chat.bsky.convo.sendMessage")
            .collect();
        assert_eq!(sends.len(), 1, "one message should be sent");
        let sent_text = sends[0].json().unwrap()["message"]["text"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(sent_text, "echo: hello");
    }

    #[tokio::test]
    async fn stream_post_fixture_exposes_text() {
        let bot = MockBot::new().await;
        let event = bot.stream_post("did:plc:author0000000000000000000", "rust is great");
        assert_eq!(event.text().as_deref(), Some("rust is great"));
    }

    // --- write-action wire shapes (roadmap #9) -----------------------------

    /// Every `putRecord` request the bot made, as parsed JSON bodies.
    fn puts(bot: &MockBot) -> Vec<Value> {
        bot.requests()
            .into_iter()
            .filter(|r| r.nsid == "com.atproto.repo.putRecord")
            .filter_map(|r| r.json())
            .collect()
    }

    #[tokio::test]
    async fn block_creates_a_public_block_record() {
        let bot = MockBot::new().await;
        let did = "did:plc:troll000000000000000000000";
        bot.context().block(did).await.expect("block ok");

        let blocks = bot.created_in("app.bsky.graph.block");
        assert_eq!(blocks.len(), 1, "exactly one block record");
        assert_eq!(blocks[0].get("subject").and_then(Value::as_str), Some(did));
    }

    #[tokio::test]
    async fn mute_calls_the_procedure_and_writes_no_record() {
        let bot = MockBot::new().await;
        bot.context().mute("alice.test").await.expect("mute ok");

        let mutes: Vec<_> = bot
            .requests()
            .into_iter()
            .filter(|r| r.nsid == "app.bsky.graph.muteActor")
            .collect();
        assert_eq!(mutes.len(), 1, "one muteActor procedure call");
        assert_eq!(
            mutes[0].json().unwrap()["actor"].as_str(),
            Some("alice.test")
        );
        assert!(
            bot.created().is_empty(),
            "a mute is a preference, not a repo record"
        );
    }

    #[tokio::test]
    async fn unfollow_deletes_the_follow_record_when_following() {
        let bot = MockBot::new().await;
        let follow_uri = format!("at://{}/app.bsky.graph.follow/abc", bot.context().did());
        bot.set_profile_view(json!({
            "did": "did:plc:friend0000000000000000000",
            "handle": "friend.test",
            "viewer": { "following": follow_uri },
        }));

        let removed = bot.context().unfollow("friend.test").await.expect("ok");
        assert!(removed, "an existing follow should be reported removed");

        let deletes: Vec<_> = bot
            .requests()
            .into_iter()
            .filter(|r| r.nsid == "com.atproto.repo.deleteRecord")
            .collect();
        assert_eq!(deletes.len(), 1, "the follow record is deleted");
        assert_eq!(
            deletes[0].json().unwrap()["collection"].as_str(),
            Some("app.bsky.graph.follow"),
        );
    }

    #[tokio::test]
    async fn unfollow_is_a_noop_when_not_following() {
        // The default profile view carries no viewer relationship.
        let bot = MockBot::new().await;
        let removed = bot.context().unfollow("stranger.test").await.expect("ok");
        assert!(!removed, "not following → nothing to remove");
        assert!(
            bot.requests()
                .iter()
                .all(|r| r.nsid != "com.atproto.repo.deleteRecord"),
            "must not delete anything when not following",
        );
    }

    #[tokio::test]
    async fn set_reply_gate_puts_a_threadgate_sharing_the_post_rkey() {
        use crate::context::ReplyGate;
        let bot = MockBot::new().await;
        let post_uri = format!("at://{}/app.bsky.feed.post/xyz", bot.context().did());
        bot.context()
            .set_reply_gate(&post_uri, [ReplyGate::Following, ReplyGate::Mentioned])
            .await
            .expect("gate ok");

        let puts = puts(&bot);
        assert_eq!(puts.len(), 1);
        assert_eq!(
            puts[0]["collection"].as_str(),
            Some("app.bsky.feed.threadgate")
        );
        assert_eq!(
            puts[0]["rkey"].as_str(),
            Some("xyz"),
            "a thread-gate shares the record key of the post it governs",
        );
        let types: Vec<&str> = puts[0]["record"]["allow"]
            .as_array()
            .expect("allow list")
            .iter()
            .filter_map(|a| a["$type"].as_str())
            .collect();
        assert!(types.contains(&"app.bsky.feed.threadgate#followingRule"));
        assert!(types.contains(&"app.bsky.feed.threadgate#mentionRule"));
    }

    #[tokio::test]
    async fn disable_replies_writes_an_empty_allow_list() {
        let bot = MockBot::new().await;
        let post_uri = format!("at://{}/app.bsky.feed.post/xyz", bot.context().did());
        bot.context().disable_replies(&post_uri).await.expect("ok");

        let puts = puts(&bot);
        assert_eq!(puts.len(), 1);
        assert!(
            puts[0]["record"]["allow"]
                .as_array()
                .expect("allow list")
                .is_empty(),
            "an empty allow list closes the thread to everyone but the author",
        );
    }

    #[tokio::test]
    async fn set_display_name_preserves_the_existing_bio() {
        let bot = MockBot::new().await;
        // A profile already exists with a description that must survive the edit.
        bot.set_profile_record(json!({
            "$type": "app.bsky.actor.profile",
            "description": "keep me",
        }));
        bot.context()
            .set_display_name("New Name")
            .await
            .expect("ok");

        let profile_puts: Vec<_> = puts(&bot)
            .into_iter()
            .filter(|p| p["collection"].as_str() == Some("app.bsky.actor.profile"))
            .collect();
        assert_eq!(profile_puts.len(), 1);
        let rec = &profile_puts[0]["record"];
        assert_eq!(rec["displayName"].as_str(), Some("New Name"));
        assert_eq!(
            rec["description"].as_str(),
            Some("keep me"),
            "read-modify-write must not drop the existing bio",
        );
    }

    #[tokio::test]
    async fn create_list_then_add_member_writes_list_and_listitem() {
        let bot = MockBot::new().await;
        bot.context()
            .create_list("My List", Some("a list".into()))
            .await
            .expect("list ok");
        let lists = bot.created_in("app.bsky.graph.list");
        assert_eq!(lists.len(), 1);
        assert_eq!(lists[0]["name"].as_str(), Some("My List"));
        assert_eq!(
            lists[0]["purpose"].as_str(),
            Some("app.bsky.graph.defs#curatelist"),
        );

        let list_uri = format!("at://{}/app.bsky.graph.list/l1", bot.context().did());
        let member = "did:plc:member0000000000000000000";
        bot.context()
            .add_to_list(&list_uri, member)
            .await
            .expect("add ok");
        let items = bot.created_in("app.bsky.graph.listitem");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["subject"].as_str(), Some(member));
        assert_eq!(items[0]["list"].as_str(), Some(list_uri.as_str()));
    }

    // --- paginated reads (roadmap #8) --------------------------------------

    #[tokio::test]
    async fn followers_flattens_a_page_and_sends_the_actor() {
        let bot = MockBot::new().await;
        bot.set_read_response(
            "app.bsky.graph.getFollowers",
            json!({
                "subject": { "did": "did:plc:target0000000000000000000", "handle": "target.test" },
                "followers": [
                    { "did": "did:plc:a0000000000000000000000000", "handle": "a.test" },
                    { "did": "did:plc:b0000000000000000000000000", "handle": "b.test" },
                ],
            }),
        );

        let list = bot
            .context()
            .followers("target.test")
            .collect_all()
            .await
            .expect("followers ok");
        assert_eq!(list.len(), 2, "both followers on the page are yielded");
        assert_eq!(list[0].handle.as_str(), "a.test");
        assert_eq!(list[1].handle.as_str(), "b.test");

        let reqs: Vec<_> = bot
            .requests()
            .into_iter()
            .filter(|r| r.nsid == "app.bsky.graph.getFollowers")
            .collect();
        assert_eq!(reqs.len(), 1, "one page fetched (no cursor for a second)");
        assert!(
            reqs[0].has_query("actor", "target.test"),
            "the read must carry the requested actor: {:?}",
            reqs[0].query,
        );
    }

    #[tokio::test]
    async fn timeline_with_an_empty_feed_yields_nothing() {
        let bot = MockBot::new().await;
        let posts = bot
            .context()
            .timeline()
            .take(10)
            .collect_all()
            .await
            .expect("timeline ok");
        assert!(posts.is_empty(), "an empty feed yields no items");
        assert!(
            bot.requests()
                .iter()
                .any(|r| r.nsid == "app.bsky.feed.getTimeline"),
            "the timeline endpoint was actually queried",
        );
    }

    #[tokio::test]
    async fn user_posts_with_an_invalid_actor_errors_without_a_network_call() {
        let bot = MockBot::new().await;
        let mut stream = bot.context().user_posts("not a valid actor");
        let first = stream.next().await.expect("one item");
        assert!(
            matches!(first, Err(crate::error::Error::InvalidInput(_))),
            "an unparseable actor surfaces as an error, not a panic",
        );
        assert!(
            bot.requests()
                .iter()
                .all(|r| r.nsid != "app.bsky.feed.getAuthorFeed"),
            "a bad actor must not reach the network",
        );
    }
}
