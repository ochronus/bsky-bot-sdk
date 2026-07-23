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
}

impl MockTransport {
    pub(crate) fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            get_record: Mutex::new(None),
        }
    }

    fn set_get_record(&self, value: Option<Value>) {
        *self.get_record.lock().expect("mock lock") = value;
    }

    fn records(&self) -> Vec<RecordedRequest> {
        self.records.lock().expect("mock lock").clone()
    }

    /// Record the request and return the canned response for its NSID.
    pub(crate) fn respond(&self, request: Request<Vec<u8>>) -> Response<Vec<u8>> {
        let method = request.method().to_string();
        let path = request.uri().path().to_string();
        let nsid = path.strip_prefix("/xrpc/").unwrap_or(&path).to_string();
        self.records
            .lock()
            .expect("mock lock")
            .push(RecordedRequest {
                nsid: nsid.clone(),
                method,
                body: request.body().clone(),
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
}
