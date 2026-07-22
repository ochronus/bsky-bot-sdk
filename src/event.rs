//! Notification reasons and a convenience wrapper over the raw AT Protocol
//! notification type.

use atrium_api::app::bsky::feed::post;
use atrium_api::app::bsky::notification::list_notifications;
use atrium_api::com::atproto::repo::strong_ref;
use atrium_api::types::string::{Cid, Datetime};

/// The raw notification type returned by `app.bsky.notification.listNotifications`.
pub type RawNotification = list_notifications::Notification;

/// Why a notification was delivered.
///
/// Mirrors the string `reason` field of a Bluesky notification, but as a typed
/// enum so handlers can match on it exhaustively. Unknown / future reasons are
/// preserved via [`NotificationReason::Other`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum NotificationReason {
    /// Someone liked one of your posts.
    Like,
    /// Someone reposted one of your posts.
    Repost,
    /// Someone followed you.
    Follow,
    /// Someone mentioned you in a post.
    Mention,
    /// Someone replied to one of your posts.
    Reply,
    /// Someone quote-posted one of your posts.
    Quote,
    /// Someone joined via one of your starter packs.
    StarterpackJoined,
    /// Your account was verified by a trusted verifier.
    Verified,
    /// A verification of your account was removed.
    Unverified,
    /// Any other / future reason, carrying the raw string.
    Other(String),
}

impl NotificationReason {
    /// Parse the wire `reason` string into a [`NotificationReason`].
    pub fn from_reason(reason: &str) -> Self {
        match reason {
            "like" => Self::Like,
            "repost" => Self::Repost,
            "follow" => Self::Follow,
            "mention" => Self::Mention,
            "reply" => Self::Reply,
            "quote" => Self::Quote,
            "starterpack-joined" => Self::StarterpackJoined,
            "verified" => Self::Verified,
            "unverified" => Self::Unverified,
            other => Self::Other(other.to_string()),
        }
    }

    /// The wire `reason` string for this reason.
    pub fn as_reason(&self) -> &str {
        match self {
            Self::Like => "like",
            Self::Repost => "repost",
            Self::Follow => "follow",
            Self::Mention => "mention",
            Self::Reply => "reply",
            Self::Quote => "quote",
            Self::StarterpackJoined => "starterpack-joined",
            Self::Verified => "verified",
            Self::Unverified => "unverified",
            Self::Other(s) => s.as_str(),
        }
    }
}

impl core::fmt::Display for NotificationReason {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_reason())
    }
}

/// An ergonomic wrapper around a single Bluesky notification.
///
/// It derefs-through accessors expose the most commonly needed fields (author,
/// reason, subject `uri`/`cid`, text) without forcing callers to navigate the
/// deeply-nested generated types. The underlying value is always available via
/// [`Notification::raw`].
#[derive(Debug, Clone)]
pub struct Notification {
    inner: RawNotification,
}

impl Notification {
    /// Wrap a raw notification.
    pub fn new(inner: RawNotification) -> Self {
        Self { inner }
    }

    /// Borrow the underlying raw notification.
    pub fn raw(&self) -> &RawNotification {
        &self.inner
    }

    /// Consume the wrapper, returning the raw notification.
    pub fn into_raw(self) -> RawNotification {
        self.inner
    }

    /// The typed reason this notification was delivered.
    pub fn reason(&self) -> NotificationReason {
        NotificationReason::from_reason(&self.inner.reason)
    }

    /// The profile of the actor who triggered this notification.
    pub fn author(&self) -> &atrium_api::app::bsky::actor::defs::ProfileView {
        &self.inner.author
    }

    /// The DID of the actor who triggered this notification.
    pub fn author_did(&self) -> &str {
        self.inner.author.did.as_str()
    }

    /// The handle of the actor who triggered this notification.
    pub fn author_handle(&self) -> &str {
        self.inner.author.handle.as_str()
    }

    /// The AT-URI of the record that generated this notification (e.g. the post
    /// that mentioned you, or the like/follow record).
    pub fn uri(&self) -> &str {
        &self.inner.uri
    }

    /// The CID of the record that generated this notification.
    pub fn cid(&self) -> &Cid {
        &self.inner.cid
    }

    /// When the record was indexed. Used for chronological ordering / dedup.
    pub fn indexed_at(&self) -> &Datetime {
        &self.inner.indexed_at
    }

    /// Whether the notification has already been marked read on the server.
    pub fn is_read(&self) -> bool {
        self.inner.is_read
    }

    /// For `like`/`repost`/`reply`/`quote` notifications, the AT-URI of the subject
    /// post this notification is *about* (usually one of your own posts).
    pub fn reason_subject(&self) -> Option<&str> {
        self.inner.reason_subject.as_deref()
    }

    /// A [`strong_ref`] (`uri` + `cid`) pointing at the record that generated this
    /// notification — the thing you would like, repost, or reply to.
    pub fn subject_ref(&self) -> strong_ref::Main {
        strong_ref::MainData {
            cid: self.inner.cid.clone(),
            uri: self.inner.uri.clone(),
        }
        .into()
    }

    /// Attempt to decode the notification's record as an `app.bsky.feed.post`.
    ///
    /// Returns `None` for notifications whose record is not a post (e.g. a
    /// `follow` or `like`). This is fallible-by-design: it never panics on a
    /// type mismatch.
    pub fn as_post(&self) -> Option<post::RecordData> {
        let value = serde_json::to_value(&self.inner.record).ok()?;
        serde_json::from_value(value).ok()
    }

    /// The plain text of the notification's record, if it is a post.
    pub fn text(&self) -> Option<String> {
        self.as_post().map(|p| p.text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notification_json(reason: &str, record: serde_json::Value) -> RawNotification {
        let value = serde_json::json!({
            "author": { "did": "did:plc:alice000000000000000000", "handle": "alice.test" },
            "cid": "bafyreiclp443lavogvhj3d2ob2cxbfuscni2k5jk7bebjzg7khl3esabwq",
            "indexedAt": "2026-07-22T10:00:00.000Z",
            "isRead": false,
            "reason": reason,
            "record": record,
            "uri": "at://did:plc:alice000000000000000000/app.bsky.feed.post/abc123",
        });
        serde_json::from_value(value).expect("valid notification fixture")
    }

    fn post_record(text: &str) -> serde_json::Value {
        serde_json::json!({
            "$type": "app.bsky.feed.post",
            "text": text,
            "createdAt": "2026-07-22T10:00:00.000Z",
        })
    }

    #[test]
    fn reason_round_trips_known_values() {
        for reason in ["like", "repost", "follow", "mention", "reply", "quote"] {
            let parsed = NotificationReason::from_reason(reason);
            assert_eq!(parsed.as_reason(), reason, "round-trip failed for {reason}");
        }
    }

    #[test]
    fn reason_preserves_unknown_values() {
        let parsed = NotificationReason::from_reason("something-new");
        assert_eq!(parsed, NotificationReason::Other("something-new".into()));
        assert_eq!(parsed.as_reason(), "something-new");
    }

    #[test]
    fn accessors_expose_author_and_reason() {
        let notif = Notification::new(notification_json("mention", post_record("hi @bot.test")));
        assert_eq!(notif.reason(), NotificationReason::Mention);
        assert_eq!(notif.author_did(), "did:plc:alice000000000000000000");
        assert_eq!(notif.author_handle(), "alice.test");
        assert!(!notif.is_read());
        assert_eq!(
            notif.uri(),
            "at://did:plc:alice000000000000000000/app.bsky.feed.post/abc123"
        );
    }

    #[test]
    fn subject_ref_carries_uri_and_cid() {
        let notif = Notification::new(notification_json("reply", post_record("a reply")));
        let subject = notif.subject_ref();
        assert_eq!(subject.uri, notif.uri());
        assert_eq!(&subject.cid, notif.cid());
    }

    #[test]
    fn as_post_decodes_a_post_record() {
        let notif = Notification::new(notification_json("mention", post_record("hello world")));
        let post = notif.as_post().expect("record is a post");
        assert_eq!(post.text, "hello world");
        assert_eq!(notif.text().as_deref(), Some("hello world"));
    }

    #[test]
    fn as_post_returns_none_for_non_post_records() {
        // A follow record is not a post; decoding must fail gracefully, not panic.
        let follow = serde_json::json!({
            "$type": "app.bsky.graph.follow",
            "subject": "did:plc:bob0000000000000000000000",
            "createdAt": "2026-07-22T10:00:00.000Z",
        });
        let notif = Notification::new(notification_json("follow", follow));
        assert!(notif.as_post().is_none());
        assert!(notif.text().is_none());
    }
}
