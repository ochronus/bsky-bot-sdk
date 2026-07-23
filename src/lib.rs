//! # bsky-bot-sdk
//!
//! An ergonomic, event-driven SDK for building [Bluesky](https://bsky.app)
//! (AT Protocol) bots in Rust. It sits on top of atrium's
//! [`bsky-sdk`](https://crates.io/crates/bsky-sdk), adding the pieces a bot needs
//! that the low-level client leaves to you:
//!
//! - **A notification event loop** that polls `listNotifications`, de-duplicates
//!   across restarts, and dispatches each event to your handlers.
//! - **Real-time network ingestion** via the
//!   [Jetstream](https://docs.bsky.app/blog/jetstream) firehose — react to the
//!   *whole network* with [`on_keyword`](BotBuilder::on_keyword),
//!   [`on_hashtag`](BotBuilder::on_hashtag), or
//!   [`on_firehose`](BotBuilder::on_firehose), not just your own notifications.
//! - **Typed events** — match on [`NotificationReason::Mention`],
//!   [`NotificationReason::Follow`], … instead of stringly-typed reasons.
//! - **Action helpers** on [`Context`] — [`reply_to`](Context::reply_to),
//!   [`like`](Context::like), [`repost`](Context::repost),
//!   [`follow_back`](Context::follow_back), [`post`](Context::post) — with
//!   automatic rich-text facet detection (mentions, links, hashtags).
//! - **Media & embeds** via [`ctx.compose()`](Context::compose) — a
//!   [`PostBuilder`] for images (with **required** alt text), video, external
//!   link cards (auto-fetched OpenGraph), and quote posts, all uploaded to the
//!   bot's own PDS so they work on any server.
//! - **Session persistence** so restarts resume instead of re-authenticating.
//! - **Client-side rate limiting** that respects Bluesky's points-based write
//!   budget.
//! - **Scheduling** — run actions on an interval or a cron schedule (see
//!   [`Schedule`] and [`BotBuilder::every`]/[`BotBuilder::cron`]).
//! - **Graceful shutdown** on `Ctrl-C` or any future you provide.
//!
//! ## Quick start
//!
//! ```no_run
//! use bsky_bot_sdk::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     Bot::builder()
//!         .credentials("mybot.bsky.social", "xxxx-xxxx-xxxx-xxxx")
//!         .session_file("session.json")
//!         .on_mention(|ctx, notif| async move {
//!             let who = notif.author_handle().to_string();
//!             ctx.reply_to(&notif, format!("👋 hi @{who}!")).await?;
//!             Ok(())
//!         })
//!         .on_follow(|ctx, notif| async move {
//!             ctx.follow_back(&notif).await?;
//!             Ok(())
//!         })
//!         .build()
//!         .await?
//!         .run()
//!         .await
//! }
//! ```
//!
//! Handlers are async closures of the form
//! `Fn(Context, Notification) -> impl Future<Output = Result<()>>`. Register as
//! many as you like; a handler returning `Err` is routed to your
//! [`on_error`](BotBuilder::on_error) handler (or logged) and never aborts the
//! loop.
//!
//! ## Re-exports
//!
//! The underlying [`bsky_sdk`] and [`atrium_api`] crates are re-exported so you
//! can reach for lower-level types (custom records, embeds, XRPC calls) without a
//! separate dependency or a version-mismatch risk.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod bot;
mod config;
mod context;
mod dedup;
mod embed;
mod error;
mod event;
mod handler;
mod ratelimit;
mod schedule;
mod stream;

pub mod prelude;

pub use bot::{Bot, BotBuilder};
pub use config::{BotConfig, DEFAULT_SERVICE};
pub use context::{BotIdentity, Context};
pub use dedup::Dedup;
pub use embed::{MAX_IMAGES, PostBuilder};
pub use error::{Error, Result};
pub use event::{Notification, NotificationReason, RawNotification};
pub use handler::BoxFuture;
pub use ratelimit::{RateLimitConfig, RateLimiter};
pub use schedule::{Schedule, Tz};
pub use stream::{
    Backoff, CommitOp, DEFAULT_JETSTREAM_ENDPOINT, JetstreamConfig, RawCommit, RawStreamEvent,
    StreamEvent, StreamKind,
};

// Re-export the underlying crates for advanced use and to guarantee a single,
// consistent version of the AT Protocol types across your app.
pub use atrium_api;
pub use bsky_sdk;
