//! Convenient glob-import of the crate's most-used types.
//!
//! ```
//! use bsky_bot_sdk::prelude::*;
//! ```

pub use crate::{
    Bot, BotBuilder, BotConfig, BotIdentity, CommitOp, Context, Dedup, Error, JetstreamConfig,
    Notification, NotificationReason, PostBuilder, RateLimitConfig, Result, Schedule, StreamEvent,
    StreamKind, Tz,
};
