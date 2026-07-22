//! Convenient glob-import of the crate's most-used types.
//!
//! ```
//! use bsky_bot_sdk::prelude::*;
//! ```

pub use crate::{
    Bot, BotBuilder, BotConfig, BotIdentity, Context, Dedup, Error, Notification,
    NotificationReason, RateLimitConfig, Result,
};
