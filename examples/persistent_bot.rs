//! Persistence: resume across restarts and greet each person exactly once.
//!
//! Attaching a [`Store`] (here a [`FileStore`] JSON file) gives the bot two things:
//!
//! 1. **Watermark resume** — on restart the bot continues from the last processed
//!    notification instead of skipping whatever backlog exists at startup, so no
//!    mentions are missed across downtime. This is automatic once a store is set.
//! 2. **Idempotency** — `is_remembered` / `remember` let a handler act at most once
//!    per key, surviving restarts. Here the bot greets each account only the first
//!    time it mentions the bot, even across process restarts.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example persistent_bot
//! ```

use bsky_bot_sdk::FileStore;
use bsky_bot_sdk::prelude::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,bsky_bot_sdk=debug")),
        )
        .init();

    Bot::builder()
        .from_env()?
        .session_file("session.json")
        // The watermark and idempotency set persist to this JSON file.
        .store(FileStore::new("bot-state.json")?)
        .on_mention(|ctx, notif| async move {
            let key = format!("greeted:{}", notif.author_did());
            if ctx.is_remembered(&key).await? {
                tracing::debug!(who = notif.author_handle(), "already greeted; skipping");
                return Ok(());
            }
            ctx.reply_to(&notif, "👋 nice to meet you! (I only say this once.)")
                .await?;
            ctx.remember(&key).await?;
            Ok(())
        })
        .build()
        .await?
        .run()
        .await?;
    Ok(())
}
