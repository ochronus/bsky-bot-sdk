//! A fuller example wiring up handlers for several notification reasons, a custom
//! poll interval, and an error handler.
//!
//! - mentions      → wave back
//! - replies       → answer "ping" with "pong"
//! - follows       → follow back
//! - likes         → just log
//! - handler errors → logged centrally via `on_error`
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example reactor
//! ```

use std::time::Duration;

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
        .poll_interval(Duration::from_secs(10))
        .notification_limit(50)
        .on_mention(|ctx, notif| async move {
            ctx.reply_to(&notif, "👋").await?;
            Ok(())
        })
        .on_reply(|ctx, notif| async move {
            if notif
                .text()
                .map(|t| t.to_lowercase().contains("ping"))
                .unwrap_or(false)
            {
                ctx.reply_to(&notif, "pong 🏓").await?;
            }
            Ok(())
        })
        .on_follow(|ctx, notif| async move {
            ctx.follow_back(&notif).await?;
            Ok(())
        })
        .on_like(|_ctx, notif| async move {
            tracing::info!(who = notif.author_handle(), "someone liked a post");
            Ok(())
        })
        .on_error(|_ctx, notif, err| async move {
            tracing::error!(uri = notif.uri(), error = %err, "handler failed");
        })
        .build()
        .await?
        .run()
        .await?;

    Ok(())
}
