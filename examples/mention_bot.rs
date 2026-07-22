//! A bot that likes and replies to every mention.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example mention_bot
//! ```

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
        .from_env()? // BSKY_IDENTIFIER + BSKY_APP_PASSWORD
        .session_file("session.json")
        .on_mention(|ctx, notif| async move {
            tracing::info!(from = notif.author_handle(), text = ?notif.text(), "mention");
            // Like the post that mentioned us, then reply in-thread.
            ctx.like(&notif).await?;
            ctx.reply_to(
                &notif,
                format!("thanks for the mention, @{}! 🦀", notif.author_handle()),
            )
            .await?;
            Ok(())
        })
        .build()
        .await?
        .run()
        .await?;

    Ok(())
}
