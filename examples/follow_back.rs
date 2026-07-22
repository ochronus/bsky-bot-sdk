//! A minimal follow-back bot: whenever someone follows the account, follow them
//! back. It restricts polling to `follow` notifications only.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example follow_back
//! ```

use bsky_bot_sdk::prelude::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    Bot::builder()
        .from_env()?
        .session_file("session.json")
        // Only fetch follow notifications — nothing else is relevant here.
        .reasons([NotificationReason::Follow])
        .on_follow(|ctx, notif| async move {
            tracing::info!(who = notif.author_handle(), "following back");
            ctx.follow_back(&notif).await?;
            Ok(())
        })
        .build()
        .await?
        .run()
        .await?;

    Ok(())
}
