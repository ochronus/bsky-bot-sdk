//! Declaring the account as an automated bot with the `bot` self-label.
//!
//! Bluesky's bot guidelines recommend that automated accounts add a `bot`
//! self-label to their profile so people and moderation tooling can recognize
//! them. `.automated_label(true)` does exactly that on startup, writing the label
//! into the account's `app.bsky.actor.profile` record while preserving the display
//! name, description, avatar, and any other self-labels.
//!
//! It's idempotent (a warm restart won't rewrite the profile) and applied once
//! during `build()`. For a runtime change, call
//! `ctx.set_automated_label(true | false)`.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example self_label_bot
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

    let bot = Bot::builder()
        .from_env()?
        .session_file("session.json")
        // Mark this account as automated on startup. `BOT_SELF_LABEL` is the exact
        // wire value ("bot") written into the profile's self-labels.
        .automated_label(true)
        .on_mention(|ctx, notif| async move {
            ctx.reply_to(&notif, "🤖 beep boop — I'm a bot, and my profile says so.")
                .await?;
            Ok(())
        })
        .build()
        .await?;

    tracing::info!(
        label = BOT_SELF_LABEL,
        handle = bot.identity().handle(),
        "profile self-labeled as automated",
    );

    bot.run().await?;
    Ok(())
}
