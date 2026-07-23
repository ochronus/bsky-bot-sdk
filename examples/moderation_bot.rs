//! Moderation & profile write actions: mute, block, unfollow, and profile edits.
//!
//! On startup this bot tidies its own profile (display name + bio). Then it offers
//! command handlers that act on the author of the mention:
//!
//! - `@yourbot mute`   → privately mute the author (no public record; they aren't told)
//! - `@yourbot block`  → publicly block the author (an `app.bsky.graph.block` record)
//! - `@yourbot unfollow` → stop following the author, if the bot was
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example moderation_bot
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
        .command("mute", |ctx, notif, _cmd| async move {
            // Muting is private and one-way — the author is never notified.
            ctx.mute(notif.author_did()).await?;
            tracing::info!(who = notif.author_handle(), "muted");
            Ok(())
        })
        .command("block", |ctx, notif, _cmd| async move {
            ctx.block(notif.author_did()).await?;
            tracing::info!(who = notif.author_handle(), "blocked");
            Ok(())
        })
        .command("unfollow", |ctx, notif, _cmd| async move {
            let was_following = ctx.unfollow(notif.author_did()).await?;
            tracing::info!(who = notif.author_handle(), was_following, "unfollow");
            Ok(())
        })
        .build()
        .await?;

    // Read-modify-write the profile, preserving every field we don't touch.
    bot.context()
        .update_profile(|p| {
            p.display_name = Some("Moderation Demo Bot".into());
            p.description = Some("I mute, block, and mind my own business. 🤖".into());
        })
        .await?;
    tracing::info!("profile updated");

    bot.run().await?;
    Ok(())
}
