//! A direct-message echo bot: it replies to every incoming DM with the same text,
//! prefixed, and greets a chosen DID on startup.
//!
//! Direct messages live behind Bluesky's chat service (`chat.bsky.convo`). Two
//! settings gate them:
//!
//! - Your **app password must have direct-message access** — a per-app-password
//!   opt-in in Settings → Privacy and security → App passwords. Without it the
//!   server rejects every chat call.
//! - To receive DMs from accounts the bot does not follow, the bot's
//!   `chat.bsky.actor.declaration` must set `allowIncoming = "all"` (the default
//!   blocks non-followed senders). See the crate README / `dm` module docs for
//!   the one-time record write that opens the inbox.
//!
//! - incoming DM   → reply "echo: <text>" into the same conversation
//! - startup       → optionally send a one-off hello to `GREET_DID`
//! - handler errors → logged centrally via `on_message_error`
//!
//! The bot never sees its own messages, so the echo cannot loop.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example dm_bot
//!
//! # Optionally greet someone by DID on startup:
//! GREET_DID=did:plc:… cargo run --example dm_bot
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
        .on_message(|ctx, dm| async move {
            tracing::info!(
                from = dm.sender_did(),
                convo = dm.convo_id(),
                "received dm: {}",
                dm.text(),
            );
            // Reply into the same conversation — cheaper than send_dm(did, …),
            // which would re-resolve the conversation first.
            ctx.send_dm_to_convo(dm.convo_id(), format!("echo: {}", dm.text()))
                .await?;
            Ok(())
        })
        .on_message_error(|_ctx, dm, err| async move {
            tracing::error!(convo = dm.convo_id(), error = %err, "dm handler failed");
        })
        .build()
        .await?;

    // Optional: send a one-off greeting by DID before entering the run loop.
    if let Ok(did) = std::env::var("GREET_DID") {
        match bot
            .context()
            .send_dm(&did, "👋 hi! I'm an echo bot — say something.")
            .await
        {
            Ok(sent) => tracing::info!(convo = sent.convo_id(), "sent greeting to {did}"),
            Err(err) => tracing::error!(error = %err, "failed to greet {did}"),
        }
    }

    bot.run().await?;
    Ok(())
}
