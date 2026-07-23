//! React to the *whole network* in real time via the Jetstream firehose.
//!
//! Unlike the notification-driven examples, this bot doesn't wait to be
//! mentioned — it watches every new post on Bluesky and reacts to the ones that
//! match a keyword or hashtag. It runs with *only* stream handlers: no
//! notification loop is needed.
//!
//! By default it just logs matches (safe to run against the live network). Set
//! `LIKE=1` to also like matching posts, which shows how [`StreamEvent`] gives
//! you a strong ref you can act on directly.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example keyword_stream
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

    // Opt in to liking matches with `LIKE=1`; otherwise we only observe.
    let should_like = std::env::var("LIKE").is_ok_and(|v| v == "1");

    let bot = Bot::builder()
        .from_env()?
        .session_file("session.json")
        // Any post mentioning one of these keywords (case-insensitive).
        .on_keywords(["rustlang", "atproto"], move |ctx, event| async move {
            let text = event.text().unwrap_or_default();
            tracing::info!(author = event.did(), %text, "keyword match");
            if should_like && let Some(subject) = event.strong_ref() {
                ctx.like_ref(subject).await?;
            }
            Ok(())
        })
        // Any post carrying the #bluesky hashtag.
        .on_hashtag("bluesky", |_ctx, event| async move {
            if let Some(uri) = event.uri() {
                tracing::info!(%uri, "hashtag match");
            }
            Ok(())
        })
        .build()
        .await?;

    tracing::info!(
        handle = bot.identity().handle(),
        "watching the network firehose; Ctrl-C to stop"
    );
    bot.run().await?;

    Ok(())
}
