//! A bot that replies to mentions with rich media & embeds.
//!
//! It demonstrates the [`Context::compose`] builder: quoting the mention,
//! attaching an image with **required** alt text, and — when the mention
//! contains a link — an auto-fetched OpenGraph link card.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//! # optional: a PNG/JPEG/GIF/WebP to attach to replies
//! MEDIA_IMAGE=./cat.png \
//! # optional: for a non-bsky.social PDS
//! # BSKY_SERVICE=https://eurosky.social \
//!   cargo run --example media_bot
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

    // Optionally attach a local image to every reply. Alt text is required by the
    // builder's type signature, so there is no way to post it without a caption.
    let image = std::env::var("MEDIA_IMAGE").ok().and_then(|path| {
        std::fs::read(&path)
            .map_err(|e| tracing::warn!(%path, error = %e, "could not read MEDIA_IMAGE"))
            .ok()
    });

    let mut builder = Bot::builder().from_env()?.session_file("session.json");
    if let Ok(service) = std::env::var("BSKY_SERVICE") {
        builder = builder.service(service);
    }

    builder
        .on_mention(move |ctx, notif| {
            let image = image.clone();
            async move {
                tracing::info!(
                    from = notif.author_handle(),
                    "mention → composing rich reply"
                );

                // Reply, quoting the mention. Chain on media as available.
                let mut post = ctx.compose().reply_to(&notif).quote(&notif).text(format!(
                    "thanks for the mention, @{}! 🦀",
                    notif.author_handle()
                ));

                if let Some(bytes) = image {
                    post = post.image(bytes, "The bot's mascot, a happy crab");
                } else if let Some(url) = first_url(notif.text().as_deref().unwrap_or_default()) {
                    // No image? If the mention linked something, show its card.
                    post = post.link_card(url);
                }

                post.send().await?;
                Ok(())
            }
        })
        .build()
        .await?
        .run()
        .await?;

    Ok(())
}

/// Return the first `http(s)://` token in `text`, if any.
fn first_url(text: &str) -> Option<String> {
    text.split_whitespace()
        .find(|w| w.starts_with("http://") || w.starts_with("https://"))
        .map(|w| {
            w.trim_end_matches(['.', ',', ')', ']', '!', '?'])
                .to_string()
        })
}
