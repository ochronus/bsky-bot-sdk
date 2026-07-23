//! A bot that replies to mentions with a threaded, auto-split answer.
//!
//! It demonstrates [`Context::thread`]: when someone mentions the bot, it replies
//! with a long block of text that would never fit in a single 300-grapheme post.
//! The [`ThreadBuilder`] splits it, at word boundaries, into a numbered thread —
//! each post replying to the one before it.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//! # optional: for a non-bsky.social PDS
//! # BSKY_SERVICE=https://eurosky.social \
//!   cargo run --example thread_bot
//! ```

use bsky_bot_sdk::prelude::*;
use tracing_subscriber::EnvFilter;

/// A canned reply long enough to need several posts.
const ESSAY: &str = "\
Thanks for the mention! Here's a longer thought that doesn't fit in one post, \
so the SDK splits it into a thread for me. It counts Unicode grapheme clusters \
— the same way Bluesky's 300-character limit does — and it breaks at word \
boundaries, so links and @mentions never get chopped in half. Each post you're \
reading replies to the one above it, and they all share the same thread root. \
I didn't have to compute a single reply reference by hand: I just handed the \
whole essay to ctx.thread(), asked for numbering, and called send(). That's the \
entire feature — long text in, tidy thread out. 🧵";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,bsky_bot_sdk=debug")),
        )
        .init();

    let mut builder = Bot::builder().from_env()?.session_file("session.json");
    if let Ok(service) = std::env::var("BSKY_SERVICE") {
        builder = builder.service(service);
    }

    builder
        .on_mention(|ctx, notif| async move {
            tracing::info!(from = notif.author_handle(), "mention → posting a thread");

            let posts = ctx
                .thread()
                .reply_to(&notif)
                .text(ESSAY)
                .numbered()
                .send()
                .await?;

            tracing::info!(parts = posts.len(), "thread posted");
            Ok(())
        })
        .build()
        .await?
        .run()
        .await?;

    Ok(())
}
