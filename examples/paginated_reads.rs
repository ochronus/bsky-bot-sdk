//! Reading with transparent pagination: followers and the home timeline.
//!
//! The read helpers return a [`Paginated`] stream that fetches each page lazily as
//! you consume it. A **bounded** list (your followers) is safe to drain with
//! `collect_all`; an **unbounded** feed (the timeline) must be capped with `take`.
//!
//! This example just reads and prints, so it never starts the notification loop.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example paginated_reads
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

    // A bot with no handlers is fine to `build()` — we just won't `run()` it.
    let bot = Bot::builder()
        .from_env()?
        .session_file("session.json")
        .build()
        .await?;
    let ctx = bot.context();

    // Bounded list → collect the whole thing. The stream pages under the hood.
    let followers = ctx.my_followers().collect_all().await?;
    println!("You have {} follower(s).", followers.len());
    for profile in followers.iter().take(5) {
        println!("  · @{}", profile.handle.as_str());
    }

    // Unbounded feed → always bound it. Print the 10 most recent timeline posts.
    println!("\nMost recent timeline posts:");
    let mut feed = ctx.timeline().take(10);
    while let Some(item) = feed.next().await {
        let view = item?; // a page error surfaces here
        println!(
            "  · @{} — {}",
            view.post.author.handle.as_str(),
            view.post.uri
        );
    }

    Ok(())
}
