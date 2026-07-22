//! A scheduled bot with no notification handlers at all: it just posts the
//! current date and time once a day, and logs a heartbeat every few hours.
//!
//! This shows the two scheduling styles — a cron expression (`cron`) and the
//! simple fixed-interval syntax (`every`) — and that a bot can run on schedules
//! alone, without reacting to any notifications.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example scheduled_poster
//! ```

use std::time::Duration;

use bsky_bot_sdk::prelude::*;
use chrono::Utc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let bot = Bot::builder()
        .from_env()?
        .session_file("session.json")
        // Once a day at 12:00 UTC, post the current date and time.
        // A 5-field cron expression: `min hour day-of-month month day-of-week`.
        // (`@daily` would post at 00:00 UTC instead.)
        .cron("0 12 * * *", |ctx| async move {
            let now = Utc::now().format("%A %-d %B %Y, %H:%M UTC");
            tracing::info!(%now, "posting the daily timestamp");
            ctx.post(format!("🕛 It is now {now}.")).await?;
            Ok(())
        })
        // Every 6 hours, log a heartbeat. `every` is the simple interval syntax;
        // you can register as many schedules as you like.
        .every(Duration::from_secs(6 * 60 * 60), |ctx| async move {
            tracing::info!(handle = ctx.handle(), "still alive");
            Ok(())
        })
        .build()
        .await?;

    tracing::info!(
        handle = bot.identity().handle(),
        "scheduled poster running; Ctrl-C to stop"
    );
    bot.run().await?;

    Ok(())
}
