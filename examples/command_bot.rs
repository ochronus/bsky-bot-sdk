//! A command bot: `@yourbot ping`, `@yourbot echo hello`, plus middleware.
//!
//! Registers a small command language on mentions. The first word after the bot's
//! mention is the command; matching is case-insensitive. `default_command` catches
//! anything unrecognized (a natural place for a help message). The middleware chain
//! runs before every handler: here `ignore_self` drops the bot's own activity and
//! `block_authors` silences a list of accounts.
//!
//! For a sigil-style prefix (`@yourbot !ping`), add `.command_prefix("!")`.
//!
//! ```bash
//! BSKY_IDENTIFIER=you.bsky.social \
//! BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
//!   cargo run --example command_bot
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
        .from_env()?
        .session_file("session.json")
        // Middleware runs before any handler; the first `Skip` drops the event.
        .ignore_self()
        .block_authors(["spammer.example.social"])
        // `@yourbot ping` → "pong 🏓"
        .command("ping", |ctx, notif, _cmd| async move {
            ctx.reply_to(&notif, "pong 🏓").await?;
            Ok(())
        })
        // `@yourbot echo <text>` → replies with <text>
        .command("echo", |ctx, notif, cmd| async move {
            let reply = if cmd.rest().is_empty() {
                "echo what? try `echo hello world`".to_string()
            } else {
                cmd.rest().to_string()
            };
            ctx.reply_to(&notif, reply).await?;
            Ok(())
        })
        // Anything else that looks like a command falls through to here.
        .default_command(|ctx, notif, cmd| async move {
            ctx.reply_to(
                &notif,
                format!("I don't know `{}`. Try `ping` or `echo`.", cmd.name()),
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
