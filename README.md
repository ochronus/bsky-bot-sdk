# bsky-bot-sdk

[![CI](https://github.com/ochronus/bsky-bot-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/ochronus/bsky-bot-sdk/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/bsky-bot-sdk.svg)](https://crates.io/crates/bsky-bot-sdk)
[![docs.rs](https://img.shields.io/docsrs/bsky-bot-sdk)](https://docs.rs/bsky-bot-sdk)
[![license](https://img.shields.io/crates/l/bsky-bot-sdk.svg)](#license)

An ergonomic, event-driven SDK for building **Bluesky** (AT Protocol) bots in Rust.

It's built on top of atrium's [`bsky-sdk`](https://crates.io/crates/bsky-sdk) and
adds the glue a bot actually needs: a notification event loop, typed events,
one-call reply/like/repost/follow helpers with automatic rich-text detection,
session persistence, client-side rate limiting, and graceful shutdown.

```rust
use bsky_bot_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    Bot::builder()
        .credentials("mybot.bsky.social", "xxxx-xxxx-xxxx-xxxx") // use an app password
        .session_file("session.json")
        .on_mention(|ctx, notif| async move {
            ctx.reply_to(&notif, format!("👋 hi @{}!", notif.author_handle())).await?;
            Ok(())
        })
        .on_follow(|ctx, notif| async move {
            ctx.follow_back(&notif).await?;
            Ok(())
        })
        .build()
        .await?
        .run()
        .await
}
```

## Why this over raw `bsky-sdk`?

`bsky-sdk` gives you an authenticated XRPC client and typed records. A *bot*
still needs the loop around it. This crate provides:

| Concern | What you get |
| --- | --- |
| **Event loop** | Polls `listNotifications` on an interval, dispatches to your handlers. |
| **De-duplication** | A watermark tracker (`Dedup`) that survives restarts and breaks timestamp ties, so you never double-reply. |
| **Typed events** | `NotificationReason::{Mention, Reply, Follow, Like, Repost, Quote, …}` instead of magic strings. |
| **Actions** | `ctx.reply_to`, `ctx.like`, `ctx.repost`, `ctx.follow_back`, `ctx.post`, `ctx.delete` — threading and facet detection handled for you. |
| **Rich text** | Mentions, links, and hashtags are detected and attached as facets automatically. |
| **Sessions** | `session_file(...)` resumes on restart instead of re-authenticating. |
| **Rate limiting** | A token bucket modelling Bluesky's points-based write budget (on by default). |
| **Shutdown** | `run()` stops cleanly on `Ctrl-C`; `run_until(future)` stops on any signal you choose. |

## Installation

```toml
[dependencies]
bsky-bot-sdk = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Core concepts

### The builder

`Bot::builder()` configures credentials, polling, and handlers, then `build()`
authenticates (resuming a saved session when possible) and returns a runnable
`Bot`.

```rust
# use std::time::Duration;
# use bsky_bot_sdk::prelude::*;
# async fn demo() -> Result<()> {
let bot = Bot::builder()
    .from_env()?                          // BSKY_IDENTIFIER + BSKY_APP_PASSWORD
    .service("https://bsky.social")       // optional; this is the default
    .session_file("session.json")         // persist + resume the login session
    .poll_interval(Duration::from_secs(10))
    .notification_limit(50)               // per poll (1..=100)
    .reasons([NotificationReason::Mention, NotificationReason::Follow]) // optional filter
    .process_backlog(false)               // default: don't reply to old notifications on start
    .on_mention(|ctx, n| async move { ctx.reply_to(&n, "hi!").await?; Ok(()) })
    .build()
    .await?;
bot.run().await
# }
```

### Handlers

A handler is any async closure `Fn(Context, Notification) -> Future<Output = Result<()>>`.
Register one per reason, or a catch-all with `on_any`. Reason-specific handlers run
first (in registration order), then catch-alls. A handler returning `Err` is sent to
your `on_error` handler (or logged) and never stops the loop.

```rust
# use bsky_bot_sdk::prelude::*;
# fn demo(b: BotBuilder) -> BotBuilder {
b.on_reply(|ctx, notif| async move {
    if notif.text().map(|t| t.contains("ping")).unwrap_or(false) {
        ctx.reply_to(&notif, "pong 🏓").await?;
    }
    Ok(())
})
.on_error(|_ctx, notif, err| async move {
    tracing::error!(uri = notif.uri(), %err, "handler failed");
})
# }
```

### The `Context`

Every handler receives a cheap-to-clone `Context` bundling the authenticated
agent, the bot's own identity (`ctx.me()`, `ctx.did()`, `ctx.handle()`), and the
action helpers:

- `ctx.post(text)` — new top-level post (facets auto-detected)
- `ctx.reply_to(&notif, text)` — reply in-thread, root resolved automatically
- `ctx.like(&notif)` / `ctx.repost(&notif)`
- `ctx.follow_back(&notif)` / `ctx.follow(did)`
- `ctx.delete(at_uri)`
- `ctx.agent()` — drop down to raw `bsky-sdk` for anything not covered

### The `Notification`

A thin wrapper over the raw AT Protocol notification with the fields bots reach
for: `reason()`, `author_handle()`, `author_did()`, `uri()`, `cid()`,
`text()` (when the record is a post), `as_post()`, and `subject_ref()`. The raw
value is always available via `notif.raw()`.

## Examples

Run any example with credentials in the environment:

```bash
export BSKY_IDENTIFIER=you.bsky.social
export BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx
cargo run --example mention_bot   # like + reply to mentions
cargo run --example follow_back   # follow back new followers
cargo run --example reactor       # mentions, replies, follows, likes, error handling
```

## Rate limiting

By default a `RateLimiter` models Bluesky's points-based write budget (create = 3
points, update = 2, delete = 1; 5000 points/hour). Writes issued through `Context`
back-pressure automatically instead of being rejected. Tune or disable it:

```rust
# use bsky_bot_sdk::prelude::*;
# fn demo(b: BotBuilder) -> BotBuilder {
b.rate_limit(Some(RateLimitConfig { points_per_hour: 3000, ..Default::default() }))
// or: .rate_limit(None) to disable entirely
# }
```

## Session persistence

Pass `session_file("session.json")` and the bot writes its session there after
login and resumes from it on the next start. If the saved session can't be
resumed (e.g. it expired), the bot falls back to logging in with the configured
credentials and rewrites the file.

## Driving the loop yourself

`run()` / `run_until(future)` own the loop, but you can drive it manually with a
`Dedup` you keep between cycles — handy for tests or custom schedulers:

```rust
# use bsky_bot_sdk::prelude::*;
# async fn demo(bot: Bot) -> Result<()> {
let mut seen = Dedup::new();
loop {
    let handled = bot.poll_and_dispatch(&mut seen).await?;
    tracing::debug!(handled, "cycle done");
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
}
# }
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
