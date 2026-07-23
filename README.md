# bsky-bot-sdk

[![CI](https://github.com/ochronus/bsky-bot-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/ochronus/bsky-bot-sdk/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/bsky-bot-sdk.svg)](https://crates.io/crates/bsky-bot-sdk)
[![docs.rs](https://img.shields.io/docsrs/bsky-bot-sdk)](https://docs.rs/bsky-bot-sdk)
[![license](https://img.shields.io/crates/l/bsky-bot-sdk.svg)](#license)

An ergonomic, event-driven SDK for building **Bluesky** (AT Protocol) bots in Rust.

It's built on top of atrium's [`bsky-sdk`](https://crates.io/crates/bsky-sdk) and
adds the glue a bot actually needs: a notification event loop, real-time network
ingestion via the [Jetstream](https://docs.bsky.app/blog/jetstream) firehose,
typed events, one-call reply/like/repost/follow helpers with automatic rich-text
detection, session persistence, client-side rate limiting, interval/cron
scheduling, and graceful shutdown.

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
| **Real-time stream** | React to the *whole network* via the [Jetstream](https://docs.bsky.app/blog/jetstream) firehose — `on_keyword`, `on_hashtag`, `on_firehose` — with auto-reconnect and cursor resume. |
| **De-duplication** | A watermark tracker (`Dedup`) that survives restarts and breaks timestamp ties, so you never double-reply. |
| **Typed events** | `NotificationReason::{Mention, Reply, Follow, Like, Repost, Quote, …}` instead of magic strings. |
| **Actions** | `ctx.reply_to`, `ctx.like`, `ctx.repost`, `ctx.follow_back`, `ctx.post`, `ctx.delete` — threading and facet detection handled for you. |
| **Media & embeds** | `ctx.compose()` builds posts with images (**alt text required by type**), video, external link cards (auto-fetched OpenGraph), and quote posts — uploaded to your own PDS, so it works on any server. |
| **Rich text** | Mentions, links, and hashtags are detected and attached as facets automatically. |
| **Sessions** | `session_file(...)` resumes on restart instead of re-authenticating. |
| **Rate limiting** | A token bucket modelling Bluesky's points-based write budget (on by default). |
| **Scheduling** | Run actions on an interval or a cron schedule (`every`, `cron`) — many at once, alongside the notification loop. |
| **Shutdown** | `run()` stops cleanly on `Ctrl-C`; `run_until(future)` stops on any signal you choose. |

## Installation

```toml
[dependencies]
bsky-bot-sdk = "0.4"
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
- `ctx.compose()` — build a post with media/embeds (see below)
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
cargo run --example mention_bot       # like + reply to mentions
cargo run --example follow_back       # follow back new followers
cargo run --example reactor           # mentions, replies, follows, likes, error handling
cargo run --example scheduled_poster  # post the date/time once a day (no handlers)
cargo run --example keyword_stream    # react to network-wide keywords/hashtags (Jetstream)
cargo run --example media_bot         # reply with images / quotes / link cards
```

## Media & embeds

`ctx.compose()` returns a fluent `PostBuilder`. Every method is synchronous and
just records intent; the uploads, OpenGraph fetches, and video processing all
happen once, when you `await` `.send()`.

```rust
# use bsky_bot_sdk::prelude::*;
# async fn demo(ctx: Context, notif: Notification) -> Result<()> {
# let jpeg_bytes = vec![];
ctx.compose()
    .text("first post with a picture, and the one that inspired it")
    // Alt text is a required argument — you cannot attach an image without it.
    .image(jpeg_bytes, "A sunset over the ocean, silhouetting a lone surfer")
    .quote(&notif)             // image + quote ⇒ a recordWithMedia embed
    .send()
    .await?;

// An external link card: the URL is fetched and its OpenGraph title,
// description, and preview image become the card (thumbnail uploaded for you).
ctx.compose()
    .text("worth a read:")
    .link_card("https://example.com/article")
    .send()
    .await?;
# Ok(())
# }
```

The builder covers:

- `.image(bytes, alt)` (up to four; MIME sniffed) / `.image_with(bytes, alt, mime)`
- `.video(bytes, alt)` — MP4 via the Bluesky video service
- `.link_card(url)` — auto-fetch OpenGraph / Twitter-card metadata; or
  `.external(uri, title, description)` with no fetching
- `.quote(&notif)` / `.quote_ref(strong_ref)` — quote posts (record embeds)
- `.reply_to(&notif)`, `.text(..)`, `.langs(..)`

A post carries a single media kind (images **or** video **or** an external card),
optionally alongside a quote. **Alt text is required by the type signature** for
images and video — omitting it is a compile error, not a lint. All blobs upload
to the bot's own PDS, so media works the same on `bsky.social` and on
third-party / self-hosted PDSes.

## Real-time streaming (Jetstream)

Notifications only tell you about *your own* account. To react to the **whole
network** in real time, connect a bot to a public
[Jetstream](https://docs.bsky.app/blog/jetstream) instance — a lightweight JSON
view of the AT Protocol firehose. Stream handlers dispatch just like notification
handlers, and a bot can mix both (or run with *only* a stream).

```rust
# use bsky_bot_sdk::prelude::*;
# fn demo(b: BotBuilder) -> BotBuilder {
b
    // Any network post whose text contains a keyword (case-insensitive):
    .on_keyword("rustlang", |ctx, event| async move {
        // A StreamEvent gives you a strong ref you can act on directly.
        if let Some(subject) = event.strong_ref() {
            ctx.like_ref(subject).await?;
        }
        Ok(())
    })
    // Any post carrying a hashtag (matched from text and structured tags):
    .on_hashtag("bluesky", |_ctx, event| async move {
        if let Some(text) = event.text() {
            println!("#bluesky: {text}");
        }
        Ok(())
    })
    // Or the raw firehose for a set of collections you configure:
    .jetstream_collections(["app.bsky.graph.follow"])
    .on_firehose(|_ctx, event| async move {
        println!("{} {:?} {:?}", event.did(), event.operation(), event.collection());
        Ok(())
    })
# }
```

Keyword and hashtag handlers subscribe to `app.bsky.feed.post` automatically. The
connection reconnects on its own with exponential backoff and jitter, and tracks
a time-based cursor so a reconnect resumes without gaps. Tune the endpoint,
collection/DID filters, and starting cursor with `jetstream_endpoint`,
`jetstream_collections`, `jetstream_dids`, and `jetstream_cursor` (or replace the
whole `JetstreamConfig`). Compression (`zstd`) is not yet supported.

## Scheduling

Besides reacting to notifications, a bot can run **actions on a schedule** — an
interval or a cron expression. Register as many as you like; they run
concurrently with the notification loop and stop cleanly on shutdown. A bot may
have *only* schedules and no notification handlers.

```rust
# use std::time::Duration;
# use bsky_bot_sdk::prelude::*;
# fn demo(b: BotBuilder) -> BotBuilder {
b
    // Simple fixed interval:
    .every(Duration::from_secs(3600), |ctx| async move {
        ctx.post("hourly heartbeat").await?;
        Ok(())
    })
    // Cron, evaluated in UTC. 5-field and 6-field (with seconds) both work,
    // as do macros like @daily / @hourly:
    .cron("0 12 * * *", |ctx| async move {   // 12:00 UTC every day
        ctx.post("daily digest").await?;
        Ok(())
    })
    // Cron in the host's local timezone instead of UTC:
    .cron_local("*/15 9-17 * * MON-FRI", |ctx| async move {
        ctx.post("every 15 min, 9–5 on weekdays, local time").await?;
        Ok(())
    })
# }
```

You can also build a [`Schedule`] from a string and pass it to `schedule(...)`:
`"@every 30m"` (simple interval syntax) or any cron expression / macro. An
invalid cron expression is reported from `build()`, so the builder chain stays
fluent. Scheduled-task errors are logged and never stop the loop.

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
