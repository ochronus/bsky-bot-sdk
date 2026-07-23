# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-07-23

### Added

- **Threads with grapheme-aware auto-split.** A fluent `Context::thread()` builder
  publishes a sequence of posts as a connected reply chain (each post replies to
  the previous one; all share the thread root):
  - `.text(piece)` / `.texts([...])` — add content. Each piece becomes at least
    one post (pieces are never merged); a piece longer than the per-post limit is
    split, at word boundaries, across as many posts as it needs.
  - Splitting counts Unicode **extended grapheme clusters** — the same unit, via
    the same `unicode-segmentation` crate, that Bluesky's 300-character limit and
    `bsky-sdk`'s `RichText::grapheme_len` use — so the boundary matches what the
    server enforces, and a grapheme cluster is never split across posts. Breaks
    prefer whitespace, so URLs, `@mentions`, and `#hashtags` stay whole and their
    facets are detected correctly.
  - `.numbered()` — append a ` i/N` suffix to every post, reserving grapheme
    budget for the suffix (via a fixed-point over the post count) so numbered
    posts still fit the limit. A single-post thread is left un-numbered.
  - `.reply_to(&notif)` / `.reply(parent, root)` — root the whole thread as a
    reply, threading correctly; `.langs([...])` sets the language of every post.
  - `.send().await` returns one `create_record::Output` per post, in order.
  - Public `ThreadBuilder` type and `MAX_POST_GRAPHEMES` constant.
  - Example: `thread_bot` (replies to mentions with an auto-split numbered thread).

### Changed

- Added `unicode-segmentation` as a direct dependency for grapheme-cluster
  segmentation. It was already in the tree via `bsky-sdk`'s rich-text feature, so
  declaring it directly adds no new crate and does not affect the MSRV.

## [0.4.0] - 2026-07-23

### Added

- **Media & embeds.** A fluent post builder, `Context::compose()`, attaches rich
  media and embeds to a post; every builder method is synchronous and all network
  work happens once in `PostBuilder::send().await`:
  - `.image(bytes, alt)` — attach an image (up to 4), with **alt text required at
    the type level**: it is a mandatory argument, so a post without a description
    is a compile error, not a lint. `.image_with(bytes, alt, mime)` declares an
    explicit MIME type. The type is otherwise sniffed from the image bytes (PNG,
    JPEG, GIF, WebP).
  - `.video(bytes, alt)` — attach an MP4, uploaded through the Bluesky video
    service (service-auth → `video.bsky.app` → job polling). Alt text is required
    here too.
  - `.link_card(url)` — attach an external link "card": the URL is fetched, its
    OpenGraph / Twitter-card metadata parsed, and any preview image uploaded as
    the thumbnail. `.external(uri, title, description)` builds a card with no
    fetching.
  - `.quote(&notif)` / `.quote_ref(strong_ref)` — quote-post a record. Quoting
    combined with media produces a `recordWithMedia` embed automatically.
  - `.reply_to(&notif)` / `.reply(parent, root)`, `.text(..)`, and `.langs(..)`.
  - `Context::upload_blob(bytes)` — upload a raw blob to the bot's own PDS for
    advanced/custom records.
  - Public `PostBuilder` type and `MAX_IMAGES` constant.
  - Example: `media_bot` (replies to mentions with images / quotes / link cards).
- All media is uploaded to the bot's **own PDS**, so images and link-card
  thumbnails work identically on `bsky.social` and third-party / self-hosted
  PDSes. Uploaded blobs are re-stamped with the sniffed MIME type so rendering
  does not depend on a given PDS's content-type handling. Verified end-to-end
  against a third-party PDS.

### Changed

- New error variants `Error::Http` and `Error::VideoUpload` for outbound
  link-card fetches and the video service (both `#[non_exhaustive]`-compatible).
- Added `reqwest` (with `gzip`) as a direct dependency for OpenGraph fetching and
  the video service. It was already in the tree via atrium's default client and
  uses `native-tls` — no second TLS stack.

## [0.3.0] - 2026-07-23

### Added

- **Real-time ingestion via Jetstream.** React to the *whole network* in real
  time, not just the bot's own notifications, over a WebSocket connection to a
  public [Jetstream] instance:
  - `BotBuilder::on_keyword(keyword, handler)` / `on_keywords([...], handler)` —
    fire on network posts whose text contains a keyword (case-insensitive).
  - `BotBuilder::on_hashtag(tag, handler)` — fire on posts carrying a hashtag
    (matched from both `#tag` text tokens and structured record tags).
  - `BotBuilder::on_firehose(handler)` — fire on every event in the subscribed
    collections.
  - `BotBuilder::on_stream_error(handler)` — error handler for stream handlers.
  - Configuration: `jetstream_endpoint`, `jetstream_collections`,
    `jetstream_dids`, `jetstream_cursor`, and `jetstream_config`, plus the public
    `JetstreamConfig` and `Backoff` types and `DEFAULT_JETSTREAM_ENDPOINT`.
  - New `StreamEvent` type with typed accessors (`kind`, `operation`,
    `collection`, `uri`, `as_post`, `text`, `strong_ref`, `hashtags`, …) and the
    `StreamKind` / `CommitOp` enums. `strong_ref()` lets handlers like, repost,
    or reply to a streamed post directly.
  - The stream runs concurrently with the notification loop and schedules, with
    automatic reconnect (exponential backoff + jitter) and time-based cursor
    tracking for gapless resume. A bot may run with *only* stream handlers.
  - Example: `keyword_stream` (watch the network for keywords/hashtags).
  - Keyword and hashtag handlers subscribe to `app.bsky.feed.post` automatically;
    a firehose handler with no collection filter subscribes to the whole network
    (logged as a warning).

### Changed

- `run_until` now also drives the Jetstream stream, and only returns
  `Error::NoHandlers` when no notification handler, stream handler, *or* schedule
  is registered.

### Notes

- Jetstream `zstd` compression is not yet supported; the uncompressed JSON
  stream is consumed. The WebSocket client uses `native-tls`, matching the TLS
  backend already pulled in by `reqwest` — no second TLS stack is added.

[Jetstream]: https://docs.bsky.app/blog/jetstream

## [0.2.0] - 2026-07-22

### Added

- **Scheduling.** Run bot actions on a fixed interval or a cron schedule,
  concurrently with the notification loop:
  - `BotBuilder::every(interval, task)` — simple fixed-interval syntax.
  - `BotBuilder::cron(expr, task)` / `cron_local(expr, task)` — cron expressions
    evaluated in UTC or the host's local timezone. Accepts 5-field and 6-field
    (with seconds) expressions plus `@daily`/`@hourly`-style macros (via
    [`croner`]).
  - `BotBuilder::schedule(schedule, task)` and the public `Schedule` type, which
    also parses from strings (`"@every 30m"`, cron expressions, macros) via
    `FromStr`. New `Tz` enum selects UTC or local evaluation.
  - A bot may now run with *only* scheduled jobs and no notification handlers.
  - Invalid cron expressions passed to `cron`/`cron_local` surface from `build()`.
- Example: `scheduled_poster` (posts the current date/time once a day).

### Changed

- `run_until` now drives scheduled jobs alongside the notification loop and only
  returns `Error::NoHandlers` when neither a handler nor a schedule is
  registered.
- Minimum supported Rust version raised to **1.88**, required by transitive
  dependencies pulled in through `atrium`/`reqwest` (notably `base45`, which uses
  `<[T]>::as_chunks`, stabilised in Rust 1.88).

[`croner`]: https://crates.io/crates/croner

## [0.1.0] - 2026-07-22

### Added

- Initial release.
- `Bot` builder with credential/environment login and session persistence.
- Notification event loop that polls `listNotifications`, de-duplicates across
  restarts via a watermark `Dedup`, and dispatches typed events to handlers.
- Typed `NotificationReason` events (mention, reply, follow, like, repost, quote)
  with reason-specific and catch-all (`on_any`) handler registration.
- `Context` action helpers: `post`, `reply_to`, `like`, `repost`, `follow`,
  `follow_back`, and `delete`, with automatic rich-text facet detection.
- Client-side `RateLimiter` modelling Bluesky's points-based write budget,
  enabled by default and configurable via `RateLimitConfig`.
- Graceful shutdown through `run()` (Ctrl-C) and `run_until(future)`, plus manual
  loop driving with `poll_and_dispatch`.
- Examples: `mention_bot`, `follow_back`, and `reactor`.

[Unreleased]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ochronus/bsky-bot-sdk/releases/tag/v0.1.0
