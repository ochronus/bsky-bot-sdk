# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.0] - 2026-07-23

### Added

- **Test harness (`testkit`).** Unit-test bot logic without a network. `MockBot`
  hands you a *real* `Context` — the exact type handlers take — whose XRPC calls
  are served in-process by canned responses (no sockets, no credentials), and
  records every request so you can assert on what the handler did:
  - Input fixtures: `mention`, `reply`, `follow`, `like`, `repost`, `quote`,
    `direct_message`, and `stream_post`.
  - Assertions: `requests()`, `created()` / `created_in(collection)`, `posts()`;
    prime `getRecord` with `set_profile_record`.
  - Writes, `updateSeen`, `uploadBlob`, and chat `sendMessage` are mocked. The one
    call that still hits the network is resolving an `@mention` in a post's *text*
    (handles resolve via Bluesky's public API), so keep asserted text mention-free.
  - The harness reuses the production `RateLimitClient` (now backed by either the
    real transport or an in-process mock), so the mocked `Context` is the same
    concrete type as a live one — your real handlers run unchanged.

## [0.9.0] - 2026-07-23

### Added

- **Honor the server's `RateLimit-*` response headers.** The client-side points
  budget is an estimate; Bluesky reports the truth (including request-count limits
  the point budget doesn't model, e.g. 3000 requests / 5 min) on every response —
  headers that atrium's high-level client discards. The SDK now installs a thin
  `RateLimitClient` on its agent that records `RateLimit-Limit` /
  `RateLimit-Remaining` / `RateLimit-Reset` as a side effect of every response:
  - **Pre-emptive throttling**: before each write, if the server last reported the
    window exhausted, the write waits until the reset — pre-empting a 429 instead
    of absorbing one.
  - **Observability**: `Context::server_rate_limit() -> Option<RateLimitStatus>`
    exposes the latest snapshot.
  - This completes the resilience roadmap item (reconnect + retry + server limits).

### Changed

- `Bot::agent()` and `Context::agent()` now return `&BskyAgent<RateLimitClient>`
  rather than the default `&BskyAgent`. Every agent method is generic over the
  client, so call sites are unaffected; only an explicit `&BskyAgent` type
  annotation on the result would need updating. Added a direct dependency on
  `atrium-xrpc-client` (already present transitively, so no new crate).

## [0.8.0] - 2026-07-23

### Added

- **Resilience: automatic retry of transient failures.** Transient errors
  (bare transport failures, HTTP 5xx, 429, and 408) on the poll loops and
  idempotent reads — `listNotifications`, `updateSeen`, `getLog`, `getRecord`,
  and conversation resolution — are now retried with exponential backoff + jitter,
  so a passing network blip is ridden out within a cycle instead of stalling it.
  Permanent errors (bad input, auth, not-found) fail fast, un-retried.
  - `RetryPolicy` (`max_retries` + a reused `Backoff`) configures it, via
    `BotBuilder::retry_policy(...)` or `BotConfig::retry`. Default: 3 quick tries.
    `RetryPolicy::none()` disables retrying.
  - Record **writes are deliberately never auto-retried** — a lost response to a
    committed create would double-post. (Safe write retries need an idempotency
    key, a separate concern.)
  - This completes the retry portion of the resilience roadmap item; the Jetstream
    stream already auto-reconnects with the same backoff.

## [0.7.0] - 2026-07-23

### Added

- **Automated self-label.** A bot can now declare itself automated by adding the
  `bot` self-label to its profile, the cheap, guideline-recommended signal that an
  account is a bot (and a hedge against being mistaken for spam):
  - `BotBuilder::automated_label(true)` — declarative; the label is written once
    during `build()`, preserving the display name, description, avatar, and any
    other self-labels already on the profile. `false` removes it.
  - `Context::set_automated_label(bool)` — the runtime primitive.
  - The write is idempotent and skipped entirely when the profile is already in
    the requested state, so it costs nothing on a warm restart.
  - `BOT_SELF_LABEL` exposes the exact wire value (`"bot"`).
  - New `self_label_bot` example.

## [0.6.1] - 2026-07-23

### Added

- **Direct-message inbox policy helpers.** A typed `DmAccess`
  (`Everyone` / `Following` / `Nobody`) plus two ways to publish the bot's
  `chat.bsky.actor.declaration` (`allowIncoming`) record, so a bot that should
  receive DMs from non-followed accounts no longer has to hand-roll a `putRecord`:
  - `BotBuilder::accept_dms_from(DmAccess)` — declarative; the record is written
    once during `build()`.
  - `Context::set_dm_access(DmAccess)` — the runtime primitive.
  - The `dm_bot` example now calls `.accept_dms_from(DmAccess::Everyone)` so it
    works out of the box.

## [0.6.0] - 2026-07-23

### Added

- **Direct messages (`chat.bsky.convo`).** Bots can now react to and send private
  messages, rounding out the reactive surface:
  - `on_message(|ctx, dm| …)` registers a handler invoked for each new direct
    message across all of the bot's conversations. Runs concurrently with the
    notification loop, the Jetstream stream, and schedules; a bot may run with
    *only* message handlers. `on_message_error` mirrors `on_error` for message
    handlers.
  - Messages the bot itself sent are filtered out before dispatch, so an echo
    handler cannot loop.
  - `Context::send_dm(did, text)` resolves (or creates) the one-to-one
    conversation with an actor and sends a message, detecting rich-text facets
    like `post`. `Context::send_dm_to_convo(convo_id, text)` sends into a known
    conversation — the efficient way to reply from a handler.
    `Context::convo_id_for(did)` exposes the conversation lookup on its own.
  - `DirectMessage` wrapper with typed accessors (`convo_id`, `id`, `rev`,
    `sender_did`, `text`, `sent_at`, `raw`), mirroring `Notification` /
    `StreamEvent`. Public `DmConfig` and `RawMessage` types.
  - Ingestion polls `chat.bsky.convo.getLog` (the cursor-based conversation-event
    log). It skips the pre-startup backlog by default — opt in with
    `process_dm_backlog(true)` — and the poll cadence is tunable via
    `dm_poll_interval` (default 5s) or a full `DmConfig`.
  - Example: `dm_bot` (echoes incoming DMs; optional startup greeting by DID).

### Notes

- Direct messages require an **app password with direct-message access** (a
  per-app-password opt-in in the Bluesky settings). Chat calls are routed through
  the `api.bsky.chat` service via the `atproto-proxy` header. No new dependency:
  the `chat.bsky.convo` types were already available through `atrium-api`'s
  `bluesky` feature.
- To **receive** DMs from accounts the bot does not follow, the bot's
  `chat.bsky.actor.declaration` record must set `allowIncoming = "all"` (the
  default blocks non-followed senders). There is no builder shortcut yet; the
  README and the `dm` module docs show how to publish it through the agent.
  Live-validated end-to-end against the real network (including a third-party
  PDS).

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

[Unreleased]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.6.1...HEAD
[0.6.1]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ochronus/bsky-bot-sdk/releases/tag/v0.1.0
