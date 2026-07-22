# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ochronus/bsky-bot-sdk/releases/tag/v0.1.0
