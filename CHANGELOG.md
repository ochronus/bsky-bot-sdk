# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/ochronus/bsky-bot-sdk/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ochronus/bsky-bot-sdk/releases/tag/v0.1.0
