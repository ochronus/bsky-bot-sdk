//! Watermark-based de-duplication for the notification poll loop.
//!
//! Bluesky's `listNotifications` returns the newest notifications first and has no
//! "only since" parameter that survives restarts, so a polling bot must decide for
//! itself which notifications it has already handled. [`Dedup`] tracks a high-water
//! mark (the newest `indexedAt` processed) plus the set of URIs seen *exactly at*
//! that timestamp, which breaks ties when several notifications share an instant.

use std::cmp::Ordering;
use std::collections::HashSet;

use atrium_api::types::string::Datetime;

use crate::event::Notification;

/// Tracks which notifications have already been handled across poll cycles.
///
/// Advanced users driving the loop manually (via
/// [`Bot::poll_and_dispatch`](crate::Bot::poll_and_dispatch)) own a `Dedup` and
/// pass it to each cycle. The default [`Bot::run`](crate::Bot::run) loop manages
/// one internally.
#[derive(Debug, Default)]
pub struct Dedup {
    high_water: Option<Datetime>,
    /// URIs of notifications processed at exactly `high_water`.
    seen_at_watermark: HashSet<String>,
}

impl Dedup {
    /// Create an empty tracker that considers every notification new.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `notif` has not yet been processed.
    pub fn is_new(&self, notif: &Notification) -> bool {
        match &self.high_water {
            None => true,
            Some(watermark) => match notif.indexed_at().cmp(watermark) {
                Ordering::Greater => true,
                Ordering::Equal => !self.seen_at_watermark.contains(notif.uri()),
                Ordering::Less => false,
            },
        }
    }

    /// Record `notif` as processed, advancing the watermark as needed.
    pub fn mark(&mut self, notif: &Notification) {
        let indexed = notif.indexed_at();
        match self
            .high_water
            .as_ref()
            .map(|watermark| indexed.cmp(watermark))
        {
            Some(Ordering::Equal) => {
                self.seen_at_watermark.insert(notif.uri().to_string());
            }
            Some(Ordering::Less) => {
                // Older than the watermark: already implicitly covered.
            }
            // Strictly newer, or the first mark ever: advance and reset the tie set.
            Some(Ordering::Greater) | None => {
                self.high_water = Some(indexed.clone());
                self.seen_at_watermark.clear();
                self.seen_at_watermark.insert(notif.uri().to_string());
            }
        }
    }

    /// Advance the watermark past all of `notifs` *without* treating them as work.
    ///
    /// Used on startup to skip a backlog of pre-existing notifications.
    pub fn prime(&mut self, notifs: &[Notification]) {
        for notif in notifs {
            self.mark(notif);
        }
    }

    /// Filter `notifs` down to the not-yet-seen ones, sort them oldest-first, and
    /// mark them processed. The returned notifications are ready to dispatch in
    /// chronological order.
    pub fn take_new_sorted(&mut self, notifs: Vec<Notification>) -> Vec<Notification> {
        let mut fresh: Vec<Notification> = notifs.into_iter().filter(|n| self.is_new(n)).collect();
        fresh.sort_by(|a, b| a.indexed_at().cmp(b.indexed_at()));
        for notif in &fresh {
            self.mark(notif);
        }
        fresh
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notif(uri: &str, indexed_at: &str) -> Notification {
        let value = serde_json::json!({
            "author": { "did": "did:plc:alice000000000000000000", "handle": "alice.test" },
            "cid": "bafyreiclp443lavogvhj3d2ob2cxbfuscni2k5jk7bebjzg7khl3esabwq",
            "indexedAt": indexed_at,
            "isRead": false,
            "reason": "mention",
            "record": { "$type": "app.bsky.feed.post", "text": "hi", "createdAt": indexed_at },
            "uri": uri,
        });
        Notification::new(serde_json::from_value(value).expect("valid notification fixture"))
    }

    #[test]
    fn everything_is_new_on_a_fresh_tracker() {
        let dedup = Dedup::new();
        assert!(dedup.is_new(&notif("at://x/1", "2026-07-22T10:00:00.000Z")));
    }

    #[test]
    fn marked_notifications_are_not_new_again() {
        let mut dedup = Dedup::new();
        let n = notif("at://x/1", "2026-07-22T10:00:00.000Z");
        dedup.mark(&n);
        assert!(
            !dedup.is_new(&n),
            "an already-marked notification must not be re-processed"
        );
    }

    #[test]
    fn newer_timestamps_are_new_older_are_not() {
        let mut dedup = Dedup::new();
        dedup.mark(&notif("at://x/mid", "2026-07-22T10:00:00.000Z"));

        assert!(dedup.is_new(&notif("at://x/newer", "2026-07-22T10:00:01.000Z")));
        assert!(!dedup.is_new(&notif("at://x/older", "2026-07-22T09:59:59.000Z")));
    }

    #[test]
    fn ties_at_the_watermark_are_disambiguated_by_uri() {
        let mut dedup = Dedup::new();
        let ts = "2026-07-22T10:00:00.000Z";
        let a = notif("at://x/a", ts);
        let b = notif("at://x/b", ts);

        dedup.mark(&a);
        // Same instant, different URI → still unseen.
        assert!(
            dedup.is_new(&b),
            "a distinct notification at the same instant is still new"
        );
        assert!(!dedup.is_new(&a));

        dedup.mark(&b);
        assert!(!dedup.is_new(&b));
    }

    #[test]
    fn prime_skips_the_entire_backlog() {
        let mut dedup = Dedup::new();
        let backlog = vec![
            notif("at://x/1", "2026-07-22T09:00:00.000Z"),
            notif("at://x/2", "2026-07-22T10:00:00.000Z"),
            notif("at://x/3", "2026-07-22T10:00:00.000Z"), // tie at the newest instant
        ];
        dedup.prime(&backlog);

        // Re-presenting the same batch yields nothing to process.
        let fresh = dedup.take_new_sorted(backlog);
        assert!(fresh.is_empty(), "primed backlog must not be reprocessed");
    }

    #[test]
    fn take_new_sorted_returns_only_fresh_in_chronological_order() {
        let mut dedup = Dedup::new();
        dedup.prime(&[notif("at://x/old", "2026-07-22T09:00:00.000Z")]);

        let batch = vec![
            notif("at://x/c", "2026-07-22T12:00:00.000Z"),
            notif("at://x/old", "2026-07-22T09:00:00.000Z"), // already primed
            notif("at://x/a", "2026-07-22T10:00:00.000Z"),
            notif("at://x/b", "2026-07-22T11:00:00.000Z"),
        ];
        let fresh = dedup.take_new_sorted(batch);

        let uris: Vec<&str> = fresh.iter().map(|n| n.uri()).collect();
        assert_eq!(
            uris,
            ["at://x/a", "at://x/b", "at://x/c"],
            "fresh items, oldest first"
        );
    }

    #[test]
    fn a_later_arrival_at_the_prior_watermark_instant_is_still_processed() {
        let mut dedup = Dedup::new();
        let ts = "2026-07-22T10:00:00.000Z";
        dedup.mark(&notif("at://x/first", ts));

        // A second notification indexed at the very same instant, seen in a later
        // poll, must not be silently dropped.
        let late = notif("at://x/second", ts);
        assert!(dedup.is_new(&late));
        let fresh = dedup.take_new_sorted(vec![late]);
        assert_eq!(fresh.len(), 1);
    }
}
