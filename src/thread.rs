//! Multi-post threads with grapheme-aware auto-split.
//!
//! A [`ThreadBuilder`] — obtained from [`Context::thread`](crate::Context::thread)
//! — publishes a sequence of posts as a connected reply chain: post *n+1* replies
//! to post *n*, and every post shares the thread's root. Text that overflows
//! Bluesky's [`MAX_POST_GRAPHEMES`]-grapheme limit is split automatically at word
//! boundaries, so a long string becomes a tidy thread instead of a rejected post.
//!
//! ## Counted in graphemes, like Bluesky
//!
//! Bluesky's 300-character post limit is measured in Unicode *extended grapheme
//! clusters*, not bytes or `char`s — so `"👨‍👩‍👧‍👦"` counts as **one**. The splitter counts
//! the same way (via the same [`unicode-segmentation`] crate `bsky-sdk`'s
//! `RichText::grapheme_len` uses), so the boundary it picks matches the one the
//! server enforces. A grapheme cluster is never split across two posts.
//!
//! ## Word boundaries keep facets intact
//!
//! Splitting prefers whitespace boundaries, so a URL, `@mention`, or `#hashtag`
//! — none of which contain spaces — stays whole within a single post and its
//! rich-text facet is detected correctly. Only a single token longer than the
//! limit (a pathologically long URL) is hard-split.
//!
//! ```no_run
//! # use bsky_bot_sdk::prelude::*;
//! # async fn f(ctx: Context) -> Result<()> {
//! let long = "A very long essay that runs well past three hundred graphemes …";
//! let posts = ctx.thread().text(long).numbered().send().await?;
//! println!("posted a {}-part thread", posts.len());
//! # Ok(())
//! # }
//! ```
//!
//! [`unicode-segmentation`]: https://crates.io/crates/unicode-segmentation

use atrium_api::app::bsky::feed::post;
use atrium_api::com::atproto::repo::{create_record, strong_ref};
use atrium_api::types::string::Language;
use unicode_segmentation::UnicodeSegmentation;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::event::Notification;

/// Bluesky's maximum post length, in Unicode extended grapheme clusters.
///
/// This is the ceiling the server enforces on `app.bsky.feed.post`'s `text`, and
/// the size [`ThreadBuilder`] splits long text down to.
pub const MAX_POST_GRAPHEMES: usize = 300;

/// A fluent builder that publishes a connected thread of posts.
///
/// Construct one with [`Context::thread`](crate::Context::thread), add content
/// with [`text`](Self::text) / [`texts`](Self::texts), then call
/// [`send`](Self::send). Each piece you add becomes at least one post (pieces are
/// never merged); a piece longer than [`MAX_POST_GRAPHEMES`] is auto-split into as
/// many posts as it needs. Builder methods are cheap and synchronous — nothing is
/// posted until `send().await`.
#[must_use = "a ThreadBuilder does nothing until you call `.send().await`"]
pub struct ThreadBuilder {
    ctx: Context,
    pieces: Vec<String>,
    reply: Option<post::ReplyRef>,
    langs: Option<Vec<Language>>,
    numbered: bool,
}

impl ThreadBuilder {
    pub(crate) fn new(ctx: Context) -> Self {
        Self {
            ctx,
            pieces: Vec::new(),
            reply: None,
            langs: None,
            numbered: false,
        }
    }

    /// Add one piece of text. It becomes at least one post; if it exceeds
    /// [`MAX_POST_GRAPHEMES`] it is split, at word boundaries, across several.
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.pieces.push(text.into());
        self
    }

    /// Add several pieces at once (each treated as its own [`text`](Self::text)).
    pub fn texts<I, S>(mut self, texts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.pieces.extend(texts.into_iter().map(Into::into));
        self
    }

    /// Number every post with a ` i/N` suffix (e.g. `2/5`). The grapheme budget
    /// each post is split to is reduced to leave room for the suffix, so numbered
    /// posts still fit within [`MAX_POST_GRAPHEMES`]. A single-post thread is left
    /// un-numbered.
    pub fn numbered(mut self) -> Self {
        self.numbered = true;
        self
    }

    /// Root the whole thread as a reply to a notification, threading correctly
    /// (the thread's `root` is inherited from the parent's thread root when
    /// present). Without this, the thread stands on its own with its first post as
    /// the root.
    pub fn reply_to(mut self, notif: &Notification) -> Self {
        let parent = notif.subject_ref();
        let root = notif
            .as_post()
            .and_then(|p| p.reply.map(|r| r.root.clone()))
            .unwrap_or_else(|| parent.clone());
        self.reply = Some(post::ReplyRefData { parent, root }.into());
        self
    }

    /// Root the thread as a reply with explicit `parent` and `root` strong refs.
    pub fn reply(mut self, parent: strong_ref::Main, root: strong_ref::Main) -> Self {
        self.reply = Some(post::ReplyRefData { parent, root }.into());
        self
    }

    /// Declare the language(s) of every post in the thread as BCP-47 tags (e.g.
    /// `"en"`, `"pt-BR"`). Invalid tags are silently skipped.
    pub fn langs<I, S>(mut self, langs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let parsed: Vec<Language> = langs
            .into_iter()
            .filter_map(|s| s.as_ref().parse().ok())
            .collect();
        self.langs = if parsed.is_empty() {
            None
        } else {
            Some(parsed)
        };
        self
    }

    /// Publish the thread, posting each segment as a reply to the previous one.
    ///
    /// Returns one [`create_record::Output`] per post, in order. Facets (mentions,
    /// links, hashtags) are detected per post.
    ///
    /// Errors if there is no content to post. A failure partway through leaves the
    /// posts already made in place — the error carries no partial result, so a
    /// caller that needs all-or-nothing semantics should keep its own record of
    /// what returned successfully.
    pub async fn send(self) -> Result<Vec<create_record::Output>> {
        let ThreadBuilder {
            ctx,
            pieces,
            reply,
            langs,
            numbered,
        } = self;

        let segments = plan_segments(&pieces, numbered, MAX_POST_GRAPHEMES);
        if segments.is_empty() {
            return Err(Error::invalid_input("thread has no content to post"));
        }

        let mut outputs = Vec::with_capacity(segments.len());
        // `root` / `parent` track the thread as it grows. When the thread is a
        // reply, both start from the notification; otherwise the first post is
        // top-level and becomes the root for everything after it.
        let mut root: Option<strong_ref::Main> = reply.as_ref().map(|r| r.root.clone());
        let mut parent: Option<strong_ref::Main> = reply.as_ref().map(|r| r.parent.clone());

        for segment in segments {
            let reply_ref = match (&root, &parent) {
                (Some(r), Some(p)) => Some(
                    post::ReplyRefData {
                        parent: p.clone(),
                        root: r.clone(),
                    }
                    .into(),
                ),
                _ => None,
            };
            let mut record = ctx.build_post(&segment, reply_ref).await?;
            record.langs = langs.clone();
            let out = ctx.post_record(record).await?;

            let this_ref = output_to_ref(&out);
            // The first post of a standalone thread becomes its root.
            if root.is_none() {
                root = Some(this_ref.clone());
            }
            parent = Some(this_ref);
            outputs.push(out);
        }

        Ok(outputs)
    }
}

/// Build a [`strong_ref`] pointing at a just-created record, so it can serve as
/// the parent/root of the next post in a thread.
fn output_to_ref(out: &create_record::Output) -> strong_ref::Main {
    strong_ref::MainData {
        cid: out.cid.clone(),
        uri: out.uri.clone(),
    }
    .into()
}

// --- splitting (pure, unit-tested) -----------------------------------------

/// Turn the builder's raw pieces into the final list of post texts: split each
/// piece to fit, then (optionally) append ` i/N` numbering, reserving grapheme
/// budget for the suffix so every numbered post still fits within `limit`.
fn plan_segments(pieces: &[String], numbered: bool, limit: usize) -> Vec<String> {
    let split_all = |reserve: usize| -> Vec<String> {
        let per_post = limit.saturating_sub(reserve).max(1);
        pieces
            .iter()
            .flat_map(|p| split_into_chunks(p, per_post))
            .collect::<Vec<_>>()
    };

    if !numbered {
        return split_all(0);
    }

    // Numbering the posts costs graphemes, and how many depends on the post count
    // — which depends on the reserved budget. Iterate to a fixed point: the digit
    // width only grows as the count crosses a power of ten, so this settles fast.
    let mut segments = split_all(0);
    if segments.is_empty() {
        return segments;
    }
    let mut count = segments.len();
    for _ in 0..8 {
        let reserve = numbering_reserve(count);
        let next = split_all(reserve);
        let stable = next.len() == count;
        count = next.len();
        segments = next;
        if stable {
            break;
        }
    }

    let total = segments.len();
    if total <= 1 {
        // A thread that fits in one post reads better without a "1/1" tag.
        return segments;
    }
    segments
        .into_iter()
        .enumerate()
        .map(|(i, s)| format!("{s} {}/{total}", i + 1))
        .collect()
}

/// Worst-case grapheme length of a ` i/N` suffix when there are `total` posts.
/// Every digit / slash / space is a single grapheme, and `i` never has more
/// digits than `total`, so reserving for two `total`-width numbers is always
/// enough.
fn numbering_reserve(total: usize) -> usize {
    let digits = total.max(1).to_string().len();
    // one space + `i` + one slash + `N`
    2 + 2 * digits
}

/// Whether a grapheme cluster is entirely whitespace (a space, tab, or newline).
fn is_whitespace(grapheme: &str) -> bool {
    !grapheme.is_empty() && grapheme.chars().all(char::is_whitespace)
}

/// Split `text` into chunks of at most `limit` grapheme clusters, breaking at
/// whitespace where possible and never splitting a single grapheme. Leading and
/// trailing whitespace of each chunk is trimmed; empty chunks are dropped. A
/// single token longer than `limit` (e.g. a very long URL) is hard-split.
fn split_into_chunks(text: &str, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    let graphemes: Vec<&str> = text.graphemes(true).collect();
    let n = graphemes.len();
    let mut chunks: Vec<String> = Vec::new();
    let mut start = 0usize;

    while start < n {
        // Don't let a chunk begin with whitespace.
        while start < n && is_whitespace(graphemes[start]) {
            start += 1;
        }
        if start >= n {
            break;
        }

        // Everything that remains fits in one post.
        if n - start <= limit {
            push_chunk(&mut chunks, &graphemes[start..n]);
            break;
        }

        // The chunk can extend no further than `window_end` (< n here). Prefer to
        // break at the last whitespace at or before it, so whole words stay
        // together; fall back to a hard split when a token has no break point.
        let window_end = start + limit;
        let mut break_at: Option<usize> = None;
        let mut i = window_end;
        while i > start {
            if is_whitespace(graphemes[i]) {
                break_at = Some(i);
                break;
            }
            if i == start + 1 {
                break;
            }
            i -= 1;
        }

        match break_at {
            Some(bi) => {
                push_chunk(&mut chunks, &graphemes[start..bi]);
                start = bi;
            }
            None => {
                push_chunk(&mut chunks, &graphemes[start..window_end]);
                start = window_end;
            }
        }
    }

    chunks
}

/// Concatenate a grapheme slice, trim its ends, and push it unless it is empty.
fn push_chunk(chunks: &mut Vec<String>, graphemes: &[&str]) {
    let joined = graphemes.concat();
    let trimmed = joined.trim();
    if !trimmed.is_empty() {
        chunks.push(trimmed.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Count grapheme clusters the same way Bluesky (and the splitter) does.
    fn graphemes(s: &str) -> usize {
        s.graphemes(true).count()
    }

    // --- basic splitting: one behavior per test ----------------------------

    #[test]
    fn short_text_stays_a_single_chunk() {
        let chunks = split_into_chunks("hello world", 300);
        assert_eq!(chunks, vec!["hello world".to_string()]);
    }

    #[test]
    fn empty_and_whitespace_only_yield_no_chunks() {
        assert!(split_into_chunks("", 300).is_empty());
        assert!(split_into_chunks("   \n\t  ", 300).is_empty());
    }

    #[test]
    fn text_at_exactly_the_limit_is_not_split() {
        let text = "a".repeat(10);
        let chunks = split_into_chunks(&text, 10);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], text);
    }

    #[test]
    fn text_one_over_the_limit_splits_in_two() {
        // 11 non-breakable graphemes, limit 10 -> two chunks (hard split).
        let text = "a".repeat(11);
        let chunks = split_into_chunks(&text, 10);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0], "a".repeat(10));
        assert_eq!(chunks[1], "a");
    }

    // --- word-boundary behavior --------------------------------------------

    #[test]
    fn breaks_at_whitespace_not_mid_word() {
        // limit 10: "hello" (5) + " " + "world" (5) would be 11, so "world"
        // moves to the next chunk rather than being cut.
        let chunks = split_into_chunks("hello world foo", 10);
        assert_eq!(chunks[0], "hello");
        assert!(chunks.iter().all(|c| graphemes(c) <= 10));
        // No chunk starts or ends with whitespace.
        assert!(
            chunks
                .iter()
                .all(|c| c.trim() == c.as_str() && !c.is_empty())
        );
    }

    #[test]
    fn no_chunk_ever_exceeds_the_limit() {
        let text = "The quick brown fox jumps over the lazy dog. ".repeat(40);
        for limit in [12usize, 30, 50, 100, 300] {
            let chunks = split_into_chunks(&text, limit);
            for c in &chunks {
                assert!(
                    graphemes(c) <= limit,
                    "chunk {c:?} has {} graphemes, over limit {limit}",
                    graphemes(c)
                );
            }
        }
    }

    #[test]
    fn splitting_preserves_words_in_order() {
        let text = "one two three four five six seven eight nine ten";
        let chunks = split_into_chunks(text, 13);
        // Rejoining the chunks reproduces the original word sequence.
        let rejoined = chunks.join(" ");
        assert_eq!(
            rejoined.split_whitespace().collect::<Vec<_>>(),
            text.split_whitespace().collect::<Vec<_>>()
        );
    }

    #[test]
    fn oversized_single_token_is_hard_split_without_loss() {
        // A URL-like token with no spaces, longer than the limit.
        let token = format!("https://example.com/{}", "x".repeat(50));
        let chunks = split_into_chunks(&token, 20);
        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|c| graphemes(c) <= 20));
        // Concatenating recovers the token exactly (no graphemes dropped).
        assert_eq!(chunks.concat(), token);
    }

    // --- grapheme-cluster awareness ----------------------------------------

    #[test]
    fn multi_codepoint_grapheme_counts_as_one() {
        // A family emoji is a single extended grapheme cluster (many codepoints).
        let family = "👨‍👩‍👧‍👦";
        assert_eq!(graphemes(family), 1);
        let text = family.repeat(5);
        assert_eq!(split_into_chunks(&text, 5), vec![text.clone()]);
    }

    #[test]
    fn never_splits_inside_a_grapheme_cluster() {
        let family = "👨‍👩‍👧‍👦";
        let text = family.repeat(5); // 5 clusters
        let chunks = split_into_chunks(&text, 2); // no whitespace -> hard split
        // Each cluster stays intact: every chunk is a whole number of families,
        // each chunk holds <= 2 clusters, and nothing is lost.
        assert!(chunks.iter().all(|c| graphemes(c) <= 2));
        assert_eq!(chunks.concat(), text);
        assert!(
            chunks
                .iter()
                .all(|c| c.chars().count() % family.chars().count() == 0)
        );
    }

    // --- numbering (plan_segments) -----------------------------------------

    #[test]
    fn unnumbered_plan_leaves_text_alone() {
        let pieces = vec!["a".repeat(11)];
        let segs = plan_segments(&pieces, false, 10);
        assert_eq!(segs, vec!["a".repeat(10), "a".to_string()]);
    }

    #[test]
    fn numbering_appends_index_over_total() {
        let pieces = vec!["one two three four five six".to_string()];
        let segs = plan_segments(&pieces, true, 10);
        let total = segs.len();
        assert!(total > 1);
        for (i, s) in segs.iter().enumerate() {
            assert!(
                s.ends_with(&format!(" {}/{total}", i + 1)),
                "segment {s:?} is not numbered {}/{total}",
                i + 1
            );
        }
    }

    #[test]
    fn numbered_segments_stay_within_the_limit() {
        // Long text -> a many-part numbered thread; every part (suffix included)
        // must fit the limit.
        let pieces = vec!["word ".repeat(200)];
        let segs = plan_segments(&pieces, true, 40);
        assert!(segs.len() > 1);
        for s in &segs {
            assert!(
                graphemes(s) <= 40,
                "numbered segment {s:?} has {} graphemes, over 40",
                graphemes(s)
            );
        }
    }

    #[test]
    fn numbering_total_is_self_consistent_across_digit_boundary() {
        // Enough content to produce a two-digit part count; the reserved suffix
        // width must not desync the announced total from the real one.
        let pieces = vec!["alpha bravo charlie delta echo foxtrot ".repeat(30)];
        let segs = plan_segments(&pieces, true, 30);
        let total = segs.len();
        assert!(total >= 10, "expected a two-digit thread, got {total}");
        // The "/N" every post advertises equals the real number of posts.
        assert!(segs.iter().all(|s| s.ends_with(&format!("/{total}"))));
        assert!(segs.iter().all(|s| graphemes(s) <= 30));
    }

    #[test]
    fn single_post_thread_is_not_numbered() {
        let pieces = vec!["short".to_string()];
        let segs = plan_segments(&pieces, true, 300);
        assert_eq!(segs, vec!["short".to_string()]);
    }

    // --- multiple pieces ---------------------------------------------------

    #[test]
    fn each_piece_is_at_least_one_post_and_pieces_are_not_merged() {
        let pieces = vec!["first".to_string(), "second".to_string()];
        let segs = plan_segments(&pieces, false, 300);
        assert_eq!(segs, vec!["first".to_string(), "second".to_string()]);
    }

    #[test]
    fn a_long_piece_among_short_ones_expands_in_place() {
        let pieces = vec![
            "intro".to_string(),
            "a".repeat(25), // splits into 3 at limit 10
            "outro".to_string(),
        ];
        let segs = plan_segments(&pieces, false, 10);
        assert_eq!(segs.first().map(String::as_str), Some("intro"));
        assert_eq!(segs.last().map(String::as_str), Some("outro"));
        // intro + (10 + 10 + 5) + outro = 5 posts
        assert_eq!(segs.len(), 5);
    }

    #[test]
    fn empty_pieces_produce_no_segments() {
        let pieces = vec!["".to_string(), "   ".to_string()];
        assert!(plan_segments(&pieces, false, 300).is_empty());
        assert!(plan_segments(&pieces, true, 300).is_empty());
    }

    // --- output -> ref chaining --------------------------------------------

    #[test]
    fn output_ref_carries_uri_and_cid() {
        let out: create_record::Output = serde_json::from_value(serde_json::json!({
            "cid": "bafyreiclp443lavogvhj3d2ob2cxbfuscni2k5jk7bebjzg7khl3esabwq",
            "uri": "at://did:plc:alice000000000000000000/app.bsky.feed.post/xyz",
        }))
        .expect("valid createRecord output");
        let strong = output_to_ref(&out);
        assert_eq!(strong.uri, out.uri);
        assert_eq!(strong.cid, out.cid);
    }
}
