//! Transparent pagination for the read endpoints.
//!
//! Bluesky's list/feed endpoints return one page plus a `cursor` for the next.
//! [`Paginated`] hides that cursor loop behind an async stream: each page is
//! fetched lazily as you consume items, so `ctx.followers(..)` reads like a single
//! sequence even though it spans many round-trips. Read helpers on
//! [`Context`](crate::Context) — [`timeline`](crate::Context::timeline),
//! [`followers`](crate::Context::followers),
//! [`following`](crate::Context::following),
//! [`user_posts`](crate::Context::user_posts) — return one of these.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use futures_util::StreamExt;
use futures_util::stream::{self, BoxStream, Stream};

use crate::error::{Error, Result};

/// One page fetched from a paginating endpoint: a batch of items plus the cursor
/// to fetch the next page (`None` when the data is exhausted).
pub(crate) struct Page<T> {
    pub items: Vec<T>,
    pub cursor: Option<String>,
}

/// State threaded through the pagination unfold: the not-yet-yielded items of the
/// current page, the cursor for the next page, and whether the server has told us
/// there is no next page.
struct PageState<T> {
    cursor: Option<String>,
    buffer: std::vec::IntoIter<T>,
    exhausted: bool,
}

/// Build a [`Paginated`] stream from a `fetch` closure that maps a cursor to one
/// [`Page`]. The stream yields each item in order, fetching the next page only
/// once the current one is drained, and ends when a page comes back empty or
/// without a next cursor.
pub(crate) fn paginate<T, F, Fut>(fetch: F) -> Paginated<T>
where
    T: Send + 'static,
    F: Fn(Option<String>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<Page<T>>> + Send + 'static,
{
    let init = PageState {
        cursor: None,
        buffer: Vec::new().into_iter(),
        exhausted: false,
    };
    let stream = stream::try_unfold(init, move |mut state| {
        let fetch = fetch.clone();
        async move {
            loop {
                if let Some(item) = state.buffer.next() {
                    return Ok(Some((item, state)));
                }
                if state.exhausted {
                    return Ok(None);
                }
                let page = fetch(state.cursor.take()).await?;
                // An empty page ends the stream regardless of any cursor, so a
                // server that returns an empty page with a repeating cursor can
                // never spin this loop forever.
                if page.items.is_empty() {
                    return Ok(None);
                }
                state.exhausted = page.cursor.is_none();
                state.cursor = page.cursor;
                state.buffer = page.items.into_iter();
            }
        }
    });
    Paginated {
        inner: stream.boxed(),
    }
}

/// An async stream over a Bluesky list or feed that pages transparently.
///
/// Consume it with the inherent [`next`](Paginated::next) in a `while let` loop,
/// drain a bounded list with [`collect_all`](Paginated::collect_all), or cap an
/// unbounded feed with [`take`](Paginated::take). It also implements
/// [`futures_util::Stream`], so the wider combinator ecosystem works if you bring
/// your own `StreamExt`.
///
/// ```no_run
/// # use bsky_bot_sdk::prelude::*;
/// # async fn f(ctx: Context) -> Result<()> {
/// // Print the 20 most recent posts on the home timeline.
/// let mut feed = ctx.timeline().take(20);
/// while let Some(item) = feed.next().await {
///     let post = item?;
///     println!("{}", post.post.author.handle.as_str());
/// }
/// # Ok(())
/// # }
/// ```
#[must_use = "a Paginated stream does nothing until you consume it"]
pub struct Paginated<T> {
    inner: BoxStream<'static, Result<T>>,
}

impl<T: Send + 'static> Paginated<T> {
    /// A stream that yields a single error, for a helper that fails before it can
    /// begin paging (e.g. an unparseable actor identifier).
    pub(crate) fn once_err(err: Error) -> Self {
        Self {
            inner: stream::once(async move { Err(err) }).boxed(),
        }
    }

    /// Fetch the next item, or `None` at the end of the data. A failed page surfaces
    /// as `Some(Err(_))`, after which the stream ends.
    pub async fn next(&mut self) -> Option<Result<T>> {
        self.inner.next().await
    }

    /// Collect **all** remaining items into a `Vec`, following the cursor to the
    /// end and stopping at the first error.
    ///
    /// Use this only on bounded lists (followers, follows, list members). Feeds
    /// like the timeline page back through the entire history; bound them with
    /// [`take`](Paginated::take) first.
    pub async fn collect_all(mut self) -> Result<Vec<T>> {
        let mut out = Vec::new();
        while let Some(item) = self.inner.next().await {
            out.push(item?);
        }
        Ok(out)
    }

    /// Limit the stream to at most `n` items, after which it ends without fetching
    /// further pages. Essential for unbounded feeds.
    pub fn take(self, n: usize) -> Paginated<T> {
        Paginated {
            inner: self.inner.take(n).boxed(),
        }
    }
}

impl<T> Stream for Paginated<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        // `inner` is a `Pin<Box<..>>`, which is `Unpin`, so `Paginated` is `Unpin`
        // and we can freely take a mutable reference to it.
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Drive `paginate` over an in-memory list of pages `(items, next_cursor)`,
    /// recording how many fetches happened so we can prove laziness.
    fn canned(pages: Vec<(Vec<i32>, Option<&'static str>)>) -> (Paginated<i32>, Arc<Mutex<usize>>) {
        let pages = Arc::new(pages);
        let calls = Arc::new(Mutex::new(0usize));
        let calls_inner = Arc::clone(&calls);
        let stream = paginate(move |cursor| {
            let pages = Arc::clone(&pages);
            let calls = Arc::clone(&calls_inner);
            async move {
                *calls.lock().unwrap() += 1;
                // The cursor is the *index* of the page to return, as a string;
                // the first fetch (cursor `None`) returns page 0.
                let idx: usize = cursor.as_deref().map(|c| c.parse().unwrap()).unwrap_or(0);
                let (items, next) = pages[idx].clone();
                Ok(Page {
                    items,
                    cursor: next.map(str::to_string),
                })
            }
        });
        (stream, calls)
    }

    #[tokio::test]
    async fn pages_are_flattened_in_order_across_the_cursor() {
        let (stream, _) = canned(vec![
            (vec![1, 2], Some("1")),
            (vec![3, 4], Some("2")),
            (vec![5], None),
        ]);
        let all = stream.collect_all().await.expect("no error");
        assert_eq!(all, vec![1, 2, 3, 4, 5], "all pages, concatenated in order");
    }

    #[tokio::test]
    async fn an_empty_page_ends_the_stream_even_with_a_cursor() {
        // Page 1 is empty but still carries a (bogus) cursor; the stream must stop
        // rather than loop forever.
        let (stream, calls) = canned(vec![(vec![1, 2], Some("1")), (vec![], Some("1"))]);
        let all = stream.collect_all().await.expect("no error");
        assert_eq!(all, vec![1, 2]);
        assert_eq!(
            *calls.lock().unwrap(),
            2,
            "fetched exactly twice, then stopped"
        );
    }

    #[tokio::test]
    async fn take_bounds_the_stream_and_stops_early() {
        // Three pages available, but taking 3 items must not fetch the third page.
        let (stream, calls) = canned(vec![
            (vec![1, 2], Some("1")),
            (vec![3, 4], Some("2")),
            (vec![5, 6], None),
        ]);
        let some = stream.take(3).collect_all().await.expect("no error");
        assert_eq!(some, vec![1, 2, 3]);
        assert_eq!(
            *calls.lock().unwrap(),
            2,
            "take(3) should read only the first two pages, never the third",
        );
    }

    #[tokio::test]
    async fn next_yields_items_one_at_a_time_then_none() {
        let (mut stream, _) = canned(vec![(vec![7], Some("1")), (vec![8], None)]);
        assert_eq!(stream.next().await.transpose().unwrap(), Some(7));
        assert_eq!(stream.next().await.transpose().unwrap(), Some(8));
        assert!(
            stream.next().await.is_none(),
            "exhausted stream yields None"
        );
    }

    #[tokio::test]
    async fn once_err_yields_a_single_error() {
        let mut stream = Paginated::<i32>::once_err(Error::invalid_input("bad actor"));
        let first = stream.next().await.expect("one item");
        assert!(matches!(first, Err(Error::InvalidInput(_))));
        assert!(
            stream.next().await.is_none(),
            "only the one error, then done"
        );
    }
}
