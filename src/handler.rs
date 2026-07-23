//! Handler registration and dispatch.
//!
//! Handlers are async closures of the shape
//! `Fn(Context, Notification) -> Future<Output = Result<()>>`. They are stored as
//! boxed, reference-counted trait objects so many can be registered per reason.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::event::{Notification, NotificationReason};

/// A boxed, `Send` future — the erased return type of a handler.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Whether a [`before`](crate::BotBuilder::before) middleware lets a notification
/// reach the handlers, or drops it.
///
/// Returned by middleware registered with [`before`](crate::BotBuilder::before);
/// the convenience filters ([`block_authors`](crate::BotBuilder::block_authors),
/// [`allow_authors`](crate::BotBuilder::allow_authors),
/// [`ignore_self`](crate::BotBuilder::ignore_self)) produce it for you.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    /// Continue to the next middleware, then the handlers.
    Continue,
    /// Drop this notification: skip the remaining middleware, every handler, and
    /// the `after` chain.
    Skip,
}

pub(crate) type HandlerFn =
    Arc<dyn Fn(Context, Notification) -> BoxFuture<Result<()>> + Send + Sync>;

pub(crate) type ErrorHandlerFn =
    Arc<dyn Fn(Context, Notification, Error) -> BoxFuture<()> + Send + Sync>;

pub(crate) type MiddlewareFn = Arc<dyn Fn(Context, Notification) -> BoxFuture<Flow> + Send + Sync>;

pub(crate) type AfterFn = Arc<dyn Fn(Context, Notification) -> BoxFuture<()> + Send + Sync>;

/// Erase a concrete async handler closure into a [`HandlerFn`].
pub(crate) fn boxed_handler<F, Fut>(handler: F) -> HandlerFn
where
    F: Fn(Context, Notification) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    Arc::new(move |ctx, notif| Box::pin(handler(ctx, notif)))
}

/// Erase a concrete async error handler into an [`ErrorHandlerFn`].
pub(crate) fn boxed_error_handler<F, Fut>(handler: F) -> ErrorHandlerFn
where
    F: Fn(Context, Notification, Error) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Arc::new(move |ctx, notif, err| Box::pin(handler(ctx, notif, err)))
}

/// Erase a concrete async `before` middleware into a [`MiddlewareFn`].
pub(crate) fn boxed_middleware<F, Fut>(filter: F) -> MiddlewareFn
where
    F: Fn(Context, Notification) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Flow> + Send + 'static,
{
    Arc::new(move |ctx, notif| Box::pin(filter(ctx, notif)))
}

/// Erase a concrete async `after` hook into an [`AfterFn`].
pub(crate) fn boxed_after<F, Fut>(hook: F) -> AfterFn
where
    F: Fn(Context, Notification) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Arc::new(move |ctx, notif| Box::pin(hook(ctx, notif)))
}

/// The registry of handlers for a bot.
#[derive(Default, Clone)]
pub(crate) struct Handlers {
    by_reason: HashMap<NotificationReason, Vec<HandlerFn>>,
    any: Vec<HandlerFn>,
    on_error: Option<ErrorHandlerFn>,
    /// Middleware run (in order) before any handler; a [`Flow::Skip`] drops the
    /// notification.
    before: Vec<MiddlewareFn>,
    /// Hooks run (in order) after all handlers, unless a `before` middleware
    /// skipped the notification.
    after: Vec<AfterFn>,
}

impl Handlers {
    pub(crate) fn register(&mut self, reason: NotificationReason, handler: HandlerFn) {
        self.by_reason.entry(reason).or_default().push(handler);
    }

    pub(crate) fn register_any(&mut self, handler: HandlerFn) {
        self.any.push(handler);
    }

    pub(crate) fn set_error(&mut self, handler: ErrorHandlerFn) {
        self.on_error = Some(handler);
    }

    pub(crate) fn add_before(&mut self, mw: MiddlewareFn) {
        self.before.push(mw);
    }

    pub(crate) fn add_after(&mut self, hook: AfterFn) {
        self.after.push(hook);
    }

    /// True when no reason-specific and no catch-all handlers are registered.
    pub(crate) fn is_empty(&self) -> bool {
        self.by_reason.is_empty() && self.any.is_empty()
    }

    /// Dispatch a single notification: run the `before` middleware (any
    /// [`Flow::Skip`] drops it), then every applicable handler — reason-specific
    /// ones first (in registration order), then the catch-alls — then the `after`
    /// hooks.
    ///
    /// A handler returning `Err` never aborts the batch; the error is routed to
    /// the registered error handler, or logged if there is none.
    pub(crate) async fn dispatch(&self, ctx: Context, notif: Notification) {
        // Pre-filters: the first that returns `Skip` drops the notification before
        // any handler or `after` hook runs.
        for mw in &self.before {
            if mw(ctx.clone(), notif.clone()).await == Flow::Skip {
                tracing::trace!(uri = %notif.uri(), "notification skipped by middleware");
                return;
            }
        }

        let reason = notif.reason();
        // Reason-specific handlers first, then catch-alls — without allocating.
        let selected = self
            .by_reason
            .get(&reason)
            .into_iter()
            .flatten()
            .chain(self.any.iter());

        for handler in selected {
            if let Err(err) = handler(ctx.clone(), notif.clone()).await {
                match &self.on_error {
                    Some(on_error) => on_error(ctx.clone(), notif.clone(), err).await,
                    None => tracing::error!(
                        reason = %reason,
                        uri = %notif.uri(),
                        author = %notif.author_handle(),
                        error = %err,
                        "handler returned an error",
                    ),
                }
            }
        }

        for hook in &self.after {
            hook(ctx.clone(), notif.clone()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::BotIdentity;
    use std::sync::Mutex;

    async fn test_context() -> Context {
        // Building an agent performs no network I/O when there is no session.
        let agent = crate::ratelimit::test_agent().await;
        let identity = Arc::new(BotIdentity::new(
            "did:plc:bot00000000000000000000000"
                .parse()
                .expect("valid did"),
            "bot.test".parse().expect("valid handle"),
        ));
        Context::new(agent, identity, crate::ratelimit::WriteBudget::new(None))
    }

    fn notif(reason: &str) -> Notification {
        let value = serde_json::json!({
            "author": { "did": "did:plc:alice000000000000000000", "handle": "alice.test" },
            "cid": "bafyreiclp443lavogvhj3d2ob2cxbfuscni2k5jk7bebjzg7khl3esabwq",
            "indexedAt": "2026-07-22T10:00:00.000Z",
            "isRead": false,
            "reason": reason,
            "record": { "$type": "app.bsky.feed.post", "text": "hi", "createdAt": "2026-07-22T10:00:00.000Z" },
            "uri": "at://x/1",
        });
        Notification::new(serde_json::from_value(value).expect("valid notification fixture"))
    }

    #[tokio::test]
    async fn routes_to_reason_specific_and_catch_all_handlers() {
        let log = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut handlers = Handlers::default();

        let mention_log = Arc::clone(&log);
        handlers.register(
            NotificationReason::Mention,
            boxed_handler(move |_ctx, _n| {
                let log = Arc::clone(&mention_log);
                async move {
                    log.lock().unwrap().push("mention".to_string());
                    Ok(())
                }
            }),
        );
        let any_log = Arc::clone(&log);
        handlers.register_any(boxed_handler(move |_ctx, _n| {
            let log = Arc::clone(&any_log);
            async move {
                log.lock().unwrap().push("any".to_string());
                Ok(())
            }
        }));

        let ctx = test_context().await;
        handlers.dispatch(ctx.clone(), notif("mention")).await;
        assert_eq!(
            &*log.lock().unwrap(),
            &["mention", "any"],
            "reason handler then catch-all"
        );

        log.lock().unwrap().clear();
        // A follow has no reason-specific handler, so only the catch-all fires.
        handlers.dispatch(ctx, notif("follow")).await;
        assert_eq!(
            &*log.lock().unwrap(),
            &["any"],
            "unmatched reason hits only catch-all"
        );
    }

    #[tokio::test]
    async fn handler_errors_are_routed_to_the_error_handler() {
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut handlers = Handlers::default();

        handlers.register(
            NotificationReason::Mention,
            boxed_handler(|_ctx, _n| async move { Err(Error::invalid_input("boom")) }),
        );
        let err_seen = Arc::clone(&seen);
        handlers.set_error(boxed_error_handler(move |_ctx, _n, err| {
            let seen = Arc::clone(&err_seen);
            async move {
                seen.lock().unwrap().push(err.to_string());
            }
        }));

        let ctx = test_context().await;
        handlers.dispatch(ctx, notif("mention")).await;

        let seen = seen.lock().unwrap();
        assert_eq!(
            seen.len(),
            1,
            "the error handler should have been invoked once"
        );
        assert!(
            seen[0].contains("boom"),
            "error payload should be forwarded: {}",
            seen[0]
        );
    }

    #[tokio::test]
    async fn one_handler_error_does_not_prevent_the_next_handler() {
        let ran = Arc::new(Mutex::new(false));
        let mut handlers = Handlers::default();

        handlers.register(
            NotificationReason::Mention,
            boxed_handler(|_ctx, _n| async move { Err(Error::invalid_input("first fails")) }),
        );
        let ran_flag = Arc::clone(&ran);
        handlers.register(
            NotificationReason::Mention,
            boxed_handler(move |_ctx, _n| {
                let ran = Arc::clone(&ran_flag);
                async move {
                    *ran.lock().unwrap() = true;
                    Ok(())
                }
            }),
        );

        let ctx = test_context().await;
        handlers.dispatch(ctx, notif("mention")).await;
        assert!(
            *ran.lock().unwrap(),
            "a failing handler must not skip subsequent handlers"
        );
    }

    #[test]
    fn is_empty_reflects_registration() {
        let mut handlers = Handlers::default();
        assert!(handlers.is_empty());
        handlers.register_any(boxed_handler(|_c, _n| async move { Ok(()) }));
        assert!(!handlers.is_empty());
    }

    #[tokio::test]
    async fn before_skip_prevents_handlers_and_after_hooks() {
        let log = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut handlers = Handlers::default();

        // A `before` that always skips.
        handlers.add_before(boxed_middleware(|_c, _n| async move { Flow::Skip }));

        let handler_log = Arc::clone(&log);
        handlers.register_any(boxed_handler(move |_c, _n| {
            let log = Arc::clone(&handler_log);
            async move {
                log.lock().unwrap().push("handler".into());
                Ok(())
            }
        }));
        let after_log = Arc::clone(&log);
        handlers.add_after(boxed_after(move |_c, _n| {
            let log = Arc::clone(&after_log);
            async move {
                log.lock().unwrap().push("after".into());
            }
        }));

        let ctx = test_context().await;
        handlers.dispatch(ctx, notif("mention")).await;
        assert!(
            log.lock().unwrap().is_empty(),
            "a skip must run neither the handler nor the after hook",
        );
    }

    #[tokio::test]
    async fn before_continue_runs_handler_then_after_in_order() {
        let log = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut handlers = Handlers::default();

        let before_log = Arc::clone(&log);
        handlers.add_before(boxed_middleware(move |_c, _n| {
            let log = Arc::clone(&before_log);
            async move {
                log.lock().unwrap().push("before".into());
                Flow::Continue
            }
        }));
        let handler_log = Arc::clone(&log);
        handlers.register_any(boxed_handler(move |_c, _n| {
            let log = Arc::clone(&handler_log);
            async move {
                log.lock().unwrap().push("handler".into());
                Ok(())
            }
        }));
        let after_log = Arc::clone(&log);
        handlers.add_after(boxed_after(move |_c, _n| {
            let log = Arc::clone(&after_log);
            async move {
                log.lock().unwrap().push("after".into());
            }
        }));

        let ctx = test_context().await;
        handlers.dispatch(ctx, notif("mention")).await;
        assert_eq!(
            &*log.lock().unwrap(),
            &["before", "handler", "after"],
            "the pipeline runs before → handler → after in order",
        );
    }

    #[tokio::test]
    async fn a_later_middleware_skip_short_circuits_earlier_continues() {
        let ran_handler = Arc::new(Mutex::new(false));
        let mut handlers = Handlers::default();

        handlers.add_before(boxed_middleware(|_c, _n| async move { Flow::Continue }));
        handlers.add_before(boxed_middleware(|_c, _n| async move { Flow::Skip }));

        let flag = Arc::clone(&ran_handler);
        handlers.register_any(boxed_handler(move |_c, _n| {
            let flag = Arc::clone(&flag);
            async move {
                *flag.lock().unwrap() = true;
                Ok(())
            }
        }));

        let ctx = test_context().await;
        handlers.dispatch(ctx, notif("mention")).await;
        assert!(
            !*ran_handler.lock().unwrap(),
            "a Skip from any middleware must stop the pipeline",
        );
    }
}
