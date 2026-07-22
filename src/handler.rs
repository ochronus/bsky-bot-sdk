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

pub(crate) type HandlerFn =
    Arc<dyn Fn(Context, Notification) -> BoxFuture<Result<()>> + Send + Sync>;

pub(crate) type ErrorHandlerFn =
    Arc<dyn Fn(Context, Notification, Error) -> BoxFuture<()> + Send + Sync>;

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

/// The registry of handlers for a bot.
#[derive(Default, Clone)]
pub(crate) struct Handlers {
    by_reason: HashMap<NotificationReason, Vec<HandlerFn>>,
    any: Vec<HandlerFn>,
    on_error: Option<ErrorHandlerFn>,
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

    /// True when no reason-specific and no catch-all handlers are registered.
    pub(crate) fn is_empty(&self) -> bool {
        self.by_reason.is_empty() && self.any.is_empty()
    }

    /// Dispatch a single notification to every handler that applies: first the
    /// reason-specific ones (in registration order), then the catch-all ones.
    ///
    /// A handler returning `Err` never aborts the batch; the error is routed to
    /// the registered error handler, or logged if there is none.
    pub(crate) async fn dispatch(&self, ctx: Context, notif: Notification) {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::BotIdentity;
    use std::sync::Mutex;

    async fn test_context() -> Context {
        // Building an agent performs no network I/O when there is no session.
        let agent = bsky_sdk::BskyAgent::builder()
            .build()
            .await
            .expect("build agent");
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
}
