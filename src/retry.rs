//! Retrying transient failures with exponential backoff + jitter.
//!
//! A polling bot that dies — or gives up for a whole poll interval — on a passing
//! network blip isn't production-grade. This module wraps a fallible async
//! operation and retries it a bounded number of times when the failure looks
//! *transient* (a 5xx, a 429, a request timeout, or a bare transport error),
//! while passing *permanent* failures (bad input, auth, not-found) straight
//! through — retrying those would only waste time.
//!
//! The backoff schedule reuses the same [`Backoff`] (and clock-derived jitter)
//! the Jetstream reconnect loop uses, so there is one backoff implementation in
//! the crate, not two.
//!
//! Retries are applied to **idempotent** operations only — reads and the
//! cursor-based poll loops. Record *writes* are deliberately never auto-retried
//! here: a create that succeeds on the server but whose response is lost would be
//! duplicated by a blind retry (double-post). Safe write retries need an
//! idempotency key, which is a separate concern.

use std::future::Future;
use std::time::Duration;

use crate::error::Error;
use crate::stream::{Backoff, jitter_unit};

/// How to retry an operation that fails transiently.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of *re*-tries after the initial attempt. Total attempts is
    /// therefore `max_retries + 1`; `0` disables retrying.
    pub max_retries: u32,
    /// The exponential-backoff-with-jitter schedule between attempts.
    pub backoff: Backoff,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        // Snappier than the stream reconnect default: a few quick tries so a
        // blip is ridden out within a poll interval, not a 30s reconnect cap.
        Self {
            max_retries: 3,
            backoff: Backoff {
                initial: Duration::from_millis(250),
                max: Duration::from_secs(10),
                factor: 2.0,
            },
        }
    }
}

impl RetryPolicy {
    /// A policy that never retries (total attempts = 1).
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            backoff: Backoff::default(),
        }
    }
}

/// Whether an error looks *transient* — worth retrying — as opposed to a
/// permanent client error that a retry could not fix.
pub(crate) fn is_transient(err: &Error) -> bool {
    match err {
        Error::Sdk(bsky_sdk::Error::Xrpc(xrpc)) => is_transient_xrpc(xrpc),
        // A non-XRPC HTTP call (link-card OpenGraph fetch, video service): a
        // network hiccup there is worth another try.
        Error::Http(_) => true,
        // Everything else — invalid input, missing credentials, not-authenticated,
        // decode failures, i/o — is not something a retry resolves.
        _ => false,
    }
}

/// Classify an XRPC failure. Server-side and transport failures are transient;
/// 4xx client errors (other than 408/429) are permanent.
fn is_transient_xrpc(err: &bsky_sdk::error::GenericXrpcError) -> bool {
    use bsky_sdk::error::GenericXrpcError;
    match err {
        GenericXrpcError::Response { status, .. } => {
            let code = status.as_u16();
            // 408 Request Timeout and 429 Too Many Requests recover on their own;
            // any 5xx is a server-side fault. All other 4xx are permanent.
            code == 408 || code == 429 || status.is_server_error()
        }
        // A transport-level failure (connection reset, DNS, TLS) with no HTTP
        // response at all — retry.
        GenericXrpcError::Other(_) => true,
    }
}

/// Run `op`, retrying transient failures per `policy` with jittered backoff.
///
/// Returns the first `Ok`, or the last error once retries are exhausted or a
/// permanent error is hit (that error is returned immediately, un-retried).
/// `op` is invoked afresh on each attempt, so it must build a new future each
/// call.
pub(crate) async fn retry<T, F, Fut>(policy: &RetryPolicy, mut op: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut attempt: u32 = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt >= policy.max_retries || !is_transient(&err) {
                    return Err(err);
                }
                let delay = policy.backoff.delay_with_jitter(attempt, jitter_unit());
                tracing::debug!(
                    attempt = attempt + 1,
                    max = policy.max_retries,
                    delay_ms = delay.as_millis() as u64,
                    error = %err,
                    "retrying transient error",
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atrium_api::xrpc::http::StatusCode;
    use std::cell::Cell;

    /// A retry policy with zero backoff, so the combinator's timing is exercised
    /// without any real sleeping (keeps the tests instant and dependency-free —
    /// no need for tokio's `test-util` paused clock).
    fn fast(max_retries: u32) -> RetryPolicy {
        RetryPolicy {
            max_retries,
            backoff: Backoff {
                initial: Duration::ZERO,
                max: Duration::ZERO,
                factor: 2.0,
            },
        }
    }

    fn xrpc_status(code: u16) -> Error {
        Error::Sdk(bsky_sdk::Error::Xrpc(Box::new(
            bsky_sdk::error::GenericXrpcError::Response {
                status: StatusCode::from_u16(code).unwrap(),
                error: None,
            },
        )))
    }

    fn xrpc_transport() -> Error {
        Error::Sdk(bsky_sdk::Error::Xrpc(Box::new(
            bsky_sdk::error::GenericXrpcError::Other("connection reset".into()),
        )))
    }

    // --- classification -----------------------------------------------------

    #[test]
    fn server_errors_and_throttling_are_transient() {
        for code in [500u16, 502, 503, 504, 429, 408] {
            assert!(
                is_transient(&xrpc_status(code)),
                "HTTP {code} should be transient"
            );
        }
    }

    #[test]
    fn client_errors_are_permanent() {
        for code in [400u16, 401, 403, 404, 409] {
            assert!(
                !is_transient(&xrpc_status(code)),
                "HTTP {code} should be permanent"
            );
        }
    }

    #[test]
    fn transport_and_http_errors_are_transient_but_logic_errors_are_not() {
        assert!(
            is_transient(&xrpc_transport()),
            "bare transport → transient"
        );
        assert!(is_transient(&Error::http("dns failure")));
        assert!(!is_transient(&Error::invalid_input("bad did")));
        assert!(!is_transient(&Error::MissingCredentials));
        assert!(!is_transient(&Error::NotAuthenticated));
    }

    // --- retry combinator ---------------------------------------------------

    #[tokio::test]
    async fn returns_immediately_on_success_without_retrying() {
        let calls = Cell::new(0);
        let out: Result<u8, Error> = retry(&fast(3), || {
            calls.set(calls.get() + 1);
            async { Ok(7u8) }
        })
        .await;
        assert_eq!(out.unwrap(), 7);
        assert_eq!(calls.get(), 1, "a success must not be retried");
    }

    #[tokio::test]
    async fn retries_a_transient_failure_then_succeeds() {
        let calls = Cell::new(0);
        let out: Result<&str, Error> = retry(&fast(3), || {
            let n = calls.get() + 1;
            calls.set(n);
            async move {
                if n < 3 {
                    Err(xrpc_status(503))
                } else {
                    Ok("ok")
                }
            }
        })
        .await;
        assert_eq!(out.unwrap(), "ok");
        assert_eq!(calls.get(), 3, "two failures then a success = three calls");
    }

    #[tokio::test]
    async fn gives_up_after_max_retries_and_returns_the_last_error() {
        let calls = Cell::new(0);
        let out: Result<(), Error> = retry(&fast(2), || {
            calls.set(calls.get() + 1);
            async { Err(xrpc_status(500)) }
        })
        .await;
        assert!(out.is_err());
        assert_eq!(
            calls.get(),
            3,
            "max_retries=2 means 1 initial + 2 retries = 3 attempts"
        );
    }

    #[tokio::test]
    async fn does_not_retry_a_permanent_error() {
        let calls = Cell::new(0);
        let out: Result<(), Error> = retry(&fast(3), || {
            calls.set(calls.get() + 1);
            async { Err(xrpc_status(400)) }
        })
        .await;
        assert!(out.is_err());
        assert_eq!(calls.get(), 1, "a 400 must fail fast, no retries");
    }

    #[tokio::test]
    async fn max_retries_zero_disables_retrying() {
        let calls = Cell::new(0);
        let out: Result<(), Error> = retry(&RetryPolicy::none(), || {
            calls.set(calls.get() + 1);
            async { Err(xrpc_status(503)) }
        })
        .await;
        assert!(out.is_err());
        assert_eq!(calls.get(), 1, "max_retries=0 → exactly one attempt");
    }
}
