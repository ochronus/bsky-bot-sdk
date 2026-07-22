//! Error and result types for `bsky-bot-sdk`.

use thiserror::Error;

/// Convenience `Result` alias used throughout the crate.
pub type Result<T> = core::result::Result<T, Error>;

/// The error type returned by all fallible operations in this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// An error bubbled up from the underlying [`bsky_sdk`] / AT Protocol client.
    #[error("bluesky sdk error: {0}")]
    Sdk(#[from] bsky_sdk::Error),

    /// No credentials and no resumable session were available when one was required.
    #[error(
        "missing credentials: provide an identifier and app password (or a valid session file)"
    )]
    MissingCredentials,

    /// The agent has no active session (login/resume never succeeded).
    #[error("not authenticated: the agent has no active session")]
    NotAuthenticated,

    /// A bot was started with no handlers registered — it would do nothing.
    #[error("no handlers registered: register at least one handler before calling run()")]
    NoHandlers,

    /// A record could not be decoded into the requested type.
    #[error("could not decode record: {0}")]
    InvalidRecord(String),

    /// A supplied value (handle, DID, at-uri, …) was not valid.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// A filesystem error while reading/writing the session file.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// A (de)serialization error.
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

impl Error {
    /// Construct an [`Error::InvalidInput`] from any displayable value.
    pub fn invalid_input(msg: impl core::fmt::Display) -> Self {
        Error::InvalidInput(msg.to_string())
    }
}

/// Convert a raw XRPC transport/response error straight into our error type, so
/// `?` works directly on `atrium-api` client calls without an explicit `map_err`.
impl<E> From<atrium_api::xrpc::Error<E>> for Error
where
    E: core::fmt::Debug,
{
    fn from(err: atrium_api::xrpc::Error<E>) -> Self {
        Error::Sdk(bsky_sdk::Error::from(err))
    }
}
