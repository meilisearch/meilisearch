use core::fmt;
use std::string::FromUtf8Error;

use eventsource_stream::EventStreamError;
use http_client::reqwest::header::HeaderValue;
#[cfg(doc)]
use http_client::reqwest::RequestBuilder;
use http_client::reqwest::{Error as ReqwestError, Response, StatusCode};
use nom::error::Error as NomError;

/// Error raised when a [`RequestBuilder`] cannot be cloned. See [`RequestBuilder::try_clone`] for
/// more information
#[derive(Debug, Clone, Copy)]
pub struct CannotCloneRequestError;

impl fmt::Display for CannotCloneRequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("expected a cloneable request")
    }
}

impl std::error::Error for CannotCloneRequestError {}

/// Error raised by the EventSource stream fetching and parsing
#[derive(Debug, Error)]
pub enum Error {
    /// Source stream is not valid UTF8
    #[error(transparent)]
    Utf8(FromUtf8Error),
    /// Source stream is not a valid EventStream
    #[error(transparent)]
    Parser(NomError<String>),
    /// The HTTP Request could not be completed
    #[error(transparent)]
    Transport(ReqwestError),
    /// The `Content-Type` returned by the server is invalid
    #[error("Invalid header value: {0:?}")]
    InvalidContentType(HeaderValue, Response),
    /// The status code returned by the server is invalid
    #[error("Invalid status code: {0}")]
    InvalidStatusCode(StatusCode, Response),
    /// The `Last-Event-ID` cannot be formed into a Header to be submitted to the server
    #[error("Invalid `Last-Event-ID`: {0}")]
    InvalidLastEventId(String),
    /// The stream ended
    #[error("Stream ended")]
    StreamEnded,
}

impl From<EventStreamError<ReqwestError>> for Error {
    fn from(err: EventStreamError<ReqwestError>) -> Self {
        match err {
            EventStreamError::Utf8(err) => Self::Utf8(err),
            EventStreamError::Parser(err) => Self::Parser(err),
            EventStreamError::Transport(err) => Self::Transport(err),
        }
    }
}
