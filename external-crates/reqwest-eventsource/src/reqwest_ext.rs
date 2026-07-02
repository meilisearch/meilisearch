use http_client::reqwest::RequestBuilder;

use crate::error::CannotCloneRequestError;
use crate::event_source::EventSource;

/// Provides an easy interface to build an [`EventSource`] from a [`RequestBuilder`]
pub trait RequestBuilderExt {
    /// Create a new [`EventSource`] from a [`RequestBuilder`]
    fn eventsource(self) -> Result<EventSource, CannotCloneRequestError>;
}

impl RequestBuilderExt for RequestBuilder {
    fn eventsource(self) -> Result<EventSource, CannotCloneRequestError> {
        EventSource::new(self)
    }
}
