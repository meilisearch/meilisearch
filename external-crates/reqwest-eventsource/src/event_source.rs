use core::pin::Pin;
use std::time::Duration;

use eventsource_stream::Eventsource;
pub use eventsource_stream::{Event as MessageEvent, EventStreamError};
#[cfg(not(target_arch = "wasm32"))]
use futures_core::future::BoxFuture;
use futures_core::future::Future;
#[cfg(target_arch = "wasm32")]
use futures_core::future::LocalBoxFuture;
#[cfg(not(target_arch = "wasm32"))]
use futures_core::stream::BoxStream;
#[cfg(target_arch = "wasm32")]
use futures_core::stream::LocalBoxStream;
use futures_core::stream::Stream;
use futures_core::task::{Context, Poll};
use futures_timer::Delay;
use futures_util::TryStreamExt as _;
use http_client::policy::IpPolicy;
use http_client::reqwest::header::{HeaderName, HeaderValue};
use http_client::reqwest::{Error as ReqwestError, IntoUrl, RequestBuilder, Response, StatusCode};
use pin_project_lite::pin_project;

use crate::error::{CannotCloneRequestError, Error};
use crate::retry::{RetryPolicy, DEFAULT_RETRY};

#[cfg(not(target_arch = "wasm32"))]
type ResponseFuture = BoxFuture<'static, Result<Response, ReqwestError>>;
#[cfg(target_arch = "wasm32")]
type ResponseFuture = LocalBoxFuture<'static, Result<Response, ReqwestError>>;

#[cfg(not(target_arch = "wasm32"))]
type EventStream = BoxStream<'static, Result<MessageEvent, EventStreamError<ReqwestError>>>;
#[cfg(target_arch = "wasm32")]
type EventStream = LocalBoxStream<'static, Result<MessageEvent, EventStreamError<ReqwestError>>>;

type BoxedRetry = Box<dyn RetryPolicy + Send + Unpin + 'static>;

/// The ready state of an [`EventSource`]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum ReadyState {
    /// The EventSource is waiting on a response from the endpoint
    Connecting = 0,
    /// The EventSource is connected
    Open = 1,
    /// The EventSource is closed and no longer emitting Events
    Closed = 2,
}

pin_project! {
/// Provides the [`Stream`] implementation for the [`Event`] items. This wraps the
/// [`RequestBuilder`] and retries requests when they fail.
#[project = EventSourceProjection]
pub struct EventSource {
    builder: RequestBuilder,
    #[pin]
    next_response: Option<ResponseFuture>,
    #[pin]
    cur_stream: Option<EventStream>,
    #[pin]
    delay: Option<Delay>,
    is_closed: bool,
    retry_policy: BoxedRetry,
    last_event_id: String,
    last_retry: Option<(usize, Duration)>
}
}

impl EventSource {
    /// Wrap a [`RequestBuilder`]
    pub fn new(builder: RequestBuilder) -> Result<Self, CannotCloneRequestError> {
        let builder = builder.prepare(|builder| {
            builder.header(
                http_client::reqwest::header::ACCEPT,
                HeaderValue::from_static("text/event-stream"),
            )
        });
        let res_future = Box::pin(builder.try_clone().ok_or(CannotCloneRequestError)?.send());
        Ok(Self {
            builder,
            next_response: Some(res_future),
            cur_stream: None,
            delay: None,
            is_closed: false,
            retry_policy: Box::new(DEFAULT_RETRY),
            last_event_id: String::new(),
            last_retry: None,
        })
    }

    /// Create a simple EventSource based on a GET request
    pub fn get<T: IntoUrl>(url: T) -> Self {
        Self::new(
            http_client::reqwest::Client::builder()
                .build_with_policies(
                    IpPolicy::deny_all_local_ips(),
                    http_client::reqwest::redirect::Policy::default(),
                )
                .unwrap()
                .get(url),
        )
        .unwrap()
    }

    /// Close the EventSource stream and stop trying to reconnect
    pub fn close(&mut self) {
        self.is_closed = true;
    }

    /// Set the retry policy
    pub fn set_retry_policy(&mut self, policy: BoxedRetry) {
        self.retry_policy = policy
    }

    /// Get the last event id
    pub fn last_event_id(&self) -> &str {
        &self.last_event_id
    }

    /// Get the current ready state
    pub fn ready_state(&self) -> ReadyState {
        if self.is_closed {
            ReadyState::Closed
        } else if self.delay.is_some() || self.next_response.is_some() {
            ReadyState::Connecting
        } else {
            ReadyState::Open
        }
    }
}

fn check_response(response: Response) -> Result<Response, Error> {
    match response.status() {
        StatusCode::OK => {}
        status => {
            return Err(Error::InvalidStatusCode(status, response));
        }
    }
    let content_type = if let Some(content_type) =
        response.headers().get(&http_client::reqwest::header::CONTENT_TYPE)
    {
        content_type
    } else {
        return Err(Error::InvalidContentType(HeaderValue::from_static(""), response));
    };
    if content_type
        .to_str()
        .map_err(|_| ())
        .and_then(|s| s.parse::<mime::Mime>().map_err(|_| ()))
        .map(|mime_type| {
            matches!((mime_type.type_(), mime_type.subtype()), (mime::TEXT, mime::EVENT_STREAM))
        })
        .unwrap_or(false)
    {
        Ok(response)
    } else {
        Err(Error::InvalidContentType(content_type.clone(), response))
    }
}

impl<'a> EventSourceProjection<'a> {
    fn clear_fetch(&mut self) {
        self.next_response.take();
        self.cur_stream.take();
    }

    fn retry_fetch(&mut self) -> Result<(), Error> {
        self.cur_stream.take();
        let req = self.builder.try_clone().unwrap().try_prepare(|builder| -> Result<_, Error> {
            Ok(builder.header(
                HeaderName::from_static("last-event-id"),
                HeaderValue::from_str(self.last_event_id)
                    .map_err(|_| Error::InvalidLastEventId(self.last_event_id.clone()))?,
            ))
        })?;
        let res_future = Box::pin(req.send());
        self.next_response.replace(res_future);
        Ok(())
    }

    fn handle_response(&mut self, res: Response) {
        self.last_retry.take();
        let mut stream = res.bytes_stream().map_err(ReqwestError::from).eventsource();
        stream.set_last_event_id(self.last_event_id.clone());
        self.cur_stream.replace(Box::pin(stream));
    }

    fn handle_event(&mut self, event: &MessageEvent) {
        *self.last_event_id = event.id.clone();
        if let Some(duration) = event.retry {
            self.retry_policy.set_reconnection_time(duration)
        }
    }

    fn handle_error(&mut self, error: &Error) {
        self.clear_fetch();
        if let Some(retry_delay) = self.retry_policy.retry(error, *self.last_retry) {
            let retry_num = self.last_retry.map(|retry| retry.0).unwrap_or(1);
            *self.last_retry = Some((retry_num, retry_delay));
            self.delay.replace(Delay::new(retry_delay));
        } else {
            *self.is_closed = true;
        }
    }
}

/// Events created by the [`EventSource`]
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Event {
    /// The event fired when the connection is opened
    Open,
    /// The event fired when a [`MessageEvent`] is received
    Message(MessageEvent),
}

impl From<MessageEvent> for Event {
    fn from(event: MessageEvent) -> Self {
        Event::Message(event)
    }
}

impl Stream for EventSource {
    type Item = Result<Event, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.is_closed {
            return Poll::Ready(None);
        }

        if let Some(delay) = this.delay.as_mut().as_pin_mut() {
            match delay.poll(cx) {
                Poll::Ready(_) => {
                    this.delay.take();
                    if let Err(err) = this.retry_fetch() {
                        *this.is_closed = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        if let Some(response_future) = this.next_response.as_mut().as_pin_mut() {
            match response_future.poll(cx) {
                Poll::Ready(Ok(res)) => {
                    this.clear_fetch();
                    match check_response(res) {
                        Ok(res) => {
                            this.handle_response(res);
                            return Poll::Ready(Some(Ok(Event::Open)));
                        }
                        Err(err) => {
                            *this.is_closed = true;
                            return Poll::Ready(Some(Err(err)));
                        }
                    }
                }
                Poll::Ready(Err(err)) => {
                    let err = Error::Transport(err);
                    this.handle_error(&err);
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        match this.cur_stream.as_mut().as_pin_mut().unwrap().as_mut().poll_next(cx) {
            Poll::Ready(Some(Err(err))) => {
                let err = err.into();
                this.handle_error(&err);
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(Some(Ok(event))) => {
                this.handle_event(&event);
                Poll::Ready(Some(Ok(event.into())))
            }
            Poll::Ready(None) => {
                let err = Error::StreamEnded;
                this.handle_error(&err);
                Poll::Ready(Some(Err(err)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
