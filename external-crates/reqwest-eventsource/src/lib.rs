//! Provides a simple wrapper for [`reqwest`] to provide an Event Source implementation.
//! You can learn more about Server Sent Events (SSE) take a look at [the MDN
//! docs](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
//! This crate uses [`eventsource_stream`] to wrap the underlying Bytes stream, and retries failed
//! requests.
//!
//! # Example
//!
//! ```ignore
//! let mut es = EventSource::get("http://localhost:8000/events");
//! while let Some(event) = es.next().await {
//!     match event {
//!         Ok(Event::Open) => println!("Connection Open!"),
//!         Ok(Event::Message(message)) => println!("Message: {:#?}", message),
//!         Err(err) => {
//!             println!("Error: {}", err);
//!             es.close();
//!         }
//!     }
//! }
//! ```

#![allow(warnings)]

#[macro_use]
extern crate thiserror;

mod error;
mod event_source;
mod reqwest_ext;
pub mod retry;

pub use error::{CannotCloneRequestError, Error};
pub use event_source::{Event, EventSource, ReadyState};
pub use reqwest_ext::RequestBuilderExt;
