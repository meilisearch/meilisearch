use std::pin::Pin;
use std::task::{Context, Poll};

use actix_web::error::PayloadError;
use actix_web::{dev, web, FromRequest, HttpRequest};
use futures::future::{ready, Ready};
use futures::Stream;

pub struct Payload {
    payload: dev::Payload,
    limit: usize,
}

pub struct PayloadConfig {
    limit: usize,
}

impl PayloadConfig {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl Default for PayloadConfig {
    fn default() -> Self {
        Self { limit: 256 * 1024 }
    }
}

impl FromRequest for Payload {
    type Config = PayloadConfig;

    type Error = PayloadError;

    type Future = Ready<Result<Payload, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, payload: &mut dev::Payload) -> Self::Future {
        let limit = req
            .app_data::<PayloadConfig>()
            .map(|c| c.limit)
            .unwrap_or(Self::Config::default().limit);
        ready(Ok(Payload {
            payload: payload.take(),
            limit,
        }))
    }
}

impl Stream for Payload {
    type Item = Result<web::Bytes, PayloadError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.payload).poll_next(cx) {
            Poll::Ready(Some(result)) => match result {
                Ok(bytes) => match self.limit.checked_sub(bytes.len()) {
                    Some(new_limit) => {
                        self.limit = new_limit;
                        Poll::Ready(Some(Ok(bytes)))
                    }
                    None => Poll::Ready(Some(Err(PayloadError::Overflow))),
                },
                x => Poll::Ready(Some(x)),
            },
            otherwise => otherwise,
        }
    }
}
