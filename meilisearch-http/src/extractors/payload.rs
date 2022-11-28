use std::pin::Pin;
use std::task::{Context, Poll};

use actix_http::encoding::Decoder as Decompress;
use actix_web::{dev, web, FromRequest, HttpRequest};
use futures::future::{ready, Ready};
use futures::Stream;

use crate::error::MeilisearchHttpError;

pub struct Payload {
    payload: Decompress<dev::Payload>,
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
    type Error = MeilisearchHttpError;

    type Future = Ready<Result<Payload, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, payload: &mut dev::Payload) -> Self::Future {
        let limit = req
            .app_data::<PayloadConfig>()
            .map(|c| c.limit)
            .unwrap_or(PayloadConfig::default().limit);
        ready(Ok(Payload {
            payload: Decompress::from_headers(payload.take(), req.headers()),
            limit,
        }))
    }
}

impl Stream for Payload {
    type Item = Result<web::Bytes, MeilisearchHttpError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.payload).poll_next(cx) {
            Poll::Ready(Some(result)) => match result {
                Ok(bytes) => match self.limit.checked_sub(bytes.len()) {
                    Some(new_limit) => {
                        self.limit = new_limit;
                        Poll::Ready(Some(Ok(bytes)))
                    }
                    None => Poll::Ready(Some(Err(MeilisearchHttpError::PayloadTooLarge))),
                },
                x => Poll::Ready(Some(x.map_err(MeilisearchHttpError::from))),
            },
            otherwise => otherwise.map(|o| o.map(|o| o.map_err(MeilisearchHttpError::from))),
        }
    }
}
