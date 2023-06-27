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
    remaining: usize,
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
            remaining: limit,
        }))
    }
}

impl Stream for Payload {
    type Item = Result<web::Bytes, MeilisearchHttpError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.payload).poll_next(cx) {
            Poll::Ready(Some(result)) => match result {
                Ok(bytes) => match self.remaining.checked_sub(bytes.len()) {
                    Some(new_limit) => {
                        self.remaining = new_limit;
                        Poll::Ready(Some(Ok(bytes)))
                    }
                    None => {
                        Poll::Ready(Some(Err(MeilisearchHttpError::PayloadTooLarge(self.limit))))
                    }
                },
                x => Poll::Ready(Some(x.map_err(MeilisearchHttpError::from))),
            },
            otherwise => otherwise.map(|o| o.map(|o| o.map_err(MeilisearchHttpError::from))),
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_http::encoding::Decoder as Decompress;
    use actix_http::BoxedPayloadStream;
    use bytes::Bytes;
    use futures_util::StreamExt;
    use meili_snap::snapshot;

    use super::*;

    #[actix_rt::test]
    async fn payload_to_large() {
        let stream = futures::stream::iter(vec![
            Ok(Bytes::from("1")),
            Ok(Bytes::from("2")),
            Ok(Bytes::from("3")),
            Ok(Bytes::from("4")),
        ]);
        let boxed_stream: BoxedPayloadStream = Box::pin(stream);
        let actix_payload = dev::Payload::from(boxed_stream);

        let payload = Payload {
            limit: 3,
            remaining: 3,
            payload: Decompress::new(actix_payload, actix_http::ContentEncoding::Identity),
        };

        let mut enumerated_payload_stream = payload.enumerate();

        while let Some((idx, chunk)) = enumerated_payload_stream.next().await {
            if idx == 3 {
                snapshot!(chunk.unwrap_err(), @"The provided payload reached the size limit. The maximum accepted payload size is 3 B.");
            }
        }
    }
}
