use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use actix_web::dev::Payload;
use actix_web::web::Json;
use actix_web::{FromRequest, HttpRequest};
use deserr::{DeserializeError, DeserializeFromValue};
use futures::ready;
use meilisearch_types::error::{ErrorCode, ResponseError};

/// Extractor for typed data from Json request payloads
/// deserialised by deserr.
///
/// # Extractor
/// To extract typed data from a request body, the inner type `T` must implement the
/// [`deserr::DeserializeFromError<E>`] trait. The inner type `E` must implement the
/// [`ErrorCode`](meilisearch_error::ErrorCode) trait.
#[derive(Debug)]
pub struct ValidatedJson<T, E>(pub T, PhantomData<*const E>);

impl<T, E> ValidatedJson<T, E> {
    pub fn new(data: T) -> Self {
        ValidatedJson(data, PhantomData)
    }
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T, E> FromRequest for ValidatedJson<T, E>
where
    E: DeserializeError + ErrorCode + 'static,
    T: DeserializeFromValue<E>,
{
    type Error = actix_web::Error;
    type Future = ValidatedJsonExtractFut<T, E>;

    #[inline]
    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        ValidatedJsonExtractFut {
            fut: Json::<serde_json::Value>::from_request(req, payload),
            _phantom: PhantomData,
        }
    }
}

pub struct ValidatedJsonExtractFut<T, E> {
    fut: <Json<serde_json::Value> as FromRequest>::Future,
    _phantom: PhantomData<*const (T, E)>,
}

impl<T, E> Future for ValidatedJsonExtractFut<T, E>
where
    T: DeserializeFromValue<E>,
    E: DeserializeError + ErrorCode + 'static,
{
    type Output = Result<ValidatedJson<T, E>, actix_web::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ValidatedJsonExtractFut { fut, .. } = self.get_mut();
        let fut = Pin::new(fut);

        let res = ready!(fut.poll(cx));

        let res = match res {
            Err(err) => Err(err),
            Ok(data) => match deserr::deserialize::<_, _, E>(data.into_inner()) {
                Ok(data) => Ok(ValidatedJson::new(data)),
                Err(e) => Err(ResponseError::from(e).into()),
            },
        };

        Poll::Ready(res)
    }
}
