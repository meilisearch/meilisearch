use actix_web::{dev::Payload, web::Json, FromRequest, HttpRequest};
use futures::ready;
use jayson::{DeserializeError, DeserializeFromValue, MergeWithError, ValuePointer};
use meilisearch_lib::milli::AscDescError;
use meilisearch_types::error::{Code, ErrorCode, ResponseError};
use std::{
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

// pub struct MeilisearchDeserializeError {
//     pub Vec<(ValuePointer, Box<dyn Error>)>,
// }
// impl MergeWithError<AscDescError> for MeilisearchDeserializeError {
//     fn merge(self_: Option<Self>, other: AscDescError, merge_location: jayson::ValuePointerRef) -> Result<Self, Self> {
//         todo!()
//     }
// }
// /*
// {
//     !
//     x: {
//         y: {
//             z: {
//                 a: 2
//             }
//         }
//     }
// }

//  */
//  impl MergeWithError<MeilisearchDeserializeError> for MeilisearchDeserializeError {

//  }
// impl DeserializeError for MeilisearchDeserializeError{
//     fn location(&self) -> Option<jayson::ValuePointer> {
//         todo!()
//     }

//     fn incorrect_value_kind(
//         self_: Option<Self>,
//         actual: jayson::ValueKind,
//         accepted: &[jayson::ValueKind],
//         location: jayson::ValuePointerRef,
//     ) -> Result<Self, Self> {
//         todo!()
//     }

//     fn missing_field(
//         self_: Option<Self>,
//         field: &str,
//         location: jayson::ValuePointerRef,
//     ) -> Result<Self, Self> {
//         todo!()
//     }

//     fn unknown_key(
//         self_: Option<Self>,
//         key: &str,
//         accepted: &[&str],
//         location: jayson::ValuePointerRef,
//     ) -> Result<Self, Self> {
//         todo!()
//     }

//     fn unexpected(self_: Option<Self>, msg: &str, location: jayson::ValuePointerRef) -> Result<Self, Self> {
//         todo!()
//     }
// }

/// Extractor for typed data from Json request payloads
/// deserialised by Jayson.
///
/// # Extractor
/// To extract typed data from a request body, the inner type `T` must implement the
/// [`jayson::DeserializeFromError<E>`] trait. The inner type `E` must implement the
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
    type Error = ResponseError;
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
    type Output = Result<ValidatedJson<T, E>, ResponseError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let res = ready!(Pin::new(&mut this.fut).poll(cx));

        let res = match res {
            Err(err) => Err(ResponseError::from_msg(
                format!("{err}"),
                Code::MalformedPayload,
            )),
            Ok(data) => match jayson::deserialize::<_, _, E>(data.into_inner()) {
                Ok(data) => Ok(ValidatedJson::new(data)),
                Err(e) => Err(e.into()),
            },
        };

        Poll::Ready(res)
    }
}
