//! A module to parse query parameter with deserr

use std::marker::PhantomData;
use std::{fmt, ops};

use actix_http::Payload;
use actix_utils::future::{err, ok, Ready};
use actix_web::{FromRequest, HttpRequest};
use deserr::{DeserializeError, DeserializeFromValue};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryParameter<T, E>(pub T, PhantomData<*const E>);

impl<T, E> QueryParameter<T, E> {
    /// Unwrap into inner `T` value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T, E> QueryParameter<T, E>
where
    T: DeserializeFromValue<E>,
    E: DeserializeError + ErrorCode + std::error::Error + 'static,
{
    pub fn from_query(query_str: &str) -> Result<Self, actix_web::Error> {
        let value = serde_urlencoded::from_str::<serde_json::Value>(query_str)
            .map_err(|e| ResponseError::from_msg(e.to_string(), Code::BadRequest))?;

        match deserr::deserialize::<_, _, E>(value) {
            Ok(data) => Ok(QueryParameter(data, PhantomData)),
            Err(e) => Err(ResponseError::from(e).into()),
        }
    }
}

impl<T, E> ops::Deref for QueryParameter<T, E> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T, E> ops::DerefMut for QueryParameter<T, E> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: fmt::Display, E> fmt::Display for QueryParameter<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T, E> FromRequest for QueryParameter<T, E>
where
    T: DeserializeFromValue<E>,
    E: DeserializeError + ErrorCode + std::error::Error + 'static,
{
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, actix_web::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        QueryParameter::from_query(req.query_string()).map(ok).unwrap_or_else(err)
    }
}
