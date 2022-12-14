//! For query parameter extractor documentation, see [`Query`].

use std::marker::PhantomData;
use std::{fmt, ops};

use actix_http::Payload;
use actix_utils::future::{err, ok, Ready};
use actix_web::{FromRequest, HttpRequest};
use deserr::{DeserializeError, DeserializeFromValue};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};

/// Extract typed information from the request's query.
///
/// To extract typed data from the URL query string, the inner type `T` must implement the
/// [`DeserializeOwned`] trait.
///
/// Use [`QueryConfig`] to configure extraction process.
///
/// # Panics
/// A query string consists of unordered `key=value` pairs, therefore it cannot be decoded into any
/// type which depends upon data ordering (eg. tuples). Trying to do so will result in a panic.
///
/// # Examples
/// ```
/// use actix_web::{get, web};
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// pub enum ResponseType {
///    Token,
///    Code
/// }
///
/// #[derive(Debug, Deserialize)]
/// pub struct AuthRequest {
///    id: u64,
///    response_type: ResponseType,
/// }
///
/// // Deserialize `AuthRequest` struct from query string.
/// // This handler gets called only if the request's query parameters contain both fields.
/// // A valid request path for this handler would be `/?id=64&response_type=Code"`.
/// #[get("/")]
/// async fn index(info: web::Query<AuthRequest>) -> String {
///     format!("Authorization request for id={} and type={:?}!", info.id, info.response_type)
/// }
///
/// // To access the entire underlying query struct, use `.into_inner()`.
/// #[get("/debug1")]
/// async fn debug1(info: web::Query<AuthRequest>) -> String {
///     dbg!("Authorization object = {:?}", info.into_inner());
///     "OK".to_string()
/// }
///
/// // Or use destructuring, which is equivalent to `.into_inner()`.
/// #[get("/debug2")]
/// async fn debug2(web::Query(info): web::Query<AuthRequest>) -> String {
///     dbg!("Authorization object = {:?}", info);
///     "OK".to_string()
/// }
/// ```
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
    E: DeserializeError + ErrorCode + 'static,
{
    /// Deserialize a `T` from the URL encoded query parameter string.
    ///
    /// ```
    /// # use std::collections::HashMap;
    /// # use actix_web::web::Query;
    /// let numbers = Query::<HashMap<String, u32>>::from_query("one=1&two=2").unwrap();
    /// assert_eq!(numbers.get("one"), Some(&1));
    /// assert_eq!(numbers.get("two"), Some(&2));
    /// assert!(numbers.get("three").is_none());
    /// ```
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

/// See [here](#Examples) for example of usage as an extractor.
impl<T, E> FromRequest for QueryParameter<T, E>
where
    T: DeserializeFromValue<E>,
    E: DeserializeError + ErrorCode + 'static,
{
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, actix_web::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        QueryParameter::from_query(&req.query_string()).map(ok).unwrap_or_else(err)
        // serde_urlencoded::from_str::<serde_json::Value>(req.query_string())
        //     .map(|val| Ok(QueryParameter(val, PhantomData)))
        //     .unwrap_or_else(|e| err(ResponseError::from_msg(e.to_string(), Code::BadRequest)))
    }
}
