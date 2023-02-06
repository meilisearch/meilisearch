/*!
This module provides helper traits, types, and functions to deserialize query parameters.

The source of the problem is that query parameters only give us a string to work with.
This means `deserr` is never given a sequence or numbers, and thus the default deserialization
code for common types such as `usize` or `Vec<T>` does not work. To work around it, we create a
wrapper type called `Param<T>`, which is deserialised using the `from_query_param` method of the trait
`FromQueryParameter`.

We also use other helper types such as `CS` (i.e. comma-separated) from `serde_cs` as well as
`StarOr`, `OptionStarOr`, and `OptionStarOrList`.
*/

use std::convert::Infallible;
use std::ops::Deref;
use std::str::FromStr;

use deserr::{DeserializeError, DeserializeFromValue, MergeWithError, ValueKind};

use super::{DeserrParseBoolError, DeserrParseIntError};
use crate::error::unwrap_any;
use crate::index_uid::IndexUid;
use crate::tasks::{Kind, Status};

/// A wrapper type indicating that the inner value should be
/// deserialised from a query parameter string.
///
/// Note that if the field is optional, it is better to use
/// `Option<Param<T>>` instead of `Param<Option<T>>`.
#[derive(Default, Debug, Clone, Copy)]
pub struct Param<T>(pub T);

impl<T> Deref for Param<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, E> DeserializeFromValue<E> for Param<T>
where
    E: DeserializeError + MergeWithError<T::Err>,
    T: FromQueryParameter,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        match value {
            deserr::Value::String(s) => match T::from_query_param(&s) {
                Ok(x) => Ok(Param(x)),
                Err(e) => Err(unwrap_any(E::merge(None, e, location))),
            },
            _ => Err(unwrap_any(E::error(
                None,
                deserr::ErrorKind::IncorrectValueKind {
                    actual: value,
                    accepted: &[ValueKind::String],
                },
                location,
            ))),
        }
    }
}

/// Parse a value from a query parameter string.
///
/// This trait is functionally equivalent to `FromStr`.
/// Having a separate trait trait allows us to return better
/// deserializatio error messages.
pub trait FromQueryParameter: Sized {
    type Err;
    fn from_query_param(p: &str) -> Result<Self, Self::Err>;
}

/// Implement `FromQueryParameter` for the given type using its `FromStr`
/// trait implementation.
macro_rules! impl_from_query_param_from_str {
    ($type:ty) => {
        impl FromQueryParameter for $type {
            type Err = <$type as FromStr>::Err;
            fn from_query_param(p: &str) -> Result<Self, Self::Err> {
                p.parse()
            }
        }
    };
}
impl_from_query_param_from_str!(Kind);
impl_from_query_param_from_str!(Status);
impl_from_query_param_from_str!(IndexUid);

/// Implement `FromQueryParameter` for the given type using its `FromStr`
/// trait implementation, replacing the returned error with a struct
/// that wraps the original query parameter.
macro_rules! impl_from_query_param_wrap_original_value_in_error {
    ($type:ty, $err_type:path) => {
        impl FromQueryParameter for $type {
            type Err = $err_type;
            fn from_query_param(p: &str) -> Result<Self, Self::Err> {
                p.parse().map_err(|_| $err_type(p.to_owned()))
            }
        }
    };
}
impl_from_query_param_wrap_original_value_in_error!(usize, DeserrParseIntError);
impl_from_query_param_wrap_original_value_in_error!(u32, DeserrParseIntError);
impl_from_query_param_wrap_original_value_in_error!(bool, DeserrParseBoolError);

impl FromQueryParameter for String {
    type Err = Infallible;
    fn from_query_param(p: &str) -> Result<Self, Infallible> {
        Ok(p.to_owned())
    }
}
