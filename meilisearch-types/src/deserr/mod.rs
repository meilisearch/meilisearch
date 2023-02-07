use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;

use deserr::{DeserializeError, MergeWithError, ValuePointerRef};

use crate::error::deserr_codes::{self, *};
use crate::error::{
    unwrap_any, Code, DeserrParseBoolError, DeserrParseIntError, ErrorCode, InvalidTaskDateError,
    ParseOffsetDateTimeError,
};
use crate::index_uid::IndexUidFormatError;
use crate::tasks::{ParseTaskKindError, ParseTaskStatusError};

pub mod error_messages;
pub mod query_params;

/// Marker type for the Json format
pub struct DeserrJson;
/// Marker type for the Query Parameter format
pub struct DeserrQueryParam;

pub type DeserrJsonError<C = deserr_codes::BadRequest> = DeserrError<DeserrJson, C>;
pub type DeserrQueryParamError<C = deserr_codes::BadRequest> = DeserrError<DeserrQueryParam, C>;

/// A request deserialization error.
///
/// The first generic paramater is a marker type describing the format of the request: either json (e.g. [`DeserrJson`] or [`DeserrQueryParam`]).
/// The second generic parameter is the default error code for the deserialization error, in case it is not given.
pub struct DeserrError<Format, C: Default + ErrorCode> {
    pub msg: String,
    pub code: Code,
    _phantom: PhantomData<(Format, C)>,
}
impl<Format, C: Default + ErrorCode> DeserrError<Format, C> {
    pub fn new(msg: String, code: Code) -> Self {
        Self { msg, code, _phantom: PhantomData }
    }
}
impl<Format, C: Default + ErrorCode> std::fmt::Debug for DeserrError<Format, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeserrError").field("msg", &self.msg).field("code", &self.code).finish()
    }
}

impl<Format, C: Default + ErrorCode> std::fmt::Display for DeserrError<Format, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl<Format, C: Default + ErrorCode> std::error::Error for DeserrError<Format, C> {}
impl<Format, C: Default + ErrorCode> ErrorCode for DeserrError<Format, C> {
    fn error_code(&self) -> Code {
        self.code
    }
}

// For now, we don't accumulate errors. Only one deserialisation error is ever returned at a time.
impl<Format, C1: Default + ErrorCode, C2: Default + ErrorCode>
    MergeWithError<DeserrError<Format, C2>> for DeserrError<Format, C1>
{
    fn merge(
        _self_: Option<Self>,
        other: DeserrError<Format, C2>,
        _merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        Err(DeserrError { msg: other.msg, code: other.code, _phantom: PhantomData })
    }
}

impl<Format, C: Default + ErrorCode> MergeWithError<Infallible> for DeserrError<Format, C> {
    fn merge(
        _self_: Option<Self>,
        _other: Infallible,
        _merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        unreachable!()
    }
}

// Implement a convenience function to build a `missing_field` error
macro_rules! make_missing_field_convenience_builder {
    ($err_code:ident, $fn_name:ident) => {
        impl DeserrJsonError<$err_code> {
            pub fn $fn_name(field: &str, location: ValuePointerRef) -> Self {
                let x = unwrap_any(Self::error::<Infallible>(
                    None,
                    deserr::ErrorKind::MissingField { field },
                    location,
                ));
                Self { msg: x.msg, code: $err_code.error_code(), _phantom: PhantomData }
            }
        }
    };
}
make_missing_field_convenience_builder!(MissingIndexUid, missing_index_uid);
make_missing_field_convenience_builder!(MissingApiKeyActions, missing_api_key_actions);
make_missing_field_convenience_builder!(MissingApiKeyExpiresAt, missing_api_key_expires_at);
make_missing_field_convenience_builder!(MissingApiKeyIndexes, missing_api_key_indexes);
make_missing_field_convenience_builder!(MissingSwapIndexes, missing_swap_indexes);

// Integrate a sub-error into a [`DeserrError`] by taking its error message but using
// the default error code (C) from `Self`
macro_rules! merge_with_error_impl_take_error_message {
    ($err_type:ty) => {
        impl<Format, C: Default + ErrorCode> MergeWithError<$err_type> for DeserrError<Format, C>
        where
            DeserrError<Format, C>: deserr::DeserializeError,
        {
            fn merge(
                _self_: Option<Self>,
                other: $err_type,
                merge_location: ValuePointerRef,
            ) -> Result<Self, Self> {
                DeserrError::<Format, C>::error::<Infallible>(
                    None,
                    deserr::ErrorKind::Unexpected { msg: other.to_string() },
                    merge_location,
                )
            }
        }
    };
}

// All these errors can be merged into a `DeserrError`
merge_with_error_impl_take_error_message!(DeserrParseIntError);
merge_with_error_impl_take_error_message!(DeserrParseBoolError);
merge_with_error_impl_take_error_message!(uuid::Error);
merge_with_error_impl_take_error_message!(InvalidTaskDateError);
merge_with_error_impl_take_error_message!(ParseOffsetDateTimeError);
merge_with_error_impl_take_error_message!(ParseTaskKindError);
merge_with_error_impl_take_error_message!(ParseTaskStatusError);
merge_with_error_impl_take_error_message!(IndexUidFormatError);
