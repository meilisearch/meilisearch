use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::ops::ControlFlow;

use deserr::errors::{JsonError, QueryParamError};
use deserr::{take_cf_content, DeserializeError, IntoValue, MergeWithError, ValuePointerRef};

use crate::error::deserr_codes::*;
use crate::error::{
    Code, DeserrParseBoolError, DeserrParseIntError, ErrorCode, InvalidTaskDateError,
    ParseOffsetDateTimeError,
};
use crate::index_uid::IndexUidFormatError;
use crate::tasks::{ParseTaskKindError, ParseTaskStatusError};

pub mod query_params;

/// Marker type for the Json format
pub struct DeserrJson;
/// Marker type for the Query Parameter format
pub struct DeserrQueryParam;

pub type DeserrJsonError<C = BadRequest> = DeserrError<DeserrJson, C>;
pub type DeserrQueryParamError<C = BadRequest> = DeserrError<DeserrQueryParam, C>;

/// A request deserialization error.
///
/// The first generic parameter is a marker type describing the format of the request: either json (e.g. [`DeserrJson`] or [`DeserrQueryParam`]).
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

impl<F, C: Default + ErrorCode> actix_web::ResponseError for DeserrError<F, C> {
    fn status_code(&self) -> actix_web::http::StatusCode {
        self.code.http()
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        crate::error::ResponseError::from_msg(self.msg.to_string(), self.code).error_response()
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
    ) -> ControlFlow<Self, Self> {
        ControlFlow::Break(DeserrError { msg: other.msg, code: other.code, _phantom: PhantomData })
    }
}

impl<Format, C: Default + ErrorCode> MergeWithError<Infallible> for DeserrError<Format, C> {
    fn merge(
        _self_: Option<Self>,
        _other: Infallible,
        _merge_location: ValuePointerRef,
    ) -> ControlFlow<Self, Self> {
        unreachable!()
    }
}

impl<C: Default + ErrorCode> DeserializeError for DeserrJsonError<C> {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> ControlFlow<Self, Self> {
        ControlFlow::Break(DeserrJsonError::new(
            take_cf_content(JsonError::error(None, error, location)).to_string(),
            C::default().error_code(),
        ))
    }
}

impl<C: Default + ErrorCode> DeserializeError for DeserrQueryParamError<C> {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> ControlFlow<Self, Self> {
        ControlFlow::Break(DeserrQueryParamError::new(
            take_cf_content(QueryParamError::error(None, error, location)).to_string(),
            C::default().error_code(),
        ))
    }
}

pub fn immutable_field_error(field: &str, accepted: &[&str], code: Code) -> DeserrJsonError {
    let msg = format!(
        "Immutable field `{field}`: expected one of {}",
        accepted
            .iter()
            .map(|accepted| format!("`{}`", accepted))
            .collect::<Vec<String>>()
            .join(", ")
    );

    DeserrJsonError::new(msg, code)
}

// Implement a convenience function to build a `missing_field` error
macro_rules! make_missing_field_convenience_builder {
    ($err_code:ident, $fn_name:ident) => {
        impl DeserrJsonError<$err_code> {
            pub fn $fn_name(field: &str, location: ValuePointerRef) -> Self {
                let x = deserr::take_cf_content(Self::error::<Infallible>(
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
make_missing_field_convenience_builder!(MissingDocumentFilter, missing_document_filter);
make_missing_field_convenience_builder!(
    MissingFacetSearchFacetName,
    missing_facet_search_facet_name
);
make_missing_field_convenience_builder!(
    MissingDocumentEditionFunction,
    missing_document_edition_function
);

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
            ) -> ControlFlow<Self, Self> {
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
merge_with_error_impl_take_error_message!(InvalidMultiSearchWeight);
merge_with_error_impl_take_error_message!(InvalidNetworkUrl);
merge_with_error_impl_take_error_message!(InvalidNetworkSearchApiKey);
merge_with_error_impl_take_error_message!(InvalidSearchSemanticRatio);
merge_with_error_impl_take_error_message!(InvalidSearchRankingScoreThreshold);
merge_with_error_impl_take_error_message!(InvalidSimilarRankingScoreThreshold);
merge_with_error_impl_take_error_message!(InvalidSimilarId);
