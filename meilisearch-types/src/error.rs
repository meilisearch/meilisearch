use std::convert::Infallible;
use std::marker::PhantomData;
use std::{fmt, io};

use actix_web::http::StatusCode;
use actix_web::{self as aweb, HttpResponseBuilder};
use aweb::rt::task::JoinError;
use convert_case::Casing;
use deserr::{DeserializeError, IntoValue, MergeWithError, ValuePointerRef};
use milli::heed::{Error as HeedError, MdbError};
use serde::{Deserialize, Serialize};

use self::deserr_codes::MissingIndexUid;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "test-traits", derive(proptest_derive::Arbitrary))]
pub struct ResponseError {
    #[serde(skip)]
    #[cfg_attr(feature = "test-traits", proptest(strategy = "strategy::status_code_strategy()"))]
    code: StatusCode,
    message: String,
    #[serde(rename = "code")]
    error_code: String,
    #[serde(rename = "type")]
    error_type: String,
    #[serde(rename = "link")]
    error_link: String,
}

impl ResponseError {
    pub fn from_msg(mut message: String, code: Code) -> Self {
        if code == Code::IoError {
            message.push_str(". This error generally happens when you have no space left on device or when your database doesn't have read or write right.");
        }
        Self {
            code: code.http(),
            message,
            error_code: code.err_code().error_name,
            error_type: code.type_(),
            error_link: code.url(),
        }
    }
}

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for ResponseError {}

impl<T> From<T> for ResponseError
where
    T: std::error::Error + ErrorCode,
{
    fn from(other: T) -> Self {
        Self::from_msg(other.to_string(), other.error_code())
    }
}

impl aweb::error::ResponseError for ResponseError {
    fn error_response(&self) -> aweb::HttpResponse {
        let json = serde_json::to_vec(self).unwrap();
        HttpResponseBuilder::new(self.status_code()).content_type("application/json").body(json)
    }

    fn status_code(&self) -> StatusCode {
        self.code
    }
}

pub trait ErrorCode {
    fn error_code(&self) -> Code;

    /// returns the HTTP status code associated with the error
    fn http_status(&self) -> StatusCode {
        self.error_code().http()
    }

    /// returns the doc url associated with the error
    fn error_url(&self) -> String {
        self.error_code().url()
    }

    /// returns error name, used as error code
    fn error_name(&self) -> String {
        self.error_code().name()
    }

    /// return the error type
    fn error_type(&self) -> String {
        self.error_code().type_()
    }
}

#[allow(clippy::enum_variant_names)]
enum ErrorType {
    InternalError,
    InvalidRequestError,
    AuthenticationError,
    System,
}

impl fmt::Display for ErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ErrorType::*;

        match self {
            InternalError => write!(f, "internal"),
            InvalidRequestError => write!(f, "invalid_request"),
            AuthenticationError => write!(f, "auth"),
            System => write!(f, "system"),
        }
    }
}

macro_rules! make_error_codes {
    ($($code_ident:ident, $err_type:ident, $status:ident);*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Code {
            $($code_ident),*
        }
        impl Code {
            /// associate a `Code` variant to the actual ErrCode
            fn err_code(&self) -> ErrCode {
                match self {
                    $(
                        Code::$code_ident => {
                            ErrCode::$err_type( stringify!($code_ident).to_case(convert_case::Case::Snake), StatusCode::$status)
                        }
                    )*
                }
            }
            /// return the HTTP status code associated with the `Code`
            fn http(&self) -> StatusCode {
                self.err_code().status_code
            }

            /// return error name, used as error code
            fn name(&self) -> String {
                self.err_code().error_name.to_string()
            }

            /// return the error type
            fn type_(&self) -> String {
                self.err_code().error_type.to_string()
            }

            /// return the doc url associated with the error
            fn url(&self) -> String {
                format!(
                    "https://docs.meilisearch.com/errors#{}",
                    self.name().to_case(convert_case::Case::Kebab)
                )
            }
        }
        pub mod deserr_codes {
            use super::{Code, ErrorCode};
            $(
                #[derive(Default)]
                pub struct $code_ident;
                impl ErrorCode for $code_ident {
                    fn error_code(&self) -> Code {
                        Code::$code_ident
                    }
                }
            )*
        }
    }
}
make_error_codes! {
ApiKeyAlreadyExists                   , invalid       , CONFLICT ;
ApiKeyNotFound                        , invalid       , NOT_FOUND ;
BadParameter                          , invalid       , BAD_REQUEST;
BadRequest                            , invalid       , BAD_REQUEST;
DatabaseSizeLimitReached              , internal      , INTERNAL_SERVER_ERROR;
DocumentNotFound                      , invalid       , NOT_FOUND;
DumpAlreadyProcessing                 , invalid       , CONFLICT;
DumpNotFound                          , invalid       , NOT_FOUND;
DumpProcessFailed                     , internal      , INTERNAL_SERVER_ERROR;
DuplicateIndexFound                   , invalid       , BAD_REQUEST;

ImmutableApiKeyUid                    , invalid       , BAD_REQUEST;
ImmutableApiKeyKey                    , invalid       , BAD_REQUEST;
ImmutableApiKeyActions                , invalid       , BAD_REQUEST;
ImmutableApiKeyIndexes                , invalid       , BAD_REQUEST;
ImmutableApiKeyExpiresAt              , invalid       , BAD_REQUEST;
ImmutableApiKeyCreatedAt              , invalid       , BAD_REQUEST;
ImmutableApiKeyUpdatedAt              , invalid       , BAD_REQUEST;

ImmutableIndexUid                     , invalid       , BAD_REQUEST;
ImmutableIndexCreatedAt               , invalid       , BAD_REQUEST;
ImmutableIndexUpdatedAt               , invalid       , BAD_REQUEST;

IndexAlreadyExists                    , invalid       , CONFLICT ;
IndexCreationFailed                   , internal      , INTERNAL_SERVER_ERROR;
IndexNotFound                         , invalid       , NOT_FOUND;
IndexPrimaryKeyAlreadyExists          , invalid       , BAD_REQUEST ;
IndexPrimaryKeyNoCandidateFound       , invalid       , BAD_REQUEST ;
IndexPrimaryKeyMultipleCandidatesFound, invalid       , BAD_REQUEST;
Internal                              , internal      , INTERNAL_SERVER_ERROR ;
InvalidApiKeyActions                  , invalid       , BAD_REQUEST ;
InvalidApiKeyDescription              , invalid       , BAD_REQUEST ;
InvalidApiKeyExpiresAt                , invalid       , BAD_REQUEST ;
InvalidApiKeyIndexes                  , invalid       , BAD_REQUEST ;
InvalidApiKeyLimit                    , invalid       , BAD_REQUEST ;
InvalidApiKeyName                     , invalid       , BAD_REQUEST ;
InvalidApiKeyOffset                   , invalid       , BAD_REQUEST ;
InvalidApiKeyUid                      , invalid       , BAD_REQUEST ;
InvalidApiKey                         , authentication, FORBIDDEN ;
InvalidContentType                    , invalid       , UNSUPPORTED_MEDIA_TYPE ;
InvalidDocumentFields                 , invalid       , BAD_REQUEST ;
InvalidDocumentGeoField               , invalid       , BAD_REQUEST ;
InvalidDocumentId                     , invalid       , BAD_REQUEST ;
InvalidDocumentLimit                  , invalid       , BAD_REQUEST ;
InvalidDocumentOffset                 , invalid       , BAD_REQUEST ;
InvalidIndexLimit                     , invalid       , BAD_REQUEST ;
InvalidIndexOffset                    , invalid       , BAD_REQUEST ;
InvalidIndexPrimaryKey                , invalid       , BAD_REQUEST ;
InvalidIndexUid                       , invalid       , BAD_REQUEST ;
InvalidMinWordLengthForTypo           , invalid       , BAD_REQUEST ;
InvalidSearchAttributesToCrop         , invalid       , BAD_REQUEST ;
InvalidSearchAttributesToHighlight    , invalid       , BAD_REQUEST ;
InvalidSearchAttributesToRetrieve     , invalid       , BAD_REQUEST ;
InvalidSearchCropLength               , invalid       , BAD_REQUEST ;
InvalidSearchCropMarker               , invalid       , BAD_REQUEST ;
InvalidSearchFacets                   , invalid       , BAD_REQUEST ;
InvalidSearchFilter                   , invalid       , BAD_REQUEST ;
InvalidSearchHighlightPostTag         , invalid       , BAD_REQUEST ;
InvalidSearchHighlightPreTag          , invalid       , BAD_REQUEST ;
InvalidSearchHitsPerPage              , invalid       , BAD_REQUEST ;
InvalidSearchLimit                    , invalid       , BAD_REQUEST ;
InvalidSearchMatchingStrategy         , invalid       , BAD_REQUEST ;
InvalidSearchOffset                   , invalid       , BAD_REQUEST ;
InvalidSearchPage                     , invalid       , BAD_REQUEST ;
InvalidSearchQ                        , invalid       , BAD_REQUEST ;
InvalidSearchShowMatchesPosition      , invalid       , BAD_REQUEST ;
InvalidSearchSort                     , invalid       , BAD_REQUEST ;
InvalidSettingsDisplayedAttributes    , invalid       , BAD_REQUEST ;
InvalidSettingsDistinctAttribute      , invalid       , BAD_REQUEST ;
InvalidSettingsFaceting               , invalid       , BAD_REQUEST ;
InvalidSettingsFilterableAttributes   , invalid       , BAD_REQUEST ;
InvalidSettingsPagination             , invalid       , BAD_REQUEST ;
InvalidSettingsRankingRules           , invalid       , BAD_REQUEST ;
InvalidSettingsSearchableAttributes   , invalid       , BAD_REQUEST ;
InvalidSettingsSortableAttributes     , invalid       , BAD_REQUEST ;
InvalidSettingsStopWords              , invalid       , BAD_REQUEST ;
InvalidSettingsSynonyms               , invalid       , BAD_REQUEST ;
InvalidSettingsTypoTolerance          , invalid       , BAD_REQUEST ;
InvalidState                          , internal      , INTERNAL_SERVER_ERROR ;
InvalidStoreFile                      , internal      , INTERNAL_SERVER_ERROR ;
InvalidSwapDuplicateIndexFound        , invalid       , BAD_REQUEST ;
InvalidSwapIndexes                    , invalid       , BAD_REQUEST ;
InvalidTaskAfterEnqueuedAt            , invalid       , BAD_REQUEST ;
InvalidTaskAfterFinishedAt            , invalid       , BAD_REQUEST ;
InvalidTaskAfterStartedAt             , invalid       , BAD_REQUEST ;
InvalidTaskBeforeEnqueuedAt           , invalid       , BAD_REQUEST ;
InvalidTaskBeforeFinishedAt           , invalid       , BAD_REQUEST ;
InvalidTaskBeforeStartedAt            , invalid       , BAD_REQUEST ;
InvalidTaskCanceledBy                 , invalid       , BAD_REQUEST ;
InvalidTaskFrom                       , invalid       , BAD_REQUEST ;
InvalidTaskLimit                      , invalid       , BAD_REQUEST ;
InvalidTaskStatuses                   , invalid       , BAD_REQUEST ;
InvalidTaskTypes                      , invalid       , BAD_REQUEST ;
InvalidTaskUids                       , invalid       , BAD_REQUEST  ;
IoError                               , system        , UNPROCESSABLE_ENTITY;
MalformedPayload                      , invalid       , BAD_REQUEST ;
MaxFieldsLimitExceeded                , invalid       , BAD_REQUEST ;
MissingApiKeyActions                  , invalid       , BAD_REQUEST ;
MissingApiKeyExpiresAt                , invalid       , BAD_REQUEST ;
MissingApiKeyIndexes                  , invalid       , BAD_REQUEST ;
MissingAuthorizationHeader            , authentication, UNAUTHORIZED ;
MissingContentType                    , invalid       , UNSUPPORTED_MEDIA_TYPE ;
MissingDocumentId                     , invalid       , BAD_REQUEST ;
MissingIndexUid                       , invalid       , BAD_REQUEST ;
MissingMasterKey                      , authentication, UNAUTHORIZED ;
MissingPayload                        , invalid       , BAD_REQUEST ;
MissingTaskFilters                    , invalid       , BAD_REQUEST ;
NoSpaceLeftOnDevice                   , system        , UNPROCESSABLE_ENTITY;
PayloadTooLarge                       , invalid       , PAYLOAD_TOO_LARGE ;
TaskNotFound                          , invalid       , NOT_FOUND ;
TooManyOpenFiles                      , system        , UNPROCESSABLE_ENTITY ;
UnretrievableDocument                 , internal      , BAD_REQUEST ;
UnretrievableErrorCode                , invalid       , BAD_REQUEST ;
UnsupportedMediaType                  , invalid       , UNSUPPORTED_MEDIA_TYPE
}

/// Internal structure providing a convenient way to create error codes
struct ErrCode {
    status_code: StatusCode,
    error_type: ErrorType,
    error_name: String,
}

impl ErrCode {
    fn authentication(error_name: String, status_code: StatusCode) -> ErrCode {
        ErrCode { status_code, error_name, error_type: ErrorType::AuthenticationError }
    }

    fn internal(error_name: String, status_code: StatusCode) -> ErrCode {
        ErrCode { status_code, error_name, error_type: ErrorType::InternalError }
    }

    fn invalid(error_name: String, status_code: StatusCode) -> ErrCode {
        ErrCode { status_code, error_name, error_type: ErrorType::InvalidRequestError }
    }

    fn system(error_name: String, status_code: StatusCode) -> ErrCode {
        ErrCode { status_code, error_name, error_type: ErrorType::System }
    }
}

impl ErrorCode for JoinError {
    fn error_code(&self) -> Code {
        Code::Internal
    }
}

impl ErrorCode for milli::Error {
    fn error_code(&self) -> Code {
        use milli::{Error, UserError};

        match self {
            Error::InternalError(_) => Code::Internal,
            Error::IoError(e) => e.error_code(),
            Error::UserError(ref error) => {
                match error {
                    // TODO: wait for spec for new error codes.
                    UserError::SerdeJson(_)
                    | UserError::InvalidLmdbOpenOptions
                    | UserError::DocumentLimitReached
                    | UserError::AccessingSoftDeletedDocument { .. }
                    | UserError::UnknownInternalDocumentId { .. } => Code::Internal,
                    UserError::InvalidStoreFile => Code::InvalidStoreFile,
                    UserError::NoSpaceLeftOnDevice => Code::NoSpaceLeftOnDevice,
                    UserError::MaxDatabaseSizeReached => Code::DatabaseSizeLimitReached,
                    UserError::AttributeLimitReached => Code::MaxFieldsLimitExceeded,
                    UserError::InvalidFilter(_) => Code::InvalidSearchFilter,
                    UserError::MissingDocumentId { .. } => Code::MissingDocumentId,
                    UserError::InvalidDocumentId { .. } | UserError::TooManyDocumentIds { .. } => {
                        Code::InvalidDocumentId
                    }
                    UserError::NoPrimaryKeyCandidateFound => Code::IndexPrimaryKeyNoCandidateFound,
                    UserError::MultiplePrimaryKeyCandidatesFound { .. } => {
                        Code::IndexPrimaryKeyMultipleCandidatesFound
                    }
                    UserError::PrimaryKeyCannotBeChanged(_) => Code::IndexPrimaryKeyAlreadyExists,
                    UserError::SortRankingRuleMissing => Code::InvalidSearchSort,
                    UserError::InvalidFacetsDistribution { .. } => Code::BadRequest,
                    UserError::InvalidSortableAttribute { .. } => Code::InvalidSearchSort,
                    UserError::CriterionError(_) => Code::InvalidSettingsRankingRules,
                    UserError::InvalidGeoField { .. } => Code::InvalidDocumentGeoField,
                    UserError::SortError(_) => Code::InvalidSearchSort,
                    UserError::InvalidMinTypoWordLenSetting(_, _) => {
                        Code::InvalidMinWordLengthForTypo
                    }
                }
            }
        }
    }
}

impl ErrorCode for file_store::Error {
    fn error_code(&self) -> Code {
        match self {
            Self::IoError(e) => e.error_code(),
            Self::PersistError(e) => e.error_code(),
        }
    }
}

impl ErrorCode for tempfile::PersistError {
    fn error_code(&self) -> Code {
        self.error.error_code()
    }
}

impl ErrorCode for HeedError {
    fn error_code(&self) -> Code {
        match self {
            HeedError::Mdb(MdbError::MapFull) => Code::DatabaseSizeLimitReached,
            HeedError::Mdb(MdbError::Invalid) => Code::InvalidStoreFile,
            HeedError::Io(e) => e.error_code(),
            HeedError::Mdb(_)
            | HeedError::Encoding
            | HeedError::Decoding
            | HeedError::InvalidDatabaseTyping
            | HeedError::DatabaseClosing
            | HeedError::BadOpenOptions => Code::Internal,
        }
    }
}

impl ErrorCode for io::Error {
    fn error_code(&self) -> Code {
        match self.raw_os_error() {
            Some(5) => Code::IoError,
            Some(24) => Code::TooManyOpenFiles,
            Some(28) => Code::NoSpaceLeftOnDevice,
            _ => Code::Internal,
        }
    }
}

pub fn unwrap_any<T>(any: Result<T, T>) -> T {
    match any {
        Ok(any) => any,
        Err(any) => any,
    }
}

#[cfg(feature = "test-traits")]
mod strategy {
    use proptest::strategy::Strategy;

    use super::*;

    pub(super) fn status_code_strategy() -> impl Strategy<Value = StatusCode> {
        (100..999u16).prop_map(|i| StatusCode::from_u16(i).unwrap())
    }
}

pub struct DeserrError<C: ErrorCode = deserr_codes::BadRequest> {
    pub msg: String,
    pub code: Code,
    _phantom: PhantomData<C>,
}
impl<C: ErrorCode> std::fmt::Debug for DeserrError<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeserrError").field("msg", &self.msg).field("code", &self.code).finish()
    }
}

impl<C: ErrorCode> std::fmt::Display for DeserrError<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl<C: ErrorCode> std::error::Error for DeserrError<C> {}
impl<C: ErrorCode> ErrorCode for DeserrError<C> {
    fn error_code(&self) -> Code {
        self.code
    }
}

impl<C1: ErrorCode, C2: ErrorCode> MergeWithError<DeserrError<C2>> for DeserrError<C1> {
    fn merge(
        _self_: Option<Self>,
        other: DeserrError<C2>,
        _merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        Err(DeserrError { msg: other.msg, code: other.code, _phantom: PhantomData })
    }
}

impl DeserrError<MissingIndexUid> {
    pub fn missing_index_uid(field: &str, location: ValuePointerRef) -> Self {
        let x = unwrap_any(Self::error::<Infallible>(
            None,
            deserr::ErrorKind::MissingField { field },
            location,
        ));
        Self { msg: x.msg, code: MissingIndexUid.error_code(), _phantom: PhantomData }
    }
}

impl<C: Default + ErrorCode> deserr::DeserializeError for DeserrError<C> {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let msg = unwrap_any(deserr::serde_json::JsonError::error(None, error, location)).0;

        Err(DeserrError { msg, code: C::default().error_code(), _phantom: PhantomData })
    }
}

pub struct TakeErrorMessage<T>(pub T);

impl<C: Default + ErrorCode, T> MergeWithError<TakeErrorMessage<T>> for DeserrError<C>
where
    T: std::error::Error,
{
    fn merge(
        _self_: Option<Self>,
        other: TakeErrorMessage<T>,
        merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        DeserrError::error::<Infallible>(
            None,
            deserr::ErrorKind::Unexpected { msg: other.0.to_string() },
            merge_location,
        )
    }
}

#[macro_export]
macro_rules! internal_error {
    ($target:ty : $($other:path), *) => {
        $(
            impl From<$other> for $target {
                fn from(other: $other) -> Self {
                    Self::Internal(Box::new(other))
                }
            }
        )*
    }
}
