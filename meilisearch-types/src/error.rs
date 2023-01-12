use std::convert::Infallible;
use std::marker::PhantomData;
use std::str::FromStr;
use std::{fmt, io};

use actix_web::http::StatusCode;
use actix_web::{self as aweb, HttpResponseBuilder};
use aweb::rt::task::JoinError;
use convert_case::Casing;
use deserr::{DeserializeError, ErrorKind, IntoValue, MergeWithError, ValueKind, ValuePointerRef};
use milli::heed::{Error as HeedError, MdbError};
use serde::{Deserialize, Serialize};
use serde_cs::vec::CS;

use crate::star_or::StarOr;

use self::deserr_codes::{
    InvalidSwapIndexes, MissingApiKeyActions, MissingApiKeyExpiresAt, MissingApiKeyIndexes,
    MissingIndexUid, MissingSwapIndexes,
};

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
MissingSwapIndexes             , invalid       , BAD_REQUEST ;
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

pub struct DeserrJson;
pub struct DeserrQueryParam;

pub type DeserrJsonError<C = deserr_codes::BadRequest> = DeserrError<DeserrJson, C>;
pub type DeserrQueryParamError<C = deserr_codes::BadRequest> = DeserrError<DeserrQueryParam, C>;

pub struct DeserrError<Format, C: Default + ErrorCode> {
    pub msg: String,
    pub code: Code,
    _phantom: PhantomData<(Format, C)>,
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

impl DeserrJsonError<MissingIndexUid> {
    pub fn missing_index_uid(field: &str, location: ValuePointerRef) -> Self {
        let x = unwrap_any(Self::error::<Infallible>(
            None,
            deserr::ErrorKind::MissingField { field },
            location,
        ));
        Self { msg: x.msg, code: MissingIndexUid.error_code(), _phantom: PhantomData }
    }
}
impl DeserrJsonError<MissingApiKeyActions> {
    pub fn missing_api_key_actions(field: &str, location: ValuePointerRef) -> Self {
        let x = unwrap_any(Self::error::<Infallible>(
            None,
            deserr::ErrorKind::MissingField { field },
            location,
        ));
        Self { msg: x.msg, code: MissingApiKeyActions.error_code(), _phantom: PhantomData }
    }
}
impl DeserrJsonError<MissingApiKeyExpiresAt> {
    pub fn missing_api_key_expires_at(field: &str, location: ValuePointerRef) -> Self {
        let x = unwrap_any(Self::error::<Infallible>(
            None,
            deserr::ErrorKind::MissingField { field },
            location,
        ));
        Self { msg: x.msg, code: MissingApiKeyExpiresAt.error_code(), _phantom: PhantomData }
    }
}
impl DeserrJsonError<MissingApiKeyIndexes> {
    pub fn missing_api_key_indexes(field: &str, location: ValuePointerRef) -> Self {
        let x = unwrap_any(Self::error::<Infallible>(
            None,
            deserr::ErrorKind::MissingField { field },
            location,
        ));
        Self { msg: x.msg, code: MissingApiKeyIndexes.error_code(), _phantom: PhantomData }
    }
}

impl DeserrJsonError<InvalidSwapIndexes> {
    pub fn missing_swap_indexes_indexes(field: &str, location: ValuePointerRef) -> Self {
        let x = unwrap_any(Self::error::<Infallible>(
            None,
            deserr::ErrorKind::MissingField { field },
            location,
        ));
        Self { msg: x.msg, code: MissingSwapIndexes.error_code(), _phantom: PhantomData }
    }
}

// if the error happened in the root, then an empty string is returned.
pub fn location_json_description(location: ValuePointerRef, article: &str) -> String {
    fn rec(location: ValuePointerRef) -> String {
        match location {
            ValuePointerRef::Origin => String::new(),
            ValuePointerRef::Key { key, prev } => rec(*prev) + "." + key,
            ValuePointerRef::Index { index, prev } => format!("{}[{index}]", rec(*prev)),
        }
    }
    match location {
        ValuePointerRef::Origin => String::new(),
        _ => {
            format!("{article} `{}`", rec(location))
        }
    }
}

fn value_kinds_description_json(kinds: &[ValueKind]) -> String {
    fn order(kind: &ValueKind) -> u8 {
        match kind {
            ValueKind::Null => 0,
            ValueKind::Boolean => 1,
            ValueKind::Integer => 2,
            ValueKind::NegativeInteger => 3,
            ValueKind::Float => 4,
            ValueKind::String => 5,
            ValueKind::Sequence => 6,
            ValueKind::Map => 7,
        }
    }

    fn single_description(kind: &ValueKind) -> &'static str {
        match kind {
            ValueKind::Null => "null",
            ValueKind::Boolean => "a boolean",
            ValueKind::Integer => "a positive integer",
            ValueKind::NegativeInteger => "an integer",
            ValueKind::Float => "a number",
            ValueKind::String => "a string",
            ValueKind::Sequence => "an array",
            ValueKind::Map => "an object",
        }
    }

    fn description_rec(kinds: &[ValueKind], count_items: &mut usize, message: &mut String) {
        let (msg_part, rest): (_, &[ValueKind]) = match kinds {
            [] => (String::new(), &[]),
            [ValueKind::Integer | ValueKind::NegativeInteger, ValueKind::Float, rest @ ..] => {
                ("a number".to_owned(), rest)
            }
            [ValueKind::Integer, ValueKind::NegativeInteger, ValueKind::Float, rest @ ..] => {
                ("a number".to_owned(), rest)
            }
            [ValueKind::Integer, ValueKind::NegativeInteger, rest @ ..] => {
                ("an integer".to_owned(), rest)
            }
            [a] => (single_description(a).to_owned(), &[]),
            [a, rest @ ..] => (single_description(a).to_owned(), rest),
        };

        if rest.is_empty() {
            if *count_items == 0 {
                message.push_str(&msg_part);
            } else if *count_items == 1 {
                message.push_str(&format!(" or {msg_part}"));
            } else {
                message.push_str(&format!(", or {msg_part}"));
            }
        } else {
            if *count_items == 0 {
                message.push_str(&msg_part);
            } else {
                message.push_str(&format!(", {msg_part}"));
            }

            *count_items += 1;
            description_rec(rest, count_items, message);
        }
    }

    let mut kinds = kinds.to_owned();
    kinds.sort_by_key(order);
    kinds.dedup();

    if kinds.is_empty() {
        "a different value".to_owned()
    } else {
        let mut message = String::new();
        description_rec(kinds.as_slice(), &mut 0, &mut message);
        message
    }
}

fn value_description_with_kind_json(v: &serde_json::Value) -> String {
    match v.kind() {
        ValueKind::Null => "null".to_owned(),
        kind => {
            format!(
                "{}: `{}`",
                value_kinds_description_json(&[kind]),
                serde_json::to_string(v).unwrap()
            )
        }
    }
}

impl<C: Default + ErrorCode> deserr::DeserializeError for DeserrJsonError<C> {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let mut message = String::new();

        message.push_str(&match error {
            ErrorKind::IncorrectValueKind { actual, accepted } => {
                let expected = value_kinds_description_json(accepted);
                // if we're not able to get the value as a string then we print nothing.
                let received = value_description_with_kind_json(&serde_json::Value::from(actual));

                let location = location_json_description(location, " at");

                format!("Invalid value type{location}: expected {expected}, but found {received}")
            }
            ErrorKind::MissingField { field } => {
                // serde_json original message:
                // Json deserialize error: missing field `lol` at line 1 column 2
                let location = location_json_description(location, " inside");
                format!("Missing field `{field}`{location}")
            }
            ErrorKind::UnknownKey { key, accepted } => {
                let location = location_json_description(location, " inside");
                format!(
                    "Unknown field `{}`{location}: expected one of {}",
                    key,
                    accepted
                        .iter()
                        .map(|accepted| format!("`{}`", accepted))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            ErrorKind::UnknownValue { value, accepted } => {
                let location = location_json_description(location, " at");
                format!(
                    "Unknown value `{}`{location}: expected one of {}",
                    value,
                    accepted
                        .iter()
                        .map(|accepted| format!("`{}`", accepted))
                        .collect::<Vec<String>>()
                        .join(", "),
                )
            }
            ErrorKind::Unexpected { msg } => {
                let location = location_json_description(location, " at");
                // serde_json original message:
                // The json payload provided is malformed. `trailing characters at line 1 column 19`.
                format!("Invalid value{location}: {msg}")
            }
        });

        Err(DeserrJsonError {
            msg: message,
            code: C::default().error_code(),
            _phantom: PhantomData,
        })
    }
}

// if the error happened in the root, then an empty string is returned.
pub fn location_query_param_description(location: ValuePointerRef, article: &str) -> String {
    fn rec(location: ValuePointerRef) -> String {
        match location {
            ValuePointerRef::Origin => String::new(),
            ValuePointerRef::Key { key, prev } => {
                if matches!(prev, ValuePointerRef::Origin) {
                    key.to_owned()
                } else {
                    rec(*prev) + "." + key
                }
            }
            ValuePointerRef::Index { index, prev } => format!("{}[{index}]", rec(*prev)),
        }
    }
    match location {
        ValuePointerRef::Origin => String::new(),
        _ => {
            format!("{article} `{}`", rec(location))
        }
    }
}

impl<C: Default + ErrorCode> deserr::DeserializeError for DeserrQueryParamError<C> {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let mut message = String::new();

        message.push_str(&match error {
            ErrorKind::IncorrectValueKind { actual, accepted } => {
                let expected = value_kinds_description_query_param(accepted);
                // if we're not able to get the value as a string then we print nothing.
                let received = value_description_with_kind_query_param(actual);

                let location = location_query_param_description(location, " for parameter");

                format!("Invalid value type{location}: expected {expected}, but found {received}")
            }
            ErrorKind::MissingField { field } => {
                // serde_json original message:
                // Json deserialize error: missing field `lol` at line 1 column 2
                let location = location_query_param_description(location, " inside");
                format!("Missing parameter `{field}`{location}")
            }
            ErrorKind::UnknownKey { key, accepted } => {
                let location = location_query_param_description(location, " inside");
                format!(
                    "Unknown parameter `{}`{location}: expected one of {}",
                    key,
                    accepted
                        .iter()
                        .map(|accepted| format!("`{}`", accepted))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            ErrorKind::UnknownValue { value, accepted } => {
                let location = location_query_param_description(location, " for parameter");
                format!(
                    "Unknown value `{}`{location}: expected one of {}",
                    value,
                    accepted
                        .iter()
                        .map(|accepted| format!("`{}`", accepted))
                        .collect::<Vec<String>>()
                        .join(", "),
                )
            }
            ErrorKind::Unexpected { msg } => {
                let location = location_query_param_description(location, " in parameter");
                // serde_json original message:
                // The json payload provided is malformed. `trailing characters at line 1 column 19`.
                format!("Invalid value{location}: {msg}")
            }
        });

        Err(DeserrQueryParamError {
            msg: message,
            code: C::default().error_code(),
            _phantom: PhantomData,
        })
    }
}

fn value_kinds_description_query_param(_accepted: &[ValueKind]) -> String {
    "a string".to_owned()
}

fn value_description_with_kind_query_param<V: IntoValue>(actual: deserr::Value<V>) -> String {
    match actual {
        deserr::Value::Null => "null".to_owned(),
        deserr::Value::Boolean(x) => format!("a boolean: `{x}`"),
        deserr::Value::Integer(x) => format!("an integer: `{x}`"),
        deserr::Value::NegativeInteger(x) => {
            format!("an integer: `{x}`")
        }
        deserr::Value::Float(x) => {
            format!("a number: `{x}`")
        }
        deserr::Value::String(x) => {
            format!("a string: `{x}`")
        }
        deserr::Value::Sequence(_) => "multiple values".to_owned(),
        deserr::Value::Map(_) => "multiple parameters".to_owned(),
    }
}

#[derive(Debug)]
pub struct DetailedParseIntError(String);
impl fmt::Display for DetailedParseIntError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "could not parse `{}` as a positive integer", self.0)
    }
}
impl std::error::Error for DetailedParseIntError {}

pub fn parse_u32_query_param(x: String) -> Result<u32, TakeErrorMessage<DetailedParseIntError>> {
    x.parse::<u32>().map_err(|_e| TakeErrorMessage(DetailedParseIntError(x.to_owned())))
}
pub fn parse_usize_query_param(
    x: String,
) -> Result<usize, TakeErrorMessage<DetailedParseIntError>> {
    x.parse::<usize>().map_err(|_e| TakeErrorMessage(DetailedParseIntError(x.to_owned())))
}
pub fn parse_option_usize_query_param(
    s: Option<String>,
) -> Result<Option<usize>, TakeErrorMessage<DetailedParseIntError>> {
    if let Some(s) = s {
        parse_usize_query_param(s).map(Some)
    } else {
        Ok(None)
    }
}
pub fn parse_option_u32_query_param(
    s: Option<String>,
) -> Result<Option<u32>, TakeErrorMessage<DetailedParseIntError>> {
    if let Some(s) = s {
        parse_u32_query_param(s).map(Some)
    } else {
        Ok(None)
    }
}
pub fn parse_option_vec_u32_query_param(
    s: Option<serde_cs::vec::CS<String>>,
) -> Result<Option<Vec<u32>>, TakeErrorMessage<DetailedParseIntError>> {
    if let Some(s) = s {
        s.into_iter()
            .map(parse_u32_query_param)
            .collect::<Result<Vec<u32>, TakeErrorMessage<DetailedParseIntError>>>()
            .map(Some)
    } else {
        Ok(None)
    }
}
pub fn parse_option_cs_star_or<T: FromStr>(
    s: Option<CS<StarOr<String>>>,
) -> Result<Option<Vec<T>>, TakeErrorMessage<T::Err>> {
    if let Some(s) = s.and_then(fold_star_or) as Option<Vec<String>> {
        s.into_iter()
            .map(|s| T::from_str(&s))
            .collect::<Result<Vec<T>, T::Err>>()
            .map_err(TakeErrorMessage)
            .map(Some)
    } else {
        Ok(None)
    }
}

/// Extracts the raw values from the `StarOr` types and
/// return None if a `StarOr::Star` is encountered.
pub fn fold_star_or<T, O>(content: impl IntoIterator<Item = StarOr<T>>) -> Option<O>
where
    O: FromIterator<T>,
{
    content
        .into_iter()
        .map(|value| match value {
            StarOr::Star => None,
            StarOr::Other(val) => Some(val),
        })
        .collect()
}
pub struct TakeErrorMessage<T>(pub T);

impl<C: Default + ErrorCode, T> MergeWithError<TakeErrorMessage<T>> for DeserrJsonError<C>
where
    T: std::error::Error,
{
    fn merge(
        _self_: Option<Self>,
        other: TakeErrorMessage<T>,
        merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        DeserrJsonError::error::<Infallible>(
            None,
            deserr::ErrorKind::Unexpected { msg: other.0.to_string() },
            merge_location,
        )
    }
}

impl<C: Default + ErrorCode, T> MergeWithError<TakeErrorMessage<T>> for DeserrQueryParamError<C>
where
    T: std::error::Error,
{
    fn merge(
        _self_: Option<Self>,
        other: TakeErrorMessage<T>,
        merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        DeserrQueryParamError::error::<Infallible>(
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

#[cfg(test)]
mod tests {
    use deserr::ValueKind;

    use crate::error::value_kinds_description_json;

    #[test]
    fn test_value_kinds_description_json() {
        insta::assert_display_snapshot!(value_kinds_description_json(&[]), @"a different value");

        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Boolean]), @"a boolean");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer]), @"a positive integer");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::NegativeInteger]), @"an integer");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer]), @"a positive integer");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::String]), @"a string");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Sequence]), @"an array");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Map]), @"an object");

        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer, ValueKind::Boolean]), @"a boolean or a positive integer");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Null, ValueKind::Integer]), @"null or a positive integer");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Sequence, ValueKind::NegativeInteger]), @"an integer or an array");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer, ValueKind::Float]), @"a number");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer, ValueKind::Float, ValueKind::NegativeInteger]), @"a number");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer, ValueKind::Float, ValueKind::NegativeInteger, ValueKind::Null]), @"null or a number");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Boolean, ValueKind::Integer, ValueKind::Float, ValueKind::NegativeInteger, ValueKind::Null]), @"null, a boolean, or a number");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Null, ValueKind::Boolean, ValueKind::Integer, ValueKind::Float, ValueKind::NegativeInteger, ValueKind::Null]), @"null, a boolean, or a number");
    }
}
