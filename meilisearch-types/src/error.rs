use std::{fmt, io};

use actix_web::http::StatusCode;
use actix_web::{self as aweb, HttpResponseBuilder};
use aweb::rt::task::JoinError;
use convert_case::Casing;
use milli::heed::{Error as HeedError, MdbError};
use serde::{Deserialize, Serialize};

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
            error_code: code.err_code().error_name.to_string(),
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
    T: ErrorCode,
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

pub trait ErrorCode: std::error::Error {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    // error related to your setup
    IoError,
    NoSpaceLeftOnDevice,
    TooManyOpenFiles,

    // index related error
    CreateIndex,
    IndexAlreadyExists,
    InvalidIndexPrimaryKey,
    IndexNotFound,
    InvalidIndexUid,
    MissingIndexUid,
    InvalidMinWordLengthForTypo,
    InvalidIndexLimit,
    InvalidIndexOffset,

    DuplicateIndexFound,

    // invalid state error
    InvalidState,
    NoPrimaryKeyCandidateFound,
    MultiplePrimaryKeyCandidatesFound,
    PrimaryKeyAlreadyPresent,

    MaxFieldsLimitExceeded,
    MissingDocumentId,
    InvalidDocumentId,

    Filter,
    Sort,

    // Invalid swap-indexes
    InvalidSwapIndexes,
    InvalidDuplicateIndexesFound,

    // Invalid settings update request
    InvalidSettingsDisplayedAttributes,
    InvalidSettingsSearchableAttributes,
    InvalidSettingsFilterableAttributes,
    InvalidSettingsSortableAttributes,
    InvalidSettingsRankingRules,
    InvalidSettingsStopWords,
    InvalidSettingsSynonyms,
    InvalidSettingsDistinctAttribute,
    InvalidSettingsTypoTolerance,
    InvalidSettingsFaceting,
    InvalidSettingsPagination,

    // Invalid search request
    InvalidSearchQ,
    InvalidSearchOffset,
    InvalidSearchLimit,
    InvalidSearchPage,
    InvalidSearchHitsPerPage,
    InvalidSearchAttributesToRetrieve,
    InvalidSearchAttributesToCrop,
    InvalidSearchCropLength,
    InvalidSearchAttributesToHighlight,
    InvalidSearchShowMatchesPosition,
    InvalidSearchFilter,
    InvalidSearchSort,
    InvalidSearchFacets,
    InvalidSearchHighlightPreTag,
    InvalidSearchHighlightPostTag,
    InvalidSearchCropMarker,
    InvalidSearchMatchingStrategy,

    // Related to the tasks
    InvalidTaskUids,
    InvalidTaskTypes,
    InvalidTaskStatuses,
    InvalidTaskCanceledBy,
    InvalidTaskLimit,
    InvalidTaskFrom,
    InvalidTaskBeforeEnqueuedAt,
    InvalidTaskAfterEnqueuedAt,
    InvalidTaskBeforeStartedAt,
    InvalidTaskAfterStartedAt,
    InvalidTaskBeforeFinishedAt,
    InvalidTaskAfterFinishedAt,

    // Documents API
    InvalidDocumentFields,
    InvalidDocumentLimit,
    InvalidDocumentOffset,

    BadParameter,
    BadRequest,
    DatabaseSizeLimitReached,
    DocumentNotFound,
    Internal,
    InvalidDocumentGeoField,
    InvalidRankingRule,
    InvalidStore,
    InvalidToken,
    MissingAuthorizationHeader,
    MissingMasterKey,
    DumpNotFound,
    TaskNotFound,
    TaskDeletionWithEmptyQuery,
    TaskCancelationWithEmptyQuery,
    PayloadTooLarge,
    RetrieveDocument,
    SearchDocuments,
    UnsupportedMediaType,

    DumpAlreadyInProgress,
    DumpProcessFailed,
    // Only used when importing a dump
    UnretrievableErrorCode,

    InvalidContentType,
    MissingContentType,
    MalformedPayload,
    MissingPayload,

    ApiKeyNotFound,

    MissingApiKeyActions,
    MissingApiKeyExpiresAt,
    MissingApiKeyIndexes,

    InvalidApiKeyOffset,
    InvalidApiKeyLimit,
    InvalidApiKeyActions,
    InvalidApiKeyIndexes,
    InvalidApiKeyExpiresAt,
    InvalidApiKeyDescription,
    InvalidApiKeyName,
    InvalidApiKeyUid,
    ImmutableField,
    ApiKeyAlreadyExists,
}

impl Code {
    /// associate a `Code` variant to the actual ErrCode
    fn err_code(&self) -> ErrCode {
        use Code::*;

        match self {
            // related to the setup
            IoError => ErrCode::system("io_error", StatusCode::INTERNAL_SERVER_ERROR),
            TooManyOpenFiles => {
                ErrCode::system("too_many_open_files", StatusCode::INTERNAL_SERVER_ERROR)
            }
            NoSpaceLeftOnDevice => {
                ErrCode::system("no_space_left_on_device", StatusCode::INTERNAL_SERVER_ERROR)
            }

            // index related errors
            // create index is thrown on internal error while creating an index.
            CreateIndex => {
                ErrCode::internal("index_creation_failed", StatusCode::INTERNAL_SERVER_ERROR)
            }
            IndexAlreadyExists => ErrCode::invalid("index_already_exists", StatusCode::CONFLICT),
            // thrown when requesting an unexisting index
            IndexNotFound => ErrCode::invalid("index_not_found", StatusCode::NOT_FOUND),
            InvalidIndexUid => ErrCode::invalid("invalid_index_uid", StatusCode::BAD_REQUEST),
            MissingIndexUid => ErrCode::invalid("missing_index_uid", StatusCode::BAD_REQUEST),
            InvalidIndexPrimaryKey => {
                ErrCode::invalid("invalid_index_primary_key", StatusCode::BAD_REQUEST)
            }
            InvalidIndexLimit => ErrCode::invalid("invalid_index_limit", StatusCode::BAD_REQUEST),
            InvalidIndexOffset => ErrCode::invalid("invalid_index_offset", StatusCode::BAD_REQUEST),

            // invalid state error
            InvalidState => ErrCode::internal("invalid_state", StatusCode::INTERNAL_SERVER_ERROR),
            // thrown when no primary key has been set
            NoPrimaryKeyCandidateFound => {
                ErrCode::invalid("index_primary_key_no_candidate_found", StatusCode::BAD_REQUEST)
            }
            MultiplePrimaryKeyCandidatesFound => ErrCode::invalid(
                "index_primary_key_multiple_candidates_found",
                StatusCode::BAD_REQUEST,
            ),
            // error thrown when trying to set an already existing primary key
            PrimaryKeyAlreadyPresent => {
                ErrCode::invalid("index_primary_key_already_exists", StatusCode::BAD_REQUEST)
            }
            // invalid ranking rule
            InvalidRankingRule => ErrCode::invalid("invalid_ranking_rule", StatusCode::BAD_REQUEST),

            // invalid database
            InvalidStore => {
                ErrCode::internal("invalid_store_file", StatusCode::INTERNAL_SERVER_ERROR)
            }

            // invalid document
            MaxFieldsLimitExceeded => {
                ErrCode::invalid("max_fields_limit_exceeded", StatusCode::BAD_REQUEST)
            }
            MissingDocumentId => ErrCode::invalid("missing_document_id", StatusCode::BAD_REQUEST),
            InvalidDocumentId => ErrCode::invalid("invalid_document_id", StatusCode::BAD_REQUEST),

            // error related to filters
            Filter => ErrCode::invalid("invalid_filter", StatusCode::BAD_REQUEST),
            // error related to sorts
            Sort => ErrCode::invalid("invalid_sort", StatusCode::BAD_REQUEST),

            BadParameter => ErrCode::invalid("bad_parameter", StatusCode::BAD_REQUEST),
            BadRequest => ErrCode::invalid("bad_request", StatusCode::BAD_REQUEST),
            DatabaseSizeLimitReached => {
                ErrCode::internal("database_size_limit_reached", StatusCode::INTERNAL_SERVER_ERROR)
            }
            DocumentNotFound => ErrCode::invalid("document_not_found", StatusCode::NOT_FOUND),
            Internal => ErrCode::internal("internal", StatusCode::INTERNAL_SERVER_ERROR),
            InvalidDocumentGeoField => {
                ErrCode::invalid("invalid_document_geo_field", StatusCode::BAD_REQUEST)
            }
            InvalidToken => ErrCode::authentication("invalid_api_key", StatusCode::FORBIDDEN),
            MissingAuthorizationHeader => {
                ErrCode::authentication("missing_authorization_header", StatusCode::UNAUTHORIZED)
            }
            MissingMasterKey => {
                ErrCode::authentication("missing_master_key", StatusCode::UNAUTHORIZED)
            }
            TaskNotFound => ErrCode::invalid("task_not_found", StatusCode::NOT_FOUND),
            TaskDeletionWithEmptyQuery => {
                ErrCode::invalid("missing_task_filters", StatusCode::BAD_REQUEST)
            }
            TaskCancelationWithEmptyQuery => {
                ErrCode::invalid("missing_task_filters", StatusCode::BAD_REQUEST)
            }
            DumpNotFound => ErrCode::invalid("dump_not_found", StatusCode::NOT_FOUND),
            PayloadTooLarge => ErrCode::invalid("payload_too_large", StatusCode::PAYLOAD_TOO_LARGE),
            RetrieveDocument => {
                ErrCode::internal("unretrievable_document", StatusCode::BAD_REQUEST)
            }
            SearchDocuments => ErrCode::internal("search_error", StatusCode::BAD_REQUEST),
            UnsupportedMediaType => {
                ErrCode::invalid("unsupported_media_type", StatusCode::UNSUPPORTED_MEDIA_TYPE)
            }

            // error related to dump
            DumpAlreadyInProgress => {
                ErrCode::invalid("dump_already_processing", StatusCode::CONFLICT)
            }
            DumpProcessFailed => {
                ErrCode::internal("dump_process_failed", StatusCode::INTERNAL_SERVER_ERROR)
            }
            MissingContentType => {
                ErrCode::invalid("missing_content_type", StatusCode::UNSUPPORTED_MEDIA_TYPE)
            }
            MalformedPayload => ErrCode::invalid("malformed_payload", StatusCode::BAD_REQUEST),
            InvalidContentType => {
                ErrCode::invalid("invalid_content_type", StatusCode::UNSUPPORTED_MEDIA_TYPE)
            }
            MissingPayload => ErrCode::invalid("missing_payload", StatusCode::BAD_REQUEST),
            // This one can only happen when importing a dump and encountering an unknown code in the task queue.
            UnretrievableErrorCode => {
                ErrCode::invalid("unretrievable_error_code", StatusCode::BAD_REQUEST)
            }

            // error related to keys
            ApiKeyNotFound => ErrCode::invalid("api_key_not_found", StatusCode::NOT_FOUND),

            MissingApiKeyExpiresAt => {
                ErrCode::invalid("missing_api_key_expires_at", StatusCode::BAD_REQUEST)
            }

            MissingApiKeyActions => {
                ErrCode::invalid("missing_api_key_actions", StatusCode::BAD_REQUEST)
            }

            MissingApiKeyIndexes => {
                ErrCode::invalid("missing_api_key_indexes", StatusCode::BAD_REQUEST)
            }

            InvalidApiKeyOffset => {
                ErrCode::invalid("invalid_api_key_offset", StatusCode::BAD_REQUEST)
            }
            InvalidApiKeyLimit => {
                ErrCode::invalid("invalid_api_key_limit", StatusCode::BAD_REQUEST)
            }
            InvalidApiKeyActions => {
                ErrCode::invalid("invalid_api_key_actions", StatusCode::BAD_REQUEST)
            }
            InvalidApiKeyIndexes => {
                ErrCode::invalid("invalid_api_key_indexes", StatusCode::BAD_REQUEST)
            }
            InvalidApiKeyExpiresAt => {
                ErrCode::invalid("invalid_api_key_expires_at", StatusCode::BAD_REQUEST)
            }
            InvalidApiKeyDescription => {
                ErrCode::invalid("invalid_api_key_description", StatusCode::BAD_REQUEST)
            }
            InvalidApiKeyName => ErrCode::invalid("invalid_api_key_name", StatusCode::BAD_REQUEST),
            InvalidApiKeyUid => ErrCode::invalid("invalid_api_key_uid", StatusCode::BAD_REQUEST),
            ApiKeyAlreadyExists => ErrCode::invalid("api_key_already_exists", StatusCode::CONFLICT),
            ImmutableField => ErrCode::invalid("immutable_field", StatusCode::BAD_REQUEST),
            InvalidMinWordLengthForTypo => {
                ErrCode::invalid("invalid_min_word_length_for_typo", StatusCode::BAD_REQUEST)
            }
            DuplicateIndexFound => {
                ErrCode::invalid("duplicate_index_found", StatusCode::BAD_REQUEST)
            }

            // Swap indexes error
            InvalidSwapIndexes => ErrCode::invalid("invalid_swap_indexes", StatusCode::BAD_REQUEST),
            InvalidDuplicateIndexesFound => {
                ErrCode::invalid("invalid_swap_duplicate_index_found", StatusCode::BAD_REQUEST)
            }

            // Invalid settings
            InvalidSettingsDisplayedAttributes => {
                ErrCode::invalid("invalid_settings_displayed_attributes", StatusCode::BAD_REQUEST)
            }
            InvalidSettingsSearchableAttributes => {
                ErrCode::invalid("invalid_settings_searchable_attributes", StatusCode::BAD_REQUEST)
            }
            InvalidSettingsFilterableAttributes => {
                ErrCode::invalid("invalid_settings_filterable_attributes", StatusCode::BAD_REQUEST)
            }
            InvalidSettingsSortableAttributes => {
                ErrCode::invalid("invalid_settings_sortable_attributes", StatusCode::BAD_REQUEST)
            }
            InvalidSettingsRankingRules => {
                ErrCode::invalid("invalid_settings_ranking_rules", StatusCode::BAD_REQUEST)
            }
            InvalidSettingsStopWords => {
                ErrCode::invalid("invalid_settings_stop_words", StatusCode::BAD_REQUEST)
            }
            InvalidSettingsSynonyms => {
                ErrCode::invalid("invalid_settings_synonyms", StatusCode::BAD_REQUEST)
            }
            InvalidSettingsDistinctAttribute => {
                ErrCode::invalid("invalid_settings_distinct_attribute", StatusCode::BAD_REQUEST)
            }
            InvalidSettingsTypoTolerance => {
                ErrCode::invalid("invalid_settings_typo_tolerance", StatusCode::BAD_REQUEST)
            }
            InvalidSettingsFaceting => {
                ErrCode::invalid("invalid_settings_faceting", StatusCode::BAD_REQUEST)
            }
            InvalidSettingsPagination => {
                ErrCode::invalid("invalid_settings_pagination", StatusCode::BAD_REQUEST)
            }

            // Invalid search
            InvalidSearchQ => ErrCode::invalid("invalid_search_q", StatusCode::BAD_REQUEST),
            InvalidSearchOffset => {
                ErrCode::invalid("invalid_search_offset", StatusCode::BAD_REQUEST)
            }
            InvalidSearchLimit => ErrCode::invalid("invalid_search_limit", StatusCode::BAD_REQUEST),
            InvalidSearchPage => ErrCode::invalid("invalid_search_page", StatusCode::BAD_REQUEST),
            InvalidSearchHitsPerPage => {
                ErrCode::invalid("invalid_search_hits_per_page", StatusCode::BAD_REQUEST)
            }
            InvalidSearchAttributesToRetrieve => {
                ErrCode::invalid("invalid_search_attributes_to_retrieve", StatusCode::BAD_REQUEST)
            }
            InvalidSearchAttributesToCrop => {
                ErrCode::invalid("invalid_search_attributes_to_crop", StatusCode::BAD_REQUEST)
            }
            InvalidSearchCropLength => {
                ErrCode::invalid("invalid_search_crop_length", StatusCode::BAD_REQUEST)
            }
            InvalidSearchAttributesToHighlight => {
                ErrCode::invalid("invalid_search_attributes_to_highlight", StatusCode::BAD_REQUEST)
            }
            InvalidSearchShowMatchesPosition => {
                ErrCode::invalid("invalid_search_show_matches_position", StatusCode::BAD_REQUEST)
            }
            InvalidSearchFilter => {
                ErrCode::invalid("invalid_search_filter", StatusCode::BAD_REQUEST)
            }
            InvalidSearchSort => ErrCode::invalid("invalid_search_sort", StatusCode::BAD_REQUEST),
            InvalidSearchFacets => {
                ErrCode::invalid("invalid_search_facets", StatusCode::BAD_REQUEST)
            }
            InvalidSearchHighlightPreTag => {
                ErrCode::invalid("invalid_search_highlight_pre_tag", StatusCode::BAD_REQUEST)
            }
            InvalidSearchHighlightPostTag => {
                ErrCode::invalid("invalid_search_highlight_post_tag", StatusCode::BAD_REQUEST)
            }
            InvalidSearchCropMarker => {
                ErrCode::invalid("invalid_search_crop_marker", StatusCode::BAD_REQUEST)
            }
            InvalidSearchMatchingStrategy => {
                ErrCode::invalid("invalid_search_matching_strategy", StatusCode::BAD_REQUEST)
            }

            // Related to the tasks
            InvalidTaskUids => ErrCode::invalid("invalid_task_uids", StatusCode::BAD_REQUEST),
            InvalidTaskTypes => ErrCode::invalid("invalid_task_types", StatusCode::BAD_REQUEST),
            InvalidTaskStatuses => {
                ErrCode::invalid("invalid_task_statuses", StatusCode::BAD_REQUEST)
            }
            InvalidTaskCanceledBy => {
                ErrCode::invalid("invalid_task_canceled_by", StatusCode::BAD_REQUEST)
            }
            InvalidTaskLimit => ErrCode::invalid("invalid_task_limit", StatusCode::BAD_REQUEST),
            InvalidTaskFrom => ErrCode::invalid("invalid_task_from", StatusCode::BAD_REQUEST),
            InvalidTaskBeforeEnqueuedAt => {
                ErrCode::invalid("invalid_task_before_enqueued_at", StatusCode::BAD_REQUEST)
            }
            InvalidTaskAfterEnqueuedAt => {
                ErrCode::invalid("invalid_task_after_enqueued_at", StatusCode::BAD_REQUEST)
            }
            InvalidTaskBeforeStartedAt => {
                ErrCode::invalid("invalid_task_before_started_at", StatusCode::BAD_REQUEST)
            }
            InvalidTaskAfterStartedAt => {
                ErrCode::invalid("invalid_task_after_started_at", StatusCode::BAD_REQUEST)
            }
            InvalidTaskBeforeFinishedAt => {
                ErrCode::invalid("invalid_task_before_finished_at", StatusCode::BAD_REQUEST)
            }
            InvalidTaskAfterFinishedAt => {
                ErrCode::invalid("invalid_task_after_finished_at", StatusCode::BAD_REQUEST)
            }

            InvalidDocumentFields => {
                ErrCode::invalid("invalid_document_fields", StatusCode::BAD_REQUEST)
            }
            InvalidDocumentLimit => {
                ErrCode::invalid("invalid_document_limit", StatusCode::BAD_REQUEST)
            }
            InvalidDocumentOffset => {
                ErrCode::invalid("invalid_document_offset", StatusCode::BAD_REQUEST)
            }
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

/// Internal structure providing a convenient way to create error codes
struct ErrCode {
    status_code: StatusCode,
    error_type: ErrorType,
    error_name: &'static str,
}

impl ErrCode {
    fn authentication(error_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode { status_code, error_name, error_type: ErrorType::AuthenticationError }
    }

    fn internal(error_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode { status_code, error_name, error_type: ErrorType::InternalError }
    }

    fn invalid(error_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode { status_code, error_name, error_type: ErrorType::InvalidRequestError }
    }

    fn system(error_name: &'static str, status_code: StatusCode) -> ErrCode {
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
                    UserError::InvalidStoreFile => Code::InvalidStore,
                    UserError::NoSpaceLeftOnDevice => Code::NoSpaceLeftOnDevice,
                    UserError::MaxDatabaseSizeReached => Code::DatabaseSizeLimitReached,
                    UserError::AttributeLimitReached => Code::MaxFieldsLimitExceeded,
                    UserError::InvalidFilter(_) => Code::Filter,
                    UserError::MissingDocumentId { .. } => Code::MissingDocumentId,
                    UserError::InvalidDocumentId { .. } | UserError::TooManyDocumentIds { .. } => {
                        Code::InvalidDocumentId
                    }
                    UserError::NoPrimaryKeyCandidateFound => Code::NoPrimaryKeyCandidateFound,
                    UserError::MultiplePrimaryKeyCandidatesFound { .. } => {
                        Code::MultiplePrimaryKeyCandidatesFound
                    }
                    UserError::PrimaryKeyCannotBeChanged(_) => Code::PrimaryKeyAlreadyPresent,
                    UserError::SortRankingRuleMissing => Code::Sort,
                    UserError::InvalidFacetsDistribution { .. } => Code::BadRequest,
                    UserError::InvalidSortableAttribute { .. } => Code::Sort,
                    UserError::CriterionError(_) => Code::InvalidRankingRule,
                    UserError::InvalidGeoField { .. } => Code::InvalidDocumentGeoField,
                    UserError::SortError(_) => Code::Sort,
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
            HeedError::Mdb(MdbError::Invalid) => Code::InvalidStore,
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
