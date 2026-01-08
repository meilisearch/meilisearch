use std::{fmt, io};

use actix_web::http::StatusCode;
use actix_web::{self as aweb, HttpResponseBuilder};
use aweb::http::header;
use aweb::rt::task::JoinError;
use convert_case::Casing;
use milli::cellulite;
use milli::heed::{Error as HeedError, MdbError};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ResponseError {
    #[serde(skip)]
    pub code: StatusCode,
    /// The error message.
    pub message: String,
    /// The error code.
    #[schema(value_type = Code)]
    #[serde(rename = "code")]
    error_code: String,
    /// The error type.
    #[schema(value_type = ErrorType)]
    #[serde(rename = "type")]
    error_type: String,
    /// A link to the documentation about this specific error.
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
            error_code: code.name(),
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
        let mut builder = HttpResponseBuilder::new(self.status_code());
        builder.content_type("application/json");

        if self.code == StatusCode::SERVICE_UNAVAILABLE {
            builder.insert_header((header::RETRY_AFTER, "10"));
        }

        builder.body(json)
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
#[derive(ToSchema)]
#[schema(rename_all = "snake_case")]
pub enum ErrorType {
    Internal,
    InvalidRequest,
    Auth,
    System,
}

impl fmt::Display for ErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ErrorType::*;

        match self {
            Internal => write!(f, "internal"),
            InvalidRequest => write!(f, "invalid_request"),
            Auth => write!(f, "auth"),
            System => write!(f, "system"),
        }
    }
}

/// Implement all the error codes.
///
/// 1. Make an enum `Code` where each error code is a variant
/// 2. Implement the `http`, `name`, and `type_` method on the enum
/// 3. Make a unit type for each error code in the module `deserr_codes`.
///
/// The unit type's purpose is to be used as a marker type parameter, e.g.
/// `DeserrJsonError<MyErrorCode>`. It implements `Default` and `ErrorCode`,
/// so we can get a value of the `Code` enum with the correct variant by calling
/// `MyErrorCode::default().error_code()`.
macro_rules! make_error_codes {
    ($($code_ident:ident, $err_type:ident, $status:ident);*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, ToSchema)]
        #[schema(rename_all = "snake_case")]
        pub enum Code {
            $($code_ident),*
        }
        impl Code {
            /// return the HTTP status code associated with the `Code`
            pub fn http(&self) -> StatusCode {
                match self {
                    $(
                        Code::$code_ident => StatusCode::$status
                    ),*
                }
            }

            /// return error name, used as error code
            pub fn name(&self) -> String {
                match self {
                    $(
                        Code::$code_ident => stringify!($code_ident).to_case(convert_case::Case::Snake)
                    ),*
                }
            }

            /// return the error type
            fn type_(&self) -> String {
                match self {
                    $(
                        Code::$code_ident => ErrorType::$err_type.to_string()
                    ),*
                }
            }

            /// return the doc url associated with the error
            fn url(&self) -> String {
                format!("https://docs.meilisearch.com/errors#{}", self.name())
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

// An exhaustive list of all the error codes used by meilisearch.
make_error_codes! {
ApiKeyAlreadyExists                            , InvalidRequest       , CONFLICT ;
ApiKeyNotFound                                 , InvalidRequest       , NOT_FOUND ;
BadParameter                                   , InvalidRequest       , BAD_REQUEST;
BadRequest                                     , InvalidRequest       , BAD_REQUEST;
DatabaseSizeLimitReached                       , Internal             , INTERNAL_SERVER_ERROR;
DocumentNotFound                               , InvalidRequest       , NOT_FOUND;
DumpAlreadyProcessing                          , InvalidRequest       , CONFLICT;
DumpNotFound                                   , InvalidRequest       , NOT_FOUND;
DumpProcessFailed                              , Internal             , INTERNAL_SERVER_ERROR;
DuplicateIndexFound                            , InvalidRequest       , BAD_REQUEST;
ImmutableApiKeyActions                         , InvalidRequest       , BAD_REQUEST;
ImmutableApiKeyCreatedAt                       , InvalidRequest       , BAD_REQUEST;
ImmutableApiKeyExpiresAt                       , InvalidRequest       , BAD_REQUEST;
ImmutableApiKeyIndexes                         , InvalidRequest       , BAD_REQUEST;
ImmutableApiKeyKey                             , InvalidRequest       , BAD_REQUEST;
ImmutableApiKeyUid                             , InvalidRequest       , BAD_REQUEST;
ImmutableApiKeyUpdatedAt                       , InvalidRequest       , BAD_REQUEST;
ImmutableIndexCreatedAt                        , InvalidRequest       , BAD_REQUEST;
ImmutableIndexUpdatedAt                        , InvalidRequest       , BAD_REQUEST;
ImportTaskAlreadyReceived                      , InvalidRequest       , PRECONDITION_FAILED;
ImportTaskUnknownRemote                        , InvalidRequest       , PRECONDITION_FAILED;
ImportTaskWithoutNetworkTask                   , InvalidRequest       , SERVICE_UNAVAILABLE;
IndexAlreadyExists                             , InvalidRequest       , CONFLICT ;
IndexCreationFailed                            , Internal             , INTERNAL_SERVER_ERROR;
IndexNotFound                                  , InvalidRequest       , NOT_FOUND;
IndexPrimaryKeyAlreadyExists                   , InvalidRequest       , BAD_REQUEST ;
IndexPrimaryKeyMultipleCandidatesFound         , InvalidRequest       , BAD_REQUEST;
IndexPrimaryKeyNoCandidateFound                , InvalidRequest       , BAD_REQUEST ;
Internal                                       , Internal             , INTERNAL_SERVER_ERROR ;
InvalidApiKey                                  , Auth                 , FORBIDDEN ;
InvalidApiKeyActions                           , InvalidRequest       , BAD_REQUEST ;
InvalidApiKeyDescription                       , InvalidRequest       , BAD_REQUEST ;
InvalidApiKeyExpiresAt                         , InvalidRequest       , BAD_REQUEST ;
InvalidApiKeyIndexes                           , InvalidRequest       , BAD_REQUEST ;
InvalidApiKeyLimit                             , InvalidRequest       , BAD_REQUEST ;
InvalidApiKeyName                              , InvalidRequest       , BAD_REQUEST ;
InvalidApiKeyOffset                            , InvalidRequest       , BAD_REQUEST ;
InvalidApiKeyUid                               , InvalidRequest       , BAD_REQUEST ;
InvalidContentType                             , InvalidRequest       , UNSUPPORTED_MEDIA_TYPE ;
InvalidDocumentCsvDelimiter                    , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentFields                          , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentRetrieveVectors                 , InvalidRequest       , BAD_REQUEST ;
MissingDocumentFilter                          , InvalidRequest       , BAD_REQUEST ;
MissingDocumentEditionFunction                 , InvalidRequest       , BAD_REQUEST ;
InconsistentDocumentChangeHeaders              , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentFilter                          , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentSort                            , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentGeoField                        , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentGeojsonField                    , InvalidRequest       , BAD_REQUEST ;
InvalidHeaderValue                             , InvalidRequest       , BAD_REQUEST ;
InvalidVectorDimensions                        , InvalidRequest       , BAD_REQUEST ;
InvalidVectorsType                             , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentId                              , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentIds                             , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentLimit                           , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentOffset                          , InvalidRequest       , BAD_REQUEST ;
InvalidSearchEmbedder                          , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarEmbedder                         , InvalidRequest       , BAD_REQUEST ;
InvalidSearchHybridQuery                       , InvalidRequest       , BAD_REQUEST ;
InvalidIndexLimit                              , InvalidRequest       , BAD_REQUEST ;
InvalidIndexOffset                             , InvalidRequest       , BAD_REQUEST ;
InvalidIndexPrimaryKey                         , InvalidRequest       , BAD_REQUEST ;
InvalidIndexCustomMetadata                     , InvalidRequest       , BAD_REQUEST ;
InvalidSkipCreation                            , InvalidRequest       , BAD_REQUEST ;
InvalidIndexUid                                , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchFacets                       , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchFacetsByIndex                , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchFacetOrder                   , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchQueryPersonalization         , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchFederated                    , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchFederationOptions            , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchMaxValuesPerFacet            , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchMergeFacets                  , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchQueryFacets                  , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchQueryPagination              , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchQueryRankingRules            , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchQueryPosition                , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchRemote                       , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchWeight                       , InvalidRequest       , BAD_REQUEST ;
InvalidNetworkLeader                           , InvalidRequest       , BAD_REQUEST ;
InvalidNetworkRemotes                          , InvalidRequest       , BAD_REQUEST ;
InvalidNetworkSelf                             , InvalidRequest       , BAD_REQUEST ;
InvalidNetworkSearchApiKey                     , InvalidRequest       , BAD_REQUEST ;
InvalidNetworkWriteApiKey                      , InvalidRequest       , BAD_REQUEST ;
InvalidNetworkUrl                              , InvalidRequest       , BAD_REQUEST ;
InvalidSearchAttributesToSearchOn              , InvalidRequest       , BAD_REQUEST ;
InvalidSearchAttributesToCrop                  , InvalidRequest       , BAD_REQUEST ;
InvalidSearchAttributesToHighlight             , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarAttributesToRetrieve             , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarRetrieveVectors                  , InvalidRequest       , BAD_REQUEST ;
InvalidSearchAttributesToRetrieve              , InvalidRequest       , BAD_REQUEST ;
InvalidSearchRankingScoreThreshold             , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarRankingScoreThreshold            , InvalidRequest       , BAD_REQUEST ;
InvalidSearchRetrieveVectors                   , InvalidRequest       , BAD_REQUEST ;
InvalidSearchCropLength                        , InvalidRequest       , BAD_REQUEST ;
InvalidSearchCropMarker                        , InvalidRequest       , BAD_REQUEST ;
InvalidSearchFacets                            , InvalidRequest       , BAD_REQUEST ;
InvalidSearchSemanticRatio                     , InvalidRequest       , BAD_REQUEST ;
InvalidSearchLocales                           , InvalidRequest       , BAD_REQUEST ;
InvalidFacetSearchExhaustiveFacetCount         , InvalidRequest       , BAD_REQUEST ;
InvalidFacetSearchFacetName                    , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarId                               , InvalidRequest       , BAD_REQUEST ;
InvalidSearchFilter                            , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarFilter                           , InvalidRequest       , BAD_REQUEST ;
InvalidSearchHighlightPostTag                  , InvalidRequest       , BAD_REQUEST ;
InvalidSearchHighlightPreTag                   , InvalidRequest       , BAD_REQUEST ;
InvalidSearchHitsPerPage                       , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarLimit                            , InvalidRequest       , BAD_REQUEST ;
InvalidSearchLimit                             , InvalidRequest       , BAD_REQUEST ;
InvalidSearchMatchingStrategy                  , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarOffset                           , InvalidRequest       , BAD_REQUEST ;
InvalidSearchOffset                            , InvalidRequest       , BAD_REQUEST ;
InvalidSearchPage                              , InvalidRequest       , BAD_REQUEST ;
InvalidSearchQ                                 , InvalidRequest       , BAD_REQUEST ;
InvalidFacetSearchQuery                        , InvalidRequest       , BAD_REQUEST ;
InvalidFacetSearchName                         , InvalidRequest       , BAD_REQUEST ;
FacetSearchDisabled                            , InvalidRequest       , BAD_REQUEST ;
InvalidSearchVector                            , InvalidRequest       , BAD_REQUEST ;
InvalidSearchMedia                             , InvalidRequest       , BAD_REQUEST ;
InvalidSearchShowMatchesPosition               , InvalidRequest       , BAD_REQUEST ;
InvalidSearchShowRankingScore                  , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarShowRankingScore                 , InvalidRequest       , BAD_REQUEST ;
InvalidSearchShowRankingScoreDetails           , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarShowRankingScoreDetails          , InvalidRequest       , BAD_REQUEST ;
InvalidSearchSort                              , InvalidRequest       , BAD_REQUEST ;
InvalidSearchDistinct                          , InvalidRequest       , BAD_REQUEST ;
InvalidSearchPersonalize                       , InvalidRequest       , BAD_REQUEST ;
InvalidSearchPersonalizeUserContext            , InvalidRequest       , BAD_REQUEST ;
InvalidSearchMediaAndVector                    , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsDisplayedAttributes             , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsDistinctAttribute               , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsProximityPrecision              , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsFacetSearch                     , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsPrefixSearch                    , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsFaceting                        , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsFilterableAttributes            , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsPagination                      , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsSearchCutoffMs                  , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsEmbedders                       , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsRankingRules                    , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsSearchableAttributes            , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsSortableAttributes              , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsStopWords                       , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsNonSeparatorTokens              , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsSeparatorTokens                 , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsDictionary                      , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsSynonyms                        , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsTypoTolerance                   , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsLocalizedAttributes             , InvalidRequest       , BAD_REQUEST ;
InvalidState                                   , Internal             , INTERNAL_SERVER_ERROR ;
InvalidStoreFile                               , Internal             , INTERNAL_SERVER_ERROR ;
InvalidSwapDuplicateIndexFound                 , InvalidRequest       , BAD_REQUEST ;
InvalidSwapIndexes                             , InvalidRequest       , BAD_REQUEST ;
InvalidSwapRename                              , InvalidRequest       , BAD_REQUEST ;
InvalidTaskAfterEnqueuedAt                     , InvalidRequest       , BAD_REQUEST ;
InvalidTaskAfterFinishedAt                     , InvalidRequest       , BAD_REQUEST ;
InvalidTaskAfterStartedAt                      , InvalidRequest       , BAD_REQUEST ;
InvalidTaskBeforeEnqueuedAt                    , InvalidRequest       , BAD_REQUEST ;
InvalidTaskBeforeFinishedAt                    , InvalidRequest       , BAD_REQUEST ;
InvalidTaskBeforeStartedAt                     , InvalidRequest       , BAD_REQUEST ;
InvalidTaskCanceledBy                          , InvalidRequest       , BAD_REQUEST ;
InvalidTaskFrom                                , InvalidRequest       , BAD_REQUEST ;
InvalidTaskLimit                               , InvalidRequest       , BAD_REQUEST ;
InvalidTaskReverse                             , InvalidRequest       , BAD_REQUEST ;
InvalidTaskStatuses                            , InvalidRequest       , BAD_REQUEST ;
InvalidTaskTypes                               , InvalidRequest       , BAD_REQUEST ;
InvalidTaskUids                                , InvalidRequest       , BAD_REQUEST  ;
InvalidBatchUids                               , InvalidRequest       , BAD_REQUEST  ;
IoError                                        , System               , UNPROCESSABLE_ENTITY;
FeatureNotEnabled                              , InvalidRequest       , BAD_REQUEST ;
MalformedPayload                               , InvalidRequest       , BAD_REQUEST ;
MaxFieldsLimitExceeded                         , InvalidRequest       , BAD_REQUEST ;
MissingApiKeyActions                           , InvalidRequest       , BAD_REQUEST ;
MissingApiKeyExpiresAt                         , InvalidRequest       , BAD_REQUEST ;
MissingApiKeyIndexes                           , InvalidRequest       , BAD_REQUEST ;
MissingAuthorizationHeader                     , Auth                 , UNAUTHORIZED ;
MissingContentType                             , InvalidRequest       , UNSUPPORTED_MEDIA_TYPE ;
MissingDocumentId                              , InvalidRequest       , BAD_REQUEST ;
MissingFacetSearchFacetName                    , InvalidRequest       , BAD_REQUEST ;
MissingIndexUid                                , InvalidRequest       , BAD_REQUEST ;
MissingMasterKey                               , Auth                 , UNAUTHORIZED ;
MissingNetworkUrl                              , InvalidRequest       , BAD_REQUEST ;
MissingPayload                                 , InvalidRequest       , BAD_REQUEST ;
MissingSearchHybrid                            , InvalidRequest       , BAD_REQUEST ;
MissingSwapIndexes                             , InvalidRequest       , BAD_REQUEST ;
MissingTaskFilters                             , InvalidRequest       , BAD_REQUEST ;
NetworkVersionMismatch                         , InvalidRequest       , PRECONDITION_FAILED ;
NoSpaceLeftOnDevice                            , System               , UNPROCESSABLE_ENTITY;
NotLeader                                      , InvalidRequest       , BAD_REQUEST ;
PayloadTooLarge                                , InvalidRequest       , PAYLOAD_TOO_LARGE ;
RemoteBadResponse                              , System               , BAD_GATEWAY ;
RemoteBadRequest                               , InvalidRequest       , BAD_REQUEST ;
RemoteCouldNotSendRequest                      , System               , BAD_GATEWAY ;
RemoteInvalidApiKey                            , Auth                 , FORBIDDEN ;
RemoteRemoteError                              , System               , BAD_GATEWAY ;
RemoteTimeout                                  , System               , BAD_GATEWAY ;
TooManySearchRequests                          , System               , SERVICE_UNAVAILABLE ;
TaskNotFound                                   , InvalidRequest       , NOT_FOUND ;
TaskFileNotFound                               , InvalidRequest       , NOT_FOUND ;
BatchNotFound                                  , InvalidRequest       , NOT_FOUND ;
TooManyOpenFiles                               , System               , UNPROCESSABLE_ENTITY ;
TooManyVectors                                 , InvalidRequest       , BAD_REQUEST ;
UnexpectedNetworkPreviousRemotes               , InvalidRequest       , BAD_REQUEST ;
NetworkVersionTooOld                           , InvalidRequest       , BAD_REQUEST ;
UnprocessedNetworkTask                         , InvalidRequest       , BAD_REQUEST ;
UnretrievableDocument                          , Internal             , BAD_REQUEST ;
UnretrievableErrorCode                         , InvalidRequest       , BAD_REQUEST ;
UnsupportedMediaType                           , InvalidRequest       , UNSUPPORTED_MEDIA_TYPE ;
InvalidS3SnapshotRequest                       , Internal             , BAD_REQUEST ;
InvalidS3SnapshotParameters                    , Internal             , BAD_REQUEST ;
S3SnapshotServerError                          , Internal             , BAD_GATEWAY ;

// Experimental features
VectorEmbeddingError                           , InvalidRequest       , BAD_REQUEST ;
NotFoundSimilarId                              , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentEditionContext                  , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentEditionFunctionFilter           , InvalidRequest       , BAD_REQUEST ;
EditDocumentsByFunctionError                   , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsIndexChat                       , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsVectorStore                     , InvalidRequest       , BAD_REQUEST ;
// Export
InvalidExportUrl                               , InvalidRequest       , BAD_REQUEST ;
InvalidExportApiKey                            , InvalidRequest       , BAD_REQUEST ;
InvalidExportPayloadSize                       , InvalidRequest       , BAD_REQUEST ;
InvalidExportIndexesPatterns                   , InvalidRequest       , BAD_REQUEST ;
InvalidExportIndexFilter                       , InvalidRequest       , BAD_REQUEST ;
InvalidExportIndexOverrideSettings             , InvalidRequest       , BAD_REQUEST ;
// Experimental features - Chat Completions
UnimplementedExternalFunctionCalling           , InvalidRequest       , NOT_IMPLEMENTED ;
UnimplementedNonStreamingChatCompletions       , InvalidRequest       , NOT_IMPLEMENTED ;
UnimplementedMultiChoiceChatCompletions        , InvalidRequest       , NOT_IMPLEMENTED ;
ChatNotFound                                   , InvalidRequest       , NOT_FOUND   ;
InvalidChatSettingDocumentTemplate             , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionOrgId                     , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionProjectId                 , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionApiVersion                , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionDeploymentId              , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionSource                    , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionBaseApi                   , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionApiKey                    , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionPrompts                   , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionSystemPrompt              , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionSearchDescriptionPrompt   , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionSearchQueryParamPrompt    , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionSearchFilterParamPrompt   , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionSearchIndexUidParamPrompt , InvalidRequest       , BAD_REQUEST ;
InvalidChatCompletionPreQueryPrompt            , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilter                       , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterStartsWith             , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterContains               , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterRegex                  , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterGlob                   , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterDisplayed              , InvalidRequest       , BAD_REQUEST ;
RequiresEnterpriseEdition                      , InvalidRequest       , UNAVAILABLE_FOR_LEGAL_REASONS ;
// Webhooks
InvalidWebhooks                                , InvalidRequest       , BAD_REQUEST ;
InvalidWebhookUrl                              , InvalidRequest       , BAD_REQUEST ;
InvalidWebhookHeaders                          , InvalidRequest       , BAD_REQUEST ;
ImmutableWebhook                               , InvalidRequest       , BAD_REQUEST ;
InvalidWebhookUuid                             , InvalidRequest       , BAD_REQUEST ;
WebhookNotFound                                , InvalidRequest       , NOT_FOUND ;
ImmutableWebhookUuid                           , InvalidRequest       , BAD_REQUEST ;
ImmutableWebhookIsEditable                     , InvalidRequest       , BAD_REQUEST
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
                    | UserError::EnvAlreadyOpened
                    | UserError::DocumentLimitReached
                    | UserError::UnknownInternalDocumentId { .. } => Code::Internal,
                    UserError::InvalidStoreFile => Code::InvalidStoreFile,
                    UserError::NoSpaceLeftOnDevice => Code::NoSpaceLeftOnDevice,
                    UserError::MaxDatabaseSizeReached => Code::DatabaseSizeLimitReached,
                    UserError::AttributeLimitReached => Code::MaxFieldsLimitExceeded,
                    UserError::InvalidFilter(_) => Code::InvalidSearchFilter,
                    UserError::InvalidFilterExpression(..) => Code::InvalidSearchFilter,
                    UserError::FilterOperatorNotAllowed { .. } => Code::InvalidSearchFilter,
                    UserError::MissingDocumentId { .. } => Code::MissingDocumentId,
                    UserError::InvalidDocumentId { .. } | UserError::TooManyDocumentIds { .. } => {
                        Code::InvalidDocumentId
                    }
                    UserError::MissingDocumentField(_) => Code::InvalidDocumentFields,
                    UserError::InvalidFieldForSource { .. }
                    | UserError::MissingFieldForSource { .. }
                    | UserError::InvalidOpenAiModel { .. }
                    | UserError::InvalidOpenAiModelDimensions { .. }
                    | UserError::InvalidOpenAiModelDimensionsMax { .. }
                    | UserError::InvalidSettingsDimensions { .. }
                    | UserError::InvalidUrl { .. }
                    | UserError::InvalidSettingsDocumentTemplateMaxBytes { .. }
                    | UserError::InvalidChatSettingsDocumentTemplateMaxBytes
                    | UserError::InvalidPrompt(_)
                    | UserError::InvalidDisableBinaryQuantization { .. }
                    | UserError::InvalidSourceForNested { .. }
                    | UserError::MissingSourceForNested { .. }
                    | UserError::InvalidSettingsEmbedder { .. } => Code::InvalidSettingsEmbedders,
                    UserError::TooManyEmbedders(_) => Code::InvalidSettingsEmbedders,
                    UserError::TooManyFragments(_) => Code::InvalidSettingsEmbedders,
                    UserError::InvalidPromptForEmbeddings(..) => Code::InvalidSettingsEmbedders,
                    UserError::NoPrimaryKeyCandidateFound => Code::IndexPrimaryKeyNoCandidateFound,
                    UserError::MultiplePrimaryKeyCandidatesFound { .. } => {
                        Code::IndexPrimaryKeyMultipleCandidatesFound
                    }
                    UserError::PrimaryKeyCannotBeChanged(_) => Code::IndexPrimaryKeyAlreadyExists,
                    UserError::InvalidDistinctAttribute { .. } => Code::InvalidSearchDistinct,
                    UserError::SortRankingRuleMissing => Code::InvalidSearchSort,
                    UserError::InvalidFacetsDistribution { .. } => Code::InvalidSearchFacets,
                    UserError::InvalidSearchSortableAttribute { .. } => Code::InvalidSearchSort,
                    UserError::InvalidDocumentSortableAttribute { .. } => Code::InvalidDocumentSort,
                    UserError::InvalidSearchableAttribute { .. } => {
                        Code::InvalidSearchAttributesToSearchOn
                    }
                    UserError::InvalidFacetSearchFacetName { .. } => {
                        Code::InvalidFacetSearchFacetName
                    }
                    UserError::CriterionError(_) => Code::InvalidSettingsRankingRules,
                    UserError::InvalidGeoField { .. } | UserError::GeoJsonError(_) => {
                        Code::InvalidDocumentGeoField
                    }
                    UserError::InvalidVectorDimensions { .. }
                    | UserError::InvalidIndexingVectorDimensions { .. } => {
                        Code::InvalidVectorDimensions
                    }
                    UserError::InvalidVectorsMapType { .. }
                    | UserError::InvalidVectorsEmbedderConf { .. } => Code::InvalidVectorsType,
                    UserError::TooManyVectors(_, _) => Code::TooManyVectors,
                    UserError::SortError { search: true, .. } => Code::InvalidSearchSort,
                    UserError::SortError { search: false, .. } => Code::InvalidDocumentSort,
                    UserError::InvalidMinTypoWordLenSetting(_, _) => {
                        Code::InvalidSettingsTypoTolerance
                    }
                    UserError::InvalidSearchEmbedder(_) => Code::InvalidSearchEmbedder,
                    UserError::InvalidSimilarEmbedder(_) => Code::InvalidSimilarEmbedder,
                    UserError::VectorEmbeddingError(_) | UserError::DocumentEmbeddingError(_) => {
                        Code::VectorEmbeddingError
                    }
                    UserError::DocumentEditionCannotModifyPrimaryKey
                    | UserError::DocumentEditionDocumentMustBeObject
                    | UserError::DocumentEditionRuntimeError(_)
                    | UserError::DocumentEditionCompilationError(_) => {
                        Code::EditDocumentsByFunctionError
                    }
                    UserError::CelluliteError(err) => match err {
                        cellulite::Error::BuildCanceled
                        | cellulite::Error::VersionMismatchOnBuild(_)
                        | cellulite::Error::DatabaseDoesntExists
                        | cellulite::Error::Heed(_)
                        | cellulite::Error::InvalidGeometry(_)
                        | cellulite::Error::InternalDocIdMissing(_, _)
                        | cellulite::Error::CannotConvertLineToCell(_, _, _) => Code::Internal,
                        cellulite::Error::InvalidGeoJson(_) => Code::InvalidDocumentGeojsonField,
                    },
                    UserError::MalformedGeojson(_) => Code::InvalidDocumentGeojsonField,
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
            Self::CouldNotParseFileNameAsUtf8 | Self::UuidError(_) => Code::Internal,
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
            | HeedError::Encoding(_)
            | HeedError::Decoding(_)
            | HeedError::EnvAlreadyOpened => Code::Internal,
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

/// Deserialization when `deserr` cannot parse an API key date.
#[derive(Debug)]
pub struct ParseOffsetDateTimeError(pub String);
impl fmt::Display for ParseOffsetDateTimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "`{original}` is not a valid date. It should follow the RFC 3339 format to represents a date or datetime in the future or specified as a null value. e.g. 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.", original = self.0)
    }
}

/// Deserialization when `deserr` cannot parse a task date.
#[derive(Debug)]
pub struct InvalidTaskDateError(pub String);
impl std::fmt::Display for InvalidTaskDateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "`{}` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.", self.0)
    }
}

/// Deserialization error when `deserr` cannot parse a String
/// into a bool.
#[derive(Debug)]
pub struct DeserrParseBoolError(pub String);
impl fmt::Display for DeserrParseBoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "could not parse `{}` as a boolean, expected either `true` or `false`", self.0)
    }
}

/// Deserialization error when `deserr` cannot parse a String
/// into an integer.
#[derive(Debug)]
pub struct DeserrParseIntError(pub String);
impl fmt::Display for DeserrParseIntError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "could not parse `{}` as a positive integer", self.0)
    }
}

impl fmt::Display for deserr_codes::InvalidSearchSemanticRatio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the value of `semanticRatio` is invalid, expected a float between `0.0` and `1.0`."
        )
    }
}

impl fmt::Display for deserr_codes::InvalidMultiSearchWeight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the value of `weight` is invalid, expected a positive float (>= 0.0).")
    }
}

impl fmt::Display for deserr_codes::InvalidSimilarId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the value of `id` is invalid. \
            A document identifier can be of type integer or string, \
            only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_), \
            and can not be more than 511 bytes."
        )
    }
}

impl fmt::Display for deserr_codes::InvalidSearchRankingScoreThreshold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the value of `rankingScoreThreshold` is invalid, expected a float between `0.0` and `1.0`."
        )
    }
}

impl fmt::Display for deserr_codes::InvalidSimilarRankingScoreThreshold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        deserr_codes::InvalidSearchRankingScoreThreshold.fmt(f)
    }
}

impl fmt::Display for deserr_codes::InvalidNetworkUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the value of `url` is invalid, expected a string.")
    }
}

impl fmt::Display for deserr_codes::InvalidNetworkSearchApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the value of `searchApiKey` is invalid, expected a string.")
    }
}

impl fmt::Display for deserr_codes::InvalidSearchPersonalize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the value of `personalize` is invalid, expected a JSON object with `userContext` string.")
    }
}

impl fmt::Display for deserr_codes::InvalidSearchPersonalizeUserContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the value of `userContext` is invalid, expected a string.")
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
