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
    pub error_code: String,
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
    ($($code_ident:ident, $err_type:ident, $status:ident, $description:literal);*) => {
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

            pub fn description(&self) -> &'static str {
                match self {
                    $(
                        Code::$code_ident => $description
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
ApiKeyAlreadyExists                            , InvalidRequest       , CONFLICT,
r#"A key with this [`uid`](/reference/api/keys/get-api-key#response-uid) already exists."# ;
ApiKeyNotFound                                 , InvalidRequest       , NOT_FOUND,
r#"The requested API key could not be found."# ;
IndexScopedApiKeyWithGlobalAction              , InvalidRequest       , BAD_REQUEST;
BadParameter                                   , InvalidRequest       , BAD_REQUEST;
BadRequest                                     , InvalidRequest       , BAD_REQUEST,
r#"The request is invalid, check the error message for more information."# ;
DatabaseSizeLimitReached                       , Internal             , INTERNAL_SERVER_ERROR,
"The requested database has reached its maximum size.";
DocumentNotFound                               , InvalidRequest       , NOT_FOUND,
"The requested document cannot be retrieved because it does not exist.";
DumpAlreadyProcessing                          , InvalidRequest       , CONFLICT;
DumpNotFound                                   , InvalidRequest       , NOT_FOUND;
DumpProcessFailed                              , Internal             , INTERNAL_SERVER_ERROR,
"An error occurred during the dump creation process. The task was aborted.";
DuplicateIndexFound                            , InvalidRequest       , BAD_REQUEST;
ImmutableApiKeyActions                         , InvalidRequest       , BAD_REQUEST,
"The [`actions`](/reference/api/keys/list-api-keys) field of an API key cannot be modified.";
ImmutableApiKeyCreatedAt                       , InvalidRequest       , BAD_REQUEST,
"The [`createdAt`](/reference/api/keys/get-api-key#response-created-at) field of an API key cannot be modified.";
ImmutableApiKeyExpiresAt                       , InvalidRequest       , BAD_REQUEST,
"The [`expiresAt`](/reference/api/keys/get-api-key#response-expiresat) field of an API key cannot be modified.";
ImmutableApiKeyIndexes                         , InvalidRequest       , BAD_REQUEST,
"The [`indexes`](/reference/api/keys/get-api-key#response-indexes) field of an API key cannot be modified.";
ImmutableApiKeyKey                             , InvalidRequest       , BAD_REQUEST,
"The [`key`](/reference/api/keys/get-api-key#response-key) field of an API key cannot be modified.";
ImmutableApiKeyUid                             , InvalidRequest       , BAD_REQUEST,
"The [`uid`](/reference/api/keys/get-api-key#response-uid) field of an API key cannot be modified.";
ImmutableApiKeyUpdatedAt                       , InvalidRequest       , BAD_REQUEST,
"The [`updatedAt`](/reference/api/keys/get-api-key#response-updated-at) field of an API key cannot be modified.";
ImmutableIndexCreatedAt                        , InvalidRequest       , BAD_REQUEST;
ImmutableIndexUpdatedAt                        , InvalidRequest       , BAD_REQUEST,
"The [`updatedAt`](/reference/api/indexes/get-index) field of an index cannot be modified.";
ImportTaskAlreadyReceived                      , InvalidRequest       , PRECONDITION_FAILED;
ImportTaskUnknownRemote                        , InvalidRequest       , PRECONDITION_FAILED;
ReceiveImportFinishedUnknownRemote             , InvalidRequest       , PRECONDITION_FAILED;
ImportTaskWithoutNetworkTask                   , InvalidRequest       , SERVICE_UNAVAILABLE;
IndexAlreadyExists                             , InvalidRequest       , CONFLICT,
"An index with this [`uid`](/reference/api/indexes/get-index) already exists, check out our guide on [index creation](/resources/internals/indexes).";
IndexCreationFailed                            , Internal             , INTERNAL_SERVER_ERROR;
IndexNotFound                                  , InvalidRequest       , NOT_FOUND,
"An index with this `uid` was not found, check out our guide on [index creation](/resources/internals/indexes).";
IndexPrimaryKeyAlreadyExists                   , InvalidRequest       , BAD_REQUEST,
"The requested index already has a primary key that [cannot be changed](/resources/internals/primary_key#changing-your-primary-key-with-the-update-index-endpoint)." ;
IndexPrimaryKeyMultipleCandidatesFound         , InvalidRequest       , BAD_REQUEST,
"[Primary key inference](/resources/internals/primary_key#meilisearch-guesses-your-primary-key) failed because the received documents contain multiple fields ending with `id`. Use the [update index endpoint](/reference/api/indexes/update-index) to manually set a primary key.";
IndexPrimaryKeyNoCandidateFound                , InvalidRequest       , BAD_REQUEST,
"[Primary key inference](/resources/internals/primary_key#meilisearch-guesses-your-primary-key) failed as the received documents do not contain any fields ending with `id`. [Manually designate the primary key](/resources/internals/primary_key#setting-the-primary-key), or add some field ending with `id` to your documents." ;
Internal                                       , Internal             , INTERNAL_SERVER_ERROR,
"Meilisearch experienced an internal error. Check the error message, and [open an issue](https://github.com/meilisearch/meilisearch/issues/new?assignees=&labels=&template=bug_report&title=) if necessary." ;
InvalidApiKey                                  , Auth                 , FORBIDDEN,
"The requested resources are protected with an API key. The provided API key is invalid. Read more about it in our [security tutorial](/resources/self_hosting/security/basic_security)." ;
InvalidApiKeyActions                           , InvalidRequest       , BAD_REQUEST,
"The [`actions`](/reference/api/keys/list-api-keys) field for the provided API key resource is invalid. It should be an array of strings representing action names." ;
InvalidApiKeyDescription                       , InvalidRequest       , BAD_REQUEST,
"The [`description`](/reference/api/keys/get-api-key#response-description) field for the provided API key resource is invalid. It should either be a string or set to `null`." ;
InvalidApiKeyExpiresAt                         , InvalidRequest       , BAD_REQUEST,
"The [`expiresAt`](/reference/api/keys/get-api-key#response-expiresat) field for the provided API key resource is invalid. It should either show a future date or datetime in the [RFC 3339](https://www.ietf.org/rfc/rfc3339.txt) format or be set to `null`." ;
InvalidApiKeyIndexes                           , InvalidRequest       , BAD_REQUEST,
"The [`indexes`](/reference/api/keys/get-api-key#response-indexes) field for the provided API key resource is invalid. It should be an array of strings representing index names." ;
InvalidApiKeyLimit                             , InvalidRequest       , BAD_REQUEST,
"The [`limit`](/reference/api/keys/list-api-keys) parameter is invalid. It should be an integer." ;
InvalidApiKeyName                              , InvalidRequest       , BAD_REQUEST,
"The given [`name`](/reference/api/keys/get-api-key#response-name) is invalid. It should either be a string or set to `null`." ;
InvalidApiKeyOffset                            , InvalidRequest       , BAD_REQUEST,
"The [`offset`](/reference/api/keys/list-api-keys) parameter is invalid. It should be an integer." ;
InvalidApiKeyUid                               , InvalidRequest       , BAD_REQUEST,
"The given [`uid`](/reference/api/keys/get-api-key#response-uid) is invalid. The `uid` must follow the [uuid v4](https://www.sohamkamani.com/uuid-versions-explained) format." ;
InvalidContentType                             , InvalidRequest       , UNSUPPORTED_MEDIA_TYPE,
r#"The [Content-Type header](/reference/api/headers) is not supported by Meilisearch.

- For document additions, Meilisearch supports JSON, CSV and NDJSON.
- For other routes, Meilisearch expects JSON content.
"# ;
InvalidDocumentCsvDelimiter                    , InvalidRequest       , BAD_REQUEST,
"The [`csvDelimiter`](/reference/api/documents/add-or-replace-documents) parameter is invalid. It should either be a string or [a single ASCII character](https://www.rfc-editor.org/rfc/rfc20)." ;
InvalidDocumentFields                          , InvalidRequest       , BAD_REQUEST,
"The [`fields`](/reference/api/documents/list-documents-with-get) parameter is invalid. It should be a string." ;
InvalidDocumentRetrieveVectors                 , InvalidRequest       , BAD_REQUEST ;
MissingDocumentFilter                          , InvalidRequest       , BAD_REQUEST ;
MissingDocumentEditionFunction                 , InvalidRequest       , BAD_REQUEST ;
InconsistentDocumentChangeHeaders              , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentFilter                          , InvalidRequest       , BAD_REQUEST,
r#"
This error occurs if:

- The [`filter`](/reference/api/documents/list-documents-with-get) parameter is invalid
  - It should be a string, array of strings, or array of array of strings for the [get documents with POST endpoint](/reference/api/documents/list-documents-with-post)
  - It should be a string for the [get documents with GET endpoint](/reference/api/documents/list-documents-with-get)
- The attribute used for filtering is not defined in the [`filterableAttributes` list](/reference/api/settings/get-filterableattributes)
- The [filter expression](/capabilities/filtering_sorting_faceting/advanced/filter_expression_syntax) has a missing or invalid operator. [Read more about our supported operators](/capabilities/filtering_sorting_faceting/advanced/filter_expression_syntax)
"# ;
InvalidDocumentSort                            , InvalidRequest       , BAD_REQUEST,
r#"
This error occurs if:

- The syntax for the [`sort`](/reference/api/documents/list-documents-with-post) parameter is invalid
- The attribute used for sorting is not defined in the [`sortableAttributes`](/reference/api/settings/get-sortableattributes) list or the `sort` ranking rule is missing from the settings
- A reserved keyword like `_geo`, `_geoDistance`, `_geoRadius`, or `_geoBoundingBox` is used as a filter
"# ;
InvalidDocumentUseNetwork                      , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentRetrieve                     , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentGeoField                        , InvalidRequest       , BAD_REQUEST,
"The provided `_geo` field of one or more documents is invalid. Meilisearch expects `_geo` to be an object with two fields, `lat` and `lng`, each containing geographic coordinates expressed as a string or floating point number. Read more about `_geo` and how to troubleshoot it in [our dedicated guide](/capabilities/geo_search/getting_started)." ;
InvalidDocumentGeojsonField                    , InvalidRequest       , BAD_REQUEST,
"The `geojson` field in one or more documents is invalid or doesn't match the [GeoJSON specification](https://datatracker.ietf.org/doc/html/rfc7946)." ;
InvalidHeaderValue                             , InvalidRequest       , BAD_REQUEST ;
InvalidVectorDimensions                        , InvalidRequest       , BAD_REQUEST ;
InvalidVectorsType                             , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentId                              , InvalidRequest       , BAD_REQUEST,
"The provided [document identifier](/resources/internals/primary_key#document-id) does not meet the format requirements. A document identifier must be of type integer or string, composed only of alphanumeric characters (a-z A-Z 0-9), hyphens (-), and underscores (_)." ;
InvalidDocumentIds                             , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentLimit                           , InvalidRequest       , BAD_REQUEST,
"The [`limit`](/reference/api/documents/list-documents-with-get) parameter is invalid. It should be an integer." ;
InvalidDocumentOffset                          , InvalidRequest       , BAD_REQUEST,
"The [`offset`](/reference/api/documents/list-documents-with-get) parameter is invalid. It should be an integer." ;
InvalidSearchEmbedder                          , InvalidRequest       , BAD_REQUEST,
"[`embedder`](/reference/api/search/search-with-post#body-hybrid) is invalid. It should be a string corresponding to the name of a configured embedder." ;
InvalidSimilarEmbedder                         , InvalidRequest       , BAD_REQUEST,
"[`embedder`](/reference/api/similar-documents/get-similar-documents-with-post) is invalid. It should be a string corresponding to the name of a configured embedder." ;
InvalidSearchHybridQuery                       , InvalidRequest       , BAD_REQUEST,
"The [`hybrid`](/reference/api/search/search-with-post#body-hybrid) parameter is neither `null` nor an object, or it is an object with unknown keys." ;
InvalidIndexLimit                              , InvalidRequest       , BAD_REQUEST,
"The [`limit`](/reference/api/indexes/list-all-indexes) parameter is invalid. It should be an integer." ;
InvalidIndexOffset                             , InvalidRequest       , BAD_REQUEST,
"The [`offset`](/reference/api/indexes/list-all-indexes) parameter is invalid. It should be an integer." ;
InvalidIndexPrimaryKey                         , InvalidRequest       , BAD_REQUEST,
"The [`primaryKey`](/reference/api/indexes/swap-indexes) field is invalid. It should either be a string or set to `null`." ;
InvalidIndexCustomMetadata                     , InvalidRequest       , BAD_REQUEST ;
InvalidSkipCreation                            , InvalidRequest       , BAD_REQUEST ;
InvalidIndexUid                                , InvalidRequest       , BAD_REQUEST,
"There is an error in the provided index format, check out our guide on [index creation](/resources/internals/indexes)." ;
InvalidMultiSearchFacets                       , InvalidRequest       , BAD_REQUEST,
"`federation.facetsByIndex.<INDEX_NAME>` contains a value that is not in the filterable attributes list." ;
InvalidMultiSearchFacetsByIndex                , InvalidRequest       , BAD_REQUEST,
"`facetsByIndex` is not an object or contains unknown fields." ;
InvalidMultiSearchFacetOrder                   , InvalidRequest       , BAD_REQUEST,
"Two or more indexes have a different `faceting.sortFacetValuesBy` for the same requested facet." ;
InvalidMultiSearchQueryPersonalization         , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchQueryShowPerformanceDetails  , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchFederation                   , InvalidRequest       , BAD_REQUEST,
"The [`federation`](/reference/api/multi-search/perform-a-multi-search#body-federation-one-of-1) parameter is invalid. It should either be an object or set to `null`." ;
InvalidMultiSearchFederationOptions            , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchMaxValuesPerFacet            , InvalidRequest       , BAD_REQUEST,
"`federation.mergeFacets.maxValuesPerFacet` is not a positive integer." ;
InvalidMultiSearchMergeFacets                  , InvalidRequest       , BAD_REQUEST,
"`federation.mergeFacets` is not an object or contains unexpected fields." ;
InvalidMultiSearchQueryFacets                  , InvalidRequest       , BAD_REQUEST,
"A query in the queries array contains `facets` when federation is present and non-`null`." ;
InvalidMultiSearchDistinct                     , InvalidRequest       , BAD_REQUEST ;
InvalidMultiSearchQueryPagination              , InvalidRequest       , BAD_REQUEST,
"A multi-search query contains `page`, `hitsPerPage`, `limit` or `offset`, but the top-level federation object is not `null`." ;
InvalidMultiSearchQueryRankingRules            , InvalidRequest       , BAD_REQUEST,
"Two or more queries in a multi-search request have incompatible results." ;
InvalidMultiSearchQueryPosition                , InvalidRequest       , BAD_REQUEST,
"`federationOptions.queryPosition` is not a positive integer." ;
InvalidMultiSearchRemote                       , InvalidRequest       , BAD_REQUEST,
"`federationOptions.remote` is not `network.self` and is not a key in `network.remotes`." ;
InvalidMultiSearchWeight                       , InvalidRequest       , BAD_REQUEST,
"A multi-search query contains a negative value for `federated.weight`." ;
InvalidNetworkLeader                           , InvalidRequest       , BAD_REQUEST ;
InvalidNetworkRemotes                          , InvalidRequest       , BAD_REQUEST,
"The [network object](/reference/api/network/get-network) contains a `remotes` that is not an object or `null`." ;
InvalidNetworkShards                           , InvalidRequest       , BAD_REQUEST ;
InvalidNetworkSelf                             , InvalidRequest       , BAD_REQUEST,
"The [network object](/reference/api/network/get-network) contains a `self` that is not a string or `null`." ;
InvalidNetworkSearchApiKey                     , InvalidRequest       , BAD_REQUEST,
"One of the remotes in the [network object](/reference/api/network/get-network) contains a `searchApiKey` that is not a string or `null`." ;
InvalidNetworkWriteApiKey                      , InvalidRequest       , BAD_REQUEST,
"One of the remotes in the [network object](/reference/api/network/get-network) contains a `writeApiKey` that is not a string or `null`." ;
InvalidNetworkUrl                              , InvalidRequest       , BAD_REQUEST,
"One of the remotes in the [network object](/reference/api/network/get-network) contains a `url` that is not a string." ;
InvalidSearchAttributesToSearchOn              , InvalidRequest       , BAD_REQUEST,
"The value passed to [`attributesToSearchOn`](/reference/api/search/search-with-post#body-attributes-to-search-on) is invalid. `attributesToSearchOn` accepts an array of strings indicating document attributes. Attributes given to `attributesToSearchOn` must be present in the [`searchableAttributes` list](/capabilities/full_text_search/how_to/configure_displayed_attributes#the-searchableattributes-list)." ;
InvalidSearchAttributesToCrop                  , InvalidRequest       , BAD_REQUEST,
"The [`attributesToCrop`](/reference/api/search/search-with-post#body-attributes-to-crop) parameter is invalid. It should be an array of strings, a string, or set to `null`." ;
InvalidSearchAttributesToHighlight             , InvalidRequest       , BAD_REQUEST,
"The [`attributesToHighlight`](/reference/api/search/search-with-post#body-attributes-to-highlight) parameter is invalid. It should be an array of strings, a string, or set to `null`." ;
InvalidSimilarAttributesToRetrieve             , InvalidRequest       , BAD_REQUEST,
"[`attributesToRetrieve`](/reference/api/search/search-with-post#body-attributes-to-retrieve) is invalid. It should be an array of strings, a string, or set to null.";
InvalidSimilarRetrieveVectors                  , InvalidRequest       , BAD_REQUEST ;
InvalidSearchAttributesToRetrieve              , InvalidRequest       , BAD_REQUEST,
"The [`attributesToRetrieve`](/reference/api/search/search-with-post#body-attributes-to-retrieve) parameter is invalid. It should be an array of strings, a string, or set to `null`." ;
InvalidSearchRankingScoreThreshold             , InvalidRequest       , BAD_REQUEST,
"The [`rankingScoreThreshold`](/reference/api/search/search-with-post#body-show-ranking-score-threshold) in a search or multi-search request is not a number between `0.0` and `1.0`." ;
InvalidSimilarRankingScoreThreshold            , InvalidRequest       , BAD_REQUEST,
"The [`rankingScoreThreshold`](/reference/api/search/search-with-post#body-show-ranking-score-threshold) in a similar documents request is not a number between `0.0` and `1.0`." ;
InvalidSearchRetrieveVectors                   , InvalidRequest       , BAD_REQUEST ;
InvalidSearchCropLength                        , InvalidRequest       , BAD_REQUEST,
"The [`cropLength`](/reference/api/search/search-with-post#body-crop-length) parameter is invalid. It should be an integer." ;
InvalidSearchCropMarker                        , InvalidRequest       , BAD_REQUEST,
"The [`cropMarker`](/reference/api/search/search-with-post#body-crop-marker) parameter is invalid. It should be a string or set to `null`." ;
InvalidSearchFacets                            , InvalidRequest       , BAD_REQUEST,
r#"This error occurs if:

- The [`facets`](/reference/api/search/search-with-post#body-facets) parameter is invalid. It should be an array of strings, a string, or set to `null`
- The attribute used for faceting is not defined in the [`filterableAttributes` list](/reference/api/settings/get-filterableattributes)
"# ;
InvalidSearchSemanticRatio                     , InvalidRequest       , BAD_REQUEST ;
InvalidSearchLocales                           , InvalidRequest       , BAD_REQUEST,
"The [`locales`](/reference/api/search/search-with-post#body-locales) parameter is invalid." ;
InvalidFacetSearchExhaustiveFacetCount         , InvalidRequest       , BAD_REQUEST ;
InvalidFacetSearchFacetName                    , InvalidRequest       , BAD_REQUEST,
"The attribute used for the `facetName` field is either not a string or not defined in the [`filterableAttributes` list](/reference/api/settings/get-filterableattributes)." ;
InvalidSimilarId                               , InvalidRequest       , BAD_REQUEST,
"The provided target document identifier is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_)." ;
InvalidSearchFilter                            , InvalidRequest       , BAD_REQUEST,
r#"This error occurs if:

- The syntax for the [`filter`](/reference/api/search/search-with-post#body-filter) parameter is invalid
- The attribute used for filtering is not defined in the [`filterableAttributes` list](/reference/api/settings/get-filterableattributes)
- A reserved keyword like `_geo`, `_geoDistance`, or `_geoPoint` is used as a filter
"# ;
InvalidSimilarFilter                           , InvalidRequest       , BAD_REQUEST,
r#"[`filter`](/reference/api/search/search-with-post#body-filter) is invalid or contains a filter expression with a missing or invalid operator. Filter expressions must be a string, array of strings, or array of array of strings for the POST endpoint. It must be a string for the GET endpoint.

Meilisearch also throws this error if the attribute used for filtering is not defined in the `filterableAttributes` list.
"# ;
InvalidSearchHighlightPostTag                  , InvalidRequest       , BAD_REQUEST,
"The [`highlightPostTag`](/reference/api/search/search-with-post#body-highlight-pre-tag) parameter is invalid. It should be a string." ;
InvalidSearchHighlightPreTag                   , InvalidRequest       , BAD_REQUEST,
"The [`highlightPreTag`](/reference/api/search/search-with-post#body-highlight-pre-tag) parameter is invalid. It should be a string." ;
InvalidSearchHitsPerPage                       , InvalidRequest       , BAD_REQUEST,
"The [`hitsPerPage`](/reference/api/search/search-with-post#body-hits-per-page) parameter is invalid. It should be an integer." ;
InvalidSimilarLimit                            , InvalidRequest       , BAD_REQUEST,
"[`limit`](/reference/api/search/search-with-post#body-limit) is invalid. It should be an integer." ;
InvalidSearchLimit                             , InvalidRequest       , BAD_REQUEST,
"The [`limit`](/reference/api/search/search-with-post#body-limit) parameter is invalid. It should be an integer." ;
InvalidSearchMatchingStrategy                  , InvalidRequest       , BAD_REQUEST,
"The [`matchingStrategy`](/reference/api/search/search-with-post#body-matching-strategy) parameter is invalid. It should either be set to `last` or `all`." ;
InvalidSimilarOffset                           , InvalidRequest       , BAD_REQUEST,
"[`offset`](/reference/api/search/search-with-post#body-offset) is invalid. It should be an integer." ;
InvalidSearchOffset                            , InvalidRequest       , BAD_REQUEST,
"The [`offset`](/reference/api/search/search-with-post#body-offset) parameter is invalid. It should be an integer." ;
InvalidSearchPage                              , InvalidRequest       , BAD_REQUEST,
"The [`page`](/reference/api/search/search-with-post#body-page) parameter is invalid. It should be an integer." ;
InvalidSearchQ                                 , InvalidRequest       , BAD_REQUEST,
"The [`q`](/reference/api/search/search-with-post#body-q) parameter is invalid. It should be a string or set to `null`" ;
InvalidFacetSearchQuery                        , InvalidRequest       , BAD_REQUEST,
"The provided value for `facetQuery` is invalid. It should either be a string or `null`." ;
InvalidFacetSearchName                         , InvalidRequest       , BAD_REQUEST ;
FacetSearchDisabled                            , InvalidRequest       , BAD_REQUEST,
r#"The [`/facet-search`](/reference/api/facet-search/search-for-facet-values) route has been queried while [the `facetSearch` index setting](/reference/api/settings/get-facetsearch) is set to `false`."#;
InvalidSearchVector                            , InvalidRequest       , BAD_REQUEST ;
InvalidSearchMedia                             , InvalidRequest       , BAD_REQUEST,
"The value passed to [`media`](/reference/api/search/search-with-post#body-media) is not a valid JSON object." ;
InvalidSearchShowMatchesPosition               , InvalidRequest       , BAD_REQUEST,
"The [`showMatchesPosition`](/reference/api/search/search-with-post#body-show-matches-position) parameter is invalid. It should either be a boolean or set to `null`." ;
InvalidSearchShowRankingScore                  , InvalidRequest       , BAD_REQUEST,
"[`ranking_score`](/reference/api/search/search-with-post#body-show-ranking-score) is invalid. It should be a boolean." ;
InvalidSimilarShowRankingScore                 , InvalidRequest       , BAD_REQUEST,
"[`ranking_score`](/reference/api/search/search-with-post#body-show-ranking-score) is invalid. It should be a boolean." ;
InvalidSearchShowRankingScoreDetails           , InvalidRequest       , BAD_REQUEST,
"[`ranking_score_details`](/reference/api/search/search-with-post#body-show-ranking-score-details) is invalid. It should be a boolean." ;
InvalidSearchShowPerformanceDetails            , InvalidRequest       , BAD_REQUEST, ;
InvalidSearchUseNetwork                        , InvalidRequest       , BAD_REQUEST ;
InvalidSimilarShowRankingScoreDetails          , InvalidRequest       , BAD_REQUEST,
"[`ranking_score_details`](/reference/api/search/search-with-post#body-show-ranking-score-details) is invalid. It should be a boolean.";
InvalidSimilarShowPerformanceDetails           , InvalidRequest       , BAD_REQUEST ;
InvalidSearchSort                              , InvalidRequest       , BAD_REQUEST,
r#"This error occurs if:

- The syntax for the [`sort`](/reference/api/search/search-with-post#body-sort) parameter is invalid
- The attribute used for sorting is not defined in the [`sortableAttributes`](/reference/api/settings/get-sortableattributes) list or the `sort` ranking rule is missing from the settings
- A reserved keyword like `_geo`, `_geoDistance`, `_geoRadius`, or `_geoBoundingBox` is used as a filter
"# ;
InvalidSearchDistinct                          , InvalidRequest       , BAD_REQUEST ;
InvalidSearchPersonalize                       , InvalidRequest       , BAD_REQUEST ;
InvalidSearchPersonalizeUserContext            , InvalidRequest       , BAD_REQUEST ;
InvalidSearchMediaAndVector                    , InvalidRequest       , BAD_REQUEST,
"The search query contains non-`null` values for both [`media`](/reference/api/search/search-with-post#body-media) and [`vector`](/reference/api/search/search-with-post#body-media). These two parameters are mutually exclusive, since `media` generates vector embeddings via the embedder configured in `hybrid`." ;
InvalidSettingsDisplayedAttributes             , InvalidRequest       , BAD_REQUEST,
"The value of [displayed attributes](/capabilities/full_text_search/how_to/configure_displayed_attributes#displayed-fields) is invalid. It should be an empty array, an array of strings, or set to `null`." ;
InvalidSettingsDistinctAttribute               , InvalidRequest       , BAD_REQUEST,
"The value of [distinct attributes](/capabilities/full_text_search/how_to/configure_distinct_attribute) is invalid. It should be a string or set to `null`." ;
InvalidSettingsProximityPrecision              , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsFacetSearch                     , InvalidRequest       , BAD_REQUEST,
"The [`facetSearch`](/reference/api/settings/get-facetsearch) index setting value is invalid." ;
InvalidSettingsPrefixSearch                    , InvalidRequest       , BAD_REQUEST,
"The [`prefixSearch`](/reference/api/settings/get-prefixsearch) index setting value is invalid." ;
InvalidSettingsFaceting                        , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsFilterableAttributes            , InvalidRequest       , BAD_REQUEST,
"The value of [filterable attributes](/reference/api/settings/get-filterableattributes) is invalid. It should be an empty array, an array of strings, or set to `null`." ;
InvalidSettingsForeignKeys                     , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsPagination                      , InvalidRequest       , BAD_REQUEST,
"The value for the [`maxTotalHits`](/reference/api/settings/update-pagination) field is invalid. It should either be an integer or set to `null`." ;
InvalidSettingsSearchCutoffMs                  , InvalidRequest       , BAD_REQUEST,
"The specified value for [`searchCutoffMs`](/reference/api/settings/update-searchcutoffms) is invalid. It should be an integer indicating the cutoff in milliseconds." ;
InvalidSettingsEmbedders                       , InvalidRequest       , BAD_REQUEST,
"The [`embedders`](/reference/api/settings/get-embedders) index setting value is invalid." ;
InvalidSettingsRankingRules                    , InvalidRequest       , BAD_REQUEST,
r#"This error occurs if:

- The [settings payload](/reference/api/settings/update-all-settings) has an invalid format
- A non-existent ranking rule is specified
- A custom ranking rule is malformed
- A reserved keyword like `_geo`, `_geoDistance`, `_geoRadius`, `_geoBoundingBox`, or `_geoPoint` is used as a custom ranking rule
"# ;
InvalidSettingsSearchableAttributes            , InvalidRequest       , BAD_REQUEST,
"The value of [searchable attributes](/reference/api/settings/get-searchableattributes) is invalid. It should be an empty array, an array of strings or set to `null`." ;
InvalidSettingsSortableAttributes              , InvalidRequest       , BAD_REQUEST,
"The value of [sortable attributes](/reference/api/settings/get-sortableattributes) is invalid. It should be an empty array, an array of strings or set to `null`." ;
InvalidSettingsStopWords                       , InvalidRequest       , BAD_REQUEST,
"The value of [stop words](/reference/api/settings/get-stopwords) is invalid. It should be an empty array, an array of strings or set to `null`." ;
InvalidSettingsNonSeparatorTokens              , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsSeparatorTokens                 , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsDictionary                      , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsSynonyms                        , InvalidRequest       , BAD_REQUEST,
"The value of the [synonyms](/reference/api/settings/get-synonyms) is invalid. It should either be an object or set to `null`." ;
InvalidSettingsTypoTolerance                   , InvalidRequest       , BAD_REQUEST,
r#"This error occurs if:

- The [`enabled`](/reference/api/settings/get-typotolerance) field is invalid. It should either be a boolean or set to `null`
- The [`disableOnAttributes`](/reference/api/settings/get-typotolerance) field is invalid. It should either be an array of strings or set to `null`
- The [`disableOnWords`](/reference/api/settings/get-typotolerance) field is invalid. It should either be an array of strings or set to `null`
- The [`minWordSizeForTypos`](/reference/api/settings/get-typotolerance) field is invalid. It should either be an integer or set to `null`
- The value of either [`oneTypo`](/reference/api/settings/get-typotolerance) or [`twoTypos`](/reference/api/settings/get-typotolerance) is invalid. It should either be an integer or set to `null`
"# ;
InvalidSettingsLocalizedAttributes             , InvalidRequest       , BAD_REQUEST,
"The [`localizedAttributes`](/reference/api/settings/get-localizedattributes) index setting value is invalid." ;
InvalidState                                   , Internal             , INTERNAL_SERVER_ERROR,
"The database is in an invalid state. Deleting the database and re-indexing should solve the problem." ;
InvalidStatsShowInternalDatabaseSizes          , InvalidRequest       , BAD_REQUEST ;
InvalidStatsSizeFormat                         , InvalidRequest       , BAD_REQUEST ;
InvalidStoreFile                               , Internal             , INTERNAL_SERVER_ERROR,
"The `data.ms` folder is in an invalid state. Your `b` file is corrupted or the `data.ms` folder has been replaced by a file." ;
InvalidSwapDuplicateIndexFound                 , InvalidRequest       , BAD_REQUEST,
"The indexes used in the [`indexes`](/reference/api/indexes/swap-indexes) array for a [swap index](/reference/api/indexes/swap-indexes) request have been declared multiple times. You must declare each index only once." ;
InvalidSwapIndexes                             , InvalidRequest       , BAD_REQUEST,
r#"This error happens if:

- The payload doesn't contain exactly two index [`uids`](/reference/api/indexes/swap-indexes) for a swap operation
- The payload contains an invalid index name in the [`indexes`](/reference/api/indexes/swap-indexes) array
"# ;
InvalidSwapRename                              , InvalidRequest       , BAD_REQUEST ;
InvalidTaskAfterEnqueuedAt                     , InvalidRequest       , BAD_REQUEST,
"The [`afterEnqueuedAt`](/reference/api/tasks/list-tasks) query parameter is invalid." ;
InvalidTaskAfterFinishedAt                     , InvalidRequest       , BAD_REQUEST,
"The [`afterFinishedAt`](/reference/api/tasks/list-tasks) query parameter is invalid." ;
InvalidTaskAfterStartedAt                      , InvalidRequest       , BAD_REQUEST,
"The [`afterStartedAt`](/reference/api/tasks/list-tasks) query parameter is invalid." ;
InvalidTaskBeforeEnqueuedAt                    , InvalidRequest       , BAD_REQUEST,
"The [`beforeEnqueuedAt`](/reference/api/tasks/list-tasks) query parameter is invalid." ;
InvalidTaskBeforeFinishedAt                    , InvalidRequest       , BAD_REQUEST,
"The [`beforeFinishedAt`](/reference/api/tasks/list-tasks) query parameter is invalid." ;
InvalidTaskBeforeStartedAt                     , InvalidRequest       , BAD_REQUEST,
"The [`beforeStartedAt`](/reference/api/tasks/list-tasks) query parameter is invalid." ;
InvalidTaskCanceledBy                          , InvalidRequest       , BAD_REQUEST,
"The [`canceledBy`](/reference/api/tasks/list-tasks) query parameter is invalid. It should be an integer. Multiple `uid`s should be separated by commas (`,`)." ;
InvalidTaskFrom                                , InvalidRequest       , BAD_REQUEST ;
InvalidTaskLimit                               , InvalidRequest       , BAD_REQUEST,
"The [`limit`](/reference/api/tasks/list-tasks) parameter is invalid. It must be an integer." ;
InvalidTaskReverse                             , InvalidRequest       , BAD_REQUEST ;
InvalidTaskStatuses                            , InvalidRequest       , BAD_REQUEST,
"The requested task status is invalid. Please use one of the [possible values](/reference/api/tasks/get-task)." ;
InvalidTaskTypes                               , InvalidRequest       , BAD_REQUEST,
"The requested task type is invalid. Please use one of the [possible values](/reference/api/tasks/get-task)." ;
InvalidTaskUids                                , InvalidRequest       , BAD_REQUEST,
"The [`uids`](/reference/api/tasks/list-tasks) query parameter is invalid."  ;
InvalidBatchUids                               , InvalidRequest       , BAD_REQUEST  ;
IoError                                        , System               , UNPROCESSABLE_ENTITY,
"This error generally occurs when the host system has no space left on the device or when the database doesn't have read or write access.";
FeatureNotEnabled                              , InvalidRequest       , BAD_REQUEST,
"You have tried using an [experimental feature](/resources/help/experimental_features_overview) without activating it." ;
MalformedPayload                               , InvalidRequest       , BAD_REQUEST,
"The [Content-Type header](/reference/api/headers) does not match the request body payload format or the format is invalid." ;
MaxFieldsLimitExceeded                         , InvalidRequest       , BAD_REQUEST,
r"The index exceeds the [maximum limit of 65,536 attributes](/resources/help/known_limitations#maximum-number-of-attributes-per-index)." ;
MissingApiKeyActions                           , InvalidRequest       , BAD_REQUEST,
"The [`actions`](/reference/api/keys/list-api-keys) field is missing from payload." ;
MissingApiKeyExpiresAt                         , InvalidRequest       , BAD_REQUEST,
"The [`expiresAt`](/reference/api/keys/get-api-key#response-expiresat) field is missing from payload." ;
MissingApiKeyIndexes                           , InvalidRequest       , BAD_REQUEST,
"The [`indexes`](/reference/api/keys/get-api-key#response-indexes) field is missing from payload." ;
MissingAuthorizationHeader                     , Auth                 , UNAUTHORIZED,
r#"This error happens if:

- The requested resources are protected with an API key that was not provided in the request header. Check our [security tutorial](/resources/self_hosting/security/basic_security) for more information
"# ;
MissingContentType                             , InvalidRequest       , UNSUPPORTED_MEDIA_TYPE,
r#"The payload does not contain a [Content-Type header](/reference/api/headers).

- For document additions, Meilisearch supports JSON, CSV and NDJSON.
- For other routes, Meilisearch expects JSON content.
"#;
MissingDocumentId                              , InvalidRequest       , BAD_REQUEST,
"A document does not contain any value for the required primary key, and is thus invalid. Check documents in the current addition for the invalid ones." ;
MissingFacetSearchFacetName                    , InvalidRequest       , BAD_REQUEST,
"The [`facetName`](/reference/api/facet-search/search-for-facet-values) parameter is required." ;
MissingIndexUid                                , InvalidRequest       , BAD_REQUEST,
"The payload is missing the [`uid`](/reference/api/indexes/get-index) field." ;
MissingMasterKey                               , Auth                 , UNAUTHORIZED,
"You need to set a master key before you can access the `/keys` route. Read more about setting a master key at launch in our [security tutorial](/resources/self_hosting/security/basic_security)." ;
MissingNetworkUrl                              , InvalidRequest       , BAD_REQUEST,
"One of the remotes in the [network object](/reference/api/network/get-network) does not contain the `url` field." ;
MissingPayload                                 , InvalidRequest       , BAD_REQUEST,
"The Content-Type header was specified, but no request body was sent to the server or the request body is empty." ;
MissingSearchHybrid                            , InvalidRequest       , BAD_REQUEST ;
MissingSwapIndexes                             , InvalidRequest       , BAD_REQUEST,
"The index swap payload is missing the [`indexes`](/reference/api/indexes/swap-indexes) object." ;
MissingTaskFilters                             , InvalidRequest       , BAD_REQUEST,
"The [cancel tasks](/reference/api/tasks/cancel-tasks) and [delete tasks](/reference/api/tasks/delete-tasks) endpoints require one of the available query parameters." ;
NetworkVersionMismatch                         , InvalidRequest       , PRECONDITION_FAILED ;
NoSpaceLeftOnDevice                            , System               , UNPROCESSABLE_ENTITY,
r#"
This error occurs if:

- The host system partition reaches its maximum capacity and can no longer accept writes
- The tasks queue reaches its limit and can no longer accept writes. You can delete tasks using the [delete tasks endpoint](/reference/api/tasks/delete-tasks) to continue write operations
- While indexing, the temporary storage partition (for example, `/tmp`) ran out and can no longer accept writes
"#;
NotLeader                                      , InvalidRequest       , BAD_REQUEST ;
PayloadTooLarge                                , InvalidRequest       , PAYLOAD_TOO_LARGE,
"The payload sent to the server was too large. Check out this [guide](/resources/self_hosting/configuration/reference#payload-limit-size) to customize the maximum payload size accepted by Meilisearch." ;
RemoteBadResponse                              , System               , BAD_GATEWAY,
"The remote instance answered with a response that this instance could not use as a federated search response." ;
RemoteBadRequest                               , InvalidRequest       , BAD_REQUEST,
"The remote instance answered with `400 BAD REQUEST`." ;
UnknownRemote                                  , InvalidRequest       , BAD_REQUEST ;
RemoteCouldNotSendRequest                      , System               , BAD_GATEWAY,
"There was an error while sending the remote federated search request." ;
RemoteInvalidApiKey                            , Auth                 , FORBIDDEN,
"The remote instance answered with `403 FORBIDDEN` or `401 UNAUTHORIZED` to this instance’s request. The configured API keys are either missing, invalid, or lack the required permissions." ;
RemoteRemoteError                              , System               , BAD_GATEWAY,
"The remote instance answered with `500 INTERNAL ERROR`." ;
RemoteTimeout                                  , System               , BAD_GATEWAY,
"The remote did not answer in the allocated time." ;
TooManySearchRequests                          , System               , SERVICE_UNAVAILABLE ;
TaskNotFound                                   , InvalidRequest       , NOT_FOUND ;
TaskFileNotFound                               , InvalidRequest       , NOT_FOUND ;
BatchNotFound                                  , InvalidRequest       , NOT_FOUND,
"The requested batch does not exist. Please ensure that you are using the correct [`uid`](/reference/api/batches/list-batches)." ;
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
NotFoundSimilarId                              , InvalidRequest       , BAD_REQUEST,
"Meilisearch could not find the target document. Make sure your target document identifier corresponds to a document in your index." ;
InvalidDocumentEditionContext                  , InvalidRequest       , BAD_REQUEST ;
InvalidDocumentEditionFunctionFilter           , InvalidRequest       , BAD_REQUEST ;
EditDocumentsByFunctionError                   , InvalidRequest       , BAD_REQUEST ;
InvalidSettingsIndexChat                       , InvalidRequest       , BAD_REQUEST ;
// Export
InvalidExportUrl                               , InvalidRequest       , BAD_REQUEST,
r#"The export target instance URL is invalid or could not be reached.

If the target instance URL is in your private network, please check that it [resolves to an IP in an allowed range](https://www.meilisearch.com/docs/resources/self_hosting/sharding/manage_network#private-network-security).
"# ;
InvalidExportApiKey                            , InvalidRequest       , BAD_REQUEST,
"The supplied security key does not have the required permissions to access the target instance." ;
InvalidExportPayloadSize                       , InvalidRequest       , BAD_REQUEST,
"The provided payload size is invalid. The payload size must be a string indicating the maximum payload size in a human-readable format." ;
InvalidExportIndexesPatterns                   , InvalidRequest       , BAD_REQUEST,
"The provided index pattern is invalid. The index pattern must be an alphanumeric string, optionally including a wildcard." ;
InvalidExportIndexFilter                       , InvalidRequest       , BAD_REQUEST,
"The provided index export filter is not a valid [filter expression](/capabilities/filtering_sorting_faceting/advanced/filter_expression_syntax)." ;
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
InvalidIndexFieldsFilterAttributePatterns      , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterDisplayed              , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterSearchable             , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterSortable               , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterDistinct               , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterRankingRule            , InvalidRequest       , BAD_REQUEST ;
InvalidIndexFieldsFilterFilterable             , InvalidRequest       , BAD_REQUEST ;
RequiresEnterpriseEdition                      , InvalidRequest       , UNAVAILABLE_FOR_LEGAL_REASONS ;
// Render
InvalidRenderTemplate                          , InvalidRequest       , BAD_REQUEST ;
InvalidRenderInput                             , InvalidRequest       , BAD_REQUEST ;
RenderDocumentNotFound                         , InvalidRequest       , NOT_FOUND ;
TemplateParsingError                           , InvalidRequest       , BAD_REQUEST ;
TemplateRenderingError                         , InvalidRequest       , BAD_REQUEST ;
// Webhooks
InvalidWebhooks                                , InvalidRequest       , BAD_REQUEST,
"The create webhook request did not contain a valid JSON payload. Meilisearch also returns this error when you try to create more than 20 webhooks." ;
InvalidWebhookUrl                              , InvalidRequest       , BAD_REQUEST,
"The provided webhook URL isn’t a valid JSON string, is `null`, is missing, or its value cannot be parsed as a valid URL." ;
InvalidWebhookHeaders                          , InvalidRequest       , BAD_REQUEST,
"The provided webhook `headers` field is not a JSON object or not a valid HTTP header. Meilisearch also returns this error if you set more than 200 header fields for a single webhook.";
ImmutableWebhook                               , InvalidRequest       , BAD_REQUEST,
"You tried to modify a reserved [webhook](/reference/api/management/list-webhooks). Reserved webhooks are configured by Meilisearch Cloud and have `isEditable` set to `false`. Webhooks created with an instance option are also immutable." ;
InvalidWebhookUuid                             , InvalidRequest       , BAD_REQUEST,
"The provided webhook `uuid` is not a valid UUID." ;
WebhookNotFound                                , InvalidRequest       , NOT_FOUND,
"The provided webhook `uuid` does not correspond to any configured webhooks in the instance." ;
ImmutableWebhookUuid                           , InvalidRequest       , BAD_REQUEST,
"You tried to manually set a webhook `uuid`. Meilisearch automatically generates `uuid` for webhooks." ;
ImmutableWebhookIsEditable                     , InvalidRequest       , BAD_REQUEST,
"You tried to manually set a webhook's `isEditable` field. Meilisearch automatically sets `isEditable` for all webhooks. Only reserved webhooks have `isEditable` set to `false`." ;
InvalidDynamicSearchRuleOffset                 , InvalidRequest       , BAD_REQUEST ;
InvalidDynamicSearchRuleLimit                  , InvalidRequest       , BAD_REQUEST ;
InvalidDynamicSearchRuleFilter                 , InvalidRequest       , BAD_REQUEST ;
InvalidDynamicSearchRuleDescription            , InvalidRequest       , BAD_REQUEST ;
InvalidDynamicSearchRulePriority               , InvalidRequest       , BAD_REQUEST ;
InvalidDynamicSearchRuleActive                 , InvalidRequest       , BAD_REQUEST ;
InvalidDynamicSearchRuleConditions             , InvalidRequest       , BAD_REQUEST ;
InvalidDynamicSearchRuleActions                , InvalidRequest       , BAD_REQUEST ;
InvalidDynamicSearchRuleFilterQuery            , InvalidRequest       , BAD_REQUEST ;
InvalidDynamicSearchRuleFilterActive           , InvalidRequest       , BAD_REQUEST ;
DynamicSearchRuleNotFound                      , InvalidRequest       , NOT_FOUND
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
            Error::UserError(ref error) => match error {
                UserError::SerdeJson(_)
                | UserError::EnvAlreadyOpened
                | UserError::DocumentLimitReached
                | UserError::UnknownInternalDocumentId { .. } => Code::Internal,
                UserError::InvalidStoreFile => Code::InvalidStoreFile,
                UserError::NoSpaceLeftOnDevice => Code::NoSpaceLeftOnDevice,
                UserError::MaxDatabaseSizeReached => Code::DatabaseSizeLimitReached,
                UserError::AttributeLimitReached => Code::MaxFieldsLimitExceeded,
                UserError::InvalidFilter(_)
                | UserError::InvalidFilterExpression(..)
                | UserError::FilterOperatorNotAllowed { .. }
                | UserError::FilterShardNotExist { .. }
                | UserError::FilterShardOperatorNotAllowed { .. } => Code::InvalidSearchFilter,
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
                | UserError::InvalidSettingsEmbedder { .. }
                | UserError::TooManyEmbedders(_)
                | UserError::TooManyFragments(_)
                | UserError::InvalidPromptForEmbeddings(..) => Code::InvalidSettingsEmbedders,
                UserError::InvalidChatSettingsDocumentTemplate(_) => {
                    Code::InvalidChatSettingDocumentTemplate
                }
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
                UserError::InvalidFacetSearchFacetName { .. } => Code::InvalidFacetSearchFacetName,
                UserError::CriterionError(_) | UserError::MixedAttributeRankingRulesUsage => {
                    Code::InvalidSettingsRankingRules
                }
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
                UserError::InvalidMinTypoWordLenSetting(_, _) => Code::InvalidSettingsTypoTolerance,
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
            },
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
