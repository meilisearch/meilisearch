mod mock_analytics;
#[cfg(feature = "analytics")]
mod segment_analytics;

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use actix_web::HttpRequest;
use meilisearch_types::InstanceUid;
pub use mock_analytics::MockAnalytics;
use once_cell::sync::Lazy;
use platform_dirs::AppDirs;
use serde_json::Value;

use crate::routes::indexes::documents::{DocumentEditionByFunction, UpdateDocumentsQuery};

// if the analytics feature is disabled
// the `SegmentAnalytics` point to the mock instead of the real analytics
#[cfg(not(feature = "analytics"))]
pub type SegmentAnalytics = mock_analytics::MockAnalytics;
#[cfg(not(feature = "analytics"))]
pub type SearchAggregator = mock_analytics::SearchAggregator;
#[cfg(not(feature = "analytics"))]
pub type SimilarAggregator = mock_analytics::SimilarAggregator;
#[cfg(not(feature = "analytics"))]
pub type MultiSearchAggregator = mock_analytics::MultiSearchAggregator;
#[cfg(not(feature = "analytics"))]
pub type FacetSearchAggregator = mock_analytics::FacetSearchAggregator;

// if the feature analytics is enabled we use the real analytics
#[cfg(feature = "analytics")]
pub type SegmentAnalytics = segment_analytics::SegmentAnalytics;
#[cfg(feature = "analytics")]
pub type SearchAggregator = segment_analytics::SearchAggregator;
#[cfg(feature = "analytics")]
pub type SimilarAggregator = segment_analytics::SimilarAggregator;
#[cfg(feature = "analytics")]
pub type MultiSearchAggregator = segment_analytics::MultiSearchAggregator;
#[cfg(feature = "analytics")]
pub type FacetSearchAggregator = segment_analytics::FacetSearchAggregator;

/// The Meilisearch config dir:
/// `~/.config/Meilisearch` on *NIX or *BSD.
/// `~/Library/ApplicationSupport` on macOS.
/// `%APPDATA` (= `C:\Users%USERNAME%\AppData\Roaming`) on windows.
static MEILISEARCH_CONFIG_PATH: Lazy<Option<PathBuf>> =
    Lazy::new(|| AppDirs::new(Some("Meilisearch"), false).map(|appdir| appdir.config_dir));

fn config_user_id_path(db_path: &Path) -> Option<PathBuf> {
    db_path
        .canonicalize()
        .ok()
        .map(|path| path.join("instance-uid").display().to_string().replace('/', "-"))
        .zip(MEILISEARCH_CONFIG_PATH.as_ref())
        .map(|(filename, config_path)| config_path.join(filename.trim_start_matches('-')))
}

/// Look for the instance-uid in the `data.ms` or in `~/.config/Meilisearch/path-to-db-instance-uid`
fn find_user_id(db_path: &Path) -> Option<InstanceUid> {
    fs::read_to_string(db_path.join("instance-uid"))
        .ok()
        .or_else(|| fs::read_to_string(config_user_id_path(db_path)?).ok())
        .and_then(|uid| InstanceUid::from_str(&uid).ok())
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DocumentDeletionKind {
    PerDocumentId,
    ClearAll,
    PerBatch,
    PerFilter,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DocumentFetchKind {
    PerDocumentId { retrieve_vectors: bool },
    Normal { with_filter: bool, limit: usize, offset: usize, retrieve_vectors: bool },
}

pub trait Analytics: Sync + Send {
    fn instance_uid(&self) -> Option<&InstanceUid>;

    /// The method used to publish most analytics that do not need to be batched every hours
    fn publish(&self, event_name: String, send: Value, request: Option<&HttpRequest>);

    /// This method should be called to aggregate a get search
    fn get_search(&self, aggregate: SearchAggregator);

    /// This method should be called to aggregate a post search
    fn post_search(&self, aggregate: SearchAggregator);

    /// This method should be called to aggregate a get similar request
    fn get_similar(&self, aggregate: SimilarAggregator);

    /// This method should be called to aggregate a post similar request
    fn post_similar(&self, aggregate: SimilarAggregator);

    /// This method should be called to aggregate a post array of searches
    fn post_multi_search(&self, aggregate: MultiSearchAggregator);

    /// This method should be called to aggregate post facet values searches
    fn post_facet_search(&self, aggregate: FacetSearchAggregator);

    // this method should be called to aggregate a add documents request
    fn add_documents(
        &self,
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    );

    // this method should be called to aggregate a fetch documents request
    fn get_fetch_documents(&self, documents_query: &DocumentFetchKind, request: &HttpRequest);

    // this method should be called to aggregate a fetch documents request
    fn post_fetch_documents(&self, documents_query: &DocumentFetchKind, request: &HttpRequest);

    // this method should be called to aggregate a add documents request
    fn delete_documents(&self, kind: DocumentDeletionKind, request: &HttpRequest);

    // this method should be called to batch an update documents request
    fn update_documents(
        &self,
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    );

    // this method should be called to batch an update documents by function request
    fn update_documents_by_function(
        &self,
        documents_query: &DocumentEditionByFunction,
        index_creation: bool,
        request: &HttpRequest,
    );
}
