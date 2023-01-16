mod mock_analytics;
// if we are in release mode and the feature analytics was enabled
#[cfg(all(not(debug_assertions), feature = "analytics"))]
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

use crate::routes::indexes::documents::UpdateDocumentsQuery;
use crate::routes::tasks::TasksFilterQuery;

// if we are in debug mode OR the analytics feature is disabled
// the `SegmentAnalytics` point to the mock instead of the real analytics
#[cfg(any(debug_assertions, not(feature = "analytics")))]
pub type SegmentAnalytics = mock_analytics::MockAnalytics;
#[cfg(any(debug_assertions, not(feature = "analytics")))]
pub type SearchAggregator = mock_analytics::SearchAggregator;

// if we are in release mode and the feature analytics was enabled
// we use the real analytics
#[cfg(all(not(debug_assertions), feature = "analytics"))]
pub type SegmentAnalytics = segment_analytics::SegmentAnalytics;
#[cfg(all(not(debug_assertions), feature = "analytics"))]
pub type SearchAggregator = segment_analytics::SearchAggregator;

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
}

pub trait Analytics: Sync + Send {
    fn instance_uid(&self) -> Option<&InstanceUid>;

    /// The method used to publish most analytics that do not need to be batched every hours
    fn publish(&self, event_name: String, send: Value, request: Option<&HttpRequest>);

    /// This method should be called to aggregate a get search
    fn get_search(&self, aggregate: SearchAggregator);

    /// This method should be called to aggregate a post search
    fn post_search(&self, aggregate: SearchAggregator);

    // this method should be called to aggregate a add documents request
    fn add_documents(
        &self,
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    );

    // this method should be called to aggregate a add documents request
    fn delete_documents(&self, kind: DocumentDeletionKind, request: &HttpRequest);

    // this method should be called to batch a update documents request
    fn update_documents(
        &self,
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    );

    // this method should be called to aggregate the get tasks requests.
    fn get_tasks(&self, query: &TasksFilterQuery, request: &HttpRequest);

    // this method should be called to aggregate a add documents request
    fn health_seen(&self, request: &HttpRequest);
}
