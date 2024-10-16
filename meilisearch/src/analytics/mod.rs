pub mod segment_analytics;

use std::any::TypeId;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use actix_web::HttpRequest;
use meilisearch_types::InstanceUid;
use once_cell::sync::Lazy;
use platform_dirs::AppDirs;
use segment::message::User;
use serde::Serialize;

// if the feature analytics is enabled we use the real analytics
pub type SegmentAnalytics = segment_analytics::SegmentAnalytics;
pub use segment_analytics::SearchAggregator;
pub use segment_analytics::SimilarAggregator;

use self::segment_analytics::extract_user_agents;
pub type MultiSearchAggregator = segment_analytics::MultiSearchAggregator;
pub type FacetSearchAggregator = segment_analytics::FacetSearchAggregator;

/// A macro used to quickly define events that don't aggregate or send anything besides an empty event with its name.
#[macro_export]
macro_rules! empty_analytics {
    ($struct_name:ident, $event_name:literal) => {
        #[derive(Default)]
        struct $struct_name {}

        impl $crate::analytics::Aggregate for $struct_name {
            fn event_name(&self) -> &'static str {
                $event_name
            }

            fn aggregate(self, _other: Self) -> Self {
                self
            }

            fn into_event(self) -> impl serde::Serialize {
                serde_json::json!({})
            }
        }
    };
}

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

pub trait Aggregate {
    fn event_name(&self) -> &'static str;

    fn aggregate(self, other: Self) -> Self
    where
        Self: Sized;

    fn into_event(self) -> impl Serialize
    where
        Self: Sized;
}

/// Helper trait to define multiple aggregate with the same content but a different name.
/// Commonly used when you must aggregate a search with POST or with GET for example.
pub trait AggregateMethod {
    fn event_name() -> &'static str;
}

/// A macro used to quickly define multiple aggregate method with their name
#[macro_export]
macro_rules! aggregate_methods {
    ($method:ident => $event_name:literal) => {
        pub enum $method {}

        impl $crate::analytics::AggregateMethod for $method {
            fn event_name() -> &'static str {
                $event_name
            }
        }
    };
    ($($method:ident => $event_name:literal,)+) => {
        $(
            aggregate_methods!($method => $event_name);
        )+

    };
}

pub struct Analytics {
    // TODO: TAMO: remove
    inner: Option<SegmentAnalytics>,

    instance_uid: Option<InstanceUid>,
    user: Option<User>,
    events: HashMap<TypeId, Box<dyn Aggregate>>,
}

impl Analytics {
    fn no_analytics() -> Self {
        Self { inner: None, events: HashMap::new(), instance_uid: None, user: None }
    }

    fn segment_analytics(segment: SegmentAnalytics) -> Self {
        Self {
            instance_uid: Some(segment.instance_uid),
            user: Some(segment.user),
            inner: Some(segment),
            events: HashMap::new(),
        }
    }

    pub fn instance_uid(&self) -> Option<&InstanceUid> {
        self.instance_uid
    }

    /// The method used to publish most analytics that do not need to be batched every hours
    pub fn publish(&self, send: impl Aggregate, request: &HttpRequest) {
        let Some(segment) = self.inner else { return };
        let user_agents = extract_user_agents(request);
    }
}
