pub mod segment_analytics;

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use actix_web::HttpRequest;
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::InstanceUid;
use mopa::mopafy;
use once_cell::sync::Lazy;
use platform_dirs::AppDirs;

// if the feature analytics is enabled we use the real analytics
pub type SegmentAnalytics = segment_analytics::SegmentAnalytics;

use crate::Opt;

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

            fn aggregate(self: Box<Self>, _other: Box<Self>) -> Box<Self> {
                self
            }

            fn into_event(self: Box<Self>) -> serde_json::Value {
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

/// To send an event to segment, your event must be able to aggregate itself with another event of the same type.
pub trait Aggregate: 'static + mopa::Any + Send {
    /// The name of the event that will be sent to segment.
    fn event_name(&self) -> &'static str;

    /// Will be called every time an event has been used twice before segment flushed its buffer.
    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self>
    where
        Self: Sized;

    /// Converts your structure to the final event that'll be sent to segment.
    fn into_event(self: Box<Self>) -> serde_json::Value;
}

mopafy!(Aggregate);

/// Helper trait to define multiple aggregates with the same content but a different name.
/// Commonly used when you must aggregate a search with POST or with GET, for example.
pub trait AggregateMethod: 'static + Default + Send {
    fn event_name() -> &'static str;
}

/// A macro used to quickly define multiple aggregate method with their name
/// Usage:
/// ```rust
/// use meilisearch::aggregate_methods;
///
/// aggregate_methods!(
///     SearchGET => "Documents Searched GET",
///     SearchPOST => "Documents Searched POST",
/// );
/// ```
#[macro_export]
macro_rules! aggregate_methods {
    ($method:ident => $event_name:literal) => {
        #[derive(Default)]
        pub struct $method {}

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

#[derive(Clone)]
pub struct Analytics {
    segment: Option<Arc<SegmentAnalytics>>,
}

impl Analytics {
    pub async fn new(
        opt: &Opt,
        index_scheduler: Arc<IndexScheduler>,
        auth_controller: Arc<AuthController>,
    ) -> Self {
        if opt.no_analytics {
            Self { segment: None }
        } else {
            Self { segment: SegmentAnalytics::new(opt, index_scheduler, auth_controller).await }
        }
    }

    pub fn no_analytics() -> Self {
        Self { segment: None }
    }

    pub fn instance_uid(&self) -> Option<&InstanceUid> {
        self.segment.as_ref().map(|segment| segment.instance_uid.as_ref())
    }

    /// The method used to publish most analytics that do not need to be batched every hours
    pub fn publish<T: Aggregate>(&self, event: T, request: &HttpRequest) {
        if let Some(ref segment) = self.segment {
            let _ = segment.sender.try_send(segment_analytics::Message::new(event, request));
        }
    }
}
