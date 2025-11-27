#![allow(clippy::result_large_err)]

pub mod batch_view;
pub mod batches;
#[cfg(not(feature = "enterprise"))]
pub mod community_edition;
pub mod compression;
pub mod deserr;
pub mod document_formats;
#[cfg(feature = "enterprise")]
pub mod enterprise_edition;
#[cfg(not(feature = "enterprise"))]
pub use community_edition as current_edition;
#[cfg(feature = "enterprise")]
pub use enterprise_edition as current_edition;
pub mod error;
pub mod facet_values_sort;
pub mod features;
pub mod index_uid;
pub mod index_uid_pattern;
pub mod keys;
pub mod locales;
pub mod network;
pub mod settings;
pub mod star_or;
pub mod task_view;
pub mod tasks;
pub mod versioning;
pub mod webhooks;
pub use milli::{heed, Index};
use uuid::Uuid;
pub use versioning::VERSION_FILE_NAME;
pub use {byte_unit, milli, serde_cs};

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type InstanceUid = Uuid;
