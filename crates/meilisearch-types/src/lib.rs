pub mod batch_view;
pub mod batches;
pub mod compression;
pub mod deserr;
pub mod document_formats;
pub mod error;
pub mod facet_values_sort;
pub mod features;
pub mod index_uid;
pub mod index_uid_pattern;
pub mod keys;
pub mod locales;
pub mod settings;
pub mod star_or;
pub mod task_view;
pub mod tasks;
pub mod versioning;

pub mod api_key_restrictions;

// Rate limiting modules
mod api_key_rate_limiter_wrapper;
pub mod in_memory_rate_limiter;
pub mod rate_limiter_trait;
pub mod redis_rate_limiter;

// Re-export the main RateLimiter and RateLimitInfo
pub mod api_key_rate_limiter {
    pub use crate::api_key_rate_limiter_wrapper::RateLimiter;
    pub use crate::rate_limiter_trait::RateLimitInfo;
}
pub use milli::{heed, Index};
use uuid::Uuid;
pub use versioning::VERSION_FILE_NAME;
pub use {byte_unit, milli, serde_cs};

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type InstanceUid = Uuid;
