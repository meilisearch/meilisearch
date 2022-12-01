pub mod compression;
pub mod document_formats;
pub mod error;
pub mod index_uid;
pub mod index_uid_pattern;
pub mod keys;
pub mod settings;
pub mod star_or;
pub mod tasks;
pub mod versioning;

pub use milli;
pub use milli::{heed, Index};
use uuid::Uuid;
pub use versioning::VERSION_FILE_NAME;

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type InstanceUid = Uuid;
