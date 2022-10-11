pub mod document_formats;
pub mod error;
pub mod index_uid;
pub mod settings;
pub mod star_or;

pub use milli;
pub use milli::heed;
pub use milli::Index;

pub type Document = serde_json::Map<String, serde_json::Value>;
