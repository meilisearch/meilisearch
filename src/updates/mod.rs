mod settings;

pub use settings::{Settings, Facets};

use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum UpdateMeta {
    DocumentsAddition { method: String, format: String },
    ClearDocuments,
    Settings(Settings),
    Facets(Facets),
}

#[derive(Clone, Debug)]
pub struct UpdateQueue;
