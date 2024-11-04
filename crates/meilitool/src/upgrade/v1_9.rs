use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub type FieldDistribution = std::collections::BTreeMap<String, u64>;

/// The statistics that can be computed from an `Index` object.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct IndexStats {
    /// Number of documents in the index.
    pub number_of_documents: u64,
    /// Size taken up by the index' DB, in bytes.
    ///
    /// This includes the size taken by both the used and free pages of the DB, and as the free pages
    /// are not returned to the disk after a deletion, this number is typically larger than
    /// `used_database_size` that only includes the size of the used pages.
    pub database_size: u64,
    /// Size taken by the used pages of the index' DB, in bytes.
    ///
    /// As the DB backend does not return to the disk the pages that are not currently used by the DB,
    /// this value is typically smaller than `database_size`.
    pub used_database_size: u64,
    /// Association of every field name with the number of times it occurs in the documents.
    pub field_distribution: FieldDistribution,
    /// Creation date of the index.
    pub created_at: LegacyTime,
    /// Date of the last update of the index.
    pub updated_at: LegacyTime,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IndexEmbeddingConfig {
    pub name: String,
    pub config: EmbeddingConfig,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct EmbeddingConfig {
    /// Options of the embedder, specific to each kind of embedder
    pub embedder_options: EmbedderOptions,
}

/// Options of an embedder, specific to each kind of embedder.
#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum EmbedderOptions {
    HuggingFace(hf::EmbedderOptions),
    OpenAi(openai::EmbedderOptions),
    Ollama(ollama::EmbedderOptions),
    UserProvided(manual::EmbedderOptions),
    Rest(rest::EmbedderOptions),
}

impl Default for EmbedderOptions {
    fn default() -> Self {
        Self::OpenAi(openai::EmbedderOptions { api_key: None, dimensions: None })
    }
}

mod hf {
    #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub struct EmbedderOptions {
        pub model: String,
        pub revision: Option<String>,
    }
}
mod openai {

    #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub struct EmbedderOptions {
        pub api_key: Option<String>,
        pub dimensions: Option<usize>,
    }
}
mod ollama {
    #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub struct EmbedderOptions {
        pub embedding_model: String,
        pub url: Option<String>,
        pub api_key: Option<String>,
    }
}
mod manual {
    #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub struct EmbedderOptions {
        pub dimensions: usize,
    }
}
mod rest {
    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize, Hash)]
    pub struct EmbedderOptions {
        pub api_key: Option<String>,
        pub dimensions: Option<usize>,
        pub url: String,
        pub input_field: Vec<String>,
        // path to the array of embeddings
        pub path_to_embeddings: Vec<String>,
        // shape of a single embedding
        pub embedding_object: Vec<String>,
    }
}

// 2024-11-04 13:32:08.48368 +00:00:00
time::serde::format_description!(legacy_datetime, OffsetDateTime, "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond] [offset_hour sign:mandatory]:[offset_minute]:[offset_second]");

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct LegacyTime(#[serde(with = "legacy_datetime")] pub OffsetDateTime);
