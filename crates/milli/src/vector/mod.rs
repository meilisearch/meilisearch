pub mod db;
mod distribution;
pub mod embedder;
mod embeddings;
pub mod error;
pub mod extractor;
pub mod json_template;
pub mod parsed_vectors;
mod runtime;
pub mod session;
pub mod settings;
mod store;

pub use self::error::Error;

pub type Embedding = Vec<f32>;

pub use distribution::DistributionShift;
pub use embedder::{Embedder, EmbedderOptions, EmbeddingConfig, SearchQuery};
pub use embeddings::Embeddings;
pub use runtime::{RuntimeEmbedder, RuntimeEmbedders, RuntimeFragment};
pub use store::{VectorStore, VectorStoreBackend, VectorStoreStats};

pub const REQUEST_PARALLELISM: usize = 40;

/// Whether CUDA is supported in this version of Meilisearch.
pub const fn is_cuda_enabled() -> bool {
    cfg!(feature = "cuda")
}
