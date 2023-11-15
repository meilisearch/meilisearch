use self::error::{EmbedError, NewEmbedderError};
use crate::prompt::PromptData;

pub mod error;
pub mod hf;
pub mod openai;
pub mod settings;

pub use self::error::Error;

pub type Embedding = Vec<f32>;

pub struct Embeddings<F> {
    data: Vec<F>,
    dimension: usize,
}

impl<F> Embeddings<F> {
    pub fn new(dimension: usize) -> Self {
        Self { data: Default::default(), dimension }
    }

    pub fn from_single_embedding(embedding: Vec<F>) -> Self {
        Self { dimension: embedding.len(), data: embedding }
    }

    pub fn from_inner(data: Vec<F>, dimension: usize) -> Result<Self, Vec<F>> {
        let mut this = Self::new(dimension);
        this.append(data)?;
        Ok(this)
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn into_inner(self) -> Vec<F> {
        self.data
    }

    pub fn as_inner(&self) -> &[F] {
        &self.data
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ [F]> + '_ {
        self.data.as_slice().chunks_exact(self.dimension)
    }

    pub fn push(&mut self, mut embedding: Vec<F>) -> Result<(), Vec<F>> {
        if embedding.len() != self.dimension {
            return Err(embedding);
        }
        self.data.append(&mut embedding);
        Ok(())
    }

    pub fn append(&mut self, mut embeddings: Vec<F>) -> Result<(), Vec<F>> {
        if embeddings.len() % self.dimension != 0 {
            return Err(embeddings);
        }
        self.data.append(&mut embeddings);
        Ok(())
    }
}

#[derive(Debug)]
pub enum Embedder {
    HuggingFace(hf::Embedder),
    OpenAi(openai::Embedder),
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct EmbeddingConfig {
    pub embedder_options: EmbedderOptions,
    pub prompt: PromptData,
    // TODO: add metrics and anything needed
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum EmbedderOptions {
    HuggingFace(hf::EmbedderOptions),
    OpenAi(openai::EmbedderOptions),
}

impl Default for EmbedderOptions {
    fn default() -> Self {
        Self::HuggingFace(Default::default())
    }
}

impl EmbedderOptions {
    pub fn huggingface() -> Self {
        Self::HuggingFace(hf::EmbedderOptions::new())
    }

    pub fn openai(api_key: String) -> Self {
        Self::OpenAi(openai::EmbedderOptions::with_default_model(api_key))
    }
}

impl Embedder {
    pub fn new(options: EmbedderOptions) -> std::result::Result<Self, NewEmbedderError> {
        Ok(match options {
            EmbedderOptions::HuggingFace(options) => Self::HuggingFace(hf::Embedder::new(options)?),
            EmbedderOptions::OpenAi(options) => Self::OpenAi(openai::Embedder::new(options)?),
        })
    }

    pub async fn embed(
        &self,
        texts: Vec<String>,
    ) -> std::result::Result<Vec<Embeddings<f32>>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed(texts).await,
            Embedder::OpenAi(embedder) => embedder.embed(texts).await,
        }
    }

    pub async fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
    ) -> std::result::Result<Vec<Vec<Embeddings<f32>>>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed_chunks(text_chunks).await,
            Embedder::OpenAi(embedder) => embedder.embed_chunks(text_chunks).await,
        }
    }

    pub fn chunk_count_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.chunk_count_hint(),
            Embedder::OpenAi(embedder) => embedder.chunk_count_hint(),
        }
    }

    pub fn prompt_count_in_chunk_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::OpenAi(embedder) => embedder.prompt_count_in_chunk_hint(),
        }
    }
}
