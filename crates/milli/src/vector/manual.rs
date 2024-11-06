use super::error::EmbedError;
use super::DistributionShift;
use crate::vector::Embedding;

#[derive(Debug, Clone, Copy)]
pub struct Embedder {
    dimensions: usize,
    distribution: Option<DistributionShift>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct EmbedderOptions {
    pub dimensions: usize,
    pub distribution: Option<DistributionShift>,
}

impl Embedder {
    pub fn new(options: EmbedderOptions) -> Self {
        Self { dimensions: options.dimensions, distribution: options.distribution }
    }

    pub fn embed<S: AsRef<str>>(&self, texts: &[S]) -> Result<Vec<Embedding>, EmbedError> {
        texts.as_ref().iter().map(|text| self.embed_one(text)).collect()
    }

    pub fn embed_one<S: AsRef<str>>(&self, text: S) -> Result<Embedding, EmbedError> {
        Err(EmbedError::embed_on_manual_embedder(text.as_ref().chars().take(250).collect()))
    }
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
    ) -> Result<Vec<Vec<Embedding>>, EmbedError> {
        text_chunks.into_iter().map(|prompts| self.embed(&prompts)).collect()
    }

    pub fn distribution(&self) -> Option<DistributionShift> {
        self.distribution
    }

    pub(crate) fn embed_chunks_ref(&self, texts: &[&str]) -> Result<Vec<Embedding>, EmbedError> {
        texts.iter().map(|text| self.embed_one(text)).collect()
    }
}
