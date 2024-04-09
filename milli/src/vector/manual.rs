use super::error::EmbedError;
use super::{DistributionShift, Embeddings};

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

    pub fn embed(&self, mut texts: Vec<String>) -> Result<Vec<Embeddings<f32>>, EmbedError> {
        let Some(text) = texts.pop() else { return Ok(Default::default()) };
        Err(EmbedError::embed_on_manual_embedder(text))
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
    ) -> Result<Vec<Vec<Embeddings<f32>>>, EmbedError> {
        text_chunks.into_iter().map(|prompts| self.embed(prompts)).collect()
    }

    pub fn distribution(&self) -> Option<DistributionShift> {
        self.distribution
    }
}
