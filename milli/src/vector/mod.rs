use std::collections::HashMap;
use std::sync::Arc;

use self::error::{EmbedError, NewEmbedderError};
use crate::prompt::{Prompt, PromptData};

pub mod error;
pub mod hf;
pub mod manual;
pub mod openai;
pub mod settings;

pub mod ollama;

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

    pub fn embedding_count(&self) -> usize {
        self.data.len() / self.dimension
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
    UserProvided(manual::Embedder),
    Ollama(ollama::Embedder),
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct EmbeddingConfig {
    pub embedder_options: EmbedderOptions,
    pub prompt: PromptData,
    // TODO: add metrics and anything needed
}

#[derive(Clone, Default)]
pub struct EmbeddingConfigs(HashMap<String, (Arc<Embedder>, Arc<Prompt>)>);

impl EmbeddingConfigs {
    pub fn new(data: HashMap<String, (Arc<Embedder>, Arc<Prompt>)>) -> Self {
        Self(data)
    }

    pub fn get(&self, name: &str) -> Option<(Arc<Embedder>, Arc<Prompt>)> {
        self.0.get(name).cloned()
    }

    pub fn get_default(&self) -> Option<(Arc<Embedder>, Arc<Prompt>)> {
        self.get_default_embedder_name().and_then(|default| self.get(&default))
    }

    pub fn get_default_embedder_name(&self) -> Option<String> {
        let mut it = self.0.keys();
        let first_name = it.next();
        let second_name = it.next();
        match (first_name, second_name) {
            (None, _) => None,
            (Some(first), None) => Some(first.to_owned()),
            (Some(_), Some(_)) => Some("default".to_owned()),
        }
    }
}

impl IntoIterator for EmbeddingConfigs {
    type Item = (String, (Arc<Embedder>, Arc<Prompt>));

    type IntoIter = std::collections::hash_map::IntoIter<String, (Arc<Embedder>, Arc<Prompt>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum EmbedderOptions {
    HuggingFace(hf::EmbedderOptions),
    OpenAi(openai::EmbedderOptions),
    Ollama(ollama::EmbedderOptions),
    UserProvided(manual::EmbedderOptions),
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

    pub fn openai(api_key: Option<String>) -> Self {
        Self::OpenAi(openai::EmbedderOptions::with_default_model(api_key))
    }

    pub fn ollama() -> Self {
        Self::Ollama(ollama::EmbedderOptions::with_default_model())
    }
}

impl Embedder {
    pub fn new(options: EmbedderOptions) -> std::result::Result<Self, NewEmbedderError> {
        Ok(match options {
            EmbedderOptions::HuggingFace(options) => Self::HuggingFace(hf::Embedder::new(options)?),
            EmbedderOptions::OpenAi(options) => Self::OpenAi(openai::Embedder::new(options)?),
            EmbedderOptions::Ollama(options) => Self::Ollama(ollama::Embedder::new(options)?),
            EmbedderOptions::UserProvided(options) => {
                Self::UserProvided(manual::Embedder::new(options))
            }
        })
    }

    pub async fn embed(
        &self,
        texts: Vec<String>,
    ) -> std::result::Result<Vec<Embeddings<f32>>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed(texts),
            Embedder::OpenAi(embedder) => {
                let client = embedder.new_client()?;
                embedder.embed(texts, &client).await
            }
            Embedder::Ollama(embedder) => {
                let client = embedder.new_client()?;
                embedder.embed(texts, &client).await
            }
            Embedder::UserProvided(embedder) => embedder.embed(texts),
        }
    }

    /// # Panics
    ///
    /// - if called from an asynchronous context
    pub fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
    ) -> std::result::Result<Vec<Vec<Embeddings<f32>>>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed_chunks(text_chunks),
            Embedder::OpenAi(embedder) => embedder.embed_chunks(text_chunks),
            Embedder::Ollama(embedder) => embedder.embed_chunks(text_chunks),
            Embedder::UserProvided(embedder) => embedder.embed_chunks(text_chunks),
        }
    }

    pub fn chunk_count_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.chunk_count_hint(),
            Embedder::OpenAi(embedder) => embedder.chunk_count_hint(),
            Embedder::Ollama(embedder) => embedder.chunk_count_hint(),
            Embedder::UserProvided(_) => 1,
        }
    }

    pub fn prompt_count_in_chunk_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::OpenAi(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::Ollama(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::UserProvided(_) => 1,
        }
    }

    pub fn dimensions(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.dimensions(),
            Embedder::OpenAi(embedder) => embedder.dimensions(),
            Embedder::Ollama(embedder) => embedder.dimensions(),
            Embedder::UserProvided(embedder) => embedder.dimensions(),
        }
    }

    pub fn distribution(&self) -> Option<DistributionShift> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.distribution(),
            Embedder::OpenAi(embedder) => embedder.distribution(),
            Embedder::Ollama(embedder) => embedder.distribution(),
            Embedder::UserProvided(_embedder) => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DistributionShift {
    pub current_mean: f32,
    pub current_sigma: f32,
}

impl DistributionShift {
    /// `None` if sigma <= 0.
    pub fn new(mean: f32, sigma: f32) -> Option<Self> {
        if sigma <= 0.0 {
            None
        } else {
            Some(Self { current_mean: mean, current_sigma: sigma })
        }
    }

    pub fn shift(&self, score: f32) -> f32 {
        // <https://math.stackexchange.com/a/2894689>
        // We're somewhat abusively mapping the distribution of distances to a gaussian.
        // The parameters we're given is the mean and sigma of the native result distribution.
        // We're using them to retarget the distribution to a gaussian centered on 0.5 with a sigma of 0.4.

        let target_mean = 0.5;
        let target_sigma = 0.4;

        // a^2 sig1^2 = sig2^2 => a^2 = sig2^2 / sig1^2 => a = sig2 / sig1, assuming a, sig1, and sig2 positive.
        let factor = target_sigma / self.current_sigma;
        // a*mu1 + b = mu2 => b = mu2 - a*mu1
        let offset = target_mean - (factor * self.current_mean);

        let mut score = factor * score + offset;

        // clamp the final score in the ]0, 1] interval.
        if score <= 0.0 {
            score = f32::EPSILON;
        }
        if score > 1.0 {
            score = 1.0;
        }

        score
    }
}

pub const fn is_cuda_enabled() -> bool {
    cfg!(feature = "cuda")
}
