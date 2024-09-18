use std::collections::HashMap;
use std::sync::Arc;

use arroy::distances::{Angular, BinaryQuantizedAngular};
use arroy::ItemId;
use deserr::{DeserializeError, Deserr};
use heed::{RoTxn, RwTxn, Unspecified};
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use self::error::{EmbedError, NewEmbedderError};
use crate::prompt::{Prompt, PromptData};
use crate::ThreadPoolNoAbort;

pub mod error;
pub mod hf;
pub mod json_template;
pub mod manual;
pub mod openai;
pub mod parsed_vectors;
pub mod settings;

pub mod ollama;
pub mod rest;

pub use self::error::Error;

pub type Embedding = Vec<f32>;

pub const REQUEST_PARALLELISM: usize = 40;

pub struct ArroyReader {
    quantized: bool,
    index: u16,
    database: arroy::Database<Unspecified>,
}

impl ArroyReader {
    pub fn new(database: arroy::Database<Unspecified>, index: u16, quantized: bool) -> Self {
        Self { database, index, quantized }
    }

    pub fn index(&self) -> u16 {
        self.index
    }

    pub fn dimensions(&self, rtxn: &RoTxn) -> Result<usize, arroy::Error> {
        if self.quantized {
            Ok(arroy::Reader::open(rtxn, self.index, self.quantized_db())?.dimensions())
        } else {
            Ok(arroy::Reader::open(rtxn, self.index, self.angular_db())?.dimensions())
        }
    }

    pub fn quantize(
        &mut self,
        wtxn: &mut RwTxn,
        index: u16,
        dimension: usize,
    ) -> Result<(), arroy::Error> {
        if !self.quantized {
            let writer = arroy::Writer::new(self.angular_db(), index, dimension);
            writer.prepare_changing_distance::<BinaryQuantizedAngular>(wtxn)?;
            self.quantized = true;
        }
        Ok(())
    }

    pub fn need_build(&self, rtxn: &RoTxn, dimension: usize) -> Result<bool, arroy::Error> {
        if self.quantized {
            arroy::Writer::new(self.quantized_db(), self.index, dimension).need_build(rtxn)
        } else {
            arroy::Writer::new(self.angular_db(), self.index, dimension).need_build(rtxn)
        }
    }

    pub fn build<R: rand::Rng + rand::SeedableRng>(
        &self,
        wtxn: &mut RwTxn,
        rng: &mut R,
        dimension: usize,
    ) -> Result<(), arroy::Error> {
        if self.quantized {
            arroy::Writer::new(self.quantized_db(), self.index, dimension).build(wtxn, rng, None)
        } else {
            arroy::Writer::new(self.angular_db(), self.index, dimension).build(wtxn, rng, None)
        }
    }

    pub fn add_item(
        &self,
        wtxn: &mut RwTxn,
        dimension: usize,
        item_id: arroy::ItemId,
        vector: &[f32],
    ) -> Result<(), arroy::Error> {
        if self.quantized {
            arroy::Writer::new(self.quantized_db(), self.index, dimension)
                .add_item(wtxn, item_id, vector)
        } else {
            arroy::Writer::new(self.angular_db(), self.index, dimension)
                .add_item(wtxn, item_id, vector)
        }
    }

    pub fn del_item(
        &self,
        wtxn: &mut RwTxn,
        dimension: usize,
        item_id: arroy::ItemId,
    ) -> Result<bool, arroy::Error> {
        if self.quantized {
            arroy::Writer::new(self.quantized_db(), self.index, dimension).del_item(wtxn, item_id)
        } else {
            arroy::Writer::new(self.angular_db(), self.index, dimension).del_item(wtxn, item_id)
        }
    }

    pub fn clear(&self, wtxn: &mut RwTxn, dimension: usize) -> Result<(), arroy::Error> {
        if self.quantized {
            arroy::Writer::new(self.quantized_db(), self.index, dimension).clear(wtxn)
        } else {
            arroy::Writer::new(self.angular_db(), self.index, dimension).clear(wtxn)
        }
    }

    pub fn is_empty(&self, rtxn: &RoTxn, dimension: usize) -> Result<bool, arroy::Error> {
        if self.quantized {
            arroy::Writer::new(self.quantized_db(), self.index, dimension).is_empty(rtxn)
        } else {
            arroy::Writer::new(self.angular_db(), self.index, dimension).is_empty(rtxn)
        }
    }

    pub fn contains_item(
        &self,
        rtxn: &RoTxn,
        dimension: usize,
        item: arroy::ItemId,
    ) -> Result<bool, arroy::Error> {
        if self.quantized {
            arroy::Writer::new(self.quantized_db(), self.index, dimension).contains_item(rtxn, item)
        } else {
            arroy::Writer::new(self.angular_db(), self.index, dimension).contains_item(rtxn, item)
        }
    }

    pub fn nns_by_item(
        &self,
        rtxn: &RoTxn,
        item: ItemId,
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Option<Vec<(ItemId, f32)>>, arroy::Error> {
        if self.quantized {
            arroy::Reader::open(rtxn, self.index, self.quantized_db())?
                .nns_by_item(rtxn, item, limit, None, None, filter)
        } else {
            arroy::Reader::open(rtxn, self.index, self.angular_db())?
                .nns_by_item(rtxn, item, limit, None, None, filter)
        }
    }

    pub fn nns_by_vector(
        &self,
        txn: &RoTxn,
        item: &[f32],
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, arroy::Error> {
        if self.quantized {
            arroy::Reader::open(txn, self.index, self.quantized_db())?
                .nns_by_vector(txn, item, limit, None, None, filter)
        } else {
            arroy::Reader::open(txn, self.index, self.angular_db())?
                .nns_by_vector(txn, item, limit, None, None, filter)
        }
    }

    pub fn item_vector(&self, rtxn: &RoTxn, docid: u32) -> Result<Option<Vec<f32>>, arroy::Error> {
        if self.quantized {
            arroy::Reader::open(rtxn, self.index, self.quantized_db())?.item_vector(rtxn, docid)
        } else {
            arroy::Reader::open(rtxn, self.index, self.angular_db())?.item_vector(rtxn, docid)
        }
    }

    fn angular_db(&self) -> arroy::Database<Angular> {
        self.database.remap_data_type()
    }

    fn quantized_db(&self) -> arroy::Database<BinaryQuantizedAngular> {
        self.database.remap_data_type()
    }
}

/// One or multiple embeddings stored consecutively in a flat vector.
pub struct Embeddings<F> {
    data: Vec<F>,
    dimension: usize,
}

impl<F> Embeddings<F> {
    /// Declares an empty  vector of embeddings of the specified dimensions.
    pub fn new(dimension: usize) -> Self {
        Self { data: Default::default(), dimension }
    }

    /// Declares a vector of embeddings containing a single element.
    ///
    /// The dimension is inferred from the length of the passed embedding.
    pub fn from_single_embedding(embedding: Vec<F>) -> Self {
        Self { dimension: embedding.len(), data: embedding }
    }

    /// Declares a vector of embeddings from its components.
    ///
    /// `data.len()` must be a multiple of `dimension`, otherwise an error is returned.
    pub fn from_inner(data: Vec<F>, dimension: usize) -> Result<Self, Vec<F>> {
        let mut this = Self::new(dimension);
        this.append(data)?;
        Ok(this)
    }

    /// Returns the number of embeddings in this vector of embeddings.
    pub fn embedding_count(&self) -> usize {
        self.data.len() / self.dimension
    }

    /// Dimension of a single embedding.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Deconstructs self into the inner flat vector.
    pub fn into_inner(self) -> Vec<F> {
        self.data
    }

    /// A reference to the inner flat vector.
    pub fn as_inner(&self) -> &[F] {
        &self.data
    }

    /// Iterates over the embeddings contained in the flat vector.
    pub fn iter(&self) -> impl Iterator<Item = &'_ [F]> + '_ {
        self.data.as_slice().chunks_exact(self.dimension)
    }

    /// Push an embedding at the end of the embeddings.
    ///
    /// If `embedding.len() != self.dimension`, then the push operation fails.
    pub fn push(&mut self, mut embedding: Vec<F>) -> Result<(), Vec<F>> {
        if embedding.len() != self.dimension {
            return Err(embedding);
        }
        self.data.append(&mut embedding);
        Ok(())
    }

    /// Append a flat vector of embeddings a the end of the embeddings.
    ///
    /// If `embeddings.len() % self.dimension != 0`, then the append operation fails.
    pub fn append(&mut self, mut embeddings: Vec<F>) -> Result<(), Vec<F>> {
        if embeddings.len() % self.dimension != 0 {
            return Err(embeddings);
        }
        self.data.append(&mut embeddings);
        Ok(())
    }
}

/// An embedder can be used to transform text into embeddings.
#[derive(Debug)]
pub enum Embedder {
    /// An embedder based on running local models, fetched from the Hugging Face Hub.
    HuggingFace(hf::Embedder),
    /// An embedder based on making embedding queries against the OpenAI API.
    OpenAi(openai::Embedder),
    /// An embedder based on the user providing the embeddings in the documents and queries.
    UserProvided(manual::Embedder),
    /// An embedder based on making embedding queries against an <https://ollama.com> embedding server.
    Ollama(ollama::Embedder),
    /// An embedder based on making embedding queries against a generic JSON/REST embedding server.
    Rest(rest::Embedder),
}

/// Configuration for an embedder.
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct EmbeddingConfig {
    /// Options of the embedder, specific to each kind of embedder
    pub embedder_options: EmbedderOptions,
    /// Document template
    pub prompt: PromptData,
    /// If this embedder is binary quantized
    pub quantized: Option<bool>,
    // TODO: add metrics and anything needed
}

impl EmbeddingConfig {
    pub fn quantized(&self) -> bool {
        self.quantized.unwrap_or_default()
    }
}

/// Map of embedder configurations.
///
/// Each configuration is mapped to a name.
#[derive(Clone, Default)]
pub struct EmbeddingConfigs(HashMap<String, (Arc<Embedder>, Arc<Prompt>, bool)>);

impl EmbeddingConfigs {
    /// Create the map from its internal component.s
    pub fn new(data: HashMap<String, (Arc<Embedder>, Arc<Prompt>, bool)>) -> Self {
        Self(data)
    }

    /// Get an embedder configuration and template from its name.
    pub fn get(&self, name: &str) -> Option<(Arc<Embedder>, Arc<Prompt>, bool)> {
        self.0.get(name).cloned()
    }

    pub fn inner_as_ref(&self) -> &HashMap<String, (Arc<Embedder>, Arc<Prompt>, bool)> {
        &self.0
    }

    pub fn into_inner(self) -> HashMap<String, (Arc<Embedder>, Arc<Prompt>, bool)> {
        self.0
    }
}

impl IntoIterator for EmbeddingConfigs {
    type Item = (String, (Arc<Embedder>, Arc<Prompt>, bool));

    type IntoIter =
        std::collections::hash_map::IntoIter<String, (Arc<Embedder>, Arc<Prompt>, bool)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
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
        Self::HuggingFace(Default::default())
    }
}

impl Embedder {
    /// Spawns a new embedder built from its options.
    pub fn new(options: EmbedderOptions) -> std::result::Result<Self, NewEmbedderError> {
        Ok(match options {
            EmbedderOptions::HuggingFace(options) => Self::HuggingFace(hf::Embedder::new(options)?),
            EmbedderOptions::OpenAi(options) => Self::OpenAi(openai::Embedder::new(options)?),
            EmbedderOptions::Ollama(options) => Self::Ollama(ollama::Embedder::new(options)?),
            EmbedderOptions::UserProvided(options) => {
                Self::UserProvided(manual::Embedder::new(options))
            }
            EmbedderOptions::Rest(options) => {
                Self::Rest(rest::Embedder::new(options, rest::ConfigurationSource::User)?)
            }
        })
    }

    /// Embed one or multiple texts.
    ///
    /// Each text can be embedded as one or multiple embeddings.
    pub fn embed(
        &self,
        texts: Vec<String>,
    ) -> std::result::Result<Vec<Embeddings<f32>>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed(texts),
            Embedder::OpenAi(embedder) => embedder.embed(texts),
            Embedder::Ollama(embedder) => embedder.embed(texts),
            Embedder::UserProvided(embedder) => embedder.embed(texts),
            Embedder::Rest(embedder) => embedder.embed(texts),
        }
    }

    pub fn embed_one(&self, text: String) -> std::result::Result<Embedding, EmbedError> {
        let mut embeddings = self.embed(vec![text])?;
        let embeddings = embeddings.pop().ok_or_else(EmbedError::missing_embedding)?;
        Ok(if embeddings.iter().nth(1).is_some() {
            tracing::warn!("Ignoring embeddings past the first one in long search query");
            embeddings.iter().next().unwrap().to_vec()
        } else {
            embeddings.into_inner()
        })
    }

    /// Embed multiple chunks of texts.
    ///
    /// Each chunk is composed of one or multiple texts.
    pub fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
        threads: &ThreadPoolNoAbort,
    ) -> std::result::Result<Vec<Vec<Embeddings<f32>>>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed_chunks(text_chunks),
            Embedder::OpenAi(embedder) => embedder.embed_chunks(text_chunks, threads),
            Embedder::Ollama(embedder) => embedder.embed_chunks(text_chunks, threads),
            Embedder::UserProvided(embedder) => embedder.embed_chunks(text_chunks),
            Embedder::Rest(embedder) => embedder.embed_chunks(text_chunks, threads),
        }
    }

    /// Indicates the preferred number of chunks to pass to [`Self::embed_chunks`]
    pub fn chunk_count_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.chunk_count_hint(),
            Embedder::OpenAi(embedder) => embedder.chunk_count_hint(),
            Embedder::Ollama(embedder) => embedder.chunk_count_hint(),
            Embedder::UserProvided(_) => 1,
            Embedder::Rest(embedder) => embedder.chunk_count_hint(),
        }
    }

    /// Indicates the preferred number of texts in a single chunk passed to [`Self::embed`]
    pub fn prompt_count_in_chunk_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::OpenAi(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::Ollama(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::UserProvided(_) => 1,
            Embedder::Rest(embedder) => embedder.prompt_count_in_chunk_hint(),
        }
    }

    /// Indicates the dimensions of a single embedding produced by the embedder.
    pub fn dimensions(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.dimensions(),
            Embedder::OpenAi(embedder) => embedder.dimensions(),
            Embedder::Ollama(embedder) => embedder.dimensions(),
            Embedder::UserProvided(embedder) => embedder.dimensions(),
            Embedder::Rest(embedder) => embedder.dimensions(),
        }
    }

    /// An optional distribution used to apply an affine transformation to the similarity score of a document.
    pub fn distribution(&self) -> Option<DistributionShift> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.distribution(),
            Embedder::OpenAi(embedder) => embedder.distribution(),
            Embedder::Ollama(embedder) => embedder.distribution(),
            Embedder::UserProvided(embedder) => embedder.distribution(),
            Embedder::Rest(embedder) => embedder.distribution(),
        }
    }

    pub fn uses_document_template(&self) -> bool {
        match self {
            Embedder::HuggingFace(_)
            | Embedder::OpenAi(_)
            | Embedder::Ollama(_)
            | Embedder::Rest(_) => true,
            Embedder::UserProvided(_) => false,
        }
    }
}

/// Describes the mean and sigma of distribution of embedding similarity in the embedding space.
///
/// The intended use is to make the similarity score more comparable to the regular ranking score.
/// This allows to correct effects where results are too "packed" around a certain value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(from = "DistributionShiftSerializable")]
#[serde(into = "DistributionShiftSerializable")]
pub struct DistributionShift {
    /// Value where the results are "packed".
    ///
    /// Similarity scores are translated so that they are packed around 0.5 instead
    pub current_mean: OrderedFloat<f32>,

    /// standard deviation of a similarity score.
    ///
    /// Set below 0.4 to make the results less packed around the mean, and above 0.4 to make them more packed.
    pub current_sigma: OrderedFloat<f32>,
}

impl<E> Deserr<E> for DistributionShift
where
    E: DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef<'_>,
    ) -> Result<Self, E> {
        let value = DistributionShiftSerializable::deserialize_from_value(value, location)?;
        if value.mean < 0. || value.mean > 1. {
            return Err(deserr::take_cf_content(E::error::<std::convert::Infallible>(
                None,
                deserr::ErrorKind::Unexpected {
                    msg: format!(
                        "the distribution mean must be in the range [0, 1], got {}",
                        value.mean
                    ),
                },
                location,
            )));
        }
        if value.sigma <= 0. || value.sigma > 1. {
            return Err(deserr::take_cf_content(E::error::<std::convert::Infallible>(
                None,
                deserr::ErrorKind::Unexpected {
                    msg: format!(
                        "the distribution sigma must be in the range ]0, 1], got {}",
                        value.sigma
                    ),
                },
                location,
            )));
        }

        Ok(value.into())
    }
}

#[derive(Serialize, Deserialize, Deserr)]
#[serde(deny_unknown_fields)]
#[deserr(deny_unknown_fields)]
struct DistributionShiftSerializable {
    mean: f32,
    sigma: f32,
}

impl From<DistributionShift> for DistributionShiftSerializable {
    fn from(
        DistributionShift {
            current_mean: OrderedFloat(current_mean),
            current_sigma: OrderedFloat(current_sigma),
        }: DistributionShift,
    ) -> Self {
        Self { mean: current_mean, sigma: current_sigma }
    }
}

impl From<DistributionShiftSerializable> for DistributionShift {
    fn from(DistributionShiftSerializable { mean, sigma }: DistributionShiftSerializable) -> Self {
        Self { current_mean: OrderedFloat(mean), current_sigma: OrderedFloat(sigma) }
    }
}

impl DistributionShift {
    /// `None` if sigma <= 0.
    pub fn new(mean: f32, sigma: f32) -> Option<Self> {
        if sigma <= 0.0 {
            None
        } else {
            Some(Self { current_mean: OrderedFloat(mean), current_sigma: OrderedFloat(sigma) })
        }
    }

    pub fn shift(&self, score: f32) -> f32 {
        let current_mean = self.current_mean.0;
        let current_sigma = self.current_sigma.0;
        // <https://math.stackexchange.com/a/2894689>
        // We're somewhat abusively mapping the distribution of distances to a gaussian.
        // The parameters we're given is the mean and sigma of the native result distribution.
        // We're using them to retarget the distribution to a gaussian centered on 0.5 with a sigma of 0.4.

        let target_mean = 0.5;
        let target_sigma = 0.4;

        // a^2 sig1^2 = sig2^2 => a^2 = sig2^2 / sig1^2 => a = sig2 / sig1, assuming a, sig1, and sig2 positive.
        let factor = target_sigma / current_sigma;
        // a*mu1 + b = mu2 => b = mu2 - a*mu1
        let offset = target_mean - (factor * current_mean);

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

/// Whether CUDA is supported in this version of Meilisearch.
pub const fn is_cuda_enabled() -> bool {
    cfg!(feature = "cuda")
}

pub fn arroy_db_range_for_embedder(embedder_id: u8) -> impl Iterator<Item = u16> {
    let embedder_id = (embedder_id as u16) << 8;

    (0..=u8::MAX).map(move |k| embedder_id | (k as u16))
}
