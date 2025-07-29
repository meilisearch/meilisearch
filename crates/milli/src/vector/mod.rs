use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use deserr::{DeserializeError, Deserr};
use hannoy::distances::{BinaryQuantizedCosine, Cosine};
use hannoy::ItemId;
use heed::{RoTxn, RwTxn, Unspecified};
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use self::error::{EmbedError, NewEmbedderError};
use crate::progress::{EmbedderStats, Progress};
use crate::prompt::{Prompt, PromptData};
use crate::vector::composite::SubEmbedderOptions;
use crate::vector::json_template::JsonTemplate;
use crate::ThreadPoolNoAbort;

pub mod composite;
pub mod db;
pub mod error;
pub mod extractor;
pub mod hf;
pub mod json_template;
pub mod manual;
pub mod openai;
pub mod parsed_vectors;
pub mod session;
pub mod settings;

pub mod ollama;
pub mod rest;

pub use self::error::Error;

pub type Embedding = Vec<f32>;

pub const REQUEST_PARALLELISM: usize = 40;
pub const MAX_COMPOSITE_DISTANCE: f32 = 0.01;

const HANNOY_EF_CONSTRUCTION: usize = 48;
const HANNOY_M: usize = 16;
const HANNOY_M0: usize = 32;

pub struct HannoyWrapper {
    quantized: bool,
    embedder_index: u8,
    database: hannoy::Database<Unspecified>,
}

impl HannoyWrapper {
    pub fn new(
        database: hannoy::Database<Unspecified>,
        embedder_index: u8,
        quantized: bool,
    ) -> Self {
        Self { database, embedder_index, quantized }
    }

    pub fn embedder_index(&self) -> u8 {
        self.embedder_index
    }

    fn readers<'a, D: hannoy::Distance>(
        &'a self,
        rtxn: &'a RoTxn<'a>,
        db: hannoy::Database<D>,
    ) -> impl Iterator<Item = Result<hannoy::Reader<'a, D>, hannoy::Error>> + 'a {
        hannoy_store_range_for_embedder(self.embedder_index).filter_map(move |index| {
            match hannoy::Reader::open(rtxn, index, db) {
                Ok(reader) => match reader.is_empty(rtxn) {
                    Ok(false) => Some(Ok(reader)),
                    Ok(true) => None,
                    Err(e) => Some(Err(e)),
                },
                Err(hannoy::Error::MissingMetadata(_)) => None,
                Err(e) => Some(Err(e)),
            }
        })
    }

    /// The item ids that are present in the store specified by its id.
    ///
    /// The ids are accessed via a lambda to avoid lifetime shenanigans.
    pub fn items_in_store<F, O>(
        &self,
        rtxn: &RoTxn,
        store_id: u8,
        with_items: F,
    ) -> Result<O, hannoy::Error>
    where
        F: FnOnce(&RoaringBitmap) -> O,
    {
        if self.quantized {
            self._items_in_store(rtxn, self.quantized_db(), store_id, with_items)
        } else {
            self._items_in_store(rtxn, self.angular_db(), store_id, with_items)
        }
    }

    fn _items_in_store<D: hannoy::Distance, F, O>(
        &self,
        rtxn: &RoTxn,
        db: hannoy::Database<D>,
        store_id: u8,
        with_items: F,
    ) -> Result<O, hannoy::Error>
    where
        F: FnOnce(&RoaringBitmap) -> O,
    {
        let index = hannoy_store_for_embedder(self.embedder_index, store_id);
        let reader = hannoy::Reader::open(rtxn, index, db);
        match reader {
            Ok(reader) => Ok(with_items(reader.item_ids())),
            Err(hannoy::Error::MissingMetadata(_)) => Ok(with_items(&RoaringBitmap::new())),
            Err(err) => Err(err),
        }
    }

    pub fn dimensions(&self, rtxn: &RoTxn) -> Result<Option<usize>, hannoy::Error> {
        if self.quantized {
            Ok(self
                .readers(rtxn, self.quantized_db())
                .next()
                .transpose()?
                .map(|reader| reader.dimensions()))
        } else {
            Ok(self
                .readers(rtxn, self.angular_db())
                .next()
                .transpose()?
                .map(|reader| reader.dimensions()))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_and_quantize<R: rand::Rng + rand::SeedableRng>(
        &mut self,
        wtxn: &mut RwTxn,
        progress: &Progress,
        rng: &mut R,
        dimension: usize,
        quantizing: bool,
        hannoy_memory: Option<usize>,
        cancel: &(impl Fn() -> bool + Sync + Send),
    ) -> Result<(), hannoy::Error> {
        for index in hannoy_store_range_for_embedder(self.embedder_index) {
            if self.quantized {
                let writer = hannoy::Writer::new(self.quantized_db(), index, dimension);
                if writer.need_build(wtxn)? {
                    writer
                        .builder(rng)
                        .ef_construction(HANNOY_EF_CONSTRUCTION)
                        .build::<HANNOY_M, HANNOY_M0>(wtxn)?
                } else if writer.is_empty(wtxn)? {
                    continue;
                }
            } else {
                let writer = hannoy::Writer::new(self.angular_db(), index, dimension);
                // If we are quantizing the databases, we can't know from meilisearch
                // if the db was empty but still contained the wrong metadata, thus we need
                // to quantize everything and can't stop early. Since this operation can
                // only happens once in the life of an embedder, it's not very performances
                // sensitive.
                if quantizing && !self.quantized {
                    // let writer = writer.prepare_changing_distance::<BinaryQuantizedCosine>(wtxn)?;
                    // writer
                    //     .builder(rng)
                    //     .available_memory(hannoy_memory.unwrap_or(usize::MAX))
                    //     .progress(|step| progress.update_progress_from_hannoy(step))
                    //     .cancel(cancel)
                    //     .build(wtxn)?;
                    unimplemented!("switching from quantized to non-quantized");
                } else if writer.need_build(wtxn)? {
                    writer
                        .builder(rng)
                        .available_memory(hannoy_memory.unwrap_or(usize::MAX))
                        // .progress(|step| progress.update_progress_from_hannoy(step))
                        // .cancel(cancel)
                        .ef_construction(HANNOY_EF_CONSTRUCTION)
                        .build::<HANNOY_M, HANNOY_M0>(wtxn)?;
                } else if writer.is_empty(wtxn)? {
                    continue;
                }
            }
        }
        Ok(())
    }

    /// Overwrite all the embeddings associated with the index and item ID.
    /// /!\ It won't remove embeddings after the last passed embedding, which can leave stale embeddings.
    ///     You should call `del_items` on the `item_id` before calling this method.
    /// /!\ Cannot insert more than u8::MAX embeddings; after inserting u8::MAX embeddings, all the remaining ones will be silently ignored.
    pub fn add_items(
        &self,
        wtxn: &mut RwTxn,
        item_id: hannoy::ItemId,
        embeddings: &Embeddings<f32>,
    ) -> Result<(), hannoy::Error> {
        let dimension = embeddings.dimension();
        for (index, vector) in
            hannoy_store_range_for_embedder(self.embedder_index).zip(embeddings.iter())
        {
            if self.quantized {
                hannoy::Writer::new(self.quantized_db(), index, dimension)
                    .add_item(wtxn, item_id, vector)?
            } else {
                hannoy::Writer::new(self.angular_db(), index, dimension)
                    .add_item(wtxn, item_id, vector)?
            }
        }
        Ok(())
    }

    /// Add one document int for this index where we can find an empty spot.
    pub fn add_item(
        &self,
        wtxn: &mut RwTxn,
        item_id: hannoy::ItemId,
        vector: &[f32],
    ) -> Result<(), hannoy::Error> {
        if self.quantized {
            self._add_item(wtxn, self.quantized_db(), item_id, vector)
        } else {
            self._add_item(wtxn, self.angular_db(), item_id, vector)
        }
    }

    fn _add_item<D: hannoy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: hannoy::Database<D>,
        item_id: hannoy::ItemId,
        vector: &[f32],
    ) -> Result<(), hannoy::Error> {
        let dimension = vector.len();

        for index in hannoy_store_range_for_embedder(self.embedder_index) {
            let writer = hannoy::Writer::new(db, index, dimension);
            if !writer.contains_item(wtxn, item_id)? {
                writer.add_item(wtxn, item_id, vector)?;
                break;
            }
        }
        Ok(())
    }

    /// Add a vector associated with a document in store specified by its id.
    ///
    /// Any existing vector associated with the document in the store will be replaced by the new vector.
    pub fn add_item_in_store(
        &self,
        wtxn: &mut RwTxn,
        item_id: hannoy::ItemId,
        store_id: u8,
        vector: &[f32],
    ) -> Result<(), hannoy::Error> {
        if self.quantized {
            self._add_item_in_store(wtxn, self.quantized_db(), item_id, store_id, vector)
        } else {
            self._add_item_in_store(wtxn, self.angular_db(), item_id, store_id, vector)
        }
    }

    fn _add_item_in_store<D: hannoy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: hannoy::Database<D>,
        item_id: hannoy::ItemId,
        store_id: u8,
        vector: &[f32],
    ) -> Result<(), hannoy::Error> {
        let dimension = vector.len();

        let index = hannoy_store_for_embedder(self.embedder_index, store_id);
        let writer = hannoy::Writer::new(db, index, dimension);
        writer.add_item(wtxn, item_id, vector)
    }

    /// Delete all embeddings from a specific `item_id`
    pub fn del_items(
        &self,
        wtxn: &mut RwTxn,
        dimension: usize,
        item_id: hannoy::ItemId,
    ) -> Result<(), hannoy::Error> {
        for index in hannoy_store_range_for_embedder(self.embedder_index) {
            if self.quantized {
                let writer = hannoy::Writer::new(self.quantized_db(), index, dimension);
                writer.del_item(wtxn, item_id)?;
            } else {
                let writer = hannoy::Writer::new(self.angular_db(), index, dimension);
                writer.del_item(wtxn, item_id)?;
            }
        }

        Ok(())
    }

    /// Removes the item specified by its id from the store specified by its id.
    ///
    /// Returns whether the item was removed.
    ///
    /// # Warning
    ///
    /// - This function will silently fail to remove the item if used against an arroy database that was never built.
    pub fn del_item_in_store(
        &self,
        wtxn: &mut RwTxn,
        item_id: hannoy::ItemId,
        store_id: u8,
        dimensions: usize,
    ) -> Result<bool, hannoy::Error> {
        if self.quantized {
            self._del_item_in_store(wtxn, self.quantized_db(), item_id, store_id, dimensions)
        } else {
            self._del_item_in_store(wtxn, self.angular_db(), item_id, store_id, dimensions)
        }
    }

    fn _del_item_in_store<D: hannoy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: hannoy::Database<D>,
        item_id: hannoy::ItemId,
        store_id: u8,
        dimensions: usize,
    ) -> Result<bool, hannoy::Error> {
        let index = hannoy_store_for_embedder(self.embedder_index, store_id);
        let writer = hannoy::Writer::new(db, index, dimensions);
        writer.del_item(wtxn, item_id)
    }

    /// Removes all items from the store specified by its id.
    ///
    /// # Warning
    ///
    /// - This function will silently fail to remove the items if used against an arroy database that was never built.
    pub fn clear_store(
        &self,
        wtxn: &mut RwTxn,
        store_id: u8,
        dimensions: usize,
    ) -> Result<(), hannoy::Error> {
        if self.quantized {
            self._clear_store(wtxn, self.quantized_db(), store_id, dimensions)
        } else {
            self._clear_store(wtxn, self.angular_db(), store_id, dimensions)
        }
    }

    fn _clear_store<D: hannoy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: hannoy::Database<D>,
        store_id: u8,
        dimensions: usize,
    ) -> Result<(), hannoy::Error> {
        let index = hannoy_store_for_embedder(self.embedder_index, store_id);
        let writer = hannoy::Writer::new(db, index, dimensions);
        writer.clear(wtxn)
    }

    /// Delete one item from its value.
    pub fn del_item(
        &self,
        wtxn: &mut RwTxn,
        item_id: hannoy::ItemId,
        vector: &[f32],
    ) -> Result<bool, hannoy::Error> {
        if self.quantized {
            self._del_item(wtxn, self.quantized_db(), item_id, vector)
        } else {
            self._del_item(wtxn, self.angular_db(), item_id, vector)
        }
    }

    fn _del_item<D: hannoy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: hannoy::Database<D>,
        item_id: hannoy::ItemId,
        vector: &[f32],
    ) -> Result<bool, hannoy::Error> {
        let dimension = vector.len();

        for index in hannoy_store_range_for_embedder(self.embedder_index) {
            let writer = hannoy::Writer::new(db, index, dimension);
            if writer.contains_item(wtxn, item_id)? {
                return writer.del_item(wtxn, item_id);
            }
        }
        Ok(false)
    }

    pub fn clear(&self, wtxn: &mut RwTxn, dimension: usize) -> Result<(), hannoy::Error> {
        for index in hannoy_store_range_for_embedder(self.embedder_index) {
            if self.quantized {
                let writer = hannoy::Writer::new(self.quantized_db(), index, dimension);
                if writer.is_empty(wtxn)? {
                    continue;
                }
                writer.clear(wtxn)?;
            } else {
                let writer = hannoy::Writer::new(self.angular_db(), index, dimension);
                if writer.is_empty(wtxn)? {
                    continue;
                }
                writer.clear(wtxn)?;
            }
        }
        Ok(())
    }

    pub fn contains_item(
        &self,
        rtxn: &RoTxn,
        dimension: usize,
        item: hannoy::ItemId,
    ) -> Result<bool, hannoy::Error> {
        for index in hannoy_store_range_for_embedder(self.embedder_index) {
            let contains = if self.quantized {
                let writer = hannoy::Writer::new(self.quantized_db(), index, dimension);
                if writer.is_empty(rtxn)? {
                    continue;
                }
                writer.contains_item(rtxn, item)?
            } else {
                let writer = hannoy::Writer::new(self.angular_db(), index, dimension);
                if writer.is_empty(rtxn)? {
                    continue;
                }
                writer.contains_item(rtxn, item)?
            };
            if contains {
                return Ok(contains);
            }
        }
        Ok(false)
    }

    pub fn nns_by_item(
        &self,
        rtxn: &RoTxn,
        item: ItemId,
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, hannoy::Error> {
        if self.quantized {
            self._nns_by_item(rtxn, self.quantized_db(), item, limit, filter)
        } else {
            self._nns_by_item(rtxn, self.angular_db(), item, limit, filter)
        }
    }

    fn _nns_by_item<D: hannoy::Distance>(
        &self,
        rtxn: &RoTxn,
        db: hannoy::Database<D>,
        item: ItemId,
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, hannoy::Error> {
        let mut results = Vec::new();

        for reader in self.readers(rtxn, db) {
            let reader = reader?;
            let mut searcher = reader.nns(limit, limit * 2); // TODO find better ef
            if let Some(filter) = filter {
                if reader.item_ids().is_disjoint(filter) {
                    continue;
                }
                searcher.candidates(filter);
            }

            if let Some(mut ret) = searcher.by_item(rtxn, item)? {
                results.append(&mut ret);
            }
        }
        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));
        Ok(results)
    }

    pub fn nns_by_vector(
        &self,
        rtxn: &RoTxn,
        vector: &[f32],
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, hannoy::Error> {
        if self.quantized {
            self._nns_by_vector(rtxn, self.quantized_db(), vector, limit, filter)
        } else {
            self._nns_by_vector(rtxn, self.angular_db(), vector, limit, filter)
        }
    }

    fn _nns_by_vector<D: hannoy::Distance>(
        &self,
        rtxn: &RoTxn,
        db: hannoy::Database<D>,
        vector: &[f32],
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, hannoy::Error> {
        let mut results = Vec::new();

        for reader in self.readers(rtxn, db) {
            let reader = reader?;
            let mut searcher = reader.nns(limit, limit * 2); // TODO find better ef
            if let Some(filter) = filter {
                if reader.item_ids().is_disjoint(filter) {
                    continue;
                }
                searcher.candidates(filter);
            }

            results.append(&mut searcher.by_vector(rtxn, vector)?);
        }

        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));

        Ok(results)
    }

    pub fn item_vectors(&self, rtxn: &RoTxn, item_id: u32) -> Result<Vec<Vec<f32>>, hannoy::Error> {
        let mut vectors = Vec::new();

        if self.quantized {
            for reader in self.readers(rtxn, self.quantized_db()) {
                if let Some(vec) = reader?.item_vector(rtxn, item_id)? {
                    vectors.push(vec);
                }
            }
        } else {
            for reader in self.readers(rtxn, self.angular_db()) {
                if let Some(vec) = reader?.item_vector(rtxn, item_id)? {
                    vectors.push(vec);
                }
            }
        }
        Ok(vectors)
    }

    fn angular_db(&self) -> hannoy::Database<Cosine> {
        self.database.remap_data_type()
    }

    fn quantized_db(&self) -> hannoy::Database<BinaryQuantizedCosine> {
        self.database.remap_data_type()
    }

    pub fn aggregate_stats(
        &self,
        rtxn: &RoTxn,
        stats: &mut HannoyStats,
    ) -> Result<(), hannoy::Error> {
        if self.quantized {
            for reader in self.readers(rtxn, self.quantized_db()) {
                let reader = reader?;
                let documents = reader.item_ids();
                if documents.is_empty() {
                    break;
                }
                stats.documents |= documents;
                stats.number_of_embeddings += documents.len();
            }
        } else {
            for reader in self.readers(rtxn, self.angular_db()) {
                let reader = reader?;
                let documents = reader.item_ids();
                if documents.is_empty() {
                    break;
                }
                stats.documents |= documents;
                stats.number_of_embeddings += documents.len();
            }
        }

        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct HannoyStats {
    pub number_of_embeddings: u64,
    pub documents: RoaringBitmap,
}

/// One or multiple embeddings stored consecutively in a flat vector.
#[derive(Debug, PartialEq)]
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

    /// Append a flat vector of embeddings at the end of the embeddings.
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
    /// An embedder composed of an embedder at search time and an embedder at indexing time.
    Composite(composite::Embedder),
}

#[derive(Debug)]
struct EmbeddingCache {
    data: Option<Mutex<lru::LruCache<String, Embedding>>>,
}

impl EmbeddingCache {
    const MAX_TEXT_LEN: usize = 2000;

    pub fn new(cap: usize) -> Self {
        let data = NonZeroUsize::new(cap).map(lru::LruCache::new).map(Mutex::new);
        Self { data }
    }

    /// Get the embedding corresponding to `text`, if any is present in the cache.
    pub fn get(&self, text: &str) -> Option<Embedding> {
        let data = self.data.as_ref()?;
        if text.len() > Self::MAX_TEXT_LEN {
            return None;
        }
        let mut cache = data.lock().unwrap();

        cache.get(text).cloned()
    }

    /// Puts a new embedding for the specified `text`
    pub fn put(&self, text: String, embedding: Embedding) {
        let Some(data) = self.data.as_ref() else {
            return;
        };
        if text.len() > Self::MAX_TEXT_LEN {
            return;
        }
        tracing::trace!(text, "embedding added to cache");

        let mut cache = data.lock().unwrap();

        cache.put(text, embedding);
    }
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

/// Map of runtime embedder data.
#[derive(Clone, Default)]
pub struct RuntimeEmbedders(HashMap<String, Arc<RuntimeEmbedder>>);

pub struct RuntimeEmbedder {
    pub embedder: Arc<Embedder>,
    pub document_template: Prompt,
    fragments: Vec<RuntimeFragment>,
    pub is_quantized: bool,
}

impl RuntimeEmbedder {
    pub fn new(
        embedder: Arc<Embedder>,
        document_template: Prompt,
        mut fragments: Vec<RuntimeFragment>,
        is_quantized: bool,
    ) -> Self {
        fragments.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        Self { embedder, document_template, fragments, is_quantized }
    }

    /// The runtime fragments sorted by name.
    pub fn fragments(&self) -> &[RuntimeFragment] {
        self.fragments.as_slice()
    }
}

pub struct RuntimeFragment {
    pub name: String,
    pub id: u8,
    pub template: JsonTemplate,
}

impl RuntimeEmbedders {
    /// Create the map from its internal component.s
    pub fn new(data: HashMap<String, Arc<RuntimeEmbedder>>) -> Self {
        Self(data)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Get an embedder configuration and template from its name.
    pub fn get(&self, name: &str) -> Option<&Arc<RuntimeEmbedder>> {
        self.0.get(name)
    }

    pub fn inner_as_ref(&self) -> &HashMap<String, Arc<RuntimeEmbedder>> {
        &self.0
    }

    pub fn into_inner(self) -> HashMap<String, Arc<RuntimeEmbedder>> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl IntoIterator for RuntimeEmbedders {
    type Item = (String, Arc<RuntimeEmbedder>);

    type IntoIter = std::collections::hash_map::IntoIter<String, Arc<RuntimeEmbedder>>;

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
    Composite(composite::EmbedderOptions),
}

impl EmbedderOptions {
    pub fn fragment(&self, name: &str) -> Option<&serde_json::Value> {
        match &self {
            EmbedderOptions::HuggingFace(_)
            | EmbedderOptions::OpenAi(_)
            | EmbedderOptions::Ollama(_)
            | EmbedderOptions::UserProvided(_) => None,
            EmbedderOptions::Rest(embedder_options) => {
                embedder_options.indexing_fragments.get(name)
            }
            EmbedderOptions::Composite(embedder_options) => {
                if let SubEmbedderOptions::Rest(embedder_options) = &embedder_options.index {
                    embedder_options.indexing_fragments.get(name)
                } else {
                    None
                }
            }
        }
    }

    pub fn has_fragments(&self) -> bool {
        match &self {
            EmbedderOptions::HuggingFace(_)
            | EmbedderOptions::OpenAi(_)
            | EmbedderOptions::Ollama(_)
            | EmbedderOptions::UserProvided(_) => false,
            EmbedderOptions::Rest(embedder_options) => {
                !embedder_options.indexing_fragments.is_empty()
            }
            EmbedderOptions::Composite(embedder_options) => {
                if let SubEmbedderOptions::Rest(embedder_options) = &embedder_options.index {
                    !embedder_options.indexing_fragments.is_empty()
                } else {
                    false
                }
            }
        }
    }
}

impl Default for EmbedderOptions {
    fn default() -> Self {
        Self::HuggingFace(Default::default())
    }
}

impl Embedder {
    /// Spawns a new embedder built from its options.
    pub fn new(
        options: EmbedderOptions,
        cache_cap: usize,
    ) -> std::result::Result<Self, NewEmbedderError> {
        Ok(match options {
            EmbedderOptions::HuggingFace(options) => {
                Self::HuggingFace(hf::Embedder::new(options, cache_cap)?)
            }
            EmbedderOptions::OpenAi(options) => {
                Self::OpenAi(openai::Embedder::new(options, cache_cap)?)
            }
            EmbedderOptions::Ollama(options) => {
                Self::Ollama(ollama::Embedder::new(options, cache_cap)?)
            }
            EmbedderOptions::UserProvided(options) => {
                Self::UserProvided(manual::Embedder::new(options))
            }
            EmbedderOptions::Rest(options) => Self::Rest(rest::Embedder::new(
                options,
                cache_cap,
                rest::ConfigurationSource::User,
            )?),
            EmbedderOptions::Composite(options) => {
                Self::Composite(composite::Embedder::new(options, cache_cap)?)
            }
        })
    }

    /// Embed in search context

    #[tracing::instrument(level = "debug", skip_all, target = "search")]
    pub fn embed_search(
        &self,
        query: SearchQuery<'_>,
        deadline: Option<Instant>,
    ) -> std::result::Result<Embedding, EmbedError> {
        match query {
            SearchQuery::Text(text) => self.embed_search_text(text, deadline),
            SearchQuery::Media { q, media } => self.embed_search_media(q, media, deadline),
        }
    }

    pub fn embed_search_text(
        &self,
        text: &str,
        deadline: Option<Instant>,
    ) -> std::result::Result<Embedding, EmbedError> {
        if let Some(cache) = self.cache() {
            if let Some(embedding) = cache.get(text) {
                tracing::trace!(text, "embedding found in cache");
                return Ok(embedding);
            }
        }
        let embedding = match self {
            Embedder::HuggingFace(embedder) => embedder.embed_one(text),
            Embedder::OpenAi(embedder) => embedder
                .embed(&[text], deadline, None)?
                .pop()
                .ok_or_else(EmbedError::missing_embedding),
            Embedder::Ollama(embedder) => embedder
                .embed(&[text], deadline, None)?
                .pop()
                .ok_or_else(EmbedError::missing_embedding),
            Embedder::UserProvided(embedder) => embedder.embed_one(text),
            Embedder::Rest(embedder) => embedder.embed_one(SearchQuery::Text(text), deadline, None),
            Embedder::Composite(embedder) => embedder.search.embed_one(text, deadline, None),
        }?;

        if let Some(cache) = self.cache() {
            cache.put(text.to_owned(), embedding.clone());
        }

        Ok(embedding)
    }

    pub fn embed_search_media(
        &self,
        q: Option<&str>,
        media: Option<&serde_json::Value>,
        deadline: Option<Instant>,
    ) -> std::result::Result<Embedding, EmbedError> {
        let Embedder::Rest(embedder) = self else {
            return Err(EmbedError::rest_media_not_a_rest());
        };
        embedder.embed_one(SearchQuery::Media { q, media }, deadline, None)
    }

    /// Embed multiple chunks of texts.
    ///
    /// Each chunk is composed of one or multiple texts.
    pub fn embed_index(
        &self,
        text_chunks: Vec<Vec<String>>,
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> std::result::Result<Vec<Vec<Embedding>>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed_index(text_chunks),
            Embedder::OpenAi(embedder) => {
                embedder.embed_index(text_chunks, threads, embedder_stats)
            }
            Embedder::Ollama(embedder) => {
                embedder.embed_index(text_chunks, threads, embedder_stats)
            }
            Embedder::UserProvided(embedder) => embedder.embed_index(text_chunks),
            Embedder::Rest(embedder) => embedder.embed_index(text_chunks, threads, embedder_stats),
            Embedder::Composite(embedder) => {
                embedder.index.embed_index(text_chunks, threads, embedder_stats)
            }
        }
    }

    /// Non-owning variant of [`Self::embed_index`].
    pub fn embed_index_ref(
        &self,
        texts: &[&str],
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> std::result::Result<Vec<Embedding>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed_index_ref(texts),
            Embedder::OpenAi(embedder) => embedder.embed_index_ref(texts, threads, embedder_stats),
            Embedder::Ollama(embedder) => embedder.embed_index_ref(texts, threads, embedder_stats),
            Embedder::UserProvided(embedder) => embedder.embed_index_ref(texts),
            Embedder::Rest(embedder) => embedder.embed_index_ref(texts, threads, embedder_stats),
            Embedder::Composite(embedder) => {
                embedder.index.embed_index_ref(texts, threads, embedder_stats)
            }
        }
    }

    pub fn embed_index_ref_fragments(
        &self,
        fragments: &[serde_json::Value],
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> std::result::Result<Vec<Embedding>, EmbedError> {
        if let Embedder::Rest(embedder) = self {
            embedder.embed_index_ref(fragments, threads, embedder_stats)
        } else {
            let Embedder::Composite(embedder) = self else {
                unimplemented!("embedding fragments is only available for rest embedders")
            };
            let crate::vector::composite::SubEmbedder::Rest(embedder) = &embedder.index else {
                unimplemented!("embedding fragments is only available for rest embedders")
            };

            embedder.embed_index_ref(fragments, threads, embedder_stats)
        }
    }

    /// Indicates the preferred number of chunks to pass to [`Self::embed_chunks`]
    pub fn chunk_count_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.chunk_count_hint(),
            Embedder::OpenAi(embedder) => embedder.chunk_count_hint(),
            Embedder::Ollama(embedder) => embedder.chunk_count_hint(),
            Embedder::UserProvided(_) => 100,
            Embedder::Rest(embedder) => embedder.chunk_count_hint(),
            Embedder::Composite(embedder) => embedder.index.chunk_count_hint(),
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
            Embedder::Composite(embedder) => embedder.index.prompt_count_in_chunk_hint(),
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
            Embedder::Composite(embedder) => embedder.dimensions(),
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
            Embedder::Composite(embedder) => embedder.distribution(),
        }
    }

    pub fn uses_document_template(&self) -> bool {
        match self {
            Embedder::HuggingFace(_)
            | Embedder::OpenAi(_)
            | Embedder::Ollama(_)
            | Embedder::Rest(_) => true,
            Embedder::UserProvided(_) => false,
            Embedder::Composite(embedder) => embedder.index.uses_document_template(),
        }
    }

    fn cache(&self) -> Option<&EmbeddingCache> {
        match self {
            Embedder::HuggingFace(embedder) => Some(embedder.cache()),
            Embedder::OpenAi(embedder) => Some(embedder.cache()),
            Embedder::UserProvided(_) => None,
            Embedder::Ollama(embedder) => Some(embedder.cache()),
            Embedder::Rest(embedder) => Some(embedder.cache()),
            Embedder::Composite(embedder) => embedder.search.cache(),
        }
    }
}

#[derive(Clone, Copy)]
pub enum SearchQuery<'a> {
    Text(&'a str),
    Media { q: Option<&'a str>, media: Option<&'a serde_json::Value> },
}

/// Describes the mean and sigma of distribution of embedding similarity in the embedding space.
///
/// The intended use is to make the similarity score more comparable to the regular ranking score.
/// This allows to correct effects where results are too "packed" around a certain value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize, ToSchema)]
#[serde(from = "DistributionShiftSerializable")]
#[serde(into = "DistributionShiftSerializable")]
pub struct DistributionShift {
    /// Value where the results are "packed".
    ///
    /// Similarity scores are translated so that they are packed around 0.5 instead
    #[schema(value_type = f32)]
    pub current_mean: OrderedFloat<f32>,

    /// standard deviation of a similarity score.
    ///
    /// Set below 0.4 to make the results less packed around the mean, and above 0.4 to make them more packed.
    #[schema(value_type = f32)]
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

fn hannoy_store_range_for_embedder(embedder_id: u8) -> impl Iterator<Item = u16> {
    (0..=u8::MAX).map(move |store_id| hannoy_store_for_embedder(embedder_id, store_id))
}

fn hannoy_store_for_embedder(embedder_id: u8, store_id: u8) -> u16 {
    let embedder_id = (embedder_id as u16) << 8;
    embedder_id | (store_id as u16)
}
