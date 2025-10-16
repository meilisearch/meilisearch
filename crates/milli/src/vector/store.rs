use hannoy::distances::{Cosine, Hamming};
use hannoy::ItemId;
use heed::{RoTxn, RwTxn, Unspecified};
use ordered_float::OrderedFloat;
use rand::SeedableRng as _;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::progress::Progress;
use crate::vector::Embeddings;
use crate::TimeBudget;

const HANNOY_EF_CONSTRUCTION: usize = 125;
const HANNOY_M: usize = 16;
const HANNOY_M0: usize = 32;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    deserr::Deserr,
    utoipa::ToSchema,
)]
pub enum VectorStoreBackend {
    #[default]
    #[deserr(rename = "stable")]
    #[serde(rename = "stable")]
    Arroy,
    #[deserr(rename = "experimental")]
    #[serde(rename = "experimental")]
    Hannoy,
}

pub struct VectorStore {
    backend: VectorStoreBackend,
    database: hannoy::Database<Unspecified>,
    embedder_index: u8,
    quantized: bool,
}

impl VectorStore {
    // backend-independent public functions

    pub fn new(
        backend: VectorStoreBackend,
        database: hannoy::Database<Unspecified>,
        embedder_index: u8,
        quantized: bool,
    ) -> Self {
        Self { backend, database, embedder_index, quantized }
    }

    pub fn embedder_index(&self) -> u8 {
        self.embedder_index
    }

    // backend-dependent public functions

    /// The item ids that are present in the store specified by its id.
    ///
    /// The ids are accessed via a lambda to avoid lifetime shenanigans.
    pub fn items_in_store<F, O>(
        &self,
        rtxn: &RoTxn,
        store_id: u8,
        with_items: F,
    ) -> crate::Result<O>
    where
        F: FnOnce(&RoaringBitmap) -> O,
    {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_items_in_store(rtxn, self._arroy_quantized_db(), store_id, with_items)
                    .map_err(Into::into)
            } else {
                self._arroy_items_in_store(rtxn, self._arroy_angular_db(), store_id, with_items)
                    .map_err(Into::into)
            }
        } else if self.quantized {
            self._hannoy_items_in_store(rtxn, self._hannoy_quantized_db(), store_id, with_items)
                .map_err(Into::into)
        } else {
            self._hannoy_items_in_store(rtxn, self._hannoy_angular_db(), store_id, with_items)
                .map_err(Into::into)
        }
    }

    pub fn dimensions(&self, rtxn: &RoTxn) -> crate::Result<Option<usize>> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                Ok(self
                    ._arroy_readers(rtxn, self._arroy_quantized_db())
                    .next()
                    .transpose()?
                    .map(|reader| reader.dimensions()))
            } else {
                Ok(self
                    ._arroy_readers(rtxn, self._arroy_angular_db())
                    .next()
                    .transpose()?
                    .map(|reader| reader.dimensions()))
            }
        } else if self.quantized {
            Ok(self
                ._hannoy_readers(rtxn, self._hannoy_quantized_db())
                .next()
                .transpose()?
                .map(|reader| reader.dimensions()))
        } else {
            Ok(self
                ._hannoy_readers(rtxn, self._hannoy_angular_db())
                .next()
                .transpose()?
                .map(|reader| reader.dimensions()))
        }
    }

    pub fn change_backend<MSP>(
        self,
        rtxn: &RoTxn,
        wtxn: &mut RwTxn,
        progress: Progress,
        must_stop_processing: &MSP,
        available_memory: Option<usize>,
    ) -> crate::Result<()>
    where
        MSP: Fn() -> bool + Sync,
    {
        let mut rng = rand::rngs::StdRng::from_entropy();
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_to_hannoy_bq::<arroy::distances::BinaryQuantizedCosine, hannoy::distances::Hamming, _>(rtxn, wtxn, &progress, &mut rng, &must_stop_processing)
            } else {
                let dimensions = self
                    ._arroy_readers(wtxn, self._arroy_angular_db())
                    .next()
                    .transpose()?
                    .map(|reader| reader.dimensions());

                let Some(dimensions) = dimensions else { return Ok(()) };

                for index in vector_store_range_for_embedder(self.embedder_index) {
                    let writer = hannoy::Writer::new(self._hannoy_angular_db(), index, dimensions);
                    let mut builder = writer.builder(&mut rng).progress(progress.clone());
                    builder.cancel(must_stop_processing);
                    builder.prepare_arroy_conversion(wtxn)?;
                    builder.build::<HANNOY_M, HANNOY_M0>(wtxn)?;
                }

                Ok(())
            }
        } else if self.quantized {
            self._hannoy_to_arroy_bq::<
                hannoy::distances::Hamming,
                arroy::distances::BinaryQuantizedCosine,
                _>(rtxn, wtxn, &progress, &mut rng, available_memory, &must_stop_processing)
        } else {
            let dimensions = self
                ._hannoy_readers(wtxn, self._hannoy_angular_db())
                .next()
                .transpose()?
                .map(|reader| reader.dimensions());

            let Some(dimensions) = dimensions else { return Ok(()) };

            for index in vector_store_range_for_embedder(self.embedder_index) {
                let writer = arroy::Writer::new(self._arroy_angular_db(), index, dimensions);
                let mut builder = writer.builder(&mut rng);
                let builder = builder.progress(|step| progress.update_progress_from_arroy(step));
                builder.prepare_hannoy_conversion(wtxn)?;
                builder.build(wtxn)?;
            }

            Ok(())
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_and_quantize<R: rand::Rng + rand::SeedableRng>(
        &mut self,
        wtxn: &mut RwTxn,
        progress: Progress,
        rng: &mut R,
        dimension: usize,
        quantizing: bool,
        available_memory: Option<usize>,
        cancel: &(impl Fn() -> bool + Sync + Send),
    ) -> Result<(), crate::Error> {
        for index in vector_store_range_for_embedder(self.embedder_index) {
            if self.backend == VectorStoreBackend::Arroy {
                if self.quantized {
                    let writer = arroy::Writer::new(self._arroy_quantized_db(), index, dimension);
                    if writer.need_build(wtxn)? {
                        arroy_build(wtxn, &progress, rng, available_memory, cancel, &writer)?;
                    } else if writer.is_empty(wtxn)? {
                        continue;
                    }
                } else {
                    let writer = arroy::Writer::new(self._arroy_angular_db(), index, dimension);
                    // If we are quantizing the databases, we can't know from meilisearch
                    // if the db was empty but still contained the wrong metadata, thus we need
                    // to quantize everything and can't stop early. Since this operation can
                    // only happens once in the life of an embedder, it's not very performance
                    // sensitive.
                    if quantizing && !self.quantized {
                        let writer = writer
                            .prepare_changing_distance::<arroy::distances::BinaryQuantizedCosine>(
                                wtxn,
                            )?;
                        arroy_build(wtxn, &progress, rng, available_memory, cancel, &writer)?;
                    } else if writer.need_build(wtxn)? {
                        arroy_build(wtxn, &progress, rng, available_memory, cancel, &writer)?;
                    } else if writer.is_empty(wtxn)? {
                        continue;
                    }
                }
            } else if self.quantized {
                let writer = hannoy::Writer::new(self._hannoy_quantized_db(), index, dimension);
                if writer.need_build(wtxn)? {
                    hannoy_build(wtxn, &progress, rng, cancel, &writer)?;
                } else if writer.is_empty(wtxn)? {
                    continue;
                }
            } else {
                let writer = hannoy::Writer::new(self._hannoy_angular_db(), index, dimension);
                // If we are quantizing the databases, we can't know from meilisearch
                // if the db was empty but still contained the wrong metadata, thus we need
                // to quantize everything and can't stop early. Since this operation can
                // only happens once in the life of an embedder, it's not very performance
                // sensitive.
                if quantizing && !self.quantized {
                    let writer = writer.prepare_changing_distance::<Hamming>(wtxn)?;
                    hannoy_build(wtxn, &progress, rng, cancel, &writer)?;
                } else if writer.need_build(wtxn)? {
                    hannoy_build(wtxn, &progress, rng, cancel, &writer)?;
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
    ) -> Result<(), crate::Error> {
        let dimension = embeddings.dimension();
        for (index, vector) in
            vector_store_range_for_embedder(self.embedder_index).zip(embeddings.iter())
        {
            if self.backend == VectorStoreBackend::Arroy {
                if self.quantized {
                    arroy::Writer::new(self._arroy_quantized_db(), index, dimension)
                        .add_item(wtxn, item_id, vector)?
                } else {
                    arroy::Writer::new(self._arroy_angular_db(), index, dimension)
                        .add_item(wtxn, item_id, vector)?
                }
            } else if self.quantized {
                hannoy::Writer::new(self._hannoy_quantized_db(), index, dimension)
                    .add_item(wtxn, item_id, vector)?
            } else {
                hannoy::Writer::new(self._hannoy_angular_db(), index, dimension)
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
    ) -> Result<(), crate::Error> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_add_item(wtxn, self._arroy_quantized_db(), item_id, vector)
                    .map_err(Into::into)
            } else {
                self._arroy_add_item(wtxn, self._arroy_angular_db(), item_id, vector)
                    .map_err(Into::into)
            }
        } else if self.quantized {
            self._hannoy_add_item(wtxn, self._hannoy_quantized_db(), item_id, vector)
                .map_err(Into::into)
        } else {
            self._hannoy_add_item(wtxn, self._hannoy_angular_db(), item_id, vector)
                .map_err(Into::into)
        }
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
    ) -> Result<(), crate::Error> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_add_item_in_store(
                    wtxn,
                    self._arroy_quantized_db(),
                    item_id,
                    store_id,
                    vector,
                )
                .map_err(Into::into)
            } else {
                self._arroy_add_item_in_store(
                    wtxn,
                    self._arroy_angular_db(),
                    item_id,
                    store_id,
                    vector,
                )
                .map_err(Into::into)
            }
        } else if self.quantized {
            self._hannoy_add_item_in_store(
                wtxn,
                self._hannoy_quantized_db(),
                item_id,
                store_id,
                vector,
            )
            .map_err(Into::into)
        } else {
            self._hannoy_add_item_in_store(
                wtxn,
                self._hannoy_angular_db(),
                item_id,
                store_id,
                vector,
            )
            .map_err(Into::into)
        }
    }

    /// Delete one item from its value.
    pub fn del_item(
        &self,
        wtxn: &mut RwTxn,
        item_id: hannoy::ItemId,
        vector: &[f32],
    ) -> Result<bool, crate::Error> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_del_item(wtxn, self._arroy_quantized_db(), item_id, vector)
                    .map_err(Into::into)
            } else {
                self._arroy_del_item(wtxn, self._arroy_angular_db(), item_id, vector)
                    .map_err(Into::into)
            }
        } else if self.quantized {
            self._hannoy_del_item(wtxn, self._hannoy_quantized_db(), item_id, vector)
                .map_err(Into::into)
        } else {
            self._hannoy_del_item(wtxn, self._hannoy_angular_db(), item_id, vector)
                .map_err(Into::into)
        }
    }

    /// Delete all embeddings from a specific `item_id`
    pub fn del_items(
        &self,
        wtxn: &mut RwTxn,
        dimension: usize,
        item_id: hannoy::ItemId,
    ) -> Result<(), crate::Error> {
        for index in vector_store_range_for_embedder(self.embedder_index) {
            if self.backend == VectorStoreBackend::Arroy {
                if self.quantized {
                    let writer = arroy::Writer::new(self._arroy_quantized_db(), index, dimension);
                    writer.del_item(wtxn, item_id)?;
                } else {
                    let writer = arroy::Writer::new(self._arroy_angular_db(), index, dimension);
                    writer.del_item(wtxn, item_id)?;
                }
            } else if self.quantized {
                let writer = hannoy::Writer::new(self._hannoy_quantized_db(), index, dimension);
                writer.del_item(wtxn, item_id)?;
            } else {
                let writer = hannoy::Writer::new(self._hannoy_angular_db(), index, dimension);
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
    ) -> Result<bool, crate::Error> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_del_item_in_store(
                    wtxn,
                    self._arroy_quantized_db(),
                    item_id,
                    store_id,
                    dimensions,
                )
                .map_err(Into::into)
            } else {
                self._arroy_del_item_in_store(
                    wtxn,
                    self._arroy_angular_db(),
                    item_id,
                    store_id,
                    dimensions,
                )
                .map_err(Into::into)
            }
        } else if self.quantized {
            self._hannoy_del_item_in_store(
                wtxn,
                self._hannoy_quantized_db(),
                item_id,
                store_id,
                dimensions,
            )
            .map_err(Into::into)
        } else {
            self._hannoy_del_item_in_store(
                wtxn,
                self._hannoy_angular_db(),
                item_id,
                store_id,
                dimensions,
            )
            .map_err(Into::into)
        }
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
    ) -> Result<(), crate::Error> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_clear_store(wtxn, self._arroy_quantized_db(), store_id, dimensions)
                    .map_err(Into::into)
            } else {
                self._arroy_clear_store(wtxn, self._arroy_angular_db(), store_id, dimensions)
                    .map_err(Into::into)
            }
        } else if self.quantized {
            self._hannoy_clear_store(wtxn, self._hannoy_quantized_db(), store_id, dimensions)
                .map_err(Into::into)
        } else {
            self._hannoy_clear_store(wtxn, self._hannoy_angular_db(), store_id, dimensions)
                .map_err(Into::into)
        }
    }

    pub fn clear(&self, wtxn: &mut RwTxn, dimension: usize) -> Result<(), crate::Error> {
        for index in vector_store_range_for_embedder(self.embedder_index) {
            if self.backend == VectorStoreBackend::Arroy {
                if self.quantized {
                    let writer = arroy::Writer::new(self._arroy_quantized_db(), index, dimension);
                    if writer.is_empty(wtxn)? {
                        continue;
                    }
                    writer.clear(wtxn)?;
                } else {
                    let writer = arroy::Writer::new(self._arroy_angular_db(), index, dimension);
                    if writer.is_empty(wtxn)? {
                        continue;
                    }
                    writer.clear(wtxn)?;
                }
            } else if self.quantized {
                let writer = hannoy::Writer::new(self._hannoy_quantized_db(), index, dimension);
                if writer.is_empty(wtxn)? {
                    continue;
                }
                writer.clear(wtxn)?;
            } else {
                let writer = hannoy::Writer::new(self._hannoy_angular_db(), index, dimension);
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
    ) -> crate::Result<bool> {
        for index in vector_store_range_for_embedder(self.embedder_index) {
            let contains = if self.backend == VectorStoreBackend::Arroy {
                if self.quantized {
                    let writer = arroy::Writer::new(self._arroy_quantized_db(), index, dimension);
                    if writer.is_empty(rtxn)? {
                        continue;
                    }
                    writer.contains_item(rtxn, item)?
                } else {
                    let writer = arroy::Writer::new(self._arroy_angular_db(), index, dimension);
                    if writer.is_empty(rtxn)? {
                        continue;
                    }
                    writer.contains_item(rtxn, item)?
                }
            } else if self.quantized {
                let writer = hannoy::Writer::new(self._hannoy_quantized_db(), index, dimension);
                if writer.is_empty(rtxn)? {
                    continue;
                }
                writer.contains_item(rtxn, item)?
            } else {
                let writer = hannoy::Writer::new(self._hannoy_angular_db(), index, dimension);
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
    ) -> crate::Result<Vec<(ItemId, f32)>> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_nns_by_item(rtxn, self._arroy_quantized_db(), item, limit, filter)
                    .map_err(Into::into)
            } else {
                self._arroy_nns_by_item(rtxn, self._arroy_angular_db(), item, limit, filter)
                    .map_err(Into::into)
            }
        } else if self.quantized {
            self._hannoy_nns_by_item(rtxn, self._hannoy_quantized_db(), item, limit, filter)
                .map_err(Into::into)
        } else {
            self._hannoy_nns_by_item(rtxn, self._hannoy_angular_db(), item, limit, filter)
                .map_err(Into::into)
        }
    }
    pub fn nns_by_vector(
        &self,
        rtxn: &RoTxn,
        vector: &[f32],
        limit: usize,
        filter: Option<&RoaringBitmap>,
        time_budget: &TimeBudget,
    ) -> crate::Result<Vec<(ItemId, f32)>> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_nns_by_vector(rtxn, self._arroy_quantized_db(), vector, limit, filter)
                    .map_err(Into::into)
            } else {
                self._arroy_nns_by_vector(rtxn, self._arroy_angular_db(), vector, limit, filter)
                    .map_err(Into::into)
            }
        } else if self.quantized {
            self._hannoy_nns_by_vector(
                rtxn,
                self._hannoy_quantized_db(),
                vector,
                limit,
                filter,
                time_budget,
            )
            .map_err(Into::into)
        } else {
            self._hannoy_nns_by_vector(
                rtxn,
                self._hannoy_angular_db(),
                vector,
                limit,
                filter,
                time_budget,
            )
            .map_err(Into::into)
        }
    }
    pub fn item_vectors(&self, rtxn: &RoTxn, item_id: u32) -> crate::Result<Vec<Vec<f32>>> {
        let mut vectors = Vec::new();

        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                for reader in self._arroy_readers(rtxn, self._arroy_quantized_db()) {
                    if let Some(vec) = reader?.item_vector(rtxn, item_id)? {
                        vectors.push(vec);
                    }
                }
            } else {
                for reader in self._arroy_readers(rtxn, self._arroy_angular_db()) {
                    if let Some(vec) = reader?.item_vector(rtxn, item_id)? {
                        vectors.push(vec);
                    }
                }
            }
        } else if self.quantized {
            for reader in self._hannoy_readers(rtxn, self._hannoy_quantized_db()) {
                if let Some(vec) = reader?.item_vector(rtxn, item_id)? {
                    vectors.push(vec);
                }
            }
        } else {
            for reader in self._hannoy_readers(rtxn, self._hannoy_angular_db()) {
                if let Some(vec) = reader?.item_vector(rtxn, item_id)? {
                    vectors.push(vec);
                }
            }
        }

        Ok(vectors)
    }

    pub fn aggregate_stats(
        &self,
        rtxn: &RoTxn,
        stats: &mut VectorStoreStats,
    ) -> Result<(), crate::Error> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                for reader in self._arroy_readers(rtxn, self._arroy_quantized_db()) {
                    let reader = reader?;
                    let documents = reader.item_ids();
                    stats.documents |= documents;
                    stats.number_of_embeddings += documents.len();
                }
            } else {
                for reader in self._arroy_readers(rtxn, self._arroy_angular_db()) {
                    let reader = reader?;
                    let documents = reader.item_ids();
                    stats.documents |= documents;
                    stats.number_of_embeddings += documents.len();
                }
            }
        } else if self.quantized {
            for reader in self._hannoy_readers(rtxn, self._hannoy_quantized_db()) {
                let reader = reader?;
                let documents = reader.item_ids();
                stats.documents |= documents;
                stats.number_of_embeddings += documents.len();
            }
        } else {
            for reader in self._hannoy_readers(rtxn, self._hannoy_angular_db()) {
                let reader = reader?;
                let documents = reader.item_ids();
                stats.documents |= documents;
                stats.number_of_embeddings += documents.len();
            }
        }

        Ok(())
    }

    // private functions
    fn _arroy_readers<'a, D: arroy::Distance>(
        &'a self,
        rtxn: &'a RoTxn<'a>,
        db: arroy::Database<D>,
    ) -> impl Iterator<Item = Result<arroy::Reader<'a, D>, arroy::Error>> + 'a {
        vector_store_range_for_embedder(self.embedder_index).filter_map(move |index| {
            match arroy::Reader::open(rtxn, index, db) {
                Ok(reader) => match reader.is_empty(rtxn) {
                    Ok(false) => Some(Ok(reader)),
                    Ok(true) => None,
                    Err(e) => Some(Err(e)),
                },
                Err(arroy::Error::MissingMetadata(_)) => None,
                Err(e) => Some(Err(e)),
            }
        })
    }

    fn _hannoy_readers<'a, D: hannoy::Distance>(
        &'a self,
        rtxn: &'a RoTxn<'a>,
        db: hannoy::Database<D>,
    ) -> impl Iterator<Item = Result<hannoy::Reader<D>, hannoy::Error>> + 'a {
        vector_store_range_for_embedder(self.embedder_index).filter_map(move |index| {
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

    fn _arroy_items_in_store<D: arroy::Distance, F, O>(
        &self,
        rtxn: &RoTxn,
        db: arroy::Database<D>,
        store_id: u8,
        with_items: F,
    ) -> Result<O, arroy::Error>
    where
        F: FnOnce(&RoaringBitmap) -> O,
    {
        let index = vector_store_for_embedder(self.embedder_index, store_id);
        let reader = arroy::Reader::open(rtxn, index, db);
        match reader {
            Ok(reader) => Ok(with_items(reader.item_ids())),
            Err(arroy::Error::MissingMetadata(_)) => Ok(with_items(&RoaringBitmap::new())),
            Err(err) => Err(err),
        }
    }

    fn _hannoy_items_in_store<D: hannoy::Distance, F, O>(
        &self,
        rtxn: &RoTxn,
        db: hannoy::Database<D>,
        store_id: u8,
        with_items: F,
    ) -> Result<O, hannoy::Error>
    where
        F: FnOnce(&RoaringBitmap) -> O,
    {
        let index = vector_store_for_embedder(self.embedder_index, store_id);
        let reader = hannoy::Reader::open(rtxn, index, db);
        match reader {
            Ok(reader) => Ok(with_items(reader.item_ids())),
            Err(hannoy::Error::MissingMetadata(_)) => Ok(with_items(&RoaringBitmap::new())),
            Err(err) => Err(err),
        }
    }

    fn _arroy_add_item<D: arroy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: arroy::Database<D>,
        item_id: arroy::ItemId,
        vector: &[f32],
    ) -> Result<(), arroy::Error> {
        let dimension = vector.len();

        for index in vector_store_range_for_embedder(self.embedder_index) {
            let writer = arroy::Writer::new(db, index, dimension);
            if !writer.contains_item(wtxn, item_id)? {
                writer.add_item(wtxn, item_id, vector)?;
                break;
            }
        }
        Ok(())
    }

    fn _hannoy_add_item<D: hannoy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: hannoy::Database<D>,
        item_id: hannoy::ItemId,
        vector: &[f32],
    ) -> Result<(), hannoy::Error> {
        let dimension = vector.len();

        for index in vector_store_range_for_embedder(self.embedder_index) {
            let writer = hannoy::Writer::new(db, index, dimension);
            if !writer.contains_item(wtxn, item_id)? {
                writer.add_item(wtxn, item_id, vector)?;
                break;
            }
        }
        Ok(())
    }

    fn _arroy_add_item_in_store<D: arroy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: arroy::Database<D>,
        item_id: arroy::ItemId,
        store_id: u8,
        vector: &[f32],
    ) -> Result<(), arroy::Error> {
        let dimension = vector.len();

        let index = vector_store_for_embedder(self.embedder_index, store_id);
        let writer = arroy::Writer::new(db, index, dimension);
        writer.add_item(wtxn, item_id, vector)
    }

    fn _hannoy_add_item_in_store<D: hannoy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: hannoy::Database<D>,
        item_id: hannoy::ItemId,
        store_id: u8,
        vector: &[f32],
    ) -> Result<(), hannoy::Error> {
        let dimension = vector.len();

        let index = vector_store_for_embedder(self.embedder_index, store_id);
        let writer = hannoy::Writer::new(db, index, dimension);
        writer.add_item(wtxn, item_id, vector)
    }

    fn _arroy_del_item_in_store<D: arroy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: arroy::Database<D>,
        item_id: arroy::ItemId,
        store_id: u8,
        dimensions: usize,
    ) -> Result<bool, arroy::Error> {
        let index = vector_store_for_embedder(self.embedder_index, store_id);
        let writer = arroy::Writer::new(db, index, dimensions);
        writer.del_item(wtxn, item_id)
    }

    fn _hannoy_del_item_in_store<D: hannoy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: hannoy::Database<D>,
        item_id: hannoy::ItemId,
        store_id: u8,
        dimensions: usize,
    ) -> Result<bool, hannoy::Error> {
        let index = vector_store_for_embedder(self.embedder_index, store_id);
        let writer = hannoy::Writer::new(db, index, dimensions);
        writer.del_item(wtxn, item_id)
    }

    fn _arroy_clear_store<D: arroy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: arroy::Database<D>,
        store_id: u8,
        dimensions: usize,
    ) -> Result<(), arroy::Error> {
        let index = vector_store_for_embedder(self.embedder_index, store_id);
        let writer = arroy::Writer::new(db, index, dimensions);
        writer.clear(wtxn)
    }

    fn _hannoy_clear_store<D: hannoy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: hannoy::Database<D>,
        store_id: u8,
        dimensions: usize,
    ) -> Result<(), hannoy::Error> {
        let index = vector_store_for_embedder(self.embedder_index, store_id);
        let writer = hannoy::Writer::new(db, index, dimensions);
        writer.clear(wtxn)
    }

    fn _arroy_del_item<D: arroy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: arroy::Database<D>,
        item_id: arroy::ItemId,
        vector: &[f32],
    ) -> Result<bool, arroy::Error> {
        let dimension = vector.len();

        for index in vector_store_range_for_embedder(self.embedder_index) {
            let writer = arroy::Writer::new(db, index, dimension);
            if writer.contains_item(wtxn, item_id)? {
                return writer.del_item(wtxn, item_id);
            }
        }
        Ok(false)
    }

    fn _hannoy_del_item<D: hannoy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: hannoy::Database<D>,
        item_id: hannoy::ItemId,
        vector: &[f32],
    ) -> Result<bool, hannoy::Error> {
        let dimension = vector.len();

        for index in vector_store_range_for_embedder(self.embedder_index) {
            let writer = hannoy::Writer::new(db, index, dimension);
            if writer.contains_item(wtxn, item_id)? {
                return writer.del_item(wtxn, item_id);
            }
        }
        Ok(false)
    }

    fn _arroy_nns_by_item<D: arroy::Distance>(
        &self,
        rtxn: &RoTxn,
        db: arroy::Database<D>,
        item: ItemId,
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, arroy::Error> {
        let mut results = Vec::new();

        for reader in self._arroy_readers(rtxn, db) {
            let reader = reader?;
            let mut searcher = reader.nns(limit);
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

    fn _hannoy_nns_by_item<D: hannoy::Distance>(
        &self,
        rtxn: &RoTxn,
        db: hannoy::Database<D>,
        item: ItemId,
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, hannoy::Error> {
        let mut results = Vec::new();

        for reader in self._hannoy_readers(rtxn, db) {
            let reader = reader?;
            let mut searcher = reader.nns(limit);
            searcher.ef_search((limit * 10).max(100)); // TODO find better ef
            if let Some(filter) = filter {
                searcher.candidates(filter);
            }

            if let Some(mut ret) = searcher.by_item(rtxn, item)? {
                results.append(&mut ret);
            }
        }
        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));
        Ok(results)
    }

    fn _arroy_nns_by_vector<D: arroy::Distance>(
        &self,
        rtxn: &RoTxn,
        db: arroy::Database<D>,
        vector: &[f32],
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, arroy::Error> {
        let mut results = Vec::new();

        for reader in self._arroy_readers(rtxn, db) {
            let reader = reader?;
            let mut searcher = reader.nns(limit);
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

    fn _hannoy_nns_by_vector<D: hannoy::Distance>(
        &self,
        rtxn: &RoTxn,
        db: hannoy::Database<D>,
        vector: &[f32],
        limit: usize,
        filter: Option<&RoaringBitmap>,
        time_budget: &TimeBudget,
    ) -> Result<Vec<(ItemId, f32)>, hannoy::Error> {
        let mut results = Vec::new();

        for reader in self._hannoy_readers(rtxn, db) {
            let reader = reader?;
            let mut searcher = reader.nns(limit);
            searcher.ef_search((limit * 10).max(100)); // TODO find better ef
            if let Some(filter) = filter {
                searcher.candidates(filter);
            }

            let (res, _degraded) =
                &mut searcher
                    .by_vector_with_cancellation(rtxn, vector, || time_budget.exceeded())?;
            results.append(res);
        }

        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));

        Ok(results)
    }

    fn _arroy_angular_db(&self) -> arroy::Database<arroy::distances::Cosine> {
        self.database.remap_types()
    }

    fn _arroy_quantized_db(&self) -> arroy::Database<arroy::distances::BinaryQuantizedCosine> {
        self.database.remap_types()
    }

    fn _hannoy_angular_db(&self) -> hannoy::Database<Cosine> {
        self.database.remap_data_type()
    }

    fn _hannoy_quantized_db(&self) -> hannoy::Database<Hamming> {
        self.database.remap_data_type()
    }

    fn _arroy_to_hannoy_bq<AD: arroy::Distance, HD: hannoy::Distance, R>(
        self,
        arroy_rtxn: &RoTxn,
        hannoy_wtxn: &mut RwTxn,
        progress: &Progress,
        rng: &mut R,
        cancel: &(impl Fn() -> bool + Sync + Send),
    ) -> crate::Result<()>
    where
        R: rand::Rng + rand::SeedableRng,
    {
        // No work if distances are the same
        if AD::name() == HD::name() {
            return Ok(());
        }
        for index in vector_store_range_for_embedder(self.embedder_index) {
            let arroy_reader: arroy::Reader<AD> =
                match arroy::Reader::open(arroy_rtxn, index, self.database.remap_types()) {
                    Ok(reader) => reader,
                    Err(arroy::Error::MissingMetadata(_)) => continue,
                    Err(err) => return Err(err.into()),
                };
            let dimensions = arroy_reader.dimensions();
            let hannoy_writer: hannoy::Writer<HD> =
                hannoy::Writer::new(self.database.remap_types(), index, dimensions);
            // Since the bq mode of arroy and hannoy are not compatible, we have to clear and re-insert everything
            hannoy_writer.clear(hannoy_wtxn)?;
            for entry in arroy_reader.iter(arroy_rtxn)? {
                let (item, mut vector) = entry?;
                // arroy bug? the `vector` here can be longer than `dimensions`.
                // workaround: truncating.
                if vector.len() > dimensions {
                    vector.truncate(dimensions);
                }
                hannoy_writer.add_item(hannoy_wtxn, item, &vector)?;
            }
            hannoy_build(hannoy_wtxn, progress, rng, cancel, &hannoy_writer)?;
        }
        Ok(())
    }

    fn _hannoy_to_arroy_bq<HD: hannoy::Distance, AD: arroy::Distance, R>(
        self,
        hannoy_rtxn: &RoTxn,
        arroy_wtxn: &mut RwTxn,
        progress: &Progress,
        rng: &mut R,
        available_memory: Option<usize>,
        cancel: &(impl Fn() -> bool + Sync + Send),
    ) -> crate::Result<()>
    where
        R: rand::Rng + rand::SeedableRng,
    {
        // No work if distances are the same
        if AD::name() == HD::name() {
            return Ok(());
        }
        for index in vector_store_range_for_embedder(self.embedder_index) {
            let hannoy_reader: hannoy::Reader<HD> =
                match hannoy::Reader::open(hannoy_rtxn, index, self.database.remap_types()) {
                    Ok(reader) => reader,
                    Err(hannoy::Error::MissingMetadata(_)) => continue,
                    Err(err) => return Err(err.into()),
                };
            let dimensions = hannoy_reader.dimensions();
            let arroy_writer: arroy::Writer<AD> =
                arroy::Writer::new(self.database.remap_types(), index, dimensions);
            // Since the bq mode of arroy and hannoy are not compatible, we have to clear and re-insert everything
            arroy_writer.clear(arroy_wtxn)?;
            for entry in hannoy_reader.iter(hannoy_rtxn)? {
                let (item, mut vector) = entry?;
                debug_assert!(vector.len() == dimensions);
                // arroy and hannoy disagreement over the 0 value if distance is Hamming
                // - arroy does:
                //     - if x >= 0 => 1
                //     - if x < 0 => -1
                // - hannoy does:
                //     - if x > 0 => 1
                //     - if x <= 0 => 0
                // because of this, a 0 from a bq hannoy will be converted to a 1 in arroy, destroying the information.
                // to fix that, we subtract 0.5 from the hannoy vector, so that any zero value is translated to a strictly
                // negative value.
                for x in &mut vector {
                    *x -= 0.5;
                }

                arroy_writer.add_item(arroy_wtxn, item, &vector)?;
            }
            arroy_build(arroy_wtxn, progress, rng, available_memory, cancel, &arroy_writer)?;
        }
        Ok(())
    }
}

fn arroy_build<R, D>(
    wtxn: &mut RwTxn<'_>,
    progress: &Progress,
    rng: &mut R,
    available_memory: Option<usize>,
    cancel: &(impl Fn() -> bool + Sync + Send),
    writer: &arroy::Writer<D>,
) -> Result<(), crate::Error>
where
    R: rand::Rng + rand::SeedableRng,
    D: arroy::Distance,
{
    let mut builder = writer.builder(rng);
    let builder = builder.progress(|step| progress.update_progress_from_arroy(step));
    builder.available_memory(available_memory.unwrap_or(usize::MAX)).cancel(cancel).build(wtxn)?;
    Ok(())
}

fn hannoy_build<R, D>(
    wtxn: &mut RwTxn<'_>,
    progress: &Progress,
    rng: &mut R,
    cancel: &(impl Fn() -> bool + Sync + Send),
    writer: &hannoy::Writer<D>,
) -> Result<(), crate::Error>
where
    R: rand::Rng + rand::SeedableRng,
    D: hannoy::Distance,
{
    let mut builder = writer.builder(rng).progress(progress.clone());
    builder
        .cancel(cancel)
        .ef_construction(HANNOY_EF_CONSTRUCTION)
        .build::<HANNOY_M, HANNOY_M0>(wtxn)?;
    Ok(())
}

#[derive(Debug, Default, Clone)]
pub struct VectorStoreStats {
    pub number_of_embeddings: u64,
    pub documents: RoaringBitmap,
}

fn vector_store_range_for_embedder(embedder_id: u8) -> impl Iterator<Item = u16> {
    (0..=u8::MAX).map(move |store_id| vector_store_for_embedder(embedder_id, store_id))
}

fn vector_store_for_embedder(embedder_id: u8, store_id: u8) -> u16 {
    let embedder_id = (embedder_id as u16) << 8;
    embedder_id | (store_id as u16)
}
