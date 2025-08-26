use hannoy::distances::{Cosine, Hamming};
use hannoy::ItemId;
use heed::{RoTxn, RwTxn, Unspecified};
use ordered_float::OrderedFloat;
use rand::SeedableRng as _;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::progress::Progress;
use crate::vector::Embeddings;

const HANNOY_EF_CONSTRUCTION: usize = 125;
const HANNOY_M: usize = 16;
const HANNOY_M0: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum VectorStoreBackend {
    #[default]
    Arroy,
    Hannoy,
}

pub struct VectorStore {
    backend: VectorStoreBackend,
    database: hannoy::Database<Unspecified>,
    embedder_index: u8,
    quantized: bool,
}

impl VectorStore {
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

    fn arroy_readers<'a, D: arroy::Distance>(
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

    fn readers<'a, D: hannoy::Distance>(
        &'a self,
        rtxn: &'a RoTxn<'a>,
        db: hannoy::Database<D>,
    ) -> impl Iterator<Item = Result<hannoy::Reader<'a, D>, hannoy::Error>> + 'a {
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
                self._arroy_items_in_store(rtxn, self.arroy_quantized_db(), store_id, with_items)
                    .map_err(Into::into)
            } else {
                self._arroy_items_in_store(rtxn, self.arroy_angular_db(), store_id, with_items)
                    .map_err(Into::into)
            }
        } else if self.quantized {
            self._items_in_store(rtxn, self.quantized_db(), store_id, with_items)
                .map_err(Into::into)
        } else {
            self._items_in_store(rtxn, self.angular_db(), store_id, with_items).map_err(Into::into)
        }
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
        let index = vector_store_for_embedder(self.embedder_index, store_id);
        let reader = hannoy::Reader::open(rtxn, index, db);
        match reader {
            Ok(reader) => Ok(with_items(reader.item_ids())),
            Err(hannoy::Error::MissingMetadata(_)) => Ok(with_items(&RoaringBitmap::new())),
            Err(err) => Err(err),
        }
    }

    pub fn dimensions(&self, rtxn: &RoTxn) -> crate::Result<Option<usize>> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                Ok(self
                    .arroy_readers(rtxn, self.arroy_quantized_db())
                    .next()
                    .transpose()?
                    .map(|reader| reader.dimensions()))
            } else {
                Ok(self
                    .arroy_readers(rtxn, self.arroy_angular_db())
                    .next()
                    .transpose()?
                    .map(|reader| reader.dimensions()))
            }
        } else if self.quantized {
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

    pub fn convert_from_arroy(&self, wtxn: &mut RwTxn, progress: Progress) -> crate::Result<()> {
        if self.quantized {
            let dimensions = self
                .arroy_readers(wtxn, self.arroy_quantized_db())
                .next()
                .transpose()?
                .map(|reader| reader.dimensions());

            let Some(dimensions) = dimensions else { return Ok(()) };

            for index in vector_store_range_for_embedder(self.embedder_index) {
                let mut rng = rand::rngs::StdRng::from_entropy();
                let writer = hannoy::Writer::new(self.quantized_db(), index, dimensions);
                let mut builder = writer.builder(&mut rng).progress(progress.clone());
                builder.prepare_arroy_conversion(wtxn)?;
                builder.build::<HANNOY_M, HANNOY_M0>(wtxn)?;
            }

            Ok(())
        } else {
            let dimensions = self
                .arroy_readers(wtxn, self.arroy_angular_db())
                .next()
                .transpose()?
                .map(|reader| reader.dimensions());

            let Some(dimensions) = dimensions else { return Ok(()) };

            for index in vector_store_range_for_embedder(self.embedder_index) {
                let mut rng = rand::rngs::StdRng::from_entropy();
                let writer = hannoy::Writer::new(self.angular_db(), index, dimensions);
                let mut builder = writer.builder(&mut rng).progress(progress.clone());
                builder.prepare_arroy_conversion(wtxn)?;
                builder.build::<HANNOY_M, HANNOY_M0>(wtxn)?;
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
        hannoy_memory: Option<usize>,
        cancel: &(impl Fn() -> bool + Sync + Send),
    ) -> Result<(), hannoy::Error> {
        for index in vector_store_range_for_embedder(self.embedder_index) {
            if self.quantized {
                let writer = hannoy::Writer::new(self.quantized_db(), index, dimension);
                if writer.need_build(wtxn)? {
                    let mut builder = writer.builder(rng).progress(progress.clone());
                    builder
                        .available_memory(hannoy_memory.unwrap_or(usize::MAX))
                        .cancel(cancel)
                        .ef_construction(HANNOY_EF_CONSTRUCTION)
                        .build::<HANNOY_M, HANNOY_M0>(wtxn)?;
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
                    let writer = writer.prepare_changing_distance::<Hamming>(wtxn)?;
                    let mut builder = writer.builder(rng).progress(progress.clone());
                    builder
                        .available_memory(hannoy_memory.unwrap_or(usize::MAX))
                        .cancel(cancel)
                        .ef_construction(HANNOY_EF_CONSTRUCTION)
                        .build::<HANNOY_M, HANNOY_M0>(wtxn)?;
                } else if writer.need_build(wtxn)? {
                    let mut builder = writer.builder(rng).progress(progress.clone());
                    builder
                        .available_memory(hannoy_memory.unwrap_or(usize::MAX))
                        .cancel(cancel)
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
            vector_store_range_for_embedder(self.embedder_index).zip(embeddings.iter())
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

        for index in vector_store_range_for_embedder(self.embedder_index) {
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

        let index = vector_store_for_embedder(self.embedder_index, store_id);
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
        for index in vector_store_range_for_embedder(self.embedder_index) {
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
        let index = vector_store_for_embedder(self.embedder_index, store_id);
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
        let index = vector_store_for_embedder(self.embedder_index, store_id);
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

        for index in vector_store_range_for_embedder(self.embedder_index) {
            let writer = hannoy::Writer::new(db, index, dimension);
            if writer.contains_item(wtxn, item_id)? {
                return writer.del_item(wtxn, item_id);
            }
        }
        Ok(false)
    }

    pub fn clear(&self, wtxn: &mut RwTxn, dimension: usize) -> Result<(), hannoy::Error> {
        for index in vector_store_range_for_embedder(self.embedder_index) {
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
    ) -> crate::Result<bool> {
        for index in vector_store_range_for_embedder(self.embedder_index) {
            let contains = if self.backend == VectorStoreBackend::Arroy {
                if self.quantized {
                    let writer = arroy::Writer::new(self.arroy_quantized_db(), index, dimension);
                    if writer.is_empty(rtxn)? {
                        continue;
                    }
                    writer.contains_item(rtxn, item)?
                } else {
                    let writer = arroy::Writer::new(self.arroy_angular_db(), index, dimension);
                    if writer.is_empty(rtxn)? {
                        continue;
                    }
                    writer.contains_item(rtxn, item)?
                }
            } else if self.quantized {
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
    ) -> crate::Result<Vec<(ItemId, f32)>> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_nns_by_item(rtxn, self.arroy_quantized_db(), item, limit, filter)
                    .map_err(Into::into)
            } else {
                self._arroy_nns_by_item(rtxn, self.arroy_angular_db(), item, limit, filter)
                    .map_err(Into::into)
            }
        } else if self.quantized {
            self._nns_by_item(rtxn, self.quantized_db(), item, limit, filter).map_err(Into::into)
        } else {
            self._nns_by_item(rtxn, self.angular_db(), item, limit, filter).map_err(Into::into)
        }
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

        for reader in self.arroy_readers(rtxn, db) {
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

    pub fn nns_by_vector(
        &self,
        rtxn: &RoTxn,
        vector: &[f32],
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> crate::Result<Vec<(ItemId, f32)>> {
        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                self._arroy_nns_by_vector(rtxn, self.arroy_quantized_db(), vector, limit, filter)
                    .map_err(Into::into)
            } else {
                self._arroy_nns_by_vector(rtxn, self.arroy_angular_db(), vector, limit, filter)
                    .map_err(Into::into)
            }
        } else if self.quantized {
            self._nns_by_vector(rtxn, self.quantized_db(), vector, limit, filter)
                .map_err(Into::into)
        } else {
            self._nns_by_vector(rtxn, self.angular_db(), vector, limit, filter).map_err(Into::into)
        }
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

        for reader in self.arroy_readers(rtxn, db) {
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
            let mut searcher = reader.nns(limit);
            searcher.ef_search((limit * 10).max(100)); // TODO find better ef
            if let Some(filter) = filter {
                searcher.candidates(filter);
            }

            results.append(&mut searcher.by_vector(rtxn, vector)?);
        }

        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));

        Ok(results)
    }

    pub fn item_vectors(&self, rtxn: &RoTxn, item_id: u32) -> crate::Result<Vec<Vec<f32>>> {
        let mut vectors = Vec::new();

        if self.backend == VectorStoreBackend::Arroy {
            if self.quantized {
                for reader in self.arroy_readers(rtxn, self.arroy_quantized_db()) {
                    if let Some(vec) = reader?.item_vector(rtxn, item_id)? {
                        vectors.push(vec);
                    }
                }
            } else {
                for reader in self.arroy_readers(rtxn, self.arroy_angular_db()) {
                    if let Some(vec) = reader?.item_vector(rtxn, item_id)? {
                        vectors.push(vec);
                    }
                }
            }
        } else if self.quantized {
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

    fn arroy_angular_db(&self) -> arroy::Database<arroy::distances::Cosine> {
        self.database.remap_types()
    }

    fn arroy_quantized_db(&self) -> arroy::Database<arroy::distances::BinaryQuantizedCosine> {
        self.database.remap_types()
    }

    fn angular_db(&self) -> hannoy::Database<Cosine> {
        self.database.remap_data_type()
    }

    fn quantized_db(&self) -> hannoy::Database<Hamming> {
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
                stats.documents |= documents;
                stats.number_of_embeddings += documents.len();
            }
        } else {
            for reader in self.readers(rtxn, self.angular_db()) {
                let reader = reader?;
                let documents = reader.item_ids();
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

fn vector_store_range_for_embedder(embedder_id: u8) -> impl Iterator<Item = u16> {
    (0..=u8::MAX).map(move |store_id| vector_store_for_embedder(embedder_id, store_id))
}

fn vector_store_for_embedder(embedder_id: u8, store_id: u8) -> u16 {
    let embedder_id = (embedder_id as u16) << 8;
    embedder_id | (store_id as u16)
}
