use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::iter::FromIterator;
use std::time::Instant;
use std::{cmp, iter};

use bstr::ByteSlice as _;
use concat_arrays::concat_arrays;
use fst::Set;
use grenad::{CompressionType, FileFuse, Reader, Sorter, Writer};
use heed::BytesEncode;
use linked_hash_map::LinkedHashMap;
use log::{debug, info, warn};
use meilisearch_tokenizer::token::SeparatorKind;
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig, Token, TokenKind};
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use serde_json::Value;
use tempfile::tempfile;

use super::merge_function::{
    cbo_roaring_bitmap_merge, fst_merge, keep_first, roaring_bitmap_merge,
};
use super::{create_sorter, create_writer, writer_into_reader, MergeFn};
use crate::error::{Error, InternalError, SerializationError};
use crate::heed_codec::facet::{
    FacetLevelValueF64Codec, FacetStringLevelZeroCodec, FieldDocIdFacetF64Codec,
    FieldDocIdFacetStringCodec,
};
use crate::heed_codec::{BoRoaringBitmapCodec, CboRoaringBitmapCodec};
use crate::update::UpdateIndexingStep;
use crate::{json_to_string, DocumentId, FieldId, Position, Result, SmallVec32};

const LMDB_MAX_KEY_LENGTH: usize = 511;
const ONE_KILOBYTE: usize = 1024 * 1024;

const MAX_POSITION: usize = 1000;
const WORDS_FST_KEY: &[u8] = crate::index::main_key::WORDS_FST_KEY.as_bytes();

pub struct Readers {
    pub main: Reader<FileFuse>,
    pub word_docids: Reader<FileFuse>,
    pub docid_word_positions: Reader<FileFuse>,
    pub words_pairs_proximities_docids: Reader<FileFuse>,
    pub word_level_position_docids: Reader<FileFuse>,
    pub field_id_word_count_docids: Reader<FileFuse>,
    pub facet_field_numbers_docids: Reader<FileFuse>,
    pub facet_field_strings_docids: Reader<FileFuse>,
    pub field_id_docid_facet_numbers: Reader<FileFuse>,
    pub field_id_docid_facet_strings: Reader<FileFuse>,
    pub documents: Reader<FileFuse>,
}

pub struct Store<'s, A> {
    // Indexing parameters
    searchable_fields: HashSet<FieldId>,
    filterable_fields: HashSet<FieldId>,
    // Caches
    word_docids: LinkedHashMap<SmallVec32<u8>, RoaringBitmap>,
    word_docids_limit: usize,
    field_id_word_count_docids: HashMap<(FieldId, u8), RoaringBitmap>,
    words_pairs_proximities_docids:
        LinkedHashMap<(SmallVec32<u8>, SmallVec32<u8>, u8), RoaringBitmap>,
    words_pairs_proximities_docids_limit: usize,
    facet_field_number_docids: LinkedHashMap<(FieldId, OrderedFloat<f64>), RoaringBitmap>,
    facet_field_string_docids: LinkedHashMap<(FieldId, String), RoaringBitmap>,
    facet_field_value_docids_limit: usize,
    // MTBL parameters
    chunk_compression_type: CompressionType,
    chunk_compression_level: Option<u32>,
    chunk_fusing_shrink_size: Option<u64>,
    // MTBL sorters
    main_sorter: Sorter<MergeFn<Error>>,
    word_docids_sorter: Sorter<MergeFn<Error>>,
    words_pairs_proximities_docids_sorter: Sorter<MergeFn<Error>>,
    word_level_position_docids_sorter: Sorter<MergeFn<Error>>,
    field_id_word_count_docids_sorter: Sorter<MergeFn<Error>>,
    facet_field_numbers_docids_sorter: Sorter<MergeFn<Error>>,
    facet_field_strings_docids_sorter: Sorter<MergeFn<Error>>,
    field_id_docid_facet_numbers_sorter: Sorter<MergeFn<Error>>,
    field_id_docid_facet_strings_sorter: Sorter<MergeFn<Error>>,
    // MTBL writers
    docid_word_positions_writer: Writer<File>,
    documents_writer: Writer<File>,
    // tokenizer
    analyzer: Analyzer<'s, A>,
}

impl<'s, A: AsRef<[u8]>> Store<'s, A> {
    pub fn new(
        searchable_fields: HashSet<FieldId>,
        filterable_fields: HashSet<FieldId>,
        linked_hash_map_size: Option<usize>,
        max_nb_chunks: Option<usize>,
        max_memory: Option<usize>,
        chunk_compression_type: CompressionType,
        chunk_compression_level: Option<u32>,
        chunk_fusing_shrink_size: Option<u64>,
        stop_words: Option<&'s Set<A>>,
    ) -> Result<Self> {
        // We divide the max memory by the number of sorter the Store have.
        let max_memory = max_memory.map(|mm| cmp::max(ONE_KILOBYTE, mm / 5));
        let linked_hash_map_size = linked_hash_map_size.unwrap_or(500);

        let main_sorter = create_sorter(
            fst_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let word_docids_sorter = create_sorter(
            roaring_bitmap_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let words_pairs_proximities_docids_sorter = create_sorter(
            cbo_roaring_bitmap_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let word_level_position_docids_sorter = create_sorter(
            cbo_roaring_bitmap_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let field_id_word_count_docids_sorter = create_sorter(
            cbo_roaring_bitmap_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let facet_field_numbers_docids_sorter = create_sorter(
            cbo_roaring_bitmap_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let facet_field_strings_docids_sorter = create_sorter(
            cbo_roaring_bitmap_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let field_id_docid_facet_numbers_sorter = create_sorter(
            keep_first,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            Some(1024 * 1024 * 1024), // 1MB
        );
        let field_id_docid_facet_strings_sorter = create_sorter(
            keep_first,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            Some(1024 * 1024 * 1024), // 1MB
        );

        let documents_writer = tempfile()
            .and_then(|f| create_writer(chunk_compression_type, chunk_compression_level, f))?;
        let docid_word_positions_writer = tempfile()
            .and_then(|f| create_writer(chunk_compression_type, chunk_compression_level, f))?;

        let mut config = AnalyzerConfig::default();
        if let Some(stop_words) = stop_words {
            config.stop_words(stop_words);
        }
        let analyzer = Analyzer::new(config);

        Ok(Store {
            // Indexing parameters.
            searchable_fields,
            filterable_fields,
            // Caches
            word_docids: LinkedHashMap::with_capacity(linked_hash_map_size),
            field_id_word_count_docids: HashMap::new(),
            word_docids_limit: linked_hash_map_size,
            words_pairs_proximities_docids: LinkedHashMap::with_capacity(linked_hash_map_size),
            words_pairs_proximities_docids_limit: linked_hash_map_size,
            facet_field_number_docids: LinkedHashMap::with_capacity(linked_hash_map_size),
            facet_field_string_docids: LinkedHashMap::with_capacity(linked_hash_map_size),
            facet_field_value_docids_limit: linked_hash_map_size,
            // MTBL parameters
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            // MTBL sorters
            main_sorter,
            word_docids_sorter,
            words_pairs_proximities_docids_sorter,
            word_level_position_docids_sorter,
            field_id_word_count_docids_sorter,
            facet_field_numbers_docids_sorter,
            facet_field_strings_docids_sorter,
            field_id_docid_facet_numbers_sorter,
            field_id_docid_facet_strings_sorter,
            // MTBL writers
            docid_word_positions_writer,
            documents_writer,
            // tokenizer
            analyzer,
        })
    }

    // Save the documents ids under the position and word we have seen it.
    fn insert_word_docid(&mut self, word: &str, id: DocumentId) -> Result<()> {
        // if get_refresh finds the element it is assured to be at the end of the linked hash map.
        match self.word_docids.get_refresh(word.as_bytes()) {
            Some(old) => {
                old.insert(id);
            }
            None => {
                let word_vec = SmallVec32::from(word.as_bytes());
                // A newly inserted element is append at the end of the linked hash map.
                self.word_docids.insert(word_vec, RoaringBitmap::from_iter(Some(id)));
                // If the word docids just reached it's capacity we must make sure to remove
                // one element, this way next time we insert we doesn't grow the capacity.
                if self.word_docids.len() == self.word_docids_limit {
                    // Removing the front element is equivalent to removing the LRU element.
                    let lru = self.word_docids.pop_front();
                    Self::write_word_docids(&mut self.word_docids_sorter, lru)?;
                }
            }
        }
        Ok(())
    }

    fn insert_facet_number_values_docid(
        &mut self,
        field_id: FieldId,
        value: OrderedFloat<f64>,
        id: DocumentId,
    ) -> Result<()> {
        let sorter = &mut self.field_id_docid_facet_numbers_sorter;
        Self::write_field_id_docid_facet_number_value(sorter, field_id, id, value)?;

        let key = (field_id, value);
        // if get_refresh finds the element it is assured to be at the end of the linked hash map.
        match self.facet_field_number_docids.get_refresh(&key) {
            Some(old) => {
                old.insert(id);
            }
            None => {
                // A newly inserted element is append at the end of the linked hash map.
                self.facet_field_number_docids.insert(key, RoaringBitmap::from_iter(Some(id)));
                // If the word docids just reached it's capacity we must make sure to remove
                // one element, this way next time we insert we doesn't grow the capacity.
                if self.facet_field_number_docids.len() == self.facet_field_value_docids_limit {
                    // Removing the front element is equivalent to removing the LRU element.
                    Self::write_facet_field_number_docids(
                        &mut self.facet_field_numbers_docids_sorter,
                        self.facet_field_number_docids.pop_front(),
                    )?;
                }
            }
        }

        Ok(())
    }

    // Save the documents ids under the facet field id and value we have seen it.
    fn insert_facet_string_values_docid(
        &mut self,
        field_id: FieldId,
        value: String,
        id: DocumentId,
    ) -> Result<()> {
        if value.is_empty() {
            return Ok(());
        }

        let sorter = &mut self.field_id_docid_facet_strings_sorter;
        Self::write_field_id_docid_facet_string_value(sorter, field_id, id, &value)?;

        let key = (field_id, value);
        // if get_refresh finds the element it is assured to be at the end of the linked hash map.
        match self.facet_field_string_docids.get_refresh(&key) {
            Some(old) => {
                old.insert(id);
            }
            None => {
                // A newly inserted element is append at the end of the linked hash map.
                self.facet_field_string_docids.insert(key, RoaringBitmap::from_iter(Some(id)));
                // If the word docids just reached it's capacity we must make sure to remove
                // one element, this way next time we insert we doesn't grow the capacity.
                if self.facet_field_string_docids.len() == self.facet_field_value_docids_limit {
                    // Removing the front element is equivalent to removing the LRU element.
                    Self::write_facet_field_string_docids(
                        &mut self.facet_field_strings_docids_sorter,
                        self.facet_field_string_docids.pop_front(),
                    )?;
                }
            }
        }

        Ok(())
    }

    // Save the documents ids under the words pairs proximities that it contains.
    fn insert_words_pairs_proximities_docids<'a>(
        &mut self,
        words_pairs_proximities: impl IntoIterator<Item = ((&'a str, &'a str), u8)>,
        id: DocumentId,
    ) -> Result<()> {
        for ((w1, w2), prox) in words_pairs_proximities {
            let w1 = SmallVec32::from(w1.as_bytes());
            let w2 = SmallVec32::from(w2.as_bytes());
            let key = (w1, w2, prox);
            // if get_refresh finds the element it is assured
            // to be at the end of the linked hash map.
            match self.words_pairs_proximities_docids.get_refresh(&key) {
                Some(old) => {
                    old.insert(id);
                }
                None => {
                    // A newly inserted element is append at the end of the linked hash map.
                    let ids = RoaringBitmap::from_iter(Some(id));
                    self.words_pairs_proximities_docids.insert(key, ids);
                }
            }
        }

        // If the linked hashmap is over capacity we must remove the overflowing elements.
        let len = self.words_pairs_proximities_docids.len();
        let overflow = len.checked_sub(self.words_pairs_proximities_docids_limit);
        if let Some(overflow) = overflow {
            let mut lrus = Vec::with_capacity(overflow);
            // Removing front elements is equivalent to removing the LRUs.
            let iter = iter::from_fn(|| self.words_pairs_proximities_docids.pop_front());
            iter.take(overflow).for_each(|x| lrus.push(x));
            Self::write_words_pairs_proximities(
                &mut self.words_pairs_proximities_docids_sorter,
                lrus,
            )?;
        }

        Ok(())
    }

    fn write_document(
        &mut self,
        document_id: DocumentId,
        words_positions: &mut HashMap<String, SmallVec32<Position>>,
        facet_numbers_values: &mut HashMap<FieldId, Vec<f64>>,
        facet_strings_values: &mut HashMap<FieldId, Vec<String>>,
        record: &[u8],
    ) -> Result<()> {
        // We compute the list of words pairs proximities (self-join) and write it directly to disk.
        let words_pair_proximities = compute_words_pair_proximities(&words_positions);
        self.insert_words_pairs_proximities_docids(words_pair_proximities, document_id)?;

        // We store document_id associated with all the words the record contains.
        for (word, _) in words_positions.iter() {
            self.insert_word_docid(word, document_id)?;
        }

        self.documents_writer.insert(document_id.to_be_bytes(), record)?;
        Self::write_docid_word_positions(
            &mut self.docid_word_positions_writer,
            document_id,
            words_positions,
        )?;
        Self::write_word_position_docids(
            &mut self.word_level_position_docids_sorter,
            document_id,
            words_positions,
        )?;

        words_positions.clear();

        // We store document_id associated with all the facet numbers fields ids and values.
        for (field, values) in facet_numbers_values.drain() {
            for value in values {
                let value = OrderedFloat::from(value);
                self.insert_facet_number_values_docid(field, value, document_id)?;
            }
        }

        // We store document_id associated with all the facet strings fields ids and values.
        for (field, values) in facet_strings_values.drain() {
            for value in values {
                self.insert_facet_string_values_docid(field, value, document_id)?;
            }
        }

        Ok(())
    }

    fn write_words_pairs_proximities<E>(
        sorter: &mut Sorter<MergeFn<E>>,
        iter: impl IntoIterator<Item = ((SmallVec32<u8>, SmallVec32<u8>, u8), RoaringBitmap)>,
    ) -> Result<()>
    where
        Error: From<E>,
    {
        let mut key = Vec::new();
        let mut buffer = Vec::new();

        for ((w1, w2, min_prox), docids) in iter {
            key.clear();
            key.extend_from_slice(w1.as_bytes());
            key.push(0);
            key.extend_from_slice(w2.as_bytes());
            // Storing the minimun proximity found between those words
            key.push(min_prox);
            // We serialize the document ids into a buffer
            buffer.clear();
            buffer.reserve(CboRoaringBitmapCodec::serialized_size(&docids));
            CboRoaringBitmapCodec::serialize_into(&docids, &mut buffer);
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &buffer)?;
            } else {
                warn!(
                    "words pairs proximity ({:?} - {:?}, {:?}) is too large to be saved",
                    w1, w2, min_prox
                );
            }
        }

        Ok(())
    }

    fn write_docid_word_positions(
        writer: &mut Writer<File>,
        id: DocumentId,
        words_positions: &HashMap<String, SmallVec32<Position>>,
    ) -> Result<()> {
        // We prefix the words by the document id.
        let mut key = id.to_be_bytes().to_vec();
        let mut buffer = Vec::new();
        let base_size = key.len();

        // We order the words lexicographically, this way we avoid passing by a sorter.
        let words_positions = BTreeMap::from_iter(words_positions);

        for (word, positions) in words_positions {
            key.truncate(base_size);
            key.extend_from_slice(word.as_bytes());
            buffer.clear();

            // We serialize the positions into a buffer.
            let positions = RoaringBitmap::from_iter(positions.iter().cloned());
            BoRoaringBitmapCodec::serialize_into(&positions, &mut buffer);

            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                writer.insert(&key, &buffer)?;
            } else {
                warn!("word {:?} is too large to be saved", word);
            }
        }

        Ok(())
    }

    fn write_word_position_docids<E>(
        writer: &mut Sorter<MergeFn<E>>,
        document_id: DocumentId,
        words_positions: &HashMap<String, SmallVec32<Position>>,
    ) -> Result<()>
    where
        Error: From<E>,
    {
        let mut key_buffer = Vec::new();
        let mut data_buffer = Vec::new();

        for (word, positions) in words_positions {
            key_buffer.clear();
            key_buffer.extend_from_slice(word.as_bytes());
            key_buffer.push(0); // level 0

            for position in positions {
                key_buffer.truncate(word.len() + 1);
                let position_bytes = position.to_be_bytes();
                key_buffer.extend_from_slice(position_bytes.as_bytes());
                key_buffer.extend_from_slice(position_bytes.as_bytes());

                data_buffer.clear();
                let positions = RoaringBitmap::from_iter(Some(document_id));
                // We serialize the positions into a buffer.
                CboRoaringBitmapCodec::serialize_into(&positions, &mut data_buffer);

                // that we write under the generated key into MTBL
                if lmdb_key_valid_size(&key_buffer) {
                    writer.insert(&key_buffer, &data_buffer)?;
                } else {
                    warn!("word {:?} is too large to be saved", word);
                }
            }
        }

        Ok(())
    }

    fn write_facet_field_string_docids<I, E>(sorter: &mut Sorter<MergeFn<E>>, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = ((FieldId, String), RoaringBitmap)>,
        Error: From<E>,
    {
        let mut key_buffer = Vec::new();
        let mut data_buffer = Vec::new();

        for ((field_id, value), docids) in iter {
            key_buffer.clear();
            data_buffer.clear();

            FacetStringLevelZeroCodec::serialize_into(field_id, &value, &mut key_buffer);
            CboRoaringBitmapCodec::serialize_into(&docids, &mut data_buffer);

            if lmdb_key_valid_size(&key_buffer) {
                sorter.insert(&key_buffer, &data_buffer)?;
            } else {
                warn!("facet value {:?} is too large to be saved", value);
            }
        }

        Ok(())
    }

    fn write_facet_field_number_docids<I, E>(sorter: &mut Sorter<MergeFn<E>>, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = ((FieldId, OrderedFloat<f64>), RoaringBitmap)>,
        Error: From<E>,
    {
        let mut data_buffer = Vec::new();

        for ((field_id, value), docids) in iter {
            data_buffer.clear();

            let key = FacetLevelValueF64Codec::bytes_encode(&(field_id, 0, *value, *value))
                .map(Cow::into_owned)
                .ok_or(SerializationError::Encoding { db_name: Some("facet level value") })?;

            CboRoaringBitmapCodec::serialize_into(&docids, &mut data_buffer);

            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &data_buffer)?;
            }
        }

        Ok(())
    }

    fn write_field_id_docid_facet_number_value<E>(
        sorter: &mut Sorter<MergeFn<E>>,
        field_id: FieldId,
        document_id: DocumentId,
        value: OrderedFloat<f64>,
    ) -> Result<()>
    where
        Error: From<E>,
    {
        let key = FieldDocIdFacetF64Codec::bytes_encode(&(field_id, document_id, *value))
            .map(Cow::into_owned)
            .ok_or(SerializationError::Encoding { db_name: Some("facet level value") })?;

        if lmdb_key_valid_size(&key) {
            sorter.insert(&key, &[])?;
        }

        Ok(())
    }

    fn write_field_id_docid_facet_string_value<E>(
        sorter: &mut Sorter<MergeFn<E>>,
        field_id: FieldId,
        document_id: DocumentId,
        value: &str,
    ) -> Result<()>
    where
        Error: From<E>,
    {
        let mut buffer = Vec::new();

        FieldDocIdFacetStringCodec::serialize_into(field_id, document_id, value, &mut buffer);

        if lmdb_key_valid_size(&buffer) {
            sorter.insert(&buffer, &[])?;
        } else {
            warn!("facet value {:?} is too large to be saved", value);
        }

        Ok(())
    }

    fn write_word_docids<I, E>(sorter: &mut Sorter<MergeFn<E>>, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = (SmallVec32<u8>, RoaringBitmap)>,
        Error: From<E>,
    {
        let mut key = Vec::new();
        let mut buffer = Vec::new();

        for (word, ids) in iter {
            key.clear();
            key.extend_from_slice(&word);
            // We serialize the document ids into a buffer
            buffer.clear();
            let ids = RoaringBitmap::from_iter(ids);
            buffer.reserve(ids.serialized_size());
            ids.serialize_into(&mut buffer)?;
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &buffer)?;
            } else {
                warn!("word {:?} is too large to be saved", word);
            }
        }

        Ok(())
    }

    pub fn index<F>(
        mut self,
        mut documents: grenad::Reader<&[u8]>,
        documents_count: usize,
        thread_index: usize,
        num_threads: usize,
        log_every_n: Option<usize>,
        mut progress_callback: F,
    ) -> Result<Readers>
    where
        F: FnMut(UpdateIndexingStep),
    {
        debug!("{:?}: Indexing in a Store...", thread_index);

        let mut before = Instant::now();
        let mut words_positions = HashMap::new();
        let mut facet_numbers_values = HashMap::new();
        let mut facet_strings_values = HashMap::new();

        let mut count: usize = 0;
        while let Some((key, value)) = documents.next()? {
            let document_id = key.try_into().map(u32::from_be_bytes).unwrap();
            let document = obkv::KvReader::new(value);

            // We skip documents that must not be indexed by this thread.
            if count % num_threads == thread_index {
                // This is a log routine that we do every `log_every_n` documents.
                if thread_index == 0 && log_every_n.map_or(false, |len| count % len == 0) {
                    info!(
                        "We have seen {} documents so far ({:.02?}).",
                        format_count(count),
                        before.elapsed()
                    );
                    progress_callback(UpdateIndexingStep::IndexDocuments {
                        documents_seen: count,
                        total_documents: documents_count,
                    });
                    before = Instant::now();
                }

                for (attr, content) in document.iter() {
                    if self.filterable_fields.contains(&attr)
                        || self.searchable_fields.contains(&attr)
                    {
                        let value =
                            serde_json::from_slice(content).map_err(InternalError::SerdeJson)?;

                        if self.filterable_fields.contains(&attr) {
                            let (facet_numbers, facet_strings) = extract_facet_values(&value);
                            facet_numbers_values
                                .entry(attr)
                                .or_insert_with(Vec::new)
                                .extend(facet_numbers);
                            facet_strings_values
                                .entry(attr)
                                .or_insert_with(Vec::new)
                                .extend(facet_strings);
                        }

                        if self.searchable_fields.contains(&attr) {
                            let content = match json_to_string(&value) {
                                Some(content) => content,
                                None => continue,
                            };

                            let analyzed = self.analyzer.analyze(&content);
                            let tokens = process_tokens(analyzed.tokens());

                            let mut last_pos = None;
                            for (pos, token) in tokens.take_while(|(pos, _)| *pos < MAX_POSITION) {
                                last_pos = Some(pos);
                                let position = (attr as usize * MAX_POSITION + pos) as u32;
                                words_positions
                                    .entry(token.text().to_string())
                                    .or_insert_with(SmallVec32::new)
                                    .push(position);
                            }

                            if let Some(last_pos) = last_pos.filter(|p| *p <= 10) {
                                let key = (attr, last_pos as u8 + 1);
                                self.field_id_word_count_docids
                                    .entry(key)
                                    .or_insert_with(RoaringBitmap::new)
                                    .insert(document_id);
                            }
                        }
                    }
                }

                // We write the document in the documents store.
                self.write_document(
                    document_id,
                    &mut words_positions,
                    &mut facet_numbers_values,
                    &mut facet_strings_values,
                    value,
                )?;
            }

            // Compute the document id of the next document.
            count += 1;
        }

        progress_callback(UpdateIndexingStep::IndexDocuments {
            documents_seen: count,
            total_documents: documents_count,
        });

        let readers = self.finish()?;
        debug!("{:?}: Store created!", thread_index);
        Ok(readers)
    }

    fn finish(mut self) -> Result<Readers> {
        let comp_type = self.chunk_compression_type;
        let comp_level = self.chunk_compression_level;
        let shrink_size = self.chunk_fusing_shrink_size;

        Self::write_word_docids(&mut self.word_docids_sorter, self.word_docids)?;
        Self::write_words_pairs_proximities(
            &mut self.words_pairs_proximities_docids_sorter,
            self.words_pairs_proximities_docids,
        )?;
        Self::write_facet_field_number_docids(
            &mut self.facet_field_numbers_docids_sorter,
            self.facet_field_number_docids,
        )?;

        Self::write_facet_field_string_docids(
            &mut self.facet_field_strings_docids_sorter,
            self.facet_field_string_docids,
        )?;

        let mut word_docids_wtr =
            tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        let mut builder = fst::SetBuilder::memory();

        let mut iter = self.word_docids_sorter.into_iter()?;
        while let Some((word, val)) = iter.next()? {
            // This is a lexicographically ordered word position
            // we use the key to construct the words fst.
            builder.insert(word)?;
            word_docids_wtr.insert(word, val)?;
        }

        let mut docids_buffer = Vec::new();
        for ((fid, count), docids) in self.field_id_word_count_docids {
            docids_buffer.clear();
            CboRoaringBitmapCodec::serialize_into(&docids, &mut docids_buffer);
            let key: [u8; 3] = concat_arrays!(fid.to_be_bytes(), [count]);
            self.field_id_word_count_docids_sorter.insert(key, &docids_buffer)?;
        }

        let fst = builder.into_set();
        self.main_sorter.insert(WORDS_FST_KEY, fst.as_fst().as_bytes())?;

        let mut main_wtr = tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.main_sorter.write_into(&mut main_wtr)?;

        let mut words_pairs_proximities_docids_wtr =
            tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.words_pairs_proximities_docids_sorter
            .write_into(&mut words_pairs_proximities_docids_wtr)?;

        let mut word_level_position_docids_wtr =
            tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.word_level_position_docids_sorter.write_into(&mut word_level_position_docids_wtr)?;

        let mut field_id_word_count_docids_wtr =
            tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.field_id_word_count_docids_sorter.write_into(&mut field_id_word_count_docids_wtr)?;

        let mut facet_field_numbers_docids_wtr =
            tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.facet_field_numbers_docids_sorter.write_into(&mut facet_field_numbers_docids_wtr)?;

        let mut facet_field_strings_docids_wtr =
            tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.facet_field_strings_docids_sorter.write_into(&mut facet_field_strings_docids_wtr)?;

        let mut field_id_docid_facet_numbers_wtr =
            tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.field_id_docid_facet_numbers_sorter
            .write_into(&mut field_id_docid_facet_numbers_wtr)?;

        let mut field_id_docid_facet_strings_wtr =
            tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.field_id_docid_facet_strings_sorter
            .write_into(&mut field_id_docid_facet_strings_wtr)?;

        let main = writer_into_reader(main_wtr, shrink_size)?;
        let word_docids = writer_into_reader(word_docids_wtr, shrink_size)?;
        let words_pairs_proximities_docids =
            writer_into_reader(words_pairs_proximities_docids_wtr, shrink_size)?;
        let word_level_position_docids =
            writer_into_reader(word_level_position_docids_wtr, shrink_size)?;
        let field_id_word_count_docids =
            writer_into_reader(field_id_word_count_docids_wtr, shrink_size)?;
        let facet_field_numbers_docids =
            writer_into_reader(facet_field_numbers_docids_wtr, shrink_size)?;
        let facet_field_strings_docids =
            writer_into_reader(facet_field_strings_docids_wtr, shrink_size)?;
        let field_id_docid_facet_numbers =
            writer_into_reader(field_id_docid_facet_numbers_wtr, shrink_size)?;
        let field_id_docid_facet_strings =
            writer_into_reader(field_id_docid_facet_strings_wtr, shrink_size)?;
        let docid_word_positions =
            writer_into_reader(self.docid_word_positions_writer, shrink_size)?;
        let documents = writer_into_reader(self.documents_writer, shrink_size)?;

        Ok(Readers {
            main,
            word_docids,
            docid_word_positions,
            words_pairs_proximities_docids,
            word_level_position_docids,
            field_id_word_count_docids,
            facet_field_numbers_docids,
            facet_field_strings_docids,
            field_id_docid_facet_numbers,
            field_id_docid_facet_strings,
            documents,
        })
    }
}

/// Outputs a list of all pairs of words with the shortest proximity between 1 and 7 inclusive.
///
/// This list is used by the engine to calculate the documents containing words that are
/// close to each other.
fn compute_words_pair_proximities(
    word_positions: &HashMap<String, SmallVec32<Position>>,
) -> HashMap<(&str, &str), u8> {
    use itertools::Itertools;

    let mut words_pair_proximities = HashMap::new();
    for ((w1, ps1), (w2, ps2)) in word_positions.iter().cartesian_product(word_positions) {
        let mut min_prox = None;
        for (ps1, ps2) in ps1.iter().cartesian_product(ps2) {
            let prox = crate::proximity::positions_proximity(*ps1, *ps2);
            let prox = u8::try_from(prox).unwrap();
            // We don't care about a word that appear at the
            // same position or too far from the other.
            if prox >= 1 && prox <= 7 && min_prox.map_or(true, |mp| prox < mp) {
                min_prox = Some(prox)
            }
        }

        if let Some(min_prox) = min_prox {
            words_pair_proximities.insert((w1.as_str(), w2.as_str()), min_prox);
        }
    }

    words_pair_proximities
}

fn format_count(n: usize) -> String {
    human_format::Formatter::new().with_decimals(1).with_separator("").format(n as f64)
}

fn lmdb_key_valid_size(key: &[u8]) -> bool {
    !key.is_empty() && key.len() <= LMDB_MAX_KEY_LENGTH
}

/// take an iterator on tokens and compute their relative position depending on separator kinds
/// if it's an `Hard` separator we add an additional relative proximity of 8 between words,
/// else we keep the standart proximity of 1 between words.
fn process_tokens<'a>(
    tokens: impl Iterator<Item = Token<'a>>,
) -> impl Iterator<Item = (usize, Token<'a>)> {
    tokens
        .skip_while(|token| token.is_separator().is_some())
        .scan((0, None), |(offset, prev_kind), token| {
            match token.kind {
                TokenKind::Word | TokenKind::StopWord | TokenKind::Unknown => {
                    *offset += match *prev_kind {
                        Some(TokenKind::Separator(SeparatorKind::Hard)) => 8,
                        Some(_) => 1,
                        None => 0,
                    };
                    *prev_kind = Some(token.kind)
                }
                TokenKind::Separator(SeparatorKind::Hard) => {
                    *prev_kind = Some(token.kind);
                }
                TokenKind::Separator(SeparatorKind::Soft)
                    if *prev_kind != Some(TokenKind::Separator(SeparatorKind::Hard)) =>
                {
                    *prev_kind = Some(token.kind);
                }
                _ => (),
            }
            Some((*offset, token))
        })
        .filter(|(_, t)| t.is_word())
}

fn extract_facet_values(value: &Value) -> (Vec<f64>, Vec<String>) {
    fn inner_extract_facet_values(
        value: &Value,
        can_recurse: bool,
        output_numbers: &mut Vec<f64>,
        output_strings: &mut Vec<String>,
    ) {
        match value {
            Value::Null => (),
            Value::Bool(b) => output_strings.push(b.to_string()),
            Value::Number(number) => {
                if let Some(float) = number.as_f64() {
                    output_numbers.push(float);
                }
            }
            Value::String(string) => {
                let string = string.trim().to_lowercase();
                output_strings.push(string);
            }
            Value::Array(values) => {
                if can_recurse {
                    for value in values {
                        inner_extract_facet_values(value, false, output_numbers, output_strings);
                    }
                }
            }
            Value::Object(_) => (),
        }
    }

    let mut facet_number_values = Vec::new();
    let mut facet_string_values = Vec::new();
    inner_extract_facet_values(value, true, &mut facet_number_values, &mut facet_string_values);

    (facet_number_values, facet_string_values)
}
