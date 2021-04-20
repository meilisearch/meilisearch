use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::iter::FromIterator;
use std::time::Instant;
use std::{cmp, iter};

use anyhow::{bail, Context};
use bstr::ByteSlice as _;
use fst::Set;
use grenad::{Reader, FileFuse, Writer, Sorter, CompressionType};
use heed::BytesEncode;
use linked_hash_map::LinkedHashMap;
use log::{debug, info, warn};
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig, Token, TokenKind, token::SeparatorKind};
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use serde_json::Value;
use tempfile::tempfile;

use crate::facet::{FacetType, FacetValue};
use crate::heed_codec::facet::{FacetValueStringCodec, FacetLevelValueF64Codec};
use crate::heed_codec::facet::{FieldDocIdFacetStringCodec, FieldDocIdFacetF64Codec};
use crate::heed_codec::{BoRoaringBitmapCodec, CboRoaringBitmapCodec};
use crate::update::UpdateIndexingStep;
use crate::{json_to_string, SmallVec8, SmallVec32, Position, DocumentId, FieldId, FieldsIdsMap};

use super::{MergeFn, create_writer, create_sorter, writer_into_reader};
use super::merge_function::{
    main_merge, word_docids_merge, words_pairs_proximities_docids_merge,
    facet_field_value_docids_merge, field_id_docid_facet_values_merge,
};

const LMDB_MAX_KEY_LENGTH: usize = 511;
const ONE_KILOBYTE: usize = 1024 * 1024;

const MAX_POSITION: usize = 1000;
const WORDS_FST_KEY: &[u8] = crate::index::WORDS_FST_KEY.as_bytes();

pub struct Readers {
    pub main: Reader<FileFuse>,
    pub word_docids: Reader<FileFuse>,
    pub docid_word_positions: Reader<FileFuse>,
    pub words_pairs_proximities_docids: Reader<FileFuse>,
    pub facet_field_value_docids: Reader<FileFuse>,
    pub field_id_docid_facet_values: Reader<FileFuse>,
    pub documents: Reader<FileFuse>,
}

pub struct Store<'s, A> {
    // Indexing parameters
    primary_key: String,
    fields_ids_map: FieldsIdsMap,
    searchable_fields: HashSet<FieldId>,
    faceted_fields: HashMap<FieldId, FacetType>,
    // Caches
    word_docids: LinkedHashMap<SmallVec32<u8>, RoaringBitmap>,
    word_docids_limit: usize,
    words_pairs_proximities_docids: LinkedHashMap<(SmallVec32<u8>, SmallVec32<u8>, u8), RoaringBitmap>,
    words_pairs_proximities_docids_limit: usize,
    facet_field_value_docids: LinkedHashMap<(u8, FacetValue), RoaringBitmap>,
    facet_field_value_docids_limit: usize,
    // MTBL parameters
    chunk_compression_type: CompressionType,
    chunk_compression_level: Option<u32>,
    chunk_fusing_shrink_size: Option<u64>,
    // MTBL sorters
    main_sorter: Sorter<MergeFn>,
    word_docids_sorter: Sorter<MergeFn>,
    words_pairs_proximities_docids_sorter: Sorter<MergeFn>,
    facet_field_value_docids_sorter: Sorter<MergeFn>,
    field_id_docid_facet_values_sorter: Sorter<MergeFn>,
    // MTBL writers
    docid_word_positions_writer: Writer<File>,
    documents_writer: Writer<File>,
    // tokenizer
    analyzer: Analyzer<'s, A>,
}

impl<'s, A: AsRef<[u8]>> Store<'s, A> {
    pub fn new(
        primary_key: String,
        fields_ids_map: FieldsIdsMap,
        searchable_fields: HashSet<FieldId>,
        faceted_fields: HashMap<FieldId, FacetType>,
        linked_hash_map_size: Option<usize>,
        max_nb_chunks: Option<usize>,
        max_memory: Option<usize>,
        chunk_compression_type: CompressionType,
        chunk_compression_level: Option<u32>,
        chunk_fusing_shrink_size: Option<u64>,
        stop_words: Option<&'s Set<A>>,
    ) -> anyhow::Result<Self>
    {
        // We divide the max memory by the number of sorter the Store have.
        let max_memory = max_memory.map(|mm| cmp::max(ONE_KILOBYTE, mm / 4));
        let linked_hash_map_size = linked_hash_map_size.unwrap_or(500);

        let main_sorter = create_sorter(
            main_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let word_docids_sorter = create_sorter(
            word_docids_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let words_pairs_proximities_docids_sorter = create_sorter(
            words_pairs_proximities_docids_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let facet_field_value_docids_sorter = create_sorter(
            facet_field_value_docids_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let field_id_docid_facet_values_sorter = create_sorter(
            field_id_docid_facet_values_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            Some(1024 * 1024 * 1024), // 1MB
        );

        let documents_writer = tempfile().and_then(|f| {
            create_writer(chunk_compression_type, chunk_compression_level, f)
        })?;
        let docid_word_positions_writer = tempfile().and_then(|f| {
            create_writer(chunk_compression_type, chunk_compression_level, f)
        })?;

        let mut config = AnalyzerConfig::default();
        if let Some(stop_words) = stop_words {
            config.stop_words(stop_words);
        }
        let analyzer = Analyzer::new(config);

        Ok(Store {
            // Indexing parameters.
            primary_key,
            fields_ids_map,
            searchable_fields,
            faceted_fields,
            // Caches
            word_docids: LinkedHashMap::with_capacity(linked_hash_map_size),
            word_docids_limit: linked_hash_map_size,
            words_pairs_proximities_docids: LinkedHashMap::with_capacity(linked_hash_map_size),
            words_pairs_proximities_docids_limit: linked_hash_map_size,
            facet_field_value_docids: LinkedHashMap::with_capacity(linked_hash_map_size),
            facet_field_value_docids_limit: linked_hash_map_size,
            // MTBL parameters
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            // MTBL sorters
            main_sorter,
            word_docids_sorter,
            words_pairs_proximities_docids_sorter,
            facet_field_value_docids_sorter,
            field_id_docid_facet_values_sorter,
            // MTBL writers
            docid_word_positions_writer,
            documents_writer,
            // tokenizer
            analyzer,
        })
    }

    // Save the documents ids under the position and word we have seen it.
    fn insert_word_docid(&mut self, word: &str, id: DocumentId) -> anyhow::Result<()> {
        // if get_refresh finds the element it is assured to be at the end of the linked hash map.
        match self.word_docids.get_refresh(word.as_bytes()) {
            Some(old) => { old.insert(id); },
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

    // Save the documents ids under the facet field id and value we have seen it.
    fn insert_facet_values_docid(
        &mut self,
        field_id: FieldId,
        field_value: FacetValue,
        id: DocumentId,
    ) -> anyhow::Result<()>
    {
        Self::write_field_id_docid_facet_value(&mut self.field_id_docid_facet_values_sorter, field_id, id, &field_value)?;

        let key = (field_id, field_value);
        // if get_refresh finds the element it is assured to be at the end of the linked hash map.
        match self.facet_field_value_docids.get_refresh(&key) {
            Some(old) => { old.insert(id); },
            None => {
                // A newly inserted element is append at the end of the linked hash map.
                self.facet_field_value_docids.insert(key, RoaringBitmap::from_iter(Some(id)));
                // If the word docids just reached it's capacity we must make sure to remove
                // one element, this way next time we insert we doesn't grow the capacity.
                if self.facet_field_value_docids.len() == self.facet_field_value_docids_limit {
                    // Removing the front element is equivalent to removing the LRU element.
                    Self::write_facet_field_value_docids(
                        &mut self.facet_field_value_docids_sorter,
                        self.facet_field_value_docids.pop_front(),
                    )?;
                }
            }
        }
        Ok(())
    }

    // Save the documents ids under the words pairs proximities that it contains.
    fn insert_words_pairs_proximities_docids<'a>(
        &mut self,
        words_pairs_proximities: impl IntoIterator<Item=((&'a str, &'a str), u8)>,
        id: DocumentId,
    ) -> anyhow::Result<()>
    {
        for ((w1, w2), prox) in words_pairs_proximities {
            let w1 = SmallVec32::from(w1.as_bytes());
            let w2 = SmallVec32::from(w2.as_bytes());
            let key = (w1, w2, prox);
            // if get_refresh finds the element it is assured
            // to be at the end of the linked hash map.
            match self.words_pairs_proximities_docids.get_refresh(&key) {
                Some(old) => { old.insert(id); },
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
            Self::write_words_pairs_proximities(&mut self.words_pairs_proximities_docids_sorter, lrus)?;
        }

        Ok(())
    }

    fn write_document(
        &mut self,
        document_id: DocumentId,
        words_positions: &mut HashMap<String, SmallVec32<Position>>,
        facet_values: &mut HashMap<FieldId, SmallVec8<FacetValue>>,
        record: &[u8],
    ) -> anyhow::Result<()>
    {
        // We compute the list of words pairs proximities (self-join) and write it directly to disk.
        let words_pair_proximities = compute_words_pair_proximities(&words_positions);
        self.insert_words_pairs_proximities_docids(words_pair_proximities, document_id)?;

        // We store document_id associated with all the words the record contains.
        for (word, _) in words_positions.iter() {
            self.insert_word_docid(word, document_id)?;
        }

        self.documents_writer.insert(document_id.to_be_bytes(), record)?;
        Self::write_docid_word_positions(&mut self.docid_word_positions_writer, document_id, words_positions)?;

        words_positions.clear();

        // We store document_id associated with all the field id and values.
        for (field, values) in facet_values.drain() {
            for value in values {
                self.insert_facet_values_docid(field, value, document_id)?;
            }
        }

        Ok(())
    }

    fn write_words_pairs_proximities(
        sorter: &mut Sorter<MergeFn>,
        iter: impl IntoIterator<Item=((SmallVec32<u8>, SmallVec32<u8>, u8), RoaringBitmap)>,
    ) -> anyhow::Result<()>
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
            CboRoaringBitmapCodec::serialize_into(&docids, &mut buffer)?;
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &buffer)?;
            }
        }

        Ok(())
    }

    fn write_docid_word_positions(
        writer: &mut Writer<File>,
        id: DocumentId,
        words_positions: &HashMap<String, SmallVec32<Position>>,
    ) -> anyhow::Result<()>
    {
        // We prefix the words by the document id.
        let mut key = id.to_be_bytes().to_vec();
        let base_size = key.len();

        // We order the words lexicographically, this way we avoid passing by a sorter.
        let words_positions = BTreeMap::from_iter(words_positions);

        for (word, positions) in words_positions {
            key.truncate(base_size);
            key.extend_from_slice(word.as_bytes());
            // We serialize the positions into a buffer.
            let positions = RoaringBitmap::from_iter(positions.iter().cloned());
            let bytes = BoRoaringBitmapCodec::bytes_encode(&positions)
                .with_context(|| "could not serialize positions")?;
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                writer.insert(&key, &bytes)?;
            }
        }

        Ok(())
    }

    fn write_facet_field_value_docids<I>(
        sorter: &mut Sorter<MergeFn>,
        iter: I,
    ) -> anyhow::Result<()>
    where I: IntoIterator<Item=((FieldId, FacetValue), RoaringBitmap)>
    {
        use FacetValue::*;

        for ((field_id, value), docids) in iter {
            let result = match value {
                String(s) => FacetValueStringCodec::bytes_encode(&(field_id, &s)).map(Cow::into_owned),
                Number(f) => FacetLevelValueF64Codec::bytes_encode(&(field_id, 0, *f, *f)).map(Cow::into_owned),
            };
            let key = result.context("could not serialize facet key")?;
            let bytes = CboRoaringBitmapCodec::bytes_encode(&docids)
                .context("could not serialize docids")?;
            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &bytes)?;
            }
        }

        Ok(())
    }

    fn write_field_id_docid_facet_value(
        sorter: &mut Sorter<MergeFn>,
        field_id: FieldId,
        document_id: DocumentId,
        value: &FacetValue,
    ) -> anyhow::Result<()>
    {
        use FacetValue::*;

        let result = match value {
            String(s) => FieldDocIdFacetStringCodec::bytes_encode(&(field_id, document_id, s)).map(Cow::into_owned),
            Number(f) => FieldDocIdFacetF64Codec::bytes_encode(&(field_id, document_id, **f)).map(Cow::into_owned),
        };

        let key = result.context("could not serialize facet key")?;
        if lmdb_key_valid_size(&key) {
            sorter.insert(&key, &[])?;
        }

        Ok(())
    }

    fn write_word_docids<I>(sorter: &mut Sorter<MergeFn>, iter: I) -> anyhow::Result<()>
    where I: IntoIterator<Item=(SmallVec32<u8>, RoaringBitmap)>
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
    ) -> anyhow::Result<Readers>
    where F: FnMut(UpdateIndexingStep),
    {
        debug!("{:?}: Indexing in a Store...", thread_index);

        let mut before = Instant::now();
        let mut words_positions = HashMap::new();
        let mut facet_values = HashMap::new();

        let mut count: usize = 0;
        while let Some((key, value)) = documents.next()? {
            let document_id = key.try_into().map(u32::from_be_bytes).unwrap();
            let document = obkv::KvReader::new(value);

            // We skip documents that must not be indexed by this thread.
            if count % num_threads == thread_index {
                // This is a log routine that we do every `log_every_n` documents.
                if thread_index == 0 && log_every_n.map_or(false, |len| count % len == 0) {
                    info!("We have seen {} documents so far ({:.02?}).", format_count(count), before.elapsed());
                    progress_callback(UpdateIndexingStep::IndexDocuments {
                        documents_seen: count,
                        total_documents: documents_count,
                    });
                    before = Instant::now();
                }

                for (attr, content) in document.iter() {
                    if self.faceted_fields.contains_key(&attr) || self.searchable_fields.contains(&attr) {
                        let value = serde_json::from_slice(content)?;

                        if let Some(ftype) = self.faceted_fields.get(&attr) {
                            let mut values = match parse_facet_value(*ftype, &value) {
                                Ok(values) => values,
                                Err(e) => {
                                    // We extract the name of the attribute and the document id
                                    // to help users debug a facet type conversion.
                                    let attr_name = self.fields_ids_map.name(attr).unwrap();
                                    let document_id: Value = self.fields_ids_map.id(&self.primary_key)
                                        .and_then(|fid| document.get(fid))
                                        .map(serde_json::from_slice)
                                        .unwrap()?;

                                    let context = format!(
                                        "while extracting facet from the {:?} attribute in the {} document",
                                        attr_name, document_id,
                                    );
                                    warn!("{}", e.context(context));

                                    SmallVec8::default()
                                },
                            };
                            facet_values.entry(attr).or_insert_with(SmallVec8::new).extend(values.drain(..));
                        }

                        if self.searchable_fields.contains(&attr) {
                            let content = match json_to_string(&value) {
                                Some(content) => content,
                                None => continue,
                            };

                            let analyzed = self.analyzer.analyze(&content);
                            let tokens = process_tokens(analyzed.tokens());

                            for (pos, token) in tokens.take_while(|(pos, _)| *pos < MAX_POSITION) {
                                let position = (attr as usize * MAX_POSITION + pos) as u32;
                                words_positions.entry(token.text().to_string()).or_insert_with(SmallVec32::new).push(position);
                            }
                        }
                    }
                }

                // We write the document in the documents store.
                self.write_document(document_id, &mut words_positions, &mut facet_values, value)?;
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

    fn finish(mut self) -> anyhow::Result<Readers> {
        let comp_type = self.chunk_compression_type;
        let comp_level = self.chunk_compression_level;
        let shrink_size = self.chunk_fusing_shrink_size;

        Self::write_word_docids(&mut self.word_docids_sorter, self.word_docids)?;
        Self::write_words_pairs_proximities(
            &mut self.words_pairs_proximities_docids_sorter,
            self.words_pairs_proximities_docids,
        )?;
        Self::write_facet_field_value_docids(
            &mut self.facet_field_value_docids_sorter,
            self.facet_field_value_docids,
        )?;

        let mut word_docids_wtr = tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        let mut builder = fst::SetBuilder::memory();

        let mut iter = self.word_docids_sorter.into_iter()?;
        while let Some((word, val)) = iter.next()? {
            // This is a lexicographically ordered word position
            // we use the key to construct the words fst.
            builder.insert(word)?;
            word_docids_wtr.insert(word, val)?;
        }

        let fst = builder.into_set();
        self.main_sorter.insert(WORDS_FST_KEY, fst.as_fst().as_bytes())?;

        let mut main_wtr = tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.main_sorter.write_into(&mut main_wtr)?;

        let mut words_pairs_proximities_docids_wtr = tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.words_pairs_proximities_docids_sorter.write_into(&mut words_pairs_proximities_docids_wtr)?;

        let mut facet_field_value_docids_wtr = tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.facet_field_value_docids_sorter.write_into(&mut facet_field_value_docids_wtr)?;

        let mut field_id_docid_facet_values_wtr = tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.field_id_docid_facet_values_sorter.write_into(&mut field_id_docid_facet_values_wtr)?;

        let main = writer_into_reader(main_wtr, shrink_size)?;
        let word_docids = writer_into_reader(word_docids_wtr, shrink_size)?;
        let words_pairs_proximities_docids = writer_into_reader(words_pairs_proximities_docids_wtr, shrink_size)?;
        let facet_field_value_docids = writer_into_reader(facet_field_value_docids_wtr, shrink_size)?;
        let field_id_docid_facet_values = writer_into_reader(field_id_docid_facet_values_wtr, shrink_size)?;
        let docid_word_positions = writer_into_reader(self.docid_word_positions_writer, shrink_size)?;
        let documents = writer_into_reader(self.documents_writer, shrink_size)?;

        Ok(Readers {
            main,
            word_docids,
            docid_word_positions,
            words_pairs_proximities_docids,
            facet_field_value_docids,
            field_id_docid_facet_values,
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
) -> HashMap<(&str, &str), u8>
{
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
fn process_tokens<'a>(tokens: impl Iterator<Item = Token<'a>>) -> impl Iterator<Item = (usize, Token<'a>)> {
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
                        if *prev_kind != Some(TokenKind::Separator(SeparatorKind::Hard)) => {
                        *prev_kind = Some(token.kind);
                    }
                    _ => (),
                }
            Some((*offset, token))
        })
    .filter(|(_, t)| t.is_word())
}

fn parse_facet_value(ftype: FacetType, value: &Value) -> anyhow::Result<SmallVec8<FacetValue>> {
    use FacetValue::*;

    fn inner_parse_facet_value(
        ftype: FacetType,
        value: &Value,
        can_recurse: bool,
        output: &mut SmallVec8<FacetValue>,
    ) -> anyhow::Result<()>
    {
        match value {
            Value::Null => Ok(()),
            Value::Bool(b) => match ftype {
                FacetType::String => {
                    output.push(String(b.to_string()));
                    Ok(())
                },
                FacetType::Number => {
                    output.push(Number(OrderedFloat(if *b { 1.0 } else { 0.0 })));
                    Ok(())
                },
            },
            Value::Number(number) => match ftype {
                FacetType::String => {
                    output.push(String(number.to_string()));
                    Ok(())
                },
                FacetType::Number => match number.as_f64() {
                    Some(float) => {
                        output.push(Number(OrderedFloat(float)));
                        Ok(())
                    },
                    None => bail!("invalid facet type, expecting {} found number", ftype),
                },
            },
            Value::String(string) => {
                // TODO must be normalized and not only lowercased.
                let string = string.trim().to_lowercase();
                match ftype {
                    FacetType::String => {
                        output.push(String(string));
                        Ok(())
                    },
                    FacetType::Number => match string.parse() {
                        Ok(float) => {
                            output.push(Number(OrderedFloat(float)));
                            Ok(())
                        },
                        Err(_err) => bail!("invalid facet type, expecting {} found string", ftype),
                    },
                }
            },
            Value::Array(values) => if can_recurse {
                values.iter().map(|v| inner_parse_facet_value(ftype, v, false, output)).collect()
            } else {
                bail!(
                    "invalid facet type, expecting {} found array (recursive arrays are not supported)",
                    ftype,
                );
            },
            Value::Object(_) => bail!("invalid facet type, expecting {} found object", ftype),
        }
    }

    let mut facet_values = SmallVec8::new();
    inner_parse_facet_value(ftype, value, true, &mut facet_values)?;
    Ok(facet_values)
}
