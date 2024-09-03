use std::fs::File;

use charabia::TokenizerBuilder;
use grenad::{Merger, ReaderCursor};
use heed::RoTxn;
use rayon::iter::{IntoParallelIterator, ParallelBridge, ParallelIterator};

use super::cache::CachedSorter;
use super::tokenize_document::DocumentTokenizer;
use crate::update::new::{DocumentChange, ItemsPool};
use crate::update::{create_sorter, GrenadParameters, MergeDeladdCboRoaringBitmaps};
use crate::{FieldsIdsMap, GlobalFieldsIdsMap, Index, Result, MAX_POSITION_PER_ATTRIBUTE};

pub trait SearchableExtractor {
    fn run_extraction(
        index: &Index,
        fields_ids_map: &GlobalFieldsIdsMap,
        indexer: GrenadParameters,
        document_changes: impl IntoParallelIterator<Item = Result<DocumentChange>>,
    ) -> Result<Merger<File, MergeDeladdCboRoaringBitmaps>> {
        let max_memory = indexer.max_memory_by_thread();

        let rtxn = index.read_txn()?;
        let stop_words = index.stop_words(&rtxn)?;
        let allowed_separators = index.allowed_separators(&rtxn)?;
        let allowed_separators: Option<Vec<_>> =
            allowed_separators.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let dictionary = index.dictionary(&rtxn)?;
        let dictionary: Option<Vec<_>> =
            dictionary.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let builder = tokenizer_builder(
            stop_words.as_ref(),
            allowed_separators.as_deref(),
            dictionary.as_deref(),
        );
        let tokenizer = builder.into_tokenizer();

        let user_defined_searchable_fields = index.user_defined_searchable_fields(&rtxn)?;
        let localized_attributes_rules =
            index.localized_attributes_rules(&rtxn)?.unwrap_or_default();

        let document_tokenizer = DocumentTokenizer {
            tokenizer: &tokenizer,
            searchable_attributes: user_defined_searchable_fields.as_deref(),
            localized_attributes_rules: &localized_attributes_rules,
            max_positions_per_attributes: MAX_POSITION_PER_ATTRIBUTE,
        };

        let context_pool = ItemsPool::new(|| {
            Ok((
                index.read_txn()?,
                &document_tokenizer,
                fields_ids_map.clone(),
                CachedSorter::new(
                    // TODO use a better value
                    100.try_into().unwrap(),
                    create_sorter(
                        grenad::SortAlgorithm::Stable,
                        MergeDeladdCboRoaringBitmaps,
                        indexer.chunk_compression_type,
                        indexer.chunk_compression_level,
                        indexer.max_nb_chunks,
                        max_memory,
                    ),
                ),
            ))
        });

        document_changes.into_par_iter().try_for_each(|document_change| {
            context_pool.with(|(rtxn, document_tokenizer, fields_ids_map, cached_sorter)| {
                Self::extract_document_change(
                    &*rtxn,
                    index,
                    document_tokenizer,
                    fields_ids_map,
                    cached_sorter,
                    document_change?,
                )
            })
        })?;

        let mut builder = grenad::MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
        for (_rtxn, _tokenizer, _fields_ids_map, cache) in context_pool.into_items() {
            let sorter = cache.into_sorter()?;
            let readers = sorter.into_reader_cursors()?;
            builder.extend(readers);
        }

        Ok(builder.build())
    }

    fn extract_document_change(
        rtxn: &RoTxn,
        index: &Index,
        document_tokenizer: &DocumentTokenizer,
        fields_ids_map: &mut GlobalFieldsIdsMap,
        cached_sorter: &mut CachedSorter<MergeDeladdCboRoaringBitmaps>,
        document_change: DocumentChange,
    ) -> Result<()>;
}

pub struct WordDocidsExtractor;
impl SearchableExtractor for WordDocidsExtractor {
    fn extract_document_change(
        rtxn: &RoTxn,
        index: &Index,
        document_tokenizer: &DocumentTokenizer,
        fields_ids_map: &mut GlobalFieldsIdsMap,
        cached_sorter: &mut CachedSorter<MergeDeladdCboRoaringBitmaps>,
        document_change: DocumentChange,
    ) -> crate::Result<()> {
        match document_change {
            DocumentChange::Deletion(inner) => {
                let mut token_fn = |_fid, _pos: u16, word: &str| {
                    cached_sorter.insert_del_u32(word.as_bytes(), inner.docid()).unwrap();
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index),
                    fields_ids_map,
                    &mut token_fn,
                )?;
            }
            DocumentChange::Update(inner) => {
                let mut token_fn = |_fid, _pos, word: &str| {
                    cached_sorter.insert_del_u32(word.as_bytes(), inner.docid()).unwrap();
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index),
                    fields_ids_map,
                    &mut token_fn,
                )?;

                let mut token_fn = |_fid, _pos, word: &str| {
                    cached_sorter.insert_add_u32(word.as_bytes(), inner.docid()).unwrap();
                };
                document_tokenizer.tokenize_document(inner.new(), fields_ids_map, &mut token_fn)?;
            }
            DocumentChange::Insertion(inner) => {
                let mut token_fn = |_fid, _pos, word: &str| {
                    cached_sorter.insert_add_u32(word.as_bytes(), inner.docid()).unwrap();
                };
                document_tokenizer.tokenize_document(inner.new(), fields_ids_map, &mut token_fn)?;
            }
        }

        Ok(())
    }
}

/// Factorize tokenizer building.
fn tokenizer_builder<'a>(
    stop_words: Option<&'a fst::Set<&'a [u8]>>,
    allowed_separators: Option<&'a [&str]>,
    dictionary: Option<&'a [&str]>,
) -> TokenizerBuilder<'a, &'a [u8]> {
    let mut tokenizer_builder = TokenizerBuilder::new();
    if let Some(stop_words) = stop_words {
        tokenizer_builder.stop_words(stop_words);
    }
    if let Some(dictionary) = dictionary {
        tokenizer_builder.words_dict(dictionary);
    }
    if let Some(separators) = allowed_separators {
        tokenizer_builder.separators(separators);
    }

    tokenizer_builder
}
