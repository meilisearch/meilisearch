mod extract_word_docids;
mod tokenize_document;

use std::borrow::Cow;
use std::fs::File;

pub use extract_word_docids::{
    ExactWordDocidsExtractor, WordDocidsExtractor, WordFidDocidsExtractor,
    WordPositionDocidsExtractor,
};
use grenad::Merger;
use heed::RoTxn;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokenize_document::{tokenizer_builder, DocumentTokenizer};

use super::cache::CboCachedSorter;
use crate::update::new::{DocumentChange, ItemsPool};
use crate::update::{create_sorter, GrenadParameters, MergeDeladdCboRoaringBitmaps};
use crate::{FieldId, GlobalFieldsIdsMap, Index, Result, MAX_POSITION_PER_ATTRIBUTE};

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

        let attributes_to_extract = Self::attributes_to_extract(&rtxn, index)?;
        let attributes_to_skip = Self::attributes_to_skip(&rtxn, index)?;
        let localized_attributes_rules =
            index.localized_attributes_rules(&rtxn)?.unwrap_or_default();

        let document_tokenizer = DocumentTokenizer {
            tokenizer: &tokenizer,
            attribute_to_extract: attributes_to_extract.as_deref(),
            attribute_to_skip: attributes_to_skip.as_slice(),
            localized_attributes_rules: &localized_attributes_rules,
            max_positions_per_attributes: MAX_POSITION_PER_ATTRIBUTE,
        };

        let context_pool = ItemsPool::new(|| {
            Ok((
                index.read_txn()?,
                &document_tokenizer,
                fields_ids_map.clone(),
                CboCachedSorter::new(
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
        cached_sorter: &mut CboCachedSorter<MergeDeladdCboRoaringBitmaps>,
        document_change: DocumentChange,
    ) -> Result<()> {
        match document_change {
            DocumentChange::Deletion(inner) => {
                let mut token_fn = |fid, pos: u16, word: &str| {
                    let key = Self::build_key(fid, pos, word);
                    /// TODO manage the error
                    cached_sorter.insert_del_u32(&key, inner.docid()).unwrap();
                    Ok(())
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index)?.unwrap(),
                    fields_ids_map,
                    &mut token_fn,
                )?;
            }
            DocumentChange::Update(inner) => {
                let mut token_fn = |fid, pos, word: &str| {
                    let key = Self::build_key(fid, pos, word);
                    /// TODO manage the error
                    cached_sorter.insert_del_u32(&key, inner.docid()).unwrap();
                    Ok(())
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index)?.unwrap(),
                    fields_ids_map,
                    &mut token_fn,
                )?;

                let mut token_fn = |fid, pos, word: &str| {
                    let key = Self::build_key(fid, pos, word);
                    /// TODO manage the error
                    cached_sorter.insert_add_u32(&key, inner.docid()).unwrap();
                    Ok(())
                };
                document_tokenizer.tokenize_document(inner.new(), fields_ids_map, &mut token_fn)?;
            }
            DocumentChange::Insertion(inner) => {
                let mut token_fn = |fid, pos, word: &str| {
                    let key = Self::build_key(fid, pos, word);
                    /// TODO manage the error
                    cached_sorter.insert_add_u32(&key, inner.docid()).unwrap();
                    Ok(())
                };
                document_tokenizer.tokenize_document(inner.new(), fields_ids_map, &mut token_fn)?;
            }
        }

        Ok(())
    }

    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index)
        -> Result<Option<Vec<&'a str>>>;

    fn attributes_to_skip<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<Vec<&'a str>>;

    fn build_key(field_id: FieldId, position: u16, word: &str) -> Cow<'_, [u8]>;
}
