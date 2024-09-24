mod extract_fid_word_count_docids;
mod extract_word_docids;
mod extract_word_pair_proximity_docids;
mod tokenize_document;

use std::fs::File;

pub use extract_word_docids::{WordDocidsExtractors, WordDocidsMergers};
pub use extract_word_pair_proximity_docids::WordPairProximityDocidsExtractor;
use grenad::Merger;
use heed::RoTxn;
use rayon::iter::{IntoParallelIterator, ParallelBridge, ParallelIterator};
use tokenize_document::{tokenizer_builder, DocumentTokenizer};

use super::cache::CboCachedSorter;
use super::DocidsExtractor;
use crate::update::new::{DocumentChange, ItemsPool};
use crate::update::{create_sorter, GrenadParameters, MergeDeladdCboRoaringBitmaps};
use crate::{GlobalFieldsIdsMap, Index, Result, MAX_POSITION_PER_ATTRIBUTE};

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
                    1_000_000.try_into().unwrap(),
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

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "docids_extraction");
            let _entered = span.enter();
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
        }
        {
            let mut builder = grenad::MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "merger_building");
            let _entered = span.enter();

            let readers: Vec<_> = context_pool
                .into_items()
                .par_bridge()
                .map(|(_rtxn, _tokenizer, _fields_ids_map, cached_sorter)| {
                    let sorter = cached_sorter.into_sorter()?;
                    sorter.into_reader_cursors()
                })
                .collect();
            for reader in readers {
                builder.extend(reader?);
            }
            Ok(builder.build())
        }
    }

    fn extract_document_change(
        rtxn: &RoTxn,
        index: &Index,
        document_tokenizer: &DocumentTokenizer,
        fields_ids_map: &mut GlobalFieldsIdsMap,
        cached_sorter: &mut CboCachedSorter<MergeDeladdCboRoaringBitmaps>,
        document_change: DocumentChange,
    ) -> Result<()>;

    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index)
        -> Result<Option<Vec<&'a str>>>;

    fn attributes_to_skip<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<Vec<&'a str>>;
}

impl<T: SearchableExtractor> DocidsExtractor for T {
    fn run_extraction(
        index: &Index,
        fields_ids_map: &GlobalFieldsIdsMap,
        indexer: GrenadParameters,
        document_changes: impl IntoParallelIterator<Item = Result<DocumentChange>>,
    ) -> Result<Merger<File, MergeDeladdCboRoaringBitmaps>> {
        Self::run_extraction(index, fields_ids_map, indexer, document_changes)
    }
}
