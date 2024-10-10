mod extract_word_docids;
mod extract_word_pair_proximity_docids;
mod tokenize_document;

use std::cell::RefCell;
use std::fs::File;
use std::marker::PhantomData;

use bumpalo::Bump;
pub use extract_word_docids::{WordDocidsExtractors, WordDocidsMergers};
pub use extract_word_pair_proximity_docids::WordPairProximityDocidsExtractor;
use grenad::Merger;
use heed::RoTxn;
use rayon::iter::{ParallelBridge, ParallelIterator};
use tokenize_document::{tokenizer_builder, DocumentTokenizer};

use super::cache::CboCachedSorter;
use super::DocidsExtractor;
use crate::update::new::indexer::document_changes::{
    for_each_document_change, DocumentChangeContext, DocumentChanges, Extractor, FullySend,
    IndexingContext, ThreadLocal,
};
use crate::update::new::DocumentChange;
use crate::update::{create_sorter, GrenadParameters, MergeDeladdCboRoaringBitmaps};
use crate::{Index, Result, MAX_POSITION_PER_ATTRIBUTE};

pub struct SearchableExtractorData<'extractor, EX: SearchableExtractor> {
    tokenizer: &'extractor DocumentTokenizer<'extractor>,
    grenad_parameters: GrenadParameters,
    max_memory: Option<usize>,
    _ex: PhantomData<EX>,
}

impl<'extractor, EX: SearchableExtractor + Sync> Extractor<'extractor>
    for SearchableExtractorData<'extractor, EX>
{
    type Data = FullySend<RefCell<CboCachedSorter<MergeDeladdCboRoaringBitmaps>>>;

    fn init_data(
        &self,
        _extractor_alloc: raw_collections::alloc::RefBump<'extractor>,
    ) -> Result<Self::Data> {
        Ok(FullySend(RefCell::new(CboCachedSorter::new(
            // TODO use a better value
            1_000_000.try_into().unwrap(),
            create_sorter(
                grenad::SortAlgorithm::Stable,
                MergeDeladdCboRoaringBitmaps,
                self.grenad_parameters.chunk_compression_type,
                self.grenad_parameters.chunk_compression_level,
                self.grenad_parameters.max_nb_chunks,
                self.max_memory,
            ),
        ))))
    }

    fn process(
        &self,
        change: DocumentChange,
        context: &crate::update::new::indexer::document_changes::DocumentChangeContext<Self::Data>,
    ) -> Result<()> {
        EX::extract_document_change(context, self.tokenizer, change)
    }
}

pub trait SearchableExtractor: Sized + Sync {
    fn run_extraction<'pl, 'fid, 'indexer, 'index, DC: DocumentChanges<'pl>>(
        grenad_parameters: GrenadParameters,
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index>,
        extractor_allocs: &mut ThreadLocal<FullySend<RefCell<Bump>>>,
    ) -> Result<Merger<File, MergeDeladdCboRoaringBitmaps>> {
        let max_memory = grenad_parameters.max_memory_by_thread();

        let rtxn = indexing_context.index.read_txn()?;
        let stop_words = indexing_context.index.stop_words(&rtxn)?;
        let allowed_separators = indexing_context.index.allowed_separators(&rtxn)?;
        let allowed_separators: Option<Vec<_>> =
            allowed_separators.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let dictionary = indexing_context.index.dictionary(&rtxn)?;
        let dictionary: Option<Vec<_>> =
            dictionary.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let builder = tokenizer_builder(
            stop_words.as_ref(),
            allowed_separators.as_deref(),
            dictionary.as_deref(),
        );
        let tokenizer = builder.into_tokenizer();

        let attributes_to_extract = Self::attributes_to_extract(&rtxn, indexing_context.index)?;
        let attributes_to_skip = Self::attributes_to_skip(&rtxn, indexing_context.index)?;
        let localized_attributes_rules =
            indexing_context.index.localized_attributes_rules(&rtxn)?.unwrap_or_default();

        let document_tokenizer = DocumentTokenizer {
            tokenizer: &tokenizer,
            attribute_to_extract: attributes_to_extract.as_deref(),
            attribute_to_skip: attributes_to_skip.as_slice(),
            localized_attributes_rules: &localized_attributes_rules,
            max_positions_per_attributes: MAX_POSITION_PER_ATTRIBUTE,
        };

        let extractor_data: SearchableExtractorData<Self> = SearchableExtractorData {
            tokenizer: &document_tokenizer,
            grenad_parameters,
            max_memory,
            _ex: PhantomData,
        };

        let datastore = ThreadLocal::new();

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "docids_extraction");
            let _entered = span.enter();
            for_each_document_change(
                document_changes,
                &extractor_data,
                indexing_context,
                extractor_allocs,
                &datastore,
            )?;
        }
        {
            let mut builder = grenad::MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "merger_building");
            let _entered = span.enter();

            let readers: Vec<_> = datastore
                .into_iter()
                .par_bridge()
                .map(|cache_entry| {
                    let cached_sorter: FullySend<
                        RefCell<CboCachedSorter<MergeDeladdCboRoaringBitmaps>>,
                    > = cache_entry;
                    let cached_sorter = cached_sorter.0.into_inner();
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
        context: &DocumentChangeContext<
            FullySend<RefCell<CboCachedSorter<MergeDeladdCboRoaringBitmaps>>>,
        >,
        document_tokenizer: &DocumentTokenizer,
        document_change: DocumentChange,
    ) -> Result<()>;

    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index)
        -> Result<Option<Vec<&'a str>>>;

    fn attributes_to_skip<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<Vec<&'a str>>;
}

impl<T: SearchableExtractor> DocidsExtractor for T {
    fn run_extraction<'pl, 'fid, 'indexer, 'index, DC: DocumentChanges<'pl>>(
        grenad_parameters: GrenadParameters,
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index>,
        extractor_allocs: &mut ThreadLocal<FullySend<RefCell<Bump>>>,
    ) -> Result<Merger<File, MergeDeladdCboRoaringBitmaps>> {
        Self::run_extraction(
            grenad_parameters,
            document_changes,
            indexing_context,
            extractor_allocs,
        )
    }
}
