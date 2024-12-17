use std::cell::RefCell;
use std::sync::atomic::{self, AtomicBool, AtomicUsize};
use std::sync::RwLock;
use std::thread::{self, Builder};

use big_s::S;
use bumpalo::Bump;
pub use document_changes::{extract, DocumentChanges, IndexingContext};
use document_changes::{DocumentChangeContext, Extractor};
use bumparaw_collections::RawMap;
pub use document_deletion::DocumentDeletion;
pub use document_operation::{DocumentOperation, PayloadStats};
use hashbrown::HashMap;
use heed::{RoTxn, RwTxn};
pub use partial_dump::PartialDump;
pub use update_by_function::UpdateByFunction;
use write::{build_vectors, update_index, write_to_db};
use zstd::dict::{DecoderDictionary, EncoderDictionary};

use super::document::Document as _;
use super::extract::*;
use super::ref_cell_ext::RefCellExt as _;
use super::steps::IndexingStep;
use super::thread_local::{FullySend, MostlySend, ThreadLocal};

use super::{channel::*, DocumentChange};
use crate::documents::PrimaryKey;
use crate::fields_ids_map::metadata::{FieldIdMapWithMetadata, MetadataBuilder};

use crate::progress::Progress;

use crate::update::GrenadParameters;
use crate::vector::{ArroyWrapper, EmbeddingConfigs};
use crate::{FieldsIdsMap, GlobalFieldsIdsMap, Index, InternalError, Result, ThreadPoolNoAbort};

pub(crate) mod de;
pub mod document_changes;
mod document_deletion;
mod document_operation;
mod extract;
mod guess_primary_key;
mod partial_dump;
mod post_processing;
mod update_by_function;
mod write;

/// This is the main function of this crate.
///
/// Give it the output of the [`Indexer::document_changes`] method and it will execute it in the [`rayon::ThreadPool`].
///
/// TODO return stats
#[allow(clippy::too_many_arguments)] // clippy: 😝
pub fn index<'pl, 'indexer, 'index, DC, MSP>(
    wtxn: &mut RwTxn,
    index: &'index Index,
    pool: &ThreadPoolNoAbort,
    grenad_parameters: GrenadParameters,
    db_fields_ids_map: &'indexer FieldsIdsMap,
    new_fields_ids_map: FieldsIdsMap,
    new_primary_key: Option<PrimaryKey<'pl>>,
    document_changes: &DC,
    embedders: EmbeddingConfigs,
    must_stop_processing: &'indexer MSP,
    progress: &'indexer Progress,
) -> Result<()>
where
    DC: DocumentChanges<'pl>,
    MSP: Fn() -> bool + Sync,
{
    let mut bbbuffers = Vec::new();
    let finished_extraction = AtomicBool::new(false);

    // We reduce the actual memory used to 5%. The reason we do this here and not in Meilisearch
    // is because we still use the old indexer for the settings and it is highly impacted by the
    // max memory. So we keep the changes here and will remove these changes once we use the new
    // indexer to also index settings. Related to #5125 and #5141.
    let grenad_parameters = GrenadParameters {
        max_memory: grenad_parameters.max_memory.map(|mm| mm * 5 / 100),
        ..grenad_parameters
    };

    // We compute and remove the allocated BBQueues buffers capacity from the indexing memory.
    let minimum_capacity = 50 * 1024 * 1024 * pool.current_num_threads(); // 50 MiB
    let (grenad_parameters, total_bbbuffer_capacity) = grenad_parameters.max_memory.map_or(
        (grenad_parameters, 2 * minimum_capacity), // 100 MiB by thread by default
        |max_memory| {
            // 2% of the indexing memory
            let total_bbbuffer_capacity = (max_memory / 100 / 2).max(minimum_capacity);
            let new_grenad_parameters = GrenadParameters {
                max_memory: Some(
                    max_memory.saturating_sub(total_bbbuffer_capacity).max(100 * 1024 * 1024),
                ),
                ..grenad_parameters
            };
            (new_grenad_parameters, total_bbbuffer_capacity)
        },
    );

    let (extractor_sender, writer_receiver) = pool
        .install(|| extractor_writer_bbqueue(&mut bbbuffers, total_bbbuffer_capacity, 1000))
        .unwrap();

    let db_document_decompression_dictionary = index
        .document_compression_raw_dictionary(wtxn)
        .map(|opt| opt.map(DecoderDictionary::copy))?;
    let metadata_builder = MetadataBuilder::from_index(index, wtxn)?;
    let new_fields_ids_map = FieldIdMapWithMetadata::new(new_fields_ids_map, metadata_builder);
    let new_fields_ids_map = RwLock::new(new_fields_ids_map);
    let fields_ids_map_store = ThreadLocal::with_capacity(rayon::current_num_threads());
    let mut extractor_allocs = ThreadLocal::with_capacity(rayon::current_num_threads());
    let doc_allocs = ThreadLocal::with_capacity(rayon::current_num_threads());

    let indexing_context = IndexingContext {
        index,
        db_fields_ids_map,
        db_document_decompression_dictionary: db_document_decompression_dictionary.as_ref(),
        new_fields_ids_map: &new_fields_ids_map,
        doc_allocs: &doc_allocs,
        fields_ids_map_store: &fields_ids_map_store,
        must_stop_processing,
        progress,
        grenad_parameters: &grenad_parameters,
    };

    let document_compression_dictionary = pool
        .install(|| {
            retrieve_or_compute_document_compression_dictionary(
                index,
                wtxn,
                document_changes,
                indexing_context,
                &mut extractor_allocs,
            )
        })
        .unwrap()?;

    let mut index_embeddings = index.embedding_configs(wtxn)?;
    let mut field_distribution = index.field_distribution(wtxn)?;
    let mut document_ids = index.documents_ids(wtxn)?;

    thread::scope(|s| -> Result<()> {
        let indexer_span = tracing::Span::current();
        let embedders = &embedders;
        let finished_extraction = &finished_extraction;
        // prevent moving the field_distribution and document_ids in the inner closure...
        let field_distribution = &mut field_distribution;
        let document_ids = &mut document_ids;
        let extractor_handle =
            Builder::new().name(S("indexer-extractors")).spawn_scoped(s, move || {
                pool.install(move || {
                    extract::extract_all(
                        document_changes,
                        indexing_context,
                        indexer_span,
                        extractor_sender,
                        document_compression_dictionary.as_ref(),
                        embedders,
                        &mut extractor_allocs,
                        finished_extraction,
                        field_distribution,
                        index_embeddings,
                        document_ids,
                    )
                })
                .unwrap()
            })?;

        let global_fields_ids_map = GlobalFieldsIdsMap::new(&new_fields_ids_map);

        let vector_arroy = index.vector_arroy;
        let arroy_writers: Result<HashMap<_, _>> = embedders
            .inner_as_ref()
            .iter()
            .map(|(embedder_name, (embedder, _, was_quantized))| {
                let embedder_index = index.embedder_category_id.get(wtxn, embedder_name)?.ok_or(
                    InternalError::DatabaseMissingEntry {
                        db_name: "embedder_category_id",
                        key: None,
                    },
                )?;

                let dimensions = embedder.dimensions();
                let writer = ArroyWrapper::new(vector_arroy, embedder_index, *was_quantized);

                Ok((
                    embedder_index,
                    (embedder_name.as_str(), embedder.as_ref(), writer, dimensions),
                ))
            })
            .collect();

        let mut arroy_writers = arroy_writers?;

        write_to_db(writer_receiver, finished_extraction, index, wtxn, &arroy_writers)?;

        indexing_context.progress.update_progress(IndexingStep::WaitingForExtractors);

        let (facet_field_ids_delta, index_embeddings) = extractor_handle.join().unwrap()?;

        indexing_context.progress.update_progress(IndexingStep::WritingEmbeddingsToDatabase);

        build_vectors(
            index,
            wtxn,
            index_embeddings,
            &mut arroy_writers,
            &indexing_context.must_stop_processing,
        )?;

        post_processing::post_process(
            indexing_context,
            wtxn,
            global_fields_ids_map,
            facet_field_ids_delta,
        )?;

        indexing_context.progress.update_progress(IndexingStep::Finalizing);

        Ok(()) as Result<_>
    })?;

    // required to into_inner the new_fields_ids_map
    drop(fields_ids_map_store);

    let new_fields_ids_map = new_fields_ids_map.into_inner().unwrap();
    update_index(
        index,
        wtxn,
        new_fields_ids_map,
        new_primary_key,
        embedders,
        field_distribution,
        document_ids,
    )?;

    Ok(())
}

/// The compression level to use when compressing documents.
const DOCUMENT_COMPRESSION_LEVEL: i32 = 19;
/// The sample size used to generate the document compression dictionary.
const DOCUMENT_COMPRESSION_SAMPLE_SIZE: usize = 10_000;
/// The maximum size the document compression dictionary can be.
const DOCUMENT_COMPRESSION_DICTIONARY_MAX_SIZE: usize = 64_000;
/// The maximum number of documents we accept to compress if they
/// weren't already compressed in the database. If this threshold
/// is reached we do not generate a dictionary and continue as is.
const DOCUMENT_COMPRESSION_COMPRESS_LIMIT: u64 = 5_000_000;

/// A function dedicated to use the existing or generate an appropriate
/// document compression dictionay based on the documents available in
/// the database and the ones in the payload.
///
/// If there are too many documents already in the database and no
/// compression dictionary we prefer not to generate a dictionary to avoid
/// compressing all of the documents and potentially blow up disk space.
fn compute_document_compression_dictionary<'pl, 'extractor, DC, MSP>(
    index: &Index,
    rtxn: &RoTxn<'_>,
    document_changes: &DC,
    indexing_context: IndexingContext<MSP>,
    extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
) -> Result<Option<EncoderDictionary<'static>>>
where
    DC: DocumentChanges<'pl>,
    MSP: Fn() -> bool + Sync,
{
    match index.document_compression_raw_dictionary(rtxn)? {
        Some(dict) => Ok(Some(EncoderDictionary::copy(dict, DOCUMENT_COMPRESSION_LEVEL))),
        None if index.number_of_documents(rtxn)? >= DOCUMENT_COMPRESSION_COMPRESS_LIMIT => Ok(None),
        None => {
            let datastore = ThreadLocal::with_capacity(rayon::current_num_threads());
            let extractor = CompressorExtractor {
                total_documents_to_extract: DOCUMENT_COMPRESSION_SAMPLE_SIZE,
                extracted_documents_count: AtomicUsize::new(0),
            };

            todo!("collect the documents samples from the database first (or after)");

            // This extraction only takes care about documents replacement
            // and not update (merges). The merged documents are ignore as
            // we will only use the previous version of them in the database.
            extract(
                document_changes,
                &extractor,
                indexing_context,
                extractor_allocs,
                &datastore,
                IndexingStep::PreparingCompressionDictionary,
            )?;

            let mut sample_data = Vec::new();
            let mut sample_sizes = Vec::new();
            for data in datastore {
                let CompressorExtractorData { buffer, must_stop: _ } = data.into_inner();
                let mut subsample_size = 0;
                for subsample in buffer {
                    sample_data.extend_from_slice(subsample);
                    subsample_size += subsample.len();
                }
                sample_sizes.push(subsample_size);
            }

            let dictionary = zstd::dict::from_continuous(
                &sample_data,
                &sample_sizes,
                DOCUMENT_COMPRESSION_DICTIONARY_MAX_SIZE,
            )?;

            Ok(Some(EncoderDictionary::copy(&dictionary, DOCUMENT_COMPRESSION_LEVEL)))
        }
    }
}

struct CompressorExtractor {
    total_documents_to_extract: usize,
    extracted_documents_count: AtomicUsize,
}

#[derive(Default)]
struct CompressorExtractorData<'extractor> {
    buffer: Vec<&'extractor [u8]>,
    /// We extracted the expected count of documents, we can skip everything now.
    must_stop: bool,
}

unsafe impl<'extractor> MostlySend for RefCell<CompressorExtractorData<'extractor>> {}

impl<'extractor> Extractor<'extractor> for CompressorExtractor {
    type Data = RefCell<CompressorExtractorData<'extractor>>;

    fn init_data<'doc>(
        &'doc self,
        _extractor_alloc: &'extractor bumpalo::Bump,
    ) -> crate::Result<Self::Data> {
        Ok(RefCell::new(CompressorExtractorData::default()))
    }

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = crate::Result<DocumentChange<'doc>>>,
        context: &'doc DocumentChangeContext<'_, 'extractor, '_, '_, Self::Data>,
    ) -> crate::Result<()> {
        let mut data = context.data.borrow_mut_or_yield();

        for change in changes {
            if data.must_stop {
                return Ok(());
            }

            let change = change?;
            match change {
                DocumentChange::Deletion(_) => (),
                DocumentChange::Update(_) => (),
                DocumentChange::Insertion(insertion) => {
                    for result in insertion.inserted().iter_top_level_fields() {
                        let (_field_name, raw_value) = result?;
                        let bytes = raw_value.get().as_bytes();
                        data.buffer.push(context.extractor_alloc.alloc_slice_copy(bytes));
                    }

                    let previous_count =
                        self.extracted_documents_count.fetch_add(1, atomic::Ordering::SeqCst);
                    data.must_stop = previous_count >= self.total_documents_to_extract;
                }
            }
        }

        Ok(())
    }
}
