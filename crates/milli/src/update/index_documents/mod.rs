mod enrich;
mod extract;
mod helpers;
mod transform;
mod typed_chunk;

use std::collections::HashSet;
use std::io::{Read, Seek};
use std::iter;
use std::num::NonZeroU32;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};
use enrich::enrich_documents_batch;
pub use extract::request_threads;
use grenad::{Merger, MergerBuilder};
use hashbrown::HashMap;
use heed::types::Str;
use heed::Database;
use rand::SeedableRng as _;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use slice_group_by::GroupBy;
use tracing::debug;
use typed_chunk::{write_typed_chunk_into_index, ChunkAccumulator, TypedChunk};

pub use self::enrich::{extract_finite_float_from_value, DocumentId};
pub use self::helpers::*;
pub use self::transform::{Transform, TransformOutput};
use super::facet::clear_facet_levels_based_on_settings_diff;
use super::new::StdResult;
use crate::database_stats::DatabaseStats;
use crate::documents::{obkv_to_object, DocumentsBatchReader};
use crate::error::{Error, InternalError};
use crate::index::{PrefixSearch, PrefixSettings};
use crate::progress::{EmbedderStats, Progress};
pub use crate::update::index_documents::helpers::CursorClonableMmap;
use crate::update::{
    IndexerConfig, UpdateIndexingStep, WordPrefixDocids, WordPrefixIntegerDocids, WordsPrefixesFst,
};
use crate::vector::db::EmbedderInfo;
use crate::vector::{RuntimeEmbedders, VectorStore};
use crate::{CboRoaringBitmapCodec, Index, MustStopProcessing, Result, UserError};

static MERGED_DATABASE_COUNT: usize = 7;
static PREFIX_DATABASE_COUNT: usize = 4;
static TOTAL_POSTING_DATABASE_COUNT: usize = MERGED_DATABASE_COUNT + PREFIX_DATABASE_COUNT;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentAdditionResult {
    /// The number of documents that were indexed during the update
    pub indexed_documents: u64,
    /// The total number of documents in the index after the update
    pub number_of_documents: u64,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum IndexDocumentsMethod {
    /// Replace the previous document with the new one,
    /// removing all the already known attributes.
    #[default]
    ReplaceDocuments,

    /// Merge the previous version of the document with the new version,
    /// replacing old attributes values with the new ones and add the new attributes.
    UpdateDocuments,
}

/// Controls whether new documents should be created when they don't already exist.
///
/// This policy is checked when processing a document whose ID is not found in the index.
/// It applies to both update and replace operations.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MissingDocumentPolicy {
    /// Create the document if it doesn't exist. This is the default behavior.
    #[default]
    Create,

    /// Skip the document silently if it doesn't exist. No error is returned, the document is simply
    /// not indexed.
    Skip,
}

pub struct IndexDocuments<'t, 'i, 'a, FP> {
    wtxn: &'t mut heed::RwTxn<'i>,
    index: &'i Index,
    config: IndexDocumentsConfig,
    indexer_config: &'a IndexerConfig,
    transform: Option<Transform<'a, 'i>>,
    progress: FP,
    should_abort: &'a MustStopProcessing,
    added_documents: u64,
    deleted_documents: u64,
    embedders: RuntimeEmbedders,
    embedder_stats: &'t Arc<EmbedderStats>,
}

#[derive(Default, Debug, Clone)]
pub struct IndexDocumentsConfig {
    pub words_positions_level_group_size: Option<NonZeroU32>,
    pub words_positions_min_level_size: Option<NonZeroU32>,
    pub update_method: IndexDocumentsMethod,
    pub autogenerate_docids: bool,
}

impl<'t, 'i, 'a, FP> IndexDocuments<'t, 'i, 'a, FP>
where
    FP: Fn(UpdateIndexingStep) + Sync + Send,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i>,
        index: &'i Index,
        indexer_config: &'a IndexerConfig,
        config: IndexDocumentsConfig,
        progress: FP,
        should_abort: &'a MustStopProcessing,
        embedder_stats: &'t Arc<EmbedderStats>,
        embedder_ip_policy: &'a http_client::policy::IpPolicy,
    ) -> Result<IndexDocuments<'t, 'i, 'a, FP>> {
        let transform = Some(Transform::new(
            wtxn,
            index,
            indexer_config,
            config.update_method,
            embedder_ip_policy,
            config.autogenerate_docids,
        )?);

        Ok(IndexDocuments {
            transform,
            config,
            indexer_config,
            progress,
            should_abort,
            wtxn,
            index,
            added_documents: 0,
            deleted_documents: 0,
            embedders: Default::default(),
            embedder_stats,
        })
    }

    /// Adds a batch of documents to the current builder.
    ///
    /// Since the documents are progressively added to the writer, a failure will cause only
    /// return an error and not the `IndexDocuments` struct as it is invalid to use it afterward.
    ///
    /// Returns the number of documents added to the builder.
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::documents")]
    pub fn add_documents<R: Read + Seek>(
        mut self,
        reader: DocumentsBatchReader<R>,
    ) -> Result<(Self, StdResult<u64, UserError>)> {
        // Early return when there is no document to add
        if reader.is_empty() {
            return Ok((self, Ok(0)));
        }

        // We check for user errors in this validator and if there is one, we can return
        // the `IndexDocument` struct as it is valid to send more documents into it.
        // However, if there is an internal error we throw it away!
        let enriched_documents_reader = match enrich_documents_batch(
            self.wtxn,
            self.index,
            self.config.autogenerate_docids,
            reader,
        )? {
            Ok(reader) => reader,
            Err(user_error) => return Ok((self, Err(user_error))),
        };

        let indexed_documents =
            self.transform.as_mut().expect("Invalid document addition state").read_documents(
                enriched_documents_reader,
                self.wtxn,
                &self.progress,
                self.should_abort,
            )? as u64;

        self.added_documents += indexed_documents;

        Ok((self, Ok(indexed_documents)))
    }

    pub fn with_embedders(mut self, embedders: RuntimeEmbedders) -> Self {
        self.embedders = embedders;
        self
    }

    #[tracing::instrument(
        level = "trace"
        skip_all,
        target = "indexing::documents",
        name = "index_documents"
    )]
    pub fn execute(mut self) -> Result<DocumentAdditionResult> {
        if self.added_documents == 0 && self.deleted_documents == 0 {
            let number_of_documents = self.index.number_of_documents(self.wtxn)?;
            return Ok(DocumentAdditionResult { indexed_documents: 0, number_of_documents });
        }
        let output = self
            .transform
            .take()
            .expect("Invalid document addition state")
            .output_from_sorter(self.wtxn, &self.progress)?;

        let indexed_documents = output.documents_count as u64;
        let number_of_documents = self.execute_raw(output)?;

        Ok(DocumentAdditionResult { indexed_documents, number_of_documents })
    }

    /// Returns the total number of documents in the index after the update.
    #[tracing::instrument(
        level = "trace",
        skip_all,
        target = "indexing::details",
        name = "index_documents_raw"
    )]
    pub fn execute_raw(mut self, output: TransformOutput) -> Result<u64>
    where
        FP: Fn(UpdateIndexingStep) + Sync,
    {
        let TransformOutput {
            primary_key,
            mut settings_diff,
            field_distribution,
            documents_count,
            original_documents,
            flattened_documents,
        } = output;

        // update the searchable list,
        // because they might have changed due to the nested documents flattening.
        settings_diff.new.recompute_searchables(self.wtxn, self.index)?;

        let settings_diff = Arc::new(settings_diff);
        let embedder_infos: heed::Result<Vec<(String, EmbedderInfo)>> = self
            .index
            .embedding_configs()
            .iter_embedder_info(self.wtxn)?
            .map(|res| res.map(|(name, info)| (name.to_owned(), info)))
            .collect();
        let embedder_infos = Arc::new(embedder_infos?);

        let possible_embedding_mistakes =
            crate::vector::error::PossibleEmbeddingMistakes::new(&field_distribution);

        let pool = &self.indexer_config.thread_pool;

        // create LMDB writer channel
        let (lmdb_writer_sx, lmdb_writer_rx): (
            Sender<Result<TypedChunk>>,
            Receiver<Result<TypedChunk>>,
        ) = crossbeam_channel::unbounded();

        // get the primary key field id
        let primary_key_id = settings_diff.new.fields_ids_map.id(&primary_key).unwrap();

        let pool_params = GrenadParameters {
            chunk_compression_type: self.indexer_config.chunk_compression_type,
            chunk_compression_level: self.indexer_config.chunk_compression_level,
            max_memory: self.indexer_config.max_memory,
            max_nb_chunks: self.indexer_config.max_nb_chunks, // default value, may be chosen.
        };
        let documents_chunk_size = match self.indexer_config.documents_chunk_size {
            Some(chunk_size) => chunk_size,
            None => {
                let default_chunk_size = 1024 * 1024 * 4; // 4MiB
                let min_chunk_size = 1024 * 512; // 512KiB

                // compute the chunk size from the number of available threads and the inputed data size.
                let total_size = match flattened_documents.as_ref() {
                    Some(flattened_documents) => flattened_documents.metadata().map(|m| m.len()),
                    None => Ok(default_chunk_size as u64),
                };
                let current_num_threads = pool.current_num_threads();
                // if we have more than 2 thread, create a number of chunk equal to 3/4 threads count
                let chunk_count = if current_num_threads > 2 {
                    (current_num_threads * 3 / 4).max(2)
                } else {
                    current_num_threads
                };
                total_size
                    .map_or(default_chunk_size, |size| (size as usize) / chunk_count)
                    .max(min_chunk_size)
            }
        };

        let original_documents = match original_documents {
            Some(original_documents) => Some(grenad::Reader::new(original_documents)?),
            None => None,
        };
        let flattened_documents = match flattened_documents {
            Some(flattened_documents) => Some(grenad::Reader::new(flattened_documents)?),
            None => None,
        };

        let max_positions_per_attributes = self.indexer_config.max_positions_per_attributes;

        let mut final_documents_ids = RoaringBitmap::new();
        let mut databases_seen = 0;
        let mut word_position_docids = None;
        let mut word_fid_docids = None;
        let mut word_docids = None;
        let mut exact_word_docids = None;
        let mut chunk_accumulator = ChunkAccumulator::default();
        let mut dimension = HashMap::new();

        let current_span = tracing::Span::current();

        // Run extraction pipeline in parallel.
        let mut modified_docids = RoaringBitmap::new();
        let embedder_stats = self.embedder_stats.clone();
        pool.install(|| {
                let settings_diff_cloned = settings_diff.clone();
                rayon::spawn(move || {
                    let child_span = tracing::trace_span!(target: "indexing::details", parent: &current_span, "extract_and_send_grenad_chunks");
                    let _enter = child_span.enter();

                    // split obkv file into several chunks
                    let original_chunk_iter = match original_documents {
                        Some(original_documents) => {
                            grenad_obkv_into_chunks(original_documents,pool_params,documents_chunk_size).map(either::Left)
                        },
                        None => Ok(either::Right(iter::empty())),
                    };

                    // split obkv file into several chunks
                    let flattened_chunk_iter = match flattened_documents {
                        Some(flattened_documents) => {
                            grenad_obkv_into_chunks(flattened_documents, pool_params, documents_chunk_size).map(either::Left)
                        },
                        None => Ok(either::Right(iter::empty())),
                    };

                    let result = original_chunk_iter.and_then(|original_chunk| {
                        let flattened_chunk = flattened_chunk_iter?;
                        // extract all databases from the chunked obkv douments
                        extract::data_from_obkv_documents(
                            original_chunk,
                            flattened_chunk,
                            pool_params,
                            lmdb_writer_sx.clone(),
                            primary_key_id,
                            settings_diff_cloned,
                            max_positions_per_attributes,
                            embedder_infos,
                            Arc::new(possible_embedding_mistakes),
                            &embedder_stats
                        )
                    });

                    if let Err(e) = result {
                        let _ = lmdb_writer_sx.send(Err(e));
                    }

                    // needs to be dropped to avoid channel waiting lock.
                    drop(lmdb_writer_sx);
                });

                (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
                    databases_seen,
                    total_databases: TOTAL_POSTING_DATABASE_COUNT,
                });

                loop {
                    if self.should_abort.get() {
                        return Err(Error::InternalError(InternalError::AbortedIndexation));
                    }

                    match lmdb_writer_rx.clone().recv_timeout(std::time::Duration::from_millis(500)) {
                        Err(status) => {
                            if let Some(typed_chunks) = chunk_accumulator.pop_longest() {
                                let (docids, is_merged_database) =
                                    write_typed_chunk_into_index(self.wtxn, self.index, &settings_diff, typed_chunks, &mut modified_docids)?;
                                if !docids.is_empty() {
                                    final_documents_ids |= docids;
                                    let documents_seen_count = final_documents_ids.len();
                                    (self.progress)(UpdateIndexingStep::IndexDocuments {
                                        documents_seen: documents_seen_count as usize,
                                        total_documents: documents_count,
                                    });
                                    debug!(documents = documents_seen_count, total = documents_count, "Seen");
                                }
                                if is_merged_database {
                                    databases_seen += 1;
                                    (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
                                        databases_seen,
                                        total_databases: TOTAL_POSTING_DATABASE_COUNT,
                                    });
                                }
                            // If no more chunk remains in the chunk accumulator and the channel is disconected, break.
                            } else if status == crossbeam_channel::RecvTimeoutError::Disconnected {
                                break;
                            } else {
                                rayon::yield_now();
                            }
                        }
                        Ok(result) => {
                            let typed_chunk = match result? {
                                TypedChunk::WordDocids {
                                    word_docids_reader,
                                    exact_word_docids_reader,
                                    word_fid_docids_reader,
                                } => {
                                    let cloneable_chunk =
                                        unsafe { as_cloneable_grenad(&word_docids_reader)? };
                                    let word_docids = word_docids.get_or_insert_with(|| {
                                        MergerBuilder::new(MergeDeladdCboRoaringBitmaps)
                                    });
                                    word_docids.push(cloneable_chunk.into_cursor()?);
                                    let cloneable_chunk =
                                        unsafe { as_cloneable_grenad(&exact_word_docids_reader)? };
                                    let exact_word_docids =
                                        exact_word_docids.get_or_insert_with(|| {
                                            MergerBuilder::new(
                                                MergeDeladdCboRoaringBitmaps,
                                            )
                                        });
                                    exact_word_docids.push(cloneable_chunk.into_cursor()?);
                                    let cloneable_chunk =
                                        unsafe { as_cloneable_grenad(&word_fid_docids_reader)? };
                                    let word_fid_docids = word_fid_docids.get_or_insert_with(|| {
                                        MergerBuilder::new(MergeDeladdCboRoaringBitmaps)
                                    });
                                    word_fid_docids.push(cloneable_chunk.into_cursor()?);
                                    TypedChunk::WordDocids {
                                        word_docids_reader,
                                        exact_word_docids_reader,
                                        word_fid_docids_reader,
                                    }
                                }
                                TypedChunk::WordPositionDocids(chunk) => {
                                    let cloneable_chunk = unsafe { as_cloneable_grenad(&chunk)? };
                                    let word_position_docids =
                                        word_position_docids.get_or_insert_with(|| {
                                            MergerBuilder::new(
                                                MergeDeladdCboRoaringBitmaps,
                                            )
                                        });
                                    word_position_docids.push(cloneable_chunk.into_cursor()?);
                                    TypedChunk::WordPositionDocids(chunk)
                                }
                                TypedChunk::VectorPoints {
                                    expected_dimension,
                                    remove_vectors,
                                    embeddings_from_prompts,
                                    embeddings_from_fragments,
                                    manual_vectors,
                                    embedder_name,
                                    embedding_status_delta,
                                } => {
                                    dimension.insert(embedder_name.clone(), expected_dimension);
                                    TypedChunk::VectorPoints {
                                        remove_vectors,
                                        embeddings_from_prompts,
                                        embeddings_from_fragments,
                                        expected_dimension,
                                        manual_vectors,
                                        embedder_name,
                                        embedding_status_delta,
                                    }
                                }
                                otherwise => otherwise,
                            };

                            chunk_accumulator.insert(typed_chunk);
                        }
                    }
                }

                // If the settings are only being updated, we may have to clear some of the facet levels.
                if settings_diff.settings_update_only() {
                    clear_facet_levels_based_on_settings_diff(self.wtxn, self.index, &settings_diff)?;
                }

                Ok(())
            }).map_err(InternalError::from)??;

        if !settings_diff.settings_update_only {
            // Update the stats of the documents database when there is a document update.
            let stats = DatabaseStats::new(self.index.documents.remap_data_type(), self.wtxn)?;
            self.index.put_documents_stats(self.wtxn, stats)?;
        }
        // We write the field distribution into the main database
        self.index.put_field_distribution(self.wtxn, &field_distribution)?;

        // We write the primary key field id into the main database
        self.index.put_primary_key(self.wtxn, &primary_key)?;
        let number_of_documents = self.index.number_of_documents(self.wtxn)?;
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        // If an embedder wasn't used in the typedchunk but must be binary quantized
        // we should insert it in `dimension`
        let backend = self.index.get_vector_store(self.wtxn)?.unwrap_or_default();
        for (name, action) in settings_diff.embedding_config_updates.iter() {
            let must_rebuild = action.is_being_quantized || action.remove_fragments().is_some();
            if must_rebuild && !dimension.contains_key(name.as_str()) {
                let Some(runtime_embedder) = settings_diff.new.runtime_embedders.get(name) else {
                    continue;
                };
                dimension.insert(name.to_string(), runtime_embedder.embedder.dimensions());
            }
        }

        for (embedder_name, dimension) in dimension {
            let wtxn = &mut *self.wtxn;
            let vector_store = self.index.vector_store;
            let cancel = &self.should_abort;

            let embedder_index =
                self.index.embedding_configs().embedder_id(wtxn, &embedder_name)?.ok_or(
                    InternalError::DatabaseMissingEntry {
                        db_name: "embedder_category_id",
                        key: None,
                    },
                )?;
            let embedder_config = settings_diff.embedding_config_updates.get(&embedder_name);
            let was_quantized = settings_diff
                .old
                .runtime_embedders
                .get(&embedder_name)
                .is_some_and(|conf| conf.is_quantized);
            let is_quantizing = embedder_config.is_some_and(|action| action.is_being_quantized);

            pool.install(|| -> Result<_> {
                let mut writer =
                    VectorStore::new(backend, vector_store, embedder_index, was_quantized);
                writer.build_and_quantize(
                    wtxn,
                    // In the settings we don't have any progress to share
                    Progress::default(),
                    &mut rng,
                    dimension,
                    is_quantizing,
                    self.indexer_config.max_memory,
                    cancel,
                )?;
                Result::Ok(())
            })
            .map_err(InternalError::from)??;
        }

        self.index.cellulite.build(self.wtxn, &|| self.should_abort.get(), &Progress::default())?;

        self.execute_prefix_databases(
            word_docids.map(MergerBuilder::build),
            exact_word_docids.map(MergerBuilder::build),
            word_position_docids.map(MergerBuilder::build),
            word_fid_docids.map(MergerBuilder::build),
        )?;

        // Delete the old fid-based databases.
        // this may not be the most efficient way to do it in this indexer,
        // but we are in the legacy indexer, so it is the easiest way to do it until the new indexer is ready.
        crate::update::new::indexer::delete_old_fid_based_databases(
            self.wtxn,
            self.index,
            &*settings_diff,
            self.should_abort,
            &Progress::default(),
        )?;

        Ok(number_of_documents)
    }

    #[tracing::instrument(
        level = "trace",
        skip_all,
        target = "indexing::post_processing::prefix",
        name = "index_documents_prefix_databases"
    )]
    pub fn execute_prefix_databases(
        &mut self,
        word_docids: Option<Merger<CursorClonableMmap, MergeDeladdCboRoaringBitmaps>>,
        exact_word_docids: Option<Merger<CursorClonableMmap, MergeDeladdCboRoaringBitmaps>>,
        word_position_docids: Option<Merger<CursorClonableMmap, MergeDeladdCboRoaringBitmaps>>,
        word_fid_docids: Option<Merger<CursorClonableMmap, MergeDeladdCboRoaringBitmaps>>,
    ) -> Result<()>
    where
        FP: Fn(UpdateIndexingStep) + Sync,
    {
        // Merged databases are already been indexed, we start from this count;
        let mut databases_seen = MERGED_DATABASE_COUNT;

        if self.should_abort.get() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        databases_seen += 1;
        (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        if self.should_abort.get() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        let previous_words_prefixes_fst =
            self.index.words_prefixes_fst(self.wtxn)?.map_data(|cow| cow.into_owned())?;

        // Run the words prefixes update operation.
        let PrefixSettings { prefix_count_threshold, max_prefix_length, compute_prefixes } =
            self.index.prefix_settings(self.wtxn)?;

        // If the prefix search is enabled at indexing time, we compute the prefixes.
        if compute_prefixes == PrefixSearch::IndexingTime {
            let mut builder = WordsPrefixesFst::new(self.wtxn, self.index);
            builder.threshold(prefix_count_threshold);
            builder.max_prefix_length(max_prefix_length);
            builder.execute()?;
        } else {
            // If the prefix search is disabled at indexing time, we delete the previous words prefixes fst.
            // And all the associated docids databases.
            self.index.delete_words_prefixes_fst(self.wtxn)?;
            self.index.word_prefix_docids.clear(self.wtxn)?;
            self.index.exact_word_prefix_docids.clear(self.wtxn)?;
            self.index.word_prefix_position_docids.clear(self.wtxn)?;
            self.index.word_prefix_fid_docids.clear(self.wtxn)?;

            databases_seen += 3;
            (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
                databases_seen,
                total_databases: TOTAL_POSTING_DATABASE_COUNT,
            });

            return Ok(());
        }

        if self.should_abort.get() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        let current_prefix_fst;
        let common_prefix_fst_words_tmp;
        let common_prefix_fst_words: Vec<_>;
        let new_prefix_fst_words;
        let del_prefix_fst_words;

        {
            let span = tracing::trace_span!(target: "indexing::details", "compute_prefix_diffs");
            let _entered = span.enter();

            current_prefix_fst = self.index.words_prefixes_fst(self.wtxn)?;

            // We retrieve the common words between the previous and new prefix word fst.
            common_prefix_fst_words_tmp = fst_stream_into_vec(
                previous_words_prefixes_fst.op().add(&current_prefix_fst).intersection(),
            );
            common_prefix_fst_words = common_prefix_fst_words_tmp
                .as_slice()
                .linear_group_by_key(|x| x.chars().next().unwrap())
                .collect();

            // We retrieve the newly added words between the previous and new prefix word fst.
            new_prefix_fst_words = fst_stream_into_vec(
                current_prefix_fst.op().add(&previous_words_prefixes_fst).difference(),
            );

            // We compute the set of prefixes that are no more part of the prefix fst.
            del_prefix_fst_words = fst_stream_into_hashset(
                previous_words_prefixes_fst.op().add(&current_prefix_fst).difference(),
            );
        }

        databases_seen += 1;
        (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        if self.should_abort.get() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        if let Some(word_docids) = word_docids {
            execute_word_prefix_docids(
                self.wtxn,
                word_docids,
                self.index.word_docids,
                self.index.word_prefix_docids,
                self.indexer_config,
                &new_prefix_fst_words,
                &common_prefix_fst_words,
                &del_prefix_fst_words,
            )?;
        }

        if let Some(exact_word_docids) = exact_word_docids {
            execute_word_prefix_docids(
                self.wtxn,
                exact_word_docids,
                self.index.exact_word_docids,
                self.index.exact_word_prefix_docids,
                self.indexer_config,
                &new_prefix_fst_words,
                &common_prefix_fst_words,
                &del_prefix_fst_words,
            )?;
        }

        if self.should_abort.get() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        databases_seen += 1;
        (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        if let Some(word_position_docids) = word_position_docids {
            // Run the words prefix position docids update operation.
            let mut builder = WordPrefixIntegerDocids::new(
                self.wtxn,
                self.index.word_prefix_position_docids,
                self.index.word_position_docids,
            );
            builder.chunk_compression_type = self.indexer_config.chunk_compression_type;
            builder.chunk_compression_level = self.indexer_config.chunk_compression_level;
            builder.max_nb_chunks = self.indexer_config.max_nb_chunks;
            builder.max_memory = self.indexer_config.max_memory;

            builder.execute(
                word_position_docids,
                &new_prefix_fst_words,
                &common_prefix_fst_words,
                &del_prefix_fst_words,
            )?;
        }
        if let Some(word_fid_docids) = word_fid_docids {
            // Run the words prefix fid docids update operation.
            let mut builder = WordPrefixIntegerDocids::new(
                self.wtxn,
                self.index.word_prefix_fid_docids,
                self.index.word_fid_docids,
            );
            builder.chunk_compression_type = self.indexer_config.chunk_compression_type;
            builder.chunk_compression_level = self.indexer_config.chunk_compression_level;
            builder.max_nb_chunks = self.indexer_config.max_nb_chunks;
            builder.max_memory = self.indexer_config.max_memory;
            builder.execute(
                word_fid_docids,
                &new_prefix_fst_words,
                &common_prefix_fst_words,
                &del_prefix_fst_words,
            )?;
        }

        if self.should_abort.get() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        databases_seen += 1;
        (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        Ok(())
    }
}

/// Run the word prefix docids update operation.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    level = "trace",
    skip_all,
    target = "indexing::post_processing::prefix",
    name = "index_documents_word_prefix_docids"
)]
fn execute_word_prefix_docids(
    txn: &mut heed::RwTxn<'_>,
    merger: Merger<CursorClonableMmap, MergeDeladdCboRoaringBitmaps>,
    word_docids_db: Database<Str, CboRoaringBitmapCodec>,
    word_prefix_docids_db: Database<Str, CboRoaringBitmapCodec>,
    indexer_config: &IndexerConfig,
    new_prefix_fst_words: &[String],
    common_prefix_fst_words: &[&[String]],
    del_prefix_fst_words: &HashSet<Vec<u8>>,
) -> Result<()> {
    let mut builder = WordPrefixDocids::new(txn, word_docids_db, word_prefix_docids_db);
    builder.chunk_compression_type = indexer_config.chunk_compression_type;
    builder.chunk_compression_level = indexer_config.chunk_compression_level;
    builder.max_nb_chunks = indexer_config.max_nb_chunks;
    builder.max_memory = indexer_config.max_memory;
    builder.execute(merger, new_prefix_fst_words, common_prefix_fst_words, del_prefix_fst_words)?;
    Ok(())
}

#[cfg(test)]
mod mod_test;
