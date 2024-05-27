use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use std::mem::size_of;
use std::str::from_utf8;
use std::sync::Arc;

use bytemuck::cast_slice;
use grenad::Writer;
use itertools::EitherOrBoth;
use ordered_float::OrderedFloat;
use serde_json::Value;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::prompt::Prompt;
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::helpers::try_split_at;
use crate::update::settings::InnerIndexSettingsDiff;
use crate::vector::parsed_vectors::{ParsedVectorsDiff, RESERVED_VECTORS_FIELD_NAME};
use crate::vector::Embedder;
use crate::{DocumentId, Result, ThreadPoolNoAbort};

/// The length of the elements that are always in the buffer when inserting new values.
const TRUNCATE_SIZE: usize = size_of::<DocumentId>();

pub struct ExtractedVectorPoints {
    // docid, _index -> KvWriterDelAdd -> Vector
    pub manual_vectors: grenad::Reader<BufReader<File>>,
    // docid -> ()
    pub remove_vectors: grenad::Reader<BufReader<File>>,
    // docid -> prompt
    pub prompts: grenad::Reader<BufReader<File>>,

    // embedder
    pub embedder_name: String,
    pub embedder: Arc<Embedder>,
}

enum VectorStateDelta {
    NoChange,
    // Remove all vectors, generated or manual, from this document
    NowRemoved,

    // Add the manually specified vectors, passed in the other grenad
    // Remove any previously generated vectors
    // Note: changing the value of the manually specified vector **should not record** this delta
    WasGeneratedNowManual(Vec<Vec<f32>>),

    ManualDelta(Vec<Vec<f32>>, Vec<Vec<f32>>),

    // Add the vector computed from the specified prompt
    // Remove any previous vector
    // Note: changing the value of the prompt **does require** recording this delta
    NowGenerated(String),
}

impl VectorStateDelta {
    fn into_values(self) -> (bool, String, (Vec<Vec<f32>>, Vec<Vec<f32>>)) {
        match self {
            VectorStateDelta::NoChange => Default::default(),
            VectorStateDelta::NowRemoved => (true, Default::default(), Default::default()),
            VectorStateDelta::WasGeneratedNowManual(add) => {
                (true, Default::default(), (Default::default(), add))
            }
            VectorStateDelta::ManualDelta(del, add) => (false, Default::default(), (del, add)),
            VectorStateDelta::NowGenerated(prompt) => (true, prompt, Default::default()),
        }
    }
}

struct EmbedderVectorExtractor {
    embedder_name: String,
    embedder: Arc<Embedder>,
    prompt: Arc<Prompt>,

    // (docid, _index) -> KvWriterDelAdd -> Vector
    manual_vectors_writer: Writer<BufWriter<File>>,
    // (docid) -> (prompt)
    prompts_writer: Writer<BufWriter<File>>,
    // (docid) -> ()
    remove_vectors_writer: Writer<BufWriter<File>>,
}

/// Extracts the embedding vector contained in each document under the `_vectors` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the Vec<f32>
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_vector_points<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    settings_diff: &InnerIndexSettingsDiff,
) -> Result<Vec<ExtractedVectorPoints>> {
    let reindex_vectors = settings_diff.reindex_vectors();

    let old_fields_ids_map = &settings_diff.old.fields_ids_map;
    let new_fields_ids_map = &settings_diff.new.fields_ids_map;
    // the vector field id may have changed
    let old_vectors_fid = old_fields_ids_map.id(RESERVED_VECTORS_FIELD_NAME);
    // filter the old vector fid if the settings has been changed forcing reindexing.
    let old_vectors_fid = old_vectors_fid.filter(|_| !reindex_vectors);

    let new_vectors_fid = new_fields_ids_map.id(RESERVED_VECTORS_FIELD_NAME);

    let mut extractors = Vec::new();
    for (embedder_name, (embedder, prompt)) in
        settings_diff.new.embedding_configs.clone().into_iter()
    {
        // (docid, _index) -> KvWriterDelAdd -> Vector
        let manual_vectors_writer = create_writer(
            indexer.chunk_compression_type,
            indexer.chunk_compression_level,
            tempfile::tempfile()?,
        );

        // (docid) -> (prompt)
        let prompts_writer = create_writer(
            indexer.chunk_compression_type,
            indexer.chunk_compression_level,
            tempfile::tempfile()?,
        );

        // (docid) -> ()
        let remove_vectors_writer = create_writer(
            indexer.chunk_compression_type,
            indexer.chunk_compression_level,
            tempfile::tempfile()?,
        );

        extractors.push(EmbedderVectorExtractor {
            embedder_name,
            embedder,
            prompt,
            manual_vectors_writer,
            prompts_writer,
            remove_vectors_writer,
        });
    }

    let mut key_buffer = Vec::new();
    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        // this must always be serialized as (docid, external_docid);
        let (docid_bytes, external_id_bytes) =
            try_split_at(key, std::mem::size_of::<DocumentId>()).unwrap();
        debug_assert!(from_utf8(external_id_bytes).is_ok());

        let obkv = obkv::KvReader::new(value);
        key_buffer.clear();
        key_buffer.extend_from_slice(docid_bytes);

        // since we only need the primary key when we throw an error we create this getter to
        // lazily get it when needed
        let document_id = || -> Value { from_utf8(external_id_bytes).unwrap().into() };

        let mut parsed_vectors = ParsedVectorsDiff::new(obkv, old_vectors_fid, new_vectors_fid)
            .map_err(|error| error.to_crate_error(document_id().to_string()))?;

        for EmbedderVectorExtractor {
            embedder_name,
            embedder: _,
            prompt,
            manual_vectors_writer,
            prompts_writer,
            remove_vectors_writer,
        } in extractors.iter_mut()
        {
            let delta = match parsed_vectors.remove(embedder_name) {
                (Some(old), Some(new)) => {
                    // no autogeneration
                    let del_vectors = old.into_array_of_vectors();
                    let add_vectors = new.into_array_of_vectors();

                    if add_vectors.len() > usize::from(u8::MAX) {
                        return Err(crate::Error::UserError(crate::UserError::TooManyVectors(
                            document_id().to_string(),
                            add_vectors.len(),
                        )));
                    }

                    VectorStateDelta::ManualDelta(del_vectors, add_vectors)
                }
                (Some(_old), None) => {
                    // Do we keep this document?
                    let document_is_kept = obkv
                        .iter()
                        .map(|(_, deladd)| KvReaderDelAdd::new(deladd))
                        .any(|deladd| deladd.get(DelAdd::Addition).is_some());
                    if document_is_kept {
                        // becomes autogenerated
                        VectorStateDelta::NowGenerated(prompt.render(
                            obkv,
                            DelAdd::Addition,
                            new_fields_ids_map,
                        )?)
                    } else {
                        VectorStateDelta::NowRemoved
                    }
                }
                (None, Some(new)) => {
                    // was possibly autogenerated, remove all vectors for that document
                    let add_vectors = new.into_array_of_vectors();
                    if add_vectors.len() > usize::from(u8::MAX) {
                        return Err(crate::Error::UserError(crate::UserError::TooManyVectors(
                            document_id().to_string(),
                            add_vectors.len(),
                        )));
                    }

                    VectorStateDelta::WasGeneratedNowManual(add_vectors)
                }
                (None, None) => {
                    // Do we keep this document?
                    let document_is_kept = obkv
                        .iter()
                        .map(|(_, deladd)| KvReaderDelAdd::new(deladd))
                        .any(|deladd| deladd.get(DelAdd::Addition).is_some());

                    if document_is_kept {
                        // Don't give up if the old prompt was failing
                        let old_prompt = Some(&prompt)
                            // TODO: this filter works because we erase the vec database when a embedding setting changes.
                            // When vector pipeline will be optimized, this should be removed.
                            .filter(|_| !settings_diff.reindex_vectors())
                            .map(|p| {
                                p.render(obkv, DelAdd::Deletion, old_fields_ids_map)
                                    .unwrap_or_default()
                            });
                        let new_prompt =
                            prompt.render(obkv, DelAdd::Addition, new_fields_ids_map)?;
                        if old_prompt.as_ref() != Some(&new_prompt) {
                            let old_prompt = old_prompt.unwrap_or_default();
                            tracing::trace!(
                                "🚀 Changing prompt from\n{old_prompt}\n===to===\n{new_prompt}"
                            );
                            VectorStateDelta::NowGenerated(new_prompt)
                        } else {
                            tracing::trace!("⏭️ Prompt unmodified, skipping");
                            VectorStateDelta::NoChange
                        }
                    } else {
                        VectorStateDelta::NowRemoved
                    }
                }
            };

            // and we finally push the unique vectors into the writer
            push_vectors_diff(
                remove_vectors_writer,
                prompts_writer,
                manual_vectors_writer,
                &mut key_buffer,
                delta,
                reindex_vectors,
            )?;
        }
    }

    let mut results = Vec::new();

    for EmbedderVectorExtractor {
        embedder_name,
        embedder,
        prompt: _,
        manual_vectors_writer,
        prompts_writer,
        remove_vectors_writer,
    } in extractors
    {
        results.push(ExtractedVectorPoints {
            // docid, _index -> KvWriterDelAdd -> Vector
            manual_vectors: writer_into_reader(manual_vectors_writer)?,
            // docid -> ()
            remove_vectors: writer_into_reader(remove_vectors_writer)?,
            // docid -> prompt
            prompts: writer_into_reader(prompts_writer)?,

            embedder,
            embedder_name,
        })
    }

    Ok(results)
}

/// Computes the diff between both Del and Add numbers and
/// only inserts the parts that differ in the sorter.
fn push_vectors_diff(
    remove_vectors_writer: &mut Writer<BufWriter<File>>,
    prompts_writer: &mut Writer<BufWriter<File>>,
    manual_vectors_writer: &mut Writer<BufWriter<File>>,
    key_buffer: &mut Vec<u8>,
    delta: VectorStateDelta,
    reindex_vectors: bool,
) -> Result<()> {
    let (must_remove, prompt, (mut del_vectors, mut add_vectors)) = delta.into_values();
    if must_remove
    // TODO: the below condition works because we erase the vec database when a embedding setting changes.
    // When vector pipeline will be optimized, this should be removed.
    && !reindex_vectors
    {
        key_buffer.truncate(TRUNCATE_SIZE);
        remove_vectors_writer.insert(&key_buffer, [])?;
    }
    if !prompt.is_empty() {
        key_buffer.truncate(TRUNCATE_SIZE);
        prompts_writer.insert(&key_buffer, prompt.as_bytes())?;
    }

    // We sort and dedup the vectors
    del_vectors.sort_unstable_by(|a, b| compare_vectors(a, b));
    add_vectors.sort_unstable_by(|a, b| compare_vectors(a, b));
    del_vectors.dedup_by(|a, b| compare_vectors(a, b).is_eq());
    add_vectors.dedup_by(|a, b| compare_vectors(a, b).is_eq());

    let merged_vectors_iter =
        itertools::merge_join_by(del_vectors, add_vectors, |del, add| compare_vectors(del, add));

    // insert vectors into the writer
    for (i, eob) in merged_vectors_iter.into_iter().enumerate().take(u16::MAX as usize) {
        // Generate the key by extending the unique index to it.
        key_buffer.truncate(TRUNCATE_SIZE);
        let index = u16::try_from(i).unwrap();
        key_buffer.extend_from_slice(&index.to_be_bytes());

        match eob {
            EitherOrBoth::Both(_, _) => (), // no need to touch anything
            EitherOrBoth::Left(vector) => {
                // TODO: the below condition works because we erase the vec database when a embedding setting changes.
                // When vector pipeline will be optimized, this should be removed.
                if !reindex_vectors {
                    // We insert only the Del part of the Obkv to inform
                    // that we only want to remove all those vectors.
                    let mut obkv = KvWriterDelAdd::memory();
                    obkv.insert(DelAdd::Deletion, cast_slice(&vector))?;
                    let bytes = obkv.into_inner()?;
                    manual_vectors_writer.insert(&key_buffer, bytes)?;
                }
            }
            EitherOrBoth::Right(vector) => {
                // We insert only the Add part of the Obkv to inform
                // that we only want to remove all those vectors.
                let mut obkv = KvWriterDelAdd::memory();
                obkv.insert(DelAdd::Addition, cast_slice(&vector))?;
                let bytes = obkv.into_inner()?;
                manual_vectors_writer.insert(&key_buffer, bytes)?;
            }
        }
    }

    Ok(())
}

/// Compares two vectors by using the OrderingFloat helper.
fn compare_vectors(a: &[f32], b: &[f32]) -> Ordering {
    a.iter().copied().map(OrderedFloat).cmp(b.iter().copied().map(OrderedFloat))
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_embeddings<R: io::Read + io::Seek>(
    // docid, prompt
    prompt_reader: grenad::Reader<R>,
    indexer: GrenadParameters,
    embedder: Arc<Embedder>,
    request_threads: &ThreadPoolNoAbort,
) -> Result<grenad::Reader<BufReader<File>>> {
    let n_chunks = embedder.chunk_count_hint(); // chunk level parallelism
    let n_vectors_per_chunk = embedder.prompt_count_in_chunk_hint(); // number of vectors in a single chunk

    // docid, state with embedding
    let mut state_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let mut chunks = Vec::with_capacity(n_chunks);
    let mut current_chunk = Vec::with_capacity(n_vectors_per_chunk);
    let mut current_chunk_ids = Vec::with_capacity(n_vectors_per_chunk);
    let mut chunks_ids = Vec::with_capacity(n_chunks);
    let mut cursor = prompt_reader.into_cursor()?;

    while let Some((key, value)) = cursor.move_on_next()? {
        let docid = key.try_into().map(DocumentId::from_be_bytes).unwrap();
        // SAFETY: precondition, the grenad value was saved from a string
        let prompt = unsafe { std::str::from_utf8_unchecked(value) };
        if current_chunk.len() == current_chunk.capacity() {
            chunks.push(std::mem::replace(
                &mut current_chunk,
                Vec::with_capacity(n_vectors_per_chunk),
            ));
            chunks_ids.push(std::mem::replace(
                &mut current_chunk_ids,
                Vec::with_capacity(n_vectors_per_chunk),
            ));
        };
        current_chunk.push(prompt.to_owned());
        current_chunk_ids.push(docid);

        if chunks.len() == chunks.capacity() {
            let chunked_embeds = embedder
                .embed_chunks(
                    std::mem::replace(&mut chunks, Vec::with_capacity(n_chunks)),
                    request_threads,
                )
                .map_err(crate::vector::Error::from)
                .map_err(crate::Error::from)?;

            for (docid, embeddings) in chunks_ids
                .iter()
                .flat_map(|docids| docids.iter())
                .zip(chunked_embeds.iter().flat_map(|embeds| embeds.iter()))
            {
                state_writer.insert(docid.to_be_bytes(), cast_slice(embeddings.as_inner()))?;
            }
            chunks_ids.clear();
        }
    }

    // send last chunk
    if !chunks.is_empty() {
        let chunked_embeds = embedder
            .embed_chunks(std::mem::take(&mut chunks), request_threads)
            .map_err(crate::vector::Error::from)
            .map_err(crate::Error::from)?;
        for (docid, embeddings) in chunks_ids
            .iter()
            .flat_map(|docids| docids.iter())
            .zip(chunked_embeds.iter().flat_map(|embeds| embeds.iter()))
        {
            state_writer.insert(docid.to_be_bytes(), cast_slice(embeddings.as_inner()))?;
        }
    }

    if !current_chunk.is_empty() {
        let embeds = embedder
            .embed_chunks(vec![std::mem::take(&mut current_chunk)], request_threads)
            .map_err(crate::vector::Error::from)
            .map_err(crate::Error::from)?;

        if let Some(embeds) = embeds.first() {
            for (docid, embeddings) in current_chunk_ids.iter().zip(embeds.iter()) {
                state_writer.insert(docid.to_be_bytes(), cast_slice(embeddings.as_inner()))?;
            }
        }
    }

    writer_into_reader(state_writer)
}
