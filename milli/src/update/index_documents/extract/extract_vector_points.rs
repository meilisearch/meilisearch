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
use serde_json::{from_slice, Value};

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::error::UserError;
use crate::prompt::Prompt;
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::helpers::try_split_at;
use crate::update::settings::InnerIndexSettingsDiff;
use crate::vector::Embedder;
use crate::{DocumentId, InternalError, Result, VectorOrArrayOfVectors};

/// The length of the elements that are always in the buffer when inserting new values.
const TRUNCATE_SIZE: usize = size_of::<DocumentId>();

pub struct ExtractedVectorPoints {
    // docid, _index -> KvWriterDelAdd -> Vector
    pub manual_vectors: grenad::Reader<BufReader<File>>,
    // docid -> ()
    pub remove_vectors: grenad::Reader<BufReader<File>>,
    // docid -> prompt
    pub prompts: grenad::Reader<BufReader<File>>,
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

/// Extracts the embedding vector contained in each document under the `_vectors` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the Vec<f32>
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_vector_points<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    settings_diff: &InnerIndexSettingsDiff,
    prompt: &Prompt,
    embedder_name: &str,
) -> Result<ExtractedVectorPoints> {
    puffin::profile_function!();

    let old_fields_ids_map = &settings_diff.old.fields_ids_map;
    let new_fields_ids_map = &settings_diff.new.fields_ids_map;

    // (docid, _index) -> KvWriterDelAdd -> Vector
    let mut manual_vectors_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    // (docid) -> (prompt)
    let mut prompts_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    // (docid) -> ()
    let mut remove_vectors_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

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

        // since we only needs the primary key when we throw an error we create this getter to
        // lazily get it when needed
        let document_id = || -> Value { from_utf8(external_id_bytes).unwrap().into() };

        // the vector field id may have changed
        let old_vectors_fid = old_fields_ids_map.id("_vectors");
        // filter the old vector fid if the settings has been changed forcing reindexing.
        let old_vectors_fid = old_vectors_fid.filter(|_| !settings_diff.reindex_vectors());

        let new_vectors_fid = new_fields_ids_map.id("_vectors");
        let vectors_field = {
            let del = old_vectors_fid
                .and_then(|vectors_fid| obkv.get(vectors_fid))
                .map(KvReaderDelAdd::new)
                .map(|obkv| to_vector_map(obkv, DelAdd::Deletion, &document_id))
                .transpose()?
                .flatten();
            let add = new_vectors_fid
                .and_then(|vectors_fid| obkv.get(vectors_fid))
                .map(KvReaderDelAdd::new)
                .map(|obkv| to_vector_map(obkv, DelAdd::Addition, &document_id))
                .transpose()?
                .flatten();
            (del, add)
        };

        let (del_map, add_map) = vectors_field;

        let del_value = del_map.and_then(|mut map| map.remove(embedder_name));
        let add_value = add_map.and_then(|mut map| map.remove(embedder_name));

        let delta = match (del_value, add_value) {
            (Some(old), Some(new)) => {
                // no autogeneration
                let del_vectors = extract_vectors(old, document_id, embedder_name)?;
                let add_vectors = extract_vectors(new, document_id, embedder_name)?;

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
                let add_vectors = extract_vectors(new, document_id, embedder_name)?;
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
                    let old_prompt = prompt
                        .render(obkv, DelAdd::Deletion, old_fields_ids_map)
                        .unwrap_or_default();
                    let new_prompt = prompt.render(obkv, DelAdd::Addition, new_fields_ids_map)?;
                    if old_prompt != new_prompt {
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
            &mut remove_vectors_writer,
            &mut prompts_writer,
            &mut manual_vectors_writer,
            &mut key_buffer,
            delta,
        )?;
    }

    Ok(ExtractedVectorPoints {
        // docid, _index -> KvWriterDelAdd -> Vector
        manual_vectors: writer_into_reader(manual_vectors_writer)?,
        // docid -> ()
        remove_vectors: writer_into_reader(remove_vectors_writer)?,
        // docid -> prompt
        prompts: writer_into_reader(prompts_writer)?,
    })
}

fn to_vector_map(
    obkv: KvReaderDelAdd,
    side: DelAdd,
    document_id: &impl Fn() -> Value,
) -> Result<Option<serde_json::Map<String, Value>>> {
    Ok(if let Some(value) = obkv.get(side) {
        let Ok(value) = from_slice(value) else {
            let value = from_slice(value).map_err(InternalError::SerdeJson)?;
            return Err(crate::Error::UserError(UserError::InvalidVectorsMapType {
                document_id: document_id(),
                value,
            }));
        };
        Some(value)
    } else {
        None
    })
}

/// Computes the diff between both Del and Add numbers and
/// only inserts the parts that differ in the sorter.
fn push_vectors_diff(
    remove_vectors_writer: &mut Writer<BufWriter<File>>,
    prompts_writer: &mut Writer<BufWriter<File>>,
    manual_vectors_writer: &mut Writer<BufWriter<File>>,
    key_buffer: &mut Vec<u8>,
    delta: VectorStateDelta,
) -> Result<()> {
    puffin::profile_function!();
    let (must_remove, prompt, (mut del_vectors, mut add_vectors)) = delta.into_values();
    if must_remove {
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
                // We insert only the Del part of the Obkv to inform
                // that we only want to remove all those vectors.
                let mut obkv = KvWriterDelAdd::memory();
                obkv.insert(DelAdd::Deletion, cast_slice(&vector))?;
                let bytes = obkv.into_inner()?;
                manual_vectors_writer.insert(&key_buffer, bytes)?;
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

/// Extracts the vectors from a JSON value.
fn extract_vectors(
    value: Value,
    document_id: impl Fn() -> Value,
    name: &str,
) -> Result<Vec<Vec<f32>>> {
    // FIXME: ugly clone of the vectors here
    match serde_json::from_value(value.clone()) {
        Ok(vectors) => {
            Ok(VectorOrArrayOfVectors::into_array_of_vectors(vectors).unwrap_or_default())
        }
        Err(_) => Err(UserError::InvalidVectorsType {
            document_id: document_id(),
            value,
            subfield: name.to_owned(),
        }
        .into()),
    }
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_embeddings<R: io::Read + io::Seek>(
    // docid, prompt
    prompt_reader: grenad::Reader<R>,
    indexer: GrenadParameters,
    embedder: Arc<Embedder>,
    request_threads: &rayon::ThreadPool,
) -> Result<grenad::Reader<BufReader<File>>> {
    puffin::profile_function!();
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
