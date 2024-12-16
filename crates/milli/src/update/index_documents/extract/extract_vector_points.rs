use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use std::mem::size_of;
use std::str::from_utf8;
use std::sync::Arc;

use bytemuck::cast_slice;
use grenad::Writer;
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use serde_json::Value;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::constants::RESERVED_VECTORS_FIELD_NAME;
use crate::error::FaultSource;
use crate::index::IndexEmbeddingConfig;
use crate::prompt::{FieldsIdsMapWithMetadata, Prompt};
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::settings::InnerIndexSettingsDiff;
use crate::vector::error::{EmbedErrorKind, PossibleEmbeddingMistakes, UnusedVectorsDistribution};
use crate::vector::parsed_vectors::{ParsedVectorsDiff, VectorState};
use crate::vector::settings::ReindexAction;
use crate::vector::{Embedder, Embedding};
use crate::{try_split_array_at, DocumentId, FieldId, Result, ThreadPoolNoAbort};

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
    pub add_to_user_provided: RoaringBitmap,
    pub remove_from_user_provided: RoaringBitmap,
}

enum VectorStateDelta {
    NoChange,
    // Remove all vectors, generated or manual, from this document
    NowRemoved,

    NowManual(Vec<Vec<f32>>),

    // Add the vector computed from the specified prompt
    // Remove any previous vector
    // Note: changing the value of the prompt **does require** recording this delta
    NowGenerated(String),
}

impl VectorStateDelta {
    fn into_values(self) -> (bool, String, Vec<Vec<f32>>) {
        match self {
            VectorStateDelta::NoChange => Default::default(),
            VectorStateDelta::NowRemoved => (true, Default::default(), Default::default()),
            // We always delete the previous vectors
            VectorStateDelta::NowManual(add) => (true, Default::default(), add),
            VectorStateDelta::NowGenerated(prompt) => (true, prompt, Default::default()),
        }
    }
}

struct EmbedderVectorExtractor {
    embedder_name: String,
    embedder: Arc<Embedder>,
    prompt: Arc<Prompt>,

    // (docid) -> (prompt)
    prompts_writer: Writer<BufWriter<File>>,
    // (docid) -> ()
    remove_vectors_writer: Writer<BufWriter<File>>,
    // (docid, _index) -> KvWriterDelAdd -> Vector
    manual_vectors_writer: Writer<BufWriter<File>>,
    // The docids of the documents that contains a user defined embedding
    add_to_user_provided: RoaringBitmap,

    action: ExtractionAction,
}

struct DocumentOperation {
    // The docids of the documents that contains an auto-generated embedding
    remove_from_user_provided: RoaringBitmap,
}

enum ExtractionAction {
    SettingsFullReindex,
    SettingsRegeneratePrompts { old_prompt: Arc<Prompt> },
    DocumentOperation(DocumentOperation),
}

struct ManualEmbedderErrors {
    embedder_name: String,
    docid: String,
    other_docids: usize,
}

impl ManualEmbedderErrors {
    pub fn push_error(
        errors: &mut Option<ManualEmbedderErrors>,
        embedder_name: &str,
        document_id: impl Fn() -> Value,
    ) {
        match errors {
            Some(errors) => {
                if errors.embedder_name == embedder_name {
                    errors.other_docids = errors.other_docids.saturating_add(1)
                }
            }
            None => {
                *errors = Some(Self {
                    embedder_name: embedder_name.to_owned(),
                    docid: document_id().to_string(),
                    other_docids: 0,
                });
            }
        }
    }

    pub fn to_result(
        errors: Option<ManualEmbedderErrors>,
        possible_embedding_mistakes: &PossibleEmbeddingMistakes,
        unused_vectors_distribution: &UnusedVectorsDistribution,
    ) -> Result<()> {
        match errors {
            Some(errors) => {
                let embedder_name = &errors.embedder_name;
                let mut msg = format!(
                    r"While embedding documents for embedder `{embedder_name}`: no vectors provided for document {}{}",
                    errors.docid,
                    if errors.other_docids != 0 {
                        format!(" and at least {} other document(s)", errors.other_docids)
                    } else {
                        "".to_string()
                    }
                );

                msg += &format!("\n- Note: `{embedder_name}` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.{embedder_name}`.");

                let mut hint_count = 0;

                for (vector_misspelling, count) in
                    possible_embedding_mistakes.vector_mistakes().take(2)
                {
                    msg += &format!("\n- Hint: try replacing `{vector_misspelling}` by `_vectors` in {count} document(s).");
                    hint_count += 1;
                }

                for (embedder_misspelling, count) in possible_embedding_mistakes
                    .embedder_mistakes(embedder_name, unused_vectors_distribution)
                    .take(2)
                {
                    msg += &format!("\n- Hint: try replacing `_vectors.{embedder_misspelling}` by `_vectors.{embedder_name}` in {count} document(s).");
                    hint_count += 1;
                }

                if hint_count == 0 {
                    msg += &format!(
                        "\n- Hint: opt-out for a document with `_vectors.{embedder_name}: null`"
                    );
                }

                Err(crate::Error::UserError(crate::UserError::DocumentEmbeddingError(msg)))
            }
            None => Ok(()),
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
    embedders_configs: &[IndexEmbeddingConfig],
    settings_diff: &InnerIndexSettingsDiff,
    possible_embedding_mistakes: &PossibleEmbeddingMistakes,
) -> Result<(Vec<ExtractedVectorPoints>, UnusedVectorsDistribution)> {
    let mut unused_vectors_distribution = UnusedVectorsDistribution::new();
    let mut manual_errors = None;
    let reindex_vectors = settings_diff.reindex_vectors();

    let old_fields_ids_map = &settings_diff.old.fields_ids_map;
    let old_fields_ids_map =
        FieldsIdsMapWithMetadata::new(old_fields_ids_map, &settings_diff.old.searchable_fields_ids);

    let new_fields_ids_map = &settings_diff.new.fields_ids_map;
    let new_fields_ids_map =
        FieldsIdsMapWithMetadata::new(new_fields_ids_map, &settings_diff.new.searchable_fields_ids);

    // the vector field id may have changed
    let old_vectors_fid = old_fields_ids_map.id(RESERVED_VECTORS_FIELD_NAME);

    let new_vectors_fid = new_fields_ids_map.id(RESERVED_VECTORS_FIELD_NAME);

    let mut extractors = Vec::new();

    let mut configs = settings_diff.new.embedding_configs.clone().into_inner();
    let old_configs = &settings_diff.old.embedding_configs;

    if reindex_vectors {
        for (name, action) in settings_diff.embedding_config_updates.iter() {
            if let Some(action) = action.reindex() {
                let Some((embedder_name, (embedder, prompt, _quantized))) =
                    configs.remove_entry(name)
                else {
                    tracing::error!(embedder = name, "Requested embedder config not found");
                    continue;
                };

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

                let action = match action {
                    ReindexAction::FullReindex => ExtractionAction::SettingsFullReindex,
                    ReindexAction::RegeneratePrompts => {
                        let Some((_, old_prompt, _quantized)) = old_configs.get(name) else {
                            tracing::error!(embedder = name, "Old embedder config not found");
                            continue;
                        };

                        ExtractionAction::SettingsRegeneratePrompts { old_prompt }
                    }
                };

                extractors.push(EmbedderVectorExtractor {
                    embedder_name,
                    embedder,
                    prompt,
                    prompts_writer,
                    remove_vectors_writer,
                    manual_vectors_writer,
                    add_to_user_provided: RoaringBitmap::new(),
                    action,
                });
            } else {
                continue;
            }
        }
    } else {
        // document operation

        for (embedder_name, (embedder, prompt, _quantized)) in configs.into_iter() {
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
                prompts_writer,
                remove_vectors_writer,
                manual_vectors_writer,
                add_to_user_provided: RoaringBitmap::new(),
                action: ExtractionAction::DocumentOperation(DocumentOperation {
                    remove_from_user_provided: RoaringBitmap::new(),
                }),
            });
        }
    }

    let mut key_buffer = Vec::new();
    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        // this must always be serialized as (docid, external_docid);
        const SIZE_OF_DOCUMENTID: usize = std::mem::size_of::<DocumentId>();
        let (docid_bytes, external_id_bytes) =
            try_split_array_at::<u8, SIZE_OF_DOCUMENTID>(key).unwrap();
        debug_assert!(from_utf8(external_id_bytes).is_ok());
        let docid = DocumentId::from_be_bytes(docid_bytes);

        let obkv = obkv::KvReader::from_slice(value);
        key_buffer.clear();
        key_buffer.extend_from_slice(docid_bytes.as_slice());

        // since we only need the primary key when we throw an error we create this getter to
        // lazily get it when needed
        let document_id = || -> Value { from_utf8(external_id_bytes).unwrap().into() };

        let mut parsed_vectors = ParsedVectorsDiff::new(
            docid,
            embedders_configs,
            obkv,
            old_vectors_fid,
            new_vectors_fid,
        )
        .map_err(|error| error.to_crate_error(document_id().to_string()))?;

        for EmbedderVectorExtractor {
            embedder_name,
            embedder,
            prompt,
            prompts_writer,
            remove_vectors_writer,
            manual_vectors_writer,
            add_to_user_provided,
            action,
        } in extractors.iter_mut()
        {
            let embedder_is_manual = matches!(**embedder, Embedder::UserProvided(_));

            let (old, new) = parsed_vectors.remove(embedder_name);
            let delta = match action {
                ExtractionAction::SettingsFullReindex => match old {
                    // A full reindex can be triggered either by:
                    // 1. a new embedder
                    // 2. an existing embedder changed so that it must regenerate all generated embeddings.
                    // For a new embedder, there can be `_vectors.embedder` embeddings to add to the DB
                    VectorState::Inline(vectors) => {
                        if !vectors.must_regenerate() {
                            add_to_user_provided.insert(docid);
                        }

                        match vectors.into_array_of_vectors() {
                            Some(add_vectors) => {
                                if add_vectors.len() > usize::from(u8::MAX) {
                                    return Err(crate::Error::UserError(
                                        crate::UserError::TooManyVectors(
                                            document_id().to_string(),
                                            add_vectors.len(),
                                        ),
                                    ));
                                }
                                VectorStateDelta::NowManual(add_vectors)
                            }
                            None => VectorStateDelta::NoChange,
                        }
                    }
                    // this happens only when an existing embedder changed. We cannot regenerate userProvided vectors
                    VectorState::Manual => VectorStateDelta::NoChange,
                    // generated vectors must be regenerated
                    VectorState::Generated => {
                        if embedder_is_manual {
                            ManualEmbedderErrors::push_error(
                                &mut manual_errors,
                                embedder_name.as_str(),
                                document_id,
                            );
                            continue;
                        }
                        regenerate_prompt(obkv, prompt, &new_fields_ids_map)?
                    }
                },
                // prompt regeneration is only triggered for existing embedders
                ExtractionAction::SettingsRegeneratePrompts { old_prompt } => {
                    if old.must_regenerate() {
                        if embedder_is_manual {
                            ManualEmbedderErrors::push_error(
                                &mut manual_errors,
                                embedder_name.as_str(),
                                document_id,
                            );
                            continue;
                        }
                        regenerate_if_prompt_changed(
                            obkv,
                            (old_prompt, prompt),
                            (&old_fields_ids_map, &new_fields_ids_map),
                        )?
                    } else {
                        // we can simply ignore user provided vectors as they are not regenerated and are
                        // already in the DB since this is an existing embedder
                        VectorStateDelta::NoChange
                    }
                }
                ExtractionAction::DocumentOperation(DocumentOperation {
                    remove_from_user_provided,
                }) => extract_vector_document_diff(
                    docid,
                    obkv,
                    prompt,
                    (add_to_user_provided, remove_from_user_provided),
                    (old, new),
                    (&old_fields_ids_map, &new_fields_ids_map),
                    document_id,
                    embedder_name,
                    embedder_is_manual,
                    &mut manual_errors,
                )?,
            };
            // and we finally push the unique vectors into the writer
            push_vectors_diff(
                remove_vectors_writer,
                prompts_writer,
                manual_vectors_writer,
                &mut key_buffer,
                delta,
            )?;
        }

        unused_vectors_distribution.append(parsed_vectors);
    }

    ManualEmbedderErrors::to_result(
        manual_errors,
        possible_embedding_mistakes,
        &unused_vectors_distribution,
    )?;

    let mut results = Vec::new();

    for EmbedderVectorExtractor {
        embedder_name,
        embedder,
        prompt: _,
        prompts_writer,
        remove_vectors_writer,
        action,
        manual_vectors_writer,
        add_to_user_provided,
    } in extractors
    {
        let remove_from_user_provided =
            if let ExtractionAction::DocumentOperation(DocumentOperation {
                remove_from_user_provided,
            }) = action
            {
                remove_from_user_provided
            } else {
                Default::default()
            };

        results.push(ExtractedVectorPoints {
            manual_vectors: writer_into_reader(manual_vectors_writer)?,
            remove_vectors: writer_into_reader(remove_vectors_writer)?,
            prompts: writer_into_reader(prompts_writer)?,
            embedder,
            embedder_name,
            add_to_user_provided,
            remove_from_user_provided,
        })
    }

    Ok((results, unused_vectors_distribution))
}

#[allow(clippy::too_many_arguments)] // feel free to find efficient way to factor arguments
fn extract_vector_document_diff(
    docid: DocumentId,
    obkv: &obkv::KvReader<FieldId>,
    prompt: &Prompt,
    (add_to_user_provided, remove_from_user_provided): (&mut RoaringBitmap, &mut RoaringBitmap),
    (old, new): (VectorState, VectorState),
    (old_fields_ids_map, new_fields_ids_map): (
        &FieldsIdsMapWithMetadata,
        &FieldsIdsMapWithMetadata,
    ),
    document_id: impl Fn() -> Value,
    embedder_name: &str,
    embedder_is_manual: bool,
    manual_errors: &mut Option<ManualEmbedderErrors>,
) -> Result<VectorStateDelta> {
    match (old.must_regenerate(), new.must_regenerate()) {
        (true, true) | (false, false) => {}
        (true, false) => {
            add_to_user_provided.insert(docid);
        }
        (false, true) => {
            remove_from_user_provided.insert(docid);
        }
    }

    let delta = match (old, new) {
        // regardless of the previous state, if a document now contains inline _vectors, they must
        // be extracted manually
        (_old, VectorState::Inline(new)) => match new.into_array_of_vectors() {
            Some(add_vectors) => {
                if add_vectors.len() > usize::from(u8::MAX) {
                    return Err(crate::Error::UserError(crate::UserError::TooManyVectors(
                        document_id().to_string(),
                        add_vectors.len(),
                    )));
                }

                VectorStateDelta::NowManual(add_vectors)
            }
            None => VectorStateDelta::NoChange,
        },
        // no `_vectors` anywhere, we check for document removal and otherwise we regenerate the prompt if the
        // document changed
        (VectorState::Generated, VectorState::Generated) => {
            // Do we keep this document?
            let document_is_kept = obkv
                .iter()
                .map(|(_, deladd)| KvReaderDelAdd::from_slice(deladd))
                .any(|deladd| deladd.get(DelAdd::Addition).is_some());

            if document_is_kept {
                if embedder_is_manual {
                    ManualEmbedderErrors::push_error(manual_errors, embedder_name, document_id);
                    return Ok(VectorStateDelta::NoChange);
                }
                // Don't give up if the old prompt was failing
                let old_prompt = Some(&prompt).map(|p| {
                    p.render_kvdeladd(obkv, DelAdd::Deletion, old_fields_ids_map)
                        .unwrap_or_default()
                });
                let new_prompt =
                    prompt.render_kvdeladd(obkv, DelAdd::Addition, new_fields_ids_map)?;
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
        // inline to the left is not supposed to be possible because the embedder is not new, so `_vectors` was removed from
        // the previous version of the document.
        // Manual -> Generated is also not possible without an Inline to the right (which is handled above)
        // Generated -> Generated is handled above, so not possible
        // As a result, this code is unreachable
        (_not_generated, VectorState::Generated) => {
            // Do we keep this document?
            let document_is_kept = obkv
                .iter()
                .map(|(_, deladd)| KvReaderDelAdd::from_slice(deladd))
                .any(|deladd| deladd.get(DelAdd::Addition).is_some());
            if document_is_kept {
                if embedder_is_manual {
                    ManualEmbedderErrors::push_error(manual_errors, embedder_name, document_id);
                    return Ok(VectorStateDelta::NoChange);
                }
                // becomes autogenerated
                VectorStateDelta::NowGenerated(prompt.render_kvdeladd(
                    obkv,
                    DelAdd::Addition,
                    new_fields_ids_map,
                )?)
            } else {
                // make sure the document is always removed from user provided on removal
                remove_from_user_provided.insert(docid);
                VectorStateDelta::NowRemoved
            }
        }
        // inline to the left is not possible because the embedder is not new, and so `_vectors` was removed from the previous
        // version of the document.
        // however the Rust type system cannot know that.
        (_manual, VectorState::Manual) => {
            // Do we keep this document?
            let document_is_kept = obkv
                .iter()
                .map(|(_, deladd)| KvReaderDelAdd::from_slice(deladd))
                .any(|deladd| deladd.get(DelAdd::Addition).is_some());
            if document_is_kept {
                // if the new version of documents has the vectors in the DB,
                // then they are user-provided and nothing possibly changed
                VectorStateDelta::NoChange
            } else {
                // make sure the document is always removed from user provided on removal
                remove_from_user_provided.insert(docid);
                VectorStateDelta::NowRemoved
            }
        }
    };

    Ok(delta)
}

fn regenerate_if_prompt_changed(
    obkv: &obkv::KvReader<FieldId>,
    (old_prompt, new_prompt): (&Prompt, &Prompt),
    (old_fields_ids_map, new_fields_ids_map): (
        &FieldsIdsMapWithMetadata,
        &FieldsIdsMapWithMetadata,
    ),
) -> Result<VectorStateDelta> {
    let old_prompt = old_prompt
        .render_kvdeladd(obkv, DelAdd::Deletion, old_fields_ids_map)
        .unwrap_or(Default::default());
    let new_prompt = new_prompt.render_kvdeladd(obkv, DelAdd::Addition, new_fields_ids_map)?;

    if new_prompt == old_prompt {
        return Ok(VectorStateDelta::NoChange);
    }
    Ok(VectorStateDelta::NowGenerated(new_prompt))
}

fn regenerate_prompt(
    obkv: &obkv::KvReader<FieldId>,
    prompt: &Prompt,
    new_fields_ids_map: &FieldsIdsMapWithMetadata,
) -> Result<VectorStateDelta> {
    let prompt = prompt.render_kvdeladd(obkv, DelAdd::Addition, new_fields_ids_map)?;

    Ok(VectorStateDelta::NowGenerated(prompt))
}

/// We cannot compute the diff between both Del and Add vectors.
/// We'll push every vector and compute the difference later in TypedChunk.
fn push_vectors_diff(
    remove_vectors_writer: &mut Writer<BufWriter<File>>,
    prompts_writer: &mut Writer<BufWriter<File>>,
    manual_vectors_writer: &mut Writer<BufWriter<File>>,
    key_buffer: &mut Vec<u8>,
    delta: VectorStateDelta,
) -> Result<()> {
    let (must_remove, prompt, mut add_vectors) = delta.into_values();
    if must_remove {
        key_buffer.truncate(TRUNCATE_SIZE);
        remove_vectors_writer.insert(&key_buffer, [])?;
    }
    if !prompt.is_empty() {
        key_buffer.truncate(TRUNCATE_SIZE);
        prompts_writer.insert(&key_buffer, prompt.as_bytes())?;
    }

    // We sort and dedup the vectors
    add_vectors.sort_unstable_by(|a, b| compare_vectors(a, b));
    add_vectors.dedup_by(|a, b| compare_vectors(a, b).is_eq());

    // insert vectors into the writer
    for (i, vector) in add_vectors.into_iter().enumerate().take(u16::MAX as usize) {
        // Generate the key by extending the unique index to it.
        key_buffer.truncate(TRUNCATE_SIZE);
        let index = u16::try_from(i).unwrap();
        key_buffer.extend_from_slice(&index.to_be_bytes());

        // We insert only the Add part of the Obkv to inform
        // that we only want to remove all those vectors.
        let mut obkv = KvWriterDelAdd::memory();
        obkv.insert(DelAdd::Addition, cast_slice(&vector))?;
        let bytes = obkv.into_inner()?;
        manual_vectors_writer.insert(&key_buffer, bytes)?;
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
    embedder_name: &str,
    possible_embedding_mistakes: &PossibleEmbeddingMistakes,
    unused_vectors_distribution: &UnusedVectorsDistribution,
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
            let chunked_embeds = embed_chunks(
                &embedder,
                std::mem::replace(&mut chunks, Vec::with_capacity(n_chunks)),
                embedder_name,
                possible_embedding_mistakes,
                unused_vectors_distribution,
                request_threads,
            )?;

            for (docid, embeddings) in chunks_ids
                .iter()
                .flat_map(|docids| docids.iter())
                .zip(chunked_embeds.iter().flat_map(|embeds| embeds.iter()))
            {
                state_writer.insert(docid.to_be_bytes(), cast_slice(embeddings))?;
            }
            chunks_ids.clear();
        }
    }

    // send last chunk
    if !chunks.is_empty() {
        let chunked_embeds = embed_chunks(
            &embedder,
            std::mem::take(&mut chunks),
            embedder_name,
            possible_embedding_mistakes,
            unused_vectors_distribution,
            request_threads,
        )?;
        for (docid, embeddings) in chunks_ids
            .iter()
            .flat_map(|docids| docids.iter())
            .zip(chunked_embeds.iter().flat_map(|embeds| embeds.iter()))
        {
            state_writer.insert(docid.to_be_bytes(), cast_slice(embeddings))?;
        }
    }

    if !current_chunk.is_empty() {
        let embeds = embed_chunks(
            &embedder,
            vec![std::mem::take(&mut current_chunk)],
            embedder_name,
            possible_embedding_mistakes,
            unused_vectors_distribution,
            request_threads,
        )?;

        if let Some(embeds) = embeds.first() {
            for (docid, embeddings) in current_chunk_ids.iter().zip(embeds.iter()) {
                state_writer.insert(docid.to_be_bytes(), cast_slice(embeddings))?;
            }
        }
    }

    writer_into_reader(state_writer)
}

fn embed_chunks(
    embedder: &Embedder,
    text_chunks: Vec<Vec<String>>,
    embedder_name: &str,
    possible_embedding_mistakes: &PossibleEmbeddingMistakes,
    unused_vectors_distribution: &UnusedVectorsDistribution,
    request_threads: &ThreadPoolNoAbort,
) -> Result<Vec<Vec<Embedding>>> {
    match embedder.embed_chunks(text_chunks, request_threads) {
        Ok(chunks) => Ok(chunks),
        Err(error) => {
            if let FaultSource::Bug = error.fault {
                Err(crate::Error::InternalError(crate::InternalError::VectorEmbeddingError(
                    error.into(),
                )))
            } else {
                let mut msg =
                    format!(r"While embedding documents for embedder `{embedder_name}`: {error}");

                if let EmbedErrorKind::ManualEmbed(_) = &error.kind {
                    msg += &format!("\n- Note: `{embedder_name}` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.{embedder_name}`.");
                }

                let mut hint_count = 0;

                for (vector_misspelling, count) in
                    possible_embedding_mistakes.vector_mistakes().take(2)
                {
                    msg += &format!("\n- Hint: try replacing `{vector_misspelling}` by `_vectors` in {count} document(s).");
                    hint_count += 1;
                }

                for (embedder_misspelling, count) in possible_embedding_mistakes
                    .embedder_mistakes(embedder_name, unused_vectors_distribution)
                    .take(2)
                {
                    msg += &format!("\n- Hint: try replacing `_vectors.{embedder_misspelling}` by `_vectors.{embedder_name}` in {count} document(s).");
                    hint_count += 1;
                }

                if hint_count == 0 {
                    if let EmbedErrorKind::ManualEmbed(_) = &error.kind {
                        msg += &format!(
                            "\n- Hint: opt-out for a document with `_vectors.{embedder_name}: null`"
                        );
                    }
                }

                Err(crate::Error::UserError(crate::UserError::DocumentEmbeddingError(msg)))
            }
        }
    }
}
