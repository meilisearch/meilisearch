use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use std::mem::size_of;
use std::str::from_utf8;
use std::sync::Arc;

use bumpalo::Bump;
use bytemuck::cast_slice;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use grenad::Writer;
use obkv::KvReaderU16;
use ordered_float::OrderedFloat;
use serde_json::Value;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::constants::RESERVED_VECTORS_FIELD_NAME;
use crate::error::FaultSource;
use crate::fields_ids_map::metadata::FieldIdMapWithMetadata;
use crate::progress::EmbedderStats;
use crate::prompt::Prompt;
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::settings::InnerIndexSettingsDiff;
use crate::vector::db::{EmbedderInfo, EmbeddingStatusDelta};
use crate::vector::error::{EmbedErrorKind, PossibleEmbeddingMistakes, UnusedVectorsDistribution};
use crate::vector::extractor::{Extractor, ExtractorDiff, RequestFragmentExtractor};
use crate::vector::parsed_vectors::{ParsedVectorsDiff, VectorState};
use crate::vector::session::{EmbedSession, Metadata, OnEmbed};
use crate::vector::settings::ReindexAction;
use crate::vector::{Embedder, Embedding, RuntimeEmbedder, RuntimeFragment};
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
    // docid, extractor_id -> Option<json>
    pub inputs: grenad::Reader<BufReader<File>>,

    // embedder
    pub embedder_name: String,
    pub runtime: Arc<RuntimeEmbedder>,
    pub embedding_status_delta: EmbeddingStatusDelta,
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

    // Add and remove the vectors computed from the fragments.
    UpdateGeneratedFromFragments(Vec<(String, ExtractorDiff<Value>)>),

    /// Wasn't generated from fragments, but now is.
    /// Delete any previous vectors and add the new vectors
    NowGeneratedFromFragments(Vec<(String, Value)>),
}

impl VectorStateDelta {
    fn into_values(self) -> (bool, String, BTreeMap<String, Option<Value>>, Vec<Vec<f32>>) {
        match self {
            VectorStateDelta::NoChange => Default::default(),
            VectorStateDelta::NowRemoved => {
                (true, Default::default(), Default::default(), Default::default())
            }
            VectorStateDelta::NowManual(add) => (true, Default::default(), Default::default(), add),
            VectorStateDelta::NowGenerated(prompt) => {
                (true, prompt, Default::default(), Default::default())
            }
            VectorStateDelta::UpdateGeneratedFromFragments(fragments) => (
                false,
                Default::default(),
                ExtractorDiff::into_list_of_changes(fragments),
                Default::default(),
            ),
            VectorStateDelta::NowGeneratedFromFragments(items) => (
                true,
                Default::default(),
                ExtractorDiff::into_list_of_changes(
                    items.into_iter().map(|(name, value)| (name, ExtractorDiff::Added(value))),
                ),
                Default::default(),
            ),
        }
    }
}

struct EmbedderVectorExtractor<'a> {
    embedder_name: String,
    embedder_info: &'a EmbedderInfo,
    runtime: Arc<RuntimeEmbedder>,

    // (docid) -> (prompt)
    prompts_writer: Writer<BufWriter<File>>,
    // (docid, extractor_id) -> (Option<Value>)
    inputs_writer: Writer<BufWriter<File>>,
    // (docid) -> ()
    remove_vectors_writer: Writer<BufWriter<File>>,
    // (docid, _index) -> KvWriterDelAdd -> Vector
    manual_vectors_writer: Writer<BufWriter<File>>,
    embedding_status_delta: EmbeddingStatusDelta,

    action: ExtractionAction,
}

enum ExtractionAction {
    SettingsFullReindex,
    SettingsRegeneratePrompts {
        old_runtime: Arc<RuntimeEmbedder>,
    },
    /// List of fragments to update/add
    SettingsRegenerateFragments {
        // name and indices, respectively in old and new runtime, of the fragments to examine.
        must_regenerate_fragments: BTreeMap<String, (Option<usize>, usize)>,
        old_runtime: Arc<RuntimeEmbedder>,
    },
    DocumentOperation,
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
    settings_diff: &InnerIndexSettingsDiff,
    embedder_info: &[(String, EmbedderInfo)],
    possible_embedding_mistakes: &PossibleEmbeddingMistakes,
) -> Result<(Vec<ExtractedVectorPoints>, UnusedVectorsDistribution)> {
    let mut unused_vectors_distribution = UnusedVectorsDistribution::new();
    let mut manual_errors = None;
    let reindex_vectors = settings_diff.reindex_vectors();

    let old_fields_ids_map = &settings_diff.old.fields_ids_map;

    let new_fields_ids_map = &settings_diff.new.fields_ids_map;

    // the vector field id may have changed
    let old_vectors_fid = old_fields_ids_map.id(RESERVED_VECTORS_FIELD_NAME);

    let new_vectors_fid = new_fields_ids_map.id(RESERVED_VECTORS_FIELD_NAME);

    let mut extractors = Vec::new();

    let mut configs = settings_diff.new.runtime_embedders.clone().into_inner();
    let old_configs = &settings_diff.old.runtime_embedders;
    if reindex_vectors {
        for (name, action) in settings_diff.embedding_config_updates.iter() {
            if let Some(action) = action.reindex() {
                let (_, embedder_info) =
                    embedder_info.iter().find(|(embedder_name, _)| embedder_name == name).unwrap();

                let Some((embedder_name, runtime)) = configs.remove_entry(name) else {
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

                let inputs_writer = create_writer(
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
                    ReindexAction::RegenerateFragments(regenerate_fragments) => {
                        let Some(old_runtime) = old_configs.get(name) else {
                            tracing::error!(embedder = name, "Old embedder config not found");
                            continue;
                        };

                        let fragment_diffs = regenerate_fragments
                            .iter()
                            .filter_map(|(name, fragment)| match fragment {
                                crate::vector::settings::RegenerateFragment::Update => {
                                    let old_value = old_runtime
                                        .fragments()
                                        .binary_search_by_key(&name, |fragment| &fragment.name)
                                        .ok();
                                    let Ok(new_value) = runtime
                                        .fragments()
                                        .binary_search_by_key(&name, |fragment| &fragment.name)
                                    else {
                                        return None;
                                    };
                                    Some((name.clone(), (old_value, new_value)))
                                }
                                // was already handled in transform
                                crate::vector::settings::RegenerateFragment::Remove => None,
                                crate::vector::settings::RegenerateFragment::Add => {
                                    let Ok(new_value) = runtime
                                        .fragments()
                                        .binary_search_by_key(&name, |fragment| &fragment.name)
                                    else {
                                        return None;
                                    };
                                    Some((name.clone(), (None, new_value)))
                                }
                            })
                            .collect();
                        ExtractionAction::SettingsRegenerateFragments {
                            old_runtime: old_runtime.clone(),
                            must_regenerate_fragments: fragment_diffs,
                        }
                    }

                    ReindexAction::RegeneratePrompts => {
                        let Some(old_runtime) = old_configs.get(name) else {
                            tracing::error!(embedder = name, "Old embedder config not found");
                            continue;
                        };

                        ExtractionAction::SettingsRegeneratePrompts {
                            old_runtime: old_runtime.clone(),
                        }
                    }
                };

                extractors.push(EmbedderVectorExtractor {
                    embedder_name,
                    runtime,
                    embedder_info,
                    prompts_writer,
                    inputs_writer,
                    remove_vectors_writer,
                    manual_vectors_writer,
                    embedding_status_delta: Default::default(),
                    action,
                });
            } else {
                continue;
            }
        }
    } else {
        // document operation
        for (embedder_name, runtime) in configs.into_iter() {
            let (_, embedder_info) = embedder_info
                .iter()
                .find(|(name, _)| embedder_name.as_str() == name.as_str())
                .unwrap();

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

            let inputs_writer = create_writer(
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
                runtime,
                embedder_info,
                prompts_writer,
                inputs_writer,
                remove_vectors_writer,
                manual_vectors_writer,
                embedding_status_delta: Default::default(),
                action: ExtractionAction::DocumentOperation,
            });
        }
    }

    let mut key_buffer = Vec::new();
    let mut cursor = obkv_documents.into_cursor()?;
    let mut doc_alloc = Bump::new();
    while let Some((key, value)) = cursor.move_on_next()? {
        doc_alloc.reset();
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

        let regenerate_for_embedders = embedder_info
            .iter()
            .filter(|&(_, infos)| infos.embedding_status.must_regenerate(docid))
            .map(|(name, _)| name.clone());
        let mut parsed_vectors = ParsedVectorsDiff::new(
            regenerate_for_embedders,
            obkv,
            old_vectors_fid,
            new_vectors_fid,
        )
        .map_err(|error| error.to_crate_error(document_id().to_string()))?;

        for EmbedderVectorExtractor {
            embedder_name,
            runtime,
            embedder_info,
            prompts_writer,
            inputs_writer,
            remove_vectors_writer,
            manual_vectors_writer,
            embedding_status_delta,
            action,
        } in extractors.iter_mut()
        {
            let embedder_is_manual = matches!(*runtime.embedder, Embedder::UserProvided(_));

            let (old_is_user_provided, old_must_regenerate) =
                embedder_info.embedding_status.is_user_provided_must_regenerate(docid);
            let (old, new) = parsed_vectors.remove(embedder_name);
            let new_must_regenerate = new.must_regenerate();
            let delta = match action {
                ExtractionAction::SettingsFullReindex => match old {
                    // A full reindex can be triggered either by:
                    // 1. a new embedder
                    // 2. an existing embedder changed so that it must regenerate all generated embeddings.
                    // For a new embedder, there can be `_vectors.embedder` embeddings to add to the DB
                    VectorState::Inline(vectors) => match vectors.into_array_of_vectors() {
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
                    },
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
                        let has_fragments = !runtime.fragments().is_empty();

                        if has_fragments {
                            regenerate_all_fragments(
                                runtime.fragments(),
                                &doc_alloc,
                                new_fields_ids_map,
                                obkv,
                            )
                        } else {
                            regenerate_prompt(obkv, &runtime.document_template, new_fields_ids_map)?
                        }
                    }
                },
                ExtractionAction::SettingsRegenerateFragments {
                    must_regenerate_fragments,
                    old_runtime,
                } => {
                    if old.must_regenerate() {
                        let has_fragments = !runtime.fragments().is_empty();
                        let old_has_fragments = !old_runtime.fragments().is_empty();

                        let is_adding_fragments = has_fragments && !old_has_fragments;

                        if !has_fragments {
                            // removing fragments
                            regenerate_prompt(obkv, &runtime.document_template, new_fields_ids_map)?
                        } else if is_adding_fragments ||
                        // regenerate all fragments when going from user provided to ! user provided
                        old_is_user_provided
                        {
                            regenerate_all_fragments(
                                runtime.fragments(),
                                &doc_alloc,
                                new_fields_ids_map,
                                obkv,
                            )
                        } else {
                            let mut fragment_diff = Vec::new();
                            let new_fields_ids_map = new_fields_ids_map.as_fields_ids_map();

                            let obkv_document = crate::update::new::document::KvDelAddDocument::new(
                                obkv,
                                DelAdd::Addition,
                                new_fields_ids_map,
                            );
                            for (name, (old_index, new_index)) in must_regenerate_fragments {
                                let Some(new) = runtime.fragments().get(*new_index) else {
                                    continue;
                                };

                                let new =
                                    RequestFragmentExtractor::new(new, &doc_alloc).ignore_errors();

                                let diff = {
                                    let old = old_index.as_ref().and_then(|old| {
                                        let old = old_runtime.fragments().get(*old)?;
                                        Some(
                                            RequestFragmentExtractor::new(old, &doc_alloc)
                                                .ignore_errors(),
                                        )
                                    });
                                    let old = old.as_ref();
                                    Extractor::diff_settings(&new, &obkv_document, &(), old)
                                }
                                .expect("ignoring errors so this cannot fail");
                                fragment_diff.push((name.clone(), diff));
                            }
                            VectorStateDelta::UpdateGeneratedFromFragments(fragment_diff)
                        }
                    } else {
                        // we can simply ignore user provided vectors as they are not regenerated and are
                        // already in the DB since this is an existing embedder
                        VectorStateDelta::NoChange
                    }
                }
                // prompt regeneration is only triggered for existing embedders
                ExtractionAction::SettingsRegeneratePrompts { old_runtime } => {
                    if old.must_regenerate() {
                        if embedder_is_manual {
                            ManualEmbedderErrors::push_error(
                                &mut manual_errors,
                                embedder_name.as_str(),
                                document_id,
                            );
                            continue;
                        }
                        let has_fragments = !runtime.fragments().is_empty();

                        if has_fragments {
                            regenerate_all_fragments(
                                runtime.fragments(),
                                &doc_alloc,
                                new_fields_ids_map,
                                obkv,
                            )
                        } else {
                            regenerate_if_prompt_changed(
                                obkv,
                                (&old_runtime.document_template, &runtime.document_template),
                                (old_fields_ids_map, new_fields_ids_map),
                            )?
                        }
                    } else {
                        // we can simply ignore user provided vectors as they are not regenerated and are
                        // already in the DB since this is an existing embedder
                        VectorStateDelta::NoChange
                    }
                }
                ExtractionAction::DocumentOperation => extract_vector_document_diff(
                    obkv,
                    runtime,
                    &doc_alloc,
                    (old, new),
                    (old_fields_ids_map, new_fields_ids_map),
                    document_id,
                    embedder_name,
                    embedder_is_manual,
                    &mut manual_errors,
                )?,
            };

            // update the embedding status
            push_embedding_status_delta(
                embedding_status_delta,
                docid,
                &delta,
                new_must_regenerate,
                old_is_user_provided,
                old_must_regenerate,
            );

            // and we finally push the unique vectors into the writer
            push_vectors_diff(
                remove_vectors_writer,
                prompts_writer,
                inputs_writer,
                manual_vectors_writer,
                &mut key_buffer,
                delta,
                runtime.fragments(),
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
        runtime,
        embedder_info: _,
        prompts_writer,
        inputs_writer,
        remove_vectors_writer,
        action: _,
        manual_vectors_writer,
        embedding_status_delta,
    } in extractors
    {
        results.push(ExtractedVectorPoints {
            manual_vectors: writer_into_reader(manual_vectors_writer)?,
            remove_vectors: writer_into_reader(remove_vectors_writer)?,
            prompts: writer_into_reader(prompts_writer)?,
            inputs: writer_into_reader(inputs_writer)?,
            runtime,
            embedder_name,
            embedding_status_delta,
        })
    }

    Ok((results, unused_vectors_distribution))
}

fn push_embedding_status_delta(
    embedding_status_delta: &mut EmbeddingStatusDelta,
    docid: DocumentId,
    delta: &VectorStateDelta,
    new_must_regenerate: bool,
    old_is_user_provided: bool,
    old_must_regenerate: bool,
) {
    let new_is_user_provided = match delta {
        VectorStateDelta::NoChange => old_is_user_provided,
        VectorStateDelta::NowRemoved => {
            embedding_status_delta.clear_docid(docid, old_is_user_provided, old_must_regenerate);
            return;
        }
        VectorStateDelta::NowManual(_) => true,
        VectorStateDelta::NowGenerated(_)
        | VectorStateDelta::UpdateGeneratedFromFragments(_)
        | VectorStateDelta::NowGeneratedFromFragments(_) => false,
    };

    embedding_status_delta.push_delta(
        docid,
        old_is_user_provided,
        old_must_regenerate,
        new_is_user_provided,
        new_must_regenerate,
    );
}

#[allow(clippy::too_many_arguments)] // feel free to find efficient way to factor arguments
fn extract_vector_document_diff(
    obkv: &obkv::KvReader<FieldId>,
    runtime: &RuntimeEmbedder,
    doc_alloc: &Bump,
    (old, new): (VectorState, VectorState),
    (old_fields_ids_map, new_fields_ids_map): (&FieldIdMapWithMetadata, &FieldIdMapWithMetadata),
    document_id: impl Fn() -> Value,
    embedder_name: &str,
    embedder_is_manual: bool,
    manual_errors: &mut Option<ManualEmbedderErrors>,
) -> Result<VectorStateDelta> {
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
                let has_fragments = !runtime.fragments().is_empty();
                if has_fragments {
                    let mut fragment_diff = Vec::new();
                    let old_fields_ids_map = old_fields_ids_map.as_fields_ids_map();
                    let new_fields_ids_map = new_fields_ids_map.as_fields_ids_map();

                    let old_document = crate::update::new::document::KvDelAddDocument::new(
                        obkv,
                        DelAdd::Deletion,
                        old_fields_ids_map,
                    );

                    let new_document = crate::update::new::document::KvDelAddDocument::new(
                        obkv,
                        DelAdd::Addition,
                        new_fields_ids_map,
                    );

                    for new in runtime.fragments() {
                        let name = &new.name;
                        let fragment =
                            RequestFragmentExtractor::new(new, doc_alloc).ignore_errors();

                        let diff = fragment
                            .diff_documents(&old_document, &new_document, &())
                            .expect("ignoring errors so this cannot fail");

                        fragment_diff.push((name.clone(), diff));
                    }
                    VectorStateDelta::UpdateGeneratedFromFragments(fragment_diff)
                } else {
                    let prompt = &runtime.document_template;
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

                let has_fragments = !runtime.fragments().is_empty();

                if has_fragments {
                    regenerate_all_fragments(
                        runtime.fragments(),
                        doc_alloc,
                        new_fields_ids_map,
                        obkv,
                    )
                } else {
                    // becomes autogenerated
                    VectorStateDelta::NowGenerated(runtime.document_template.render_kvdeladd(
                        obkv,
                        DelAdd::Addition,
                        new_fields_ids_map,
                    )?)
                }
            } else {
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
                VectorStateDelta::NowRemoved
            }
        }
    };

    Ok(delta)
}

fn regenerate_if_prompt_changed(
    obkv: &obkv::KvReader<FieldId>,
    (old_prompt, new_prompt): (&Prompt, &Prompt),
    (old_fields_ids_map, new_fields_ids_map): (&FieldIdMapWithMetadata, &FieldIdMapWithMetadata),
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
    new_fields_ids_map: &FieldIdMapWithMetadata,
) -> Result<VectorStateDelta> {
    let prompt = prompt.render_kvdeladd(obkv, DelAdd::Addition, new_fields_ids_map)?;

    Ok(VectorStateDelta::NowGenerated(prompt))
}

fn regenerate_all_fragments<'a>(
    fragments: impl IntoIterator<Item = &'a RuntimeFragment>,
    doc_alloc: &Bump,
    new_fields_ids_map: &FieldIdMapWithMetadata,
    obkv: &KvReaderU16,
) -> VectorStateDelta {
    let mut fragment_diff = Vec::new();
    let new_fields_ids_map = new_fields_ids_map.as_fields_ids_map();

    let obkv_document = crate::update::new::document::KvDelAddDocument::new(
        obkv,
        DelAdd::Addition,
        new_fields_ids_map,
    );
    for new in fragments {
        let name = &new.name;
        let new = RequestFragmentExtractor::new(new, doc_alloc).ignore_errors();

        let diff = new.extract(&obkv_document, &()).expect("ignoring errors so this cannot fail");
        if let Some(value) = diff {
            fragment_diff.push((name.clone(), value));
        }
    }
    VectorStateDelta::NowGeneratedFromFragments(fragment_diff)
}

/// We cannot compute the diff between both Del and Add vectors.
/// We'll push every vector and compute the difference later in TypedChunk.
fn push_vectors_diff(
    remove_vectors_writer: &mut Writer<BufWriter<File>>,
    prompts_writer: &mut Writer<BufWriter<File>>,
    inputs_writer: &mut Writer<BufWriter<File>>,
    manual_vectors_writer: &mut Writer<BufWriter<File>>,
    key_buffer: &mut Vec<u8>,
    delta: VectorStateDelta,
    fragments: &[RuntimeFragment],
) -> Result<()> {
    let (must_remove, prompt, mut fragment_delta, mut add_vectors) = delta.into_values();
    if must_remove {
        key_buffer.truncate(TRUNCATE_SIZE);
        remove_vectors_writer.insert(&key_buffer, [])?;
    }
    if !prompt.is_empty() {
        key_buffer.truncate(TRUNCATE_SIZE);
        prompts_writer.insert(&key_buffer, prompt.as_bytes())?;
    }

    if !fragment_delta.is_empty() {
        let mut scratch = Vec::new();
        let mut fragment_delta: Vec<_> = fragments
            .iter()
            .filter_map(|fragment| {
                let delta = fragment_delta.remove(&fragment.name)?;
                Some((fragment.id, delta))
            })
            .collect();

        fragment_delta.sort_unstable_by_key(|(id, _)| *id);
        for (id, value) in fragment_delta {
            key_buffer.truncate(TRUNCATE_SIZE);
            key_buffer.push(id);
            if let Some(value) = value {
                scratch.clear();
                serde_json::to_writer(&mut scratch, &value).unwrap();
                inputs_writer.insert(&key_buffer, &scratch)?;
            } else {
                inputs_writer.insert(&key_buffer, [])?;
            }
        }
    }

    if !add_vectors.is_empty() {
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
    }

    Ok(())
}

/// Compares two vectors by using the OrderingFloat helper.
fn compare_vectors(a: &[f32], b: &[f32]) -> Ordering {
    a.iter().copied().map(OrderedFloat).cmp(b.iter().copied().map(OrderedFloat))
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_embeddings_from_prompts<R: io::Read + io::Seek>(
    // docid, prompt
    prompt_reader: grenad::Reader<R>,
    indexer: GrenadParameters,
    runtime: Arc<RuntimeEmbedder>,
    embedder_name: &str,
    possible_embedding_mistakes: &PossibleEmbeddingMistakes,
    embedder_stats: &EmbedderStats,
    unused_vectors_distribution: &UnusedVectorsDistribution,
    request_threads: &ThreadPoolNoAbort,
) -> Result<grenad::Reader<BufReader<File>>> {
    let embedder = &runtime.embedder;
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
                embedder,
                std::mem::replace(&mut chunks, Vec::with_capacity(n_chunks)),
                embedder_name,
                possible_embedding_mistakes,
                embedder_stats,
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
            embedder,
            std::mem::take(&mut chunks),
            embedder_name,
            possible_embedding_mistakes,
            embedder_stats,
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
            embedder,
            vec![std::mem::take(&mut current_chunk)],
            embedder_name,
            possible_embedding_mistakes,
            embedder_stats,
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
    embedder_stats: &EmbedderStats,
    unused_vectors_distribution: &UnusedVectorsDistribution,
    request_threads: &ThreadPoolNoAbort,
) -> Result<Vec<Vec<Embedding>>> {
    match embedder.embed_index(text_chunks, request_threads, embedder_stats) {
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

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_embeddings_from_fragments<R: io::Read + io::Seek>(
    // (docid, extractor_id) -> (Option<Value>)
    inputs_reader: grenad::Reader<R>,
    indexer: GrenadParameters,
    runtime: Arc<RuntimeEmbedder>,
    embedder_name: &str,
    possible_embedding_mistakes: &PossibleEmbeddingMistakes,
    embedder_stats: &EmbedderStats,
    unused_vectors_distribution: &UnusedVectorsDistribution,
    request_threads: &ThreadPoolNoAbort,
) -> Result<grenad::Reader<BufReader<File>>> {
    let doc_alloc = Bump::new();

    // (docid, extractor_id) -> (Option<Value>)
    let vector_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    if inputs_reader.is_empty() {
        return writer_into_reader(vector_writer);
    }

    let on_embed = WriteGrenadOnEmbed {
        waiting_responses: Default::default(),
        vector_writer,
        scratch: Default::default(),
        possible_embedding_mistakes,
    };

    let mut session = EmbedSession::new(
        &runtime.embedder,
        embedder_name,
        request_threads,
        &doc_alloc,
        embedder_stats,
        false,
        on_embed,
    );

    let mut cursor = inputs_reader.into_cursor()?;

    while let Some((mut key, value)) = cursor.move_on_next()? {
        let docid = key.read_u32::<BigEndian>().unwrap();
        let extractor_id = key.read_u8().unwrap();

        if value.is_empty() {
            // no value => removed fragment
            session.on_embed_mut().push_response(docid, extractor_id);
        } else {
            // unwrap: the grenad value was saved as a serde_json::Value
            let value: Value = serde_json::from_slice(value).unwrap();
            session.request_embedding(
                Metadata { docid, external_docid: "", extractor_id },
                value,
                unused_vectors_distribution,
            )?;
        }
    }

    // send last chunk
    let on_embed = session.drain(unused_vectors_distribution)?;
    on_embed.finish()
}

struct WriteGrenadOnEmbed<'a> {
    // list of (document_id, extractor_id) for which vectors should be removed.
    // these are written whenever a response arrives that has a larger (docid, extractor_id).
    waiting_responses: VecDeque<(DocumentId, u8)>,

    // grenad of (docid, extractor_id) -> (Option<Vector>)
    vector_writer: Writer<BufWriter<File>>,

    possible_embedding_mistakes: &'a PossibleEmbeddingMistakes,

    // scratch buffer used to write keys
    scratch: Vec<u8>,
}

impl WriteGrenadOnEmbed<'_> {
    pub fn push_response(&mut self, docid: DocumentId, extractor_id: u8) {
        self.waiting_responses.push_back((docid, extractor_id));
    }

    pub fn finish(mut self) -> Result<grenad::Reader<BufReader<File>>> {
        for (docid, extractor_id) in self.waiting_responses {
            self.scratch.clear();
            self.scratch.write_u32::<BigEndian>(docid).unwrap();
            self.scratch.write_u8(extractor_id).unwrap();
            self.vector_writer.insert(&self.scratch, []).unwrap();
        }
        writer_into_reader(self.vector_writer)
    }
}

impl<'doc> OnEmbed<'doc> for WriteGrenadOnEmbed<'_> {
    type ErrorMetadata = UnusedVectorsDistribution;
    fn process_embedding_response(
        &mut self,
        response: crate::vector::session::EmbeddingResponse<'doc>,
    ) {
        let (docid, extractor_id) = (response.metadata.docid, response.metadata.extractor_id);
        while let Some(waiting_response) = self.waiting_responses.pop_front() {
            if (docid, extractor_id) > waiting_response {
                self.scratch.clear();
                self.scratch.write_u32::<BigEndian>(docid).unwrap();
                self.scratch.write_u8(extractor_id).unwrap();
                self.vector_writer.insert(&self.scratch, []).unwrap();
            } else {
                self.waiting_responses.push_front(waiting_response);
                break;
            }
        }

        if let Some(embedding) = response.embedding {
            self.scratch.clear();
            self.scratch.write_u32::<BigEndian>(docid).unwrap();
            self.scratch.write_u8(extractor_id).unwrap();
            self.vector_writer.insert(&self.scratch, cast_slice(embedding.as_slice())).unwrap();
        }
    }

    fn process_embedding_error(
        &mut self,
        error: crate::vector::error::EmbedError,
        embedder_name: &'doc str,
        unused_vectors_distribution: &crate::vector::error::UnusedVectorsDistribution,
        _metadata: bumpalo::collections::Vec<'doc, crate::vector::session::Metadata<'doc>>,
    ) -> crate::Error {
        if let FaultSource::Bug = error.fault {
            crate::Error::InternalError(crate::InternalError::VectorEmbeddingError(error.into()))
        } else {
            let mut msg =
                format!(r"While embedding documents for embedder `{embedder_name}`: {error}");

            if let EmbedErrorKind::ManualEmbed(_) = &error.kind {
                msg += &format!("\n- Note: `{embedder_name}` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.{embedder_name}`.");
            }

            let mut hint_count = 0;

            for (vector_misspelling, count) in
                self.possible_embedding_mistakes.vector_mistakes().take(2)
            {
                msg += &format!("\n- Hint: try replacing `{vector_misspelling}` by `_vectors` in {count} document(s).");
                hint_count += 1;
            }

            for (embedder_misspelling, count) in self
                .possible_embedding_mistakes
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

            crate::Error::UserError(crate::UserError::DocumentEmbeddingError(msg))
        }
    }
}
