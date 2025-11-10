use std::cell::RefCell;
use std::fmt::Debug;

use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;
use hashbrown::{DefaultHashBuilder, HashMap};

use crate::error::FaultSource;
use crate::progress::EmbedderStats;
use crate::prompt::Prompt;
use crate::update::new::channel::EmbeddingSender;
use crate::update::new::document::{Document, DocumentContext, DocumentIdentifiers};
use crate::update::new::indexer::document_changes::Extractor;
use crate::update::new::indexer::settings_changes::SettingsChangeExtractor;
use crate::update::new::thread_local::MostlySend;
use crate::update::new::vector_document::VectorDocument;
use crate::update::new::DocumentChange;
use crate::update::settings::SettingsDelta;
use crate::vector::db::{EmbedderInfo, EmbeddingStatus, EmbeddingStatusDelta};
use crate::vector::error::{
    EmbedErrorKind, PossibleEmbeddingMistakes, UnusedVectorsDistributionBump,
};
use crate::vector::extractor::{
    DocumentTemplateExtractor, Extractor as VectorExtractor, ExtractorDiff,
    RequestFragmentExtractor,
};
use crate::vector::session::{EmbedSession, Input, Metadata, OnEmbed};
use crate::vector::settings::ReindexAction;
use crate::vector::{Embedding, RuntimeEmbedder, RuntimeEmbedders, RuntimeFragment};
use crate::{DocumentId, FieldDistribution, InternalError, Result, ThreadPoolNoAbort, UserError};

pub struct EmbeddingExtractor<'a, 'b> {
    embedders: &'a RuntimeEmbedders,
    sender: EmbeddingSender<'a, 'b>,
    possible_embedding_mistakes: PossibleEmbeddingMistakes,
    embedder_stats: &'a EmbedderStats,
    threads: &'a ThreadPoolNoAbort,
    failure_modes: EmbedderFailureModes,
}

impl<'a, 'b> EmbeddingExtractor<'a, 'b> {
    pub fn new(
        embedders: &'a RuntimeEmbedders,
        sender: EmbeddingSender<'a, 'b>,
        field_distribution: &'a FieldDistribution,
        embedder_stats: &'a EmbedderStats,
        threads: &'a ThreadPoolNoAbort,
    ) -> Self {
        let possible_embedding_mistakes = PossibleEmbeddingMistakes::new(field_distribution);
        let failure_modes = EmbedderFailureModes::from_env();
        Self {
            embedders,
            sender,
            threads,
            possible_embedding_mistakes,
            embedder_stats,
            failure_modes,
        }
    }
}

pub struct EmbeddingExtractorData<'extractor>(
    pub HashMap<String, EmbeddingStatusDelta, DefaultHashBuilder, &'extractor Bump>,
);

unsafe impl MostlySend for EmbeddingExtractorData<'_> {}

impl<'extractor> Extractor<'extractor> for EmbeddingExtractor<'_, '_> {
    type Data = RefCell<EmbeddingExtractorData<'extractor>>;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> crate::Result<Self::Data> {
        Ok(RefCell::new(EmbeddingExtractorData(HashMap::new_in(extractor_alloc))))
    }

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = crate::Result<DocumentChange<'doc>>>,
        context: &'doc DocumentContext<Self::Data>,
    ) -> crate::Result<()> {
        let embedders = self.embedders.inner_as_ref();
        let mut unused_vectors_distribution =
            UnusedVectorsDistributionBump::new_in(&context.doc_alloc);

        let mut all_chunks = BVec::with_capacity_in(embedders.len(), &context.doc_alloc);
        let embedder_db = context.index.embedding_configs();
        for (embedder_name, runtime) in embedders {
            let embedder_info = embedder_db
                .embedder_info(&context.rtxn, embedder_name)?
                .ok_or_else(|| InternalError::DatabaseMissingEntry {
                    db_name: "embedder_category_id",
                    key: None,
                })?;
            all_chunks.push(Chunks::new(
                runtime,
                embedder_info,
                embedder_name,
                context.data,
                &self.possible_embedding_mistakes,
                self.embedder_stats,
                self.threads,
                self.sender,
                &context.doc_alloc,
                self.failure_modes,
            ))
        }

        for change in changes {
            let change = change?;
            match change {
                DocumentChange::Deletion(deletion) => {
                    // vector deletion is handled by document sender,
                    // we still need to accomodate deletion from embedding_status
                    for chunks in &mut all_chunks {
                        let (is_user_provided, must_regenerate) =
                            chunks.is_user_provided_must_regenerate(deletion.docid());
                        chunks.clear_status(deletion.docid(), is_user_provided, must_regenerate);
                    }
                }
                DocumentChange::Update(update) => {
                    let new_vectors =
                        update.only_changed_vectors(&context.doc_alloc, self.embedders)?;

                    if let Some(new_vectors) = &new_vectors {
                        unused_vectors_distribution.append(new_vectors)?;
                    }

                    for chunks in &mut all_chunks {
                        let (old_is_user_provided, old_must_regenerate) =
                            chunks.is_user_provided_must_regenerate(update.docid());

                        let embedder_name = chunks.embedder_name();

                        // case where we have a `_vectors` field in the updated document
                        if let Some(new_vectors) = new_vectors.as_ref().and_then(|new_vectors| {
                            new_vectors.vectors_for_key(embedder_name).transpose()
                        }) {
                            let new_vectors = new_vectors?;
                            // do we have set embeddings?
                            if let Some(embeddings) = new_vectors.embeddings {
                                chunks.set_vectors(
                                    update.external_document_id(),
                                    update.docid(),
                                    embeddings
                                        .into_vec(&context.doc_alloc, embedder_name)
                                        .map_err(|error| UserError::InvalidVectorsEmbedderConf {
                                            document_id: update.external_document_id().to_string(),
                                            error: error.to_string(),
                                        })?,
                                    old_is_user_provided,
                                    old_must_regenerate,
                                    new_vectors.regenerate,
                                )?;
                            // regenerate if the new `_vectors` fields is set to.
                            } else if new_vectors.regenerate {
                                let new_document = update.merged(
                                    &context.rtxn,
                                    context.index,
                                    context.db_fields_ids_map,
                                )?;
                                let old_document = update.current(
                                    &context.rtxn,
                                    context.index,
                                    context.db_fields_ids_map,
                                )?;
                                chunks.update_autogenerated(
                                    update.docid(),
                                    update.external_document_id(),
                                    old_document,
                                    new_document,
                                    context.new_fields_ids_map,
                                    &unused_vectors_distribution,
                                    old_is_user_provided,
                                    old_must_regenerate,
                                    true,
                                )?;
                            }
                        // no `_vectors` field, so only regenerate if the document is already set to in the DB.
                        } else if old_must_regenerate {
                            let new_document = update.merged(
                                &context.rtxn,
                                context.index,
                                context.db_fields_ids_map,
                            )?;
                            let old_document = update.current(
                                &context.rtxn,
                                context.index,
                                context.db_fields_ids_map,
                            )?;
                            chunks.update_autogenerated(
                                update.docid(),
                                update.external_document_id(),
                                old_document,
                                new_document,
                                context.new_fields_ids_map,
                                &unused_vectors_distribution,
                                old_is_user_provided,
                                old_must_regenerate,
                                true,
                            )?;
                        }
                    }
                }
                DocumentChange::Insertion(insertion) => {
                    let (default_is_user_provided, default_must_regenerate) = (false, true);
                    let new_vectors =
                        insertion.inserted_vectors(&context.doc_alloc, self.embedders)?;
                    if let Some(new_vectors) = &new_vectors {
                        unused_vectors_distribution.append(new_vectors)?;
                    }

                    for chunks in &mut all_chunks {
                        let embedder_name = chunks.embedder_name();
                        // if no inserted vectors, then regenerate: true + no embeddings => autogenerate
                        if let Some(new_vectors) = new_vectors.as_ref().and_then(|new_vectors| {
                            new_vectors.vectors_for_key(embedder_name).transpose()
                        }) {
                            let new_vectors = new_vectors?;
                            if let Some(embeddings) = new_vectors.embeddings {
                                chunks.set_vectors(
                                    insertion.external_document_id(),
                                    insertion.docid(),
                                    embeddings
                                        .into_vec(&context.doc_alloc, embedder_name)
                                        .map_err(|error| UserError::InvalidVectorsEmbedderConf {
                                            document_id: insertion
                                                .external_document_id()
                                                .to_string(),
                                            error: error.to_string(),
                                        })?,
                                    default_is_user_provided,
                                    default_must_regenerate,
                                    new_vectors.regenerate,
                                )?;
                            } else if new_vectors.regenerate {
                                chunks.insert_autogenerated(
                                    insertion.docid(),
                                    insertion.external_document_id(),
                                    insertion.inserted(),
                                    context.new_fields_ids_map,
                                    &unused_vectors_distribution,
                                    true,
                                )?;
                            } else {
                                chunks.set_status(
                                    insertion.docid(),
                                    default_is_user_provided,
                                    default_must_regenerate,
                                    false,
                                    false,
                                );
                            }
                        } else {
                            chunks.insert_autogenerated(
                                insertion.docid(),
                                insertion.external_document_id(),
                                insertion.inserted(),
                                context.new_fields_ids_map,
                                &unused_vectors_distribution,
                                true,
                            )?;
                        }
                    }
                }
            }
        }

        for chunk in all_chunks {
            chunk.drain(&unused_vectors_distribution)?;
        }
        Ok(())
    }
}

pub struct SettingsChangeEmbeddingExtractor<'a, 'b, SD> {
    settings_delta: &'a SD,
    embedder_stats: &'a EmbedderStats,
    sender: EmbeddingSender<'a, 'b>,
    possible_embedding_mistakes: PossibleEmbeddingMistakes,
    threads: &'a ThreadPoolNoAbort,
    failure_modes: EmbedderFailureModes,
}

impl<'a, 'b, SD: SettingsDelta> SettingsChangeEmbeddingExtractor<'a, 'b, SD> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        settings_delta: &'a SD,
        embedder_stats: &'a EmbedderStats,
        sender: EmbeddingSender<'a, 'b>,
        field_distribution: &'a FieldDistribution,
        threads: &'a ThreadPoolNoAbort,
    ) -> Self {
        let possible_embedding_mistakes = PossibleEmbeddingMistakes::new(field_distribution);
        let failure_modes = EmbedderFailureModes::from_env();

        Self {
            settings_delta,
            embedder_stats,
            sender,
            threads,
            possible_embedding_mistakes,
            failure_modes,
        }
    }
}

impl<'extractor, SD: SettingsDelta + Sync> SettingsChangeExtractor<'extractor>
    for SettingsChangeEmbeddingExtractor<'_, '_, SD>
{
    type Data = RefCell<EmbeddingExtractorData<'extractor>>;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> crate::Result<Self::Data> {
        Ok(RefCell::new(EmbeddingExtractorData(HashMap::new_in(extractor_alloc))))
    }

    fn process<'doc>(
        &'doc self,
        documents: impl Iterator<Item = crate::Result<DocumentIdentifiers<'doc>>>,
        context: &'doc DocumentContext<Self::Data>,
    ) -> crate::Result<()> {
        let embedders = self.settings_delta.new_embedders();
        let old_embedders = self.settings_delta.old_embedders();
        let unused_vectors_distribution = UnusedVectorsDistributionBump::new_in(&context.doc_alloc);

        let mut all_chunks = BVec::with_capacity_in(embedders.len(), &context.doc_alloc);
        let embedder_configs = context.index.embedding_configs();
        for (embedder_name, action) in self.settings_delta.embedder_actions().iter() {
            let Some(reindex_action) = action.reindex() else {
                continue;
            };
            let runtime = embedders
                .get(embedder_name)
                .expect("A runtime must exist for all reindexed embedder");
            let embedder_info = embedder_configs
                .embedder_info(&context.rtxn, embedder_name)?
                .unwrap_or_else(|| {
                    // new embedder
                    EmbedderInfo {
                        embedder_id: *self
                            .settings_delta
                            .new_embedder_category_id()
                            .get(embedder_name)
                            .expect(
                                "An embedder_category_id must exist for all reindexed embedders",
                            ),
                        embedding_status: EmbeddingStatus::new(),
                    }
                });
            all_chunks.push((
                Chunks::new(
                    runtime,
                    embedder_info,
                    embedder_name.as_str(),
                    context.data,
                    &self.possible_embedding_mistakes,
                    self.embedder_stats,
                    self.threads,
                    self.sender,
                    &context.doc_alloc,
                    self.failure_modes,
                ),
                reindex_action,
            ));
        }
        for document in documents {
            let document = document?;

            let current_vectors = document.current_vectors(
                &context.rtxn,
                context.index,
                context.db_fields_ids_map,
                &context.doc_alloc,
            )?;

            for (chunks, reindex_action) in &mut all_chunks {
                let embedder_name = chunks.embedder_name();
                let current_vectors = current_vectors.vectors_for_key(embedder_name)?;
                let (old_is_user_provided, _) =
                    chunks.is_user_provided_must_regenerate(document.docid());
                let old_has_fragments = old_embedders
                    .get(embedder_name)
                    .map(|embedder| !embedder.fragments().is_empty())
                    .unwrap_or_default();

                let new_has_fragments = chunks.has_fragments();

                let fragments_changed = old_has_fragments ^ new_has_fragments;

                // if the vectors for this document have been already provided, we don't need to reindex.
                let (is_new_embedder, must_regenerate) =
                    current_vectors.as_ref().map_or((true, true), |vectors| {
                        (!vectors.has_configured_embedder, vectors.regenerate)
                    });

                match reindex_action {
                    ReindexAction::RegeneratePrompts | ReindexAction::RegenerateFragments(_) => {
                        if !must_regenerate {
                            continue;
                        }
                        // we need to regenerate the prompts for the document
                        chunks.settings_change_autogenerated(
                            document.docid(),
                            document.external_document_id(),
                            document.current(
                                &context.rtxn,
                                context.index,
                                context.db_fields_ids_map,
                            )?,
                            self.settings_delta,
                            context.new_fields_ids_map,
                            &unused_vectors_distribution,
                            old_is_user_provided,
                            fragments_changed,
                        )?;
                    }
                    ReindexAction::FullReindex => {
                        // if no inserted vectors, then regenerate: true + no embeddings => autogenerate
                        if let Some(embeddings) = current_vectors
                            .and_then(|vectors| vectors.embeddings)
                            // insert the embeddings only for new embedders
                            .filter(|_| is_new_embedder)
                        {
                            chunks.set_vectors(
                                document.external_document_id(),
                                document.docid(),
                                embeddings.into_vec(&context.doc_alloc, embedder_name).map_err(
                                    |error| UserError::InvalidVectorsEmbedderConf {
                                        document_id: document.external_document_id().to_string(),
                                        error: error.to_string(),
                                    },
                                )?,
                                old_is_user_provided,
                                true,
                                must_regenerate,
                            )?;
                        } else if must_regenerate {
                            chunks.settings_change_autogenerated(
                                document.docid(),
                                document.external_document_id(),
                                document.current(
                                    &context.rtxn,
                                    context.index,
                                    context.db_fields_ids_map,
                                )?,
                                self.settings_delta,
                                context.new_fields_ids_map,
                                &unused_vectors_distribution,
                                old_is_user_provided,
                                true,
                            )?;
                        } else if is_new_embedder {
                            chunks.set_status(document.docid(), false, true, false, false);
                        }
                    }
                }
            }
        }

        for (chunk, _) in all_chunks {
            chunk.drain(&unused_vectors_distribution)?;
        }

        Ok(())
    }
}

pub struct OnEmbeddingDocumentUpdates<'doc, 'b> {
    embedder_id: u8,
    sender: EmbeddingSender<'doc, 'b>,
    possible_embedding_mistakes: &'doc PossibleEmbeddingMistakes,
}

impl OnEmbeddingDocumentUpdates<'_, '_> {
    fn clear_vectors(&self, docid: DocumentId) {
        self.sender.set_vectors(docid, self.embedder_id, vec![]).unwrap();
    }

    fn process_embeddings(&mut self, metadata: Metadata<'_>, embeddings: Vec<Embedding>) {
        self.sender.set_vectors(metadata.docid, self.embedder_id, embeddings).unwrap();
    }
}

impl<'doc> OnEmbed<'doc> for OnEmbeddingDocumentUpdates<'doc, '_> {
    type ErrorMetadata = UnusedVectorsDistributionBump<'doc>;
    fn process_embedding_response(
        &mut self,
        response: crate::vector::session::EmbeddingResponse<'doc>,
    ) {
        self.sender
            .set_vector(
                response.metadata.docid,
                self.embedder_id,
                response.metadata.extractor_id,
                response.embedding,
            )
            .unwrap();
    }
    fn process_embedding_error(
        &mut self,
        error: crate::vector::error::EmbedError,
        embedder_name: &'doc str,
        unused_vectors_distribution: &UnusedVectorsDistributionBump,
        metadata: BVec<'doc, Metadata<'doc>>,
    ) -> crate::Error {
        if let FaultSource::Bug = error.fault {
            crate::Error::InternalError(crate::InternalError::VectorEmbeddingError(error.into()))
        } else {
            let mut msg = if let EmbedErrorKind::ManualEmbed(_) = &error.kind {
                format!(
                    r"While embedding documents for embedder `{embedder_name}`: no vectors provided for document `{}`{}
- Note: `{embedder_name}` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.{embedder_name}`.",
                    if let Some(first) = metadata.first() { first.external_docid } else { "???" },
                    if metadata.len() > 1 {
                        format!(" and at least {} other document(s)", metadata.len() - 1)
                    } else {
                        "".to_string()
                    }
                )
            } else {
                format!(r"While embedding documents for embedder `{embedder_name}`: {error}")
            };

            let mut hint_count = 0;

            for (vector_misspelling, count) in
                self.possible_embedding_mistakes.vector_mistakes().take(2)
            {
                msg += &format!("\n- Hint: try replacing `{vector_misspelling}` by `_vectors` in {count} document(s).");
                hint_count += 1;
            }

            for (embedder_misspelling, count) in self
                .possible_embedding_mistakes
                .embedder_mistakes_bump(embedder_name, unused_vectors_distribution)
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

struct Chunks<'a, 'b, 'extractor> {
    dimensions: usize,
    status_delta: &'a RefCell<EmbeddingExtractorData<'extractor>>,
    status: EmbeddingStatus,
    kind: ChunkType<'a, 'b>,
}

enum ChunkType<'a, 'b> {
    DocumentTemplate {
        document_template: &'a Prompt,
        ignore_document_template_failures: bool,
        session: EmbedSession<'a, OnEmbeddingDocumentUpdates<'a, 'b>, &'a str>,
    },
    Fragments {
        fragments: &'a [RuntimeFragment],
        session: EmbedSession<'a, OnEmbeddingDocumentUpdates<'a, 'b>, serde_json::Value>,
    },
}

impl<'a, 'b, 'extractor> Chunks<'a, 'b, 'extractor> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: &'a RuntimeEmbedder,
        embedder_info: EmbedderInfo,
        embedder_name: &'a str,
        status_delta: &'a RefCell<EmbeddingExtractorData<'extractor>>,
        possible_embedding_mistakes: &'a PossibleEmbeddingMistakes,
        embedder_stats: &'a EmbedderStats,
        threads: &'a ThreadPoolNoAbort,
        sender: EmbeddingSender<'a, 'b>,
        doc_alloc: &'a Bump,
        failure_modes: EmbedderFailureModes,
    ) -> Self {
        let embedder = &runtime.embedder;
        let dimensions = embedder.dimensions();

        let fragments = runtime.fragments();
        let kind = if fragments.is_empty() {
            ChunkType::DocumentTemplate {
                document_template: &runtime.document_template,
                ignore_document_template_failures: failure_modes.ignore_document_template_failures,
                session: EmbedSession::new(
                    &runtime.embedder,
                    embedder_name,
                    threads,
                    doc_alloc,
                    embedder_stats,
                    failure_modes.ignore_embedder_failures,
                    OnEmbeddingDocumentUpdates {
                        embedder_id: embedder_info.embedder_id,
                        sender,
                        possible_embedding_mistakes,
                    },
                ),
            }
        } else {
            ChunkType::Fragments {
                fragments,
                session: EmbedSession::new(
                    &runtime.embedder,
                    embedder_name,
                    threads,
                    doc_alloc,
                    embedder_stats,
                    failure_modes.ignore_embedder_failures,
                    OnEmbeddingDocumentUpdates {
                        embedder_id: embedder_info.embedder_id,
                        sender,
                        possible_embedding_mistakes,
                    },
                ),
            }
        };

        Self { dimensions, status: embedder_info.embedding_status, status_delta, kind }
    }

    pub fn is_user_provided_must_regenerate(&self, docid: DocumentId) -> (bool, bool) {
        self.status.is_user_provided_must_regenerate(docid)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn settings_change_autogenerated<'doc, D: Document<'doc> + Debug, SD: SettingsDelta>(
        &mut self,
        docid: DocumentId,
        external_docid: &'a str,
        document: D,
        settings_delta: &SD,
        fields_ids_map: &'a RefCell<crate::GlobalFieldsIdsMap>,
        unused_vectors_distribution: &UnusedVectorsDistributionBump<'a>,
        old_is_user_provided: bool,
        full_reindex: bool,
    ) -> Result<()>
    where
        'a: 'doc,
    {
        self.set_status(docid, old_is_user_provided, true, false, true);

        match &mut self.kind {
            ChunkType::Fragments { fragments, session } => {
                let doc_alloc = session.doc_alloc();
                let reindex_all_fragments =
                                // when the vectors were user-provided, Meilisearch cannot know if they come from a particular fragment,
                                // and so Meilisearch needs to clear all embeddings in that case.
                                // Fortunately, as dump export fragment vector with `regenerate` set to `false`,
                                // this case should be rare and opt-in.
                                old_is_user_provided ||
                                // full-reindex case
                                full_reindex;

                if reindex_all_fragments {
                    session.on_embed_mut().clear_vectors(docid);
                    let extractors = fragments.iter().map(|fragment| {
                        RequestFragmentExtractor::new(fragment, doc_alloc).ignore_errors()
                    });
                    insert_autogenerated(
                        docid,
                        external_docid,
                        extractors,
                        document,
                        &(),
                        session,
                        unused_vectors_distribution,
                    )?;
                    return Ok(());
                }

                settings_delta.try_for_each_fragment_diff(
                    session.embedder_name(),
                    |fragment_diff| -> Result<()> {
                        let extractor = RequestFragmentExtractor::new(fragment_diff.new, doc_alloc)
                            .ignore_errors();
                        let old = if full_reindex {
                            None
                        } else {
                            fragment_diff.old.map(|old| {
                                RequestFragmentExtractor::new(old, doc_alloc).ignore_errors()
                            })
                        };
                        let metadata = Metadata {
                            docid,
                            external_docid,
                            extractor_id: extractor.extractor_id(),
                        };

                        match extractor.diff_settings(&document, &(), old.as_ref())? {
                            ExtractorDiff::Removed => {
                                OnEmbed::process_embedding_response(
                                    session.on_embed_mut(),
                                    crate::vector::session::EmbeddingResponse {
                                        metadata,
                                        embedding: None,
                                    },
                                );
                            }
                            ExtractorDiff::Added(input) | ExtractorDiff::Updated(input) => {
                                session.request_embedding(
                                    metadata,
                                    input,
                                    unused_vectors_distribution,
                                )?;
                            }
                            ExtractorDiff::Unchanged => { /* nothing to do */ }
                        }

                        Result::Ok(())
                    },
                )?;
            }
            ChunkType::DocumentTemplate {
                document_template,
                ignore_document_template_failures,
                session,
            } => {
                let doc_alloc = session.doc_alloc();

                let old_embedder = settings_delta.old_embedders().get(session.embedder_name());
                let old_document_template = if full_reindex {
                    None
                } else {
                    old_embedder.as_ref().map(|old_embedder| &old_embedder.document_template)
                };

                let extractor =
                    DocumentTemplateExtractor::new(document_template, doc_alloc, fields_ids_map);
                let old_extractor = old_document_template.map(|old_document_template| {
                    DocumentTemplateExtractor::new(old_document_template, doc_alloc, fields_ids_map)
                });
                let metadata =
                    Metadata { docid, external_docid, extractor_id: extractor.extractor_id() };

                let extractor_diff = if *ignore_document_template_failures {
                    let extractor = extractor.ignore_errors();
                    let old_extractor = old_extractor.map(DocumentTemplateExtractor::ignore_errors);
                    extractor.diff_settings(document, &external_docid, old_extractor.as_ref())?
                } else {
                    extractor.diff_settings(document, &external_docid, old_extractor.as_ref())?
                };

                match extractor_diff {
                    ExtractorDiff::Removed => {
                        if old_is_user_provided || full_reindex {
                            session.on_embed_mut().clear_vectors(docid);
                        }
                        OnEmbed::process_embedding_response(
                            session.on_embed_mut(),
                            crate::vector::session::EmbeddingResponse { metadata, embedding: None },
                        );
                    }
                    ExtractorDiff::Added(input) | ExtractorDiff::Updated(input) => {
                        if old_is_user_provided || full_reindex {
                            session.on_embed_mut().clear_vectors(docid);
                        }
                        session.request_embedding(metadata, input, unused_vectors_distribution)?;
                    }
                    ExtractorDiff::Unchanged => { /* do nothing */ }
                }
                self.set_status(docid, old_is_user_provided, true, false, true);
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_autogenerated<'doc, OD: Document<'doc> + Debug, ND: Document<'doc> + Debug>(
        &mut self,
        docid: DocumentId,
        external_docid: &'a str,
        old_document: OD,
        new_document: ND,
        new_fields_ids_map: &'a RefCell<crate::GlobalFieldsIdsMap>,
        unused_vectors_distribution: &UnusedVectorsDistributionBump<'a>,
        old_is_user_provided: bool,
        old_must_regenerate: bool,
        new_must_regenerate: bool,
    ) -> Result<()>
    where
        'a: 'doc,
    {
        self.set_status(
            docid,
            old_is_user_provided,
            old_must_regenerate,
            false,
            new_must_regenerate,
        );
        match &mut self.kind {
            ChunkType::DocumentTemplate {
                document_template,
                ignore_document_template_failures,
                session,
            } => {
                let doc_alloc = session.doc_alloc();
                let ex = DocumentTemplateExtractor::new(
                    document_template,
                    doc_alloc,
                    new_fields_ids_map,
                );

                if *ignore_document_template_failures {
                    update_autogenerated(
                        docid,
                        external_docid,
                        [ex.ignore_errors()],
                        old_document,
                        new_document,
                        &external_docid,
                        old_must_regenerate,
                        old_is_user_provided,
                        session,
                        unused_vectors_distribution,
                    )
                } else {
                    update_autogenerated(
                        docid,
                        external_docid,
                        [ex],
                        old_document,
                        new_document,
                        &external_docid,
                        old_must_regenerate,
                        old_is_user_provided,
                        session,
                        unused_vectors_distribution,
                    )
                }?
            }
            ChunkType::Fragments { fragments, session } => {
                let doc_alloc = session.doc_alloc();
                let extractors = fragments.iter().map(|fragment| {
                    RequestFragmentExtractor::new(fragment, doc_alloc).ignore_errors()
                });

                if old_is_user_provided {
                    // when the document was `userProvided`, Meilisearch cannot know whose fragments a particular
                    // vector was referring to.
                    // So as a result Meilisearch will regenerate all fragments on this case.
                    // Fortunately, since dumps for fragments set regenerate to false, this case should be rare.
                    session.on_embed_mut().clear_vectors(docid);
                    insert_autogenerated(
                        docid,
                        external_docid,
                        extractors,
                        new_document,
                        &(),
                        session,
                        unused_vectors_distribution,
                    )?;
                    return Ok(());
                }

                update_autogenerated(
                    docid,
                    external_docid,
                    extractors,
                    old_document,
                    new_document,
                    &(),
                    old_must_regenerate,
                    false,
                    session,
                    unused_vectors_distribution,
                )?
            }
        };

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_autogenerated<'doc, D: Document<'doc> + Debug>(
        &mut self,
        docid: DocumentId,
        external_docid: &'a str,
        new_document: D,
        new_fields_ids_map: &'a RefCell<crate::GlobalFieldsIdsMap>,
        unused_vectors_distribution: &UnusedVectorsDistributionBump<'a>,
        new_must_regenerate: bool,
    ) -> Result<()>
    where
        'a: 'doc,
    {
        let (default_is_user_provided, default_must_regenerate) = (false, true);
        self.set_status(
            docid,
            default_is_user_provided,
            default_must_regenerate,
            false,
            new_must_regenerate,
        );

        match &mut self.kind {
            ChunkType::DocumentTemplate {
                document_template,
                ignore_document_template_failures,
                session,
            } => {
                let doc_alloc = session.doc_alloc();
                let ex = DocumentTemplateExtractor::new(
                    document_template,
                    doc_alloc,
                    new_fields_ids_map,
                );
                if *ignore_document_template_failures {
                    insert_autogenerated(
                        docid,
                        external_docid,
                        [ex.ignore_errors()],
                        new_document,
                        &external_docid,
                        session,
                        unused_vectors_distribution,
                    )?;
                } else {
                    insert_autogenerated(
                        docid,
                        external_docid,
                        [ex],
                        new_document,
                        &external_docid,
                        session,
                        unused_vectors_distribution,
                    )?;
                }
            }
            ChunkType::Fragments { fragments, session } => {
                let doc_alloc = session.doc_alloc();
                let extractors = fragments.iter().map(|fragment| {
                    RequestFragmentExtractor::new(fragment, doc_alloc).ignore_errors()
                });

                insert_autogenerated(
                    docid,
                    external_docid,
                    extractors,
                    new_document,
                    &(),
                    session,
                    unused_vectors_distribution,
                )?;
            }
        }
        Ok(())
    }

    pub fn drain(self, unused_vectors_distribution: &UnusedVectorsDistributionBump) -> Result<()> {
        match self.kind {
            ChunkType::DocumentTemplate {
                document_template: _,
                ignore_document_template_failures: _,
                session,
            } => {
                session.drain(unused_vectors_distribution)?;
            }
            ChunkType::Fragments { fragments: _, session } => {
                session.drain(unused_vectors_distribution)?;
            }
        }
        Ok(())
    }

    pub fn embedder_name(&self) -> &'a str {
        match &self.kind {
            ChunkType::DocumentTemplate {
                document_template: _,
                ignore_document_template_failures: _,
                session,
            } => session.embedder_name(),
            ChunkType::Fragments { fragments: _, session } => session.embedder_name(),
        }
    }

    fn set_status(
        &self,
        docid: DocumentId,
        old_is_user_provided: bool,
        old_must_regenerate: bool,
        new_is_user_provided: bool,
        new_must_regenerate: bool,
    ) {
        if EmbeddingStatusDelta::needs_change(
            old_is_user_provided,
            old_must_regenerate,
            new_is_user_provided,
            new_must_regenerate,
        ) {
            let mut status_delta = self.status_delta.borrow_mut();
            let status_delta = status_delta.0.entry_ref(self.embedder_name()).or_default();
            status_delta.push_delta(
                docid,
                old_is_user_provided,
                old_must_regenerate,
                new_is_user_provided,
                new_must_regenerate,
            );
        }
    }

    pub fn clear_status(&self, docid: DocumentId, is_user_provided: bool, must_regenerate: bool) {
        // these value ensure both roaring are at 0.
        if EmbeddingStatusDelta::needs_clear(is_user_provided, must_regenerate) {
            let mut status_delta = self.status_delta.borrow_mut();
            let status_delta = status_delta.0.entry_ref(self.embedder_name()).or_default();
            status_delta.clear_docid(docid, is_user_provided, must_regenerate);
        }
    }

    pub fn set_vectors(
        &mut self,
        external_docid: &'a str,
        docid: DocumentId,
        embeddings: Vec<Embedding>,
        old_is_user_provided: bool,
        old_must_regenerate: bool,
        new_must_regenerate: bool,
    ) -> Result<()> {
        self.set_status(
            docid,
            old_is_user_provided,
            old_must_regenerate,
            true,
            new_must_regenerate,
        );
        for (embedding_index, embedding) in embeddings.iter().enumerate() {
            if embedding.len() != self.dimensions {
                return Err(UserError::InvalidIndexingVectorDimensions {
                    expected: self.dimensions,
                    found: embedding.len(),
                    embedder_name: self.embedder_name().to_string(),
                    document_id: external_docid.to_string(),
                    embedding_index,
                }
                .into());
            }
        }
        match &mut self.kind {
            ChunkType::DocumentTemplate {
                document_template: _,
                ignore_document_template_failures: _,
                session,
            } => {
                session.on_embed_mut().process_embeddings(
                    Metadata { docid, external_docid, extractor_id: 0 },
                    embeddings,
                );
            }
            ChunkType::Fragments { fragments: _, session } => {
                session.on_embed_mut().process_embeddings(
                    Metadata { docid, external_docid, extractor_id: 0 },
                    embeddings,
                );
            }
        }

        Ok(())
    }

    fn has_fragments(&self) -> bool {
        matches!(self.kind, ChunkType::Fragments { .. })
    }
}

#[allow(clippy::too_many_arguments)]
fn update_autogenerated<'doc, 'a: 'doc, 'b, E, OD, ND>(
    docid: DocumentId,
    external_docid: &'a str,
    extractors: impl IntoIterator<Item = E>,
    old_document: OD,
    new_document: ND,
    meta: &E::DocumentMetadata,
    old_must_regenerate: bool,
    mut must_clear_on_generation: bool,
    session: &mut EmbedSession<'a, OnEmbeddingDocumentUpdates<'a, 'b>, E::Input>,
    unused_vectors_distribution: &UnusedVectorsDistributionBump<'a>,
) -> Result<()>
where
    OD: Document<'doc> + Debug,
    ND: Document<'doc> + Debug,
    E: VectorExtractor<'a>,
    E::Input: Input,
    crate::Error: From<E::Error>,
{
    for extractor in extractors {
        let new_rendered = extractor.extract(&new_document, meta)?;
        let must_regenerate = if !old_must_regenerate {
            // we just enabled `regenerate`
            true
        } else {
            let old_rendered = extractor.extract(&old_document, meta);

            if let Ok(old_rendered) = old_rendered {
                // must regenerate if the rendered changed
                new_rendered != old_rendered
            } else {
                // cannot check previous rendered, better regenerate
                true
            }
        };

        if must_regenerate {
            if must_clear_on_generation {
                must_clear_on_generation = false;
                session.on_embed_mut().clear_vectors(docid);
            }

            let metadata =
                Metadata { docid, external_docid, extractor_id: extractor.extractor_id() };

            if let Some(new_rendered) = new_rendered {
                session.request_embedding(metadata, new_rendered, unused_vectors_distribution)?
            } else {
                // remove any existing embedding
                OnEmbed::process_embedding_response(
                    session.on_embed_mut(),
                    crate::vector::session::EmbeddingResponse { metadata, embedding: None },
                );
            }
        }
    }

    Ok(())
}

fn insert_autogenerated<'doc, 'a: 'doc, 'b, E, D: Document<'doc> + Debug>(
    docid: DocumentId,
    external_docid: &'a str,
    extractors: impl IntoIterator<Item = E>,
    new_document: D,
    meta: &E::DocumentMetadata,
    session: &mut EmbedSession<'a, OnEmbeddingDocumentUpdates<'a, 'b>, E::Input>,
    unused_vectors_distribution: &UnusedVectorsDistributionBump<'a>,
) -> Result<()>
where
    E: VectorExtractor<'a>,
    E::Input: Input,
    crate::Error: From<E::Error>,
{
    for extractor in extractors {
        let new_rendered = extractor.extract(&new_document, meta)?;

        if let Some(new_rendered) = new_rendered {
            session.request_embedding(
                Metadata { docid, external_docid, extractor_id: extractor.extractor_id() },
                new_rendered,
                unused_vectors_distribution,
            )?;
        }
    }

    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
struct EmbedderFailureModes {
    pub ignore_document_template_failures: bool,
    pub ignore_embedder_failures: bool,
}

impl EmbedderFailureModes {
    fn from_env() -> Self {
        match std::env::var("MEILI_EXPERIMENTAL_CONFIG_EMBEDDER_FAILURE_MODES") {
            Ok(failure_modes) => Self::parse_from_str(
                &failure_modes,
                "`MEILI_EXPERIMENTAL_CONFIG_EMBEDDER_FAILURE_MODES`",
            ),
            Err(std::env::VarError::NotPresent) => Self::default(),
            Err(std::env::VarError::NotUnicode(_)) => panic!(
                "`MEILI_EXPERIMENTAL_CONFIG_EMBEDDER_FAILURE_MODES` contains a non-unicode value"
            ),
        }
    }

    fn parse_from_str(failure_modes: &str, provenance: &'static str) -> Self {
        let Self { mut ignore_document_template_failures, mut ignore_embedder_failures } =
            Default::default();
        for segment in failure_modes.split(',') {
            let segment = segment.trim();
            match segment {
                "ignore_document_template_failures" => {
                    ignore_document_template_failures = true;
                }
                "ignore_embedder_failures" => ignore_embedder_failures = true,
                "" => continue,
                segment => panic!("Unrecognized segment value for {provenance}: {segment}"),
            }
        }
        Self { ignore_document_template_failures, ignore_embedder_failures }
    }
}
