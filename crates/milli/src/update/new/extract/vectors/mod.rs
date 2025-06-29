use std::cell::RefCell;
use std::collections::BTreeMap;
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
use crate::vector::db::{EmbedderInfo, EmbeddingStatus, EmbeddingStatusDelta};
use crate::vector::error::{
    EmbedErrorKind, PossibleEmbeddingMistakes, UnusedVectorsDistributionBump,
};
use crate::vector::extractor::{
    DocumentTemplateExtractor, Extractor as VectorExtractor, RequestFragmentExtractor,
};
use crate::vector::session::{EmbedSession, Input, Metadata, OnEmbed};
use crate::vector::settings::{EmbedderAction, ReindexAction};
use crate::vector::{Embedding, RuntimeEmbedder, RuntimeEmbedders, RuntimeFragment};
use crate::{DocumentId, FieldDistribution, InternalError, Result, ThreadPoolNoAbort, UserError};

pub struct EmbeddingExtractor<'a, 'b> {
    embedders: &'a RuntimeEmbedders,
    sender: EmbeddingSender<'a, 'b>,
    possible_embedding_mistakes: PossibleEmbeddingMistakes,
    embedder_stats: &'a EmbedderStats,
    threads: &'a ThreadPoolNoAbort,
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
        Self { embedders, sender, threads, possible_embedding_mistakes, embedder_stats }
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

pub struct SettingsChangeEmbeddingExtractor<'a, 'b> {
    embedders: &'a EmbeddingConfigs,
    old_embedders: &'a EmbeddingConfigs,
    embedder_actions: &'a BTreeMap<String, EmbedderAction>,
    embedder_category_id: &'a std::collections::HashMap<String, u8>,
    embedder_stats: &'a EmbedderStats,
    sender: EmbeddingSender<'a, 'b>,
    possible_embedding_mistakes: PossibleEmbeddingMistakes,
    threads: &'a ThreadPoolNoAbort,
}

impl<'a, 'b> SettingsChangeEmbeddingExtractor<'a, 'b> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        embedders: &'a EmbeddingConfigs,
        old_embedders: &'a EmbeddingConfigs,
        embedder_actions: &'a BTreeMap<String, EmbedderAction>,
        embedder_category_id: &'a std::collections::HashMap<String, u8>,
        embedder_stats: &'a EmbedderStats,
        sender: EmbeddingSender<'a, 'b>,
        field_distribution: &'a FieldDistribution,
        threads: &'a ThreadPoolNoAbort,
    ) -> Self {
        let possible_embedding_mistakes = PossibleEmbeddingMistakes::new(field_distribution);
        Self {
            embedders,
            old_embedders,
            embedder_actions,
            embedder_category_id,
            embedder_stats,
            sender,
            threads,
            possible_embedding_mistakes,
        }
    }
}

impl<'extractor> SettingsChangeExtractor<'extractor> for SettingsChangeEmbeddingExtractor<'_, '_> {
    type Data = RefCell<EmbeddingExtractorData<'extractor>>;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> crate::Result<Self::Data> {
        Ok(RefCell::new(EmbeddingExtractorData(HashMap::new_in(extractor_alloc))))
    }

    fn process<'doc>(
        &'doc self,
        documents: impl Iterator<Item = crate::Result<DocumentIdentifiers<'doc>>>,
        context: &'doc DocumentContext<Self::Data>,
    ) -> crate::Result<()> {
        let embedders = self.embedders.inner_as_ref();
        let old_embedders = self.old_embedders.inner_as_ref();
        let unused_vectors_distribution = UnusedVectorsDistributionBump::new_in(&context.doc_alloc);

        let mut all_chunks = BVec::with_capacity_in(embedders.len(), &context.doc_alloc);
        for (embedder_name, (embedder, prompt, _is_quantized)) in embedders {
            // if the embedder is not in the embedder_actions, we don't need to reindex.
            if let Some((embedder_id, reindex_action)) =
                self.embedder_actions
                    .get(embedder_name)
                    // keep only the reindex actions
                    .and_then(EmbedderAction::reindex)
                    // map the reindex action to the embedder_id
                    .map(|reindex| {
                        let embedder_id = self.embedder_category_id.get(embedder_name).expect(
                            "An embedder_category_id must exist for all reindexed embedders",
                        );
                        (*embedder_id, reindex)
                    })
            {
                all_chunks.push((
                    Chunks::new(
                        embedder,
                        embedder_id,
                        embedder_name,
                        prompt,
                        context.data,
                        &self.possible_embedding_mistakes,
                        self.embedder_stats,
                        self.threads,
                        self.sender,
                        &context.doc_alloc,
                    ),
                    reindex_action,
                ))
            }
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

                // if the vectors for this document have been already provided, we don't need to reindex.
                let (is_new_embedder, must_regenerate) =
                    current_vectors.as_ref().map_or((true, true), |vectors| {
                        (!vectors.has_configured_embedder, vectors.regenerate)
                    });

                match reindex_action {
                    ReindexAction::RegeneratePrompts => {
                        if !must_regenerate {
                            continue;
                        }
                        // we need to regenerate the prompts for the document

                        // Get the old prompt and render the document with it
                        let Some((_, old_prompt, _)) = old_embedders.get(embedder_name) else {
                            unreachable!("ReindexAction::RegeneratePrompts implies that the embedder {embedder_name} is in the old_embedders")
                        };
                        let old_rendered = old_prompt.render_document(
                            document.external_document_id(),
                            document.current(
                                &context.rtxn,
                                context.index,
                                context.db_fields_ids_map,
                            )?,
                            context.new_fields_ids_map,
                            &context.doc_alloc,
                        )?;

                        // Get the new prompt and render the document with it
                        let new_prompt = chunks.prompt();
                        let new_rendered = new_prompt.render_document(
                            document.external_document_id(),
                            document.current(
                                &context.rtxn,
                                context.index,
                                context.db_fields_ids_map,
                            )?,
                            context.new_fields_ids_map,
                            &context.doc_alloc,
                        )?;

                        // Compare the rendered documents
                        // if they are different, regenerate the vectors
                        if new_rendered != old_rendered {
                            chunks.set_autogenerated(
                                document.docid(),
                                document.external_document_id(),
                                new_rendered,
                                &unused_vectors_distribution,
                            )?;
                        }
                    }
                    ReindexAction::FullReindex => {
                        let prompt = chunks.prompt();
                        // if no inserted vectors, then regenerate: true + no embeddings => autogenerate
                        if let Some(embeddings) = current_vectors
                            .and_then(|vectors| vectors.embeddings)
                            // insert the embeddings only for new embedders
                            .filter(|_| is_new_embedder)
                        {
                            chunks.set_regenerate(document.docid(), must_regenerate);
                            chunks.set_vectors(
                                document.external_document_id(),
                                document.docid(),
                                embeddings.into_vec(&context.doc_alloc, embedder_name).map_err(
                                    |error| UserError::InvalidVectorsEmbedderConf {
                                        document_id: document.external_document_id().to_string(),
                                        error: error.to_string(),
                                    },
                                )?,
                            )?;
                        } else if must_regenerate {
                            let rendered = prompt.render_document(
                                document.external_document_id(),
                                document.current(
                                    &context.rtxn,
                                    context.index,
                                    context.db_fields_ids_map,
                                )?,
                                context.new_fields_ids_map,
                                &context.doc_alloc,
                            )?;
                            chunks.set_autogenerated(
                                document.docid(),
                                document.external_document_id(),
                                rendered,
                                &unused_vectors_distribution,
                            )?;
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

    fn process_embeddings(&mut self, metadata: Metadata<'doc>, embeddings: Vec<Embedding>) {
        self.sender.set_vectors(metadata.docid, self.embedder_id, embeddings).unwrap();
    }

    fn process_embedding_error(
        &mut self,
        error: crate::vector::hf::EmbedError,
        embedder_name: &'doc str,
        unused_vectors_distribution: &UnusedVectorsDistributionBump,
        metadata: &[Metadata<'doc>],
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
    ) -> Self {
        let embedder = &runtime.embedder;
        let dimensions = embedder.dimensions();

        let fragments = runtime.fragments.as_slice();
        let kind = if fragments.is_empty() {
            ChunkType::DocumentTemplate {
                document_template: &runtime.document_template,
                session: EmbedSession::new(
                    &runtime.embedder,
                    embedder_name,
                    threads,
                    doc_alloc,
                    embedder_stats,
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
        let extracted = match &mut self.kind {
            ChunkType::DocumentTemplate { document_template, session } => {
                let doc_alloc = session.doc_alloc();
                let ex = DocumentTemplateExtractor::new(
                    document_template,
                    doc_alloc,
                    new_fields_ids_map,
                );

                if old_is_user_provided {
                    session.on_embed_mut().clear_vectors(docid);
                }

                update_autogenerated(
                    docid,
                    external_docid,
                    [ex],
                    old_document,
                    new_document,
                    &external_docid,
                    old_must_regenerate,
                    session,
                    unused_vectors_distribution,
                )?
            }
            ChunkType::Fragments { fragments, session } => {
                let doc_alloc = session.doc_alloc();
                let extractors = fragments.iter().map(|fragment| {
                    RequestFragmentExtractor::new(fragment, doc_alloc).ignore_errors()
                });

                if old_is_user_provided {
                    session.on_embed_mut().clear_vectors(docid);
                }

                update_autogenerated(
                    docid,
                    external_docid,
                    extractors,
                    old_document,
                    new_document,
                    &(),
                    old_must_regenerate,
                    session,
                    unused_vectors_distribution,
                )?
            }
        };

        self.set_status(
            docid,
            old_is_user_provided,
            old_must_regenerate,
            old_is_user_provided && !extracted,
            new_must_regenerate,
        );

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_autogenerated<D: Document<'a> + Debug>(
        &mut self,
        docid: DocumentId,
        external_docid: &'a str,
        new_document: D,
        new_fields_ids_map: &'a RefCell<crate::GlobalFieldsIdsMap>,
        unused_vectors_distribution: &UnusedVectorsDistributionBump<'a>,
        new_must_regenerate: bool,
    ) -> Result<()> {
        let (default_is_user_provided, default_must_regenerate) = (false, true);
        self.set_status(
            docid,
            default_is_user_provided,
            default_must_regenerate,
            false,
            new_must_regenerate,
        );

        match &mut self.kind {
            ChunkType::DocumentTemplate { document_template, session } => {
                let doc_alloc = session.doc_alloc();
                let ex = DocumentTemplateExtractor::new(
                    document_template,
                    doc_alloc,
                    new_fields_ids_map,
                );

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
            ChunkType::DocumentTemplate { document_template: _, session } => {
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
            ChunkType::DocumentTemplate { document_template: _, session } => {
                session.embedder_name()
            }
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
            ChunkType::DocumentTemplate { document_template: _, session } => {
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
    session: &mut EmbedSession<'a, OnEmbeddingDocumentUpdates<'a, 'b>, E::Input>,
    unused_vectors_distribution: &UnusedVectorsDistributionBump<'a>,
) -> Result<bool>
where
    OD: Document<'doc> + Debug,
    ND: Document<'doc> + Debug,
    E: VectorExtractor<'a>,
    E::Input: Input,
    crate::Error: From<E::Error>,
{
    let mut extracted = false;
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
            extracted = true;
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

    Ok(extracted)
}

fn insert_autogenerated<'a, 'b, E, D: Document<'a> + Debug>(
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
