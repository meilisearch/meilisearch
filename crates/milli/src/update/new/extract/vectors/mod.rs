use std::cell::RefCell;

use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;
use hashbrown::{DefaultHashBuilder, HashMap};

use super::cache::DelAddRoaringBitmap;
use crate::error::FaultSource;
use crate::prompt::Prompt;
use crate::update::new::channel::EmbeddingSender;
use crate::update::new::indexer::document_changes::{DocumentChangeContext, Extractor};
use crate::update::new::thread_local::MostlySend;
use crate::update::new::vector_document::VectorDocument;
use crate::update::new::DocumentChange;
use crate::vector::error::{
    EmbedErrorKind, PossibleEmbeddingMistakes, UnusedVectorsDistributionBump,
};
use crate::vector::request::{Metadata, OnEmbed, TextEmbedSession};
use crate::vector::{Embedder, Embedding, EmbeddingConfigs};
use crate::{DocumentId, FieldDistribution, InternalError, Result, ThreadPoolNoAbort, UserError};

pub struct EmbeddingExtractor<'a, 'b> {
    embedders: &'a EmbeddingConfigs,
    sender: EmbeddingSender<'a, 'b>,
    possible_embedding_mistakes: PossibleEmbeddingMistakes,
    threads: &'a ThreadPoolNoAbort,
}

impl<'a, 'b> EmbeddingExtractor<'a, 'b> {
    pub fn new(
        embedders: &'a EmbeddingConfigs,
        sender: EmbeddingSender<'a, 'b>,
        field_distribution: &'a FieldDistribution,
        threads: &'a ThreadPoolNoAbort,
    ) -> Self {
        let possible_embedding_mistakes = PossibleEmbeddingMistakes::new(field_distribution);
        Self { embedders, sender, threads, possible_embedding_mistakes }
    }
}

pub struct EmbeddingExtractorData<'extractor>(
    pub HashMap<String, DelAddRoaringBitmap, DefaultHashBuilder, &'extractor Bump>,
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
        context: &'doc DocumentChangeContext<Self::Data>,
    ) -> crate::Result<()> {
        let embedders = self.embedders.inner_as_ref();
        let mut unused_vectors_distribution =
            UnusedVectorsDistributionBump::new_in(&context.doc_alloc);

        let mut all_chunks = BVec::with_capacity_in(embedders.len(), &context.doc_alloc);
        for (embedder_name, (embedder, prompt, _is_quantized)) in embedders {
            let embedder_id =
                context.index.embedder_category_id.get(&context.rtxn, embedder_name)?.ok_or_else(
                    || InternalError::DatabaseMissingEntry {
                        db_name: "embedder_category_id",
                        key: None,
                    },
                )?;
            all_chunks.push(Chunks::new(
                embedder,
                embedder_id,
                embedder_name,
                prompt,
                context.data,
                &self.possible_embedding_mistakes,
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
                    // we still need to accomodate deletion from user_provided
                    for chunks in &mut all_chunks {
                        // regenerate: true means we delete from user_provided
                        chunks.set_regenerate(deletion.docid(), true);
                    }
                }
                DocumentChange::Update(update) => {
                    let old_vectors = update.current_vectors(
                        &context.rtxn,
                        context.index,
                        context.db_fields_ids_map,
                        &context.doc_alloc,
                    )?;
                    let new_vectors =
                        update.only_changed_vectors(&context.doc_alloc, self.embedders)?;

                    if let Some(new_vectors) = &new_vectors {
                        unused_vectors_distribution.append(new_vectors)?;
                    }

                    for chunks in &mut all_chunks {
                        let embedder_name = chunks.embedder_name();
                        let prompt = chunks.prompt();

                        let old_vectors = old_vectors.vectors_for_key(embedder_name)?.unwrap();

                        // case where we have a `_vectors` field in the updated document
                        if let Some(new_vectors) = new_vectors.as_ref().and_then(|new_vectors| {
                            new_vectors.vectors_for_key(embedder_name).transpose()
                        }) {
                            let new_vectors = new_vectors?;
                            if old_vectors.regenerate != new_vectors.regenerate {
                                chunks.set_regenerate(update.docid(), new_vectors.regenerate);
                            }
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
                                )?;
                            // regenerate if the new `_vectors` fields is set to.
                            } else if new_vectors.regenerate {
                                let new_rendered = prompt.render_document(
                                    update.external_document_id(),
                                    update.merged(
                                        &context.rtxn,
                                        context.index,
                                        context.db_fields_ids_map,
                                    )?,
                                    context.new_fields_ids_map,
                                    &context.doc_alloc,
                                )?;
                                let must_regenerate = if !old_vectors.regenerate {
                                    // we just enabled `regenerate`
                                    true
                                } else {
                                    let old_rendered = prompt.render_document(
                                        update.external_document_id(),
                                        update.current(
                                            &context.rtxn,
                                            context.index,
                                            context.db_fields_ids_map,
                                        )?,
                                        context.new_fields_ids_map,
                                        &context.doc_alloc,
                                    );

                                    if let Ok(old_rendered) = old_rendered {
                                        // must regenerate if the rendered changed
                                        new_rendered != old_rendered
                                    } else {
                                        // cannot check previous rendered, better regenerate
                                        true
                                    }
                                };

                                if must_regenerate {
                                    chunks.set_autogenerated(
                                        update.docid(),
                                        update.external_document_id(),
                                        new_rendered,
                                        &unused_vectors_distribution,
                                    )?;
                                }
                            }
                        // no `_vectors` field, so only regenerate if the document is already set to in the DB.
                        } else if old_vectors.regenerate {
                            let new_rendered = prompt.render_document(
                                update.external_document_id(),
                                update.merged(
                                    &context.rtxn,
                                    context.index,
                                    context.db_fields_ids_map,
                                )?,
                                context.new_fields_ids_map,
                                &context.doc_alloc,
                            )?;

                            let must_regenerate = {
                                let old_rendered = prompt.render_document(
                                    update.external_document_id(),
                                    update.current(
                                        &context.rtxn,
                                        context.index,
                                        context.db_fields_ids_map,
                                    )?,
                                    context.new_fields_ids_map,
                                    &context.doc_alloc,
                                );
                                if let Ok(old_rendered) = old_rendered {
                                    // regenerate if the rendered version changed
                                    new_rendered != old_rendered
                                } else {
                                    // if we cannot render the previous version of the documents, let's regenerate
                                    true
                                }
                            };

                            if must_regenerate {
                                chunks.set_autogenerated(
                                    update.docid(),
                                    update.external_document_id(),
                                    new_rendered,
                                    &unused_vectors_distribution,
                                )?;
                            }
                        }
                    }
                }
                DocumentChange::Insertion(insertion) => {
                    let new_vectors =
                        insertion.inserted_vectors(&context.doc_alloc, self.embedders)?;
                    if let Some(new_vectors) = &new_vectors {
                        unused_vectors_distribution.append(new_vectors)?;
                    }

                    for chunks in &mut all_chunks {
                        let embedder_name = chunks.embedder_name();
                        let prompt = chunks.prompt();
                        // if no inserted vectors, then regenerate: true + no embeddings => autogenerate
                        if let Some(new_vectors) = new_vectors.as_ref().and_then(|new_vectors| {
                            new_vectors.vectors_for_key(embedder_name).transpose()
                        }) {
                            let new_vectors = new_vectors?;
                            chunks.set_regenerate(insertion.docid(), new_vectors.regenerate);
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
                                )?;
                            } else if new_vectors.regenerate {
                                let rendered = prompt.render_document(
                                    insertion.external_document_id(),
                                    insertion.inserted(),
                                    context.new_fields_ids_map,
                                    &context.doc_alloc,
                                )?;
                                chunks.set_autogenerated(
                                    insertion.docid(),
                                    insertion.external_document_id(),
                                    rendered,
                                    &unused_vectors_distribution,
                                )?;
                            }
                        } else {
                            let rendered = prompt.render_document(
                                insertion.external_document_id(),
                                insertion.inserted(),
                                context.new_fields_ids_map,
                                &context.doc_alloc,
                            )?;
                            chunks.set_autogenerated(
                                insertion.docid(),
                                insertion.external_document_id(),
                                rendered,
                                &unused_vectors_distribution,
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

pub struct OnEmbeddingDocumentUpdates<'doc, 'b> {
    embedder_id: u8,
    sender: EmbeddingSender<'doc, 'b>,
    possible_embedding_mistakes: &'doc PossibleEmbeddingMistakes,
}

impl<'doc> OnEmbed<'doc> for OnEmbeddingDocumentUpdates<'doc, '_> {
    fn process_embedding_response(
        &mut self,
        response: crate::vector::request::EmbeddingResponse<'doc>,
    ) {
        self.sender
            .set_vector(response.metadata.docid, self.embedder_id, response.embedding)
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
                let Some(first) = metadata.first() else { todo!() };
                format!(
                    r"While embedding documents for embedder `{embedder_name}`: no vectors provided for document `{}`{}\n- Note: `{embedder_name}` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.{embedder_name}`.",
                    first.external_docid,
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
    prompt: &'a Prompt,
    user_provided: &'a RefCell<EmbeddingExtractorData<'extractor>>,
    session: TextEmbedSession<'a, OnEmbeddingDocumentUpdates<'a, 'b>>,
}

impl<'a, 'b, 'extractor> Chunks<'a, 'b, 'extractor> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        embedder: &'a Embedder,
        embedder_id: u8,
        embedder_name: &'a str,
        prompt: &'a Prompt,
        user_provided: &'a RefCell<EmbeddingExtractorData<'extractor>>,
        possible_embedding_mistakes: &'a PossibleEmbeddingMistakes,
        threads: &'a ThreadPoolNoAbort,
        sender: EmbeddingSender<'a, 'b>,
        doc_alloc: &'a Bump,
    ) -> Self {
        let dimensions = embedder.dimensions();

        Self {
            dimensions,
            prompt,
            user_provided,
            session: TextEmbedSession::new(
                embedder,
                embedder_name,
                threads,
                doc_alloc,
                OnEmbeddingDocumentUpdates { embedder_id, sender, possible_embedding_mistakes },
            ),
        }
    }

    pub fn set_autogenerated(
        &mut self,
        docid: DocumentId,
        external_docid: &'a str,
        rendered: &'a str,
        unused_vectors_distribution: &UnusedVectorsDistributionBump,
    ) -> Result<()> {
        self.session.request_embedding(
            Metadata { docid, external_docid, extractor_id: 1 },
            rendered,
            unused_vectors_distribution,
        )
    }

    pub fn drain(self, unused_vectors_distribution: &UnusedVectorsDistributionBump) -> Result<()> {
        self.session.drain(unused_vectors_distribution)
    }

    pub fn prompt(&self) -> &'a Prompt {
        self.prompt
    }

    pub fn embedder_name(&self) -> &'a str {
        self.session.embedder_name()
    }

    pub fn set_regenerate(&self, docid: DocumentId, regenerate: bool) {
        let mut user_provided = self.user_provided.borrow_mut();
        let user_provided = user_provided.0.entry_ref(self.embedder_name()).or_default();
        if regenerate {
            // regenerate == !user_provided
            user_provided.insert_del_u32(docid);
        } else {
            user_provided.insert_add_u32(docid);
        }
    }

    pub fn set_vectors(
        &mut self,
        external_docid: &'a str,
        docid: DocumentId,
        embeddings: Vec<Embedding>,
    ) -> Result<()> {
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
        self.session
            .on_embed_mut()
            .process_embeddings(Metadata { docid, external_docid, extractor_id: 0 }, embeddings);
        Ok(())
    }
}
