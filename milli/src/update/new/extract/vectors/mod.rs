use crate::error::FaultSource;
use crate::prompt::Prompt;
use crate::update::new::channel::EmbeddingSender;
use crate::update::new::indexer::document_changes::{Extractor, FullySend};
use crate::update::new::vector_document::VectorDocument;
use crate::update::new::DocumentChange;
use crate::vector::error::EmbedErrorKind;
use crate::vector::Embedder;
use crate::{DocumentId, Result, ThreadPoolNoAbort, UserError};

pub struct EmbeddingExtractor<'a> {
    embedder: &'a Embedder,
    prompt: &'a Prompt,
    embedder_id: u8,
    embedder_name: &'a str,
    sender: &'a EmbeddingSender<'a>,
    threads: &'a ThreadPoolNoAbort,
}

impl<'a, 'extractor> Extractor<'extractor> for EmbeddingExtractor<'a> {
    type Data = FullySend<()>;

    fn init_data<'doc>(
        &'doc self,
        _extractor_alloc: raw_collections::alloc::RefBump<'extractor>,
    ) -> crate::Result<Self::Data> {
        Ok(FullySend(()))
    }

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = crate::Result<DocumentChange<'doc>>>,
        context: &'doc crate::update::new::indexer::document_changes::DocumentChangeContext<
            Self::Data,
        >,
    ) -> crate::Result<()> {
        let embedder_name: &str = self.embedder_name;
        let embedder: &Embedder = self.embedder;
        let prompt: &Prompt = self.prompt;

        let mut chunks = Chunks::new(
            embedder,
            self.embedder_id,
            embedder_name,
            self.threads,
            self.sender,
            &context.doc_alloc,
        );

        for change in changes {
            let change = change?;
            match change {
                DocumentChange::Deletion(deletion) => {
                    self.sender.delete(deletion.docid(), self.embedder_id).unwrap();
                }
                DocumentChange::Update(update) => {
                    /// FIXME: this will force the parsing/retrieval of VectorDocument once per embedder
                    /// consider doing all embedders at once?
                    let old_vectors = update.current_vectors(
                        &context.txn,
                        context.index,
                        context.db_fields_ids_map,
                        &context.doc_alloc,
                    )?;
                    let old_vectors = old_vectors.vectors_for_key(embedder_name)?.unwrap();
                    let new_vectors = update.updated_vectors(&context.doc_alloc)?;
                    if let Some(new_vectors) = new_vectors.as_ref().and_then(|new_vectors| {
                        new_vectors.vectors_for_key(embedder_name).transpose()
                    }) {
                        let new_vectors = new_vectors?;
                        match (old_vectors.regenerate, new_vectors.regenerate) {
                            (true, true) | (false, false) => todo!(),
                            _ => {
                                self.sender
                                    .set_user_provided(
                                        update.docid(),
                                        self.embedder_id,
                                        !new_vectors.regenerate,
                                    )
                                    .unwrap();
                            }
                        }
                        // do we have set embeddings?
                        if let Some(embeddings) = new_vectors.embeddings {
                            self.sender
                                .set_vectors(
                                    update.docid(),
                                    self.embedder_id,
                                    embeddings.into_vec().map_err(UserError::SerdeJson)?,
                                )
                                .unwrap();
                        } else if new_vectors.regenerate {
                            let new_rendered = prompt.render_document(
                                update.current(
                                    &context.txn,
                                    context.index,
                                    context.db_fields_ids_map,
                                )?,
                                context.new_fields_ids_map,
                                &context.doc_alloc,
                            )?;
                            let old_rendered = prompt.render_document(
                                update.new(
                                    &context.txn,
                                    context.index,
                                    context.db_fields_ids_map,
                                )?,
                                context.new_fields_ids_map,
                                &context.doc_alloc,
                            )?;
                            if new_rendered != old_rendered {
                                chunks.push(update.docid(), new_rendered)?;
                            }
                        }
                    } else if old_vectors.regenerate {
                        let old_rendered = prompt.render_document(
                            update.current(
                                &context.txn,
                                context.index,
                                context.db_fields_ids_map,
                            )?,
                            context.new_fields_ids_map,
                            &context.doc_alloc,
                        )?;
                        let new_rendered = prompt.render_document(
                            update.new(&context.txn, context.index, context.db_fields_ids_map)?,
                            context.new_fields_ids_map,
                            &context.doc_alloc,
                        )?;
                        if new_rendered != old_rendered {
                            chunks.push(update.docid(), new_rendered)?;
                        }
                    }
                }
                DocumentChange::Insertion(insertion) => {
                    // if no inserted vectors, then regenerate: true + no embeddings => autogenerate
                    let new_vectors = insertion.inserted_vectors(&context.doc_alloc)?;
                    if let Some(new_vectors) = new_vectors.as_ref().and_then(|new_vectors| {
                        new_vectors.vectors_for_key(embedder_name).transpose()
                    }) {
                        let new_vectors = new_vectors?;
                        self.sender
                            .set_user_provided(
                                insertion.docid(),
                                self.embedder_id,
                                !new_vectors.regenerate,
                            )
                            .unwrap();
                        if let Some(embeddings) = new_vectors.embeddings {
                            self.sender
                                .set_vectors(
                                    insertion.docid(),
                                    self.embedder_id,
                                    embeddings.into_vec().map_err(UserError::SerdeJson)?,
                                )
                                .unwrap();
                        } else if new_vectors.regenerate {
                            let rendered = prompt.render_document(
                                insertion.new(),
                                context.new_fields_ids_map,
                                &context.doc_alloc,
                            )?;
                            chunks.push(insertion.docid(), rendered)?;
                        }
                    } else {
                        let rendered = prompt.render_document(
                            insertion.new(),
                            context.new_fields_ids_map,
                            &context.doc_alloc,
                        )?;
                        chunks.push(insertion.docid(), rendered)?;
                    }
                }
            }
        }

        chunks.drain()
    }
}

use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;

// **Warning**: the destructor of this struct is not normally run, make sure that all its fields:
// 1. don't have side effects tied to they destructors
// 2. if allocated, are allocated inside of the bumpalo
//
// Currently this is the case as:
// 1. BVec are inside of the bumaplo
// 2. All other fields are either trivial (u8) or references.
struct Chunks<'a> {
    texts: BVec<'a, &'a str>,
    ids: BVec<'a, DocumentId>,

    embedder: &'a Embedder,
    embedder_id: u8,
    embedder_name: &'a str,
    threads: &'a ThreadPoolNoAbort,
    sender: &'a EmbeddingSender<'a>,
}

impl<'a> Chunks<'a> {
    pub fn new(
        embedder: &'a Embedder,
        embedder_id: u8,
        embedder_name: &'a str,
        threads: &'a ThreadPoolNoAbort,
        sender: &'a EmbeddingSender<'a>,
        doc_alloc: &'a Bump,
    ) -> Self {
        let capacity = embedder.prompt_count_in_chunk_hint() * embedder.chunk_count_hint();
        let texts = BVec::with_capacity_in(capacity, doc_alloc);
        let ids = BVec::with_capacity_in(capacity, doc_alloc);
        Self { texts, ids, embedder, threads, sender, embedder_id, embedder_name }
    }

    pub fn push(&mut self, docid: DocumentId, rendered: &'a str) -> Result<()> {
        if self.texts.len() < self.texts.capacity() {
            self.texts.push(rendered);
            self.ids.push(docid);
            return Ok(());
        }

        Self::embed_chunks(
            &mut self.texts,
            &mut self.ids,
            self.embedder,
            self.embedder_id,
            self.embedder_name,
            self.threads,
            self.sender,
        )
    }

    pub fn drain(mut self) -> Result<()> {
        let res = Self::embed_chunks(
            &mut self.texts,
            &mut self.ids,
            self.embedder,
            self.embedder_id,
            self.embedder_name,
            self.threads,
            self.sender,
        );
        // optimization: don't run bvec dtors as they only contain bumpalo allocated stuff
        std::mem::forget(self);
        res
    }

    pub fn embed_chunks(
        texts: &mut BVec<'a, &'a str>,
        ids: &mut BVec<'a, DocumentId>,
        embedder: &'a Embedder,
        embedder_id: u8,
        embedder_name: &str,
        threads: &'a ThreadPoolNoAbort,
        sender: &'a EmbeddingSender<'a>,
    ) -> Result<()> {
        let res = match embedder.embed_chunks_ref(texts.as_slice(), threads) {
            Ok(embeddings) => {
                for (docid, embedding) in ids.into_iter().zip(embeddings) {
                    sender.set_vector(*docid, embedder_id, embedding).unwrap();
                }
                Ok(())
            }
            Err(error) => {
                if let FaultSource::Bug = error.fault {
                    Err(crate::Error::InternalError(crate::InternalError::VectorEmbeddingError(
                        error.into(),
                    )))
                } else {
                    let mut msg = format!(
                        r"While embedding documents for embedder `{embedder_name}`: {error}"
                    );

                    if let EmbedErrorKind::ManualEmbed(_) = &error.kind {
                        msg += &format!("\n- Note: `{embedder_name}` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.{embedder_name}`.");
                    }

                    /// FIXME: reintroduce possible_embedding_mistakes and possible_embedding_mistakes
                    let mut hint_count = 0;

                    /*
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
                    */
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
        };
        texts.clear();
        ids.clear();
        res
    }
}
