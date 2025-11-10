use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;
use serde_json::Value;

use super::error::EmbedError;
use super::{Embedder, Embedding};
use crate::progress::EmbedderStats;
use crate::{DocumentId, Result, ThreadPoolNoAbort};
type ExtractorId = u8;

#[derive(Clone, Copy)]
pub struct Metadata<'doc> {
    pub docid: DocumentId,
    pub external_docid: &'doc str,
    pub extractor_id: ExtractorId,
}

pub struct EmbeddingResponse<'doc> {
    pub metadata: Metadata<'doc>,
    pub embedding: Option<Embedding>,
}

pub trait OnEmbed<'doc> {
    type ErrorMetadata;

    fn process_embedding_response(&mut self, response: EmbeddingResponse<'doc>);
    fn process_embedding_error(
        &mut self,
        error: EmbedError,
        embedder_name: &'doc str,
        unused_vectors_distribution: &Self::ErrorMetadata,
        metadata: BVec<'doc, Metadata<'doc>>,
    ) -> crate::Error;
}

pub struct EmbedSession<'doc, C, I> {
    // requests
    inputs: BVec<'doc, I>,
    metadata: BVec<'doc, Metadata<'doc>>,

    threads: &'doc ThreadPoolNoAbort,
    embedder: &'doc Embedder,

    embedder_name: &'doc str,

    embedder_stats: &'doc EmbedderStats,
    ignore_embedding_failures: bool,

    on_embed: C,
}

pub trait Input: Sized {
    fn embed_ref(
        inputs: &[Self],
        embedder: &Embedder,
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> std::result::Result<Vec<Embedding>, EmbedError>;
}

impl Input for &'_ str {
    fn embed_ref(
        inputs: &[Self],
        embedder: &Embedder,
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> std::result::Result<Vec<Embedding>, EmbedError> {
        embedder.embed_index_ref(inputs, threads, embedder_stats)
    }
}

impl Input for Value {
    fn embed_ref(
        inputs: &[Value],
        embedder: &Embedder,
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> std::result::Result<Vec<Embedding>, EmbedError> {
        embedder.embed_index_ref_fragments(inputs, threads, embedder_stats)
    }
}

impl<'doc, C: OnEmbed<'doc>, I: Input> EmbedSession<'doc, C, I> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        embedder: &'doc Embedder,
        embedder_name: &'doc str,
        threads: &'doc ThreadPoolNoAbort,
        doc_alloc: &'doc Bump,
        embedder_stats: &'doc EmbedderStats,
        ignore_embedding_failures: bool,
        on_embed: C,
    ) -> Self {
        let capacity = embedder.prompt_count_in_chunk_hint() * embedder.chunk_count_hint();
        let texts = BVec::with_capacity_in(capacity, doc_alloc);
        let ids = BVec::with_capacity_in(capacity, doc_alloc);
        Self {
            inputs: texts,
            metadata: ids,
            embedder,
            threads,
            embedder_name,
            embedder_stats,
            ignore_embedding_failures,
            on_embed,
        }
    }

    pub fn request_embedding(
        &mut self,
        metadata: Metadata<'doc>,
        rendered: I,
        unused_vectors_distribution: &C::ErrorMetadata,
    ) -> Result<()> {
        if self.inputs.len() < self.inputs.capacity() {
            self.inputs.push(rendered);
            self.metadata.push(metadata);
            return Ok(());
        }

        self.embed_chunks(unused_vectors_distribution)
    }

    pub fn drain(mut self, unused_vectors_distribution: &C::ErrorMetadata) -> Result<C> {
        self.embed_chunks(unused_vectors_distribution)?;
        Ok(self.on_embed)
    }

    #[allow(clippy::too_many_arguments)]
    fn embed_chunks(&mut self, unused_vectors_distribution: &C::ErrorMetadata) -> Result<()> {
        if self.inputs.is_empty() {
            return Ok(());
        }
        let res = match I::embed_ref(
            self.inputs.as_slice(),
            self.embedder,
            self.threads,
            self.embedder_stats,
        ) {
            Ok(embeddings) => {
                for (metadata, embedding) in self.metadata.iter().copied().zip(embeddings) {
                    self.on_embed.process_embedding_response(EmbeddingResponse {
                        metadata,
                        embedding: Some(embedding),
                    });
                }
                Ok(())
            }
            Err(error) => {
                // send metadata to the error processing.
                let doc_alloc = self.metadata.bump();
                let metadata = std::mem::replace(
                    &mut self.metadata,
                    BVec::with_capacity_in(self.inputs.capacity(), doc_alloc),
                );
                Err(self.on_embed.process_embedding_error(
                    error,
                    self.embedder_name,
                    unused_vectors_distribution,
                    metadata,
                ))
            }
        };
        self.inputs.clear();
        self.metadata.clear();
        if self.ignore_embedding_failures {
            if let Err(err) = res {
                tracing::warn!(
                    %err,
                    "ignored error embedding batch of documents due to failure policy"
                );
            }
            Ok(())
        } else {
            res
        }
    }

    pub(crate) fn embedder_name(&self) -> &'doc str {
        self.embedder_name
    }

    pub(crate) fn doc_alloc(&self) -> &'doc Bump {
        self.inputs.bump()
    }

    pub(crate) fn on_embed_mut(&mut self) -> &mut C {
        &mut self.on_embed
    }
}
