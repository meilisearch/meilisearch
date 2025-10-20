use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;
use serde_json::Value;

use super::error::EmbedError;
use super::{Embedder, Embedding};
use crate::progress::EmbedderStats;
use crate::{DocumentId, ThreadPoolNoAbort};
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
            on_embed,
        }
    }

    pub fn request_embedding(
        &mut self,
        metadata: Metadata<'doc>,
        rendered: I,
        unused_vectors_distribution: &C::ErrorMetadata,
    ) {
        if self.inputs.len() < self.inputs.capacity() {
            self.inputs.push(rendered);
            self.metadata.push(metadata);
            return;
        }

        self.embed_chunks(unused_vectors_distribution)
    }

    pub fn drain(mut self, unused_vectors_distribution: &C::ErrorMetadata) -> C {
        self.embed_chunks(unused_vectors_distribution);
        self.on_embed
    }

    #[allow(clippy::too_many_arguments)]
    fn embed_chunks(&mut self, _unused_vectors_distribution: &C::ErrorMetadata) {
        if self.inputs.is_empty() {
            return;
        }
        match I::embed_ref(self.inputs.as_slice(), self.embedder, self.threads, self.embedder_stats)
        {
            Ok(embeddings) => {
                for (metadata, embedding) in self.metadata.iter().copied().zip(embeddings) {
                    self.on_embed.process_embedding_response(EmbeddingResponse {
                        metadata,
                        embedding: Some(embedding),
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "error embedding batch of documents, retrying one by one"
                );
                // retry with one call per input
                for (metadata, input) in self.metadata.iter().copied().zip(self.inputs.chunks(1)) {
                    match I::embed_ref(input, self.embedder, self.threads, self.embedder_stats) {
                        Ok(mut embeddings) => {
                            let Some(embedding) = embeddings.pop() else {
                                continue;
                            };
                            self.on_embed.process_embedding_response(EmbeddingResponse {
                                metadata,
                                embedding: Some(embedding),
                            })
                        }
                        Err(err) => {
                            tracing::warn!(
                                docid = metadata.external_docid,
                                %err,
                                "error embedding document"
                            );
                        }
                    }
                }
            }
        };
        self.inputs.clear();
        self.metadata.clear();
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
