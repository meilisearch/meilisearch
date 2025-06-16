use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;
use serde_json::Value;

use super::{EmbedError, Embedder, Embedding};
use crate::vector::error::UnusedVectorsDistributionBump;
use crate::{DocumentId, Result, ThreadPoolNoAbort};
type ExtractorId = u16;

#[derive(Clone, Copy)]
pub struct Metadata<'doc> {
    pub docid: DocumentId,
    pub external_docid: &'doc str,
    pub extractor_id: ExtractorId,
}

pub struct EmbeddingResponse<'doc> {
    pub metadata: Metadata<'doc>,
    pub embedding: Embedding,
}

pub trait OnEmbed<'doc> {
    fn process_embedding_response(&mut self, response: EmbeddingResponse<'doc>);
    fn process_embedding_error(
        &mut self,
        error: EmbedError,
        embedder_name: &'doc str,
        unused_vectors_distribution: &UnusedVectorsDistributionBump,
        metadata: &[Metadata<'doc>],
    ) -> crate::Error;

    fn process_embeddings(&mut self, metadata: Metadata<'doc>, embeddings: Vec<Embedding>);
}

pub struct TextEmbedSession<'doc, C> {
    // requests
    texts: BVec<'doc, &'doc str>,
    metadata: BVec<'doc, Metadata<'doc>>,

    threads: &'doc ThreadPoolNoAbort,
    embedder: &'doc Embedder,

    embedder_name: &'doc str,

    on_embed: C,
}

impl<'doc, C: OnEmbed<'doc>> TextEmbedSession<'doc, C> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        embedder: &'doc Embedder,
        embedder_name: &'doc str,
        threads: &'doc ThreadPoolNoAbort,
        doc_alloc: &'doc Bump,
        on_embed: C,
    ) -> Self {
        let capacity = embedder.prompt_count_in_chunk_hint() * embedder.chunk_count_hint();
        let texts = BVec::with_capacity_in(capacity, doc_alloc);
        let ids = BVec::with_capacity_in(capacity, doc_alloc);
        Self { texts, metadata: ids, embedder, threads, embedder_name, on_embed }
    }

    pub fn request_embedding(
        &mut self,
        metadata: Metadata<'doc>,
        rendered: &'doc str,
        unused_vectors_distribution: &UnusedVectorsDistributionBump,
    ) -> Result<()> {
        if self.texts.len() < self.texts.capacity() {
            self.texts.push(rendered);
            self.metadata.push(metadata);
            return Ok(());
        }

        self.embed_chunks(unused_vectors_distribution)
    }

    pub fn drain(
        mut self,
        unused_vectors_distribution: &UnusedVectorsDistributionBump,
    ) -> Result<()> {
        self.embed_chunks(unused_vectors_distribution)
    }

    #[allow(clippy::too_many_arguments)]
    fn embed_chunks(
        &mut self,
        unused_vectors_distribution: &UnusedVectorsDistributionBump,
    ) -> Result<()> {
        let res = match self.embedder.embed_index_ref(self.texts.as_slice(), self.threads) {
            Ok(embeddings) => {
                for (metadata, embedding) in self.metadata.iter().copied().zip(embeddings) {
                    self.on_embed
                        .process_embedding_response(EmbeddingResponse { metadata, embedding });
                }
                Ok(())
            }
            Err(error) => {
                return Err(self.on_embed.process_embedding_error(
                    error,
                    self.embedder_name,
                    unused_vectors_distribution,
                    &self.metadata,
                ))
            }
        };
        self.texts.clear();
        self.metadata.clear();
        res
    }

    pub(crate) fn embedder_name(&self) -> &'doc str {
        self.embedder_name
    }

    pub(crate) fn on_embed(&self) -> &C {
        &self.on_embed
    }

    pub(crate) fn on_embed_mut(&mut self) -> &mut C {
        &mut self.on_embed
    }
}
