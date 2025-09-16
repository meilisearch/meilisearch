pub mod composite;
pub mod hf;
pub mod manual;
pub mod ollama;
pub mod openai;
pub mod rest;

use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::Instant;

use composite::SubEmbedderOptions;

use crate::progress::EmbedderStats;
use crate::prompt::PromptData;
use crate::vector::error::{EmbedError, NewEmbedderError};
use crate::vector::{DistributionShift, Embedding};
use crate::ThreadPoolNoAbort;

/// An embedder can be used to transform text into embeddings.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Embedder {
    /// An embedder based on running local models, fetched from the Hugging Face Hub.
    HuggingFace(hf::Embedder),
    /// An embedder based on making embedding queries against the OpenAI API.
    OpenAi(openai::Embedder),
    /// An embedder based on the user providing the embeddings in the documents and queries.
    UserProvided(manual::Embedder),
    /// An embedder based on making embedding queries against an <https://ollama.com> embedding server.
    Ollama(ollama::Embedder),
    /// An embedder based on making embedding queries against a generic JSON/REST embedding server.
    Rest(rest::Embedder),
    /// An embedder composed of an embedder at search time and an embedder at indexing time.
    Composite(composite::Embedder),
}

/// Configuration for an embedder.
///
/// # Warning
///
/// This type is serialized in and deserialized from the DB, any modification should either go
/// through dumpless upgrade or be backward-compatible
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct EmbeddingConfig {
    /// Options of the embedder, specific to each kind of embedder
    pub embedder_options: EmbedderOptions,
    /// Document template
    pub prompt: PromptData,
    /// If this embedder is binary quantized
    pub quantized: Option<bool>,
    // TODO: add metrics and anything needed
}

impl EmbeddingConfig {
    pub fn quantized(&self) -> bool {
        self.quantized.unwrap_or_default()
    }
}

/// Options of an embedder, specific to each kind of embedder.
///
/// # Warning
///
/// This type is serialized in and deserialized from the DB, any modification should either go
/// through dumpless upgrade or be backward-compatible
#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum EmbedderOptions {
    HuggingFace(hf::EmbedderOptions),
    OpenAi(openai::EmbedderOptions),
    Ollama(ollama::EmbedderOptions),
    UserProvided(manual::EmbedderOptions),
    Rest(rest::EmbedderOptions),
    Composite(composite::EmbedderOptions),
}

impl EmbedderOptions {
    pub fn fragment(&self, name: &str) -> Option<&serde_json::Value> {
        match &self {
            EmbedderOptions::HuggingFace(_)
            | EmbedderOptions::OpenAi(_)
            | EmbedderOptions::Ollama(_)
            | EmbedderOptions::UserProvided(_) => None,
            EmbedderOptions::Rest(embedder_options) => {
                embedder_options.indexing_fragments.get(name)
            }
            EmbedderOptions::Composite(embedder_options) => {
                if let SubEmbedderOptions::Rest(embedder_options) = &embedder_options.index {
                    embedder_options.indexing_fragments.get(name)
                } else {
                    None
                }
            }
        }
    }

    pub fn has_fragments(&self) -> bool {
        match &self {
            EmbedderOptions::HuggingFace(_)
            | EmbedderOptions::OpenAi(_)
            | EmbedderOptions::Ollama(_)
            | EmbedderOptions::UserProvided(_) => false,
            EmbedderOptions::Rest(embedder_options) => {
                !embedder_options.indexing_fragments.is_empty()
            }
            EmbedderOptions::Composite(embedder_options) => {
                if let SubEmbedderOptions::Rest(embedder_options) = &embedder_options.index {
                    !embedder_options.indexing_fragments.is_empty()
                } else {
                    false
                }
            }
        }
    }
}

impl Default for EmbedderOptions {
    fn default() -> Self {
        Self::HuggingFace(Default::default())
    }
}

impl Embedder {
    /// Spawns a new embedder built from its options.
    pub fn new(
        options: EmbedderOptions,
        cache_cap: usize,
    ) -> std::result::Result<Self, NewEmbedderError> {
        Ok(match options {
            EmbedderOptions::HuggingFace(options) => {
                Self::HuggingFace(hf::Embedder::new(options, cache_cap)?)
            }
            EmbedderOptions::OpenAi(options) => {
                Self::OpenAi(openai::Embedder::new(options, cache_cap)?)
            }
            EmbedderOptions::Ollama(options) => {
                Self::Ollama(ollama::Embedder::new(options, cache_cap)?)
            }
            EmbedderOptions::UserProvided(options) => {
                Self::UserProvided(manual::Embedder::new(options))
            }
            EmbedderOptions::Rest(options) => Self::Rest(rest::Embedder::new(
                options,
                cache_cap,
                rest::ConfigurationSource::User,
            )?),
            EmbedderOptions::Composite(options) => {
                Self::Composite(composite::Embedder::new(options, cache_cap)?)
            }
        })
    }

    /// Embed in search context

    #[tracing::instrument(level = "debug", skip_all, target = "search")]
    pub fn embed_search(
        &self,
        query: SearchQuery<'_>,
        deadline: Option<Instant>,
    ) -> std::result::Result<Embedding, EmbedError> {
        match query {
            SearchQuery::Text(text) => self.embed_search_text(text, deadline),
            SearchQuery::Media { q, media } => self.embed_search_media(q, media, deadline),
        }
    }

    pub fn embed_search_text(
        &self,
        text: &str,
        deadline: Option<Instant>,
    ) -> std::result::Result<Embedding, EmbedError> {
        if let Some(cache) = self.cache() {
            if let Some(embedding) = cache.get(text) {
                tracing::trace!(text, "embedding found in cache");
                return Ok(embedding);
            }
        }
        let embedding = match self {
            Embedder::HuggingFace(embedder) => embedder.embed_one(text),
            Embedder::OpenAi(embedder) => embedder
                .embed(&[text], deadline, None)?
                .pop()
                .ok_or_else(EmbedError::missing_embedding),
            Embedder::Ollama(embedder) => embedder
                .embed(&[text], deadline, None)?
                .pop()
                .ok_or_else(EmbedError::missing_embedding),
            Embedder::UserProvided(embedder) => embedder.embed_one(text),
            Embedder::Rest(embedder) => embedder.embed_one(SearchQuery::Text(text), deadline, None),
            Embedder::Composite(embedder) => embedder.search.embed_one(text, deadline, None),
        }?;

        if let Some(cache) = self.cache() {
            cache.put(text.to_owned(), embedding.clone());
        }

        Ok(embedding)
    }

    pub fn embed_search_media(
        &self,
        q: Option<&str>,
        media: Option<&serde_json::Value>,
        deadline: Option<Instant>,
    ) -> std::result::Result<Embedding, EmbedError> {
        let Embedder::Rest(embedder) = self else {
            return Err(EmbedError::rest_media_not_a_rest());
        };
        embedder.embed_one(SearchQuery::Media { q, media }, deadline, None)
    }

    /// Embed multiple chunks of texts.
    ///
    /// Each chunk is composed of one or multiple texts.
    pub fn embed_index(
        &self,
        text_chunks: Vec<Vec<String>>,
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> std::result::Result<Vec<Vec<Embedding>>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed_index(text_chunks),
            Embedder::OpenAi(embedder) => {
                embedder.embed_index(text_chunks, threads, embedder_stats)
            }
            Embedder::Ollama(embedder) => {
                embedder.embed_index(text_chunks, threads, embedder_stats)
            }
            Embedder::UserProvided(embedder) => embedder.embed_index(text_chunks),
            Embedder::Rest(embedder) => embedder.embed_index(text_chunks, threads, embedder_stats),
            Embedder::Composite(embedder) => {
                embedder.index.embed_index(text_chunks, threads, embedder_stats)
            }
        }
    }

    /// Non-owning variant of [`Self::embed_index`].
    pub fn embed_index_ref(
        &self,
        texts: &[&str],
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> std::result::Result<Vec<Embedding>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed_index_ref(texts),
            Embedder::OpenAi(embedder) => embedder.embed_index_ref(texts, threads, embedder_stats),
            Embedder::Ollama(embedder) => embedder.embed_index_ref(texts, threads, embedder_stats),
            Embedder::UserProvided(embedder) => embedder.embed_index_ref(texts),
            Embedder::Rest(embedder) => embedder.embed_index_ref(texts, threads, embedder_stats),
            Embedder::Composite(embedder) => {
                embedder.index.embed_index_ref(texts, threads, embedder_stats)
            }
        }
    }

    pub fn embed_index_ref_fragments(
        &self,
        fragments: &[serde_json::Value],
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> std::result::Result<Vec<Embedding>, EmbedError> {
        if let Embedder::Rest(embedder) = self {
            embedder.embed_index_ref(fragments, threads, embedder_stats)
        } else {
            let Embedder::Composite(embedder) = self else {
                unimplemented!("embedding fragments is only available for rest embedders")
            };
            let crate::vector::embedder::composite::SubEmbedder::Rest(embedder) = &embedder.index
            else {
                unimplemented!("embedding fragments is only available for rest embedders")
            };

            embedder.embed_index_ref(fragments, threads, embedder_stats)
        }
    }

    /// Indicates the preferred number of chunks to pass to [`Self::embed_chunks`]
    pub fn chunk_count_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.chunk_count_hint(),
            Embedder::OpenAi(embedder) => embedder.chunk_count_hint(),
            Embedder::Ollama(embedder) => embedder.chunk_count_hint(),
            Embedder::UserProvided(_) => 100,
            Embedder::Rest(embedder) => embedder.chunk_count_hint(),
            Embedder::Composite(embedder) => embedder.index.chunk_count_hint(),
        }
    }

    /// Indicates the preferred number of texts in a single chunk passed to [`Self::embed`]
    pub fn prompt_count_in_chunk_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::OpenAi(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::Ollama(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::UserProvided(_) => 1,
            Embedder::Rest(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::Composite(embedder) => embedder.index.prompt_count_in_chunk_hint(),
        }
    }

    /// Indicates the dimensions of a single embedding produced by the embedder.
    pub fn dimensions(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.dimensions(),
            Embedder::OpenAi(embedder) => embedder.dimensions(),
            Embedder::Ollama(embedder) => embedder.dimensions(),
            Embedder::UserProvided(embedder) => embedder.dimensions(),
            Embedder::Rest(embedder) => embedder.dimensions(),
            Embedder::Composite(embedder) => embedder.dimensions(),
        }
    }

    /// An optional distribution used to apply an affine transformation to the similarity score of a document.
    pub fn distribution(&self) -> Option<DistributionShift> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.distribution(),
            Embedder::OpenAi(embedder) => embedder.distribution(),
            Embedder::Ollama(embedder) => embedder.distribution(),
            Embedder::UserProvided(embedder) => embedder.distribution(),
            Embedder::Rest(embedder) => embedder.distribution(),
            Embedder::Composite(embedder) => embedder.distribution(),
        }
    }

    pub fn uses_document_template(&self) -> bool {
        match self {
            Embedder::HuggingFace(_)
            | Embedder::OpenAi(_)
            | Embedder::Ollama(_)
            | Embedder::Rest(_) => true,
            Embedder::UserProvided(_) => false,
            Embedder::Composite(embedder) => embedder.index.uses_document_template(),
        }
    }

    fn cache(&self) -> Option<&EmbeddingCache> {
        match self {
            Embedder::HuggingFace(embedder) => Some(embedder.cache()),
            Embedder::OpenAi(embedder) => Some(embedder.cache()),
            Embedder::UserProvided(_) => None,
            Embedder::Ollama(embedder) => Some(embedder.cache()),
            Embedder::Rest(embedder) => Some(embedder.cache()),
            Embedder::Composite(embedder) => embedder.search.cache(),
        }
    }
}

#[derive(Clone, Copy)]
pub enum SearchQuery<'a> {
    Text(&'a str),
    Media { q: Option<&'a str>, media: Option<&'a serde_json::Value> },
}

#[derive(Debug)]
struct EmbeddingCache {
    data: Option<Mutex<lru::LruCache<String, Embedding>>>,
}

impl EmbeddingCache {
    const MAX_TEXT_LEN: usize = 2000;

    pub fn new(cap: usize) -> Self {
        let data = NonZeroUsize::new(cap).map(lru::LruCache::new).map(Mutex::new);
        Self { data }
    }

    /// Get the embedding corresponding to `text`, if any is present in the cache.
    pub fn get(&self, text: &str) -> Option<Embedding> {
        let data = self.data.as_ref()?;
        if text.len() > Self::MAX_TEXT_LEN {
            return None;
        }
        let mut cache = data.lock().unwrap();

        cache.get(text).cloned()
    }

    /// Puts a new embedding for the specified `text`
    pub fn put(&self, text: String, embedding: Embedding) {
        let Some(data) = self.data.as_ref() else {
            return;
        };
        if text.len() > Self::MAX_TEXT_LEN {
            return;
        }
        tracing::trace!(text, "embedding added to cache");

        let mut cache = data.lock().unwrap();

        cache.put(text, embedding);
    }
}
