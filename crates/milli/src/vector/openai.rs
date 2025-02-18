use std::fmt;
use std::time::Instant;

use ordered_float::OrderedFloat;
use rayon::iter::{IntoParallelIterator, ParallelIterator as _};
use rayon::slice::ParallelSlice as _;

use super::error::{EmbedError, NewEmbedderError};
use super::rest::{Embedder as RestEmbedder, EmbedderOptions as RestEmbedderOptions};
use super::{DistributionShift, REQUEST_PARALLELISM};
use crate::error::FaultSource;
use crate::vector::error::EmbedErrorKind;
use crate::vector::Embedding;
use crate::ThreadPoolNoAbort;

#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct EmbedderOptions {
    pub url: Option<String>,
    pub api_key: Option<String>,
    pub embedding_model: EmbeddingModel,
    pub dimensions: Option<usize>,
    pub distribution: Option<DistributionShift>,
}

impl EmbedderOptions {
    pub fn dimensions(&self) -> usize {
        if self.embedding_model.supports_overriding_dimensions() {
            self.dimensions.unwrap_or(self.embedding_model.default_dimensions())
        } else {
            self.embedding_model.default_dimensions()
        }
    }

    pub fn request(&self) -> serde_json::Value {
        let model = self.embedding_model.name();

        let mut request = serde_json::json!({
            "model": model,
            "input": [super::rest::REQUEST_PLACEHOLDER, super::rest::REPEAT_PLACEHOLDER]
        });

        if self.embedding_model.supports_overriding_dimensions() {
            if let Some(dimensions) = self.dimensions {
                request["dimensions"] = dimensions.into();
            }
        }

        request
    }

    pub fn distribution(&self) -> Option<DistributionShift> {
        self.distribution.or(self.embedding_model.distribution())
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    Hash,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    deserr::Deserr,
)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub enum EmbeddingModel {
    // # WARNING
    //
    // If ever adding a model, make sure to add it to the list of supported models below.
    #[serde(rename = "text-embedding-ada-002")]
    #[deserr(rename = "text-embedding-ada-002")]
    TextEmbeddingAda002,

    #[default]
    #[serde(rename = "text-embedding-3-small")]
    #[deserr(rename = "text-embedding-3-small")]
    TextEmbedding3Small,

    #[serde(rename = "text-embedding-3-large")]
    #[deserr(rename = "text-embedding-3-large")]
    TextEmbedding3Large,
}

impl EmbeddingModel {
    pub fn supported_models() -> &'static [&'static str] {
        &["text-embedding-ada-002", "text-embedding-3-small", "text-embedding-3-large"]
    }

    pub fn max_token(&self) -> usize {
        match self {
            EmbeddingModel::TextEmbeddingAda002 => 8191,
            EmbeddingModel::TextEmbedding3Large => 8191,
            EmbeddingModel::TextEmbedding3Small => 8191,
        }
    }

    pub fn default_dimensions(&self) -> usize {
        match self {
            EmbeddingModel::TextEmbeddingAda002 => 1536,
            EmbeddingModel::TextEmbedding3Large => 3072,
            EmbeddingModel::TextEmbedding3Small => 1536,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            EmbeddingModel::TextEmbeddingAda002 => "text-embedding-ada-002",
            EmbeddingModel::TextEmbedding3Large => "text-embedding-3-large",
            EmbeddingModel::TextEmbedding3Small => "text-embedding-3-small",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "text-embedding-ada-002" => Some(EmbeddingModel::TextEmbeddingAda002),
            "text-embedding-3-large" => Some(EmbeddingModel::TextEmbedding3Large),
            "text-embedding-3-small" => Some(EmbeddingModel::TextEmbedding3Small),
            _ => None,
        }
    }

    fn distribution(&self) -> Option<DistributionShift> {
        match self {
            EmbeddingModel::TextEmbeddingAda002 => Some(DistributionShift {
                current_mean: OrderedFloat(0.90),
                current_sigma: OrderedFloat(0.08),
            }),
            EmbeddingModel::TextEmbedding3Large => Some(DistributionShift {
                current_mean: OrderedFloat(0.70),
                current_sigma: OrderedFloat(0.1),
            }),
            EmbeddingModel::TextEmbedding3Small => Some(DistributionShift {
                current_mean: OrderedFloat(0.75),
                current_sigma: OrderedFloat(0.1),
            }),
        }
    }

    pub fn supports_overriding_dimensions(&self) -> bool {
        match self {
            EmbeddingModel::TextEmbeddingAda002 => false,
            EmbeddingModel::TextEmbedding3Large => true,
            EmbeddingModel::TextEmbedding3Small => true,
        }
    }
}

pub const OPENAI_EMBEDDINGS_URL: &str = "https://api.openai.com/v1/embeddings";

impl EmbedderOptions {
    pub fn with_default_model(api_key: Option<String>) -> Self {
        Self {
            api_key,
            embedding_model: Default::default(),
            dimensions: None,
            distribution: None,
            url: None,
        }
    }
}

fn infer_api_key() -> String {
    std::env::var("MEILI_OPENAI_API_KEY")
        .or_else(|_| std::env::var("OPENAI_API_KEY"))
        .unwrap_or_default()
}

pub struct Embedder {
    tokenizer: tiktoken_rs::CoreBPE,
    rest_embedder: RestEmbedder,
    options: EmbedderOptions,
}

impl Embedder {
    pub fn new(options: EmbedderOptions) -> Result<Self, NewEmbedderError> {
        let mut inferred_api_key = Default::default();
        let api_key = options.api_key.as_ref().unwrap_or_else(|| {
            inferred_api_key = infer_api_key();
            &inferred_api_key
        });

        let url = options.url.as_deref().unwrap_or(OPENAI_EMBEDDINGS_URL).to_owned();

        let rest_embedder = RestEmbedder::new(
            RestEmbedderOptions {
                api_key: (!api_key.is_empty()).then(|| api_key.clone()),
                distribution: None,
                dimensions: Some(options.dimensions()),
                url,
                request: options.request(),
                response: serde_json::json!({
                    "data": [{
                        "embedding": super::rest::RESPONSE_PLACEHOLDER
                    },
                    super::rest::REPEAT_PLACEHOLDER
                    ]
                }),
                headers: Default::default(),
            },
            super::rest::ConfigurationSource::OpenAi,
        )?;

        // looking at the code it is very unclear that this can actually fail.
        let tokenizer = tiktoken_rs::cl100k_base().unwrap();

        Ok(Self { options, rest_embedder, tokenizer })
    }

    pub fn embed<S: AsRef<str> + serde::Serialize>(
        &self,
        texts: &[S],
        deadline: Option<Instant>,
    ) -> Result<Vec<Embedding>, EmbedError> {
        match self.rest_embedder.embed_ref(texts, deadline) {
            Ok(embeddings) => Ok(embeddings),
            Err(EmbedError { kind: EmbedErrorKind::RestBadRequest(error, _), fault: _ }) => {
                tracing::warn!(error=?error, "OpenAI: received `BAD_REQUEST`. Input was maybe too long, retrying on tokenized version. For best performance, limit the size of your document template.");
                self.try_embed_tokenized(texts, deadline)
            }
            Err(error) => Err(error),
        }
    }

    fn try_embed_tokenized<S: AsRef<str>>(
        &self,
        text: &[S],
        deadline: Option<Instant>,
    ) -> Result<Vec<Embedding>, EmbedError> {
        let mut all_embeddings = Vec::with_capacity(text.len());
        for text in text {
            let text = text.as_ref();
            let max_token_count = self.options.embedding_model.max_token();
            let encoded = self.tokenizer.encode_ordinary(text);
            let len = encoded.len();
            if len < max_token_count {
                all_embeddings.append(&mut self.rest_embedder.embed_ref(&[text], deadline)?);
                continue;
            }

            let tokens = &encoded.as_slice()[0..max_token_count];

            let embedding = self.rest_embedder.embed_tokens(tokens, deadline)?;

            all_embeddings.push(embedding);
        }
        Ok(all_embeddings)
    }

    pub fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
        threads: &ThreadPoolNoAbort,
    ) -> Result<Vec<Vec<Embedding>>, EmbedError> {
        // This condition helps reduce the number of active rayon jobs
        // so that we avoid consuming all the LMDB rtxns and avoid stack overflows.
        if threads.active_operations() >= REQUEST_PARALLELISM {
            text_chunks.into_iter().map(move |chunk| self.embed(&chunk, None)).collect()
        } else {
            threads
                .install(move || {
                    text_chunks.into_par_iter().map(move |chunk| self.embed(&chunk, None)).collect()
                })
                .map_err(|error| EmbedError {
                    kind: EmbedErrorKind::PanicInThreadPool(error),
                    fault: FaultSource::Bug,
                })?
        }
    }

    pub(crate) fn embed_chunks_ref(
        &self,
        texts: &[&str],
        threads: &ThreadPoolNoAbort,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        // This condition helps reduce the number of active rayon jobs
        // so that we avoid consuming all the LMDB rtxns and avoid stack overflows.
        if threads.active_operations() >= REQUEST_PARALLELISM {
            let embeddings: Result<Vec<Vec<Embedding>>, _> = texts
                .chunks(self.prompt_count_in_chunk_hint())
                .map(move |chunk| self.embed(chunk, None))
                .collect();
            let embeddings = embeddings?;
            Ok(embeddings.into_iter().flatten().collect())
        } else {
            threads
                .install(move || {
                    let embeddings: Result<Vec<Vec<Embedding>>, _> = texts
                        .par_chunks(self.prompt_count_in_chunk_hint())
                        .map(move |chunk| self.embed(chunk, None))
                        .collect();

                    let embeddings = embeddings?;
                    Ok(embeddings.into_iter().flatten().collect())
                })
                .map_err(|error| EmbedError {
                    kind: EmbedErrorKind::PanicInThreadPool(error),
                    fault: FaultSource::Bug,
                })?
        }
    }

    pub fn chunk_count_hint(&self) -> usize {
        self.rest_embedder.chunk_count_hint()
    }

    pub fn prompt_count_in_chunk_hint(&self) -> usize {
        self.rest_embedder.prompt_count_in_chunk_hint()
    }

    pub fn dimensions(&self) -> usize {
        self.options.dimensions()
    }

    pub fn distribution(&self) -> Option<DistributionShift> {
        self.options.distribution()
    }
}

impl fmt::Debug for Embedder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Embedder")
            .field("tokenizer", &"CoreBPE")
            .field("rest_embedder", &self.rest_embedder)
            .field("options", &self.options)
            .finish()
    }
}
