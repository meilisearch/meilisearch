use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::error::{EmbedError, NewEmbedderError};
use super::{DistributionShift, Embedding, Embeddings};

#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct EmbedderOptions {
    pub api_key: Option<String>,
    pub embedding_model: EmbeddingModel,
    pub dimensions: Option<usize>,
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
    #[default]
    #[serde(rename = "text-embedding-ada-002")]
    #[deserr(rename = "text-embedding-ada-002")]
    TextEmbeddingAda002,

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
            EmbeddingModel::TextEmbeddingAda002 => {
                Some(DistributionShift { current_mean: 0.90, current_sigma: 0.08 })
            }
            EmbeddingModel::TextEmbedding3Large => {
                Some(DistributionShift { current_mean: 0.70, current_sigma: 0.1 })
            }
            EmbeddingModel::TextEmbedding3Small => {
                Some(DistributionShift { current_mean: 0.75, current_sigma: 0.1 })
            }
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
        Self { api_key, embedding_model: Default::default(), dimensions: None }
    }

    pub fn with_embedding_model(api_key: Option<String>, embedding_model: EmbeddingModel) -> Self {
        Self { api_key, embedding_model, dimensions: None }
    }
}

// retrying in case of failure

pub struct Retry {
    pub error: EmbedError,
    strategy: RetryStrategy,
}

pub enum RetryStrategy {
    GiveUp,
    Retry,
    RetryTokenized,
    RetryAfterRateLimit,
}

impl Retry {
    pub fn give_up(error: EmbedError) -> Self {
        Self { error, strategy: RetryStrategy::GiveUp }
    }

    pub fn retry_later(error: EmbedError) -> Self {
        Self { error, strategy: RetryStrategy::Retry }
    }

    pub fn retry_tokenized(error: EmbedError) -> Self {
        Self { error, strategy: RetryStrategy::RetryTokenized }
    }

    pub fn rate_limited(error: EmbedError) -> Self {
        Self { error, strategy: RetryStrategy::RetryAfterRateLimit }
    }

    pub fn into_duration(self, attempt: u32) -> Result<tokio::time::Duration, EmbedError> {
        match self.strategy {
            RetryStrategy::GiveUp => Err(self.error),
            RetryStrategy::Retry => Ok(tokio::time::Duration::from_millis((10u64).pow(attempt))),
            RetryStrategy::RetryTokenized => Ok(tokio::time::Duration::from_millis(1)),
            RetryStrategy::RetryAfterRateLimit => {
                Ok(tokio::time::Duration::from_millis(100 + 10u64.pow(attempt)))
            }
        }
    }

    pub fn must_tokenize(&self) -> bool {
        matches!(self.strategy, RetryStrategy::RetryTokenized)
    }

    pub fn into_error(self) -> EmbedError {
        self.error
    }
}

// openai api structs

#[derive(Debug, Serialize)]
struct OpenAiRequest<'a, S: AsRef<str> + serde::Serialize> {
    model: &'a str,
    input: &'a [S],
    #[serde(skip_serializing_if = "Option::is_none")]
    dimensions: Option<usize>,
}

#[derive(Debug, Serialize)]
struct OpenAiTokensRequest<'a> {
    model: &'a str,
    input: &'a [usize],
    #[serde(skip_serializing_if = "Option::is_none")]
    dimensions: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponse {
    data: Vec<OpenAiEmbedding>,
}

#[derive(Debug, Deserialize)]
struct OpenAiErrorResponse {
    error: OpenAiError,
}

#[derive(Debug, Deserialize)]
pub struct OpenAiError {
    message: String,
    // type: String,
    code: Option<String>,
}

impl Display for OpenAiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.code {
            Some(code) => write!(f, "{} ({})", self.message, code),
            None => write!(f, "{}", self.message),
        }
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiEmbedding {
    embedding: Embedding,
    // object: String,
    // index: usize,
}

fn infer_api_key() -> String {
    std::env::var("MEILI_OPENAI_API_KEY")
        .or_else(|_| std::env::var("OPENAI_API_KEY"))
        .unwrap_or_default()
}

pub mod sync {
    use rayon::iter::{IntoParallelIterator, ParallelIterator as _};

    use super::{
        EmbedError, Embedding, Embeddings, NewEmbedderError, OpenAiErrorResponse, OpenAiRequest,
        OpenAiResponse, OpenAiTokensRequest, Retry, OPENAI_EMBEDDINGS_URL,
    };
    use crate::vector::DistributionShift;

    const REQUEST_PARALLELISM: usize = 10;

    #[derive(Debug)]
    pub struct Embedder {
        tokenizer: tiktoken_rs::CoreBPE,
        options: super::EmbedderOptions,
        bearer: String,
        threads: rayon::ThreadPool,
    }

    impl Embedder {
        pub fn new(options: super::EmbedderOptions) -> Result<Self, NewEmbedderError> {
            let mut inferred_api_key = Default::default();
            let api_key = options.api_key.as_ref().unwrap_or_else(|| {
                inferred_api_key = super::infer_api_key();
                &inferred_api_key
            });
            let bearer = format!("Bearer {api_key}");

            // looking at the code it is very unclear that this can actually fail.
            let tokenizer = tiktoken_rs::cl100k_base().unwrap();

            // FIXME: unwrap
            let threads = rayon::ThreadPoolBuilder::new()
                .num_threads(REQUEST_PARALLELISM)
                .thread_name(|index| format!("embedder-chunk-{index}"))
                .build()
                .unwrap();

            Ok(Self { options, bearer, tokenizer, threads })
        }

        pub fn embed(&self, texts: Vec<String>) -> Result<Vec<Embeddings<f32>>, EmbedError> {
            let mut tokenized = false;

            let client = ureq::agent();

            for attempt in 0..7 {
                let result = if tokenized {
                    self.try_embed_tokenized(&texts, &client)
                } else {
                    self.try_embed(&texts, &client)
                };

                let retry_duration = match result {
                    Ok(embeddings) => return Ok(embeddings),
                    Err(retry) => {
                        tracing::warn!("Failed: {}", retry.error);
                        tokenized |= retry.must_tokenize();
                        retry.into_duration(attempt)
                    }
                }?;

                let retry_duration = retry_duration.min(std::time::Duration::from_secs(60)); // don't wait more than a minute
                tracing::warn!(
                    "Attempt #{}, retrying after {}ms.",
                    attempt,
                    retry_duration.as_millis()
                );
                std::thread::sleep(retry_duration);
            }

            let result = if tokenized {
                self.try_embed_tokenized(&texts, &client)
            } else {
                self.try_embed(&texts, &client)
            };

            result.map_err(Retry::into_error)
        }

        fn check_response(
            response: Result<ureq::Response, ureq::Error>,
        ) -> Result<ureq::Response, Retry> {
            match response {
                Ok(response) => Ok(response),
                Err(ureq::Error::Status(code, response)) => {
                    let error_response: Option<OpenAiErrorResponse> = response.into_json().ok();
                    let error = error_response.map(|response| response.error);
                    Err(match code {
                        401 => Retry::give_up(EmbedError::openai_auth_error(error)),
                        429 => Retry::rate_limited(EmbedError::openai_too_many_requests(error)),
                        400 => {
                            tracing::warn!("OpenAI: received `BAD_REQUEST`. Input was maybe too long, retrying on tokenized version. For best performance, limit the size of your document template.");

                            Retry::retry_tokenized(EmbedError::openai_too_many_tokens(error))
                        }
                        500..=599 => {
                            Retry::retry_later(EmbedError::openai_internal_server_error(error))
                        }
                        x => Retry::retry_later(EmbedError::openai_unhandled_status_code(code)),
                    })
                }
                Err(ureq::Error::Transport(transport)) => {
                    Err(Retry::retry_later(EmbedError::openai_network(transport)))
                }
            }
        }

        fn try_embed<S: AsRef<str> + serde::Serialize>(
            &self,
            texts: &[S],
            client: &ureq::Agent,
        ) -> Result<Vec<Embeddings<f32>>, Retry> {
            for text in texts {
                tracing::trace!("Received prompt: {}", text.as_ref())
            }
            let request = OpenAiRequest {
                model: self.options.embedding_model.name(),
                input: texts,
                dimensions: self.overriden_dimensions(),
            };
            let response = client
                .post(OPENAI_EMBEDDINGS_URL)
                .set("Authorization", &self.bearer)
                .send_json(&request);

            let response = Self::check_response(response)?;

            let response: OpenAiResponse = response
                .into_json()
                .map_err(EmbedError::openai_unexpected)
                .map_err(Retry::retry_later)?;

            tracing::trace!("response: {:?}", response.data);

            Ok(response
                .data
                .into_iter()
                .map(|data| Embeddings::from_single_embedding(data.embedding))
                .collect())
        }

        fn try_embed_tokenized(
            &self,
            text: &[String],
            client: &ureq::Agent,
        ) -> Result<Vec<Embeddings<f32>>, Retry> {
            pub const OVERLAP_SIZE: usize = 200;
            let mut all_embeddings = Vec::with_capacity(text.len());
            for text in text {
                let max_token_count = self.options.embedding_model.max_token();
                let encoded = self.tokenizer.encode_ordinary(text.as_str());
                let len = encoded.len();
                if len < max_token_count {
                    all_embeddings.append(&mut self.try_embed(&[text], client)?);
                    continue;
                }

                let mut tokens = encoded.as_slice();
                let mut embeddings_for_prompt = Embeddings::new(self.dimensions());
                while tokens.len() > max_token_count {
                    let window = &tokens[..max_token_count];
                    embeddings_for_prompt.push(self.embed_tokens(window, client)?).unwrap();

                    tokens = &tokens[max_token_count - OVERLAP_SIZE..];
                }

                // end of text
                embeddings_for_prompt.push(self.embed_tokens(tokens, client)?).unwrap();

                all_embeddings.push(embeddings_for_prompt);
            }
            Ok(all_embeddings)
        }

        fn embed_tokens(&self, tokens: &[usize], client: &ureq::Agent) -> Result<Embedding, Retry> {
            for attempt in 0..9 {
                let duration = match self.try_embed_tokens(tokens, client) {
                    Ok(embedding) => return Ok(embedding),
                    Err(retry) => retry.into_duration(attempt),
                }
                .map_err(Retry::retry_later)?;

                std::thread::sleep(duration);
            }

            self.try_embed_tokens(tokens, client)
                .map_err(|retry| Retry::give_up(retry.into_error()))
        }

        fn try_embed_tokens(
            &self,
            tokens: &[usize],
            client: &ureq::Agent,
        ) -> Result<Embedding, Retry> {
            let request = OpenAiTokensRequest {
                model: self.options.embedding_model.name(),
                input: tokens,
                dimensions: self.overriden_dimensions(),
            };
            let response = client
                .post(OPENAI_EMBEDDINGS_URL)
                .set("Authorization", &self.bearer)
                .send_json(&request);

            let response = Self::check_response(response)?;

            let mut response: OpenAiResponse = response
                .into_json()
                .map_err(EmbedError::openai_unexpected)
                .map_err(Retry::retry_later)?;

            Ok(response.data.pop().map(|data| data.embedding).unwrap_or_default())
        }

        pub fn embed_chunks(
            &self,
            text_chunks: Vec<Vec<String>>,
        ) -> Result<Vec<Vec<Embeddings<f32>>>, EmbedError> {
            self.threads
                .install(move || text_chunks.into_par_iter().map(|chunk| self.embed(chunk)))
                .collect()
        }

        pub fn chunk_count_hint(&self) -> usize {
            10
        }

        pub fn prompt_count_in_chunk_hint(&self) -> usize {
            10
        }

        pub fn dimensions(&self) -> usize {
            if self.options.embedding_model.supports_overriding_dimensions() {
                self.options.dimensions.unwrap_or(self.options.embedding_model.default_dimensions())
            } else {
                self.options.embedding_model.default_dimensions()
            }
        }

        pub fn distribution(&self) -> Option<DistributionShift> {
            self.options.embedding_model.distribution()
        }

        fn overriden_dimensions(&self) -> Option<usize> {
            if self.options.embedding_model.supports_overriding_dimensions() {
                self.options.dimensions
            } else {
                None
            }
        }
    }
}
