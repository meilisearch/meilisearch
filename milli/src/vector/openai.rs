use std::fmt::Display;

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use super::error::{EmbedError, NewEmbedderError};
use super::{Embedding, Embeddings};

#[derive(Debug)]
pub struct Embedder {
    client: reqwest::Client,
    tokenizer: tiktoken_rs::CoreBPE,
    options: EmbedderOptions,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct EmbedderOptions {
    pub api_key: Option<String>,
    pub embedding_model: EmbeddingModel,
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
    #[default]
    TextEmbeddingAda002,
}

impl EmbeddingModel {
    pub fn max_token(&self) -> usize {
        match self {
            EmbeddingModel::TextEmbeddingAda002 => 8191,
        }
    }

    pub fn dimensions(&self) -> usize {
        match self {
            EmbeddingModel::TextEmbeddingAda002 => 1536,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            EmbeddingModel::TextEmbeddingAda002 => "text-embedding-ada-002",
        }
    }

    pub fn from_name(name: &'static str) -> Option<Self> {
        match name {
            "text-embedding-ada-002" => Some(EmbeddingModel::TextEmbeddingAda002),
            _ => None,
        }
    }
}

pub const OPENAI_EMBEDDINGS_URL: &str = "https://api.openai.com/v1/embeddings";

impl EmbedderOptions {
    pub fn with_default_model(api_key: Option<String>) -> Self {
        Self { api_key, embedding_model: Default::default() }
    }

    pub fn with_embedding_model(api_key: Option<String>, embedding_model: EmbeddingModel) -> Self {
        Self { api_key, embedding_model }
    }
}

impl Embedder {
    pub fn new(options: EmbedderOptions) -> Result<Self, NewEmbedderError> {
        let mut headers = reqwest::header::HeaderMap::new();
        let mut inferred_api_key = Default::default();
        let api_key = options.api_key.as_ref().unwrap_or_else(|| {
            inferred_api_key = infer_api_key();
            &inferred_api_key
        });
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", api_key))
                .map_err(NewEmbedderError::openai_invalid_api_key_format)?,
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .build()
            .map_err(NewEmbedderError::openai_initialize_web_client)?;

        // looking at the code it is very unclear that this can actually fail.
        let tokenizer = tiktoken_rs::cl100k_base().unwrap();

        Ok(Self { options, client, tokenizer })
    }

    pub async fn embed(&self, texts: Vec<String>) -> Result<Vec<Embeddings<f32>>, EmbedError> {
        let mut tokenized = false;

        for attempt in 0..7 {
            let result = if tokenized {
                self.try_embed_tokenized(&texts).await
            } else {
                self.try_embed(&texts).await
            };

            let retry_duration = match result {
                Ok(embeddings) => return Ok(embeddings),
                Err(retry) => {
                    log::warn!("Failed: {}", retry.error);
                    tokenized |= retry.must_tokenize();
                    retry.into_duration(attempt)
                }
            }?;
            log::warn!("Attempt #{}, retrying after {}ms.", attempt, retry_duration.as_millis());
            tokio::time::sleep(retry_duration).await;
        }

        let result = if tokenized {
            self.try_embed_tokenized(&texts).await
        } else {
            self.try_embed(&texts).await
        };

        result.map_err(Retry::into_error)
    }

    async fn check_response(response: reqwest::Response) -> Result<reqwest::Response, Retry> {
        if !response.status().is_success() {
            match response.status() {
                StatusCode::UNAUTHORIZED => {
                    let error_response: OpenAiErrorResponse = response
                        .json()
                        .await
                        .map_err(EmbedError::openai_unexpected)
                        .map_err(Retry::retry_later)?;

                    return Err(Retry::give_up(EmbedError::openai_auth_error(
                        error_response.error,
                    )));
                }
                StatusCode::TOO_MANY_REQUESTS => {
                    let error_response: OpenAiErrorResponse = response
                        .json()
                        .await
                        .map_err(EmbedError::openai_unexpected)
                        .map_err(Retry::retry_later)?;

                    return Err(Retry::rate_limited(EmbedError::openai_too_many_requests(
                        error_response.error,
                    )));
                }
                StatusCode::INTERNAL_SERVER_ERROR => {
                    let error_response: OpenAiErrorResponse = response
                        .json()
                        .await
                        .map_err(EmbedError::openai_unexpected)
                        .map_err(Retry::retry_later)?;
                    return Err(Retry::retry_later(EmbedError::openai_internal_server_error(
                        error_response.error,
                    )));
                }
                StatusCode::SERVICE_UNAVAILABLE => {
                    let error_response: OpenAiErrorResponse = response
                        .json()
                        .await
                        .map_err(EmbedError::openai_unexpected)
                        .map_err(Retry::retry_later)?;
                    return Err(Retry::retry_later(EmbedError::openai_internal_server_error(
                        error_response.error,
                    )));
                }
                StatusCode::BAD_REQUEST => {
                    // Most probably, one text contained too many tokens
                    let error_response: OpenAiErrorResponse = response
                        .json()
                        .await
                        .map_err(EmbedError::openai_unexpected)
                        .map_err(Retry::retry_later)?;

                    log::warn!("OpenAI: input was too long, retrying on tokenized version. For best performance, limit the size of your prompt.");

                    return Err(Retry::retry_tokenized(EmbedError::openai_too_many_tokens(
                        error_response.error,
                    )));
                }
                code => {
                    return Err(Retry::give_up(EmbedError::openai_unhandled_status_code(
                        code.as_u16(),
                    )));
                }
            }
        }
        Ok(response)
    }

    async fn try_embed<S: AsRef<str> + serde::Serialize>(
        &self,
        texts: &[S],
    ) -> Result<Vec<Embeddings<f32>>, Retry> {
        for text in texts {
            log::trace!("Received prompt: {}", text.as_ref())
        }
        let request = OpenAiRequest { model: self.options.embedding_model.name(), input: texts };
        let response = self
            .client
            .post(OPENAI_EMBEDDINGS_URL)
            .json(&request)
            .send()
            .await
            .map_err(EmbedError::openai_network)
            .map_err(Retry::retry_later)?;

        let response = Self::check_response(response).await?;

        let response: OpenAiResponse = response
            .json()
            .await
            .map_err(EmbedError::openai_unexpected)
            .map_err(Retry::retry_later)?;

        log::trace!("response: {:?}", response.data);

        Ok(response
            .data
            .into_iter()
            .map(|data| Embeddings::from_single_embedding(data.embedding))
            .collect())
    }

    async fn try_embed_tokenized(&self, text: &[String]) -> Result<Vec<Embeddings<f32>>, Retry> {
        pub const OVERLAP_SIZE: usize = 200;
        let mut all_embeddings = Vec::with_capacity(text.len());
        for text in text {
            let max_token_count = self.options.embedding_model.max_token();
            let encoded = self.tokenizer.encode_ordinary(text.as_str());
            let len = encoded.len();
            if len < max_token_count {
                all_embeddings.append(&mut self.try_embed(&[text]).await?);
                continue;
            }

            let mut tokens = encoded.as_slice();
            let mut embeddings_for_prompt =
                Embeddings::new(self.options.embedding_model.dimensions());
            while tokens.len() > max_token_count {
                let window = &tokens[..max_token_count];
                embeddings_for_prompt.push(self.embed_tokens(window).await?).unwrap();

                tokens = &tokens[max_token_count - OVERLAP_SIZE..];
            }

            // end of text
            embeddings_for_prompt.push(self.embed_tokens(tokens).await?).unwrap();

            all_embeddings.push(embeddings_for_prompt);
        }
        Ok(all_embeddings)
    }

    async fn embed_tokens(&self, tokens: &[usize]) -> Result<Embedding, Retry> {
        for attempt in 0..9 {
            let duration = match self.try_embed_tokens(tokens).await {
                Ok(embedding) => return Ok(embedding),
                Err(retry) => retry.into_duration(attempt),
            }
            .map_err(Retry::retry_later)?;

            tokio::time::sleep(duration).await;
        }

        self.try_embed_tokens(tokens).await.map_err(|retry| Retry::give_up(retry.into_error()))
    }

    async fn try_embed_tokens(&self, tokens: &[usize]) -> Result<Embedding, Retry> {
        let request =
            OpenAiTokensRequest { model: self.options.embedding_model.name(), input: tokens };
        let response = self
            .client
            .post(OPENAI_EMBEDDINGS_URL)
            .json(&request)
            .send()
            .await
            .map_err(EmbedError::openai_network)
            .map_err(Retry::retry_later)?;

        let response = Self::check_response(response).await?;

        let mut response: OpenAiResponse = response
            .json()
            .await
            .map_err(EmbedError::openai_unexpected)
            .map_err(Retry::retry_later)?;
        Ok(response.data.pop().map(|data| data.embedding).unwrap_or_default())
    }

    pub async fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
    ) -> Result<Vec<Vec<Embeddings<f32>>>, EmbedError> {
        futures::future::try_join_all(text_chunks.into_iter().map(|prompts| self.embed(prompts)))
            .await
    }

    pub fn chunk_count_hint(&self) -> usize {
        10
    }

    pub fn prompt_count_in_chunk_hint(&self) -> usize {
        10
    }

    pub fn dimensions(&self) -> usize {
        self.options.embedding_model.dimensions()
    }
}

// retrying in case of failure

struct Retry {
    error: EmbedError,
    strategy: RetryStrategy,
}

enum RetryStrategy {
    GiveUp,
    Retry,
    RetryTokenized,
    RetryAfterRateLimit,
}

impl Retry {
    fn give_up(error: EmbedError) -> Self {
        Self { error, strategy: RetryStrategy::GiveUp }
    }

    fn retry_later(error: EmbedError) -> Self {
        Self { error, strategy: RetryStrategy::Retry }
    }

    fn retry_tokenized(error: EmbedError) -> Self {
        Self { error, strategy: RetryStrategy::RetryTokenized }
    }

    fn rate_limited(error: EmbedError) -> Self {
        Self { error, strategy: RetryStrategy::RetryAfterRateLimit }
    }

    fn into_duration(self, attempt: u32) -> Result<tokio::time::Duration, EmbedError> {
        match self.strategy {
            RetryStrategy::GiveUp => Err(self.error),
            RetryStrategy::Retry => Ok(tokio::time::Duration::from_millis((10u64).pow(attempt))),
            RetryStrategy::RetryTokenized => Ok(tokio::time::Duration::from_millis(1)),
            RetryStrategy::RetryAfterRateLimit => {
                Ok(tokio::time::Duration::from_millis(100 + 10u64.pow(attempt)))
            }
        }
    }

    fn must_tokenize(&self) -> bool {
        matches!(self.strategy, RetryStrategy::RetryTokenized)
    }

    fn into_error(self) -> EmbedError {
        self.error
    }
}

// openai api structs

#[derive(Debug, Serialize)]
struct OpenAiRequest<'a, S: AsRef<str> + serde::Serialize> {
    model: &'a str,
    input: &'a [S],
}

#[derive(Debug, Serialize)]
struct OpenAiTokensRequest<'a> {
    model: &'a str,
    input: &'a [usize],
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
