use crate::search::{Personalize, SearchResult};
use meilisearch_types::{
    error::{Code, ErrorCode, ResponseError},
    milli::TimeBudget,
};
use rand::Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, info, warn};

const COHERE_API_URL: &str = "https://api.cohere.ai/v1/rerank";
const MAX_RETRIES: u32 = 10;

#[derive(Debug, thiserror::Error)]
enum PersonalizationError {
    #[error("Personalization service: HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Personalization service: Failed to parse response: {0}")]
    Parse(String),
    #[error("Personalization service: Cohere API error: {0}")]
    Api(String),
    #[error("Personalization service: Unauthorized: invalid API key")]
    Unauthorized,
    #[error("Personalization service: Rate limited: too many requests")]
    RateLimited,
    #[error("Personalization service: Bad request: {0}")]
    BadRequest(String),
    #[error("Personalization service: Internal server error: {0}")]
    InternalServerError(String),
    #[error("Personalization service: Network error: {0}")]
    Network(String),
    #[error("Personalization service: Deadline exceeded")]
    DeadlineExceeded,
    #[error(transparent)]
    FeatureNotEnabled(#[from] index_scheduler::error::FeatureNotEnabledError),
}

impl ErrorCode for PersonalizationError {
    fn error_code(&self) -> Code {
        match self {
            PersonalizationError::FeatureNotEnabled { .. } => Code::FeatureNotEnabled,
            PersonalizationError::Unauthorized => Code::RemoteInvalidApiKey,
            PersonalizationError::RateLimited => Code::TooManySearchRequests,
            PersonalizationError::BadRequest(_) => Code::RemoteBadRequest,
            PersonalizationError::InternalServerError(_) => Code::RemoteRemoteError,
            PersonalizationError::Network(_) | PersonalizationError::Request(_) => {
                Code::RemoteCouldNotSendRequest
            }
            PersonalizationError::Parse(_) | PersonalizationError::Api(_) => {
                Code::RemoteBadResponse
            }
            PersonalizationError::DeadlineExceeded => Code::Internal, // should not be returned to the client
        }
    }
}

pub struct CohereService {
    client: Client,
    api_key: String,
}

impl CohereService {
    pub fn new(api_key: String) -> Self {
        info!("Personalization service initialized with Cohere API");
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");
        Self { client, api_key }
    }

    pub async fn rerank_search_results(
        &self,
        search_result: SearchResult,
        personalize: &Personalize,
        query: Option<&str>,
        time_budget: TimeBudget,
    ) -> Result<SearchResult, ResponseError> {
        if time_budget.exceeded() {
            warn!("Could not rerank due to deadline");
            // If the deadline is exceeded, return the original search result instead of an error
            return Ok(search_result);
        }

        // Extract user context from personalization
        let user_context = personalize.user_context.as_str();

        // Build the prompt by merging query and user context
        let prompt = match query {
            Some(q) => format!("User Context: {user_context}\nQuery: {q}"),
            None => format!("User Context: {user_context}"),
        };

        // Extract documents for reranking
        let documents: Vec<String> = search_result
            .hits
            .iter()
            .map(|hit| {
                // Convert the document to a string representation for reranking
                serde_json::to_string(&hit.document).unwrap_or_else(|_| "{}".to_string())
            })
            .collect();

        if documents.is_empty() {
            return Ok(search_result);
        }

        // Call Cohere's rerank API with retry logic
        let reranked_indices =
            match self.call_rerank_with_retry(&prompt, &documents, time_budget).await {
                Ok(indices) => indices,
                Err(PersonalizationError::DeadlineExceeded) => {
                    // If the deadline is exceeded, return the original search result instead of an error
                    return Ok(search_result);
                }
                Err(e) => return Err(e.into()),
            };

        debug!("Cohere rerank successful, reordering {} results", search_result.hits.len());

        // Reorder the hits based on Cohere's reranking
        let mut reranked_hits = Vec::new();
        for index in reranked_indices.iter() {
            if let Some(hit) = search_result.hits.get(*index) {
                reranked_hits.push(hit.clone());
            }
        }

        Ok(SearchResult { hits: reranked_hits, ..search_result })
    }

    async fn call_rerank_with_retry(
        &self,
        query: &str,
        documents: &[String],
        time_budget: TimeBudget,
    ) -> Result<Vec<usize>, PersonalizationError> {
        let request_body = CohereRerankRequest {
            query: query.to_string(),
            documents: documents.to_vec(),
            model: "rerank-english-v3.0".to_string(),
        };

        // Retry loop similar to vector extraction
        for attempt in 0..MAX_RETRIES {
            let response_result = self.send_rerank_request(&request_body).await;

            let retry_duration = match self.handle_response(response_result).await {
                Ok(indices) => return Ok(indices),
                Err(retry) => {
                    warn!("Cohere rerank attempt #{} failed: {}", attempt, retry.error);

                    if time_budget.exceeded() {
                        warn!("Could not rerank due to deadline");
                        return Err(PersonalizationError::DeadlineExceeded);
                    } else {
                        match retry.into_duration(attempt) {
                            Ok(d) => d,
                            Err(error) => return Err(error),
                        }
                    }
                }
            };

            // randomly up to double the retry duration
            let retry_duration = retry_duration
                + rand::thread_rng().gen_range(std::time::Duration::ZERO..retry_duration);

            warn!("Retrying after {}ms", retry_duration.as_millis());
            tokio::time::sleep(retry_duration).await;
        }

        // Final attempt without retry
        let response_result = self.send_rerank_request(&request_body).await;

        match self.handle_response(response_result).await {
            Ok(indices) => Ok(indices),
            Err(retry) => Err(retry.into_error()),
        }
    }

    async fn send_rerank_request(
        &self,
        request_body: &CohereRerankRequest,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.client
            .post(COHERE_API_URL)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(request_body)
            .send()
            .await
    }

    async fn handle_response(
        &self,
        response_result: Result<reqwest::Response, reqwest::Error>,
    ) -> Result<Vec<usize>, Retry> {
        let response = match response_result {
            Ok(r) => r,
            Err(e) if e.is_timeout() => {
                return Err(Retry::retry_later(PersonalizationError::Network(format!(
                    "Request timeout: {}",
                    e
                ))));
            }
            Err(e) => {
                return Err(Retry::retry_later(PersonalizationError::Network(format!(
                    "Network error: {}",
                    e
                ))));
            }
        };

        let status = response.status();
        let status_code = status.as_u16();

        if status.is_success() {
            let rerank_response: CohereRerankResponse = match response.json().await {
                Ok(r) => r,
                Err(e) => {
                    return Err(Retry::retry_later(PersonalizationError::Parse(format!(
                        "Failed to parse response: {}",
                        e
                    ))));
                }
            };

            // Extract indices from rerank results
            let indices: Vec<usize> =
                rerank_response.results.iter().map(|result| result.index as usize).collect();

            return Ok(indices);
        }

        // Handle error status codes
        let error_body = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());

        let retry = match status_code {
            401 => Retry::give_up(PersonalizationError::Unauthorized),
            429 => Retry::rate_limited(PersonalizationError::RateLimited),
            400 => Retry::give_up(PersonalizationError::BadRequest(error_body)),
            500..=599 => Retry::retry_later(PersonalizationError::InternalServerError(format!(
                "Status {}: {}",
                status_code, error_body
            ))),
            402..=499 => Retry::give_up(PersonalizationError::Api(format!(
                "Status {}: {}",
                status_code, error_body
            ))),
            _ => Retry::retry_later(PersonalizationError::Api(format!(
                "Unexpected status {}: {}",
                status_code, error_body
            ))),
        };

        Err(retry)
    }
}

#[derive(Serialize)]
struct CohereRerankRequest {
    query: String,
    documents: Vec<String>,
    model: String,
}

#[derive(Deserialize)]
struct CohereRerankResponse {
    results: Vec<CohereRerankResult>,
}

#[derive(Deserialize)]
struct CohereRerankResult {
    index: u32,
}

// Retry strategy similar to vector extraction
struct Retry {
    error: PersonalizationError,
    strategy: RetryStrategy,
}

enum RetryStrategy {
    GiveUp,
    Retry,
    RetryAfterRateLimit,
}

impl Retry {
    fn give_up(error: PersonalizationError) -> Self {
        Self { error, strategy: RetryStrategy::GiveUp }
    }

    fn retry_later(error: PersonalizationError) -> Self {
        Self { error, strategy: RetryStrategy::Retry }
    }

    fn rate_limited(error: PersonalizationError) -> Self {
        Self { error, strategy: RetryStrategy::RetryAfterRateLimit }
    }

    fn into_duration(self, attempt: u32) -> Result<Duration, PersonalizationError> {
        match self.strategy {
            RetryStrategy::GiveUp => Err(self.error),
            RetryStrategy::Retry => {
                // Exponential backoff: 10^attempt milliseconds
                Ok(Duration::from_millis((10u64).pow(attempt)))
            }
            RetryStrategy::RetryAfterRateLimit => {
                // Longer backoff for rate limits: 100ms + exponential
                Ok(Duration::from_millis(100 + (10u64).pow(attempt)))
            }
        }
    }

    fn into_error(self) -> PersonalizationError {
        self.error
    }
}

pub enum PersonalizationService {
    Cohere(CohereService),
    Disabled,
}

impl PersonalizationService {
    pub fn cohere(api_key: String) -> Self {
        // If the API key is empty, consider the personalization service as disabled
        if api_key.trim().is_empty() {
            Self::disabled()
        } else {
            Self::Cohere(CohereService::new(api_key))
        }
    }

    pub fn disabled() -> Self {
        debug!("Personalization service disabled");
        Self::Disabled
    }

    pub async fn rerank_search_results(
        &self,
        search_result: SearchResult,
        personalize: &Personalize,
        query: Option<&str>,
        time_budget: TimeBudget,
    ) -> Result<SearchResult, ResponseError> {
        match self {
            Self::Cohere(cohere_service) => {
                cohere_service
                    .rerank_search_results(search_result, personalize, query, time_budget)
                    .await
            }
            Self::Disabled => Err(PersonalizationError::FeatureNotEnabled(
                index_scheduler::error::FeatureNotEnabledError {
                    disabled_action: "reranking search results",
                    feature: "personalization",
                    issue_link: "https://github.com/orgs/meilisearch/discussions/866",
                },
            )
            .into()),
        }
    }
}
