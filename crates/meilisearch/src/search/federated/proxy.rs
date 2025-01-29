pub use error::ProxySearchError;
use error::ReqwestErrorWithoutUrl;
use meilisearch_types::features::Remote;
use rand::Rng as _;
use reqwest::{Client, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::types::{FederatedSearch, FederatedSearchResult, Federation};
use crate::search::SearchQueryWithIndex;

pub const PROXY_SEARCH_HEADER: &str = "Meili-Proxy-Search";
pub const PROXY_SEARCH_HEADER_VALUE: &str = "true";

mod error {
    use meilisearch_types::error::ResponseError;
    use reqwest::StatusCode;

    #[derive(Debug, thiserror::Error)]
    pub enum ProxySearchError {
        #[error("{0}")]
        CouldNotSendRequest(ReqwestErrorWithoutUrl),
        #[error("could not authenticate against the remote host\n  - hint: check that the remote instance was registered with a valid API key having the `search` action")]
        AuthenticationError,
        #[error(
            "could not parse response from the remote host as a federated search response{}\n  - hint: check that the remote instance is a Meilisearch instance running the same version",
            response_from_remote(response)
        )]
        CouldNotParseResponse { response: Result<String, ReqwestErrorWithoutUrl> },
        #[error("remote host responded with code {}{}\n  - hint: check that the remote instance has the correct index configuration for that request\n  - hint: check that the `network` experimental feature is enabled on the remote instance", status_code.as_u16(), response_from_remote(response))]
        BadRequest { status_code: StatusCode, response: Result<String, ReqwestErrorWithoutUrl> },
        #[error("remote host did not answer before the deadline")]
        Timeout,
        #[error("remote hit does not contain `{0}`\n  - hint: check that the remote instance is a Meilisearch instance running the same version")]
        MissingPathInResponse(&'static str),
        #[error("remote host responded with code {}{}", status_code.as_u16(), response_from_remote(response))]
        RemoteError { status_code: StatusCode, response: Result<String, ReqwestErrorWithoutUrl> },
        #[error("remote hit contains an unexpected value at path `{path}`: expected {expected_type}, received `{received_value}`\n  - hint: check that the remote instance is a Meilisearch instance running the same version")]
        UnexpectedValueInPath {
            path: &'static str,
            expected_type: &'static str,
            received_value: String,
        },
        #[error("could not parse weighted score values in the remote hit: {0}")]
        CouldNotParseWeightedScoreValues(serde_json::Error),
    }

    impl ProxySearchError {
        pub fn as_response_error(&self) -> ResponseError {
            use meilisearch_types::error::Code;
            let message = self.to_string();
            let code = match self {
                ProxySearchError::CouldNotSendRequest(_) => Code::RemoteCouldNotSendRequest,
                ProxySearchError::AuthenticationError => Code::RemoteInvalidApiKey,
                ProxySearchError::BadRequest { .. } => Code::RemoteBadRequest,
                ProxySearchError::Timeout => Code::RemoteTimeout,
                ProxySearchError::RemoteError { .. } => Code::RemoteRemoteError,
                ProxySearchError::CouldNotParseResponse { .. }
                | ProxySearchError::MissingPathInResponse(_)
                | ProxySearchError::UnexpectedValueInPath { .. }
                | ProxySearchError::CouldNotParseWeightedScoreValues(_) => Code::RemoteBadResponse,
            };
            ResponseError::from_msg(message, code)
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error(transparent)]
    pub struct ReqwestErrorWithoutUrl(reqwest::Error);
    impl ReqwestErrorWithoutUrl {
        pub fn new(inner: reqwest::Error) -> Self {
            Self(inner.without_url())
        }
    }

    fn response_from_remote(response: &Result<String, ReqwestErrorWithoutUrl>) -> String {
        match response {
            Ok(response) => {
                format!(":\n  - response from remote: {}", response)
            }
            Err(error) => {
                format!(":\n  - additionally, could not retrieve response from remote: {error}")
            }
        }
    }
}

#[derive(Clone)]
pub struct ProxySearchParams {
    pub deadline: Option<std::time::Instant>,
    pub try_count: u32,
    pub client: reqwest::Client,
}

/// Performs a federated search on a remote host and returns the results
pub async fn proxy_search(
    node: &Remote,
    queries: Vec<SearchQueryWithIndex>,
    federation: Federation,
    params: &ProxySearchParams,
) -> Result<FederatedSearchResult, ProxySearchError> {
    let url = format!("{}/multi-search", node.url);

    let federated = FederatedSearch { queries, federation: Some(federation) };

    let search_api_key = node.search_api_key.as_deref();

    let max_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

    let deadline = if let Some(deadline) = params.deadline {
        std::time::Instant::min(deadline, max_deadline)
    } else {
        max_deadline
    };

    for i in 0..params.try_count {
        match try_proxy_search(&url, search_api_key, &federated, &params.client, deadline).await {
            Ok(response) => return Ok(response),
            Err(retry) => {
                let duration = retry.into_duration(i)?;
                tokio::time::sleep(duration).await;
            }
        }
    }
    try_proxy_search(&url, search_api_key, &federated, &params.client, deadline)
        .await
        .map_err(Retry::into_error)
}

async fn try_proxy_search(
    url: &str,
    search_api_key: Option<&str>,
    federated: &FederatedSearch,
    client: &Client,
    deadline: std::time::Instant,
) -> Result<FederatedSearchResult, Retry> {
    let timeout = deadline.saturating_duration_since(std::time::Instant::now());

    let request = client.post(url).json(&federated).timeout(timeout);
    let request = if let Some(search_api_key) = search_api_key {
        request.bearer_auth(search_api_key)
    } else {
        request
    };
    let request = request.header(PROXY_SEARCH_HEADER, PROXY_SEARCH_HEADER_VALUE);

    let response = request.send().await;
    let response = match response {
        Ok(response) => response,
        Err(error) if error.is_timeout() => return Err(Retry::give_up(ProxySearchError::Timeout)),
        Err(error) => {
            return Err(Retry::retry_later(ProxySearchError::CouldNotSendRequest(
                ReqwestErrorWithoutUrl::new(error),
            )))
        }
    };

    match response.status() {
        status_code if status_code.is_success() => (),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            return Err(Retry::give_up(ProxySearchError::AuthenticationError))
        }
        status_code if status_code.is_client_error() => {
            let response = parse_error(response).await;
            return Err(Retry::give_up(ProxySearchError::BadRequest { status_code, response }));
        }
        status_code if status_code.is_server_error() => {
            let response = parse_error(response).await;
            return Err(Retry::retry_later(ProxySearchError::RemoteError {
                status_code,
                response,
            }));
        }
        status_code => {
            tracing::warn!(
                status_code = status_code.as_u16(),
                "remote replied with unexpected status code"
            );
        }
    }

    let response = match parse_response(response).await {
        Ok(response) => response,
        Err(response) => {
            return Err(Retry::retry_later(ProxySearchError::CouldNotParseResponse { response }))
        }
    };

    Ok(response)
}

/// Always parse the body of the response of a failed request as JSON.
async fn parse_error(response: Response) -> Result<String, ReqwestErrorWithoutUrl> {
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => return Err(ReqwestErrorWithoutUrl::new(error)),
    };

    Ok(parse_bytes_as_error(&bytes))
}

fn parse_bytes_as_error(bytes: &[u8]) -> String {
    match serde_json::from_slice::<Value>(bytes) {
        Ok(value) => value.to_string(),
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

async fn parse_response<T: DeserializeOwned>(
    response: Response,
) -> Result<T, Result<String, ReqwestErrorWithoutUrl>> {
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => return Err(Err(ReqwestErrorWithoutUrl::new(error))),
    };

    match serde_json::from_slice::<T>(&bytes) {
        Ok(value) => Ok(value),
        Err(_) => Err(Ok(parse_bytes_as_error(&bytes))),
    }
}

pub struct Retry {
    error: ProxySearchError,
    strategy: RetryStrategy,
}

pub enum RetryStrategy {
    GiveUp,
    Retry,
}

impl Retry {
    pub fn give_up(error: ProxySearchError) -> Self {
        Self { error, strategy: RetryStrategy::GiveUp }
    }

    pub fn retry_later(error: ProxySearchError) -> Self {
        Self { error, strategy: RetryStrategy::Retry }
    }

    pub fn into_duration(self, attempt: u32) -> Result<std::time::Duration, ProxySearchError> {
        match self.strategy {
            RetryStrategy::GiveUp => Err(self.error),
            RetryStrategy::Retry => {
                let retry_duration = std::time::Duration::from_nanos((10u64).pow(attempt));
                let retry_duration = retry_duration.min(std::time::Duration::from_millis(100)); // don't wait more than 100ms

                // randomly up to double the retry duration
                let retry_duration = retry_duration
                    + rand::thread_rng().gen_range(std::time::Duration::ZERO..retry_duration);

                tracing::warn!(
                    "Attempt #{}, failed with {}, retrying after {}ms.",
                    attempt,
                    self.error,
                    retry_duration.as_millis()
                );
                Ok(retry_duration)
            }
        }
    }

    pub fn into_error(self) -> ProxySearchError {
        self.error
    }
}
