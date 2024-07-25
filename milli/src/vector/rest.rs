use std::collections::BTreeMap;

use deserr::Deserr;
use rand::Rng;
use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};
use serde::{Deserialize, Serialize};

use super::error::EmbedErrorKind;
use super::json_template::ValueTemplate;
use super::{
    DistributionShift, EmbedError, Embedding, Embeddings, NewEmbedderError, REQUEST_PARALLELISM,
};
use crate::error::FaultSource;
use crate::ThreadPoolNoAbort;

// retrying in case of failure
pub struct Retry {
    pub error: EmbedError,
    strategy: RetryStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationSource {
    OpenAi,
    Ollama,
    User,
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

    pub fn into_duration(self, attempt: u32) -> Result<std::time::Duration, EmbedError> {
        match self.strategy {
            RetryStrategy::GiveUp => Err(self.error),
            RetryStrategy::Retry => Ok(std::time::Duration::from_millis((10u64).pow(attempt))),
            RetryStrategy::RetryTokenized => Ok(std::time::Duration::from_millis(1)),
            RetryStrategy::RetryAfterRateLimit => {
                Ok(std::time::Duration::from_millis(100 + 10u64.pow(attempt)))
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

#[derive(Debug)]
pub struct Embedder {
    data: EmbedderData,
    dimensions: usize,
    distribution: Option<DistributionShift>,
}

/// All data needed to perform requests and parse responses
#[derive(Debug)]
struct EmbedderData {
    client: ureq::Agent,
    bearer: Option<String>,
    headers: BTreeMap<String, String>,
    url: String,
    request: Request,
    response: Response,
    configuration_source: ConfigurationSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EmbedderOptions {
    pub api_key: Option<String>,
    pub distribution: Option<DistributionShift>,
    pub dimensions: Option<usize>,
    pub url: String,
    pub request: serde_json::Value,
    pub response: serde_json::Value,
    pub headers: BTreeMap<String, String>,
}

impl std::hash::Hash for EmbedderOptions {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.api_key.hash(state);
        self.distribution.hash(state);
        self.dimensions.hash(state);
        self.url.hash(state);
        // skip hashing the request and response
        // collisions in regular usage should be minimal,
        // and the list is limited to 256 values anyway
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash, Deserr)]
#[serde(rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
enum InputType {
    Text,
    TextArray,
}

impl Embedder {
    pub fn new(
        options: EmbedderOptions,
        configuration_source: ConfigurationSource,
    ) -> Result<Self, NewEmbedderError> {
        let bearer = options.api_key.as_deref().map(|api_key| format!("Bearer {api_key}"));

        let client = ureq::AgentBuilder::new()
            .max_idle_connections(REQUEST_PARALLELISM * 2)
            .max_idle_connections_per_host(REQUEST_PARALLELISM * 2)
            .build();

        let request = Request::new(options.request)?;
        let response = Response::new(options.response, &request)?;

        let data = EmbedderData {
            client,
            bearer,
            url: options.url,
            request,
            response,
            configuration_source,
            headers: options.headers,
        };

        let dimensions = if let Some(dimensions) = options.dimensions {
            dimensions
        } else {
            infer_dimensions(&data)?
        };

        Ok(Self { data, dimensions, distribution: options.distribution })
    }

    pub fn embed(&self, texts: Vec<String>) -> Result<Vec<Embeddings<f32>>, EmbedError> {
        embed(&self.data, texts.as_slice(), texts.len(), Some(self.dimensions))
    }

    pub fn embed_ref<S>(&self, texts: &[S]) -> Result<Vec<Embeddings<f32>>, EmbedError>
    where
        S: AsRef<str> + Serialize,
    {
        embed(&self.data, texts, texts.len(), Some(self.dimensions))
    }

    pub fn embed_tokens(&self, tokens: &[usize]) -> Result<Embeddings<f32>, EmbedError> {
        let mut embeddings = embed(&self.data, tokens, 1, Some(self.dimensions))?;
        // unwrap: guaranteed that embeddings.len() == 1, otherwise the previous line terminated in error
        Ok(embeddings.pop().unwrap())
    }

    pub fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
        threads: &ThreadPoolNoAbort,
    ) -> Result<Vec<Vec<Embeddings<f32>>>, EmbedError> {
        threads
            .install(move || {
                text_chunks.into_par_iter().map(move |chunk| self.embed(chunk)).collect()
            })
            .map_err(|error| EmbedError {
                kind: EmbedErrorKind::PanicInThreadPool(error),
                fault: FaultSource::Bug,
            })?
    }

    pub fn chunk_count_hint(&self) -> usize {
        super::REQUEST_PARALLELISM
    }

    pub fn prompt_count_in_chunk_hint(&self) -> usize {
        match self.data.request.input_type() {
            InputType::Text => 1,
            InputType::TextArray => 10,
        }
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn distribution(&self) -> Option<DistributionShift> {
        self.distribution
    }
}

fn infer_dimensions(data: &EmbedderData) -> Result<usize, NewEmbedderError> {
    let v = embed(data, ["test"].as_slice(), 1, None)
        .map_err(NewEmbedderError::could_not_determine_dimension)?;
    // unwrap: guaranteed that v.len() == 1, otherwise the previous line terminated in error
    Ok(v.first().unwrap().dimension())
}

fn embed<S>(
    data: &EmbedderData,
    inputs: &[S],
    expected_count: usize,
    expected_dimension: Option<usize>,
) -> Result<Vec<Embeddings<f32>>, EmbedError>
where
    S: Serialize,
{
    let request = data.client.post(&data.url);
    let request = if let Some(bearer) = &data.bearer {
        request.set("Authorization", bearer)
    } else {
        request
    };
    let mut request = request.set("Content-Type", "application/json");
    for (header, value) in &data.headers {
        request = request.set(header.as_str(), value.as_str());
    }

    let body = data.request.inject_texts(inputs);

    for attempt in 0..10 {
        let response = request.clone().send_json(&body);
        let result = check_response(response, data.configuration_source);

        let retry_duration = match result {
            Ok(response) => {
                return response_to_embedding(response, data, expected_count, expected_dimension)
            }
            Err(retry) => {
                tracing::warn!("Failed: {}", retry.error);
                retry.into_duration(attempt)
            }
        }?;

        let retry_duration = retry_duration.min(std::time::Duration::from_secs(60)); // don't wait more than a minute

        // randomly up to double the retry duration
        let retry_duration = retry_duration
            + rand::thread_rng().gen_range(std::time::Duration::ZERO..retry_duration);

        tracing::warn!("Attempt #{}, retrying after {}ms.", attempt, retry_duration.as_millis());
        std::thread::sleep(retry_duration);
    }

    let response = request.send_json(&body);
    let result = check_response(response, data.configuration_source);
    result.map_err(Retry::into_error).and_then(|response| {
        response_to_embedding(response, data, expected_count, expected_dimension)
    })
}

fn check_response(
    response: Result<ureq::Response, ureq::Error>,
    configuration_source: ConfigurationSource,
) -> Result<ureq::Response, Retry> {
    match response {
        Ok(response) => Ok(response),
        Err(ureq::Error::Status(code, response)) => {
            let error_response: Option<String> = response.into_string().ok();
            Err(match code {
                401 => Retry::give_up(EmbedError::rest_unauthorized(error_response)),
                429 => Retry::rate_limited(EmbedError::rest_too_many_requests(error_response)),
                400 => Retry::give_up(EmbedError::rest_bad_request(
                    error_response,
                    configuration_source,
                )),
                500..=599 => {
                    Retry::retry_later(EmbedError::rest_internal_server_error(code, error_response))
                }
                402..=499 => {
                    Retry::give_up(EmbedError::rest_other_status_code(code, error_response))
                }
                _ => Retry::retry_later(EmbedError::rest_other_status_code(code, error_response)),
            })
        }
        Err(ureq::Error::Transport(transport)) => {
            Err(Retry::retry_later(EmbedError::rest_network(transport)))
        }
    }
}

fn response_to_embedding(
    response: ureq::Response,
    data: &EmbedderData,
    expected_count: usize,
    expected_dimensions: Option<usize>,
) -> Result<Vec<Embeddings<f32>>, EmbedError> {
    let response: serde_json::Value =
        response.into_json().map_err(EmbedError::rest_response_deserialization)?;

    let embeddings = data.response.extract_embeddings(response)?;

    if embeddings.len() != expected_count {
        return Err(EmbedError::rest_response_embedding_count(expected_count, embeddings.len()));
    }

    if let Some(dimensions) = expected_dimensions {
        for embedding in &embeddings {
            if embedding.dimension() != dimensions {
                return Err(EmbedError::rest_unexpected_dimension(
                    dimensions,
                    embedding.dimension(),
                ));
            }
        }
    }

    Ok(embeddings)
}

pub(super) const REQUEST_PLACEHOLDER: &str = "{{text}}";
pub(super) const RESPONSE_PLACEHOLDER: &str = "{{embedding}}";
pub(super) const REPEAT_PLACEHOLDER: &str = "{{..}}";

#[derive(Debug)]
pub struct Request {
    template: ValueTemplate,
}

impl Request {
    pub fn new(template: serde_json::Value) -> Result<Self, NewEmbedderError> {
        let template = match ValueTemplate::new(template, REQUEST_PLACEHOLDER, REPEAT_PLACEHOLDER) {
            Ok(template) => template,
            Err(error) => {
                let message =
                    error.error_message("request", REQUEST_PLACEHOLDER, REPEAT_PLACEHOLDER);
                return Err(NewEmbedderError::rest_could_not_parse_template(message));
            }
        };

        Ok(Self { template })
    }

    fn input_type(&self) -> InputType {
        if self.template.has_array_value() {
            InputType::TextArray
        } else {
            InputType::Text
        }
    }

    pub fn inject_texts<S: Serialize>(
        &self,
        texts: impl IntoIterator<Item = S>,
    ) -> serde_json::Value {
        self.template.inject(texts.into_iter().map(|s| serde_json::json!(s))).unwrap()
    }
}

#[derive(Debug)]
pub struct Response {
    template: ValueTemplate,
}

impl Response {
    pub fn new(template: serde_json::Value, request: &Request) -> Result<Self, NewEmbedderError> {
        let template = match ValueTemplate::new(template, RESPONSE_PLACEHOLDER, REPEAT_PLACEHOLDER)
        {
            Ok(template) => template,
            Err(error) => {
                let message =
                    error.error_message("response", RESPONSE_PLACEHOLDER, REPEAT_PLACEHOLDER);
                return Err(NewEmbedderError::rest_could_not_parse_template(message));
            }
        };

        match (template.has_array_value(), request.template.has_array_value()) {
            (true, true) | (false, false) => Ok(Self {template}),
            (true, false) => Err(NewEmbedderError::rest_could_not_parse_template("in `response`: `response` has multiple embeddings, but `request` has only one text to embed".to_string())),
            (false, true) => Err(NewEmbedderError::rest_could_not_parse_template("in `response`: `response` has a single embedding, but `request` has multiple texts to embed".to_string())),
        }
    }

    pub fn extract_embeddings(
        &self,
        response: serde_json::Value,
    ) -> Result<Vec<Embeddings<f32>>, EmbedError> {
        let extracted_values: Vec<Embedding> = match self.template.extract(response) {
            Ok(extracted_values) => extracted_values,
            Err(error) => {
                let error_message =
                    error.error_message("response", "{{embedding}}", "an array of numbers");
                return Err(EmbedError::rest_extraction_error(error_message));
            }
        };
        let embeddings: Vec<Embeddings<f32>> =
            extracted_values.into_iter().map(Embeddings::from_single_embedding).collect();

        Ok(embeddings)
    }
}
