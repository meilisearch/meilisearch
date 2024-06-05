use deserr::Deserr;
use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};
use serde::{Deserialize, Serialize};

use super::error::EmbedErrorKind;
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
    client: ureq::Agent,
    options: EmbedderOptions,
    bearer: Option<String>,
    dimensions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EmbedderOptions {
    pub api_key: Option<String>,
    pub distribution: Option<DistributionShift>,
    pub dimensions: Option<usize>,
    pub url: String,
    pub query: serde_json::Value,
    pub input_field: Vec<String>,
    // path to the array of embeddings
    pub path_to_embeddings: Vec<String>,
    // shape of a single embedding
    pub embedding_object: Vec<String>,
    pub input_type: InputType,
}

impl Default for EmbedderOptions {
    fn default() -> Self {
        Self {
            url: Default::default(),
            query: Default::default(),
            input_field: vec!["input".into()],
            path_to_embeddings: vec!["data".into()],
            embedding_object: vec!["embedding".into()],
            input_type: InputType::Text,
            api_key: None,
            distribution: None,
            dimensions: None,
        }
    }
}

impl std::hash::Hash for EmbedderOptions {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.api_key.hash(state);
        self.distribution.hash(state);
        self.dimensions.hash(state);
        self.url.hash(state);
        // skip hashing the query
        // collisions in regular usage should be minimal,
        // and the list is limited to 256 values anyway
        self.input_field.hash(state);
        self.path_to_embeddings.hash(state);
        self.embedding_object.hash(state);
        self.input_type.hash(state);
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash, Deserr)]
#[serde(rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub enum InputType {
    Text,
    TextArray,
}

impl Embedder {
    pub fn new(options: EmbedderOptions) -> Result<Self, NewEmbedderError> {
        let bearer = options.api_key.as_deref().map(|api_key| format!("Bearer {api_key}"));

        let client = ureq::AgentBuilder::new()
            .max_idle_connections(REQUEST_PARALLELISM * 2)
            .max_idle_connections_per_host(REQUEST_PARALLELISM * 2)
            .build();

        let dimensions = if let Some(dimensions) = options.dimensions {
            dimensions
        } else {
            infer_dimensions(&client, &options, bearer.as_deref())?
        };

        Ok(Self { client, dimensions, options, bearer })
    }

    pub fn embed(&self, texts: Vec<String>) -> Result<Vec<Embeddings<f32>>, EmbedError> {
        embed(&self.client, &self.options, self.bearer.as_deref(), texts.as_slice(), texts.len())
    }

    pub fn embed_ref<S>(&self, texts: &[S]) -> Result<Vec<Embeddings<f32>>, EmbedError>
    where
        S: AsRef<str> + Serialize,
    {
        embed(&self.client, &self.options, self.bearer.as_deref(), texts, texts.len())
    }

    pub fn embed_tokens(&self, tokens: &[usize]) -> Result<Embeddings<f32>, EmbedError> {
        let mut embeddings = embed(&self.client, &self.options, self.bearer.as_deref(), tokens, 1)?;
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
        match self.options.input_type {
            InputType::Text => 1,
            InputType::TextArray => 10,
        }
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn distribution(&self) -> Option<DistributionShift> {
        self.options.distribution
    }
}

fn infer_dimensions(
    client: &ureq::Agent,
    options: &EmbedderOptions,
    bearer: Option<&str>,
) -> Result<usize, NewEmbedderError> {
    let v = embed(client, options, bearer, ["test"].as_slice(), 1)
        .map_err(NewEmbedderError::could_not_determine_dimension)?;
    // unwrap: guaranteed that v.len() == 1, otherwise the previous line terminated in error
    Ok(v.first().unwrap().dimension())
}

fn embed<S>(
    client: &ureq::Agent,
    options: &EmbedderOptions,
    bearer: Option<&str>,
    inputs: &[S],
    expected_count: usize,
) -> Result<Vec<Embeddings<f32>>, EmbedError>
where
    S: Serialize,
{
    let request = client.post(&options.url);
    let request =
        if let Some(bearer) = bearer { request.set("Authorization", bearer) } else { request };
    let request = request.set("Content-Type", "application/json");

    let input_value = match options.input_type {
        InputType::Text => serde_json::json!(inputs.first()),
        InputType::TextArray => serde_json::json!(inputs),
    };

    let body = match options.input_field.as_slice() {
        [] => {
            // inject input in body
            input_value
        }
        [input] => {
            let mut body = options.query.clone();

            body.as_object_mut()
                .ok_or_else(|| {
                    EmbedError::rest_not_an_object(
                        options.query.clone(),
                        options.input_field.clone(),
                    )
                })?
                .insert(input.clone(), input_value);
            body
        }
        [path @ .., input] => {
            let mut body = options.query.clone();

            let mut current_value = &mut body;
            for component in path {
                current_value = current_value
                    .as_object_mut()
                    .ok_or_else(|| {
                        EmbedError::rest_not_an_object(
                            options.query.clone(),
                            options.input_field.clone(),
                        )
                    })?
                    .entry(component.clone())
                    .or_insert(serde_json::json!({}));
            }

            current_value.as_object_mut().unwrap().insert(input.clone(), input_value);
            body
        }
    };

    for attempt in 0..7 {
        let response = request.clone().send_json(&body);
        let result = check_response(response);

        let retry_duration = match result {
            Ok(response) => return response_to_embedding(response, options, expected_count),
            Err(retry) => {
                tracing::warn!("Failed: {}", retry.error);
                retry.into_duration(attempt)
            }
        }?;

        let retry_duration = retry_duration.min(std::time::Duration::from_secs(60)); // don't wait more than a minute
        tracing::warn!("Attempt #{}, retrying after {}ms.", attempt, retry_duration.as_millis());
        std::thread::sleep(retry_duration);
    }

    let response = request.send_json(&body);
    let result = check_response(response);
    result
        .map_err(Retry::into_error)
        .and_then(|response| response_to_embedding(response, options, expected_count))
}

fn check_response(response: Result<ureq::Response, ureq::Error>) -> Result<ureq::Response, Retry> {
    match response {
        Ok(response) => Ok(response),
        Err(ureq::Error::Status(code, response)) => {
            let error_response: Option<String> = response.into_string().ok();
            Err(match code {
                401 => Retry::give_up(EmbedError::rest_unauthorized(error_response)),
                429 => Retry::rate_limited(EmbedError::rest_too_many_requests(error_response)),
                400 => Retry::give_up(EmbedError::rest_bad_request(error_response)),
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
    options: &EmbedderOptions,
    expected_count: usize,
) -> Result<Vec<Embeddings<f32>>, EmbedError> {
    let response: serde_json::Value =
        response.into_json().map_err(EmbedError::rest_response_deserialization)?;

    let mut current_value = &response;
    for component in &options.path_to_embeddings {
        let component = component.as_ref();
        current_value = current_value.get(component).ok_or_else(|| {
            EmbedError::rest_response_missing_embeddings(
                response.clone(),
                component,
                &options.path_to_embeddings,
            )
        })?;
    }

    let embeddings = match options.input_type {
        InputType::Text => {
            for component in &options.embedding_object {
                current_value = current_value.get(component).ok_or_else(|| {
                    EmbedError::rest_response_missing_embeddings(
                        response.clone(),
                        component,
                        &options.embedding_object,
                    )
                })?;
            }
            let embeddings = current_value.to_owned();
            let embeddings: Embedding =
                serde_json::from_value(embeddings).map_err(EmbedError::rest_response_format)?;

            vec![Embeddings::from_single_embedding(embeddings)]
        }
        InputType::TextArray => {
            let empty = vec![];
            let values = current_value.as_array().unwrap_or(&empty);
            let mut embeddings: Vec<Embeddings<f32>> = Vec::with_capacity(expected_count);
            for value in values {
                let mut current_value = value;
                for component in &options.embedding_object {
                    current_value = current_value.get(component).ok_or_else(|| {
                        EmbedError::rest_response_missing_embeddings(
                            response.clone(),
                            component,
                            &options.embedding_object,
                        )
                    })?;
                }
                let embedding = current_value.to_owned();
                let embedding: Embedding =
                    serde_json::from_value(embedding).map_err(EmbedError::rest_response_format)?;
                embeddings.push(Embeddings::from_single_embedding(embedding));
            }
            embeddings
        }
    };

    if embeddings.len() != expected_count {
        return Err(EmbedError::rest_response_embedding_count(expected_count, embeddings.len()));
    }

    Ok(embeddings)
}
