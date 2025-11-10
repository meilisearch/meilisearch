use std::collections::BTreeMap;
use std::time::Instant;

use deserr::Deserr;
use rand::Rng;
use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};
use rayon::slice::ParallelSlice as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::EmbeddingCache;
use crate::error::FaultSource;
use crate::progress::EmbedderStats;
use crate::vector::error::{EmbedError, EmbedErrorKind, NewEmbedderError};
use crate::vector::json_template::{InjectableValue, JsonTemplate};
use crate::vector::{DistributionShift, Embedding, SearchQuery, REQUEST_PARALLELISM};
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
    cache: EmbeddingCache,
}

/// All data needed to perform requests and parse responses
#[derive(Debug)]
struct EmbedderData {
    client: ureq::Agent,
    bearer: Option<String>,
    headers: BTreeMap<String, String>,
    url: String,
    request: RequestData,
    response: Response,
    configuration_source: ConfigurationSource,
    max_retry_duration: std::time::Duration,
}

#[derive(Debug)]
pub enum RequestData {
    Single(Request),
    FromFragments(RequestFromFragments),
}

impl RequestData {
    pub fn new(
        request: Value,
        indexing_fragments: BTreeMap<String, Value>,
        search_fragments: BTreeMap<String, Value>,
    ) -> Result<Self, NewEmbedderError> {
        Ok(if indexing_fragments.is_empty() && search_fragments.is_empty() {
            RequestData::Single(Request::new(request)?)
        } else {
            for (name, value) in indexing_fragments {
                JsonTemplate::new(value).map_err(|error| {
                    NewEmbedderError::rest_could_not_parse_template(
                        error.parsing(&format!(".indexingFragments.{name}")),
                    )
                })?;
            }
            RequestData::FromFragments(RequestFromFragments::new(request, search_fragments)?)
        })
    }

    fn input_type(&self) -> InputType {
        match self {
            RequestData::Single(request) => request.input_type(),
            RequestData::FromFragments(request_from_fragments) => {
                request_from_fragments.input_type()
            }
        }
    }

    fn has_fragments(&self) -> bool {
        matches!(self, RequestData::FromFragments(_))
    }
}

/// Inert embedder options for a rest embedder.
///
/// # Warning
///
/// This type is serialized in and deserialized from the DB, any modification should either go
/// through dumpless upgrade or be backward-compatible
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EmbedderOptions {
    pub api_key: Option<String>,
    pub distribution: Option<DistributionShift>,
    pub dimensions: Option<usize>,
    pub url: String,
    pub request: Value,
    #[serde(default)] // backward compatibility
    pub search_fragments: BTreeMap<String, Value>,
    #[serde(default)] // backward compatibility
    pub indexing_fragments: BTreeMap<String, Value>,
    pub response: Value,
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
        cache_cap: usize,
        configuration_source: ConfigurationSource,
    ) -> Result<Self, NewEmbedderError> {
        let bearer = options.api_key.as_deref().map(|api_key| format!("Bearer {api_key}"));

        let timeout = std::env::var("MEILI_EXPERIMENTAL_REST_EMBEDDER_TIMEOUT_SECONDS")
            .ok()
            .map(|p| p.parse().unwrap())
            .unwrap_or(30);

        let client = ureq::AgentBuilder::new()
            .max_idle_connections(REQUEST_PARALLELISM * 2)
            .max_idle_connections_per_host(REQUEST_PARALLELISM * 2)
            .timeout(std::time::Duration::from_secs(timeout))
            .build();

        let request = RequestData::new(
            options.request,
            options.indexing_fragments,
            options.search_fragments,
        )?;

        let response = Response::new(options.response, &request)?;

        let max_retry_duration =
            std::env::var("MEILI_EXPERIMENTAL_REST_EMBEDDER_MAX_RETRY_DURATION_SECONDS")
                .ok()
                .map(|p| p.parse().unwrap())
                .unwrap_or(60);

        let max_retry_duration = std::time::Duration::from_secs(max_retry_duration);

        let data = EmbedderData {
            client,
            bearer,
            url: options.url,
            request,
            response,
            configuration_source,
            headers: options.headers,
            max_retry_duration,
        };

        let dimensions = if let Some(dimensions) = options.dimensions {
            dimensions
        } else {
            infer_dimensions(&data)?
        };

        Ok(Self {
            data,
            dimensions,
            distribution: options.distribution,
            cache: EmbeddingCache::new(cache_cap),
        })
    }

    pub fn embed(
        &self,
        texts: Vec<String>,
        deadline: Option<Instant>,
        embedder_stats: Option<&EmbedderStats>,
    ) -> Result<Vec<Embedding>, EmbedError> {
        embed(
            &self.data,
            texts.as_slice(),
            texts.len(),
            Some(self.dimensions),
            deadline,
            embedder_stats,
        )
    }

    pub fn embed_ref<S>(
        &self,
        texts: &[S],
        deadline: Option<Instant>,
        embedder_stats: Option<&EmbedderStats>,
    ) -> Result<Vec<Embedding>, EmbedError>
    where
        S: Serialize,
    {
        embed(&self.data, texts, texts.len(), Some(self.dimensions), deadline, embedder_stats)
    }

    pub fn embed_tokens(
        &self,
        tokens: &[u32],
        deadline: Option<Instant>,
    ) -> Result<Embedding, EmbedError> {
        let mut embeddings = embed(&self.data, tokens, 1, Some(self.dimensions), deadline, None)?;
        // unwrap: guaranteed that embeddings.len() == 1, otherwise the previous line terminated in error
        Ok(embeddings.pop().unwrap())
    }

    pub fn embed_index(
        &self,
        text_chunks: Vec<Vec<String>>,
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> Result<Vec<Vec<Embedding>>, EmbedError> {
        // This condition helps reduce the number of active rayon jobs
        // so that we avoid consuming all the LMDB rtxns and avoid stack overflows.
        if threads.active_operations() >= REQUEST_PARALLELISM {
            text_chunks
                .into_iter()
                .map(move |chunk| self.embed(chunk, None, Some(embedder_stats)))
                .collect()
        } else {
            threads
                .install(move || {
                    text_chunks
                        .into_par_iter()
                        .map(move |chunk| self.embed(chunk, None, Some(embedder_stats)))
                        .collect()
                })
                .map_err(|error| EmbedError {
                    kind: EmbedErrorKind::PanicInThreadPool(error),
                    fault: FaultSource::Bug,
                })?
        }
    }

    pub(crate) fn embed_index_ref<S: Serialize + Sync>(
        &self,
        texts: &[S],
        threads: &ThreadPoolNoAbort,
        embedder_stats: &EmbedderStats,
    ) -> Result<Vec<Embedding>, EmbedError> {
        // This condition helps reduce the number of active rayon jobs
        // so that we avoid consuming all the LMDB rtxns and avoid stack overflows.
        if threads.active_operations() >= REQUEST_PARALLELISM {
            let embeddings: Result<Vec<Vec<Embedding>>, _> = texts
                .chunks(self.prompt_count_in_chunk_hint())
                .map(move |chunk| self.embed_ref(chunk, None, Some(embedder_stats)))
                .collect();

            let embeddings = embeddings?;
            Ok(embeddings.into_iter().flatten().collect())
        } else {
            threads
                .install(move || {
                    let embeddings: Result<Vec<Vec<Embedding>>, _> = texts
                        .par_chunks(self.prompt_count_in_chunk_hint())
                        .map(move |chunk| self.embed_ref(chunk, None, Some(embedder_stats)))
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
        crate::vector::REQUEST_PARALLELISM
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

    pub(super) fn cache(&self) -> &EmbeddingCache {
        &self.cache
    }

    pub(crate) fn embed_one(
        &self,
        query: SearchQuery,
        deadline: Option<Instant>,
        embedder_stats: Option<&EmbedderStats>,
    ) -> Result<Embedding, EmbedError> {
        let mut embeddings = match (&self.data.request, query) {
            (RequestData::Single(_), SearchQuery::Text(text)) => {
                embed(&self.data, &[text], 1, Some(self.dimensions), deadline, embedder_stats)
            }
            (RequestData::Single(_), SearchQuery::Media { q: _, media: _ }) => {
                return Err(EmbedError::rest_media_not_a_fragment())
            }
            (RequestData::FromFragments(request_from_fragments), SearchQuery::Text(q)) => {
                let fragment = request_from_fragments.render_search_fragment(Some(q), None)?;

                embed(&self.data, &[fragment], 1, Some(self.dimensions), deadline, embedder_stats)
            }
            (
                RequestData::FromFragments(request_from_fragments),
                SearchQuery::Media { q, media },
            ) => {
                let fragment = request_from_fragments.render_search_fragment(q, media)?;

                embed(&self.data, &[fragment], 1, Some(self.dimensions), deadline, embedder_stats)
            }
        }?;

        // unwrap: checked by `expected_count`
        Ok(embeddings.pop().unwrap())
    }
}

fn infer_dimensions(data: &EmbedderData) -> Result<usize, NewEmbedderError> {
    if data.request.has_fragments() {
        return Err(NewEmbedderError::rest_cannot_infer_dimensions_for_fragment());
    }
    let v = embed(data, ["test"].as_slice(), 1, None, None, None)
        .map_err(NewEmbedderError::could_not_determine_dimension)?;
    // unwrap: guaranteed that v.len() == 1, otherwise the previous line terminated in error
    Ok(v.first().unwrap().len())
}

fn embed<S>(
    data: &EmbedderData,
    inputs: &[S],
    expected_count: usize,
    expected_dimension: Option<usize>,
    deadline: Option<Instant>,
    embedder_stats: Option<&EmbedderStats>,
) -> Result<Vec<Embedding>, EmbedError>
where
    S: Serialize,
{
    if inputs.is_empty() {
        if expected_count != 0 {
            return Err(EmbedError::rest_response_embedding_count(expected_count, 0));
        }
        return Ok(Vec::new());
    }

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

    let body = match &data.request {
        RequestData::Single(request) => request.inject_texts(inputs),
        RequestData::FromFragments(request_from_fragments) => {
            request_from_fragments.request_from_fragments(inputs).expect("inputs was empty")
        }
    };

    for attempt in 0..10 {
        if let Some(embedder_stats) = &embedder_stats {
            embedder_stats.total_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let response = request.clone().send_json(&body);
        let result = check_response(response, data.configuration_source).and_then(|response| {
            response_to_embedding(response, data, expected_count, expected_dimension)
        });

        let retry_duration = match result {
            Ok(response) => return Ok(response),
            Err(retry) => {
                tracing::warn!("Failed: {}", retry.error);
                if let Some(embedder_stats) = &embedder_stats {
                    let stringified_error = retry.error.to_string();
                    let mut errors =
                        embedder_stats.errors.write().unwrap_or_else(|p| p.into_inner());
                    errors.0 = Some(stringified_error);
                    errors.1 += 1;
                }
                if let Some(deadline) = deadline {
                    let now = std::time::Instant::now();
                    if now > deadline {
                        tracing::warn!("Could not embed due to deadline");
                        return Err(retry.into_error());
                    }

                    let duration_to_deadline = deadline - now;
                    retry.into_duration(attempt).map(|duration| duration.min(duration_to_deadline))
                } else {
                    retry.into_duration(attempt)
                }
            }
        }?;

        let retry_duration = retry_duration.min(data.max_retry_duration); // don't wait more than the max duration

        // randomly up to double the retry duration
        let retry_duration = retry_duration
            + rand::thread_rng().gen_range(std::time::Duration::ZERO..retry_duration);

        tracing::warn!("Attempt #{}, retrying after {}ms.", attempt, retry_duration.as_millis());
        std::thread::sleep(retry_duration);
    }

    if let Some(embedder_stats) = &embedder_stats {
        embedder_stats.total_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let response = request.send_json(&body);
    let result = check_response(response, data.configuration_source).and_then(|response| {
        response_to_embedding(response, data, expected_count, expected_dimension)
    });

    match result {
        Ok(response) => Ok(response),
        Err(retry) => {
            if let Some(embedder_stats) = &embedder_stats {
                let stringified_error = retry.error.to_string();
                let mut errors = embedder_stats.errors.write().unwrap_or_else(|p| p.into_inner());
                errors.0 = Some(stringified_error);
                errors.1 += 1;
            };
            Err(retry.into_error())
        }
    }
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
                401 => Retry::give_up(EmbedError::rest_unauthorized(
                    error_response,
                    configuration_source,
                )),
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
) -> Result<Vec<Embedding>, Retry> {
    let response: Value = response
        .into_json()
        .map_err(EmbedError::rest_response_deserialization)
        .map_err(Retry::retry_later)?;

    let embeddings = data.response.extract_embeddings(response).map_err(Retry::give_up)?;

    if embeddings.len() != expected_count {
        return Err(Retry::give_up(EmbedError::rest_response_embedding_count(
            expected_count,
            embeddings.len(),
        )));
    }

    if let Some(dimensions) = expected_dimensions {
        for embedding in &embeddings {
            if embedding.len() != dimensions {
                return Err(Retry::give_up(EmbedError::rest_unexpected_dimension(
                    dimensions,
                    embedding.len(),
                )));
            }
        }
    }

    Ok(embeddings)
}

pub(super) const REQUEST_PLACEHOLDER: &str = "{{text}}";
pub(super) const REQUEST_FRAGMENT_PLACEHOLDER: &str = "{{fragment}}";
pub(super) const RESPONSE_PLACEHOLDER: &str = "{{embedding}}";
pub(super) const REPEAT_PLACEHOLDER: &str = "{{..}}";

#[derive(Debug)]
pub struct Request {
    template: InjectableValue,
}

impl Request {
    pub fn new(template: Value) -> Result<Self, NewEmbedderError> {
        let template = match InjectableValue::new(template, REQUEST_PLACEHOLDER, REPEAT_PLACEHOLDER)
        {
            Ok(template) => template,
            Err(error) => {
                let message =
                    error.error_message("request", REQUEST_PLACEHOLDER, REPEAT_PLACEHOLDER);
                let message = format!("{message}\n  - Note: this template is using a document template, and so expects to contain the placeholder {REQUEST_PLACEHOLDER:?} rather than {REQUEST_FRAGMENT_PLACEHOLDER:?}");
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

    pub fn inject_texts<S: Serialize>(&self, texts: impl IntoIterator<Item = S>) -> Value {
        self.template.inject(texts.into_iter().map(|s| serde_json::json!(s))).unwrap()
    }
}

#[derive(Debug)]
pub struct RequestFromFragments {
    search_fragments: BTreeMap<String, JsonTemplate>,
    request: InjectableValue,
}

impl RequestFromFragments {
    pub fn new(
        request: Value,
        search_fragments: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Self, NewEmbedderError> {
        let request = match InjectableValue::new(
            request,
            REQUEST_FRAGMENT_PLACEHOLDER,
            REPEAT_PLACEHOLDER,
        ) {
            Ok(template) => template,
            Err(error) => {
                let message = error.error_message(
                    "request",
                    REQUEST_FRAGMENT_PLACEHOLDER,
                    REPEAT_PLACEHOLDER,
                );
                let message = format!("{message}\n  - Note: this template is using fragments, and so expects to contain the placeholder {REQUEST_FRAGMENT_PLACEHOLDER:?} rathern than {REQUEST_PLACEHOLDER:?}");

                return Err(NewEmbedderError::rest_could_not_parse_template(message));
            }
        };

        let search_fragments: Result<_, NewEmbedderError> = search_fragments
            .into_iter()
            .map(|(name, value)| {
                let json_template = JsonTemplate::new(value).map_err(|error| {
                    NewEmbedderError::rest_could_not_parse_template(
                        error.parsing(&format!(".searchFragments.{name}")),
                    )
                })?;
                Ok((name, json_template))
            })
            .collect();

        Ok(Self { request, search_fragments: search_fragments? })
    }

    fn input_type(&self) -> InputType {
        if self.request.has_array_value() {
            InputType::TextArray
        } else {
            InputType::Text
        }
    }

    pub fn render_search_fragment(
        &self,
        q: Option<&str>,
        media: Option<&Value>,
    ) -> Result<Value, EmbedError> {
        let mut it = self.search_fragments.iter().filter_map(|(name, template)| {
            let render = template.render_search(q, media).ok()?;
            Some((name, render))
        });
        let Some((name, fragment)) = it.next() else {
            return Err(EmbedError::rest_search_matches_no_fragment(q, media));
        };
        if let Some((second_name, _)) = it.next() {
            return Err(EmbedError::rest_search_matches_multiple_fragments(
                name,
                second_name,
                q,
                media,
            ));
        }

        Ok(fragment)
    }

    pub fn request_from_fragments<'a, S: Serialize + 'a>(
        &self,
        fragments: impl IntoIterator<Item = &'a S>,
    ) -> Option<Value> {
        self.request.inject(fragments.into_iter().map(|fragment| serde_json::json!(fragment))).ok()
    }
}

#[derive(Debug)]
pub struct Response {
    template: InjectableValue,
}

impl Response {
    pub fn new(template: Value, request: &RequestData) -> Result<Self, NewEmbedderError> {
        let template =
            match InjectableValue::new(template, RESPONSE_PLACEHOLDER, REPEAT_PLACEHOLDER) {
                Ok(template) => template,
                Err(error) => {
                    let message =
                        error.error_message("response", RESPONSE_PLACEHOLDER, REPEAT_PLACEHOLDER);
                    return Err(NewEmbedderError::rest_could_not_parse_template(message));
                }
            };

        match (template.has_array_value(), request.input_type() == InputType::TextArray) {
            (true, true) | (false, false) => Ok(Self {template}),
            (true, false) => Err(NewEmbedderError::rest_could_not_parse_template("in `response`: `response` has multiple embeddings, but `request` has only one text to embed".to_string())),
            (false, true) => Err(NewEmbedderError::rest_could_not_parse_template("in `response`: `response` has a single embedding, but `request` has multiple texts to embed".to_string())),
        }
    }

    pub fn extract_embeddings(&self, response: Value) -> Result<Vec<Embedding>, EmbedError> {
        let extracted_values: Vec<Embedding> = match self.template.extract(response) {
            Ok(extracted_values) => extracted_values,
            Err(error) => {
                let error_message =
                    error.error_message("response", "{{embedding}}", "an array of numbers");
                return Err(EmbedError::rest_extraction_error(error_message));
            }
        };
        let embeddings: Vec<Embedding> = extracted_values.into_iter().collect();

        Ok(embeddings)
    }
}
