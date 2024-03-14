use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};

use super::openai::Retry;
use super::{DistributionShift, EmbedError, Embeddings, NewEmbedderError};
use crate::VectorOrArrayOfVectors;

pub struct Embedder {
    client: ureq::Agent,
    options: EmbedderOptions,
    bearer: Option<String>,
    dimensions: usize,
}

pub struct EmbedderOptions {
    api_key: Option<String>,
    distribution: Option<DistributionShift>,
    dimensions: Option<usize>,
    url: String,
    query: liquid::Template,
    response_field: Vec<String>,
}

impl Embedder {
    pub fn new(options: EmbedderOptions) -> Result<Self, NewEmbedderError> {
        let bearer = options.api_key.as_deref().map(|api_key| format!("Bearer: {api_key}"));

        let client = ureq::agent();

        let dimensions = if let Some(dimensions) = options.dimensions {
            dimensions
        } else {
            infer_dimensions(&client, &options, bearer.as_deref())?
        };

        Ok(Self { client, dimensions, options, bearer })
    }

    pub fn embed(&self, texts: Vec<String>) -> Result<Vec<Embeddings<f32>>, EmbedError> {
        embed(&self.client, &self.options, self.bearer.as_deref(), texts.as_slice())
    }

    pub fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
        threads: &rayon::ThreadPool,
    ) -> Result<Vec<Vec<Embeddings<f32>>>, EmbedError> {
        threads
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
    let v = embed(client, options, bearer, ["test"].as_slice())
        .map_err(NewEmbedderError::could_not_determine_dimension)?;
    // unwrap: guaranteed that v.len() == ["test"].len() == 1, otherwise the previous line terminated in error
    Ok(v.first().unwrap().dimension())
}

fn embed<S>(
    client: &ureq::Agent,
    options: &EmbedderOptions,
    bearer: Option<&str>,
    inputs: &[S],
) -> Result<Vec<Embeddings<f32>>, EmbedError>
where
    S: serde::Serialize,
{
    let request = client.post(&options.url);
    let request =
        if let Some(bearer) = bearer { request.set("Authorization", bearer) } else { request };
    let request = request.set("Content-Type", "application/json");

    let body = options
        .query
        .render(
            &liquid::to_object(&serde_json::json!({
                "input": inputs,
            }))
            .map_err(EmbedError::rest_template_context_serialization)?,
        )
        .map_err(EmbedError::rest_template_render)?;

    for attempt in 0..7 {
        let response = request.send_string(&body);
        let result = check_response(response);

        let retry_duration = match result {
            Ok(response) => {
                return response_to_embedding(response, &options.response_field, inputs.len())
            }
            Err(retry) => {
                tracing::warn!("Failed: {}", retry.error);
                retry.into_duration(attempt)
            }
        }?;

        let retry_duration = retry_duration.min(std::time::Duration::from_secs(60)); // don't wait more than a minute
        tracing::warn!("Attempt #{}, retrying after {}ms.", attempt, retry_duration.as_millis());
        std::thread::sleep(retry_duration);
    }

    let response = request.send_string(&body);
    let result = check_response(response);
    result
        .map_err(Retry::into_error)
        .and_then(|response| response_to_embedding(response, &options.response_field, inputs.len()))
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
                x => Retry::retry_later(EmbedError::rest_other_status_code(code, error_response)),
            })
        }
        Err(ureq::Error::Transport(transport)) => {
            Err(Retry::retry_later(EmbedError::rest_network(transport)))
        }
    }
}

fn response_to_embedding<S: AsRef<str>>(
    response: ureq::Response,
    response_field: &[S],
    expected_count: usize,
) -> Result<Vec<Embeddings<f32>>, EmbedError> {
    let response: serde_json::Value =
        response.into_json().map_err(EmbedError::rest_response_deserialization)?;

    let mut current_value = &response;
    for component in response_field {
        let component = component.as_ref();
        let current_value = current_value.get(component).ok_or_else(|| {
            EmbedError::rest_response_missing_embeddings(response, component, response_field)
        })?;
    }

    let embeddings = current_value.to_owned();

    let embeddings: VectorOrArrayOfVectors =
        serde_json::from_value(embeddings).map_err(EmbedError::rest_response_format)?;

    let embeddings = embeddings.into_array_of_vectors();

    let embeddings: Vec<Embeddings<f32>> = embeddings
        .into_iter()
        .flatten()
        .map(|embedding| Embeddings::from_single_embedding(embedding))
        .collect();

    if embeddings.len() != expected_count {
        return Err(EmbedError::rest_response_embedding_count(expected_count, embeddings.len()));
    }

    Ok(embeddings)
}
