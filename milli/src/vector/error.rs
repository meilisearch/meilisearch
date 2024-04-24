use std::path::PathBuf;

use hf_hub::api::sync::ApiError;

use crate::error::FaultSource;
use crate::PanicCatched;

#[derive(Debug, thiserror::Error)]
#[error("Error while generating embeddings: {inner}")]
pub struct Error {
    pub inner: Box<ErrorKind>,
}

impl<I: Into<ErrorKind>> From<I> for Error {
    fn from(value: I) -> Self {
        Self { inner: Box::new(value.into()) }
    }
}

impl Error {
    pub fn fault(&self) -> FaultSource {
        match &*self.inner {
            ErrorKind::NewEmbedderError(inner) => inner.fault,
            ErrorKind::EmbedError(inner) => inner.fault,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error(transparent)]
    NewEmbedderError(#[from] NewEmbedderError),
    #[error(transparent)]
    EmbedError(#[from] EmbedError),
}

#[derive(Debug, thiserror::Error)]
#[error("{fault}: {kind}")]
pub struct EmbedError {
    pub kind: EmbedErrorKind,
    pub fault: FaultSource,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbedErrorKind {
    #[error("could not tokenize: {0}")]
    Tokenize(Box<dyn std::error::Error + Send + Sync>),
    #[error("unexpected tensor shape: {0}")]
    TensorShape(candle_core::Error),
    #[error("unexpected tensor value: {0}")]
    TensorValue(candle_core::Error),
    #[error("could not run model: {0}")]
    ModelForward(candle_core::Error),
    #[error("attempt to embed the following text in a configuration where embeddings must be user provided: {0:?}")]
    ManualEmbed(String),
    #[error("model not found. Meilisearch will not automatically download models from the Ollama library, please pull the model manually: {0:?}")]
    OllamaModelNotFoundError(Option<String>),
    #[error("error deserialization the response body as JSON: {0}")]
    RestResponseDeserialization(std::io::Error),
    #[error("component `{0}` not found in path `{1}` in response: `{2}`")]
    RestResponseMissingEmbeddings(String, String, String),
    #[error("unexpected format of the embedding response: {0}")]
    RestResponseFormat(serde_json::Error),
    #[error("expected a response containing {0} embeddings, got only {1}")]
    RestResponseEmbeddingCount(usize, usize),
    #[error("could not authenticate against embedding server: {0:?}")]
    RestUnauthorized(Option<String>),
    #[error("sent too many requests to embedding server: {0:?}")]
    RestTooManyRequests(Option<String>),
    #[error("sent a bad request to embedding server: {0:?}")]
    RestBadRequest(Option<String>),
    #[error("received internal error from embedding server: {0:?}")]
    RestInternalServerError(u16, Option<String>),
    #[error("received HTTP {0} from embedding server: {0:?}")]
    RestOtherStatusCode(u16, Option<String>),
    #[error("could not reach embedding server: {0}")]
    RestNetwork(ureq::Transport),
    #[error("was expected '{}' to be an object in query '{0}'", .1.join("."))]
    RestNotAnObject(serde_json::Value, Vec<String>),
    #[error("while embedding tokenized, was expecting embeddings of dimension `{0}`, got embeddings of dimensions `{1}`")]
    OpenAiUnexpectedDimension(usize, usize),
    #[error("no embedding was produced")]
    MissingEmbedding,
    #[error(transparent)]
    PanicInThreadPool(#[from] PanicCatched),
}

impl EmbedError {
    pub fn tokenize(inner: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self { kind: EmbedErrorKind::Tokenize(inner), fault: FaultSource::Runtime }
    }

    pub fn tensor_shape(inner: candle_core::Error) -> Self {
        Self { kind: EmbedErrorKind::TensorShape(inner), fault: FaultSource::Bug }
    }

    pub fn tensor_value(inner: candle_core::Error) -> Self {
        Self { kind: EmbedErrorKind::TensorValue(inner), fault: FaultSource::Bug }
    }

    pub fn model_forward(inner: candle_core::Error) -> Self {
        Self { kind: EmbedErrorKind::ModelForward(inner), fault: FaultSource::Runtime }
    }

    pub(crate) fn embed_on_manual_embedder(texts: String) -> EmbedError {
        Self { kind: EmbedErrorKind::ManualEmbed(texts), fault: FaultSource::User }
    }

    pub(crate) fn ollama_model_not_found(inner: Option<String>) -> EmbedError {
        Self { kind: EmbedErrorKind::OllamaModelNotFoundError(inner), fault: FaultSource::User }
    }

    pub(crate) fn rest_response_deserialization(error: std::io::Error) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestResponseDeserialization(error),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_response_missing_embeddings<S: AsRef<str>>(
        response: serde_json::Value,
        component: &str,
        response_field: &[S],
    ) -> EmbedError {
        let response_field: Vec<&str> = response_field.iter().map(AsRef::as_ref).collect();
        let response_field = response_field.join(".");

        Self {
            kind: EmbedErrorKind::RestResponseMissingEmbeddings(
                component.to_owned(),
                response_field,
                serde_json::to_string_pretty(&response).unwrap_or_default(),
            ),
            fault: FaultSource::Undecided,
        }
    }

    pub(crate) fn rest_response_format(error: serde_json::Error) -> EmbedError {
        Self { kind: EmbedErrorKind::RestResponseFormat(error), fault: FaultSource::Undecided }
    }

    pub(crate) fn rest_response_embedding_count(expected: usize, got: usize) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestResponseEmbeddingCount(expected, got),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_unauthorized(error_response: Option<String>) -> EmbedError {
        Self { kind: EmbedErrorKind::RestUnauthorized(error_response), fault: FaultSource::User }
    }

    pub(crate) fn rest_too_many_requests(error_response: Option<String>) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestTooManyRequests(error_response),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_bad_request(error_response: Option<String>) -> EmbedError {
        Self { kind: EmbedErrorKind::RestBadRequest(error_response), fault: FaultSource::User }
    }

    pub(crate) fn rest_internal_server_error(
        code: u16,
        error_response: Option<String>,
    ) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestInternalServerError(code, error_response),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_other_status_code(code: u16, error_response: Option<String>) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestOtherStatusCode(code, error_response),
            fault: FaultSource::Undecided,
        }
    }

    pub(crate) fn rest_network(transport: ureq::Transport) -> EmbedError {
        Self { kind: EmbedErrorKind::RestNetwork(transport), fault: FaultSource::Runtime }
    }

    pub(crate) fn rest_not_an_object(
        query: serde_json::Value,
        input_path: Vec<String>,
    ) -> EmbedError {
        Self { kind: EmbedErrorKind::RestNotAnObject(query, input_path), fault: FaultSource::User }
    }

    pub(crate) fn openai_unexpected_dimension(expected: usize, got: usize) -> EmbedError {
        Self {
            kind: EmbedErrorKind::OpenAiUnexpectedDimension(expected, got),
            fault: FaultSource::Runtime,
        }
    }
    pub(crate) fn missing_embedding() -> EmbedError {
        Self { kind: EmbedErrorKind::MissingEmbedding, fault: FaultSource::Undecided }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{fault}: {kind}")]
pub struct NewEmbedderError {
    pub kind: NewEmbedderErrorKind,
    pub fault: FaultSource,
}

impl NewEmbedderError {
    pub fn open_config(config_filename: PathBuf, inner: std::io::Error) -> NewEmbedderError {
        let open_config = OpenConfig { filename: config_filename, inner };

        Self { kind: NewEmbedderErrorKind::OpenConfig(open_config), fault: FaultSource::Runtime }
    }

    pub fn deserialize_config(
        config: String,
        config_filename: PathBuf,
        inner: serde_json::Error,
    ) -> NewEmbedderError {
        let deserialize_config = DeserializeConfig { config, filename: config_filename, inner };
        Self {
            kind: NewEmbedderErrorKind::DeserializeConfig(deserialize_config),
            fault: FaultSource::Runtime,
        }
    }

    pub fn open_tokenizer(
        tokenizer_filename: PathBuf,
        inner: Box<dyn std::error::Error + Send + Sync>,
    ) -> NewEmbedderError {
        let open_tokenizer = OpenTokenizer { filename: tokenizer_filename, inner };
        Self {
            kind: NewEmbedderErrorKind::OpenTokenizer(open_tokenizer),
            fault: FaultSource::Runtime,
        }
    }

    pub fn new_api_fail(inner: ApiError) -> Self {
        Self { kind: NewEmbedderErrorKind::NewApiFail(inner), fault: FaultSource::Bug }
    }

    pub fn api_get(inner: ApiError) -> Self {
        Self { kind: NewEmbedderErrorKind::ApiGet(inner), fault: FaultSource::Undecided }
    }

    pub fn pytorch_weight(inner: candle_core::Error) -> Self {
        Self { kind: NewEmbedderErrorKind::PytorchWeight(inner), fault: FaultSource::Runtime }
    }

    pub fn safetensor_weight(inner: candle_core::Error) -> Self {
        Self { kind: NewEmbedderErrorKind::PytorchWeight(inner), fault: FaultSource::Runtime }
    }

    pub fn load_model(inner: candle_core::Error) -> Self {
        Self { kind: NewEmbedderErrorKind::LoadModel(inner), fault: FaultSource::Runtime }
    }

    pub fn could_not_determine_dimension(inner: EmbedError) -> NewEmbedderError {
        Self {
            kind: NewEmbedderErrorKind::CouldNotDetermineDimension(inner),
            fault: FaultSource::Runtime,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("could not open config at {filename:?}: {inner}")]
pub struct OpenConfig {
    pub filename: PathBuf,
    pub inner: std::io::Error,
}

#[derive(Debug, thiserror::Error)]
#[error("could not deserialize config at {filename}: {inner}. Config follows:\n{config}")]
pub struct DeserializeConfig {
    pub config: String,
    pub filename: PathBuf,
    pub inner: serde_json::Error,
}

#[derive(Debug, thiserror::Error)]
#[error("could not open tokenizer at {filename}: {inner}")]
pub struct OpenTokenizer {
    pub filename: PathBuf,
    #[source]
    pub inner: Box<dyn std::error::Error + Send + Sync>,
}

#[derive(Debug, thiserror::Error)]
pub enum NewEmbedderErrorKind {
    // hf
    #[error(transparent)]
    OpenConfig(OpenConfig),
    #[error(transparent)]
    DeserializeConfig(DeserializeConfig),
    #[error(transparent)]
    OpenTokenizer(OpenTokenizer),
    #[error("could not build weights from Pytorch weights: {0}")]
    PytorchWeight(candle_core::Error),
    #[error("could not build weights from Safetensor weights: {0}")]
    SafetensorWeight(candle_core::Error),
    #[error("could not spawn HG_HUB API client: {0}")]
    NewApiFail(ApiError),
    #[error("fetching file from HG_HUB failed: {0}")]
    ApiGet(ApiError),
    #[error("could not determine model dimensions: test embedding failed with {0}")]
    CouldNotDetermineDimension(EmbedError),
    #[error("loading model failed: {0}")]
    LoadModel(candle_core::Error),
}
