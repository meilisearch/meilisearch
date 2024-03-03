use std::path::PathBuf;

use hf_hub::api::sync::ApiError;

use super::ollama::OllamaError;
use crate::error::FaultSource;
use crate::vector::openai::OpenAiError;

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
    #[error("could not reach OpenAI: {0}")]
    OpenAiNetwork(reqwest::Error),
    #[error("unexpected response from OpenAI: {0}")]
    OpenAiUnexpected(reqwest::Error),
    #[error("could not authenticate against OpenAI: {0}")]
    OpenAiAuth(OpenAiError),
    #[error("sent too many requests to OpenAI: {0}")]
    OpenAiTooManyRequests(OpenAiError),
    #[error("received internal error from OpenAI: {0}")]
    OpenAiInternalServerError(OpenAiError),
    #[error("sent too many tokens in a request to OpenAI: {0}")]
    OpenAiTooManyTokens(OpenAiError),
    #[error("received unhandled HTTP status code {0} from OpenAI")]
    OpenAiUnhandledStatusCode(u16),
    #[error("attempt to embed the following text in a configuration where embeddings must be user provided: {0:?}")]
    ManualEmbed(String),
    #[error("could not initialize asynchronous runtime: {0}")]
    OpenAiRuntimeInit(std::io::Error),
    #[error("initializing web client for sending embedding requests failed: {0}")]
    InitWebClient(reqwest::Error),
    // Dedicated Ollama error kinds, might have to merge them into one cohesive error type for all backends.
    #[error("unexpected response from Ollama: {0}")]
    OllamaUnexpected(reqwest::Error),
    #[error("sent too many requests to Ollama: {0}")]
    OllamaTooManyRequests(OllamaError),
    #[error("received internal error from Ollama: {0}")]
    OllamaInternalServerError(OllamaError),
    #[error("received unhandled HTTP status code {0} from Ollama")]
    OllamaUnhandledStatusCode(u16),
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

    pub fn openai_network(inner: reqwest::Error) -> Self {
        Self { kind: EmbedErrorKind::OpenAiNetwork(inner), fault: FaultSource::Runtime }
    }

    pub fn openai_unexpected(inner: reqwest::Error) -> EmbedError {
        Self { kind: EmbedErrorKind::OpenAiUnexpected(inner), fault: FaultSource::Bug }
    }

    pub(crate) fn openai_auth_error(inner: OpenAiError) -> EmbedError {
        Self { kind: EmbedErrorKind::OpenAiAuth(inner), fault: FaultSource::User }
    }

    pub(crate) fn openai_too_many_requests(inner: OpenAiError) -> EmbedError {
        Self { kind: EmbedErrorKind::OpenAiTooManyRequests(inner), fault: FaultSource::Runtime }
    }

    pub(crate) fn openai_internal_server_error(inner: OpenAiError) -> EmbedError {
        Self { kind: EmbedErrorKind::OpenAiInternalServerError(inner), fault: FaultSource::Runtime }
    }

    pub(crate) fn openai_too_many_tokens(inner: OpenAiError) -> EmbedError {
        Self { kind: EmbedErrorKind::OpenAiTooManyTokens(inner), fault: FaultSource::Bug }
    }

    pub(crate) fn openai_unhandled_status_code(code: u16) -> EmbedError {
        Self { kind: EmbedErrorKind::OpenAiUnhandledStatusCode(code), fault: FaultSource::Bug }
    }

    pub(crate) fn embed_on_manual_embedder(texts: String) -> EmbedError {
        Self { kind: EmbedErrorKind::ManualEmbed(texts), fault: FaultSource::User }
    }

    pub(crate) fn openai_runtime_init(inner: std::io::Error) -> EmbedError {
        Self { kind: EmbedErrorKind::OpenAiRuntimeInit(inner), fault: FaultSource::Runtime }
    }

    pub fn openai_initialize_web_client(inner: reqwest::Error) -> Self {
        Self { kind: EmbedErrorKind::InitWebClient(inner), fault: FaultSource::Runtime }
    }

    pub fn ollama_unexpected(inner: reqwest::Error) -> EmbedError {
        Self { kind: EmbedErrorKind::OllamaUnexpected(inner), fault: FaultSource::Bug }
    }

    pub(crate) fn ollama_too_many_requests(inner: OllamaError) -> EmbedError {
        Self { kind: EmbedErrorKind::OllamaTooManyRequests(inner), fault: FaultSource::Runtime }
    }

    pub(crate) fn ollama_internal_server_error(inner: OllamaError) -> EmbedError {
        Self { kind: EmbedErrorKind::OllamaInternalServerError(inner), fault: FaultSource::Runtime }
    }

    pub(crate) fn ollama_unhandled_status_code(code: u16) -> EmbedError {
        Self { kind: EmbedErrorKind::OllamaUnhandledStatusCode(code), fault: FaultSource::Bug }
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

    pub fn hf_could_not_determine_dimension(inner: EmbedError) -> NewEmbedderError {
        Self {
            kind: NewEmbedderErrorKind::CouldNotDetermineDimension(inner),
            fault: FaultSource::Runtime,
        }
    }

    pub fn openai_invalid_api_key_format(inner: reqwest::header::InvalidHeaderValue) -> Self {
        Self { kind: NewEmbedderErrorKind::InvalidApiKeyFormat(inner), fault: FaultSource::User }
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
    // openai
    #[error("The API key passed to Authorization error was in an invalid format: {0}")]
    InvalidApiKeyFormat(reqwest::header::InvalidHeaderValue),
}
