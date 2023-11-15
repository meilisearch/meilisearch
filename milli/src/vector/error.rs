use std::fmt::Display;
use std::path::PathBuf;

use hf_hub::api::sync::ApiError;

#[derive(Debug, Clone, Copy)]
pub enum FaultSource {
    User,
    Runtime,
    Bug,
    Undecided,
}

impl Display for FaultSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FaultSource::User => "user error",
            FaultSource::Runtime => "runtime error",
            FaultSource::Bug => "coding error",
            FaultSource::Undecided => "error",
        };
        f.write_str(s)
    }
}

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
    #[error("loading model failed: {0}")]
    LoadModel(candle_core::Error),
}
