use candle_core::Tensor;
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config, DTYPE};
// FIXME: currently we'll be using the hub to retrieve model, in the future we might want to embed it into Meilisearch itself
use hf_hub::api::sync::Api;
use hf_hub::{Repo, RepoType};
use tokenizers::{PaddingParams, Tokenizer};

pub use super::error::{EmbedError, Error, NewEmbedderError};
use super::{DistributionShift, Embedding, EmbeddingCache};

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    Hash,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    deserr::Deserr,
)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
enum WeightSource {
    #[default]
    Safetensors,
    Pytorch,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct EmbedderOptions {
    pub model: String,
    pub revision: Option<String>,
    pub distribution: Option<DistributionShift>,
    #[serde(default)]
    pub pooling: OverridePooling,
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    Hash,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    utoipa::ToSchema,
    deserr::Deserr,
)]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub enum OverridePooling {
    UseModel,
    ForceCls,
    #[default]
    ForceMean,
}

impl EmbedderOptions {
    pub fn new() -> Self {
        Self {
            model: "BAAI/bge-base-en-v1.5".to_string(),
            revision: Some("617ca489d9e86b49b8167676d8220688b99db36e".into()),
            distribution: None,
            pooling: OverridePooling::UseModel,
        }
    }
}

impl Default for EmbedderOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Perform embedding of documents and queries
pub struct Embedder {
    model: BertModel,
    tokenizer: Tokenizer,
    options: EmbedderOptions,
    dimensions: usize,
    pooling: Pooling,
    cache: EmbeddingCache,
}

impl std::fmt::Debug for Embedder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Embedder")
            .field("model", &self.options.model)
            .field("tokenizer", &self.tokenizer)
            .field("options", &self.options)
            .field("pooling", &self.pooling)
            .finish()
    }
}

#[derive(Clone, Copy, serde::Deserialize)]
struct PoolingConfig {
    #[serde(default)]
    pub pooling_mode_cls_token: bool,
    #[serde(default)]
    pub pooling_mode_mean_tokens: bool,
    #[serde(default)]
    pub pooling_mode_max_tokens: bool,
    #[serde(default)]
    pub pooling_mode_mean_sqrt_len_tokens: bool,
    #[serde(default)]
    pub pooling_mode_lasttoken: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum Pooling {
    #[default]
    Mean,
    Cls,
    Max,
    MeanSqrtLen,
    LastToken,
}
impl Pooling {
    fn override_with(&mut self, pooling: OverridePooling) {
        match pooling {
            OverridePooling::UseModel => {}
            OverridePooling::ForceCls => *self = Pooling::Cls,
            OverridePooling::ForceMean => *self = Pooling::Mean,
        }
    }
}

impl From<PoolingConfig> for Pooling {
    fn from(value: PoolingConfig) -> Self {
        if value.pooling_mode_cls_token {
            Self::Cls
        } else if value.pooling_mode_mean_tokens {
            Self::Mean
        } else if value.pooling_mode_lasttoken {
            Self::LastToken
        } else if value.pooling_mode_mean_sqrt_len_tokens {
            Self::MeanSqrtLen
        } else if value.pooling_mode_max_tokens {
            Self::Max
        } else {
            Self::default()
        }
    }
}

impl Embedder {
    pub fn new(
        options: EmbedderOptions,
        cache_cap: usize,
    ) -> std::result::Result<Self, NewEmbedderError> {
        let device = match candle_core::Device::cuda_if_available(0) {
            Ok(device) => device,
            Err(error) => {
                tracing::warn!("could not initialize CUDA device for Hugging Face embedder, defaulting to CPU: {}", error);
                candle_core::Device::Cpu
            }
        };
        let repo = match options.revision.clone() {
            Some(revision) => Repo::with_revision(options.model.clone(), RepoType::Model, revision),
            None => Repo::model(options.model.clone()),
        };
        let (config_filename, tokenizer_filename, weights_filename, weight_source, pooling) = {
            let api = Api::new().map_err(NewEmbedderError::new_api_fail)?;
            let api = api.repo(repo);
            let config = api.get("config.json").map_err(NewEmbedderError::api_get)?;
            let tokenizer = api.get("tokenizer.json").map_err(NewEmbedderError::api_get)?;
            let (weights, source) = {
                api.get("model.safetensors")
                    .map(|filename| (filename, WeightSource::Safetensors))
                    .or_else(|_| {
                        api.get("pytorch_model.bin")
                            .map(|filename| (filename, WeightSource::Pytorch))
                    })
                    .map_err(NewEmbedderError::api_get)?
            };
            let pooling = match api.get("1_Pooling/config.json") {
                Ok(pooling) => Some(pooling),
                Err(hf_hub::api::sync::ApiError::RequestError(error))
                    if matches!(*error, ureq::Error::Status(404, _,)) =>
                {
                    // ignore the error if the file simply doesn't exist
                    None
                }
                Err(error) => return Err(NewEmbedderError::api_get(error)),
            };
            let mut pooling: Pooling = match pooling {
                Some(pooling_filename) => {
                    let pooling = std::fs::read_to_string(&pooling_filename).map_err(|inner| {
                        NewEmbedderError::open_pooling_config(pooling_filename.clone(), inner)
                    })?;

                    let pooling: PoolingConfig =
                        serde_json::from_str(&pooling).map_err(|inner| {
                            NewEmbedderError::deserialize_pooling_config(
                                options.model.clone(),
                                pooling_filename,
                                inner,
                            )
                        })?;
                    pooling.into()
                }
                None => Pooling::default(),
            };

            pooling.override_with(options.pooling);

            (config, tokenizer, weights, source, pooling)
        };

        let config = std::fs::read_to_string(&config_filename)
            .map_err(|inner| NewEmbedderError::open_config(config_filename.clone(), inner))?;
        let config: Config = serde_json::from_str(&config).map_err(|inner| {
            NewEmbedderError::deserialize_config(
                options.model.clone(),
                config,
                config_filename,
                inner,
            )
        })?;
        let mut tokenizer = Tokenizer::from_file(&tokenizer_filename)
            .map_err(|inner| NewEmbedderError::open_tokenizer(tokenizer_filename, inner))?;

        let vb = match weight_source {
            WeightSource::Pytorch => VarBuilder::from_pth(&weights_filename, DTYPE, &device)
                .map_err(NewEmbedderError::pytorch_weight)?,
            WeightSource::Safetensors => unsafe {
                VarBuilder::from_mmaped_safetensors(&[weights_filename], DTYPE, &device)
                    .map_err(NewEmbedderError::safetensor_weight)?
            },
        };

        tracing::debug!(model = options.model, weight=?weight_source, pooling=?pooling, "model config");

        let model = BertModel::load(vb, &config).map_err(NewEmbedderError::load_model)?;

        if let Some(pp) = tokenizer.get_padding_mut() {
            pp.strategy = tokenizers::PaddingStrategy::BatchLongest
        } else {
            let pp = PaddingParams {
                strategy: tokenizers::PaddingStrategy::BatchLongest,
                ..Default::default()
            };
            tokenizer.with_padding(Some(pp));
        }

        let mut this = Self {
            model,
            tokenizer,
            options,
            dimensions: 0,
            pooling,
            cache: EmbeddingCache::new(cache_cap),
        };

        let embeddings = this
            .embed(vec!["test".into()])
            .map_err(NewEmbedderError::could_not_determine_dimension)?;
        this.dimensions = embeddings.first().unwrap().len();

        Ok(this)
    }

    pub fn embed(&self, texts: Vec<String>) -> std::result::Result<Vec<Embedding>, EmbedError> {
        texts.into_iter().map(|text| self.embed_one(&text)).collect()
    }

    fn pooling(embeddings: Tensor, pooling: Pooling) -> Result<Tensor, EmbedError> {
        match pooling {
            Pooling::Mean => Self::mean_pooling(embeddings),
            Pooling::Cls => Self::cls_pooling(embeddings),
            Pooling::Max => Self::max_pooling(embeddings),
            Pooling::MeanSqrtLen => Self::mean_sqrt_pooling(embeddings),
            Pooling::LastToken => Self::last_token_pooling(embeddings),
        }
    }

    fn cls_pooling(embeddings: Tensor) -> Result<Tensor, EmbedError> {
        embeddings.get_on_dim(1, 0).map_err(EmbedError::tensor_value)
    }

    fn mean_sqrt_pooling(embeddings: Tensor) -> Result<Tensor, EmbedError> {
        let (_n_sentence, n_tokens, _hidden_size) =
            embeddings.dims3().map_err(EmbedError::tensor_shape)?;

        (embeddings.sum(1).map_err(EmbedError::tensor_value)? / (n_tokens as f64).sqrt())
            .map_err(EmbedError::tensor_shape)
    }

    fn mean_pooling(embeddings: Tensor) -> Result<Tensor, EmbedError> {
        let (_n_sentence, n_tokens, _hidden_size) =
            embeddings.dims3().map_err(EmbedError::tensor_shape)?;

        (embeddings.sum(1).map_err(EmbedError::tensor_value)? / (n_tokens as f64))
            .map_err(EmbedError::tensor_shape)
    }

    fn max_pooling(embeddings: Tensor) -> Result<Tensor, EmbedError> {
        embeddings.max(1).map_err(EmbedError::tensor_shape)
    }

    fn last_token_pooling(embeddings: Tensor) -> Result<Tensor, EmbedError> {
        let (_n_sentence, n_tokens, _hidden_size) =
            embeddings.dims3().map_err(EmbedError::tensor_shape)?;

        embeddings.get_on_dim(1, n_tokens - 1).map_err(EmbedError::tensor_value)
    }

    pub fn embed_one(&self, text: &str) -> std::result::Result<Embedding, EmbedError> {
        let tokens = self.tokenizer.encode(text, true).map_err(EmbedError::tokenize)?;
        let token_ids = tokens.get_ids();
        let token_ids = if token_ids.len() > 512 { &token_ids[..512] } else { token_ids };
        let token_ids =
            Tensor::new(token_ids, &self.model.device).map_err(EmbedError::tensor_shape)?;
        let token_ids = Tensor::stack(&[token_ids], 0).map_err(EmbedError::tensor_shape)?;
        let token_type_ids = token_ids.zeros_like().map_err(EmbedError::tensor_shape)?;
        let embeddings = self
            .model
            .forward(&token_ids, &token_type_ids, None)
            .map_err(EmbedError::model_forward)?;

        let embedding = Self::pooling(embeddings, self.pooling)?;

        let embedding = embedding.squeeze(0).map_err(EmbedError::tensor_shape)?;
        let embedding: Embedding = embedding.to_vec1().map_err(EmbedError::tensor_shape)?;
        Ok(embedding)
    }

    pub fn embed_index(
        &self,
        text_chunks: Vec<Vec<String>>,
    ) -> std::result::Result<Vec<Vec<Embedding>>, EmbedError> {
        text_chunks.into_iter().map(|prompts| self.embed(prompts)).collect()
    }

    pub fn chunk_count_hint(&self) -> usize {
        1
    }

    pub fn prompt_count_in_chunk_hint(&self) -> usize {
        std::thread::available_parallelism().map(|x| x.get()).unwrap_or(8)
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn distribution(&self) -> Option<DistributionShift> {
        self.options.distribution.or_else(|| {
            if self.options.model == "BAAI/bge-base-en-v1.5" {
                Some(DistributionShift {
                    current_mean: ordered_float::OrderedFloat(0.85),
                    current_sigma: ordered_float::OrderedFloat(0.1),
                })
            } else {
                None
            }
        })
    }

    pub(crate) fn embed_index_ref(&self, texts: &[&str]) -> Result<Vec<Embedding>, EmbedError> {
        texts.iter().map(|text| self.embed_one(text)).collect()
    }

    pub(super) fn cache(&self) -> &EmbeddingCache {
        &self.cache
    }
}
