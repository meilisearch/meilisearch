use deserr::Deserr;
use serde::{Deserialize, Serialize};

use super::rest::InputType;
use super::{ollama, openai};
use crate::prompt::PromptData;
use crate::update::Setting;
use crate::vector::EmbeddingConfig;
use crate::UserError;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct EmbeddingSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub source: Setting<EmbedderSource>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub model: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub revision: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub api_key: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub dimensions: Setting<usize>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub document_template: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub url: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub query: Setting<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub input_field: Setting<Vec<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub path_to_embeddings: Setting<Vec<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub embedding_object: Setting<Vec<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub input_type: Setting<InputType>,
}

pub fn check_unset<T>(
    key: &Setting<T>,
    field: &'static str,
    source: EmbedderSource,
    embedder_name: &str,
) -> Result<(), UserError> {
    if matches!(key, Setting::NotSet) {
        Ok(())
    } else {
        Err(UserError::InvalidFieldForSource {
            embedder_name: embedder_name.to_owned(),
            source_: source,
            field,
            allowed_fields_for_source: EmbeddingSettings::allowed_fields_for_source(source),
            allowed_sources_for_field: EmbeddingSettings::allowed_sources_for_field(field),
        })
    }
}

pub fn check_set<T>(
    key: &Setting<T>,
    field: &'static str,
    source: EmbedderSource,
    embedder_name: &str,
) -> Result<(), UserError> {
    if matches!(key, Setting::Set(_)) {
        Ok(())
    } else {
        Err(UserError::MissingFieldForSource {
            field,
            source_: source,
            embedder_name: embedder_name.to_owned(),
        })
    }
}

impl EmbeddingSettings {
    pub const SOURCE: &'static str = "source";
    pub const MODEL: &'static str = "model";
    pub const REVISION: &'static str = "revision";
    pub const API_KEY: &'static str = "apiKey";
    pub const DIMENSIONS: &'static str = "dimensions";
    pub const DOCUMENT_TEMPLATE: &'static str = "documentTemplate";

    pub const URL: &'static str = "url";
    pub const QUERY: &'static str = "query";
    pub const INPUT_FIELD: &'static str = "inputField";
    pub const PATH_TO_EMBEDDINGS: &'static str = "pathToEmbeddings";
    pub const EMBEDDING_OBJECT: &'static str = "embeddingObject";
    pub const INPUT_TYPE: &'static str = "inputType";

    pub fn allowed_sources_for_field(field: &'static str) -> &'static [EmbedderSource] {
        match field {
            Self::SOURCE => &[
                EmbedderSource::HuggingFace,
                EmbedderSource::OpenAi,
                EmbedderSource::UserProvided,
                EmbedderSource::Rest,
                EmbedderSource::Ollama,
            ],
            Self::MODEL => {
                &[EmbedderSource::HuggingFace, EmbedderSource::OpenAi, EmbedderSource::Ollama]
            }
            Self::REVISION => &[EmbedderSource::HuggingFace],
            Self::API_KEY => &[EmbedderSource::OpenAi, EmbedderSource::Rest],
            Self::DIMENSIONS => {
                &[EmbedderSource::OpenAi, EmbedderSource::UserProvided, EmbedderSource::Rest]
            }
            Self::DOCUMENT_TEMPLATE => &[
                EmbedderSource::HuggingFace,
                EmbedderSource::OpenAi,
                EmbedderSource::Ollama,
                EmbedderSource::Rest,
            ],
            Self::URL => &[EmbedderSource::Ollama, EmbedderSource::Rest],
            Self::QUERY => &[EmbedderSource::Rest],
            Self::INPUT_FIELD => &[EmbedderSource::Rest],
            Self::PATH_TO_EMBEDDINGS => &[EmbedderSource::Rest],
            Self::EMBEDDING_OBJECT => &[EmbedderSource::Rest],
            Self::INPUT_TYPE => &[EmbedderSource::Rest],
            _other => unreachable!("unknown field"),
        }
    }

    pub fn allowed_fields_for_source(source: EmbedderSource) -> &'static [&'static str] {
        match source {
            EmbedderSource::OpenAi => &[
                Self::SOURCE,
                Self::MODEL,
                Self::API_KEY,
                Self::DOCUMENT_TEMPLATE,
                Self::DIMENSIONS,
            ],
            EmbedderSource::HuggingFace => {
                &[Self::SOURCE, Self::MODEL, Self::REVISION, Self::DOCUMENT_TEMPLATE]
            }
            EmbedderSource::Ollama => {
                &[Self::SOURCE, Self::MODEL, Self::DOCUMENT_TEMPLATE, Self::URL]
            }
            EmbedderSource::UserProvided => &[Self::SOURCE, Self::DIMENSIONS],
            EmbedderSource::Rest => &[
                Self::SOURCE,
                Self::API_KEY,
                Self::DIMENSIONS,
                Self::DOCUMENT_TEMPLATE,
                Self::URL,
                Self::QUERY,
                Self::INPUT_FIELD,
                Self::PATH_TO_EMBEDDINGS,
                Self::EMBEDDING_OBJECT,
                Self::INPUT_TYPE,
            ],
        }
    }

    pub(crate) fn apply_default_source(setting: &mut Setting<EmbeddingSettings>) {
        if let Setting::Set(EmbeddingSettings {
            source: source @ (Setting::NotSet | Setting::Reset),
            ..
        }) = setting
        {
            *source = Setting::Set(EmbedderSource::default())
        }
    }

    pub(crate) fn apply_default_openai_model(setting: &mut Setting<EmbeddingSettings>) {
        if let Setting::Set(EmbeddingSettings {
            source: Setting::Set(EmbedderSource::OpenAi),
            model: model @ (Setting::NotSet | Setting::Reset),
            ..
        }) = setting
        {
            *model = Setting::Set(openai::EmbeddingModel::default().name().to_owned())
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub enum EmbedderSource {
    #[default]
    OpenAi,
    HuggingFace,
    Ollama,
    UserProvided,
    Rest,
}

impl std::fmt::Display for EmbedderSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            EmbedderSource::OpenAi => "openAi",
            EmbedderSource::HuggingFace => "huggingFace",
            EmbedderSource::UserProvided => "userProvided",
            EmbedderSource::Ollama => "ollama",
            EmbedderSource::Rest => "rest",
        };
        f.write_str(s)
    }
}

impl EmbeddingSettings {
    pub fn apply(&mut self, new: Self) {
        let EmbeddingSettings {
            source,
            model,
            revision,
            api_key,
            dimensions,
            document_template,
            url,
            query,
            input_field,
            path_to_embeddings,
            embedding_object,
            input_type,
        } = new;
        let old_source = self.source;
        self.source.apply(source);
        // Reinitialize the whole setting object on a source change
        if old_source != self.source {
            *self = EmbeddingSettings {
                source,
                model,
                revision,
                api_key,
                dimensions,
                document_template,
                url,
                query,
                input_field,
                path_to_embeddings,
                embedding_object,
                input_type,
            };
            return;
        }

        self.model.apply(model);
        self.revision.apply(revision);
        self.api_key.apply(api_key);
        self.dimensions.apply(dimensions);
        self.document_template.apply(document_template);

        self.url.apply(url);
        self.query.apply(query);
        self.input_field.apply(input_field);
        self.path_to_embeddings.apply(path_to_embeddings);
        self.embedding_object.apply(embedding_object);
        self.input_type.apply(input_type);
    }
}

impl From<EmbeddingConfig> for EmbeddingSettings {
    fn from(value: EmbeddingConfig) -> Self {
        let EmbeddingConfig { embedder_options, prompt } = value;
        match embedder_options {
            super::EmbedderOptions::HuggingFace(options) => Self {
                source: Setting::Set(EmbedderSource::HuggingFace),
                model: Setting::Set(options.model),
                revision: options.revision.map(Setting::Set).unwrap_or_default(),
                api_key: Setting::NotSet,
                dimensions: Setting::NotSet,
                document_template: Setting::Set(prompt.template),
                url: Setting::NotSet,
                query: Setting::NotSet,
                input_field: Setting::NotSet,
                path_to_embeddings: Setting::NotSet,
                embedding_object: Setting::NotSet,
                input_type: Setting::NotSet,
            },
            super::EmbedderOptions::OpenAi(options) => Self {
                source: Setting::Set(EmbedderSource::OpenAi),
                model: Setting::Set(options.embedding_model.name().to_owned()),
                revision: Setting::NotSet,
                api_key: options.api_key.map(Setting::Set).unwrap_or_default(),
                dimensions: options.dimensions.map(Setting::Set).unwrap_or_default(),
                document_template: Setting::Set(prompt.template),
                url: Setting::NotSet,
                query: Setting::NotSet,
                input_field: Setting::NotSet,
                path_to_embeddings: Setting::NotSet,
                embedding_object: Setting::NotSet,
                input_type: Setting::NotSet,
            },
            super::EmbedderOptions::Ollama(options) => Self {
                source: Setting::Set(EmbedderSource::Ollama),
                model: Setting::Set(options.embedding_model.to_owned()),
                revision: Setting::NotSet,
                api_key: Setting::NotSet,
                dimensions: Setting::NotSet,
                document_template: Setting::Set(prompt.template),
                url: Setting::NotSet,
                query: Setting::NotSet,
                input_field: Setting::NotSet,
                path_to_embeddings: Setting::NotSet,
                embedding_object: Setting::NotSet,
                input_type: Setting::NotSet,
            },
            super::EmbedderOptions::UserProvided(options) => Self {
                source: Setting::Set(EmbedderSource::UserProvided),
                model: Setting::NotSet,
                revision: Setting::NotSet,
                api_key: Setting::NotSet,
                dimensions: Setting::Set(options.dimensions),
                document_template: Setting::NotSet,
                url: Setting::NotSet,
                query: Setting::NotSet,
                input_field: Setting::NotSet,
                path_to_embeddings: Setting::NotSet,
                embedding_object: Setting::NotSet,
                input_type: Setting::NotSet,
            },
            super::EmbedderOptions::Rest(super::rest::EmbedderOptions {
                api_key,
                // TODO: support distribution
                distribution: _,
                dimensions,
                url,
                query,
                input_field,
                path_to_embeddings,
                embedding_object,
                input_type,
            }) => Self {
                source: Setting::Set(EmbedderSource::Rest),
                model: Setting::NotSet,
                revision: Setting::NotSet,
                api_key: api_key.map(Setting::Set).unwrap_or_default(),
                dimensions: dimensions.map(Setting::Set).unwrap_or_default(),
                document_template: Setting::Set(prompt.template),
                url: Setting::Set(url),
                query: Setting::Set(query),
                input_field: Setting::Set(input_field),
                path_to_embeddings: Setting::Set(path_to_embeddings),
                embedding_object: Setting::Set(embedding_object),
                input_type: Setting::Set(input_type),
            },
        }
    }
}

impl From<EmbeddingSettings> for EmbeddingConfig {
    fn from(value: EmbeddingSettings) -> Self {
        let mut this = Self::default();
        let EmbeddingSettings {
            source,
            model,
            revision,
            api_key,
            dimensions,
            document_template,
            url,
            query,
            input_field,
            path_to_embeddings,
            embedding_object,
            input_type,
        } = value;
        if let Some(source) = source.set() {
            match source {
                EmbedderSource::OpenAi => {
                    let mut options = super::openai::EmbedderOptions::with_default_model(None);
                    if let Some(model) = model.set() {
                        if let Some(model) = super::openai::EmbeddingModel::from_name(&model) {
                            options.embedding_model = model;
                        }
                    }
                    if let Some(api_key) = api_key.set() {
                        options.api_key = Some(api_key);
                    }
                    if let Some(dimensions) = dimensions.set() {
                        options.dimensions = Some(dimensions);
                    }
                    this.embedder_options = super::EmbedderOptions::OpenAi(options);
                }
                EmbedderSource::Ollama => {
                    let mut options: ollama::EmbedderOptions =
                        super::ollama::EmbedderOptions::with_default_model(None);
                    if let Some(model) = model.set() {
                        options.embedding_model = model;
                    }

                    if let Some(url) = url.set() {
                        options.url = Some(url)
                    }

                    this.embedder_options = super::EmbedderOptions::Ollama(options);
                }
                EmbedderSource::HuggingFace => {
                    let mut options = super::hf::EmbedderOptions::default();
                    if let Some(model) = model.set() {
                        options.model = model;
                        // Reset the revision if we are setting the model.
                        // This allows the following:
                        // "huggingFace": {} -> default model with default revision
                        // "huggingFace": { "model": "name-of-the-default-model" } -> default model without a revision
                        // "huggingFace": { "model": "some-other-model" } -> most importantly, other model without a revision
                        options.revision = None;
                    }
                    if let Some(revision) = revision.set() {
                        options.revision = Some(revision);
                    }
                    this.embedder_options = super::EmbedderOptions::HuggingFace(options);
                }
                EmbedderSource::UserProvided => {
                    this.embedder_options =
                        super::EmbedderOptions::UserProvided(super::manual::EmbedderOptions {
                            dimensions: dimensions.set().unwrap(),
                        });
                }
                EmbedderSource::Rest => {
                    let embedder_options = super::rest::EmbedderOptions::default();

                    this.embedder_options =
                        super::EmbedderOptions::Rest(super::rest::EmbedderOptions {
                            api_key: api_key.set(),
                            distribution: None,
                            dimensions: dimensions.set(),
                            url: url.set().unwrap(),
                            query: query.set().unwrap_or(embedder_options.query),
                            input_field: input_field.set().unwrap_or(embedder_options.input_field),
                            path_to_embeddings: path_to_embeddings
                                .set()
                                .unwrap_or(embedder_options.path_to_embeddings),
                            embedding_object: embedding_object
                                .set()
                                .unwrap_or(embedder_options.embedding_object),
                            input_type: input_type.set().unwrap_or(embedder_options.input_type),
                        })
                }
            }
        }

        if let Setting::Set(template) = document_template {
            this.prompt = PromptData { template }
        }

        this
    }
}
