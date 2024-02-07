use deserr::Deserr;
use serde::{Deserialize, Serialize};

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

    pub fn allowed_sources_for_field(field: &'static str) -> &'static [EmbedderSource] {
        match field {
            Self::SOURCE => {
                &[EmbedderSource::HuggingFace, EmbedderSource::OpenAi, EmbedderSource::UserProvided]
            }
            Self::MODEL => &[EmbedderSource::HuggingFace, EmbedderSource::OpenAi],
            Self::REVISION => &[EmbedderSource::HuggingFace],
            Self::API_KEY => &[EmbedderSource::OpenAi],
            Self::DIMENSIONS => &[EmbedderSource::UserProvided],
            Self::DOCUMENT_TEMPLATE => &[EmbedderSource::HuggingFace, EmbedderSource::OpenAi],
            _other => unreachable!("unknown field"),
        }
    }

    pub fn allowed_fields_for_source(source: EmbedderSource) -> &'static [&'static str] {
        match source {
            EmbedderSource::OpenAi => {
                &[Self::SOURCE, Self::MODEL, Self::API_KEY, Self::DOCUMENT_TEMPLATE]
            }
            EmbedderSource::HuggingFace => {
                &[Self::SOURCE, Self::MODEL, Self::REVISION, Self::DOCUMENT_TEMPLATE]
            }
            EmbedderSource::UserProvided => &[Self::SOURCE, Self::DIMENSIONS],
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
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub enum EmbedderSource {
    #[default]
    OpenAi,
    HuggingFace,
    UserProvided,
}

impl std::fmt::Display for EmbedderSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            EmbedderSource::OpenAi => "openAi",
            EmbedderSource::HuggingFace => "huggingFace",
            EmbedderSource::UserProvided => "userProvided",
        };
        f.write_str(s)
    }
}

impl EmbeddingSettings {
    pub fn apply(&mut self, new: Self) {
        let EmbeddingSettings { source, model, revision, api_key, dimensions, document_template } =
            new;
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
            };
            return;
        }

        self.model.apply(model);
        self.revision.apply(revision);
        self.api_key.apply(api_key);
        self.dimensions.apply(dimensions);
        self.document_template.apply(document_template);
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
            },
            super::EmbedderOptions::OpenAi(options) => Self {
                source: Setting::Set(EmbedderSource::OpenAi),
                model: Setting::Set(options.embedding_model.name().to_owned()),
                revision: Setting::NotSet,
                api_key: options.api_key.map(Setting::Set).unwrap_or_default(),
                dimensions: options.dimensions.map(Setting::Set).unwrap_or_default(),
                document_template: Setting::Set(prompt.template),
            },
            super::EmbedderOptions::UserProvided(options) => Self {
                source: Setting::Set(EmbedderSource::UserProvided),
                model: Setting::NotSet,
                revision: Setting::NotSet,
                api_key: Setting::NotSet,
                dimensions: Setting::Set(options.dimensions),
                document_template: Setting::NotSet,
            },
        }
    }
}

impl From<EmbeddingSettings> for EmbeddingConfig {
    fn from(value: EmbeddingSettings) -> Self {
        let mut this = Self::default();
        let EmbeddingSettings { source, model, revision, api_key, dimensions, document_template } =
            value;
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
            }
        }

        if let Setting::Set(template) = document_template {
            this.prompt = PromptData { template }
        }

        this
    }
}
