use deserr::Deserr;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use super::{ollama, openai, DistributionShift};
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
    pub request: Setting<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub response: Setting<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub distribution: Setting<DistributionShift>,
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

/// Indicates what action should take place during a reindexing operation for an embedder
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReindexAction {
    /// An indexing operation should take place for this embedder, keeping existing vectors
    /// and checking whether the document template changed or not
    RegeneratePrompts,
    /// An indexing operation should take place for all documents for this embedder, removing existing vectors
    /// (except userProvided ones)
    FullReindex,
}

pub enum SettingsDiff {
    Remove,
    Reindex { action: ReindexAction, updated_settings: EmbeddingSettings },
    UpdateWithoutReindex { updated_settings: EmbeddingSettings },
}

pub enum EmbedderAction {
    WriteBackToDocuments(WriteBackToDocuments),
    Reindex(ReindexAction),
}

pub struct WriteBackToDocuments {
    pub embedder_id: u8,
    pub user_provided: RoaringBitmap,
}

impl SettingsDiff {
    pub fn from_settings(old: EmbeddingSettings, new: Setting<EmbeddingSettings>) -> Self {
        match new {
            Setting::Set(new) => {
                let EmbeddingSettings {
                    mut source,
                    mut model,
                    mut revision,
                    mut api_key,
                    mut dimensions,
                    mut document_template,
                    mut url,
                    mut request,
                    mut response,
                    mut distribution,
                } = old;

                let EmbeddingSettings {
                    source: new_source,
                    model: new_model,
                    revision: new_revision,
                    api_key: new_api_key,
                    dimensions: new_dimensions,
                    document_template: new_document_template,
                    url: new_url,
                    request: new_request,
                    response: new_response,
                    distribution: new_distribution,
                } = new;

                let mut reindex_action = None;

                // **Warning**: do not use short-circuiting || here, we want all these operations applied
                if source.apply(new_source) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                    // when the source changes, we need to reapply the default settings for the new source
                    apply_default_for_source(
                        &source,
                        &mut model,
                        &mut revision,
                        &mut dimensions,
                        &mut url,
                        &mut request,
                        &mut response,
                        &mut document_template,
                    )
                }
                if model.apply(new_model) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if revision.apply(new_revision) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if dimensions.apply(new_dimensions) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if url.apply(new_url) {
                    match source {
                        // do not regenerate on an url change in OpenAI
                        Setting::Set(EmbedderSource::OpenAi) | Setting::Reset => {}
                        _ => {
                            ReindexAction::push_action(
                                &mut reindex_action,
                                ReindexAction::FullReindex,
                            );
                        }
                    }
                }
                if request.apply(new_request) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if response.apply(new_response) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if document_template.apply(new_document_template) {
                    ReindexAction::push_action(
                        &mut reindex_action,
                        ReindexAction::RegeneratePrompts,
                    );
                }

                distribution.apply(new_distribution);
                api_key.apply(new_api_key);

                let updated_settings = EmbeddingSettings {
                    source,
                    model,
                    revision,
                    api_key,
                    dimensions,
                    document_template,
                    url,
                    request,
                    response,
                    distribution,
                };

                match reindex_action {
                    Some(action) => Self::Reindex { action, updated_settings },
                    None => Self::UpdateWithoutReindex { updated_settings },
                }
            }
            Setting::Reset => Self::Remove,
            Setting::NotSet => Self::UpdateWithoutReindex { updated_settings: old },
        }
    }
}

impl ReindexAction {
    fn push_action(this: &mut Option<Self>, other: Self) {
        *this = match (*this, other) {
            (_, ReindexAction::FullReindex) => Some(ReindexAction::FullReindex),
            (Some(ReindexAction::FullReindex), _) => Some(ReindexAction::FullReindex),
            (_, ReindexAction::RegeneratePrompts) => Some(ReindexAction::RegeneratePrompts),
        }
    }
}

#[allow(clippy::too_many_arguments)] // private function
fn apply_default_for_source(
    source: &Setting<EmbedderSource>,
    model: &mut Setting<String>,
    revision: &mut Setting<String>,
    dimensions: &mut Setting<usize>,
    url: &mut Setting<String>,
    request: &mut Setting<serde_json::Value>,
    response: &mut Setting<serde_json::Value>,
    document_template: &mut Setting<String>,
) {
    match source {
        Setting::Set(EmbedderSource::HuggingFace) => {
            *model = Setting::Reset;
            *revision = Setting::Reset;
            *dimensions = Setting::NotSet;
            *url = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::Ollama) => {
            *model = Setting::Reset;
            *revision = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::OpenAi) | Setting::Reset => {
            *model = Setting::Reset;
            *revision = Setting::NotSet;
            *dimensions = Setting::NotSet;
            *url = Setting::Reset;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::Rest) => {
            *model = Setting::NotSet;
            *revision = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::Reset;
            *request = Setting::Reset;
            *response = Setting::Reset;
        }
        Setting::Set(EmbedderSource::UserProvided) => {
            *model = Setting::NotSet;
            *revision = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
            *document_template = Setting::NotSet;
        }
        Setting::NotSet => {}
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
    pub const REQUEST: &'static str = "request";
    pub const RESPONSE: &'static str = "response";

    pub const DISTRIBUTION: &'static str = "distribution";

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
            Self::API_KEY => {
                &[EmbedderSource::OpenAi, EmbedderSource::Ollama, EmbedderSource::Rest]
            }
            Self::DIMENSIONS => {
                &[EmbedderSource::OpenAi, EmbedderSource::UserProvided, EmbedderSource::Rest]
            }
            Self::DOCUMENT_TEMPLATE => &[
                EmbedderSource::HuggingFace,
                EmbedderSource::OpenAi,
                EmbedderSource::Ollama,
                EmbedderSource::Rest,
            ],
            Self::URL => &[EmbedderSource::Ollama, EmbedderSource::Rest, EmbedderSource::OpenAi],
            Self::REQUEST => &[EmbedderSource::Rest],
            Self::RESPONSE => &[EmbedderSource::Rest],
            Self::DISTRIBUTION => &[
                EmbedderSource::HuggingFace,
                EmbedderSource::Ollama,
                EmbedderSource::OpenAi,
                EmbedderSource::Rest,
                EmbedderSource::UserProvided,
            ],
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
                Self::DISTRIBUTION,
                Self::URL,
            ],
            EmbedderSource::HuggingFace => &[
                Self::SOURCE,
                Self::MODEL,
                Self::REVISION,
                Self::DOCUMENT_TEMPLATE,
                Self::DISTRIBUTION,
            ],
            EmbedderSource::Ollama => &[
                Self::SOURCE,
                Self::MODEL,
                Self::DOCUMENT_TEMPLATE,
                Self::URL,
                Self::API_KEY,
                Self::DISTRIBUTION,
            ],
            EmbedderSource::UserProvided => &[Self::SOURCE, Self::DIMENSIONS, Self::DISTRIBUTION],
            EmbedderSource::Rest => &[
                Self::SOURCE,
                Self::API_KEY,
                Self::DIMENSIONS,
                Self::DOCUMENT_TEMPLATE,
                Self::URL,
                Self::REQUEST,
                Self::RESPONSE,
                Self::DISTRIBUTION,
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

impl From<EmbeddingConfig> for EmbeddingSettings {
    fn from(value: EmbeddingConfig) -> Self {
        let EmbeddingConfig { embedder_options, prompt } = value;
        match embedder_options {
            super::EmbedderOptions::HuggingFace(super::hf::EmbedderOptions {
                model,
                revision,
                distribution,
            }) => Self {
                source: Setting::Set(EmbedderSource::HuggingFace),
                model: Setting::Set(model),
                revision: revision.map(Setting::Set).unwrap_or_default(),
                api_key: Setting::NotSet,
                dimensions: Setting::NotSet,
                document_template: Setting::Set(prompt.template),
                url: Setting::NotSet,
                request: Setting::NotSet,
                response: Setting::NotSet,
                distribution: distribution.map(Setting::Set).unwrap_or_default(),
            },
            super::EmbedderOptions::OpenAi(super::openai::EmbedderOptions {
                url,
                api_key,
                embedding_model,
                dimensions,
                distribution,
            }) => Self {
                source: Setting::Set(EmbedderSource::OpenAi),
                model: Setting::Set(embedding_model.name().to_owned()),
                revision: Setting::NotSet,
                api_key: api_key.map(Setting::Set).unwrap_or_default(),
                dimensions: dimensions.map(Setting::Set).unwrap_or_default(),
                document_template: Setting::Set(prompt.template),
                url: url.map(Setting::Set).unwrap_or_default(),
                request: Setting::NotSet,
                response: Setting::NotSet,
                distribution: distribution.map(Setting::Set).unwrap_or_default(),
            },
            super::EmbedderOptions::Ollama(super::ollama::EmbedderOptions {
                embedding_model,
                url,
                api_key,
                distribution,
            }) => Self {
                source: Setting::Set(EmbedderSource::Ollama),
                model: Setting::Set(embedding_model),
                revision: Setting::NotSet,
                api_key: api_key.map(Setting::Set).unwrap_or_default(),
                dimensions: Setting::NotSet,
                document_template: Setting::Set(prompt.template),
                url: url.map(Setting::Set).unwrap_or_default(),
                request: Setting::NotSet,
                response: Setting::NotSet,
                distribution: distribution.map(Setting::Set).unwrap_or_default(),
            },
            super::EmbedderOptions::UserProvided(super::manual::EmbedderOptions {
                dimensions,
                distribution,
            }) => Self {
                source: Setting::Set(EmbedderSource::UserProvided),
                model: Setting::NotSet,
                revision: Setting::NotSet,
                api_key: Setting::NotSet,
                dimensions: Setting::Set(dimensions),
                document_template: Setting::NotSet,
                url: Setting::NotSet,
                request: Setting::NotSet,
                response: Setting::NotSet,
                distribution: distribution.map(Setting::Set).unwrap_or_default(),
            },
            super::EmbedderOptions::Rest(super::rest::EmbedderOptions {
                api_key,
                dimensions,
                url,
                request,
                response,
                distribution,
            }) => Self {
                source: Setting::Set(EmbedderSource::Rest),
                model: Setting::NotSet,
                revision: Setting::NotSet,
                api_key: api_key.map(Setting::Set).unwrap_or_default(),
                dimensions: dimensions.map(Setting::Set).unwrap_or_default(),
                document_template: Setting::Set(prompt.template),
                url: Setting::Set(url),
                request: Setting::Set(request),
                response: Setting::Set(response),
                distribution: distribution.map(Setting::Set).unwrap_or_default(),
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
            request,
            response,
            distribution,
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
                    if let Some(url) = url.set() {
                        options.url = Some(url);
                    }
                    if let Some(api_key) = api_key.set() {
                        options.api_key = Some(api_key);
                    }
                    if let Some(dimensions) = dimensions.set() {
                        options.dimensions = Some(dimensions);
                    }
                    options.distribution = distribution.set();
                    this.embedder_options = super::EmbedderOptions::OpenAi(options);
                }
                EmbedderSource::Ollama => {
                    let mut options: ollama::EmbedderOptions =
                        super::ollama::EmbedderOptions::with_default_model(
                            api_key.set(),
                            url.set(),
                        );
                    if let Some(model) = model.set() {
                        options.embedding_model = model;
                    }

                    options.distribution = distribution.set();
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
                    options.distribution = distribution.set();
                    this.embedder_options = super::EmbedderOptions::HuggingFace(options);
                }
                EmbedderSource::UserProvided => {
                    this.embedder_options =
                        super::EmbedderOptions::UserProvided(super::manual::EmbedderOptions {
                            dimensions: dimensions.set().unwrap(),
                            distribution: distribution.set(),
                        });
                }
                EmbedderSource::Rest => {
                    this.embedder_options =
                        super::EmbedderOptions::Rest(super::rest::EmbedderOptions {
                            api_key: api_key.set(),
                            dimensions: dimensions.set(),
                            url: url.set().unwrap(),
                            request: request.set().unwrap(),
                            response: response.set().unwrap(),
                            distribution: distribution.set(),
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
