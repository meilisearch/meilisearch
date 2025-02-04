use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use deserr::Deserr;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{ollama, openai, DistributionShift};
use crate::prompt::{default_max_bytes, PromptData};
use crate::update::Setting;
use crate::vector::EmbeddingConfig;
use crate::UserError;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct EmbeddingSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<EmbedderSource>)]
    pub source: Setting<EmbedderSource>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub model: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub revision: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub api_key: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub dimensions: Setting<usize>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<bool>)]
    pub binary_quantized: Setting<bool>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<bool>)]
    pub document_template: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>)]
    pub document_template_max_bytes: Setting<usize>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub url: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<serde_json::Value>)]
    pub request: Setting<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<serde_json::Value>)]
    pub response: Setting<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<BTreeMap<String, String>>)]
    pub headers: Setting<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<DistributionShift>)]
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
    Reindex { action: ReindexAction, updated_settings: EmbeddingSettings, quantize: bool },
    UpdateWithoutReindex { updated_settings: EmbeddingSettings, quantize: bool },
}

#[derive(Default, Debug)]
pub struct EmbedderAction {
    pub was_quantized: bool,
    pub is_being_quantized: bool,
    pub write_back: Option<WriteBackToDocuments>,
    pub reindex: Option<ReindexAction>,
}

impl EmbedderAction {
    pub fn is_being_quantized(&self) -> bool {
        self.is_being_quantized
    }

    pub fn write_back(&self) -> Option<&WriteBackToDocuments> {
        self.write_back.as_ref()
    }

    pub fn reindex(&self) -> Option<&ReindexAction> {
        self.reindex.as_ref()
    }

    pub fn with_is_being_quantized(mut self, quantize: bool) -> Self {
        self.is_being_quantized = quantize;
        self
    }

    pub fn with_write_back(write_back: WriteBackToDocuments, was_quantized: bool) -> Self {
        Self {
            was_quantized,
            is_being_quantized: false,
            write_back: Some(write_back),
            reindex: None,
        }
    }

    pub fn with_reindex(reindex: ReindexAction, was_quantized: bool) -> Self {
        Self { was_quantized, is_being_quantized: false, write_back: None, reindex: Some(reindex) }
    }
}

#[derive(Debug)]
pub struct WriteBackToDocuments {
    pub embedder_id: u8,
    pub user_provided: RoaringBitmap,
}

impl SettingsDiff {
    pub fn from_settings(
        embedder_name: &str,
        old: EmbeddingSettings,
        new: Setting<EmbeddingSettings>,
    ) -> Result<Self, UserError> {
        let ret = match new {
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
                    mut headers,
                    mut document_template_max_bytes,
                    binary_quantized: mut binary_quantize,
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
                    headers: new_headers,
                    document_template_max_bytes: new_document_template_max_bytes,
                    binary_quantized: new_binary_quantize,
                } = new;

                if matches!(binary_quantize, Setting::Set(true))
                    && matches!(new_binary_quantize, Setting::Set(false))
                {
                    return Err(UserError::InvalidDisableBinaryQuantization {
                        embedder_name: embedder_name.to_string(),
                    });
                }

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
                        &mut document_template_max_bytes,
                        &mut headers,
                    )
                }
                if model.apply(new_model) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if revision.apply(new_revision) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if dimensions.apply(new_dimensions) {
                    match source {
                        // regenerate on dimensions change in OpenAI since truncation is supported
                        Setting::Set(EmbedderSource::OpenAi) | Setting::Reset => {
                            ReindexAction::push_action(
                                &mut reindex_action,
                                ReindexAction::FullReindex,
                            );
                        }
                        // for all other embedders, the parameter is a hint that should not be able to change the result
                        // and so won't cause a reindex by itself.
                        _ => {}
                    }
                }
                let binary_quantize_changed = binary_quantize.apply(new_binary_quantize);
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

                if document_template_max_bytes.apply(new_document_template_max_bytes) {
                    let previous_document_template_max_bytes =
                        document_template_max_bytes.set().unwrap_or(default_max_bytes().get());
                    let new_document_template_max_bytes =
                        new_document_template_max_bytes.set().unwrap_or(default_max_bytes().get());

                    // only reindex if the size increased. Reasoning:
                    // - size decrease is a performance optimization, so we don't reindex and we keep the more accurate vectors
                    // - size increase is an accuracy optimization, so we want to reindex
                    if new_document_template_max_bytes > previous_document_template_max_bytes {
                        ReindexAction::push_action(
                            &mut reindex_action,
                            ReindexAction::RegeneratePrompts,
                        )
                    }
                }

                distribution.apply(new_distribution);
                api_key.apply(new_api_key);
                headers.apply(new_headers);

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
                    headers,
                    document_template_max_bytes,
                    binary_quantized: binary_quantize,
                };

                match reindex_action {
                    Some(action) => Self::Reindex {
                        action,
                        updated_settings,
                        quantize: binary_quantize_changed,
                    },
                    None => Self::UpdateWithoutReindex {
                        updated_settings,
                        quantize: binary_quantize_changed,
                    },
                }
            }
            Setting::Reset => Self::Remove,
            Setting::NotSet => {
                Self::UpdateWithoutReindex { updated_settings: old, quantize: false }
            }
        };
        Ok(ret)
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
    document_template_max_bytes: &mut Setting<usize>,
    headers: &mut Setting<BTreeMap<String, String>>,
) {
    match source {
        Setting::Set(EmbedderSource::HuggingFace) => {
            *model = Setting::Reset;
            *revision = Setting::Reset;
            *dimensions = Setting::NotSet;
            *url = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
            *headers = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::Ollama) => {
            *model = Setting::Reset;
            *revision = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
            *headers = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::OpenAi) | Setting::Reset => {
            *model = Setting::Reset;
            *revision = Setting::NotSet;
            *dimensions = Setting::NotSet;
            *url = Setting::Reset;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
            *headers = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::Rest) => {
            *model = Setting::NotSet;
            *revision = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::Reset;
            *request = Setting::Reset;
            *response = Setting::Reset;
            *headers = Setting::Reset;
        }
        Setting::Set(EmbedderSource::UserProvided) => {
            *model = Setting::NotSet;
            *revision = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
            *document_template = Setting::NotSet;
            *document_template_max_bytes = Setting::NotSet;
            *headers = Setting::NotSet;
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
    pub const DOCUMENT_TEMPLATE_MAX_BYTES: &'static str = "documentTemplateMaxBytes";

    pub const URL: &'static str = "url";
    pub const REQUEST: &'static str = "request";
    pub const RESPONSE: &'static str = "response";
    pub const HEADERS: &'static str = "headers";

    pub const DISTRIBUTION: &'static str = "distribution";

    pub const BINARY_QUANTIZED: &'static str = "binaryQuantized";

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
            Self::DIMENSIONS => &[
                EmbedderSource::OpenAi,
                EmbedderSource::UserProvided,
                EmbedderSource::Ollama,
                EmbedderSource::Rest,
            ],
            Self::DOCUMENT_TEMPLATE | Self::DOCUMENT_TEMPLATE_MAX_BYTES => &[
                EmbedderSource::HuggingFace,
                EmbedderSource::OpenAi,
                EmbedderSource::Ollama,
                EmbedderSource::Rest,
            ],
            Self::URL => &[EmbedderSource::Ollama, EmbedderSource::Rest, EmbedderSource::OpenAi],
            Self::REQUEST => &[EmbedderSource::Rest],
            Self::RESPONSE => &[EmbedderSource::Rest],
            Self::HEADERS => &[EmbedderSource::Rest],
            Self::DISTRIBUTION => &[
                EmbedderSource::HuggingFace,
                EmbedderSource::Ollama,
                EmbedderSource::OpenAi,
                EmbedderSource::Rest,
                EmbedderSource::UserProvided,
            ],
            Self::BINARY_QUANTIZED => &[
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
                Self::DOCUMENT_TEMPLATE_MAX_BYTES,
                Self::DIMENSIONS,
                Self::DISTRIBUTION,
                Self::URL,
                Self::BINARY_QUANTIZED,
            ],
            EmbedderSource::HuggingFace => &[
                Self::SOURCE,
                Self::MODEL,
                Self::REVISION,
                Self::DOCUMENT_TEMPLATE,
                Self::DOCUMENT_TEMPLATE_MAX_BYTES,
                Self::DISTRIBUTION,
                Self::BINARY_QUANTIZED,
            ],
            EmbedderSource::Ollama => &[
                Self::SOURCE,
                Self::MODEL,
                Self::DOCUMENT_TEMPLATE,
                Self::DOCUMENT_TEMPLATE_MAX_BYTES,
                Self::URL,
                Self::API_KEY,
                Self::DIMENSIONS,
                Self::DISTRIBUTION,
                Self::BINARY_QUANTIZED,
            ],
            EmbedderSource::UserProvided => {
                &[Self::SOURCE, Self::DIMENSIONS, Self::DISTRIBUTION, Self::BINARY_QUANTIZED]
            }
            EmbedderSource::Rest => &[
                Self::SOURCE,
                Self::API_KEY,
                Self::DIMENSIONS,
                Self::DOCUMENT_TEMPLATE,
                Self::DOCUMENT_TEMPLATE_MAX_BYTES,
                Self::URL,
                Self::REQUEST,
                Self::RESPONSE,
                Self::HEADERS,
                Self::DISTRIBUTION,
                Self::BINARY_QUANTIZED,
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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
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
        let EmbeddingConfig { embedder_options, prompt, quantized } = value;
        let document_template_max_bytes =
            Setting::Set(prompt.max_bytes.unwrap_or(default_max_bytes()).get());
        match embedder_options {
            super::EmbedderOptions::HuggingFace(super::hf::EmbedderOptions {
                model,
                revision,
                distribution,
            }) => Self {
                source: Setting::Set(EmbedderSource::HuggingFace),
                model: Setting::Set(model),
                revision: Setting::some_or_not_set(revision),
                api_key: Setting::NotSet,
                dimensions: Setting::NotSet,
                document_template: Setting::Set(prompt.template),
                document_template_max_bytes,
                url: Setting::NotSet,
                request: Setting::NotSet,
                response: Setting::NotSet,
                headers: Setting::NotSet,
                distribution: Setting::some_or_not_set(distribution),
                binary_quantized: Setting::some_or_not_set(quantized),
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
                api_key: Setting::some_or_not_set(api_key),
                dimensions: Setting::some_or_not_set(dimensions),
                document_template: Setting::Set(prompt.template),
                document_template_max_bytes,
                url: Setting::some_or_not_set(url),
                request: Setting::NotSet,
                response: Setting::NotSet,
                headers: Setting::NotSet,
                distribution: Setting::some_or_not_set(distribution),
                binary_quantized: Setting::some_or_not_set(quantized),
            },
            super::EmbedderOptions::Ollama(super::ollama::EmbedderOptions {
                embedding_model,
                url,
                api_key,
                distribution,
                dimensions,
            }) => Self {
                source: Setting::Set(EmbedderSource::Ollama),
                model: Setting::Set(embedding_model),
                revision: Setting::NotSet,
                api_key: Setting::some_or_not_set(api_key),
                dimensions: Setting::some_or_not_set(dimensions),
                document_template: Setting::Set(prompt.template),
                document_template_max_bytes,
                url: Setting::some_or_not_set(url),
                request: Setting::NotSet,
                response: Setting::NotSet,
                headers: Setting::NotSet,
                distribution: Setting::some_or_not_set(distribution),
                binary_quantized: Setting::some_or_not_set(quantized),
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
                document_template_max_bytes: Setting::NotSet,
                url: Setting::NotSet,
                request: Setting::NotSet,
                response: Setting::NotSet,
                headers: Setting::NotSet,
                distribution: Setting::some_or_not_set(distribution),
                binary_quantized: Setting::some_or_not_set(quantized),
            },
            super::EmbedderOptions::Rest(super::rest::EmbedderOptions {
                api_key,
                dimensions,
                url,
                request,
                response,
                distribution,
                headers,
            }) => Self {
                source: Setting::Set(EmbedderSource::Rest),
                model: Setting::NotSet,
                revision: Setting::NotSet,
                api_key: Setting::some_or_not_set(api_key),
                dimensions: Setting::some_or_not_set(dimensions),
                document_template: Setting::Set(prompt.template),
                document_template_max_bytes,
                url: Setting::Set(url),
                request: Setting::Set(request),
                response: Setting::Set(response),
                distribution: Setting::some_or_not_set(distribution),
                headers: Setting::Set(headers),
                binary_quantized: Setting::some_or_not_set(quantized),
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
            document_template_max_bytes,
            url,
            request,
            response,
            distribution,
            headers,
            binary_quantized,
        } = value;

        this.quantized = binary_quantized.set();

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
                            dimensions.set(),
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
                            headers: headers.set().unwrap_or_default(),
                        })
                }
            }
        }

        if let Setting::Set(template) = document_template {
            let max_bytes = document_template_max_bytes
                .set()
                .and_then(NonZeroUsize::new)
                .unwrap_or(default_max_bytes());

            this.prompt = PromptData { template, max_bytes: Some(max_bytes) }
        }

        this
    }
}
