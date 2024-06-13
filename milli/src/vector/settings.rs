use deserr::Deserr;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use super::rest::InputType;
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
                    mut query,
                    mut input_field,
                    mut path_to_embeddings,
                    mut embedding_object,
                    mut input_type,
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
                    query: new_query,
                    input_field: new_input_field,
                    path_to_embeddings: new_path_to_embeddings,
                    embedding_object: new_embedding_object,
                    input_type: new_input_type,
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
                        &mut query,
                        &mut input_field,
                        &mut path_to_embeddings,
                        &mut embedding_object,
                        &mut input_type,
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
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if query.apply(new_query) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if input_field.apply(new_input_field) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if path_to_embeddings.apply(new_path_to_embeddings) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if embedding_object.apply(new_embedding_object) {
                    ReindexAction::push_action(&mut reindex_action, ReindexAction::FullReindex);
                }
                if input_type.apply(new_input_type) {
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
                    query,
                    input_field,
                    path_to_embeddings,
                    embedding_object,
                    input_type,
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
    query: &mut Setting<serde_json::Value>,
    input_field: &mut Setting<Vec<String>>,
    path_to_embeddings: &mut Setting<Vec<String>>,
    embedding_object: &mut Setting<Vec<String>>,
    input_type: &mut Setting<InputType>,
    document_template: &mut Setting<String>,
) {
    match source {
        Setting::Set(EmbedderSource::HuggingFace) => {
            *model = Setting::Reset;
            *revision = Setting::Reset;
            *dimensions = Setting::NotSet;
            *url = Setting::NotSet;
            *query = Setting::NotSet;
            *input_field = Setting::NotSet;
            *path_to_embeddings = Setting::NotSet;
            *embedding_object = Setting::NotSet;
            *input_type = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::Ollama) => {
            *model = Setting::Reset;
            *revision = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::NotSet;
            *query = Setting::NotSet;
            *input_field = Setting::NotSet;
            *path_to_embeddings = Setting::NotSet;
            *embedding_object = Setting::NotSet;
            *input_type = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::OpenAi) | Setting::Reset => {
            *model = Setting::Reset;
            *revision = Setting::NotSet;
            *dimensions = Setting::NotSet;
            *url = Setting::NotSet;
            *query = Setting::NotSet;
            *input_field = Setting::NotSet;
            *path_to_embeddings = Setting::NotSet;
            *embedding_object = Setting::NotSet;
            *input_type = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::Rest) => {
            *model = Setting::NotSet;
            *revision = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::Reset;
            *query = Setting::Reset;
            *input_field = Setting::Reset;
            *path_to_embeddings = Setting::Reset;
            *embedding_object = Setting::Reset;
            *input_type = Setting::Reset;
        }
        Setting::Set(EmbedderSource::UserProvided) => {
            *model = Setting::NotSet;
            *revision = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::NotSet;
            *query = Setting::NotSet;
            *input_field = Setting::NotSet;
            *path_to_embeddings = Setting::NotSet;
            *embedding_object = Setting::NotSet;
            *input_type = Setting::NotSet;
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
    pub const QUERY: &'static str = "query";
    pub const INPUT_FIELD: &'static str = "inputField";
    pub const PATH_TO_EMBEDDINGS: &'static str = "pathToEmbeddings";
    pub const EMBEDDING_OBJECT: &'static str = "embeddingObject";
    pub const INPUT_TYPE: &'static str = "inputType";

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
            Self::URL => &[EmbedderSource::Ollama, EmbedderSource::Rest],
            Self::QUERY => &[EmbedderSource::Rest],
            Self::INPUT_FIELD => &[EmbedderSource::Rest],
            Self::PATH_TO_EMBEDDINGS => &[EmbedderSource::Rest],
            Self::EMBEDDING_OBJECT => &[EmbedderSource::Rest],
            Self::INPUT_TYPE => &[EmbedderSource::Rest],
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
                Self::QUERY,
                Self::INPUT_FIELD,
                Self::PATH_TO_EMBEDDINGS,
                Self::EMBEDDING_OBJECT,
                Self::INPUT_TYPE,
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
                query: Setting::NotSet,
                input_field: Setting::NotSet,
                path_to_embeddings: Setting::NotSet,
                embedding_object: Setting::NotSet,
                input_type: Setting::NotSet,
                distribution: distribution.map(Setting::Set).unwrap_or_default(),
            },
            super::EmbedderOptions::OpenAi(super::openai::EmbedderOptions {
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
                url: Setting::NotSet,
                query: Setting::NotSet,
                input_field: Setting::NotSet,
                path_to_embeddings: Setting::NotSet,
                embedding_object: Setting::NotSet,
                input_type: Setting::NotSet,
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
                query: Setting::NotSet,
                input_field: Setting::NotSet,
                path_to_embeddings: Setting::NotSet,
                embedding_object: Setting::NotSet,
                input_type: Setting::NotSet,
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
                query: Setting::NotSet,
                input_field: Setting::NotSet,
                path_to_embeddings: Setting::NotSet,
                embedding_object: Setting::NotSet,
                input_type: Setting::NotSet,
                distribution: distribution.map(Setting::Set).unwrap_or_default(),
            },
            super::EmbedderOptions::Rest(super::rest::EmbedderOptions {
                api_key,
                dimensions,
                url,
                query,
                input_field,
                path_to_embeddings,
                embedding_object,
                input_type,
                distribution,
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
            query,
            input_field,
            path_to_embeddings,
            embedding_object,
            input_type,
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
                    let embedder_options = super::rest::EmbedderOptions::default();

                    this.embedder_options =
                        super::EmbedderOptions::Rest(super::rest::EmbedderOptions {
                            api_key: api_key.set(),
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
