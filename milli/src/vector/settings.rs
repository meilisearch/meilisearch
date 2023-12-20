use deserr::Deserr;
use serde::{Deserialize, Serialize};

use crate::prompt::PromptData;
use crate::update::Setting;
use crate::vector::EmbeddingConfig;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct EmbeddingSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set", rename = "source")]
    #[deserr(default, rename = "source")]
    pub embedder_options: Setting<EmbedderSettings>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub document_template: Setting<PromptSettings>,
}

impl EmbeddingSettings {
    pub fn apply(&mut self, new: Self) {
        let EmbeddingSettings { embedder_options, document_template: prompt } = new;
        self.embedder_options.apply(embedder_options);
        self.document_template.apply(prompt);
    }
}

impl From<EmbeddingConfig> for EmbeddingSettings {
    fn from(value: EmbeddingConfig) -> Self {
        Self {
            embedder_options: Setting::Set(value.embedder_options.into()),
            document_template: Setting::Set(value.prompt.into()),
        }
    }
}

impl From<EmbeddingSettings> for EmbeddingConfig {
    fn from(value: EmbeddingSettings) -> Self {
        let mut this = Self::default();
        let EmbeddingSettings { embedder_options, document_template: prompt } = value;
        if let Some(embedder_options) = embedder_options.set() {
            this.embedder_options = embedder_options.into();
        }
        if let Some(prompt) = prompt.set() {
            this.prompt = prompt.into();
        }
        this
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct PromptSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub template: Setting<String>,
}

impl PromptSettings {
    pub fn apply(&mut self, new: Self) {
        let PromptSettings { template } = new;
        self.template.apply(template);
    }
}

impl From<PromptData> for PromptSettings {
    fn from(value: PromptData) -> Self {
        Self { template: Setting::Set(value.template) }
    }
}

impl From<PromptSettings> for PromptData {
    fn from(value: PromptSettings) -> Self {
        let mut this = PromptData::default();
        let PromptSettings { template } = value;
        if let Some(template) = template.set() {
            this.template = template;
        }
        this
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum EmbedderSettings {
    HuggingFace(Setting<HfEmbedderSettings>),
    OpenAi(Setting<OpenAiEmbedderSettings>),
    UserProvided(UserProvidedSettings),
}

impl<E> Deserr<E> for EmbedderSettings
where
    E: deserr::DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        match value {
            deserr::Value::Map(map) => {
                if deserr::Map::len(&map) != 1 {
                    return Err(deserr::take_cf_content(E::error::<V>(
                        None,
                        deserr::ErrorKind::Unexpected {
                            msg: format!(
                                "Expected a single field, got {} fields",
                                deserr::Map::len(&map)
                            ),
                        },
                        location,
                    )));
                }
                let mut it = deserr::Map::into_iter(map);
                let (k, v) = it.next().unwrap();

                match k.as_str() {
                    "huggingFace" => Ok(EmbedderSettings::HuggingFace(Setting::Set(
                        HfEmbedderSettings::deserialize_from_value(
                            v.into_value(),
                            location.push_key(&k),
                        )?,
                    ))),
                    "openAi" => Ok(EmbedderSettings::OpenAi(Setting::Set(
                        OpenAiEmbedderSettings::deserialize_from_value(
                            v.into_value(),
                            location.push_key(&k),
                        )?,
                    ))),
                    "userProvided" => Ok(EmbedderSettings::UserProvided(
                        UserProvidedSettings::deserialize_from_value(
                            v.into_value(),
                            location.push_key(&k),
                        )?,
                    )),
                    other => Err(deserr::take_cf_content(E::error::<V>(
                        None,
                        deserr::ErrorKind::UnknownKey {
                            key: other,
                            accepted: &["huggingFace", "openAi", "userProvided"],
                        },
                        location,
                    ))),
                }
            }
            _ => Err(deserr::take_cf_content(E::error::<V>(
                None,
                deserr::ErrorKind::IncorrectValueKind {
                    actual: value,
                    accepted: &[deserr::ValueKind::Map],
                },
                location,
            ))),
        }
    }
}

impl Default for EmbedderSettings {
    fn default() -> Self {
        Self::OpenAi(Default::default())
    }
}

impl From<crate::vector::EmbedderOptions> for EmbedderSettings {
    fn from(value: crate::vector::EmbedderOptions) -> Self {
        match value {
            crate::vector::EmbedderOptions::HuggingFace(hf) => {
                Self::HuggingFace(Setting::Set(hf.into()))
            }
            crate::vector::EmbedderOptions::OpenAi(openai) => {
                Self::OpenAi(Setting::Set(openai.into()))
            }
            crate::vector::EmbedderOptions::UserProvided(user_provided) => {
                Self::UserProvided(user_provided.into())
            }
        }
    }
}

impl From<EmbedderSettings> for crate::vector::EmbedderOptions {
    fn from(value: EmbedderSettings) -> Self {
        match value {
            EmbedderSettings::HuggingFace(Setting::Set(hf)) => Self::HuggingFace(hf.into()),
            EmbedderSettings::HuggingFace(_setting) => Self::HuggingFace(Default::default()),
            EmbedderSettings::OpenAi(Setting::Set(ai)) => Self::OpenAi(ai.into()),
            EmbedderSettings::OpenAi(_setting) => {
                Self::OpenAi(crate::vector::openai::EmbedderOptions::with_default_model(None))
            }
            EmbedderSettings::UserProvided(user_provided) => {
                Self::UserProvided(user_provided.into())
            }
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct HfEmbedderSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub model: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub revision: Setting<String>,
}

impl HfEmbedderSettings {
    pub fn apply(&mut self, new: Self) {
        let HfEmbedderSettings { model, revision } = new;
        self.model.apply(model);
        self.revision.apply(revision);
    }
}

impl From<crate::vector::hf::EmbedderOptions> for HfEmbedderSettings {
    fn from(value: crate::vector::hf::EmbedderOptions) -> Self {
        Self {
            model: Setting::Set(value.model),
            revision: value.revision.map(Setting::Set).unwrap_or(Setting::NotSet),
        }
    }
}

impl From<HfEmbedderSettings> for crate::vector::hf::EmbedderOptions {
    fn from(value: HfEmbedderSettings) -> Self {
        let HfEmbedderSettings { model, revision } = value;
        let mut this = Self::default();
        if let Some(model) = model.set() {
            this.model = model;
            // Reset the revision if we are setting the model.
            // This allows the following:
            // "huggingFace": {} -> default model with default revision
            // "huggingFace": { "model": "name-of-the-default-model" } -> default model without a revision
            // "huggingFace": { "model": "some-other-model" } -> most importantly, other model without a revision
            this.revision = None;
        }
        if let Some(revision) = revision.set() {
            this.revision = Some(revision);
        }
        this
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct OpenAiEmbedderSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub api_key: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set", rename = "model")]
    #[deserr(default, rename = "model")]
    pub embedding_model: Setting<crate::vector::openai::EmbeddingModel>,
}

impl OpenAiEmbedderSettings {
    pub fn apply(&mut self, new: Self) {
        let Self { api_key, embedding_model: embedding_mode } = new;
        self.api_key.apply(api_key);
        self.embedding_model.apply(embedding_mode);
    }
}

impl From<crate::vector::openai::EmbedderOptions> for OpenAiEmbedderSettings {
    fn from(value: crate::vector::openai::EmbedderOptions) -> Self {
        Self {
            api_key: value.api_key.map(Setting::Set).unwrap_or(Setting::Reset),
            embedding_model: Setting::Set(value.embedding_model),
        }
    }
}

impl From<OpenAiEmbedderSettings> for crate::vector::openai::EmbedderOptions {
    fn from(value: OpenAiEmbedderSettings) -> Self {
        let OpenAiEmbedderSettings { api_key, embedding_model } = value;
        Self { api_key: api_key.set(), embedding_model: embedding_model.set().unwrap_or_default() }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct UserProvidedSettings {
    pub dimensions: usize,
}

impl From<UserProvidedSettings> for crate::vector::manual::EmbedderOptions {
    fn from(value: UserProvidedSettings) -> Self {
        Self { dimensions: value.dimensions }
    }
}

impl From<crate::vector::manual::EmbedderOptions> for UserProvidedSettings {
    fn from(value: crate::vector::manual::EmbedderOptions) -> Self {
        Self { dimensions: value.dimensions }
    }
}
