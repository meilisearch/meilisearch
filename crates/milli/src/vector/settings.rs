use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use deserr::Deserr;
use either::Either;
use itertools::Itertools;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::composite::SubEmbedderOptions;
use super::hf::OverridePooling;
use super::{ollama, openai, DistributionShift, EmbedderOptions};
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
    /// The source used to provide the embeddings.
    ///
    /// Which embedder parameters are available and mandatory is determined by the value of this setting.
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings.
    ///
    /// # Defaults
    ///
    /// - Defaults to `openAi`
    pub source: Setting<EmbedderSource>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    /// The name of the model to use.
    ///
    /// # Mandatory
    ///
    /// - This parameter is mandatory for source `ollama`
    ///
    /// # Availability
    ///
    /// - This parameter is available for sources `openAi`, `huggingFace`, `ollama`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings.
    ///
    /// # Defaults
    ///
    /// - For source `openAi`, defaults to `text-embedding-3-small`
    /// - For source `huggingFace`, defaults to `BAAI/bge-base-en-v1.5`
    pub model: Setting<String>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    /// The revision (commit SHA1) of the model to use.
    ///
    /// If unspecified, Meilisearch picks the latest revision of the model.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `huggingFace`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings
    ///
    /// # Defaults
    ///
    /// - When `model` is set to default, defaults to `617ca489d9e86b49b8167676d8220688b99db36e`
    /// - Otherwise, defaults to `null`
    pub revision: Setting<String>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<OverridePooling>)]
    /// The pooling method to use.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `huggingFace`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings
    ///
    /// # Defaults
    ///
    /// - Defaults to `useModel`
    ///
    /// # Compatibility Note
    ///
    /// - Embedders created before this parameter was available default to `forceMean` to preserve the existing behavior.
    pub pooling: Setting<OverridePooling>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    /// The API key to pass to the remote embedder while making requests.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `openAi`, `ollama`, `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🌱 Changing the value of this parameter never regenerates embeddings
    ///
    /// # Defaults
    ///
    /// - For source `openAi`, the key is read from `OPENAI_API_KEY`, then `MEILI_OPENAI_API_KEY`.
    /// - For other sources, no bearer token is sent if this parameter is not set.
    ///
    /// # Note
    ///
    /// - This setting is partially hidden when returned by the settings
    pub api_key: Setting<String>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    /// The expected dimensions of the embeddings produced by this embedder.
    ///
    /// # Mandatory
    ///
    /// - This parameter is mandatory for source `userProvided`
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `openAi`, `ollama`, `rest`, `userProvided`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ When the source is `openAi`, changing the value of this parameter always regenerates embeddings
    /// - 🌱 For other sources, changing the value of this parameter never regenerates embeddings
    ///
    /// # Defaults
    ///
    /// - For source `openAi`, the dimensions is the maximum allowed by the model.
    /// - For sources `ollama` and `rest`, the dimensions are inferred by embedding a sample text.
    pub dimensions: Setting<usize>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<bool>)]
    /// Whether to binary quantize the embeddings of this embedder.
    ///
    /// Binary quantized embeddings are smaller than regular embeddings, which improves
    /// disk usage and retrieval speed, at the cost of relevancy.
    ///
    /// # Availability
    ///
    /// - This parameter is available for all embedders
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ When set to `true`, embeddings are not regenerated, but they are binary quantized, which takes time.
    ///
    /// # Defaults
    ///
    /// - Defaults to `false`
    ///
    /// # Note
    ///
    /// As binary quantization is a destructive operation, it is not possible to disable again this setting after
    /// first enabling it. If you are unsure of whether the performance-relevancy tradeoff is right for you,
    /// we recommend to use this parameter on a test index first.
    pub binary_quantized: Setting<bool>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<bool>)]
    /// A liquid template used to render documents to a text that can be embedded.
    ///
    /// Meillisearch interpolates the template for each document and sends the resulting text to the embedder.
    /// The embedder then generates document vectors based on this text.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `openAi`, `huggingFace`, `ollama` and `rest
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ When modified, embeddings are regenerated for documents whose rendering through the template produces a different text.
    pub document_template: Setting<String>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>)]
    /// Rendered texts are truncated to this size.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `openAi`, `huggingFace`, `ollama` and `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ When increased, embeddings are regenerated for documents whose rendering through the template produces a different text.
    /// - 🌱 When decreased, embeddings are never regenerated
    ///
    /// # Default
    ///
    /// - Defaults to 400
    pub document_template_max_bytes: Setting<usize>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    /// URL to reach the remote embedder.
    ///
    /// # Mandatory
    ///
    /// - This parameter is mandatory for source `rest`
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `openAi`, `ollama` and `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🌱 When modified for source `openAi`, embeddings are never regenerated
    /// - 🏗️ When modified for sources `ollama` and `rest`, embeddings are always regenerated
    pub url: Setting<String>,

    /// Template fragments that will be reassembled and sent to the remote embedder at indexing time.
    ///
    /// # Availability
    ///
    /// - This parameter is available for sources `rest`.
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ When a fragment is deleted by passing `null` to its name, the corresponding embeddings are removed from documents.
    /// - 🏗️ When a fragment is modified, the corresponding embeddings are regenerated if their rendered version changes.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<BTreeMap<String, serde_json::Value>>)]
    pub indexing_fragments: Setting<BTreeMap<String, Option<Fragment>>>,

    /// Template fragments that will be reassembled and sent to the remote embedder at search time.
    ///
    /// # Availability
    ///
    /// - This parameter is available for sources `rest`.
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🌱 Changing the value of this parameter never regenerates embeddings
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<BTreeMap<String, serde_json::Value>>)]
    pub search_fragments: Setting<BTreeMap<String, Option<Fragment>>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<serde_json::Value>)]
    /// Template request to send to the remote embedder.
    ///
    /// # Mandatory
    ///
    /// - This parameter is mandatory for source `rest`
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings
    pub request: Setting<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<serde_json::Value>)]
    /// Template response indicating how to find the embeddings in the response from the remote embedder.
    ///
    /// # Mandatory
    ///
    /// - This parameter is mandatory for source `rest`
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings
    pub response: Setting<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<BTreeMap<String, String>>)]
    /// Additional headers to send to the remote embedder.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🌱 Changing the value of this parameter never regenerates embeddings
    pub headers: Setting<BTreeMap<String, String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<SubEmbeddingSettings>)]
    pub search_embedder: Setting<SubEmbeddingSettings>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<SubEmbeddingSettings>)]
    pub indexing_embedder: Setting<SubEmbeddingSettings>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<DistributionShift>)]
    /// Affine transformation applied to the semantic score to make it more comparable to the ranking score.
    ///
    /// # Availability
    ///
    /// - This parameter is available for all embedders
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🌱 Changing the value of this parameter never regenerates embeddings
    pub distribution: Setting<DistributionShift>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct SubEmbeddingSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<EmbedderSource>)]
    /// The source used to provide the embeddings.
    ///
    /// Which embedder parameters are available and mandatory is determined by the value of this setting.
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings.
    ///
    /// # Defaults
    ///
    /// - Defaults to `openAi`
    pub source: Setting<EmbedderSource>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    /// The name of the model to use.
    ///
    /// # Mandatory
    ///
    /// - This parameter is mandatory for source `ollama`
    ///
    /// # Availability
    ///
    /// - This parameter is available for sources `openAi`, `huggingFace`, `ollama`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings.
    ///
    /// # Defaults
    ///
    /// - For source `openAi`, defaults to `text-embedding-3-small`
    /// - For source `huggingFace`, defaults to `BAAI/bge-base-en-v1.5`
    pub model: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    /// The revision (commit SHA1) of the model to use.
    ///
    /// If unspecified, Meilisearch picks the latest revision of the model.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `huggingFace`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings
    ///
    /// # Defaults
    ///
    /// - When `model` is set to default, defaults to `617ca489d9e86b49b8167676d8220688b99db36e`
    /// - Otherwise, defaults to `null`
    pub revision: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<OverridePooling>)]
    /// The pooling method to use.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `huggingFace`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings
    ///
    /// # Defaults
    ///
    /// - Defaults to `useModel`
    ///
    /// # Compatibility Note
    ///
    /// - Embedders created before this parameter was available default to `forceMean` to preserve the existing behavior.
    pub pooling: Setting<OverridePooling>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    /// The API key to pass to the remote embedder while making requests.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `openAi`, `ollama`, `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🌱 Changing the value of this parameter never regenerates embeddings
    ///
    /// # Defaults
    ///
    /// - For source `openAi`, the key is read from `OPENAI_API_KEY`, then `MEILI_OPENAI_API_KEY`.
    /// - For other sources, no bearer token is sent if this parameter is not set.
    ///
    /// # Note
    ///
    /// - This setting is partially hidden when returned by the settings
    pub api_key: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    /// The expected dimensions of the embeddings produced by this embedder.
    ///
    /// # Mandatory
    ///
    /// - This parameter is mandatory for source `userProvided`
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `openAi`, `ollama`, `rest`, `userProvided`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ When the source is `openAi`, changing the value of this parameter always regenerates embeddings
    /// - 🌱 For other sources, changing the value of this parameter never regenerates embeddings
    ///
    /// # Defaults
    ///
    /// - For source `openAi`, the dimensions is the maximum allowed by the model.
    /// - For sources `ollama` and `rest`, the dimensions are inferred by embedding a sample text.
    pub dimensions: Setting<usize>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<bool>)]
    /// A liquid template used to render documents to a text that can be embedded.
    ///
    /// Meillisearch interpolates the template for each document and sends the resulting text to the embedder.
    /// The embedder then generates document vectors based on this text.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `openAi`, `huggingFace`, `ollama` and `rest
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ When modified, embeddings are regenerated for documents whose rendering through the template produces a different text.
    pub document_template: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>)]
    /// Rendered texts are truncated to this size.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `openAi`, `huggingFace`, `ollama` and `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ When increased, embeddings are regenerated for documents whose rendering through the template produces a different text.
    /// - 🌱 When decreased, embeddings are never regenerated
    ///
    /// # Default
    ///
    /// - Defaults to 400
    pub document_template_max_bytes: Setting<usize>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    /// URL to reach the remote embedder.
    ///
    /// # Mandatory
    ///
    /// - This parameter is mandatory for source `rest`
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `openAi`, `ollama` and `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🌱 When modified for source `openAi`, embeddings are never regenerated
    /// - 🏗️ When modified for sources `ollama` and `rest`, embeddings are always regenerated
    pub url: Setting<String>,

    /// Template fragments that will be reassembled and sent to the remote embedder at indexing time.
    ///
    /// # Availability
    ///
    /// - This parameter is available for sources `rest`.
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ When a fragment is deleted by passing `null` to its name, the corresponding embeddings are removed from documents.
    /// - 🏗️ When a fragment is modified, the corresponding embeddings are regenerated if their rendered version changes.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<BTreeMap<String, serde_json::Value>>)]
    pub indexing_fragments: Setting<BTreeMap<String, Option<Fragment>>>,

    /// Template fragments that will be reassembled and sent to the remote embedder at search time.
    ///
    /// # Availability
    ///
    /// - This parameter is available for sources `rest`.
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🌱 Changing the value of this parameter never regenerates embeddings
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<BTreeMap<String, serde_json::Value>>)]
    pub search_fragments: Setting<BTreeMap<String, Option<Fragment>>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<serde_json::Value>)]
    /// Template request to send to the remote embedder.
    ///
    /// # Mandatory
    ///
    /// - This parameter is mandatory for source `rest`
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings
    pub request: Setting<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<serde_json::Value>)]
    /// Template response indicating how to find the embeddings in the response from the remote embedder.
    ///
    /// # Mandatory
    ///
    /// - This parameter is mandatory for source `rest`
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🏗️ Changing the value of this parameter always regenerates embeddings
    pub response: Setting<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<BTreeMap<String, String>>)]
    /// Additional headers to send to the remote embedder.
    ///
    /// # Availability
    ///
    /// - This parameter is available for source `rest`
    ///
    /// # 🔄 Reindexing
    ///
    /// - 🌱 Changing the value of this parameter never regenerates embeddings
    pub headers: Setting<BTreeMap<String, String>>,

    // The following fields are provided for the sake of improving error handling
    // They should always be set to `NotSet`, otherwise an error will be returned
    #[serde(default, skip_serializing)]
    #[deserr(default)]
    #[schema(ignore)]
    pub distribution: Setting<DistributionShift>,

    #[serde(default, skip_serializing)]
    #[deserr(default)]
    #[schema(ignore)]
    pub binary_quantized: Setting<bool>,

    #[serde(default, skip_serializing)]
    #[deserr(default)]
    #[schema(ignore)]
    pub search_embedder: Setting<serde_json::Value>,

    #[serde(default, skip_serializing)]
    #[deserr(default)]
    #[schema(ignore)]
    pub indexing_embedder: Setting<serde_json::Value>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum EmbeddingValidationContext {
    FullSettings,
    SettingsPartialUpdate,
}

/// Indicates what action should take place during a reindexing operation for an embedder
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReindexAction {
    /// An indexing operation should take place for this embedder, keeping existing vectors
    /// and checking whether the document template changed or not
    RegeneratePrompts,
    RegenerateFragments(Vec<(String, RegenerateFragment)>),
    /// An indexing operation should take place for all documents for this embedder, removing existing vectors
    /// (except userProvided ones)
    FullReindex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegenerateFragment {
    Update,
    Remove,
    Add,
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
    pub remove_fragments: Option<RemoveFragments>,
}

#[derive(Debug)]
pub struct RemoveFragments {
    pub fragment_ids: Vec<u8>,
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

    pub fn remove_fragments(&self) -> Option<&RemoveFragments> {
        self.remove_fragments.as_ref()
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
            remove_fragments: None,
        }
    }

    pub fn with_reindex(reindex: ReindexAction, was_quantized: bool) -> Self {
        Self {
            was_quantized,
            is_being_quantized: false,
            write_back: None,
            reindex: Some(reindex),
            remove_fragments: None,
        }
    }

    pub fn with_remove_fragments(mut self, remove_fragments: RemoveFragments) -> Self {
        self.remove_fragments = Some(remove_fragments);
        self
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
                    mut pooling,
                    mut api_key,
                    mut dimensions,
                    mut document_template,
                    mut url,
                    mut indexing_fragments,
                    mut search_fragments,
                    mut request,
                    mut response,
                    mut search_embedder,
                    mut indexing_embedder,
                    mut distribution,
                    mut headers,
                    mut document_template_max_bytes,
                    binary_quantized: mut binary_quantize,
                } = old;

                let EmbeddingSettings {
                    source: new_source,
                    model: new_model,
                    revision: new_revision,
                    pooling: new_pooling,
                    api_key: new_api_key,
                    dimensions: new_dimensions,
                    document_template: new_document_template,
                    url: new_url,
                    indexing_fragments: new_indexing_fragments,
                    search_fragments: new_search_fragments,
                    request: new_request,
                    response: new_response,
                    search_embedder: new_search_embedder,
                    indexing_embedder: new_indexing_embedder,
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

                Self::apply_and_diff(
                    &mut reindex_action,
                    &mut source,
                    &mut model,
                    &mut revision,
                    &mut pooling,
                    &mut api_key,
                    &mut dimensions,
                    &mut document_template,
                    &mut document_template_max_bytes,
                    &mut url,
                    &mut indexing_fragments,
                    &mut search_fragments,
                    &mut request,
                    &mut response,
                    &mut headers,
                    new_source,
                    new_model,
                    new_revision,
                    new_pooling,
                    new_api_key,
                    new_dimensions,
                    new_document_template,
                    new_document_template_max_bytes,
                    new_url,
                    new_indexing_fragments,
                    new_search_fragments,
                    new_request,
                    new_response,
                    new_headers,
                );

                let binary_quantize_changed = binary_quantize.apply(new_binary_quantize);

                // changes to the *search* embedder never triggers any reindexing
                search_embedder.apply(new_search_embedder);
                indexing_embedder = Self::from_sub_settings(
                    indexing_embedder,
                    new_indexing_embedder,
                    &mut reindex_action,
                )?;

                distribution.apply(new_distribution);

                let updated_settings = EmbeddingSettings {
                    source,
                    model,
                    revision,
                    pooling,
                    api_key,
                    dimensions,
                    document_template,
                    url,
                    indexing_fragments,
                    search_fragments,
                    request,
                    response,
                    search_embedder,
                    indexing_embedder,
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

    fn from_sub_settings(
        sub_embedder: Setting<SubEmbeddingSettings>,
        new_sub_embedder: Setting<SubEmbeddingSettings>,
        reindex_action: &mut Option<ReindexAction>,
    ) -> Result<Setting<SubEmbeddingSettings>, UserError> {
        let ret = match new_sub_embedder {
            Setting::Set(new_sub_embedder) => {
                let Setting::Set(SubEmbeddingSettings {
                    mut source,
                    mut model,
                    mut revision,
                    mut pooling,
                    mut api_key,
                    mut dimensions,
                    mut document_template,
                    mut document_template_max_bytes,
                    mut url,
                    mut indexing_fragments,
                    mut search_fragments,
                    mut request,
                    mut response,
                    mut headers,
                    // phony settings
                    mut distribution,
                    mut binary_quantized,
                    mut search_embedder,
                    mut indexing_embedder,
                }) = sub_embedder
                else {
                    // return the new_indexing_embedder if the indexing_embedder was not set
                    // this should happen only when changing the source, so the decision to reindex is already taken.
                    return Ok(Setting::Set(new_sub_embedder));
                };

                let SubEmbeddingSettings {
                    source: new_source,
                    model: new_model,
                    revision: new_revision,
                    pooling: new_pooling,
                    api_key: new_api_key,
                    dimensions: new_dimensions,
                    document_template: new_document_template,
                    document_template_max_bytes: new_document_template_max_bytes,
                    url: new_url,
                    indexing_fragments: new_indexing_fragments,
                    search_fragments: new_search_fragments,
                    request: new_request,
                    response: new_response,
                    headers: new_headers,
                    distribution: new_distribution,
                    binary_quantized: new_binary_quantized,
                    search_embedder: new_search_embedder,
                    indexing_embedder: new_indexing_embedder,
                } = new_sub_embedder;

                Self::apply_and_diff(
                    reindex_action,
                    &mut source,
                    &mut model,
                    &mut revision,
                    &mut pooling,
                    &mut api_key,
                    &mut dimensions,
                    &mut document_template,
                    &mut document_template_max_bytes,
                    &mut url,
                    &mut indexing_fragments,
                    &mut search_fragments,
                    &mut request,
                    &mut response,
                    &mut headers,
                    new_source,
                    new_model,
                    new_revision,
                    new_pooling,
                    new_api_key,
                    new_dimensions,
                    new_document_template,
                    new_document_template_max_bytes,
                    new_url,
                    new_indexing_fragments,
                    new_search_fragments,
                    new_request,
                    new_response,
                    new_headers,
                );

                // update phony settings, it is always an error to have them set.
                distribution.apply(new_distribution);
                binary_quantized.apply(new_binary_quantized);
                search_embedder.apply(new_search_embedder);
                indexing_embedder.apply(new_indexing_embedder);

                let updated_settings = SubEmbeddingSettings {
                    source,
                    model,
                    revision,
                    pooling,
                    api_key,
                    dimensions,
                    document_template,
                    url,
                    indexing_fragments,
                    search_fragments,
                    request,
                    response,
                    headers,
                    document_template_max_bytes,
                    distribution,
                    binary_quantized,
                    search_embedder,
                    indexing_embedder,
                };
                Setting::Set(updated_settings)
            }
            // handled during validation of the settings
            Setting::Reset | Setting::NotSet => sub_embedder,
        };
        Ok(ret)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_and_diff(
        reindex_action: &mut Option<ReindexAction>,
        source: &mut Setting<EmbedderSource>,
        model: &mut Setting<String>,
        revision: &mut Setting<String>,
        pooling: &mut Setting<OverridePooling>,
        api_key: &mut Setting<String>,
        dimensions: &mut Setting<usize>,
        document_template: &mut Setting<String>,
        document_template_max_bytes: &mut Setting<usize>,
        url: &mut Setting<String>,
        indexing_fragments: &mut Setting<BTreeMap<String, Option<Fragment>>>,
        search_fragments: &mut Setting<BTreeMap<String, Option<Fragment>>>,
        request: &mut Setting<serde_json::Value>,
        response: &mut Setting<serde_json::Value>,
        headers: &mut Setting<BTreeMap<String, String>>,
        new_source: Setting<EmbedderSource>,
        new_model: Setting<String>,
        new_revision: Setting<String>,
        new_pooling: Setting<OverridePooling>,
        new_api_key: Setting<String>,
        new_dimensions: Setting<usize>,
        new_document_template: Setting<String>,
        new_document_template_max_bytes: Setting<usize>,
        new_url: Setting<String>,
        new_indexing_fragments: Setting<BTreeMap<String, Option<Fragment>>>,
        new_search_fragments: Setting<BTreeMap<String, Option<Fragment>>>,
        new_request: Setting<serde_json::Value>,
        new_response: Setting<serde_json::Value>,
        new_headers: Setting<BTreeMap<String, String>>,
    ) {
        // **Warning**: do not use short-circuiting || here, we want all these operations applied
        if source.apply(new_source) {
            ReindexAction::push_action(reindex_action, ReindexAction::FullReindex);
            // when the source changes, we need to reapply the default settings for the new source
            apply_default_for_source(
                &*source,
                model,
                revision,
                pooling,
                dimensions,
                url,
                indexing_fragments,
                search_fragments,
                request,
                response,
                document_template,
                document_template_max_bytes,
                headers,
                // send dummy values, the source cannot recursively be composite
                &mut Setting::NotSet,
                &mut Setting::NotSet,
            )
        }
        if model.apply(new_model) {
            ReindexAction::push_action(reindex_action, ReindexAction::FullReindex);
        }
        if revision.apply(new_revision) {
            ReindexAction::push_action(reindex_action, ReindexAction::FullReindex);
        }
        if pooling.apply(new_pooling) {
            ReindexAction::push_action(reindex_action, ReindexAction::FullReindex);
        }
        if dimensions.apply(new_dimensions) {
            match *source {
                // regenerate on dimensions change in OpenAI since truncation is supported
                Setting::Set(EmbedderSource::OpenAi) | Setting::Reset => {
                    ReindexAction::push_action(reindex_action, ReindexAction::FullReindex);
                }
                // for all other embedders, the parameter is a hint that should not be able to change the result
                // and so won't cause a reindex by itself.
                _ => {}
            }
        }
        if url.apply(new_url) {
            match *source {
                // do not regenerate on an url change in OpenAI
                Setting::Set(EmbedderSource::OpenAi) | Setting::Reset => {}
                _ => {
                    ReindexAction::push_action(reindex_action, ReindexAction::FullReindex);
                }
            }
        }

        *search_fragments = match (std::mem::take(search_fragments), new_search_fragments) {
            (Setting::Set(search_fragments), Setting::Set(new_search_fragments)) => {
                Setting::Set(
                    search_fragments
                        .into_iter()
                        .merge_join_by(new_search_fragments, |(left, _), (right, _)| {
                            left.cmp(right)
                        })
                        .map(|eob| {
                            match eob {
                                // merge fragments
                                itertools::EitherOrBoth::Both((name, _), (_, right)) => {
                                    (name, right)
                                }
                                // unchanged fragment
                                itertools::EitherOrBoth::Left(left) => left,
                                // new fragment
                                itertools::EitherOrBoth::Right(right) => right,
                            }
                        })
                        .collect(),
                )
            }
            (_, Setting::Reset) => Setting::Reset,
            (left, Setting::NotSet) => left,
            (Setting::NotSet | Setting::Reset, Setting::Set(new_search_fragments)) => {
                Setting::Set(new_search_fragments)
            }
        };

        let mut regenerate_fragments = Vec::new();
        *indexing_fragments = match (std::mem::take(indexing_fragments), new_indexing_fragments) {
            (Setting::Set(fragments), Setting::Set(new_fragments)) => {
                Setting::Set(
                    fragments
                        .into_iter()
                        .merge_join_by(new_fragments, |(left, _), (right, _)| left.cmp(right))
                        .map(|eob| {
                            match eob {
                                // merge fragments
                                itertools::EitherOrBoth::Both(
                                    (name, left),
                                    (other_name, right),
                                ) => {
                                    if left == right {
                                        (name, left)
                                    } else {
                                        match right {
                                            Some(right) => {
                                                regenerate_fragments
                                                    .push((other_name, RegenerateFragment::Update));
                                                (name, Some(right))
                                            }
                                            None => {
                                                regenerate_fragments
                                                    .push((other_name, RegenerateFragment::Remove));
                                                (name, None)
                                            }
                                        }
                                    }
                                }
                                // unchanged fragment
                                itertools::EitherOrBoth::Left(left) => left,
                                // new fragment
                                itertools::EitherOrBoth::Right((name, right)) => {
                                    if right.is_some() {
                                        regenerate_fragments
                                            .push((name.clone(), RegenerateFragment::Add));
                                    }
                                    (name, right)
                                }
                            }
                        })
                        .collect(),
                )
            }
            // remove all fragments => move to document template
            (_, Setting::Reset) => {
                ReindexAction::push_action(reindex_action, ReindexAction::FullReindex);
                Setting::Reset
            }
            // add all fragments
            (Setting::NotSet | Setting::Reset, Setting::Set(new_fragments)) => {
                ReindexAction::push_action(reindex_action, ReindexAction::FullReindex);

                Setting::Set(new_fragments)
            }
            // no change
            (left, Setting::NotSet) => left,
        };
        if !regenerate_fragments.is_empty() {
            regenerate_fragments.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            ReindexAction::push_action(
                reindex_action,
                ReindexAction::RegenerateFragments(regenerate_fragments),
            );
        }

        if request.apply(new_request) {
            ReindexAction::push_action(reindex_action, ReindexAction::FullReindex);
        }
        if response.apply(new_response) {
            ReindexAction::push_action(reindex_action, ReindexAction::FullReindex);
        }
        if document_template.apply(new_document_template) {
            ReindexAction::push_action(reindex_action, ReindexAction::RegeneratePrompts);
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
                ReindexAction::push_action(reindex_action, ReindexAction::RegeneratePrompts)
            }
        }

        api_key.apply(new_api_key);
        headers.apply(new_headers);
    }
}

impl ReindexAction {
    fn push_action(this: &mut Option<Self>, other: Self) {
        use ReindexAction::*;
        *this = match (this.take(), other) {
            (_, FullReindex) => Some(FullReindex),
            (Some(FullReindex), _) => Some(FullReindex),
            (_, RegenerateFragments(fragments)) => Some(RegenerateFragments(fragments)),
            (Some(RegenerateFragments(fragments)), RegeneratePrompts) => {
                Some(RegenerateFragments(fragments))
            }
            (Some(RegeneratePrompts), RegeneratePrompts) => Some(RegeneratePrompts),
            (None, RegeneratePrompts) => Some(RegeneratePrompts),
        }
    }
}

#[allow(clippy::too_many_arguments)] // private function
fn apply_default_for_source(
    source: &Setting<EmbedderSource>,
    model: &mut Setting<String>,
    revision: &mut Setting<String>,
    pooling: &mut Setting<OverridePooling>,
    dimensions: &mut Setting<usize>,
    url: &mut Setting<String>,
    indexing_fragments: &mut Setting<BTreeMap<String, Option<Fragment>>>,
    search_fragments: &mut Setting<BTreeMap<String, Option<Fragment>>>,
    request: &mut Setting<serde_json::Value>,
    response: &mut Setting<serde_json::Value>,
    document_template: &mut Setting<String>,
    document_template_max_bytes: &mut Setting<usize>,
    headers: &mut Setting<BTreeMap<String, String>>,
    search_embedder: &mut Setting<SubEmbeddingSettings>,
    indexing_embedder: &mut Setting<SubEmbeddingSettings>,
) {
    match source {
        Setting::Set(EmbedderSource::HuggingFace) => {
            *model = Setting::Reset;
            *revision = Setting::Reset;
            *pooling = Setting::Reset;
            *dimensions = Setting::NotSet;
            *url = Setting::NotSet;
            *indexing_fragments = Setting::NotSet;
            *search_fragments = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
            *headers = Setting::NotSet;
            *search_embedder = Setting::NotSet;
            *indexing_embedder = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::Ollama) => {
            *model = Setting::Reset;
            *revision = Setting::NotSet;
            *pooling = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::NotSet;
            *indexing_fragments = Setting::NotSet;
            *search_fragments = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
            *headers = Setting::NotSet;
            *search_embedder = Setting::NotSet;
            *indexing_embedder = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::OpenAi) | Setting::Reset => {
            *model = Setting::Reset;
            *revision = Setting::NotSet;
            *pooling = Setting::NotSet;
            *dimensions = Setting::NotSet;
            *url = Setting::Reset;
            *indexing_fragments = Setting::NotSet;
            *search_fragments = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
            *headers = Setting::NotSet;
            *search_embedder = Setting::NotSet;
            *indexing_embedder = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::Rest) => {
            *model = Setting::NotSet;
            *revision = Setting::NotSet;
            *pooling = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::Reset;
            *indexing_fragments = Setting::Reset;
            *search_fragments = Setting::Reset;
            *request = Setting::Reset;
            *response = Setting::Reset;
            *headers = Setting::Reset;
            *search_embedder = Setting::NotSet;
            *indexing_embedder = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::UserProvided) => {
            *model = Setting::NotSet;
            *revision = Setting::NotSet;
            *pooling = Setting::NotSet;
            *dimensions = Setting::Reset;
            *url = Setting::NotSet;
            *indexing_fragments = Setting::NotSet;
            *search_fragments = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
            *document_template = Setting::NotSet;
            *document_template_max_bytes = Setting::NotSet;
            *headers = Setting::NotSet;
            *search_embedder = Setting::NotSet;
            *indexing_embedder = Setting::NotSet;
        }
        Setting::Set(EmbedderSource::Composite) => {
            *model = Setting::NotSet;
            *revision = Setting::NotSet;
            *pooling = Setting::NotSet;
            *dimensions = Setting::NotSet;
            *url = Setting::NotSet;
            *indexing_fragments = Setting::NotSet;
            *search_fragments = Setting::NotSet;
            *request = Setting::NotSet;
            *response = Setting::NotSet;
            *document_template = Setting::NotSet;
            *document_template_max_bytes = Setting::NotSet;
            *headers = Setting::NotSet;
            *search_embedder = Setting::Reset;
            *indexing_embedder = Setting::Reset;
        }
        Setting::NotSet => {}
    }
}

pub(crate) enum FieldStatus {
    Mandatory,
    Allowed,
    Disallowed,
}

#[derive(Debug, Clone, Copy)]
pub enum NestingContext {
    NotNested,
    Search,
    Indexing,
}

impl NestingContext {
    pub fn embedder_name_with_context(&self, embedder_name: &str) -> String {
        match self {
            NestingContext::NotNested => embedder_name.to_string(),
            NestingContext::Search => format!("{embedder_name}.searchEmbedder"),
            NestingContext::Indexing => format!("{embedder_name}.indexingEmbedder",),
        }
    }

    pub fn in_context(&self) -> &'static str {
        match self {
            NestingContext::NotNested => "",
            NestingContext::Search => " for the search embedder",
            NestingContext::Indexing => " for the indexing embedder",
        }
    }

    pub fn nesting_embedders(&self) -> &'static str {
        match self {
            NestingContext::NotNested => "",
            NestingContext::Search => {
                "\n  - note: nesting embedders in `searchEmbedder` is not allowed"
            }
            NestingContext::Indexing => {
                "\n  - note: nesting embedders in `indexingEmbedder` is not allowed"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, enum_iterator::Sequence)]
pub enum MetaEmbeddingSetting {
    Source,
    Model,
    Revision,
    Pooling,
    ApiKey,
    Dimensions,
    DocumentTemplate,
    DocumentTemplateMaxBytes,
    Url,
    IndexingFragments,
    SearchFragments,
    Request,
    Response,
    Headers,
    SearchEmbedder,
    IndexingEmbedder,
    Distribution,
    BinaryQuantized,
}

impl MetaEmbeddingSetting {
    pub(crate) fn name(&self) -> &'static str {
        use MetaEmbeddingSetting::*;
        match self {
            Source => "source",
            Model => "model",
            Revision => "revision",
            Pooling => "pooling",
            ApiKey => "apiKey",
            Dimensions => "dimensions",
            DocumentTemplate => "documentTemplate",
            DocumentTemplateMaxBytes => "documentTemplateMaxBytes",
            Url => "url",
            IndexingFragments => "indexingFragments",
            SearchFragments => "searchFragments",
            Request => "request",
            Response => "response",
            Headers => "headers",
            SearchEmbedder => "searchEmbedder",
            IndexingEmbedder => "indexingEmbedder",
            Distribution => "distribution",
            BinaryQuantized => "binaryQuantized",
        }
    }
}

impl EmbeddingSettings {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn check_settings(
        embedder_name: &str,
        source: EmbedderSource,
        context: NestingContext,
        model: &Setting<String>,
        revision: &Setting<String>,
        pooling: &Setting<OverridePooling>,
        dimensions: &Setting<usize>,
        api_key: &Setting<String>,
        url: &Setting<String>,
        indexing_fragments: &Setting<BTreeMap<String, Option<Fragment>>>,
        search_fragments: &Setting<BTreeMap<String, Option<Fragment>>>,
        request: &Setting<serde_json::Value>,
        response: &Setting<serde_json::Value>,
        document_template: &Setting<String>,
        document_template_max_bytes: &Setting<usize>,
        headers: &Setting<BTreeMap<String, String>>,
        search_embedder: &Setting<SubEmbeddingSettings>,
        indexing_embedder: &Setting<SubEmbeddingSettings>,
        binary_quantized: &Setting<bool>,
        distribution: &Setting<DistributionShift>,
    ) -> Result<(), UserError> {
        Self::check_setting(embedder_name, source, MetaEmbeddingSetting::Model, context, model)?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::Revision,
            context,
            revision,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::Pooling,
            context,
            pooling,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::Dimensions,
            context,
            dimensions,
        )?;
        Self::check_setting(embedder_name, source, MetaEmbeddingSetting::ApiKey, context, api_key)?;
        Self::check_setting(embedder_name, source, MetaEmbeddingSetting::Url, context, url)?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::IndexingFragments,
            context,
            indexing_fragments,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::SearchFragments,
            context,
            search_fragments,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::Request,
            context,
            request,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::Response,
            context,
            response,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::DocumentTemplate,
            context,
            document_template,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::DocumentTemplateMaxBytes,
            context,
            document_template_max_bytes,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::Headers,
            context,
            headers,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::SearchEmbedder,
            context,
            search_embedder,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::IndexingEmbedder,
            context,
            indexing_embedder,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::BinaryQuantized,
            context,
            binary_quantized,
        )?;
        Self::check_setting(
            embedder_name,
            source,
            MetaEmbeddingSetting::Distribution,
            context,
            distribution,
        )
    }

    pub(crate) fn allowed_sources_for_field(
        field: MetaEmbeddingSetting,
        context: NestingContext,
    ) -> Vec<EmbedderSource> {
        enum_iterator::all()
            .filter(|source| {
                !matches!(Self::field_status(*source, field, context), FieldStatus::Disallowed)
            })
            .collect()
    }

    pub(crate) fn allowed_fields_for_source(
        source: EmbedderSource,
        context: NestingContext,
    ) -> Vec<&'static str> {
        enum_iterator::all()
            .filter(|field| {
                !matches!(Self::field_status(source, *field, context), FieldStatus::Disallowed)
            })
            .map(|field| field.name())
            .collect()
    }

    fn check_setting<T>(
        embedder_name: &str,
        source: EmbedderSource,
        field: MetaEmbeddingSetting,
        context: NestingContext,
        setting: &Setting<T>,
    ) -> Result<(), UserError> {
        match (Self::field_status(source, field, context), setting) {
            (FieldStatus::Mandatory, Setting::Set(_))
            | (FieldStatus::Allowed, _)
            | (FieldStatus::Disallowed, Setting::NotSet) => Ok(()),
            (FieldStatus::Disallowed, _) => Err(UserError::InvalidFieldForSource {
                embedder_name: context.embedder_name_with_context(embedder_name),
                source_: source,
                context,
                field,
            }),
            (FieldStatus::Mandatory, _) => Err(UserError::MissingFieldForSource {
                field: field.name(),
                source_: source,
                embedder_name: embedder_name.to_owned(),
            }),
        }
    }

    pub(crate) fn field_status(
        source: EmbedderSource,
        field: MetaEmbeddingSetting,
        context: NestingContext,
    ) -> FieldStatus {
        use EmbedderSource::*;
        use MetaEmbeddingSetting::*;
        use NestingContext::*;
        match (source, field, context) {
            (_, Distribution | BinaryQuantized, NotNested) => FieldStatus::Allowed,
            (_, Distribution | BinaryQuantized, _) => FieldStatus::Disallowed,
            (_, DocumentTemplate | DocumentTemplateMaxBytes, Search) => FieldStatus::Disallowed,
            (
                OpenAi,
                Source
                | Model
                | ApiKey
                | DocumentTemplate
                | DocumentTemplateMaxBytes
                | Dimensions
                | Url,
                _,
            ) => FieldStatus::Allowed,
            (
                OpenAi,
                Revision | Pooling | IndexingFragments | SearchFragments | Request | Response
                | Headers | SearchEmbedder | IndexingEmbedder,
                _,
            ) => FieldStatus::Disallowed,
            (
                HuggingFace,
                Source | Model | Revision | Pooling | DocumentTemplate | DocumentTemplateMaxBytes,
                _,
            ) => FieldStatus::Allowed,
            (
                HuggingFace,
                ApiKey | Dimensions | Url | IndexingFragments | SearchFragments | Request
                | Response | Headers | SearchEmbedder | IndexingEmbedder,
                _,
            ) => FieldStatus::Disallowed,
            (Ollama, Model, _) => FieldStatus::Mandatory,
            (
                Ollama,
                Source | DocumentTemplate | DocumentTemplateMaxBytes | Url | ApiKey | Dimensions,
                _,
            ) => FieldStatus::Allowed,
            (
                Ollama,
                Revision | Pooling | IndexingFragments | SearchFragments | Request | Response
                | Headers | SearchEmbedder | IndexingEmbedder,
                _,
            ) => FieldStatus::Disallowed,
            (UserProvided, Dimensions, _) => FieldStatus::Mandatory,
            (UserProvided, Source, _) => FieldStatus::Allowed,
            (
                UserProvided,
                Model
                | Revision
                | Pooling
                | ApiKey
                | DocumentTemplate
                | DocumentTemplateMaxBytes
                | Url
                | IndexingFragments
                | SearchFragments
                | Request
                | Response
                | Headers
                | SearchEmbedder
                | IndexingEmbedder,
                _,
            ) => FieldStatus::Disallowed,
            (Rest, Url | Request | Response, _) => FieldStatus::Mandatory,
            (
                Rest,
                Source
                | ApiKey
                | Dimensions
                | DocumentTemplate
                | DocumentTemplateMaxBytes
                | Headers,
                _,
            ) => FieldStatus::Allowed,
            (Rest, IndexingFragments, NotNested | Indexing) => FieldStatus::Allowed,
            (Rest, IndexingFragments, Search) => FieldStatus::Disallowed,
            (Rest, SearchFragments, NotNested | Search) => FieldStatus::Allowed,
            (Rest, SearchFragments, Indexing) => FieldStatus::Disallowed,
            (Rest, Model | Revision | Pooling | SearchEmbedder | IndexingEmbedder, _) => {
                FieldStatus::Disallowed
            }
            (Composite, SearchEmbedder | IndexingEmbedder, _) => FieldStatus::Mandatory,
            (Composite, Source, _) => FieldStatus::Allowed,
            (
                Composite,
                Model
                | Revision
                | Pooling
                | ApiKey
                | Dimensions
                | DocumentTemplate
                | DocumentTemplateMaxBytes
                | Url
                | IndexingFragments
                | SearchFragments
                | Request
                | Response
                | Headers,
                _,
            ) => FieldStatus::Disallowed,
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

    pub(crate) fn check_nested_source(
        embedder_name: &str,
        source: EmbedderSource,
        context: NestingContext,
    ) -> Result<(), UserError> {
        match (context, source) {
            (NestingContext::NotNested, _) => Ok(()),
            (
                NestingContext::Search | NestingContext::Indexing,
                EmbedderSource::Composite | EmbedderSource::UserProvided,
            ) => Err(UserError::InvalidSourceForNested {
                embedder_name: context.embedder_name_with_context(embedder_name),
                source_: source,
            }),
            (
                NestingContext::Search | NestingContext::Indexing,
                EmbedderSource::OpenAi
                | EmbedderSource::HuggingFace
                | EmbedderSource::Ollama
                | EmbedderSource::Rest,
            ) => Ok(()),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Deserr,
    ToSchema,
    enum_iterator::Sequence,
)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub enum EmbedderSource {
    #[default]
    OpenAi,
    HuggingFace,
    Ollama,
    UserProvided,
    Rest,
    Composite,
}

impl std::fmt::Display for EmbedderSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            EmbedderSource::OpenAi => "openAi",
            EmbedderSource::HuggingFace => "huggingFace",
            EmbedderSource::UserProvided => "userProvided",
            EmbedderSource::Ollama => "ollama",
            EmbedderSource::Rest => "rest",
            EmbedderSource::Composite => "composite",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
pub struct Fragment {
    pub value: serde_json::Value,
}

impl EmbeddingSettings {
    fn from_hugging_face(
        super::hf::EmbedderOptions {
        model,
        revision,
        distribution,
        pooling,
    }: super::hf::EmbedderOptions,
        document_template: Setting<String>,
        document_template_max_bytes: Setting<usize>,
        quantized: Option<bool>,
    ) -> Self {
        Self {
            source: Setting::Set(EmbedderSource::HuggingFace),
            model: Setting::Set(model),
            revision: Setting::some_or_not_set(revision),
            pooling: Setting::Set(pooling),
            api_key: Setting::NotSet,
            dimensions: Setting::NotSet,
            document_template,
            document_template_max_bytes,
            url: Setting::NotSet,
            indexing_fragments: Setting::NotSet,
            search_fragments: Setting::NotSet,
            request: Setting::NotSet,
            response: Setting::NotSet,
            headers: Setting::NotSet,
            search_embedder: Setting::NotSet,
            indexing_embedder: Setting::NotSet,
            distribution: Setting::some_or_not_set(distribution),
            binary_quantized: Setting::some_or_not_set(quantized),
        }
    }

    fn from_openai(
        super::openai::EmbedderOptions {
            url,
            api_key,
            embedding_model,
            dimensions,
            distribution,
        }: super::openai::EmbedderOptions,
        document_template: Setting<String>,
        document_template_max_bytes: Setting<usize>,
        quantized: Option<bool>,
    ) -> Self {
        Self {
            source: Setting::Set(EmbedderSource::OpenAi),
            model: Setting::Set(embedding_model.name().to_owned()),
            revision: Setting::NotSet,
            pooling: Setting::NotSet,
            api_key: Setting::some_or_not_set(api_key),
            dimensions: Setting::some_or_not_set(dimensions),
            document_template,
            document_template_max_bytes,
            url: Setting::some_or_not_set(url),
            indexing_fragments: Setting::NotSet,
            search_fragments: Setting::NotSet,
            request: Setting::NotSet,
            response: Setting::NotSet,
            headers: Setting::NotSet,
            search_embedder: Setting::NotSet,
            indexing_embedder: Setting::NotSet,
            distribution: Setting::some_or_not_set(distribution),
            binary_quantized: Setting::some_or_not_set(quantized),
        }
    }

    fn from_ollama(
        super::ollama::EmbedderOptions {
          embedding_model,
          url,
          api_key,
          distribution,
          dimensions,
        }: super::ollama::EmbedderOptions,
        document_template: Setting<String>,
        document_template_max_bytes: Setting<usize>,
        quantized: Option<bool>,
    ) -> Self {
        Self {
            source: Setting::Set(EmbedderSource::Ollama),
            model: Setting::Set(embedding_model),
            revision: Setting::NotSet,
            pooling: Setting::NotSet,
            api_key: Setting::some_or_not_set(api_key),
            dimensions: Setting::some_or_not_set(dimensions),
            document_template,
            document_template_max_bytes,
            url: Setting::some_or_not_set(url),
            indexing_fragments: Setting::NotSet,
            search_fragments: Setting::NotSet,
            request: Setting::NotSet,
            response: Setting::NotSet,
            headers: Setting::NotSet,
            search_embedder: Setting::NotSet,
            indexing_embedder: Setting::NotSet,
            distribution: Setting::some_or_not_set(distribution),
            binary_quantized: Setting::some_or_not_set(quantized),
        }
    }

    fn from_user_provided(
        super::manual::EmbedderOptions { dimensions, distribution }: super::manual::EmbedderOptions,
        quantized: Option<bool>,
    ) -> Self {
        Self {
            source: Setting::Set(EmbedderSource::UserProvided),
            model: Setting::NotSet,
            revision: Setting::NotSet,
            pooling: Setting::NotSet,
            api_key: Setting::NotSet,
            dimensions: Setting::Set(dimensions),
            document_template: Setting::NotSet,
            document_template_max_bytes: Setting::NotSet,
            url: Setting::NotSet,
            indexing_fragments: Setting::NotSet,
            search_fragments: Setting::NotSet,
            request: Setting::NotSet,
            response: Setting::NotSet,
            headers: Setting::NotSet,
            search_embedder: Setting::NotSet,
            indexing_embedder: Setting::NotSet,
            distribution: Setting::some_or_not_set(distribution),
            binary_quantized: Setting::some_or_not_set(quantized),
        }
    }

    fn from_rest(
        super::rest::EmbedderOptions {
            api_key,
            dimensions,
            url,
            request,
            indexing_fragments,
            search_fragments,
            response,
            distribution,
            headers,
        }: super::rest::EmbedderOptions,
        document_template: Setting<String>,
        document_template_max_bytes: Setting<usize>,
        quantized: Option<bool>,
    ) -> Self {
        Self {
            source: Setting::Set(EmbedderSource::Rest),
            model: Setting::NotSet,
            revision: Setting::NotSet,
            pooling: Setting::NotSet,
            api_key: Setting::some_or_not_set(api_key),
            dimensions: Setting::some_or_not_set(dimensions),
            document_template: if indexing_fragments.is_empty() && search_fragments.is_empty() {
                document_template
            } else {
                Setting::NotSet
            },
            document_template_max_bytes: if indexing_fragments.is_empty()
                && search_fragments.is_empty()
            {
                document_template_max_bytes
            } else {
                Setting::NotSet
            },
            url: Setting::Set(url),
            indexing_fragments: if indexing_fragments.is_empty() {
                Setting::NotSet
            } else {
                Setting::Set(
                    indexing_fragments
                        .into_iter()
                        .map(|(name, fragment)| (name, Some(Fragment { value: fragment })))
                        .collect(),
                )
            },
            search_fragments: if search_fragments.is_empty() {
                Setting::NotSet
            } else {
                Setting::Set(
                    search_fragments
                        .into_iter()
                        .map(|(name, fragment)| (name, Some(Fragment { value: fragment })))
                        .collect(),
                )
            },
            request: Setting::Set(request),
            response: Setting::Set(response),
            distribution: Setting::some_or_not_set(distribution),
            headers: Setting::Set(headers),
            search_embedder: Setting::NotSet,
            indexing_embedder: Setting::NotSet,
            binary_quantized: Setting::some_or_not_set(quantized),
        }
    }
}

impl From<EmbeddingConfig> for EmbeddingSettings {
    fn from(value: EmbeddingConfig) -> Self {
        let EmbeddingConfig { embedder_options, prompt, quantized } = value;
        let document_template_max_bytes =
            Setting::Set(prompt.max_bytes.unwrap_or(default_max_bytes()).get());
        match embedder_options {
            super::EmbedderOptions::HuggingFace(options) => Self::from_hugging_face(
                options,
                Setting::Set(prompt.template),
                document_template_max_bytes,
                quantized,
            ),
            super::EmbedderOptions::OpenAi(options) => Self::from_openai(
                options,
                Setting::Set(prompt.template),
                document_template_max_bytes,
                quantized,
            ),
            super::EmbedderOptions::Ollama(options) => Self::from_ollama(
                options,
                Setting::Set(prompt.template),
                document_template_max_bytes,
                quantized,
            ),
            super::EmbedderOptions::UserProvided(options) => {
                Self::from_user_provided(options, quantized)
            }
            super::EmbedderOptions::Rest(options) => Self::from_rest(
                options,
                Setting::Set(prompt.template),
                document_template_max_bytes,
                quantized,
            ),
            super::EmbedderOptions::Composite(super::composite::EmbedderOptions {
                search,
                index,
            }) => Self {
                source: Setting::Set(EmbedderSource::Composite),
                model: Setting::NotSet,
                revision: Setting::NotSet,
                pooling: Setting::NotSet,
                api_key: Setting::NotSet,
                dimensions: Setting::NotSet,
                binary_quantized: Setting::some_or_not_set(quantized),
                document_template: Setting::NotSet,
                document_template_max_bytes: Setting::NotSet,
                url: Setting::NotSet,
                indexing_fragments: Setting::NotSet,
                search_fragments: Setting::NotSet,
                request: Setting::NotSet,
                response: Setting::NotSet,
                headers: Setting::NotSet,
                distribution: Setting::some_or_not_set(search.distribution()),
                search_embedder: Setting::Set(SubEmbeddingSettings::from_options(
                    search,
                    Setting::NotSet,
                    Setting::NotSet,
                )),
                indexing_embedder: Setting::Set(SubEmbeddingSettings::from_options(
                    index,
                    Setting::Set(prompt.template),
                    document_template_max_bytes,
                )),
            },
        }
    }
}

impl SubEmbeddingSettings {
    fn from_options(
        options: SubEmbedderOptions,
        document_template: Setting<String>,
        document_template_max_bytes: Setting<usize>,
    ) -> Self {
        let settings = match options {
            SubEmbedderOptions::HuggingFace(embedder_options) => {
                EmbeddingSettings::from_hugging_face(
                    embedder_options,
                    document_template,
                    document_template_max_bytes,
                    None,
                )
            }
            SubEmbedderOptions::OpenAi(embedder_options) => EmbeddingSettings::from_openai(
                embedder_options,
                document_template,
                document_template_max_bytes,
                None,
            ),
            SubEmbedderOptions::Ollama(embedder_options) => EmbeddingSettings::from_ollama(
                embedder_options,
                document_template,
                document_template_max_bytes,
                None,
            ),
            SubEmbedderOptions::UserProvided(embedder_options) => {
                EmbeddingSettings::from_user_provided(embedder_options, None)
            }
            SubEmbedderOptions::Rest(embedder_options) => EmbeddingSettings::from_rest(
                embedder_options,
                document_template,
                document_template_max_bytes,
                None,
            ),
        };
        settings.into()
    }
}

impl From<EmbeddingSettings> for SubEmbeddingSettings {
    fn from(value: EmbeddingSettings) -> Self {
        let EmbeddingSettings {
            source,
            model,
            revision,
            pooling,
            api_key,
            dimensions,
            document_template,
            document_template_max_bytes,
            url,
            indexing_fragments,
            search_fragments,
            request,
            response,
            headers,
            binary_quantized: _,
            search_embedder: _,
            indexing_embedder: _,
            distribution: _,
        } = value;
        Self {
            source,
            model,
            revision,
            pooling,
            api_key,
            dimensions,
            document_template,
            document_template_max_bytes,
            url,
            indexing_fragments,
            search_fragments,
            request,
            response,
            headers,
            distribution: Setting::NotSet,
            binary_quantized: Setting::NotSet,
            search_embedder: Setting::NotSet,
            indexing_embedder: Setting::NotSet,
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
            pooling,
            api_key,
            dimensions,
            document_template,
            document_template_max_bytes,
            url,
            indexing_fragments,
            search_fragments,
            request,
            response,
            distribution,
            headers,
            binary_quantized,
            search_embedder,
            mut indexing_embedder,
        } = value;

        this.quantized = binary_quantized.set();
        if let Some((template, document_template_max_bytes)) =
            match (document_template, &mut indexing_embedder) {
                (Setting::Set(template), _) => Some((template, document_template_max_bytes)),
                // retrieve the prompt from the indexing embedder in case of a composite embedder
                (
                    _,
                    Setting::Set(SubEmbeddingSettings {
                        document_template: Setting::Set(document_template),
                        document_template_max_bytes,
                        ..
                    }),
                ) => Some((std::mem::take(document_template), *document_template_max_bytes)),
                _ => None,
            }
        {
            let max_bytes = document_template_max_bytes
                .set()
                .and_then(NonZeroUsize::new)
                .unwrap_or(default_max_bytes());

            this.prompt = PromptData { template, max_bytes: Some(max_bytes) }
        }

        if let Some(source) = source.set() {
            this.embedder_options = match source {
                EmbedderSource::OpenAi => {
                    SubEmbedderOptions::openai(model, url, api_key, dimensions, distribution).into()
                }
                EmbedderSource::Ollama => {
                    SubEmbedderOptions::ollama(model, url, api_key, dimensions, distribution).into()
                }
                EmbedderSource::HuggingFace => {
                    SubEmbedderOptions::hugging_face(model, revision, pooling, distribution).into()
                }
                EmbedderSource::UserProvided => {
                    SubEmbedderOptions::user_provided(dimensions.set().unwrap(), distribution)
                        .into()
                }
                EmbedderSource::Rest => SubEmbedderOptions::rest(
                    url.set().unwrap(),
                    api_key,
                    indexing_fragments,
                    search_fragments,
                    request.set().unwrap(),
                    response.set().unwrap(),
                    headers,
                    dimensions,
                    distribution,
                )
                .into(),
                EmbedderSource::Composite => {
                    super::EmbedderOptions::Composite(super::composite::EmbedderOptions {
                        // it is important to give the distribution to the search here, as this is from where we'll retrieve it
                        search: SubEmbedderOptions::from_settings(
                            search_embedder.set().unwrap(),
                            distribution,
                        ),
                        index: SubEmbedderOptions::from_settings(
                            indexing_embedder.set().unwrap(),
                            Setting::NotSet,
                        ),
                    })
                }
            };
        }

        this
    }
}

impl SubEmbedderOptions {
    fn from_settings(
        settings: SubEmbeddingSettings,
        distribution: Setting<DistributionShift>,
    ) -> Self {
        let SubEmbeddingSettings {
            source,
            model,
            revision,
            pooling,
            api_key,
            dimensions,
            // retrieved by the EmbeddingConfig
            document_template: _,
            document_template_max_bytes: _,
            url,
            indexing_fragments,
            search_fragments,
            request,
            response,
            headers,
            // phony parameters
            distribution: _,
            binary_quantized: _,
            search_embedder: _,
            indexing_embedder: _,
        } = settings;

        match source.set().unwrap() {
            EmbedderSource::OpenAi => Self::openai(model, url, api_key, dimensions, distribution),
            EmbedderSource::HuggingFace => {
                Self::hugging_face(model, revision, pooling, distribution)
            }
            EmbedderSource::Ollama => Self::ollama(model, url, api_key, dimensions, distribution),
            EmbedderSource::UserProvided => {
                Self::user_provided(dimensions.set().unwrap(), distribution)
            }
            EmbedderSource::Rest => Self::rest(
                url.set().unwrap(),
                api_key,
                indexing_fragments,
                search_fragments,
                request.set().unwrap(),
                response.set().unwrap(),
                headers,
                dimensions,
                distribution,
            ),
            EmbedderSource::Composite => panic!("nested composite embedders"),
        }
    }

    fn openai(
        model: Setting<String>,
        url: Setting<String>,
        api_key: Setting<String>,
        dimensions: Setting<usize>,
        distribution: Setting<DistributionShift>,
    ) -> Self {
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
        SubEmbedderOptions::OpenAi(options)
    }
    fn hugging_face(
        model: Setting<String>,
        revision: Setting<String>,
        pooling: Setting<OverridePooling>,
        distribution: Setting<DistributionShift>,
    ) -> Self {
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
        if let Some(pooling) = pooling.set() {
            options.pooling = pooling;
        }
        options.distribution = distribution.set();
        SubEmbedderOptions::HuggingFace(options)
    }
    fn user_provided(dimensions: usize, distribution: Setting<DistributionShift>) -> Self {
        Self::UserProvided(super::manual::EmbedderOptions {
            dimensions,
            distribution: distribution.set(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn rest(
        url: String,
        api_key: Setting<String>,
        indexing_fragments: Setting<BTreeMap<String, Option<Fragment>>>,
        search_fragments: Setting<BTreeMap<String, Option<Fragment>>>,
        request: serde_json::Value,
        response: serde_json::Value,
        headers: Setting<BTreeMap<String, String>>,
        dimensions: Setting<usize>,
        distribution: Setting<DistributionShift>,
    ) -> Self {
        Self::Rest(super::rest::EmbedderOptions {
            api_key: api_key.set(),
            dimensions: dimensions.set(),
            url,
            request,
            response,
            distribution: distribution.set(),
            headers: headers.set().unwrap_or_default(),
            search_fragments: search_fragments
                .set()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(name, fragment)| {
                    Some((name, fragment.map(|fragment| fragment.value)?))
                })
                .collect(),
            indexing_fragments: indexing_fragments
                .set()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(name, fragment)| {
                    Some((name, fragment.map(|fragment| fragment.value)?))
                })
                .collect(),
        })
    }
    fn ollama(
        model: Setting<String>,
        url: Setting<String>,
        api_key: Setting<String>,
        dimensions: Setting<usize>,
        distribution: Setting<DistributionShift>,
    ) -> Self {
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
        SubEmbedderOptions::Ollama(options)
    }
}

impl From<SubEmbedderOptions> for EmbedderOptions {
    fn from(value: SubEmbedderOptions) -> Self {
        match value {
            SubEmbedderOptions::HuggingFace(embedder_options) => {
                Self::HuggingFace(embedder_options)
            }
            SubEmbedderOptions::OpenAi(embedder_options) => Self::OpenAi(embedder_options),
            SubEmbedderOptions::Ollama(embedder_options) => Self::Ollama(embedder_options),
            SubEmbedderOptions::UserProvided(embedder_options) => {
                Self::UserProvided(embedder_options)
            }
            SubEmbedderOptions::Rest(embedder_options) => Self::Rest(embedder_options),
        }
    }
}

pub(crate) fn fragments_from_settings(
    setting: &Setting<EmbeddingSettings>,
) -> impl Iterator<Item = String> + '_ {
    let Some(setting) = setting.as_ref().set() else { return Either::Left(None.into_iter()) };

    let filter_map = |(name, fragment): (&String, &Option<Fragment>)| {
        if fragment.is_some() {
            Some(name.clone())
        } else {
            None
        }
    };

    if let Some(setting) = setting.indexing_fragments.as_ref().set() {
        Either::Right(setting.iter().filter_map(filter_map))
    } else {
        let Some(setting) = setting.indexing_embedder.as_ref().set() else {
            return Either::Left(None.into_iter());
        };
        let Some(setting) = setting.indexing_fragments.as_ref().set() else {
            return Either::Left(None.into_iter());
        };
        Either::Right(setting.iter().filter_map(filter_map))
    }
}
