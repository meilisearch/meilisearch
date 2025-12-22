use std::error::Error;
use std::fmt;

use deserr::errors::JsonError;
use deserr::Deserr;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::index::{self, ChatConfig, MatchingStrategy, RankingScoreThreshold, SearchParameters};
use crate::prompt::{default_max_bytes, PromptData};
use crate::update::Setting;

/// Configuration settings for AI-powered chat and search functionality.
///
/// These settings control how documents are presented to the LLM and what
/// search parameters are used when the LLM queries the index.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = JsonError, deny_unknown_fields, rename_all = camelCase)]
pub struct ChatSettings {
    /// A description of this index that helps the LLM understand its contents
    /// and purpose. This description is provided to the LLM to help it decide
    /// when and how to query this index.
    /// Example: "Contains product catalog with prices and descriptions".
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub description: Setting<String>,

    /// A liquid template used to render documents to a text that can be embedded.
    ///
    /// Meillisearch interpolates the template for each document and sends the resulting text to the embedder.
    /// The embedder then generates document vectors based on this text.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub document_template: Setting<String>,

    /// Maximum size in bytes for the rendered document text. Texts longer than
    /// this limit are truncated. This prevents very large documents from
    /// consuming too much context in the LLM conversation.
    /// Defaults to `400` bytes.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>)]
    pub document_template_max_bytes: Setting<usize>,

    /// Default search parameters used when the LLM queries this index.
    /// These settings control how search results are retrieved and ranked.
    /// If not specified, standard search defaults are used.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<ChatSearchParams>)]
    pub search_parameters: Setting<ChatSearchParams>,
}

impl From<ChatConfig> for ChatSettings {
    fn from(config: ChatConfig) -> Self {
        let ChatConfig {
            description,
            prompt: PromptData { template, max_bytes },
            search_parameters,
        } = config;
        ChatSettings {
            description: Setting::Set(description),
            document_template: Setting::Set(template),
            document_template_max_bytes: Setting::Set(
                max_bytes.unwrap_or(default_max_bytes()).get(),
            ),
            search_parameters: Setting::Set({
                let SearchParameters {
                    hybrid,
                    limit,
                    sort,
                    distinct,
                    matching_strategy,
                    attributes_to_search_on,
                    ranking_score_threshold,
                } = search_parameters;

                let hybrid = hybrid.map(|index::HybridQuery { semantic_ratio, embedder }| {
                    HybridQuery { semantic_ratio: SemanticRatio(semantic_ratio), embedder }
                });

                ChatSearchParams {
                    hybrid: Setting::some_or_not_set(hybrid),
                    limit: Setting::some_or_not_set(limit),
                    sort: Setting::some_or_not_set(sort),
                    distinct: Setting::some_or_not_set(distinct),
                    matching_strategy: Setting::some_or_not_set(matching_strategy),
                    attributes_to_search_on: Setting::some_or_not_set(attributes_to_search_on),
                    ranking_score_threshold: Setting::some_or_not_set(ranking_score_threshold),
                }
            }),
        }
    }
}

/// Search parameters that control how the LLM queries this index.
///
/// These settings are applied automatically when the chat system
/// performs searches.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = JsonError, deny_unknown_fields, rename_all = camelCase)]
pub struct ChatSearchParams {
    /// Configuration for hybrid search combining keyword and semantic search.
    /// Set the `semanticRatio` to balance between keyword matching (0.0) and
    /// semantic similarity (1.0). Requires an embedder to be configured.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<HybridQuery>)]
    pub hybrid: Setting<HybridQuery>,

    /// Maximum number of documents to return when the LLM queries this index.
    /// Higher values provide more context to the LLM but may increase
    /// response time and token usage.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>)]
    pub limit: Setting<usize>,

    /// Sort criteria for ordering search results before presenting to the LLM.
    /// Each entry should be in the format `attribute:asc` or `attribute:desc`.
    /// Example: `["price:asc", "rating:desc"]`.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<Vec<String>>)]
    pub sort: Setting<Vec<String>>,

    /// The attribute used for deduplicating results. When set, only one
    /// document per unique value of this attribute is returned. Useful for
    /// avoiding duplicate content in LLM responses.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub distinct: Setting<String>,

    /// Strategy for matching query terms. `last` (default) matches all words
    /// and returns documents matching at least the last word. `all` requires
    /// all words to match. `frequency` prioritizes less frequent words.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<MatchingStrategy>)]
    pub matching_strategy: Setting<MatchingStrategy>,

    /// Restricts the search to only the specified attributes. If not set, all
    /// searchable attributes are searched.
    /// Example: `["title", "description"]` searches only these two fields.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<Vec<String>>)]
    pub attributes_to_search_on: Setting<Vec<String>>,

    /// Minimum ranking score (0.0 to 1.0) that documents must achieve to be
    /// included in results. Documents below this threshold are excluded.
    /// Useful for filtering out low-relevance results.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<RankingScoreThreshold>)]
    pub ranking_score_threshold: Setting<RankingScoreThreshold>,
}

/// Configuration for hybrid search combining keyword and semantic search.
///
/// This allows searches that understand both exact words and conceptual
/// meaning.
#[derive(Debug, Clone, Default, Deserr, ToSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[deserr(error = JsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct HybridQuery {
    /// Controls the balance between keyword search and semantic search.
    /// A value of `0.0` uses only keyword search, `1.0` uses only semantic
    /// search, and `0.5` (the default) gives equal weight to both.
    /// Use lower values for exact term matching and higher values for
    /// conceptual similarity.
    #[deserr(default)]
    #[serde(default)]
    #[schema(default, value_type = f32)]
    pub semantic_ratio: SemanticRatio,
    /// The name of the embedder configuration to use for generating query
    /// vectors. This must match one of the embedders defined in the index's
    /// `embedders` settings.
    #[schema(value_type = String)]
    pub embedder: String,
}

#[derive(Debug, Clone, Copy, Deserr, ToSchema, PartialEq, Serialize, Deserialize)]
#[deserr(try_from(f32) = TryFrom::try_from -> InvalidSearchSemanticRatio)]
pub struct SemanticRatio(f32);

impl Default for SemanticRatio {
    fn default() -> Self {
        SemanticRatio(0.5)
    }
}

impl std::convert::TryFrom<f32> for SemanticRatio {
    type Error = InvalidSearchSemanticRatio;

    fn try_from(f: f32) -> Result<Self, Self::Error> {
        // the suggested "fix" is: `!(0.0..=1.0).contains(&f)`` which is allegedly less readable
        #[allow(clippy::manual_range_contains)]
        if f > 1.0 || f < 0.0 {
            Err(InvalidSearchSemanticRatio)
        } else {
            Ok(SemanticRatio(f))
        }
    }
}

#[derive(Debug)]
pub struct InvalidSearchSemanticRatio;

impl Error for InvalidSearchSemanticRatio {}

impl fmt::Display for InvalidSearchSemanticRatio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the value of `semanticRatio` is invalid, expected a float between `0.0` and `1.0`."
        )
    }
}

impl std::ops::Deref for SemanticRatio {
    type Target = f32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
