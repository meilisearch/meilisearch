use std::error::Error;
use std::fmt;

use deserr::errors::JsonError;
use deserr::Deserr;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::index::{self, ChatConfig, MatchingStrategy, RankingScoreThreshold, SearchParameters};
use crate::prompt::{default_max_bytes, PromptData};
use crate::update::Setting;

/// Chat (conversation) settings: how the index is described to the LLM and how it is queried.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = JsonError, deny_unknown_fields, rename_all = camelCase)]
pub struct ChatSettings {
    /// Index description shown to the LLM so it can decide when and how to query this index. Example: "Contains product catalog with prices and descriptions".
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub description: Setting<String>,

    /// Liquid template that defines the text sent to the LLM for each document.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub document_template: Setting<String>,

    /// Maximum size in bytes of the rendered document template. Longer text is truncated.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>, default = 400)]
    pub document_template_max_bytes: Setting<usize>,

    /// Search parameters used when the LLM queries this index (hybrid, limit, sort, distinct, etc.).
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

/// Search parameters applied when the LLM queries this index.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = JsonError, deny_unknown_fields, rename_all = camelCase)]
pub struct ChatSearchParams {
    /// Hybrid search: mix of keyword and semantic search. Requires an embedder.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<HybridQuery>)]
    pub hybrid: Setting<HybridQuery>,

    /// Maximum number of documents returned per search performed by the LLM.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>)]
    pub limit: Setting<usize>,

    /// Sort order: array of strings like attribute:asc or attribute:desc. Example: `["price:asc", "rating:desc"]`.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<Vec<String>>)]
    pub sort: Setting<Vec<String>>,

    /// Attribute used to return at most one document per distinct value.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub distinct: Setting<String>,

    /// How query terms are matched: last, all, or frequency.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<MatchingStrategy>)]
    pub matching_strategy: Setting<MatchingStrategy>,

    /// Attributes on which to run the search. If unset, all searchable attributes are used. Example: `["title", "description"]` searches only these two fields.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<Vec<String>>)]
    pub attributes_to_search_on: Setting<Vec<String>>,

    /// Minimum ranking score (0.0–1.0) for a document to be included. Lower scores are excluded.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<RankingScoreThreshold>)]
    pub ranking_score_threshold: Setting<RankingScoreThreshold>,
}

/// Hybrid search: balance between keyword and semantic search.
#[derive(Debug, Clone, Default, Deserr, ToSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[deserr(error = JsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct HybridQuery {
    /// Balance between keyword (0.0) and semantic (1.0) search.
    #[deserr(default)]
    #[serde(default)]
    #[schema(default, value_type = f32)]
    pub semantic_ratio: SemanticRatio,
    /// Name of the embedder from the index embedders setting. Used to vectorize the query.
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
