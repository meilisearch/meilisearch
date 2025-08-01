use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use deserr::errors::JsonError;
use deserr::Deserr;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::index::{ChatConfig, MatchingStrategy, RankingScoreThreshold, SearchParameters};
use crate::prompt::PromptData;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = JsonError, deny_unknown_fields, rename_all = camelCase)]
pub struct ChatSettings {
    pub description: String,

    /// A liquid template used to render documents to a text that can be embedded.
    ///
    /// Meillisearch interpolates the template for each document and sends the resulting text to the embedder.
    /// The embedder then generates document vectors based on this text.
    pub document_template: String,

    /// Rendered texts are truncated to this size. Defaults to 400.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>)]
    pub document_template_max_bytes: Option<NonZeroUsize>,

    /// The search parameters to use for the LLM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    #[schema(value_type = Option<ChatSearchParams>)]
    pub search_parameters: Option<ChatSearchParams>,
}

impl From<ChatConfig> for ChatSettings {
    fn from(config: ChatConfig) -> Self {
        let ChatConfig {
            description,
            prompt: PromptData { template, max_bytes },
            search_parameters,
        } = config;
        ChatSettings {
            description,
            document_template: template,
            document_template_max_bytes: max_bytes,
            search_parameters: {
                let SearchParameters {
                    hybrid,
                    limit,
                    sort,
                    distinct,
                    matching_strategy,
                    attributes_to_search_on,
                    ranking_score_threshold,
                } = search_parameters;

                if hybrid.is_none()
                    && limit.is_none()
                    && sort.is_none()
                    && distinct.is_none()
                    && matching_strategy.is_none()
                    && attributes_to_search_on.is_none()
                    && ranking_score_threshold.is_none()
                {
                    None
                } else {
                    Some(ChatSearchParams {
                        hybrid: hybrid.map(|h| HybridQuery {
                            semantic_ratio: SemanticRatio(h.semantic_ratio),
                            embedder: h.embedder,
                        }),
                        limit,
                        sort,
                        distinct,
                        matching_strategy,
                        attributes_to_search_on,
                        ranking_score_threshold,
                    })
                }
            },
        }
    }
}

impl From<ChatSettings> for ChatConfig {
    fn from(settings: ChatSettings) -> Self {
        let ChatSettings {
            description,
            document_template,
            document_template_max_bytes,
            search_parameters,
        } = settings;

        let prompt =
            PromptData { template: document_template, max_bytes: document_template_max_bytes };

        let search_parameters = match search_parameters {
            Some(params) => SearchParameters {
                hybrid: params.hybrid.map(|h| crate::index::HybridQuery {
                    semantic_ratio: h.semantic_ratio.0,
                    embedder: h.embedder,
                }),
                limit: params.limit,
                sort: params.sort,
                distinct: params.distinct,
                matching_strategy: params.matching_strategy,
                attributes_to_search_on: params.attributes_to_search_on,
                ranking_score_threshold: params.ranking_score_threshold,
            },
            None => SearchParameters::default(),
        };

        ChatConfig { description, prompt, search_parameters }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = JsonError, deny_unknown_fields, rename_all = camelCase)]
pub struct ChatSearchParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    #[schema(value_type = Option<HybridQuery>)]
    pub hybrid: Option<HybridQuery>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>)]
    pub limit: Option<usize>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    #[schema(value_type = Option<Vec<String>>)]
    pub sort: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub distinct: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    #[schema(value_type = Option<MatchingStrategy>)]
    pub matching_strategy: Option<MatchingStrategy>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    #[schema(value_type = Option<Vec<String>>)]
    pub attributes_to_search_on: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    #[schema(value_type = Option<RankingScoreThreshold>)]
    pub ranking_score_threshold: Option<RankingScoreThreshold>,
}

#[derive(Debug, Clone, Default, Deserr, ToSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[deserr(error = JsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct HybridQuery {
    #[deserr(default)]
    #[serde(default)]
    #[schema(default, value_type = f32)]
    pub semantic_ratio: SemanticRatio,
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
