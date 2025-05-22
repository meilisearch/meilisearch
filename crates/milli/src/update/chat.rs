use std::error::Error;
use std::fmt;

use deserr::errors::JsonError;
use deserr::Deserr;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::index::{self, ChatConfig, SearchParameters};
use crate::prompt::{default_max_bytes, PromptData};
use crate::update::Setting;
use crate::TermsMatchingStrategy;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = JsonError, deny_unknown_fields, rename_all = camelCase)]
pub struct ChatSettings {
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

    /// Rendered texts are truncated to this size. Defaults to 400.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>)]
    pub document_template_max_bytes: Setting<usize>,

    /// The search parameters to use for the LLM.
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

                let matching_strategy = matching_strategy.map(|ms| match ms {
                    index::MatchingStrategy::Last => MatchingStrategy::Last,
                    index::MatchingStrategy::All => MatchingStrategy::All,
                    index::MatchingStrategy::Frequency => MatchingStrategy::Frequency,
                });

                let ranking_score_threshold = ranking_score_threshold.map(RankingScoreThreshold);

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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = JsonError, deny_unknown_fields, rename_all = camelCase)]
pub struct ChatSearchParams {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<HybridQuery>)]
    pub hybrid: Setting<HybridQuery>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default = Setting::Set(20))]
    #[schema(value_type = Option<usize>)]
    pub limit: Setting<usize>,

    // #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    // #[deserr(default)]
    // pub attributes_to_retrieve: Option<BTreeSet<String>>,

    // #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    // #[deserr(default)]
    // pub filter: Option<Value>,
    //
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<Vec<String>>)]
    pub sort: Setting<Vec<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub distinct: Setting<String>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<MatchingStrategy>)]
    pub matching_strategy: Setting<MatchingStrategy>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<Vec<String>>)]
    pub attributes_to_search_on: Setting<Vec<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<RankingScoreThreshold>)]
    pub ranking_score_threshold: Setting<RankingScoreThreshold>,
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserr, ToSchema, Serialize, Deserialize)]
#[deserr(rename_all = camelCase)]
#[serde(rename_all = "camelCase")]
pub enum MatchingStrategy {
    /// Remove query words from last to first
    Last,
    /// All query words are mandatory
    All,
    /// Remove query words from the most frequent to the least
    Frequency,
}

impl Default for MatchingStrategy {
    fn default() -> Self {
        Self::Last
    }
}

impl From<MatchingStrategy> for TermsMatchingStrategy {
    fn from(other: MatchingStrategy) -> Self {
        match other {
            MatchingStrategy::Last => Self::Last,
            MatchingStrategy::All => Self::All,
            MatchingStrategy::Frequency => Self::Frequency,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserr, ToSchema, Serialize, Deserialize)]
#[deserr(try_from(f64) = TryFrom::try_from -> InvalidSearchRankingScoreThreshold)]
pub struct RankingScoreThreshold(pub f64);

impl std::convert::TryFrom<f64> for RankingScoreThreshold {
    type Error = InvalidSearchRankingScoreThreshold;

    fn try_from(f: f64) -> Result<Self, Self::Error> {
        // the suggested "fix" is: `!(0.0..=1.0).contains(&f)`` which is allegedly less readable
        #[allow(clippy::manual_range_contains)]
        if f > 1.0 || f < 0.0 {
            Err(InvalidSearchRankingScoreThreshold)
        } else {
            Ok(RankingScoreThreshold(f))
        }
    }
}

#[derive(Debug)]
pub struct InvalidSearchRankingScoreThreshold;

impl Error for InvalidSearchRankingScoreThreshold {}

impl fmt::Display for InvalidSearchRankingScoreThreshold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the value of `rankingScoreThreshold` is invalid, expected a float between `0.0` and `1.0`."
        )
    }
}
