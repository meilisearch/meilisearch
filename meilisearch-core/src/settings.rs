use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::iter::IntoIterator;
use std::str::FromStr;

use once_cell::sync::Lazy;
use serde::{Deserialize, Deserializer, Serialize};

use self::RankingRule::*;

pub const DEFAULT_RANKING_RULES: [RankingRule; 6] =
    [Typo, Words, Proximity, Attribute, WordsPosition, Exactness];

static RANKING_RULE_REGEX: Lazy<regex::Regex> =
    Lazy::new(|| regex::Regex::new(r"(asc|desc)\(([a-zA-Z0-9-_]*)\)").unwrap());

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Settings {
    #[serde(default, deserialize_with = "deserialize_some")]
    pub ranking_rules: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub distinct_attribute: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub searchable_attributes: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub displayed_attributes: Option<Option<HashSet<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub stop_words: Option<Option<BTreeSet<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub synonyms: Option<Option<BTreeMap<String, Vec<String>>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub attributes_for_faceting: Option<Option<Vec<String>>>,
}

// Any value that is present is considered Some value, including null.
fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

impl Settings {
    pub fn to_update(&self) -> Result<SettingsUpdate, RankingRuleConversionError> {
        let settings = self.clone();

        let ranking_rules = match settings.ranking_rules {
            Some(Some(rules)) => UpdateState::Update(RankingRule::try_from_iter(rules.iter())?),
            Some(None) => UpdateState::Clear,
            None => UpdateState::Nothing,
        };

        Ok(SettingsUpdate {
            ranking_rules,
            distinct_attribute: settings.distinct_attribute.into(),
            primary_key: UpdateState::Nothing,
            searchable_attributes: settings.searchable_attributes.into(),
            displayed_attributes: settings.displayed_attributes.into(),
            stop_words: settings.stop_words.into(),
            synonyms: settings.synonyms.into(),
            attributes_for_faceting: settings.attributes_for_faceting.into(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateState<T> {
    Update(T),
    Clear,
    Nothing,
}

impl<T> From<Option<Option<T>>> for UpdateState<T> {
    fn from(opt: Option<Option<T>>) -> UpdateState<T> {
        match opt {
            Some(Some(t)) => UpdateState::Update(t),
            Some(None) => UpdateState::Clear,
            None => UpdateState::Nothing,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RankingRuleConversionError;

impl std::fmt::Display for RankingRuleConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "impossible to convert into RankingRule")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RankingRule {
    Typo,
    Words,
    Proximity,
    Attribute,
    WordsPosition,
    Exactness,
    Asc(String),
    Desc(String),
}

impl std::fmt::Display for RankingRule {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RankingRule::Typo => f.write_str("typo"),
            RankingRule::Words => f.write_str("words"),
            RankingRule::Proximity => f.write_str("proximity"),
            RankingRule::Attribute => f.write_str("attribute"),
            RankingRule::WordsPosition => f.write_str("wordsPosition"),
            RankingRule::Exactness => f.write_str("exactness"),
            RankingRule::Asc(field) => write!(f, "asc({})", field),
            RankingRule::Desc(field) => write!(f, "desc({})", field),
        }
    }
}

impl FromStr for RankingRule {
    type Err = RankingRuleConversionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rule = match s {
            "typo" => RankingRule::Typo,
            "words" => RankingRule::Words,
            "proximity" => RankingRule::Proximity,
            "attribute" => RankingRule::Attribute,
            "wordsPosition" => RankingRule::WordsPosition,
            "exactness" => RankingRule::Exactness,
            _ => {
                let captures = RANKING_RULE_REGEX
                    .captures(s)
                    .ok_or(RankingRuleConversionError)?;
                match (captures.get(1).map(|m| m.as_str()), captures.get(2)) {
                    (Some("asc"), Some(field)) => RankingRule::Asc(field.as_str().to_string()),
                    (Some("desc"), Some(field)) => RankingRule::Desc(field.as_str().to_string()),
                    _ => return Err(RankingRuleConversionError),
                }
            }
        };
        Ok(rule)
    }
}

impl RankingRule {
    pub fn field(&self) -> Option<&str> {
        match self {
            RankingRule::Asc(field) | RankingRule::Desc(field) => Some(field),
            _ => None,
        }
    }

    pub fn try_from_iter(
        rules: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Vec<RankingRule>, RankingRuleConversionError> {
        rules
            .into_iter()
            .map(|s| RankingRule::from_str(s.as_ref()))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsUpdate {
    pub ranking_rules: UpdateState<Vec<RankingRule>>,
    pub distinct_attribute: UpdateState<String>,
    pub primary_key: UpdateState<String>,
    pub searchable_attributes: UpdateState<Vec<String>>,
    pub displayed_attributes: UpdateState<HashSet<String>>,
    pub stop_words: UpdateState<BTreeSet<String>>,
    pub synonyms: UpdateState<BTreeMap<String, Vec<String>>>,
    pub attributes_for_faceting: UpdateState<Vec<String>>,
}

impl Default for SettingsUpdate {
    fn default() -> Self {
        Self {
            ranking_rules: UpdateState::Nothing,
            distinct_attribute: UpdateState::Nothing,
            primary_key: UpdateState::Nothing,
            searchable_attributes: UpdateState::Nothing,
            displayed_attributes: UpdateState::Nothing,
            stop_words: UpdateState::Nothing,
            synonyms: UpdateState::Nothing,
            attributes_for_faceting: UpdateState::Nothing,
        }
    }
}
