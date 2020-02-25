use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::str::FromStr;
use std::iter::IntoIterator;

use serde::{Deserialize, Deserializer, Serialize};
use once_cell::sync::Lazy;

static RANKING_RULE_REGEX: Lazy<regex::Regex> = Lazy::new(|| {
    let regex = regex::Regex::new(r"(asc|dsc)\(([a-zA-Z0-9-_]*)\)").unwrap();
    regex
});

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Settings {
    #[serde(default, deserialize_with = "deserialize_some")]
    pub ranking_rules: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub ranking_distinct: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub searchable_attributes: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub displayed_attributes: Option<Option<HashSet<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub stop_words: Option<Option<BTreeSet<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub synonyms: Option<Option<BTreeMap<String, Vec<String>>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub index_new_fields: Option<Option<bool>>,
}

// Any value that is present is considered Some value, including null.
fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
    where T: Deserialize<'de>,
          D: Deserializer<'de>
{
    Deserialize::deserialize(deserializer).map(Some)
}

impl Settings {
    pub fn into_update(&self) -> Result<SettingsUpdate, RankingRuleConversionError> {
        let settings = self.clone();

        let ranking_rules = match settings.ranking_rules {
            Some(Some(rules)) => UpdateState::Update(RankingRule::from_iter(rules.iter())?),
            Some(None) => UpdateState::Clear,
            None => UpdateState::Nothing,
        };

        Ok(SettingsUpdate {
            ranking_rules,
            ranking_distinct: settings.ranking_distinct.into(),
            identifier: UpdateState::Nothing,
            searchable_attributes: settings.searchable_attributes.into(),
            displayed_attributes: settings.displayed_attributes.into(),
            stop_words: settings.stop_words.into(),
            synonyms: settings.synonyms.into(),
            index_new_fields: settings.index_new_fields.into(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateState<T> {
    Update(T),
    Clear,
    Nothing,
}

impl <T> From<Option<Option<T>>> for UpdateState<T> {
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
    Dsc(String),
}

impl ToString for RankingRule {
    fn to_string(&self) -> String {
        match self {
            RankingRule::Typo => "_typo".to_string(),
            RankingRule::Words => "_words".to_string(),
            RankingRule::Proximity => "_proximity".to_string(),
            RankingRule::Attribute => "_attribute".to_string(),
            RankingRule::WordsPosition => "_words_position".to_string(),
            RankingRule::Exactness => "_exactness".to_string(),
            RankingRule::Asc(field) => format!("asc({})", field),
            RankingRule::Dsc(field) => format!("dsc({})", field),
        }
    }
}

impl FromStr for RankingRule {
    type Err = RankingRuleConversionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rule = match s {
            "_typo" => RankingRule::Typo,
            "_words" => RankingRule::Words,
            "_proximity" => RankingRule::Proximity,
            "_attribute" => RankingRule::Attribute,
            "_words_position" => RankingRule::WordsPosition,
            "_exactness" => RankingRule::Exactness,
            _ => {
                let captures = RANKING_RULE_REGEX.captures(s).ok_or(RankingRuleConversionError)?;
                match (captures.get(1).map(|m| m.as_str()), captures.get(2)) {
                    (Some("asc"), Some(field)) => RankingRule::Asc(field.as_str().to_string()),
                    (Some("dsc"), Some(field)) => RankingRule::Dsc(field.as_str().to_string()),
                    _ => return Err(RankingRuleConversionError)
                }
            }
        };
        Ok(rule)
    }
}

impl RankingRule {
    pub fn field(&self) -> Option<&str> {
        match self {
            RankingRule::Asc(field) | RankingRule::Dsc(field) => Some(field),
            _ => None,
        }
    }

    pub fn from_iter(rules: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Vec<RankingRule>, RankingRuleConversionError> {
        rules.into_iter()
            .map(|s| RankingRule::from_str(s.as_ref()))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsUpdate {
    pub ranking_rules: UpdateState<Vec<RankingRule>>,
    pub ranking_distinct: UpdateState<String>,
    pub identifier: UpdateState<String>,
    pub searchable_attributes: UpdateState<Vec<String>>,
    pub displayed_attributes: UpdateState<HashSet<String>>,
    pub stop_words: UpdateState<BTreeSet<String>>,
    pub synonyms: UpdateState<BTreeMap<String, Vec<String>>>,
    pub index_new_fields: UpdateState<bool>,
}

impl Default for SettingsUpdate {
    fn default() -> Self {
        Self {
            ranking_rules: UpdateState::Nothing,
            ranking_distinct: UpdateState::Nothing,
            identifier: UpdateState::Nothing,
            searchable_attributes: UpdateState::Nothing,
            displayed_attributes: UpdateState::Nothing,
            stop_words: UpdateState::Nothing,
            synonyms: UpdateState::Nothing,
            index_new_fields: UpdateState::Nothing,
        }
    }
}
