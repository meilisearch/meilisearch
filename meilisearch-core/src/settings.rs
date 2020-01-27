use std::sync::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use once_cell::sync::Lazy;

static RANKING_RULE_REGEX: Lazy<Mutex<regex::Regex>> = Lazy::new(|| {
    let regex = regex::Regex::new(r"(asc|dsc)\(([a-zA-Z0-9-_]*)\)").unwrap();
    Mutex::new(regex)
});


#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Settings {
    pub ranking_rules: Option<Vec<String>>,
    pub ranking_distinct: Option<String>,
    pub attribute_identifier: Option<String>,
    pub attributes_searchable: Option<Vec<String>>,
    pub attributes_displayed: Option<HashSet<String>>,
    pub stop_words: Option<BTreeSet<String>>,
    pub synonyms: Option<BTreeMap<String, Vec<String>>>,
    pub index_new_fields: Option<bool>,
}

impl Settings {
    pub fn into_cleared(self) -> SettingsUpdate {
        let settings = self.clone();

        let ranking_rules = match settings.ranking_rules {
            Some(rules) => Some(RankingRule::from_vec(rules)),
            None => None
        };

        SettingsUpdate {
            ranking_rules: UpdateState::convert_with_default(ranking_rules, UpdateState::Clear),
            ranking_distinct: UpdateState::convert_with_default(settings.ranking_distinct, UpdateState::Clear),
            attribute_identifier: UpdateState::convert_with_default(settings.attribute_identifier, UpdateState::Clear),
            attributes_searchable: UpdateState::convert_with_default(settings.attributes_searchable, UpdateState::Clear),
            attributes_displayed: UpdateState::convert_with_default(settings.attributes_displayed, UpdateState::Clear),
            stop_words: UpdateState::convert_with_default(settings.stop_words, UpdateState::Clear),
            synonyms: UpdateState::convert_with_default(settings.synonyms, UpdateState::Clear),
            index_new_fields: UpdateState::convert_with_default(settings.index_new_fields, UpdateState::Clear),
        }
    }
}

impl Into<SettingsUpdate> for Settings {
    fn into(self) -> SettingsUpdate {
        let settings = self.clone();

        let ranking_rules = match settings.ranking_rules {
            Some(rules) => Some(RankingRule::from_vec(rules)),
            None => None,
        };

        SettingsUpdate {
            ranking_rules: ranking_rules.into(),
            ranking_distinct: settings.ranking_distinct.into(),
            attribute_identifier: settings.attribute_identifier.into(),
            attributes_searchable: settings.attributes_searchable.into(),
            attributes_displayed: settings.attributes_displayed.into(),
            stop_words: settings.stop_words.into(),
            synonyms: settings.synonyms.into(),
            index_new_fields: settings.index_new_fields.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateState<T> {
    Update(T),
    Add(T),
    Delete(T),
    Clear,
    Nothing,
}

impl <T> From<Option<T>> for UpdateState<T> {
    fn from(opt: Option<T>) -> UpdateState<T> {
        match opt {
            Some(t) => UpdateState::Update(t),
            None => UpdateState::Nothing,
        }
    }
}

impl<T> UpdateState<T> {
    pub fn is_changed(&self) -> bool {
        match self {
            UpdateState::Nothing => false,
            _ => true,
        }
    }

    fn convert_with_default(opt: Option<T>, default: UpdateState<T>) -> UpdateState<T> {
        match opt {
            Some(t) => UpdateState::Update(t),
            None => default,
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
    Exact,
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
            RankingRule::Exact => "_exact".to_string(),
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
            "_exact" => RankingRule::Exact,
            _ => {
                let captures = RANKING_RULE_REGEX.lock().unwrap().captures(s).unwrap();
                match captures[1].as_ref() {
                    "asc" => RankingRule::Asc(captures[2].to_string()),
                    "dsc" => RankingRule::Dsc(captures[2].to_string()),
                    _ => return Err(RankingRuleConversionError)
                }
            }
        };
        Ok(rule)
    }
}

impl RankingRule {
    pub fn get_field(&self) -> Option<String> {
        match self {
            RankingRule::Asc(field) | RankingRule::Dsc(field) => Some((*field).clone()),
            _ => None,
        }
    }

    pub fn from_vec(rules: Vec<String>) -> Vec<RankingRule> {
        rules.iter()
            .map(|s| RankingRule::from_str(s.as_str()))
            .filter_map(Result::ok)
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsUpdate {
    pub ranking_rules: UpdateState<Vec<RankingRule>>,
    pub ranking_distinct: UpdateState<String>,
    pub attribute_identifier: UpdateState<String>,
    pub attributes_searchable: UpdateState<Vec<String>>,
    pub attributes_displayed: UpdateState<HashSet<String>>,
    pub stop_words: UpdateState<BTreeSet<String>>,
    pub synonyms: UpdateState<BTreeMap<String, Vec<String>>>,
    pub index_new_fields: UpdateState<bool>,
}

impl Default for SettingsUpdate {
    fn default() -> Self {
        Self {
            ranking_rules: UpdateState::Nothing,
            ranking_distinct: UpdateState::Nothing,
            attribute_identifier: UpdateState::Nothing,
            attributes_searchable: UpdateState::Nothing,
            attributes_displayed: UpdateState::Nothing,
            stop_words: UpdateState::Nothing,
            synonyms: UpdateState::Nothing,
            index_new_fields: UpdateState::Nothing,
        }
    }
}
