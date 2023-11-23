use std::collections::{BTreeMap, BTreeSet};
use std::result::Result as StdResult;
use std::str::FromStr;

use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Settings {
    #[serde(default, deserialize_with = "deserialize_some")]
    pub ranking_rules: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub distinct_attribute: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub searchable_attributes: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub displayed_attributes: Option<Option<BTreeSet<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub stop_words: Option<Option<BTreeSet<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub synonyms: Option<Option<BTreeMap<String, Vec<String>>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub attributes_for_faceting: Option<Option<Vec<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsUpdate {
    pub ranking_rules: UpdateState<Vec<RankingRule>>,
    pub distinct_attribute: UpdateState<String>,
    pub primary_key: UpdateState<String>,
    pub searchable_attributes: UpdateState<Vec<String>>,
    pub displayed_attributes: UpdateState<BTreeSet<String>>,
    pub stop_words: UpdateState<BTreeSet<String>>,
    pub synonyms: UpdateState<BTreeMap<String, Vec<String>>>,
    pub attributes_for_faceting: UpdateState<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateState<T> {
    Update(T),
    Clear,
    Nothing,
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

static ASC_DESC_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"(asc|desc)\(([\w_-]+)\)").unwrap());

impl FromStr for RankingRule {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "typo" => Self::Typo,
            "words" => Self::Words,
            "proximity" => Self::Proximity,
            "attribute" => Self::Attribute,
            "wordsPosition" => Self::WordsPosition,
            "exactness" => Self::Exactness,
            text => {
                let caps = ASC_DESC_REGEX.captures(text).ok_or(())?;
                let order = caps.get(1).unwrap().as_str();
                let field_name = caps.get(2).unwrap().as_str();
                match order {
                    "asc" => Self::Asc(field_name.to_string()),
                    "desc" => Self::Desc(field_name.to_string()),
                    _ => return Err(()),
                }
            }
        })
    }
}

// Any value that is present is considered Some value, including null.
fn deserialize_some<'de, T, D>(deserializer: D) -> StdResult<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}
