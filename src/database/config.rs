
use std::collections::{HashSet, HashMap};

use serde_derive::{Serialize, Deserialize};
use serde::Serialize;
use serde::Serializer;
use serde::Deserialize;
use serde::Deserializer;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RankingOrdering {
    Asc,
    Dsc
}

impl Serialize for RankingOrdering {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.serialize_str(match *self {
            RankingOrdering::Asc => "asc",
            RankingOrdering::Dsc => "dsc",
        })
    }
}

impl<'de> Deserialize<'de> for RankingOrdering {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "asc" => RankingOrdering::Asc,
            "dsc" => RankingOrdering::Dsc,
            _ => RankingOrdering::Dsc
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessToken {
    pub read_key: String,
    pub write_key: String,
    pub admin_key: String,
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub stop_words: Option<HashSet<String>>,
    pub ranking_order: Option<Vec<String>>,
    pub distinct_field: Option<String>,
    pub ranking_rules: Option<HashMap<String, RankingOrdering>>,
    pub access_token: Option<AccessToken>,
}

impl Config {
    pub(crate) fn default() -> Config {
        Config {
            stop_words: None,
            ranking_order: None,
            distinct_field: None,
            ranking_rules: None,
            access_token: None,
        }
    }

    pub fn update_with(&mut self, new: Config) {
        if let Some(stop_words) = new.stop_words {
            self.stop_words = Some(stop_words);
        };
        if let Some(ranking_order) = new.ranking_order {
            self.ranking_order = Some(ranking_order);
        };
        if let Some(distinct_field) = new.distinct_field {
            self.distinct_field = Some(distinct_field);
        };
        if let Some(ranking_rules) = new.ranking_rules {
            self.ranking_rules = Some(ranking_rules);
        };
        if let Some(access_token) = new.access_token {
            self.access_token = Some(access_token);
        };
    }
}
