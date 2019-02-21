use std::collections::{HashSet, HashMap};
use serde_derive::{Serialize, Deserialize};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RankingOrdering {
    Asc,
    Dsc
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessToken {
    pub read_key: String,
    pub write_key: String,
    pub admin_key: String,
}


#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub stop_words: Option<HashSet<String>>,
    pub ranking_order: Option<Vec<String>>,
    pub distinct_field: Option<String>,
    pub ranking_rules: Option<HashMap<String, RankingOrdering>>,
    pub access_token: Option<AccessToken>,
}

impl Config {
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
