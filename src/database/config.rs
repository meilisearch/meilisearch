use std::collections::{HashSet, HashMap};

use serde_derive::{Serialize, Deserialize};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RankingOrdering {
    Asc,
    Dsc
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    stop_words: Option<HashSet<String>>,
    ranking_order: Option<Vec<String>>,
    distinct_field: Option<String>,
    ranking_rules: Option<HashMap<String, RankingOrdering>>,
}


impl Config {
    pub(crate) fn default() -> Config {
        Config {
            stop_words: None,
            ranking_order: None,
            distinct_field: None,
            ranking_rules: None,
        }
    }
}
