use serde::de::DeserializeOwned;
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use super::Error;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RankingOrdering {
    Asc,
    Dsc
}

pub type StopWords = HashSet<String>;
pub type RankingOrder = Vec<String>;
pub type DistinctField = String;
pub type RankingRules = HashMap<String, RankingOrdering>;

const STOP_WORDS_KEY:       &str = "stop-words";
const RANKING_ORDER_KEY:    &str = "ranking-order";
const DISTINCT_FIELD_KEY:   &str = "distinct-field";
const RANKING_RULES_KEY:    &str = "ranking-rules";

#[derive(Clone)]
pub struct CustomSettingsIndex(pub(crate) crate::CfTree);

impl Deref for CustomSettingsIndex {
    type Target = crate::CfTree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl CustomSettingsIndex {
    fn get<K, T>(&self, key: K) -> Result<Option<T>, Error>
    where K: AsRef<[u8]>,
          T: DeserializeOwned,
    {
        let setting = self.0.get(key)?;
        let raw = match setting {
            Some(raw) => raw,
            None => return Ok(None)
        };

        Ok(Some(bincode::deserialize(&raw)?))
    }

    fn set<K, T>(&self, key: K, data: &T) -> Result<(), Error>
    where K: AsRef<[u8]>,
          T: Serialize,
    {
        let raw = bincode::serialize(data)?;
        self.0.insert(key, &raw)?;
        Ok(())
    }

    pub fn get_stop_words(&self) -> Result<Option<StopWords>, Error> {
        self.get(STOP_WORDS_KEY)
    }

    pub fn get_ranking_order(&self) -> Result<Option<RankingOrder>, Error> {
        self.get(RANKING_ORDER_KEY)
    }

    pub fn get_distinct_field(&self) -> Result<Option<DistinctField>, Error> {
        self.get(DISTINCT_FIELD_KEY)
    }

    pub fn get_ranking_rules(&self) -> Result<Option<RankingRules>, Error> {
        self.get(RANKING_RULES_KEY)
    }

    pub fn set_stop_words(&self, value: &StopWords) -> Result<(), Error> {
        self.set(STOP_WORDS_KEY, value)
    }

    pub fn set_ranking_order(&self, value: &RankingOrder) -> Result<(), Error> {
        self.set(RANKING_ORDER_KEY, value)
    }

    pub fn set_distinct_field(&self, value: &DistinctField) -> Result<(), Error> {
        self.set(DISTINCT_FIELD_KEY, value)
    }

    pub fn set_ranking_rules(&self, value: &RankingRules) -> Result<(), Error> {
        self.set(RANKING_RULES_KEY, value)
    }
}
