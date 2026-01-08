use std::collections::{hash_map, HashMap};
use std::iter::FromIterator;

use serde::{Deserialize, Deserializer, Serialize};

use crate::OrderBy;

#[derive(Serialize, Clone, Debug)]
pub struct OrderByMap(HashMap<String, OrderBy>);

impl OrderByMap {
    pub fn get(&self, key: impl AsRef<str>) -> OrderBy {
        self.0
            .get(key.as_ref())
            .copied()
            .unwrap_or_else(|| self.0.get("*").copied().unwrap_or_default())
    }

    pub fn insert(&mut self, key: String, value: OrderBy) -> Option<OrderBy> {
        self.0.insert(key, value)
    }
}

impl Default for OrderByMap {
    fn default() -> Self {
        let mut map = HashMap::new();
        map.insert("*".to_string(), OrderBy::Lexicographic);
        OrderByMap(map)
    }
}

impl FromIterator<(String, OrderBy)> for OrderByMap {
    fn from_iter<T: IntoIterator<Item = (String, OrderBy)>>(iter: T) -> Self {
        OrderByMap(iter.into_iter().collect())
    }
}

impl IntoIterator for OrderByMap {
    type Item = (String, OrderBy);
    type IntoIter = hash_map::IntoIter<String, OrderBy>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'de> Deserialize<'de> for OrderByMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut map = Deserialize::deserialize(deserializer).map(OrderByMap)?;
        // Insert the default ordering if it is not already overwritten by the user.
        map.0.entry("*".to_string()).or_insert(OrderBy::default());
        Ok(map)
    }
}
