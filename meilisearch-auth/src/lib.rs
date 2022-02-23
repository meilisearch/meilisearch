mod action;
mod dump;
pub mod error;
mod key;
mod store;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::from_utf8;
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub use action::{actions, Action};
use error::{AuthControllerError, Result};
pub use key::Key;
pub use store::open_auth_store_env;
use store::HeedAuthStore;

#[derive(Clone)]
pub struct AuthController {
    store: Arc<HeedAuthStore>,
    master_key: Option<String>,
}

impl AuthController {
    pub fn new(db_path: impl AsRef<Path>, master_key: &Option<String>) -> Result<Self> {
        let store = HeedAuthStore::new(db_path)?;

        if store.is_empty()? {
            generate_default_keys(&store)?;
        }

        Ok(Self {
            store: Arc::new(store),
            master_key: master_key.clone(),
        })
    }

    pub async fn create_key(&self, value: Value) -> Result<Key> {
        let key = Key::create_from_value(value)?;
        self.store.put_api_key(key)
    }

    pub async fn update_key(&self, key: impl AsRef<str>, value: Value) -> Result<Key> {
        let mut key = self.get_key(key).await?;
        key.update_from_value(value)?;
        self.store.put_api_key(key)
    }

    pub async fn get_key(&self, key: impl AsRef<str>) -> Result<Key> {
        self.store
            .get_api_key(&key)?
            .ok_or_else(|| AuthControllerError::ApiKeyNotFound(key.as_ref().to_string()))
    }

    pub fn get_key_filters(
        &self,
        key: impl AsRef<str>,
        search_rules: Option<SearchRules>,
    ) -> Result<AuthFilter> {
        let mut filters = AuthFilter::default();
        if self
            .master_key
            .as_ref()
            .map_or(false, |master_key| master_key != key.as_ref())
        {
            let key = self
                .store
                .get_api_key(&key)?
                .ok_or_else(|| AuthControllerError::ApiKeyNotFound(key.as_ref().to_string()))?;

            if !key.indexes.iter().any(|i| i.as_str() == "*") {
                filters.search_rules = match search_rules {
                    // Intersect search_rules with parent key authorized indexes.
                    Some(search_rules) => SearchRules::Map(
                        key.indexes
                            .into_iter()
                            .filter_map(|index| {
                                search_rules
                                    .get_index_search_rules(&index)
                                    .map(|index_search_rules| (index, Some(index_search_rules)))
                            })
                            .collect(),
                    ),
                    None => SearchRules::Set(key.indexes.into_iter().collect()),
                };
            } else if let Some(search_rules) = search_rules {
                filters.search_rules = search_rules;
            }

            filters.allow_index_creation = key
                .actions
                .iter()
                .any(|&action| action == Action::IndexesAdd || action == Action::All);
        }

        Ok(filters)
    }

    pub async fn list_keys(&self) -> Result<Vec<Key>> {
        self.store.list_api_keys()
    }

    pub async fn delete_key(&self, key: impl AsRef<str>) -> Result<()> {
        if self.store.delete_api_key(&key)? {
            Ok(())
        } else {
            Err(AuthControllerError::ApiKeyNotFound(
                key.as_ref().to_string(),
            ))
        }
    }

    pub fn get_master_key(&self) -> Option<&String> {
        self.master_key.as_ref()
    }

    /// Generate a valid key from a key id using the current master key.
    /// Returns None if no master key has been set.
    pub fn generate_key(&self, id: &str) -> Option<String> {
        self.master_key
            .as_ref()
            .map(|master_key| generate_key(master_key.as_bytes(), id))
    }

    /// Check if the provided key is authorized to make a specific action
    /// without checking if the key is valid.
    pub fn is_key_authorized(
        &self,
        key: &[u8],
        action: Action,
        index: Option<&str>,
    ) -> Result<bool> {
        match self
            .store
            // check if the key has access to all indexes.
            .get_expiration_date(key, action, None)?
            .or(match index {
                // else check if the key has access to the requested index.
                Some(index) => {
                    self.store
                        .get_expiration_date(key, action, Some(index.as_bytes()))?
                }
                // or to any index if no index has been requested.
                None => self.store.prefix_first_expiration_date(key, action)?,
            }) {
            // check expiration date.
            Some(Some(exp)) => Ok(Utc::now() < exp),
            // no expiration date.
            Some(None) => Ok(true),
            // action or index forbidden.
            None => Ok(false),
        }
    }

    /// Check if the provided key is valid
    /// without checking if the key is authorized to make a specific action.
    pub fn is_key_valid(&self, key: &[u8]) -> Result<bool> {
        if let Some(id) = self.store.get_key_id(key) {
            let id = from_utf8(&id)?;
            if let Some(generated) = self.generate_key(id) {
                return Ok(generated.as_bytes() == key);
            }
        }

        Ok(false)
    }

    /// Check if the provided key is valid
    /// and is authorized to make a specific action.
    pub fn authenticate(&self, key: &[u8], action: Action, index: Option<&str>) -> Result<bool> {
        if self.is_key_authorized(key, action, index)? {
            self.is_key_valid(key)
        } else {
            Ok(false)
        }
    }
}

pub struct AuthFilter {
    pub search_rules: SearchRules,
    pub allow_index_creation: bool,
}

impl Default for AuthFilter {
    fn default() -> Self {
        Self {
            search_rules: SearchRules::default(),
            allow_index_creation: true,
        }
    }
}

/// Transparent wrapper around a list of allowed indexes with the search rules to apply for each.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SearchRules {
    Set(HashSet<String>),
    Map(HashMap<String, Option<IndexSearchRules>>),
}

impl Default for SearchRules {
    fn default() -> Self {
        Self::Set(Some("*".to_string()).into_iter().collect())
    }
}

impl SearchRules {
    pub fn is_index_authorized(&self, index: &str) -> bool {
        match self {
            Self::Set(set) => set.contains("*") || set.contains(index),
            Self::Map(map) => map.contains_key("*") || map.contains_key(index),
        }
    }

    pub fn get_index_search_rules(&self, index: &str) -> Option<IndexSearchRules> {
        match self {
            Self::Set(set) => {
                if set.contains("*") || set.contains(index) {
                    Some(IndexSearchRules::default())
                } else {
                    None
                }
            }
            Self::Map(map) => map
                .get(index)
                .or_else(|| map.get("*"))
                .map(|isr| isr.clone().unwrap_or_default()),
        }
    }
}

impl IntoIterator for SearchRules {
    type Item = (String, IndexSearchRules);
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Set(array) => {
                Box::new(array.into_iter().map(|i| (i, IndexSearchRules::default())))
            }
            Self::Map(map) => {
                Box::new(map.into_iter().map(|(i, isr)| (i, isr.unwrap_or_default())))
            }
        }
    }
}

/// Contains the rules to apply on the top of the search query for a specific index.
///
/// filter: search filter to apply in addition to query filters.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct IndexSearchRules {
    pub filter: Option<serde_json::Value>,
}

fn generate_key(master_key: &[u8], keyid: &str) -> String {
    let key = [keyid.as_bytes(), master_key].concat();
    let sha = Sha256::digest(&key);
    format!("{}{:x}", keyid, sha)
}

fn generate_default_keys(store: &HeedAuthStore) -> Result<()> {
    store.put_api_key(Key::default_admin())?;
    store.put_api_key(Key::default_search())?;

    Ok(())
}
