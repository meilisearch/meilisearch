mod action;
mod dump;
pub mod error;
mod key;
mod store;

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

pub use action::{actions, Action};
use error::{AuthControllerError, Result};
pub use key::Key;
use meilisearch_types::star_or::StarOr;
use store::generate_key_as_hexa;
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

    pub fn create_key(&self, value: Value) -> Result<Key> {
        let key = Key::create_from_value(value)?;
        match self.store.get_api_key(key.uid)? {
            Some(_) => Err(AuthControllerError::ApiKeyAlreadyExists(
                key.uid.to_string(),
            )),
            None => self.store.put_api_key(key),
        }
    }

    pub fn update_key(&self, uid: Uuid, value: Value) -> Result<Key> {
        let mut key = self.get_key(uid)?;
        key.update_from_value(value)?;
        self.store.put_api_key(key)
    }

    pub fn get_key(&self, uid: Uuid) -> Result<Key> {
        self.store
            .get_api_key(uid)?
            .ok_or_else(|| AuthControllerError::ApiKeyNotFound(uid.to_string()))
    }

    pub fn get_optional_uid_from_encoded_key(&self, encoded_key: &[u8]) -> Result<Option<Uuid>> {
        match &self.master_key {
            Some(master_key) => self
                .store
                .get_uid_from_encoded_key(encoded_key, master_key.as_bytes()),
            None => Ok(None),
        }
    }

    pub fn get_uid_from_encoded_key(&self, encoded_key: &str) -> Result<Uuid> {
        self.get_optional_uid_from_encoded_key(encoded_key.as_bytes())?
            .ok_or_else(|| AuthControllerError::ApiKeyNotFound(encoded_key.to_string()))
    }

    pub fn get_key_filters(
        &self,
        uid: Uuid,
        search_rules: Option<SearchRules>,
    ) -> Result<AuthFilter> {
        let mut filters = AuthFilter::default();
        let key = self
            .store
            .get_api_key(uid)?
            .ok_or_else(|| AuthControllerError::ApiKeyNotFound(uid.to_string()))?;

        if !key.indexes.iter().any(|i| i == &StarOr::Star) {
            filters.search_rules = match search_rules {
                // Intersect search_rules with parent key authorized indexes.
                Some(search_rules) => SearchRules::Map(
                    key.indexes
                        .into_iter()
                        .filter_map(|index| {
                            search_rules.get_index_search_rules(index.deref()).map(
                                |index_search_rules| {
                                    (String::from(index), Some(index_search_rules))
                                },
                            )
                        })
                        .collect(),
                ),
                None => SearchRules::Set(key.indexes.into_iter().map(String::from).collect()),
            };
        } else if let Some(search_rules) = search_rules {
            filters.search_rules = search_rules;
        }

        filters.allow_index_creation = key
            .actions
            .iter()
            .any(|&action| action == Action::IndexesAdd || action == Action::All);

        Ok(filters)
    }

    pub fn list_keys(&self) -> Result<Vec<Key>> {
        self.store.list_api_keys()
    }

    pub fn delete_key(&self, uid: Uuid) -> Result<()> {
        if self.store.delete_api_key(uid)? {
            Ok(())
        } else {
            Err(AuthControllerError::ApiKeyNotFound(uid.to_string()))
        }
    }

    pub fn get_master_key(&self) -> Option<&String> {
        self.master_key.as_ref()
    }

    /// Generate a valid key from a key id using the current master key.
    /// Returns None if no master key has been set.
    pub fn generate_key(&self, uid: Uuid) -> Option<String> {
        self.master_key
            .as_ref()
            .map(|master_key| generate_key_as_hexa(uid, master_key.as_bytes()))
    }

    /// Check if the provided key is authorized to make a specific action
    /// without checking if the key is valid.
    pub fn is_key_authorized(
        &self,
        uid: Uuid,
        action: Action,
        index: Option<&str>,
    ) -> Result<bool> {
        match self
            .store
            // check if the key has access to all indexes.
            .get_expiration_date(uid, action, None)?
            .or(match index {
                // else check if the key has access to the requested index.
                Some(index) => {
                    self.store
                        .get_expiration_date(uid, action, Some(index.as_bytes()))?
                }
                // or to any index if no index has been requested.
                None => self.store.prefix_first_expiration_date(uid, action)?,
            }) {
            // check expiration date.
            Some(Some(exp)) => Ok(OffsetDateTime::now_utc() < exp),
            // no expiration date.
            Some(None) => Ok(true),
            // action or index forbidden.
            None => Ok(false),
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

fn generate_default_keys(store: &HeedAuthStore) -> Result<()> {
    store.put_api_key(Key::default_admin())?;
    store.put_api_key(Key::default_search())?;

    Ok(())
}
