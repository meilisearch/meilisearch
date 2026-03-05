mod dump;
pub mod error;
mod store;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use error::{AuthControllerError, Result};
use maplit::hashset;
use meilisearch_types::heed::{Env, WithoutTls};
use meilisearch_types::index_uid_pattern::IndexUidPattern;
use meilisearch_types::keys::{Action, CreateApiKey, Key, PatchApiKey};
use meilisearch_types::milli::update::Setting;
use serde::{Deserialize, Serialize};
pub use store::open_auth_store_env;
use store::{generate_key_as_hexa, HeedAuthStore};
use time::OffsetDateTime;
use uuid::Uuid;

/// Classify a proposer error, recognizing cluster-specific error types.
///
/// The proposer may box an `AuthControllerError::ClusterNoLeader` or
/// `ClusterQuorumUnavailable` — if so, we downcast and return the proper
/// variant instead of wrapping it as `Internal`.
fn classify_proposer_error(
    e: Box<dyn std::error::Error + Send + Sync>,
) -> AuthControllerError {
    match e.downcast::<AuthControllerError>() {
        Ok(auth_err) => *auth_err,
        Err(e) => AuthControllerError::Internal(e),
    }
}

/// Trait for proposing API key mutations through Raft consensus.
///
/// When set on `AuthController`, key create/update/delete operations are
/// proposed through Raft instead of being applied directly. The state machine
/// applies them on all nodes via `AuthApplier`.
pub trait KeyProposer: Send + Sync {
    /// Propose a key create or update through Raft.
    /// `key` is the fully-formed Key (with timestamps set).
    fn propose_key_put(
        &self,
        key: &Key,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Propose a key deletion through Raft.
    fn propose_key_delete(
        &self,
        uid: Uuid,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Clone)]
pub struct AuthController {
    store: HeedAuthStore,
    master_key: Option<String>,
    key_proposer: Arc<std::sync::OnceLock<Arc<dyn KeyProposer>>>,
}

impl AuthController {
    pub fn new(auth_env: Env<WithoutTls>, master_key: &Option<String>) -> Result<Self> {
        let store = HeedAuthStore::new(auth_env)?;

        if store.is_empty()? {
            generate_default_keys(&store)?;
        }

        Ok(Self {
            store,
            master_key: master_key.clone(),
            key_proposer: Arc::new(std::sync::OnceLock::new()),
        })
    }

    /// Set the key proposer for cluster mode.
    /// When set, key mutations are proposed through Raft instead of applied directly.
    pub fn set_key_proposer(&self, proposer: Arc<dyn KeyProposer>) {
        let _ = self.key_proposer.set(proposer);
    }

    /// Return `Ok(())` if the auth controller is able to access one of its database.
    pub fn health(&self) -> Result<()> {
        self.store.health()?;
        Ok(())
    }

    /// Return the size of the `AuthController` database in bytes.
    pub fn size(&self) -> Result<u64> {
        self.store.size()
    }

    /// Return the used size of the `AuthController` database in bytes.
    pub fn used_size(&self) -> Result<u64> {
        self.store.used_size()
    }

    pub fn create_key(&self, create_key: CreateApiKey) -> Result<Key> {
        match self.store.get_api_key(create_key.uid)? {
            Some(_) => Err(AuthControllerError::ApiKeyAlreadyExists(create_key.uid.to_string())),
            None => {
                let key = create_key.to_key();
                if let Some(proposer) = self.key_proposer.get() {
                    // Cluster mode: propose through Raft. The state machine will
                    // apply it to the store on all nodes via AuthApplier.
                    proposer
                        .propose_key_put(&key)
                        .map_err(classify_proposer_error)?;
                    Ok(key)
                } else {
                    // Standalone: write directly to the store
                    self.store.put_api_key(key)
                }
            }
        }
    }

    pub fn update_key(&self, uid: Uuid, patch: PatchApiKey) -> Result<Key> {
        let mut key = self.get_key(uid)?;
        match patch.description {
            Setting::NotSet => (),
            description => key.description = description.set(),
        };
        match patch.name {
            Setting::NotSet => (),
            name => key.name = name.set(),
        };
        key.updated_at = OffsetDateTime::now_utc();

        if let Some(proposer) = self.key_proposer.get() {
            proposer.propose_key_put(&key).map_err(classify_proposer_error)?;
            Ok(key)
        } else {
            self.store.put_api_key(key)
        }
    }

    pub fn get_key(&self, uid: Uuid) -> Result<Key> {
        self.store
            .get_api_key(uid)?
            .ok_or_else(|| AuthControllerError::ApiKeyNotFound(uid.to_string()))
    }

    pub fn get_optional_uid_from_encoded_key(&self, encoded_key: &[u8]) -> Result<Option<Uuid>> {
        match &self.master_key {
            Some(master_key) => {
                self.store.get_uid_from_encoded_key(encoded_key, master_key.as_bytes())
            }
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
        let key = self.get_key(uid)?;

        let key_authorized_indexes = SearchRules::Set(key.indexes.into_iter().collect());

        let allow_index_creation = self.is_key_authorized(uid, Action::IndexesAdd, None)?;

        Ok(AuthFilter { search_rules, key_authorized_indexes, allow_index_creation })
    }

    pub fn list_keys(&self) -> Result<Vec<Key>> {
        self.store.list_api_keys()
    }

    pub fn delete_key(&self, uid: Uuid) -> Result<()> {
        if let Some(proposer) = self.key_proposer.get() {
            // Check key exists before proposing delete through Raft
            if self.store.get_api_key(uid)?.is_none() {
                return Err(AuthControllerError::ApiKeyNotFound(uid.to_string()));
            }
            proposer.propose_key_delete(uid).map_err(classify_proposer_error)?;
            Ok(())
        } else if self.store.delete_api_key(uid)? {
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
        self.master_key.as_ref().map(|master_key| generate_key_as_hexa(uid, master_key.as_bytes()))
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
                Some(index) => self.store.get_expiration_date(uid, action, Some(index))?,
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

    /// Delete all the keys in the DB.
    pub fn raw_delete_all_keys(&mut self) -> Result<()> {
        self.store.delete_all_keys()
    }

    /// Insert a key directly into the store (for replication/recovery).
    /// When `raft_log_index > 0`, checks idempotency to prevent duplicate application.
    pub fn raw_insert_key(&self, key: Key, raft_log_index: u64) -> Result<()> {
        if raft_log_index > 0 && self.store.raft_already_applied(raft_log_index)? {
            return Ok(());
        }
        self.store.put_api_key(key)?;
        if raft_log_index > 0 {
            self.store.set_raft_last_applied(raft_log_index)?;
        }
        Ok(())
    }

    /// Replace all keys in the auth store with the snapshot contents.
    /// Clears all existing keys (including inverted indexes) and inserts the snapshot keys.
    pub fn install_snapshot_keys(
        &self,
        key_bytes_list: &[Vec<u8>],
        last_applied_log_index: u64,
    ) -> Result<()> {
        self.store.clear_all_auth_data()?;
        for key_bytes in key_bytes_list {
            let key: Key = serde_json::from_slice(key_bytes)
                .map_err(|e| AuthControllerError::Internal(Box::new(e)))?;
            self.store.put_api_key(key)?;
        }
        if last_applied_log_index > 0 {
            self.store.set_raft_last_applied(last_applied_log_index)?;
        }
        Ok(())
    }

    /// Delete a key from the store without error if not found (for replication).
    /// When `raft_log_index > 0`, checks idempotency to prevent duplicate application.
    pub fn raw_delete_key(&self, uid: Uuid, raft_log_index: u64) -> Result<()> {
        if raft_log_index > 0 && self.store.raft_already_applied(raft_log_index)? {
            return Ok(());
        }
        self.store.delete_api_key(uid)?;
        if raft_log_index > 0 {
            self.store.set_raft_last_applied(raft_log_index)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct AuthFilter {
    search_rules: Option<SearchRules>,
    key_authorized_indexes: SearchRules,
    allow_index_creation: bool,
}

impl Default for AuthFilter {
    fn default() -> Self {
        Self {
            search_rules: None,
            key_authorized_indexes: SearchRules::default(),
            allow_index_creation: true,
        }
    }
}

impl AuthFilter {
    #[inline]
    pub fn allow_index_creation(&self, index: &str) -> bool {
        self.allow_index_creation && self.is_index_authorized(index)
    }

    #[inline]
    /// Return true if a tenant token was used to generate the search rules.
    pub fn is_tenant_token(&self) -> bool {
        self.search_rules.is_some()
    }

    pub fn with_allowed_indexes(allowed_indexes: HashSet<IndexUidPattern>) -> Self {
        Self {
            search_rules: None,
            key_authorized_indexes: SearchRules::Set(allowed_indexes),
            allow_index_creation: false,
        }
    }

    pub fn all_indexes_authorized(&self) -> bool {
        self.key_authorized_indexes.all_indexes_authorized()
            && self
                .search_rules
                .as_ref()
                .map(|search_rules| search_rules.all_indexes_authorized())
                .unwrap_or(true)
    }

    /// Check if the index is authorized by the API key and the tenant token.
    pub fn is_index_authorized(&self, index: &str) -> bool {
        self.key_authorized_indexes.is_index_authorized(index)
            && self
                .search_rules
                .as_ref()
                .map(|search_rules| search_rules.is_index_authorized(index))
                .unwrap_or(true)
    }

    /// Only check if the index is authorized by the API key
    pub fn api_key_is_index_authorized(&self, index: &str) -> bool {
        self.key_authorized_indexes.is_index_authorized(index)
    }

    /// Only check if the index is authorized by the tenant token
    pub fn tenant_token_is_index_authorized(&self, index: &str) -> bool {
        self.search_rules
            .as_ref()
            .map(|search_rules| search_rules.is_index_authorized(index))
            .unwrap_or(true)
    }

    /// Return the list of authorized indexes by the tenant token if any
    pub fn tenant_token_list_index_authorized(&self) -> Vec<String> {
        match self.search_rules {
            Some(ref search_rules) => {
                let mut indexes: Vec<_> = match search_rules {
                    SearchRules::Set(set) => set.iter().map(|s| s.to_string()).collect(),
                    SearchRules::Map(map) => map.keys().map(|s| s.to_string()).collect(),
                };
                indexes.sort_unstable();
                indexes
            }
            None => Vec::new(),
        }
    }

    /// Return the list of authorized indexes by the api key if any
    pub fn api_key_list_index_authorized(&self) -> Vec<String> {
        let mut indexes: Vec<_> = match self.key_authorized_indexes {
            SearchRules::Set(ref set) => set.iter().map(|s| s.to_string()).collect(),
            SearchRules::Map(ref map) => map.keys().map(|s| s.to_string()).collect(),
        };
        indexes.sort_unstable();
        indexes
    }

    pub fn get_index_search_rules(&self, index: &str) -> Option<IndexSearchRules> {
        if !self.is_index_authorized(index) {
            return None;
        }
        let search_rules = self.search_rules.as_ref().unwrap_or(&self.key_authorized_indexes);
        search_rules.get_index_search_rules(index)
    }
}

/// Transparent wrapper around a list of allowed indexes with the search rules to apply for each.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SearchRules {
    Set(HashSet<IndexUidPattern>),
    Map(HashMap<IndexUidPattern, Option<IndexSearchRules>>),
}

impl Default for SearchRules {
    fn default() -> Self {
        Self::Set(hashset! { IndexUidPattern::all() })
    }
}

impl SearchRules {
    fn is_index_authorized(&self, index: &str) -> bool {
        match self {
            Self::Set(set) => {
                set.contains("*")
                    || set.contains(index)
                    || set.iter().any(|pattern| pattern.matches_str(index))
            }
            Self::Map(map) => {
                map.contains_key("*")
                    || map.contains_key(index)
                    || map.keys().any(|pattern| pattern.matches_str(index))
            }
        }
    }

    fn get_index_search_rules(&self, index: &str) -> Option<IndexSearchRules> {
        match self {
            Self::Set(_) => {
                if self.is_index_authorized(index) {
                    Some(IndexSearchRules::default())
                } else {
                    None
                }
            }
            Self::Map(map) => {
                // We must take the most retrictive rule of this index uid patterns set of rules.
                map.iter()
                    .filter(|(pattern, _)| pattern.matches_str(index))
                    .max_by_key(|(pattern, _)| (pattern.is_exact(), pattern.len()))
                    .and_then(|(_, rule)| rule.clone())
            }
        }
    }

    fn all_indexes_authorized(&self) -> bool {
        match self {
            SearchRules::Set(set) => set.contains("*"),
            SearchRules::Map(map) => map.contains_key("*"),
        }
    }
}

impl IntoIterator for SearchRules {
    type Item = (IndexUidPattern, IndexSearchRules);
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
    store.put_api_key(Key::default_chat())?;
    store.put_api_key(Key::default_read_only_admin())?;
    store.put_api_key(Key::default_admin())?;
    store.put_api_key(Key::default_search())?;

    Ok(())
}

pub const MASTER_KEY_MIN_SIZE: usize = 16;
const MASTER_KEY_GEN_SIZE: usize = 32;

pub fn generate_master_key() -> String {
    use base64::Engine;
    use rand::rngs::OsRng;
    use rand::RngCore;

    // We need to use a cryptographically-secure source of randomness. That's why we're using the OsRng; https://crates.io/crates/getrandom
    let mut csprng = OsRng;
    let mut buf = vec![0; MASTER_KEY_GEN_SIZE];
    csprng.fill_bytes(&mut buf);

    // let's encode the random bytes to base64 to make them human-readable and not too long.
    // We're using the URL_SAFE alphabet that will produce keys without =, / or other unusual characters.
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}
