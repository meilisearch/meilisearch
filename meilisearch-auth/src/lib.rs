mod dump;
pub mod error;
mod store;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use error::{AuthControllerError, Result};
use maplit::hashset;
use meilisearch_types::index_uid_pattern::IndexUidPattern;
use meilisearch_types::keys::{Action, CreateApiKey, Key, PatchApiKey};
use meilisearch_types::milli::update::Setting;
use serde::{Deserialize, Serialize};
pub use store::open_auth_store_env;
use store::{generate_key_as_hexa, HeedAuthStore};
use time::OffsetDateTime;
use uuid::Uuid;
use zookeeper::{
    Acl, AddWatchMode, CreateMode, WatchedEvent, WatchedEventType, ZkError, ZooKeeper,
};

#[derive(Clone)]
pub struct AuthController {
    store: Arc<HeedAuthStore>,
    master_key: Option<String>,
    zookeeper: Option<Arc<ZooKeeper>>,
}

impl AuthController {
    pub fn new(
        db_path: impl AsRef<Path>,
        master_key: &Option<String>,
        zookeeper: Option<Arc<ZooKeeper>>,
    ) -> Result<Self> {
        let store = HeedAuthStore::new(db_path)?;
        let controller = Self { store: Arc::new(store), master_key: master_key.clone(), zookeeper };

        match controller.zookeeper {
            // setup the auth zk environment, the `auth` node
            Some(ref zookeeper) => {
                // TODO: we should catch the potential unexpected errors here https://docs.rs/zookeeper-client/latest/zookeeper_client/struct.Client.html#method.create
                // for the moment we consider that `create` only returns Error::NodeExists.
                match zookeeper.create(
                    "/auth",
                    vec![],
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent,
                ) {
                    // If the store is empty, we must generate and push the default api-keys.
                    Ok(_) => generate_default_keys(&controller)?,
                    // If the node exist we should clear our DB and download all the existing api-keys
                    Err(ZkError::NodeExists) => {
                        log::warn!("Auth directory already exists, we need to clear our keys + import the one in zookeeper");

                        let store = controller.store.clone();
                        store.delete_all_keys()?;
                        let children = zookeeper
                            .get_children("/auth", false)
                            .expect("Internal, the auth directory was deleted during execution.");

                        log::info!("Importing {} api-keys", children.len());
                        for path in children {
                            log::info!("  Importing {}", path);
                            match zookeeper.get_data(&format!("/auth/{}", &path), false) {
                                Ok((key, _stat)) => {
                                    let key = serde_json::from_slice(&key).unwrap();
                                    let store = controller.store.clone();
                                    store.put_api_key(key)?;
                                }
                                Err(e) => panic!("{e}"),
                            }
                            // else the file was deleted while we were inserting the key. We ignore it.
                            // TODO: What happens if someone updates the files before we have the time
                            //       to setup the watcher
                        }
                    }
                    e @ Err(
                        ZkError::NoNode | ZkError::NoChildrenForEphemerals | ZkError::InvalidACL,
                    ) => unreachable!("{e:?}"),
                    Err(e) => panic!("{e}"),
                }
                // TODO: Race condition above:
                //       What happens if two node join exactly at the same moment:
                //       One will create the dir
                //       The second one will delete its DB, load nothing and install a watcher
                //       The first one will push its keys and should wake up and update the second one.
                //     / BUT, if the second one delete its DB and the first one push its files before the second one install the watcher we're fucked

                // Zookeeper Event listener loop
                let controller_clone = controller.clone();
                let zkk = zookeeper.clone();
                zookeeper.add_watch("/auth", AddWatchMode::PersistentRecursive, move |event| {
                    let WatchedEvent { event_type, path, keeper_state: _ } = dbg!(event);

                    match event_type {
                        WatchedEventType::NodeDeleted => {
                            // TODO: ugly unwraps
                            let path = path.unwrap();
                            let uuid = path.strip_prefix("/auth/").unwrap();
                            let uuid = Uuid::parse_str(&uuid).unwrap();
                            log::info!("The key {} has been deleted", uuid);
                            dbg!(controller_clone.store.delete_api_key(uuid).unwrap());
                        }
                        WatchedEventType::NodeCreated | WatchedEventType::NodeDataChanged => {
                            let path = path.unwrap();
                            let (key, _stat) = zkk.get_data(&path, false).unwrap();
                            let key: Key = serde_json::from_slice(&key).unwrap();
                            log::info!("The key {} has been deleted", key.uid);

                            dbg!(controller_clone.store.put_api_key(key).unwrap());
                        }
                        otherwise => panic!("Got the unexpected `{otherwise:?}` event!"),
                    }
                })?;
            }
            None => {
                if controller.store.is_empty()? {
                    generate_default_keys(&controller)?;
                }
            }
        }

        Ok(controller)
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
            None => self.put_key(create_key.to_key()),
        }
    }

    pub fn put_key(&self, key: Key) -> Result<Key> {
        let store = self.store.clone();
        // TODO: we may commit only after zk persisted the keys
        let key = store.put_api_key(key)?;
        if let Some(zookeeper) = &self.zookeeper {
            zookeeper.create(
                &format!("/auth/{}", key.uid),
                serde_json::to_vec_pretty(&key)?,
                Acl::open_unsafe().clone(),
                CreateMode::Persistent,
            )?;
        }
        Ok(key)
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
        let store = self.store.clone();
        // TODO: we may commit only after zk persisted the keys
        let key = store.put_api_key(key)?;
        if let Some(zookeeper) = &self.zookeeper {
            zookeeper.set_data(
                &format!("/auth/{}", key.uid),
                serde_json::to_vec_pretty(&key)?,
                None,
            )?;
        }
        Ok(key)
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
        let store = self.store.clone();
        let deleted = store.delete_api_key(uid)?;
        if deleted {
            if let Some(zookeeper) = &self.zookeeper {
                zookeeper.delete(&format!("/auth/{}", uid), None)?;
            }
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

    /// Insert a key in the DB without any check on its validity
    pub fn raw_insert_key(&mut self, key: Key) -> Result<()> {
        self.store.put_api_key(key)?;
        Ok(())
    }
}

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

    pub fn is_index_authorized(&self, index: &str) -> bool {
        self.key_authorized_indexes.is_index_authorized(index)
            && self
                .search_rules
                .as_ref()
                .map(|search_rules| search_rules.is_index_authorized(index))
                .unwrap_or(true)
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

fn generate_default_keys(controller: &AuthController) -> Result<()> {
    controller.put_key(Key::default_admin())?;
    controller.put_key(Key::default_search())?;
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
