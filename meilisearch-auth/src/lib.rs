mod action;
pub mod error;
mod key;
mod store;

use std::path::Path;
use std::str::from_utf8;
use std::sync::Arc;

use chrono::Utc;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub use action::{actions, Action};
use error::{AuthControllerError, Result};
pub use key::Key;
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

    pub fn get_key_filters(&self, key: impl AsRef<str>) -> Result<AuthFilter> {
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
                filters.indexes = Some(key.indexes);
            }
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

    pub fn authenticate(&self, token: &[u8], action: Action, index: Option<&[u8]>) -> Result<bool> {
        if let Some(master_key) = &self.master_key {
            if let Some((id, exp)) = self
                .store
                // check if the key has access to all indexes.
                .get_expiration_date(token, action, None)?
                .or(match index {
                    // else check if the key has access to the requested index.
                    Some(index) => self.store.get_expiration_date(token, action, Some(index))?,
                    // or to any index if no index has been requested.
                    None => self.store.prefix_first_expiration_date(token, action)?,
                })
            {
                let id = from_utf8(&id).map_err(|e| AuthControllerError::Internal(Box::new(e)))?;
                if exp.map_or(true, |exp| Utc::now() < exp)
                    && generate_key(master_key.as_bytes(), id).as_bytes() == token
                {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}

#[derive(Default)]
pub struct AuthFilter {
    pub indexes: Option<Vec<String>>,
}

pub fn generate_key(master_key: &[u8], uid: &str) -> String {
    let key = [uid.as_bytes(), master_key].concat();
    let sha = Sha256::digest(&key);
    format!("{}{:x}", uid, sha)
}

fn generate_default_keys(store: &HeedAuthStore) -> Result<()> {
    store.put_api_key(Key::default_admin())?;
    store.put_api_key(Key::default_search())?;

    Ok(())
}
