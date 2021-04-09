use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sha2::Digest;

use crate::index::Settings;
use crate::index_controller::{IndexController, IndexStats};
use crate::index_controller::{IndexMetadata, IndexSettings};
use crate::option::Opt;

pub mod search;
mod updates;

#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner>,
}

impl Deref for Data {
    type Target = DataInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct DataInner {
    pub index_controller: IndexController,
    pub api_keys: ApiKeys,
    options: Opt,
}

#[derive(Clone)]
pub struct ApiKeys {
    pub public: Option<String>,
    pub private: Option<String>,
    pub master: Option<String>,
}

#[derive(Default)]
pub struct Stats {
    pub database_size: u64,
    pub last_update: Option<DateTime<Utc>>,
    pub indexes: HashMap<String, IndexStats>,
}

impl ApiKeys {
    pub fn generate_missing_api_keys(&mut self) {
        if let Some(master_key) = &self.master {
            if self.private.is_none() {
                let key = format!("{}-private", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.private = Some(format!("{:x}", sha));
            }
            if self.public.is_none() {
                let key = format!("{}-public", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.public = Some(format!("{:x}", sha));
            }
        }
    }
}

impl Data {
    pub fn new(options: Opt) -> anyhow::Result<Data> {
        let path = options.db_path.clone();

        let index_controller = IndexController::new(&path, &options)?;

        let mut api_keys = ApiKeys {
            master: options.clone().master_key,
            private: None,
            public: None,
        };

        api_keys.generate_missing_api_keys();

        let inner = DataInner {
            index_controller,
            options,
            api_keys,
        };
        let inner = Arc::new(inner);

        Ok(Data { inner })
    }

    pub async fn settings(&self, uid: String) -> anyhow::Result<Settings> {
        self.index_controller.settings(uid).await
    }

    pub async fn list_indexes(&self) -> anyhow::Result<Vec<IndexMetadata>> {
        self.index_controller.list_indexes().await
    }

    pub async fn index(&self, uid: String) -> anyhow::Result<IndexMetadata> {
        self.index_controller.get_index(uid).await
    }

    pub async fn create_index(
        &self,
        uid: String,
        primary_key: Option<String>,
    ) -> anyhow::Result<IndexMetadata> {
        let settings = IndexSettings {
            uid: Some(uid),
            primary_key,
        };

        let meta = self.index_controller.create_index(settings).await?;
        Ok(meta)
    }

    pub async fn get_index_stats(&self, uid: String) -> anyhow::Result<IndexStats> {
        Ok(self.index_controller.get_stats(uid).await?)
    }

    pub async fn get_stats(&self) -> anyhow::Result<Stats> {
        let mut stats = Stats::default();
        stats.database_size += self.index_controller.get_uuids_size().await?;

        for index in self.index_controller.list_indexes().await? {
            let index_stats = self.index_controller.get_stats(index.uid.clone()).await?;

            stats.database_size += index_stats.size;
            stats.database_size += self
                .index_controller
                .get_updates_size(index.uid.clone())
                .await?;

            stats.last_update = Some(match stats.last_update {
                Some(last_update) => last_update.max(index.meta.updated_at),
                None => index.meta.updated_at,
            });

            stats.indexes.insert(index.uid, index_stats);
        }

        Ok(stats)
    }

    #[inline]
    pub fn http_payload_size_limit(&self) -> usize {
        self.options.http_payload_size_limit.get_bytes() as usize
    }

    #[inline]
    pub fn api_keys(&self) -> &ApiKeys {
        &self.api_keys
    }
}
