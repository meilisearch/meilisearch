mod search;
mod updates;

pub use search::{SearchQuery, SearchResult, DEFAULT_SEARCH_LIMIT};

use std::fs::create_dir_all;
use std::ops::Deref;
use std::sync::Arc;

use sha2::Digest;
use anyhow::bail;

use crate::index_controller::{IndexController, LocalIndexController, IndexMetadata, Settings, IndexSettings};
use crate::option::Opt;

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

#[derive(Clone)]
pub struct DataInner {
    pub index_controller: Arc<LocalIndexController>,
    pub api_keys: ApiKeys,
    options: Opt,
}

#[derive(Clone)]
pub struct ApiKeys {
    pub public: Option<String>,
    pub private: Option<String>,
    pub master: Option<String>,
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
        let indexer_opts = options.indexer_options.clone();
        create_dir_all(&path)?;
        let index_controller = LocalIndexController::new(
            &path,
            indexer_opts,
            options.max_mdb_size.get_bytes(),
            options.max_udb_size.get_bytes(),
        )?;
        let index_controller = Arc::new(index_controller);

        let mut api_keys = ApiKeys {
            master: options.clone().master_key,
            private: None,
            public: None,
        };

        api_keys.generate_missing_api_keys();

        let inner = DataInner { index_controller, options, api_keys };
        let inner = Arc::new(inner);

        Ok(Data { inner })
    }

    pub fn settings<S: AsRef<str>>(&self, index_uid: S) -> anyhow::Result<Settings> {
        let index = self.index_controller
            .index(&index_uid)?
            .ok_or_else(|| anyhow::anyhow!("Index {} does not exist.", index_uid.as_ref()))?;

        let txn = index.read_txn()?;

        let displayed_attributes = index
            .displayed_fields(&txn)?
            .map(|fields| fields.into_iter().map(String::from).collect())
            .unwrap_or_else(|| vec!["*".to_string()]);

        let searchable_attributes = index
            .searchable_fields(&txn)?
            .map(|fields| fields.into_iter().map(String::from).collect())
            .unwrap_or_else(|| vec!["*".to_string()]);

        let faceted_attributes = index
            .faceted_fields(&txn)?
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();

        Ok(Settings {
            displayed_attributes: Some(Some(displayed_attributes)),
            searchable_attributes: Some(Some(searchable_attributes)),
            faceted_attributes: Some(Some(faceted_attributes)),
            criteria: None,
        })
    }

    pub fn list_indexes(&self) -> anyhow::Result<Vec<IndexMetadata>> {
        self.index_controller.list_indexes()
    }

    pub fn index(&self, name: impl AsRef<str>) -> anyhow::Result<Option<IndexMetadata>> {
        Ok(self
            .list_indexes()?
            .into_iter()
            .find(|i| i.uid == name.as_ref()))
    }

    pub fn create_index(&self, name: impl AsRef<str>, primary_key: Option<impl AsRef<str>>) -> anyhow::Result<IndexMetadata> {
        if !is_index_uid_valid(name.as_ref()) {
            bail!("invalid index uid: {:?}", name.as_ref())
        }
        let settings = IndexSettings {
            name: Some(name.as_ref().to_string()),
            primary_key: primary_key.map(|s| s.as_ref().to_string()),
        };

        let meta = self.index_controller.create_index(settings)?;
        Ok(meta)
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

fn is_index_uid_valid(uid: &str) -> bool {
    uid.chars().all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
}

