mod search;
mod updates;

pub use search::{SearchQuery, SearchResult};

use std::ops::Deref;
use std::sync::Arc;

use sha2::Digest;

use crate::{option::Opt, index_controller::Settings};
use crate::index_controller::{IndexStore, UpdateStore};

#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner<UpdateStore>>,
}

impl Deref for Data {
    type Target = DataInner<UpdateStore>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone)]
pub struct DataInner<I> {
    pub indexes: Arc<I>,
    api_keys: ApiKeys,
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
        let index_store = IndexStore::new(&path)?;
        let index_controller = UpdateStore::new(index_store);
        let indexes = Arc::new(index_controller);

        let mut api_keys = ApiKeys {
            master: options.clone().master_key,
            private: None,
            public: None,
        };

        api_keys.generate_missing_api_keys();

        let inner = DataInner { indexes, options, api_keys };
        let inner = Arc::new(inner);

        Ok(Data { inner })
    }

    pub fn settings<S: AsRef<str>>(&self, index_uid: S) -> anyhow::Result<Settings> {
        let index = self.indexes
            .get(&index_uid)?
            .ok_or_else(|| anyhow::anyhow!("Index {} does not exist.", index_uid.as_ref()))?;

        let displayed_attributes = index
            .displayed_fields()?
            .map(|fields| fields.into_iter().map(String::from).collect())
            .unwrap_or_else(|| vec!["*".to_string()]);

        let searchable_attributes = index
            .searchable_fields()?
            .map(|fields| fields.into_iter().map(String::from).collect())
            .unwrap_or_else(|| vec!["*".to_string()]);

        let faceted_attributes = index.faceted_fields()?
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

    #[inline]
    pub fn http_payload_size_limit(&self) -> usize {
        self.options.http_payload_size_limit.get_bytes() as usize
    }

    #[inline]
    pub fn api_keys(&self) -> &ApiKeys {
        &self.api_keys
    }
}
