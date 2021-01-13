mod search;
mod updates;

pub use search::{SearchQuery, SearchResult};

use std::fs::create_dir_all;
use std::ops::Deref;
use std::sync::Arc;

use milli::Index;
use sha2::Digest;

use crate::{option::Opt, updates::Settings};
use crate::updates::UpdateQueue;
use crate::index_controller::IndexController;

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
    pub indexes: Arc<IndexController>,
    pub update_queue: Arc<UpdateQueue>,
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
        let db_size = options.max_mdb_size.get_bytes() as usize;
        let indexes = IndexController::new(&options.db_path)?;
        let indexes = Arc::new(indexes);

        let update_queue = Arc::new(UpdateQueue::new(&options, indexes.clone())?);

        let mut api_keys = ApiKeys {
            master: options.clone().master_key,
            private: None,
            public: None,
        };

        api_keys.generate_missing_api_keys();

        let inner = DataInner { indexes, options, update_queue, api_keys };
        let inner = Arc::new(inner);

        Ok(Data { inner })
    }

    pub fn settings<S: AsRef<str>>(&self, _index: S) -> anyhow::Result<Settings> {
        let txn = self.indexes.env.read_txn()?;
        let fields_map = self.indexes.fields_ids_map(&txn)?;
        println!("fields_map: {:?}", fields_map);

        let displayed_attributes = self.indexes
            .displayed_fields(&txn)?
            .map(|fields| fields.into_iter().map(String::from).collect())
            .unwrap_or_else(|| vec!["*".to_string()]);

        let searchable_attributes = self.indexes
            .searchable_fields(&txn)?
            .map(|fields| fields
                .into_iter()
                .map(String::from)
                .collect())
            .unwrap_or_else(|| vec!["*".to_string()]);

        let faceted_attributes = self.indexes.faceted_fields(&txn)?
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
