use std::ops::Deref;
use std::sync::Arc;

use sha2::Digest;
use milli::Index;

use crate::option::Opt;
use crate::updates::UpdateQueue;

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
    pub indexes: Arc<Index>,
    pub update_queue: UpdateQueue,
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
        let path = options.db_path.join("main");
        let indexes = Index::new(&path, Some(db_size))?;
        let indexes = Arc::new(indexes);

        let update_queue = UpdateQueue::new(&options, indexes.clone())?;

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

    #[inline]
    pub fn http_payload_size_limit(&self) -> usize {
        self.options.http_payload_size_limit.get_bytes() as usize
    }

    pub fn api_keys(&self) -> &ApiKeys {
        &self.api_keys
    }
}
