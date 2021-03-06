pub mod search;
mod updates;

use std::fs::create_dir_all;
use std::ops::Deref;
use std::sync::Arc;

use sha2::Digest;

use crate::index_controller::{IndexMetadata, IndexSettings};
use crate::index_controller::IndexController;
use crate::index::Settings;
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

        //let indexer_opts = options.indexer_options.clone();

        create_dir_all(&path)?;
        let index_controller = IndexController::new(&path);

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

    pub async fn settings<S: AsRef<str>>(&self, index_uid: S) -> anyhow::Result<Settings> {
        self.index_controller.settings(index_uid.as_ref().to_string()).await
    }

    pub async fn list_indexes(&self) -> anyhow::Result<Vec<IndexMetadata>> {
        self.index_controller.list_indexes().await
    }

    pub fn index(&self, name: impl AsRef<str>) -> anyhow::Result<Option<IndexMetadata>> {
        todo!()
        //Ok(self
            //.list_indexes()?
            //.into_iter()
            //.find(|i| i.uid == name.as_ref()))
    }

    pub async fn create_index(&self, name: impl AsRef<str>, primary_key: Option<impl AsRef<str>>) -> anyhow::Result<IndexMetadata> {
        let settings = IndexSettings {
            name: Some(name.as_ref().to_string()),
            primary_key: primary_key.map(|s| s.as_ref().to_string()),
        };

        let meta = self.index_controller.create_index(settings).await?;
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
