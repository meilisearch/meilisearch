use std::error::Error;
use std::ops::Deref;
use std::path::PathBuf;
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
    pub update_store: UpdateQueue,
    pub db_path: String,
    pub dumps_dir: PathBuf,
    pub dump_batch_size: usize,
    pub api_keys: ApiKeys,
    pub server_pid: u32,
    pub http_payload_size_limit: usize,
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
    pub fn new(_opt: Opt) -> Result<Data, Box<dyn Error>> {
        todo!()
    }
}
