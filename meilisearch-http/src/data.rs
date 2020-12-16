use std::error::Error;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use meilisearch_core::{Database, DatabaseOptions, Index};
use sha2::Digest;

use crate::error::{Error as MSError, ResponseError};
use crate::index_update_callback;
use crate::option::Opt;
use crate::dump::DumpInfo;

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
    pub db: Arc<Database>,
    pub db_path: String,
    pub dumps_dir: PathBuf,
    pub dump_batch_size: usize,
    pub api_keys: ApiKeys,
    pub server_pid: u32,
    pub http_payload_size_limit: usize,
    pub current_dump: Arc<Mutex<Option<DumpInfo>>>,
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
    pub fn new(opt: Opt) -> Result<Data, Box<dyn Error>> {
        let db_path = opt.db_path.clone();
        let dumps_dir = opt.dumps_dir.clone();
        let dump_batch_size = opt.dump_batch_size;
        let server_pid = std::process::id();

        let db_opt = DatabaseOptions {
            main_map_size: opt.max_mdb_size,
            update_map_size: opt.max_udb_size,
        };

        let http_payload_size_limit = opt.http_payload_size_limit;

        let db = Arc::new(Database::open_or_create(opt.db_path, db_opt)?);

        let mut api_keys = ApiKeys {
            master: opt.master_key,
            private: None,
            public: None,
        };

        api_keys.generate_missing_api_keys();

        let current_dump = Arc::new(Mutex::new(None));

        let inner_data = DataInner {
            db: db.clone(),
            db_path,
            dumps_dir,
            dump_batch_size,
            api_keys,
            server_pid,
            http_payload_size_limit,
            current_dump,
        };

        let data = Data {
            inner: Arc::new(inner_data),
        };

        let callback_context = data.clone();
        db.set_update_callback(Box::new(move |index_uid, status| {
            index_update_callback(&index_uid, &callback_context, status);
        }));

        Ok(data)
    }

    fn create_index(&self, uid: &str) -> Result<Index, ResponseError> {
        if !uid
            .chars()
            .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
        {
            return Err(MSError::InvalidIndexUid.into());
        }

        let created_index = self.db.create_index(&uid).map_err(|e| match e {
            meilisearch_core::Error::IndexAlreadyExists => e.into(),
            _ => ResponseError::from(MSError::create_index(e)),
        })?;

        self.db.main_write::<_, _, ResponseError>(|mut writer| {
            created_index.main.put_name(&mut writer, uid)?;

            created_index
                .main
                .created_at(&writer)?
                .ok_or(MSError::internal("Impossible to read created at"))?;

            created_index
                .main
                .updated_at(&writer)?
                .ok_or(MSError::internal("Impossible to read updated at"))?;
            Ok(())
        })?;

        Ok(created_index)
    }

    pub fn get_current_dump_info(&self) -> Option<DumpInfo> {
        self.current_dump.lock().unwrap().clone()
    }

    pub fn set_current_dump_info(&self, dump_info: DumpInfo) {
        self.current_dump.lock().unwrap().replace(dump_info);
    }

    pub fn get_or_create_index<F, R>(&self, uid: &str, f: F) -> Result<R, ResponseError>
    where
        F: FnOnce(&Index) -> Result<R, ResponseError>,
    {
        let mut index_has_been_created = false;

        let index = match self.db.open_index(&uid) {
            Some(index) => index,
            None => {
                index_has_been_created = true;
                self.create_index(&uid)?
            }
        };

        match f(&index) {
            Ok(r) => Ok(r),
            Err(err) => {
                if index_has_been_created {
                    let _ = self.db.delete_index(&uid);
                }
                Err(err)
            }
        }
    }
}
