use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};
use crate::{store, Index, MResult};

pub struct Database {
    pub rkv: Arc<RwLock<rkv::Rkv>>,
    main_store: rkv::SingleStore,
    indexes: RwLock<HashMap<String, Index>>,
}

impl Database {
    pub fn open_or_create(path: impl AsRef<Path>) -> MResult<Database> {
        let manager = rkv::Manager::singleton();
        let mut rkv_write = manager.write().unwrap();

        let rkv = rkv_write
            .get_or_create(path.as_ref(), |path| {
                let mut builder = rkv::Rkv::environment_builder();
                builder.set_max_dbs(3000).set_map_size(10 * 1024 * 1024 * 1024); // 10GB
                rkv::Rkv::from_env(path, builder)
            })?;

        drop(rkv_write);

        let rkv_read = rkv.read().unwrap();
        let create_options = rkv::store::Options::create();
        let main_store = rkv_read.open_single("indexes", create_options)?;

        // list all indexes that needs to be opened
        let mut must_open = Vec::new();
        let reader = rkv_read.read()?;
        for result in main_store.iter_start(&reader)? {
            let (key, _) = result?;
            if let Ok(index_name) = std::str::from_utf8(key) {
                must_open.push(index_name.to_owned());
            }
        }

        drop(reader);

        // open the previously aggregated indexes
        let mut indexes = HashMap::new();
        for index_name in must_open {
            let index = store::open(&rkv_read, &index_name)?;
            indexes.insert(index_name, index);
        }

        drop(rkv_read);

        Ok(Database { rkv, main_store, indexes: RwLock::new(indexes) })
    }

    pub fn open_index(&self, name: impl Into<String>) -> MResult<Index> {
        let indexes_lock = self.indexes.read().unwrap();
        let name = name.into();

        match indexes_lock.get(&name) {
            Some(index) => Ok(*index),
            None => {
                drop(indexes_lock);

                let rkv_lock = self.rkv.read().unwrap();
                let index = store::create(&rkv_lock, &name)?;

                let mut writer = rkv_lock.write()?;
                let value = rkv::Value::Blob(&[]);
                self.main_store.put(&mut writer, &name, &value)?;

                {
                    let mut indexes_write = self.indexes.write().unwrap();
                    indexes_write.entry(name).or_insert(index);
                }

                writer.commit()?;

                Ok(index)
            },
        }
    }

    pub fn indexes_names(&self) -> MResult<Vec<String>> {
        let indexes = self.indexes.read().unwrap();
        Ok(indexes.keys().cloned().collect())
    }
}
