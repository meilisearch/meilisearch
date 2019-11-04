use std::collections::hash_map::{Entry, HashMap};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::{fs, thread};

use crossbeam_channel::Receiver;
use heed::types::{Str, Unit};
use heed::{CompactionOption, Result as ZResult};
use log::{debug, error};

use crate::{store, update, Index, MResult};

pub type BoxUpdateFn = Box<dyn Fn(update::ProcessedUpdateResult) + Send + Sync + 'static>;
type ArcSwapFn = arc_swap::ArcSwapOption<BoxUpdateFn>;

pub struct Database {
    pub env: heed::Env,
    common_store: heed::PolyDatabase,
    indexes_store: heed::Database<Str, Unit>,
    indexes: RwLock<HashMap<String, (Index, Arc<ArcSwapFn>, thread::JoinHandle<()>)>>,
}

macro_rules! r#break_try {
    ($expr:expr, $msg:tt) => {
        match $expr {
            core::result::Result::Ok(val) => val,
            core::result::Result::Err(err) => {
                log::error!(concat!($msg, ": {}"), err);
                break;
            }
        }
    };
}

fn update_awaiter(receiver: Receiver<()>, env: heed::Env, update_fn: Arc<ArcSwapFn>, index: Index) {
    for () in receiver {
        // consume all updates in order (oldest first)
        loop {
            // instantiate a main/parent transaction
            let mut writer = break_try!(env.write_txn(), "LMDB write transaction begin failed");

            // retrieve the update that needs to be processed
            let result = index.updates.pop_front(&mut writer);
            let (update_id, update) = match break_try!(result, "pop front update failed") {
                Some(value) => value,
                None => {
                    debug!("no more updates");
                    writer.abort();
                    break;
                }
            };

            // instantiate a nested transaction
            let result = env.nested_write_txn(&mut writer);
            let mut nested_writer = break_try!(result, "LMDB nested write transaction failed");

            // try to apply the update to the database using the nested transaction
            let result = update::update_task(&mut nested_writer, index.clone(), update_id, update);
            let status = break_try!(result, "update task failed");

            // commit the nested transaction if the update was successful, abort it otherwise
            if status.result.is_ok() {
                break_try!(nested_writer.commit(), "commit nested transaction failed");
            } else {
                nested_writer.abort()
            }

            // write the result of the update in the updates-results store
            let updates_results = index.updates_results;
            let result = updates_results.put_update_result(&mut writer, update_id, &status);

            // always commit the main/parent transaction, even if the update was unsuccessful
            break_try!(result, "update result store commit failed");
            break_try!(writer.commit(), "update parent transaction failed");

            // call the user callback when the update and the result are written consistently
            if let Some(ref callback) = *update_fn.load() {
                (callback)(status);
            }
        }
    }
}

impl Database {
    pub fn open_or_create(path: impl AsRef<Path>) -> MResult<Database> {
        fs::create_dir_all(path.as_ref())?;

        let env = heed::EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024 * 1024) // 10GB
            .max_dbs(3000)
            .open(path)?;

        let common_store = env.create_poly_database(Some("common"))?;
        let indexes_store = env.create_database::<Str, Unit>(Some("indexes"))?;

        // list all indexes that needs to be opened
        let mut must_open = Vec::new();
        let reader = env.read_txn()?;
        for result in indexes_store.iter(&reader)? {
            let (index_name, _) = result?;
            must_open.push(index_name.to_owned());
        }

        reader.abort();

        // open the previously aggregated indexes
        let mut indexes = HashMap::new();
        for index_name in must_open {
            let (sender, receiver) = crossbeam_channel::bounded(100);
            let index = match store::open(&env, &index_name, sender.clone())? {
                Some(index) => index,
                None => {
                    log::warn!(
                        "the index {} doesn't exist or has not all the databases",
                        index_name
                    );
                    continue;
                }
            };
            let update_fn = Arc::new(ArcSwapFn::empty());

            let env_clone = env.clone();
            let index_clone = index.clone();
            let update_fn_clone = update_fn.clone();

            let handle = thread::spawn(move || {
                update_awaiter(receiver, env_clone, update_fn_clone, index_clone)
            });

            // send an update notification to make sure that
            // possible pre-boot updates are consumed
            sender.send(()).unwrap();

            let result = indexes.insert(index_name, (index, update_fn, handle));
            assert!(
                result.is_none(),
                "The index should not have been already open"
            );
        }

        Ok(Database {
            env,
            common_store,
            indexes_store,
            indexes: RwLock::new(indexes),
        })
    }

    pub fn open_index(&self, name: impl AsRef<str>) -> Option<Index> {
        let indexes_lock = self.indexes.read().unwrap();
        match indexes_lock.get(name.as_ref()) {
            Some((index, ..)) => Some(index.clone()),
            None => None,
        }
    }

    pub fn create_index(&self, name: impl AsRef<str>) -> MResult<Index> {
        let name = name.as_ref();
        let mut indexes_lock = self.indexes.write().unwrap();

        match indexes_lock.entry(name.to_owned()) {
            Entry::Occupied(_) => Err(crate::Error::IndexAlreadyExists),
            Entry::Vacant(entry) => {
                let (sender, receiver) = crossbeam_channel::bounded(100);
                let index = store::create(&self.env, name, sender)?;

                let mut writer = self.env.write_txn()?;
                self.indexes_store.put(&mut writer, name, &())?;

                let env_clone = self.env.clone();
                let index_clone = index.clone();

                let no_update_fn = Arc::new(ArcSwapFn::empty());
                let no_update_fn_clone = no_update_fn.clone();

                let handle = thread::spawn(move || {
                    update_awaiter(receiver, env_clone, no_update_fn_clone, index_clone)
                });

                writer.commit()?;
                entry.insert((index.clone(), no_update_fn, handle));

                Ok(index)
            }
        }
    }

    pub fn set_update_callback(&self, name: impl AsRef<str>, update_fn: BoxUpdateFn) -> bool {
        let indexes_lock = self.indexes.read().unwrap();
        match indexes_lock.get(name.as_ref()) {
            Some((_, current_update_fn, _)) => {
                let update_fn = Some(Arc::new(update_fn));
                current_update_fn.swap(update_fn);
                true
            }
            None => false,
        }
    }

    pub fn unset_update_callback(&self, name: impl AsRef<str>) -> bool {
        let indexes_lock = self.indexes.read().unwrap();
        match indexes_lock.get(name.as_ref()) {
            Some((_, current_update_fn, _)) => {
                current_update_fn.swap(None);
                true
            }
            None => false,
        }
    }

    pub fn copy_and_compact_to_path<P: AsRef<Path>>(&self, path: P) -> ZResult<File> {
        self.env.copy_to_path(path, CompactionOption::Enabled)
    }

    pub fn indexes_names(&self) -> MResult<Vec<String>> {
        let indexes = self.indexes.read().unwrap();
        Ok(indexes.keys().cloned().collect())
    }

    pub fn common_store(&self) -> heed::PolyDatabase {
        self.common_store
    }
}
