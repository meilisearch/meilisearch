use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::{fs, thread};

use crossbeam_channel::Receiver;
use log::{debug, error};

use crate::{store, update, Index, MResult};

pub type BoxUpdateFn = Box<dyn Fn(update::UpdateResult) + Send + Sync + 'static>;
type ArcSwapFn = arc_swap::ArcSwapOption<BoxUpdateFn>;

pub struct Database {
    pub rkv: Arc<RwLock<rkv::Rkv>>,
    main_store: rkv::SingleStore,
    indexes_store: rkv::SingleStore,
    indexes: RwLock<HashMap<String, (Index, Arc<ArcSwapFn>, thread::JoinHandle<()>)>>,
}

fn update_awaiter(
    receiver: Receiver<()>,
    rkv: Arc<RwLock<rkv::Rkv>>,
    update_fn: Arc<ArcSwapFn>,
    index: Index,
)
{
    for () in receiver {
        // consume all updates in order (oldest first)
        loop {
            let rkv = match rkv.read() {
                Ok(rkv) => rkv,
                Err(e) => { error!("rkv RwLock read failed: {}", e); break }
            };

            let mut writer = match rkv.write() {
                Ok(writer) => writer,
                Err(e) => { error!("LMDB writer transaction begin failed: {}", e); break }
            };

            match update::update_task(&mut writer, index.clone()) {
                Ok(Some(status)) => {
                    if let Err(e) = writer.commit() { error!("update transaction failed: {}", e) }

                    if let Some(ref callback) = *update_fn.load() {
                        (callback)(status);
                    }
                },
                // no more updates to handle for now
                Ok(None) => { debug!("no more updates"); writer.abort(); break },
                Err(e) => { error!("update task failed: {}", e); writer.abort() },
            }
        }
    }
}

impl Database {
    pub fn open_or_create(path: impl AsRef<Path>) -> MResult<Database> {
        let manager = rkv::Manager::singleton();
        let mut rkv_write = manager.write().unwrap();

        fs::create_dir_all(path.as_ref())?;

        let rkv = rkv_write
            .get_or_create(path.as_ref(), |path| {
                let mut builder = rkv::Rkv::environment_builder();
                builder.set_max_dbs(3000).set_map_size(10 * 1024 * 1024 * 1024); // 10GB
                rkv::Rkv::from_env(path, builder)
            })?;

        drop(rkv_write);

        let rkv_read = rkv.read().unwrap();
        let create_options = rkv::store::Options::create();
        let main_store = rkv_read.open_single("main", create_options)?;
        let indexes_store = rkv_read.open_single("indexes", create_options)?;

        // list all indexes that needs to be opened
        let mut must_open = Vec::new();
        let reader = rkv_read.read()?;
        for result in indexes_store.iter_start(&reader)? {
            let (key, _) = result?;
            if let Ok(index_name) = std::str::from_utf8(key) {
                must_open.push(index_name.to_owned());
            }
        }

        drop(reader);

        // open the previously aggregated indexes
        let mut indexes = HashMap::new();
        for index_name in must_open {

            let (sender, receiver) = crossbeam_channel::bounded(100);
            let index = store::open(&rkv_read, &index_name, sender.clone())?;
            let update_fn = Arc::new(ArcSwapFn::empty());

            let rkv_clone = rkv.clone();
            let index_clone = index.clone();
            let update_fn_clone = update_fn.clone();

            let handle = thread::spawn(move || {
                update_awaiter(receiver, rkv_clone, update_fn_clone, index_clone)
            });

            // send an update notification to make sure that
            // possible previous boot updates are consumed
            sender.send(()).unwrap();

            let result = indexes.insert(index_name, (index, update_fn, handle));
            assert!(result.is_none(), "The index should not have been already open");
        }

        drop(rkv_read);

        Ok(Database { rkv, main_store, indexes_store, indexes: RwLock::new(indexes) })
    }

    pub fn open_index(
        &self,
        name: impl Into<String>,
        update_fn: Option<BoxUpdateFn>,
    ) -> MResult<Index>
    {
        let indexes_lock = self.indexes.read().unwrap();
        let name = name.into();

        match indexes_lock.get(&name) {
            Some((index, old_update_fn, _)) => {
                old_update_fn.swap(update_fn.map(Arc::new));
                Ok(index.clone())
            },
            None => {
                drop(indexes_lock);

                let rkv_lock = self.rkv.read().unwrap();
                let (sender, receiver) = crossbeam_channel::bounded(100);
                let index = store::create(&rkv_lock, &name, sender)?;

                let mut writer = rkv_lock.write()?;
                let value = rkv::Value::Blob(&[]);
                self.indexes_store.put(&mut writer, &name, &value)?;

                {
                    let mut indexes_write = self.indexes.write().unwrap();
                    indexes_write.entry(name).or_insert_with(|| {
                        let rkv_clone = self.rkv.clone();
                        let index_clone = index.clone();

                        let update_fn = update_fn.map(Arc::new);
                        let update_fn = Arc::new(ArcSwapFn::new(update_fn));
                        let update_fn_clone = update_fn.clone();

                        let handle = thread::spawn(move || {
                            update_awaiter(receiver, rkv_clone, update_fn_clone, index_clone)
                        });

                        (index.clone(), update_fn, handle)
                    });
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

    pub fn main_store(&self) -> rkv::SingleStore {
        self.main_store
    }
}
