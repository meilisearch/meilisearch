use std::collections::hash_map::{Entry, HashMap};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::{fs, thread};
use std::io::{Read, Write, ErrorKind};

use chrono::{DateTime, Utc};
use crossbeam_channel::{Receiver, Sender};
use heed::CompactionOption;
use heed::types::{Str, Unit, SerdeBincode};
use log::{debug, error};
use meilisearch_schema::Schema;
use regex::Regex;

use crate::{store, update, Index, MResult, Error};

pub type BoxUpdateFn = Box<dyn Fn(&str, update::ProcessedUpdateResult) + Send + Sync + 'static>;

type ArcSwapFn = arc_swap::ArcSwapOption<BoxUpdateFn>;

type SerdeDatetime = SerdeBincode<DateTime<Utc>>;

pub type MainWriter<'a, 'b> = heed::RwTxn<'a, 'b, MainT>;
pub type MainReader<'a, 'b> = heed::RoTxn<'a, MainT>;

pub type UpdateWriter<'a, 'b> = heed::RwTxn<'a, 'b, UpdateT>;
pub type UpdateReader<'a> = heed::RoTxn<'a, UpdateT>;

const LAST_UPDATE_KEY: &str = "last-update";

pub struct MainT;
pub struct UpdateT;

pub struct Database {
    env: heed::Env,
    update_env: heed::Env,
    common_store: heed::PolyDatabase,
    indexes_store: heed::Database<Str, Unit>,
    indexes: RwLock<HashMap<String, (Index, thread::JoinHandle<MResult<()>>)>>,
    update_fn: Arc<ArcSwapFn>,
    database_version: (u32, u32, u32),
}

pub struct DatabaseOptions {
    pub main_map_size: usize,
    pub update_map_size: usize,
}

impl Default for DatabaseOptions {
    fn default() -> DatabaseOptions {
        DatabaseOptions {
            main_map_size: 100 * 1024 * 1024 * 1024, //100Gb
            update_map_size: 100 * 1024 * 1024 * 1024, //100Gb
        }
    }
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

pub enum UpdateEvent {
    NewUpdate,
    MustClear,
}

pub type UpdateEvents = Receiver<UpdateEvent>;
pub type UpdateEventsEmitter = Sender<UpdateEvent>;

fn update_awaiter(
    receiver: UpdateEvents,
    env: heed::Env,
    update_env: heed::Env,
    index_uid: &str,
    update_fn: Arc<ArcSwapFn>,
    index: Index,
) -> MResult<()> {
    for event in receiver {

        // if we receive a *MustClear* event, clear the index and break the loop
        if let UpdateEvent::MustClear = event {
            let mut writer = env.typed_write_txn::<MainT>()?;
            let mut update_writer = update_env.typed_write_txn::<UpdateT>()?;

            store::clear(&mut writer, &mut update_writer, &index)?;

            writer.commit()?;
            update_writer.commit()?;

            debug!("store {} cleared", index_uid);

            break
        }

        loop {
            // We instantiate a *write* transaction to *block* the thread
            // until the *other*, notifiying, thread commits
            let result = update_env.typed_write_txn::<UpdateT>();
            let update_reader = break_try!(result, "LMDB read transaction (update) begin failed");

            // retrieve the update that needs to be processed
            let result = index.updates.first_update(&update_reader);
            let (update_id, update) = match break_try!(result, "pop front update failed") {
                Some(value) => value,
                None => {
                    debug!("no more updates");
                    break;
                }
            };

            // do not keep the reader for too long
            break_try!(update_reader.abort(), "aborting update transaction failed");

            // instantiate a transaction to touch to the main env
            let result = env.typed_write_txn::<MainT>();
            let mut main_writer = break_try!(result, "LMDB nested write transaction failed");

            // try to apply the update to the database using the main transaction
            let result = update::update_task(&mut main_writer, &index, update_id, update);
            let status = break_try!(result, "update task failed");

            // commit the main transaction if the update was successful, abort it otherwise
            if status.error.is_none() {
                break_try!(main_writer.commit(), "commit nested transaction failed");
            } else {
                break_try!(main_writer.abort(), "abborting nested transaction failed");
            }

            // now that the update has been processed we can instantiate
            // a transaction to move the result to the updates-results store
            let result = update_env.typed_write_txn::<UpdateT>();
            let mut update_writer = break_try!(result, "LMDB write transaction begin failed");

            // definitely remove the update from the updates store
            index.updates.del_update(&mut update_writer, update_id)?;

            // write the result of the updates-results store
            let updates_results = index.updates_results;
            let result = updates_results.put_update_result(&mut update_writer, update_id, &status);

            // always commit the main transaction, even if the update was unsuccessful
            break_try!(result, "update result store commit failed");
            break_try!(update_writer.commit(), "update transaction commit failed");

            // call the user callback when the update and the result are written consistently
            if let Some(ref callback) = *update_fn.load() {
                (callback)(index_uid, status);
            }
        }
    }

    debug!("update loop system stopped");

    Ok(())
}

/// Ensures Meilisearch version is compatible with the database, returns an error versions mismatch.
/// If create is set to true, a VERSION file is created with the current version.
fn version_guard(path: &Path, create: bool) -> MResult<(u32, u32, u32)> {
    let current_version_major = env!("CARGO_PKG_VERSION_MAJOR");
    let current_version_minor = env!("CARGO_PKG_VERSION_MINOR");
    let current_version_patch = env!("CARGO_PKG_VERSION_PATCH");
    let version_path = path.join("VERSION");

    match File::open(&version_path) {
        Ok(mut file) => {
            let mut version = String::new();
            file.read_to_string(&mut version)?;
            // Matches strings like XX.XX.XX
            let re = Regex::new(r"(\d+).(\d+).(\d+)").unwrap();

            // Make sure there is a result
            let version = re
                .captures_iter(&version)
                .next()
                .ok_or_else(|| Error::VersionMismatch("bad VERSION file".to_string()))?;
            // the first is always the complete match, safe to unwrap because we have a match
            let version_major = version.get(1).unwrap().as_str();
            let version_minor = version.get(2).unwrap().as_str();
            let version_patch = version.get(3).unwrap().as_str();

            if version_major != current_version_major || version_minor != current_version_minor {
                Err(Error::VersionMismatch(format!("{}.{}.XX", version_major, version_minor)))
            } else {
                Ok((
                    version_major.parse().map_err(|e| Error::VersionMismatch(format!("error parsing database version: {}", e)))?,
                    version_minor.parse().map_err(|e| Error::VersionMismatch(format!("error parsing database version: {}", e)))?,
                    version_patch.parse().map_err(|e| Error::VersionMismatch(format!("error parsing database version: {}", e)))?
                ))
            }
        }
        Err(error) => {
            match error.kind() {
                ErrorKind::NotFound => {
                    if create {
                        // when no version file is found, and we've been told to create one,
                        // create a new file with the current version in it.
                        let mut version_file = File::create(&version_path)?;
                        version_file.write_all(format!("{}.{}.{}",
                                current_version_major,
                                current_version_minor,
                                current_version_patch).as_bytes())?;

                        Ok((
                            current_version_major.parse().map_err(|e| Error::VersionMismatch(format!("error parsing database version: {}", e)))?,
                            current_version_minor.parse().map_err(|e| Error::VersionMismatch(format!("error parsing database version: {}", e)))?,
                            current_version_patch.parse().map_err(|e| Error::VersionMismatch(format!("error parsing database version: {}", e)))?
                        ))
                    } else {
                        // when no version file is found and we were not told to create one, this
                        // means that the version is inferior to the one this feature was added in.
                        Err(Error::VersionMismatch("<0.12.0".to_string()))
                    }
                }
                _ => Err(error.into())
            }
        }
    }
}

impl Database {
    pub fn open_or_create(path: impl AsRef<Path>, options: DatabaseOptions) -> MResult<Database> {
        let main_path = path.as_ref().join("main");
        let update_path = path.as_ref().join("update");

        //create db directory
        fs::create_dir_all(&path)?;

        // create file only if main db wasn't created before (first run)
        let database_version = version_guard(path.as_ref(), !main_path.exists() && !update_path.exists())?;

        fs::create_dir_all(&main_path)?;
        let env = heed::EnvOpenOptions::new()
            .map_size(options.main_map_size)
            .max_dbs(3000)
            .open(main_path)?;

        fs::create_dir_all(&update_path)?;
        let update_env = heed::EnvOpenOptions::new()
            .map_size(options.update_map_size)
            .max_dbs(3000)
            .open(update_path)?;

        let common_store = env.create_poly_database(Some("common"))?;
        let indexes_store = env.create_database::<Str, Unit>(Some("indexes"))?;
        let update_fn = Arc::new(ArcSwapFn::empty());

        // list all indexes that needs to be opened
        let mut must_open = Vec::new();
        let reader = env.read_txn()?;
        for result in indexes_store.iter(&reader)? {
            let (index_uid, _) = result?;
            must_open.push(index_uid.to_owned());
        }

        reader.abort()?;

        // open the previously aggregated indexes
        let mut indexes = HashMap::new();
        for index_uid in must_open {
            let (sender, receiver) = crossbeam_channel::unbounded();
            let index = match store::open(&env, &update_env, &index_uid, sender.clone())? {
                Some(index) => index,
                None => {
                    log::warn!(
                        "the index {} doesn't exist or has not all the databases",
                        index_uid
                    );
                    continue;
                }
            };

            let env_clone = env.clone();
            let update_env_clone = update_env.clone();
            let index_clone = index.clone();
            let name_clone = index_uid.clone();
            let update_fn_clone = update_fn.clone();

            let handle = thread::spawn(move || {
                update_awaiter(
                    receiver,
                    env_clone,
                    update_env_clone,
                    &name_clone,
                    update_fn_clone,
                    index_clone,
                )
            });

            // send an update notification to make sure that
            // possible pre-boot updates are consumed
            sender.send(UpdateEvent::NewUpdate).unwrap();

            let result = indexes.insert(index_uid, (index, handle));
            assert!(
                result.is_none(),
                "The index should not have been already open"
            );
        }

        Ok(Database {
            env,
            update_env,
            common_store,
            indexes_store,
            indexes: RwLock::new(indexes),
            update_fn,
            database_version,
        })
    }

    pub fn open_index(&self, name: impl AsRef<str>) -> Option<Index> {
        let indexes_lock = self.indexes.read().unwrap();
        match indexes_lock.get(name.as_ref()) {
            Some((index, ..)) => Some(index.clone()),
            None => None,
        }
    }

    pub fn is_indexing(&self, reader: &UpdateReader, index: &str) -> MResult<Option<bool>> {
        match self.open_index(&index) {
            Some(index) => index.current_update_id(&reader).map(|u| Some(u.is_some())),
            None => Ok(None),
        }
    }

    pub fn create_index(&self, name: impl AsRef<str>) -> MResult<Index> {
        let name = name.as_ref();
        let mut indexes_lock = self.indexes.write().unwrap();

        match indexes_lock.entry(name.to_owned()) {
            Entry::Occupied(_) => Err(crate::Error::IndexAlreadyExists),
            Entry::Vacant(entry) => {
                let (sender, receiver) = crossbeam_channel::unbounded();
                let index = store::create(&self.env, &self.update_env, name, sender)?;

                let mut writer = self.env.typed_write_txn::<MainT>()?;
                self.indexes_store.put(&mut writer, name, &())?;

                index.main.put_name(&mut writer, name)?;
                index.main.put_created_at(&mut writer)?;
                index.main.put_updated_at(&mut writer)?;
                index.main.put_schema(&mut writer, &Schema::default())?;

                let env_clone = self.env.clone();
                let update_env_clone = self.update_env.clone();
                let index_clone = index.clone();
                let name_clone = name.to_owned();
                let update_fn_clone = self.update_fn.clone();

                let handle = thread::spawn(move || {
                    update_awaiter(
                        receiver,
                        env_clone,
                        update_env_clone,
                        &name_clone,
                        update_fn_clone,
                        index_clone,
                    )
                });

                writer.commit()?;
                entry.insert((index.clone(), handle));

                Ok(index)
            }
        }
    }

    pub fn delete_index(&self, name: impl AsRef<str>) -> MResult<bool> {
        let name = name.as_ref();
        let mut indexes_lock = self.indexes.write().unwrap();

        match indexes_lock.remove_entry(name) {
            Some((name, (index, handle))) => {
                // remove the index name from the list of indexes
                // and clear all the LMDB dbi
                let mut writer = self.env.write_txn()?;
                self.indexes_store.delete(&mut writer, &name)?;
                writer.commit()?;

                // send a stop event to the update loop of the index
                index.updates_notifier.send(UpdateEvent::MustClear).unwrap();

                drop(indexes_lock);

                // join the update loop thread to ensure it is stopped
                handle.join().unwrap()?;

                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn set_update_callback(&self, update_fn: BoxUpdateFn) {
        let update_fn = Some(Arc::new(update_fn));
        self.update_fn.swap(update_fn);
    }

    pub fn unset_update_callback(&self) {
        self.update_fn.swap(None);
    }

    pub fn main_read_txn(&self) -> MResult<MainReader> {
        Ok(self.env.typed_read_txn::<MainT>()?)
    }

    pub(crate) fn main_write_txn(&self) -> MResult<MainWriter> {
        Ok(self.env.typed_write_txn::<MainT>()?)
    }

    /// Calls f providing it with a writer to the main database. After f is called, makes sure the
    /// transaction is commited. Returns whatever result f returns.
    pub fn main_write<F, R, E>(&self, f: F) -> Result<R, E>
    where
        F: FnOnce(&mut MainWriter) -> Result<R, E>,
        E: From<Error>,
    {
        let mut writer = self.main_write_txn()?;
        let result = f(&mut writer)?;
        writer.commit().map_err(Error::Heed)?;
        Ok(result)
    }

    /// provides a context with a reader to the main database. experimental.
    pub fn main_read<F, R, E>(&self, f: F) -> Result<R, E>
    where
        F: FnOnce(&MainReader) -> Result<R, E>,
        E: From<Error>,
    {
        let reader = self.main_read_txn()?;
        let result = f(&reader)?;
        reader.abort().map_err(Error::Heed)?;
        Ok(result)
    }

    pub fn update_read_txn(&self) -> MResult<UpdateReader> {
        Ok(self.update_env.typed_read_txn::<UpdateT>()?)
    }

    pub(crate) fn update_write_txn(&self) -> MResult<heed::RwTxn<UpdateT>> {
        Ok(self.update_env.typed_write_txn::<UpdateT>()?)
    }

    /// Calls f providing it with a writer to the main database. After f is called, makes sure the
    /// transaction is commited. Returns whatever result f returns.
    pub fn update_write<F, R, E>(&self, f: F) -> Result<R, E>
    where
        F: FnOnce(&mut UpdateWriter) -> Result<R, E>,
        E: From<Error>,
    {
        let mut writer = self.update_write_txn()?;
        let result = f(&mut writer)?;
        writer.commit().map_err(Error::Heed)?;
        Ok(result)
    }

    /// provides a context with a reader to the update database. experimental.
    pub fn update_read<F, R, E>(&self, f: F) -> Result<R, E>
    where
        F: FnOnce(&UpdateReader) -> Result<R, E>,
        E: From<Error>,
    {
        let reader = self.update_read_txn()?;
        let result = f(&reader)?;
        reader.abort().map_err(Error::Heed)?;
        Ok(result)
    }

    pub fn copy_and_compact_to_path<P: AsRef<Path>>(&self, path: P) -> MResult<(File, File)> {
        let path = path.as_ref();

        let env_path = path.join("main");
        let env_update_path = path.join("update");
        let env_version_path = path.join("VERSION");

        fs::create_dir(&env_path)?;
        fs::create_dir(&env_update_path)?;
    
        // write Database Version
        let (current_version_major, current_version_minor, current_version_patch) = self.database_version;
        let mut version_file = File::create(&env_version_path)?;
        version_file.write_all(format!("{}.{}.{}",
                current_version_major,
                current_version_minor,
                current_version_patch).as_bytes())?;

        let env_path = env_path.join("data.mdb");
        let env_file = self.env.copy_to_path(&env_path, CompactionOption::Enabled)?;

        let env_update_path = env_update_path.join("data.mdb");
        match self.update_env.copy_to_path(env_update_path, CompactionOption::Enabled) {
            Ok(update_env_file) => Ok((env_file, update_env_file)),
            Err(e) => {
                fs::remove_file(env_path)?;
                Err(e.into())
            },
        }
    }

    pub fn indexes_uids(&self) -> Vec<String> {
        let indexes = self.indexes.read().unwrap();
        indexes.keys().cloned().collect()
    }

    pub(crate) fn common_store(&self) -> heed::PolyDatabase {
        self.common_store
    }

    pub fn last_update(&self, reader: &heed::RoTxn<MainT>) -> MResult<Option<DateTime<Utc>>> {
        match self.common_store()
            .get::<_, Str, SerdeDatetime>(reader, LAST_UPDATE_KEY)? {
                Some(datetime) => Ok(Some(datetime)),
                None => Ok(None),
            }
    }

    pub fn set_last_update(&self, writer: &mut heed::RwTxn<MainT>, time: &DateTime<Utc>) -> MResult<()> {
        self.common_store()
            .put::<_, Str, SerdeDatetime>(writer, LAST_UPDATE_KEY, time)?;
        Ok(())
    }

    pub fn compute_stats(&self, writer: &mut MainWriter, index_uid: &str) -> MResult<()> {
        let index = match self.open_index(&index_uid) {
            Some(index) => index,
            None => {
                error!("Impossible to retrieve index {}", index_uid);
                return Ok(());
            }
        };

        let schema = match index.main.schema(&writer)? {
            Some(schema) => schema,
            None => return Ok(()),
        };

        let all_documents_fields = index
            .documents_fields_counts
            .all_documents_fields_counts(&writer)?;

        // count fields frequencies
        let mut fields_frequency = HashMap::<_, usize>::new();
        for result in all_documents_fields {
            let (_, attr, _) = result?;
            if let Some(field_id) = schema.indexed_pos_to_field_id(attr) {
                *fields_frequency.entry(field_id).or_default() += 1;
            }
        }

        // convert attributes to their names
        let frequency: BTreeMap<_, _> = fields_frequency
            .into_iter()
            .filter_map(|(a, c)| schema.name(a).map(|name| (name.to_string(), c)))
            .collect();

        index
            .main
            .put_fields_distribution(writer, &frequency)
    }

    pub fn version(&self) -> (u32, u32, u32) { self.database_version }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::bucket_sort::SortResult;
    use crate::criterion::{self, CriteriaBuilder};
    use crate::update::{ProcessedUpdateResult, UpdateStatus};
    use crate::settings::Settings;
    use crate::{Document, DocumentId};
    use serde::de::IgnoredAny;
    use std::sync::mpsc;

    #[test]
    fn valid_updates() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path(), DatabaseOptions::default()).unwrap();
        let db = &database;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let mut writer = db.main_write_txn().unwrap();
        index.main.put_schema(&mut writer, &Schema::with_primary_key("id")).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed

        let settings = {
            let data = r#"
                {
                    "searchableAttributes": ["name", "description"],
                    "displayedAttributes": ["name", "description"]
                }
            "#;
            let settings: Settings = serde_json::from_str(data).unwrap();
            settings.to_update().unwrap()
        };

        let mut update_writer = db.update_write_txn().unwrap();
        let _update_id = index.settings_update(&mut update_writer, settings).unwrap();
        update_writer.commit().unwrap();

        let mut additions = index.documents_addition();

        let doc1 = serde_json::json!({
            "id": 123,
            "name": "Marvin",
            "description": "My name is Marvin",
        });

        let doc2 = serde_json::json!({
            "id": 234,
            "name": "Kevin",
            "description": "My name is Kevin",
        });

        additions.update_document(doc1);
        additions.update_document(doc2);

        let mut update_writer = db.update_write_txn().unwrap();
        let update_id = additions.finalize(&mut update_writer).unwrap();
        update_writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.into_iter().find(|id| *id == update_id);

        let update_reader = db.update_read_txn().unwrap();
        let result = index.update_status(&update_reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
    }

    #[test]
    fn invalid_updates() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path(), DatabaseOptions::default()).unwrap();
        let db = &database;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let mut writer = db.main_write_txn().unwrap();
        index.main.put_schema(&mut writer, &Schema::with_primary_key("id")).unwrap();
        writer.commit().unwrap();

        let settings = {
            let data = r#"
                {
                    "searchableAttributes": ["name", "description"],
                    "displayedAttributes": ["name", "description"]
                }
            "#;
            let settings: Settings = serde_json::from_str(data).unwrap();
            settings.to_update().unwrap()
        };

        let mut update_writer = db.update_write_txn().unwrap();
        let _update_id = index.settings_update(&mut update_writer, settings).unwrap();
        update_writer.commit().unwrap();

        let mut additions = index.documents_addition();

        let doc1 = serde_json::json!({
            "id": 123,
            "name": "Marvin",
            "description": "My name is Marvin",
        });

        let doc2 = serde_json::json!({
            "name": "Kevin",
            "description": "My name is Kevin",
        });

        additions.update_document(doc1);
        additions.update_document(doc2);

        let mut update_writer = db.update_write_txn().unwrap();
        let update_id = additions.finalize(&mut update_writer).unwrap();
        update_writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.into_iter().find(|id| *id == update_id);

        let update_reader = db.update_read_txn().unwrap();
        let result = index.update_status(&update_reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Failed { content }) if content.error.is_some());
    }

    #[test]
    fn ignored_words_too_long() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path(), DatabaseOptions::default()).unwrap();
        let db = &database;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let mut writer = db.main_write_txn().unwrap();
        index.main.put_schema(&mut writer, &Schema::with_primary_key("id")).unwrap();
        writer.commit().unwrap();

        let settings = {
            let data = r#"
                {
                    "searchableAttributes": ["name"],
                    "displayedAttributes": ["name"]
                }
            "#;
            let settings: Settings = serde_json::from_str(data).unwrap();
            settings.to_update().unwrap()
        };

        let mut update_writer = db.update_write_txn().unwrap();
        let _update_id = index.settings_update(&mut update_writer, settings).unwrap();
        update_writer.commit().unwrap();

        let mut additions = index.documents_addition();

        let doc1 = serde_json::json!({
            "id": 123,
            "name": "s̷̡̢̡̧̺̜̞͕͉͉͕̜͔̟̼̥̝͍̟̖͔͔̪͉̲̹̝̣̖͎̞̤̥͓͎̭̩͕̙̩̿̀̋̅̈́̌́̏̍̄̽͂̆̾̀̿̕̚̚͜͠͠ͅͅļ̵̨̨̨̰̦̻̳̖̳͚̬̫͚̦͖͈̲̫̣̩̥̻̙̦̱̼̠̖̻̼̘̖͉̪̜̠̙͖̙̩͔̖̯̩̲̿̽͋̔̿̍̓͂̍̿͊͆̃͗̔̎͐͌̾̆͗́̆̒̔̾̅̚̚͜͜ͅͅī̵̛̦̅̔̓͂͌̾́͂͛̎̋͐͆̽̂̋̋́̾̀̉̓̏̽́̑̀͒̇͋͛̈́̃̉̏͊̌̄̽̿̏̇͘̕̚̕p̶̧̛̛̖̯̗͕̝̗̭̱͙̖̗̟̟̐͆̊̂͐̋̓̂̈́̓͊̆͌̾̾͐͋͗͌̆̿̅͆̈́̈́̉͋̍͊͗̌̓̅̈̎̇̃̎̈́̉̐̋͑̃͘̕͘d̴̢̨̛͕̘̯͖̭̮̝̝̐̊̈̅̐̀͒̀́̈́̀͌̽͛͆͑̀̽̿͛̃̋̇̎̀́̂́͘͠͝ǫ̵̨̛̮̩̘͚̬̯̖̱͍̼͑͑̓̐́̑̿̈́̔͌̂̄͐͝ģ̶̧̜͇̣̭̺̪̺̖̻͖̮̭̣̙̻͒͊͗̓̓͒̀̀ͅ",
        });

        additions.update_document(doc1);

        let mut update_writer = db.update_write_txn().unwrap();
        let update_id = additions.finalize(&mut update_writer).unwrap();
        update_writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.into_iter().find(|id| *id == update_id);

        let update_reader = db.update_read_txn().unwrap();
        let result = index.update_status(&update_reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
    }

    #[test]
    fn add_schema_attributes_at_end() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path(), DatabaseOptions::default()).unwrap();
        let db = &database;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let mut writer = db.main_write_txn().unwrap();
        index.main.put_schema(&mut writer, &Schema::with_primary_key("id")).unwrap();
        writer.commit().unwrap();

        let settings = {
            let data = r#"
                {
                    "searchableAttributes": ["name", "description"],
                    "displayedAttributes": ["name", "description"]
                }
            "#;
            let settings: Settings = serde_json::from_str(data).unwrap();
            settings.to_update().unwrap()
        };

        let mut update_writer = db.update_write_txn().unwrap();
        let _update_id = index.settings_update(&mut update_writer, settings).unwrap();
        update_writer.commit().unwrap();

        let mut additions = index.documents_addition();

        let doc1 = serde_json::json!({
            "id": 123,
            "name": "Marvin",
            "description": "My name is Marvin",
        });

        let doc2 = serde_json::json!({
            "id": 234,
            "name": "Kevin",
            "description": "My name is Kevin",
        });

        additions.update_document(doc1);
        additions.update_document(doc2);

        let mut update_writer = db.update_write_txn().unwrap();
        let _update_id = additions.finalize(&mut update_writer).unwrap();
        update_writer.commit().unwrap();

        let settings = {
            let data = r#"
                {
                    "searchableAttributes": ["name", "description", "age", "sex"],
                    "displayedAttributes": ["name", "description", "age", "sex"]
                }
            "#;
            let settings: Settings = serde_json::from_str(data).unwrap();
            settings.to_update().unwrap()
        };

        let mut writer = db.update_write_txn().unwrap();
        let update_id = index.settings_update(&mut writer, settings).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.iter().find(|id| *id == update_id);

        // check if it has been accepted
        let update_reader = db.update_read_txn().unwrap();
        let result = index.update_status(&update_reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
        update_reader.abort().unwrap();

        let mut additions = index.documents_addition();

        let doc1 = serde_json::json!({
            "id": 123,
            "name": "Marvin",
            "description": "My name is Marvin",
            "age": 21,
            "sex": "Male",
        });

        let doc2 = serde_json::json!({
            "id": 234,
            "name": "Kevin",
            "description": "My name is Kevin",
            "age": 23,
            "sex": "Male",
        });

        additions.update_document(doc1);
        additions.update_document(doc2);

        let mut writer = db.update_write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.iter().find(|id| *id == update_id);

        // check if it has been accepted
        let update_reader = db.update_read_txn().unwrap();
        let result = index.update_status(&update_reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
        update_reader.abort().unwrap();

        // even try to search for a document
        let reader = db.main_read_txn().unwrap();
        let SortResult {documents, .. } = index.query_builder().query(&reader, Some("21 "), 0..20).unwrap();
        assert_matches!(documents.len(), 1);

        reader.abort().unwrap();

        // try to introduce attributes in the middle of the schema
        let settings = {
            let data = r#"
                {
                    "searchableAttributes": ["name", "description", "city", "age", "sex"],
                    "displayedAttributes": ["name", "description", "city", "age", "sex"]
                }
            "#;
            let settings: Settings = serde_json::from_str(data).unwrap();
            settings.to_update().unwrap()
        };

        let mut writer = db.update_write_txn().unwrap();
        let update_id = index.settings_update(&mut writer, settings).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.iter().find(|id| *id == update_id);
        // check if it has been accepted
        let update_reader = db.update_read_txn().unwrap();
        let result = index.update_status(&update_reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
    }

    #[test]
    fn deserialize_documents() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path(), DatabaseOptions::default()).unwrap();
        let db = &database;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let mut writer = db.main_write_txn().unwrap();
        index.main.put_schema(&mut writer, &Schema::with_primary_key("id")).unwrap();
        writer.commit().unwrap();

        let settings = {
            let data = r#"
                {
                    "searchableAttributes": ["name", "description"],
                    "displayedAttributes": ["name", "description"]
                }
            "#;
            let settings: Settings = serde_json::from_str(data).unwrap();
            settings.to_update().unwrap()
        };

        let mut writer = db.update_write_txn().unwrap();
        let _update_id = index.settings_update(&mut writer, settings).unwrap();
        writer.commit().unwrap();

        let mut additions = index.documents_addition();

        // DocumentId(7900334843754999545)
        let doc1 = serde_json::json!({
            "id": 123,
            "name": "Marvin",
            "description": "My name is Marvin",
        });

        // DocumentId(8367468610878465872)
        let doc2 = serde_json::json!({
            "id": 234,
            "name": "Kevin",
            "description": "My name is Kevin",
        });

        additions.update_document(doc1);
        additions.update_document(doc2);

        let mut writer = db.update_write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.into_iter().find(|id| *id == update_id);

        let update_reader = db.update_read_txn().unwrap();
        let result = index.update_status(&update_reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
        update_reader.abort().unwrap();

        let reader = db.main_read_txn().unwrap();
        let document: Option<IgnoredAny> = index.document(&reader, None, DocumentId(25)).unwrap();
        assert!(document.is_none());

        let document: Option<IgnoredAny> = index
            .document(&reader, None, DocumentId(0))
            .unwrap();
        assert!(document.is_some());

        let document: Option<IgnoredAny> = index
            .document(&reader, None, DocumentId(1))
            .unwrap();
        assert!(document.is_some());
    }

    #[test]
    fn partial_document_update() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path(), DatabaseOptions::default()).unwrap();
        let db = &database;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let mut writer = db.main_write_txn().unwrap();
        index.main.put_schema(&mut writer, &Schema::with_primary_key("id")).unwrap();
        writer.commit().unwrap();

        let settings = {
            let data = r#"
                {
                    "searchableAttributes": ["name", "description"],
                    "displayedAttributes": ["name", "description", "id"]
                }
            "#;
            let settings: Settings = serde_json::from_str(data).unwrap();
            settings.to_update().unwrap()
        };

        let mut writer = db.update_write_txn().unwrap();
        let _update_id = index.settings_update(&mut writer, settings).unwrap();
        writer.commit().unwrap();

        let mut additions = index.documents_addition();

        // DocumentId(7900334843754999545)
        let doc1 = serde_json::json!({
            "id": 123,
            "name": "Marvin",
            "description": "My name is Marvin",
        });

        // DocumentId(8367468610878465872)
        let doc2 = serde_json::json!({
            "id": 234,
            "name": "Kevin",
            "description": "My name is Kevin",
        });

        additions.update_document(doc1);
        additions.update_document(doc2);

        let mut writer = db.update_write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.iter().find(|id| *id == update_id);

        let update_reader = db.update_read_txn().unwrap();
        let result = index.update_status(&update_reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
        update_reader.abort().unwrap();

        let reader = db.main_read_txn().unwrap();
        let document: Option<IgnoredAny> = index.document(&reader, None, DocumentId(25)).unwrap();
        assert!(document.is_none());

        let document: Option<IgnoredAny> = index
            .document(&reader, None, DocumentId(0))
            .unwrap();
        assert!(document.is_some());

        let document: Option<IgnoredAny> = index
            .document(&reader, None, DocumentId(1))
            .unwrap();
        assert!(document.is_some());

        reader.abort().unwrap();

        let mut partial_additions = index.documents_partial_addition();

        // DocumentId(7900334843754999545)
        let partial_doc1 = serde_json::json!({
            "id": 123,
            "description": "I am the new Marvin",
        });

        // DocumentId(8367468610878465872)
        let partial_doc2 = serde_json::json!({
            "id": 234,
            "description": "I am the new Kevin",
        });

        partial_additions.update_document(partial_doc1);
        partial_additions.update_document(partial_doc2);

        let mut writer = db.update_write_txn().unwrap();
        let update_id = partial_additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.iter().find(|id| *id == update_id);

        let update_reader = db.update_read_txn().unwrap();
        let result = index.update_status(&update_reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
        update_reader.abort().unwrap();

        let reader = db.main_read_txn().unwrap();
        let document: Option<serde_json::Value> = index
            .document(&reader, None, DocumentId(0))
            .unwrap();

        let new_doc1 = serde_json::json!({
            "id": 123,
            "name": "Marvin",
            "description": "I am the new Marvin",
        });
        assert_eq!(document, Some(new_doc1));

        let document: Option<serde_json::Value> = index
            .document(&reader, None, DocumentId(1))
            .unwrap();

        let new_doc2 = serde_json::json!({
            "id": 234,
            "name": "Kevin",
            "description": "I am the new Kevin",
        });
        assert_eq!(document, Some(new_doc2));
    }

    #[test]
    fn delete_index() {
        let dir = tempfile::tempdir().unwrap();

        let database = Arc::new(Database::open_or_create(dir.path(), DatabaseOptions::default()).unwrap());
        let db = &database;

        let (sender, receiver) = mpsc::sync_channel(100);
        let db_cloned = database.clone();
        let update_fn = move |name: &str, update: ProcessedUpdateResult| {
            // try to open index to trigger a lock
            let _ = db_cloned.open_index(name);
            sender.send(update.update_id).unwrap()
        };

        // create the index
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let mut writer = db.main_write_txn().unwrap();
        index.main.put_schema(&mut writer, &Schema::with_primary_key("id")).unwrap();
        writer.commit().unwrap();

        let settings = {
            let data = r#"
                {
                    "searchableAttributes": ["name", "description"],
                    "displayedAttributes": ["name", "description"]
                }
            "#;
            let settings: Settings = serde_json::from_str(data).unwrap();
            settings.to_update().unwrap()
        };

        let mut writer = db.update_write_txn().unwrap();
        let _update_id = index.settings_update(&mut writer, settings).unwrap();
        writer.commit().unwrap();

        // add documents to the index
        let mut additions = index.documents_addition();

        let doc1 = serde_json::json!({
            "id": 123,
            "name": "Marvin",
            "description": "My name is Marvin",
        });

        let doc2 = serde_json::json!({
            "id": 234,
            "name": "Kevin",
            "description": "My name is Kevin",
        });

        additions.update_document(doc1);
        additions.update_document(doc2);

        let mut writer = db.update_write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // delete the index
        let deleted = database.delete_index("test").unwrap();
        assert!(deleted);

        // block until the transaction is processed
        let _ = receiver.into_iter().find(|id| *id == update_id);

        let result = database.open_index("test");
        assert!(result.is_none());
    }

    #[test]
    fn check_number_ordering() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path(), DatabaseOptions::default()).unwrap();
        let db = &database;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let mut writer = db.main_write_txn().unwrap();
        index.main.put_schema(&mut writer, &Schema::with_primary_key("id")).unwrap();
        writer.commit().unwrap();

        let settings = {
            let data = r#"
                {
                    "rankingRules": [
                        "typo",
                        "words",
                        "proximity",
                        "attribute",
                        "wordsPosition",
                        "exactness",
                        "desc(release_date)"
                    ],
                    "searchableAttributes": ["name", "release_date"],
                    "displayedAttributes": ["name", "release_date"]
                }
            "#;
            let settings: Settings = serde_json::from_str(data).unwrap();
            settings.to_update().unwrap()
        };

        let mut writer = db.update_write_txn().unwrap();
        let _update_id = index.settings_update(&mut writer, settings).unwrap();
        writer.commit().unwrap();

        let mut additions = index.documents_addition();

        // DocumentId(7900334843754999545)
        let doc1 = serde_json::json!({
            "id": 123,
            "name": "Kevin the first",
            "release_date": -10000,
        });

        // DocumentId(8367468610878465872)
        let doc2 = serde_json::json!({
            "id": 234,
            "name": "Kevin the second",
            "release_date": 10000,
        });

        additions.update_document(doc1);
        additions.update_document(doc2);

        let mut writer = db.update_write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.into_iter().find(|id| *id == update_id);

        let reader = db.main_read_txn().unwrap();
        let schema = index.main.schema(&reader).unwrap().unwrap();
        let ranked_map = index.main.ranked_map(&reader).unwrap().unwrap();

        let criteria = CriteriaBuilder::new()
            .add(
                criterion::SortByAttr::lower_is_better(&ranked_map, &schema, "release_date")
                    .unwrap(),
            )
            .add(criterion::DocumentId)
            .build();

        let builder = index.query_builder_with_criteria(criteria);

        let SortResult {documents, .. } = builder.query(&reader, Some("Kevin"), 0..20).unwrap();
        let mut iter = documents.into_iter();

        assert_matches!(
            iter.next(),
            Some(Document {
                id: DocumentId(0),
                ..
            })
        );
        assert_matches!(
            iter.next(),
            Some(Document {
                id: DocumentId(1),
                ..
            })
        );
        assert_matches!(iter.next(), None);
    }
}
