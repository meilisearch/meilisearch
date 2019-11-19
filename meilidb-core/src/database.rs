use std::collections::hash_map::{Entry, HashMap};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::{fs, thread};

use crossbeam_channel::{Receiver, Sender};
use heed::types::{Str, Unit};
use heed::{CompactionOption, Result as ZResult};
use log::debug;

use crate::{store, update, Index, MResult};

pub type BoxUpdateFn = Box<dyn Fn(&str, update::ProcessedUpdateResult) + Send + Sync + 'static>;
type ArcSwapFn = arc_swap::ArcSwapOption<BoxUpdateFn>;

pub struct Database {
    pub env: heed::Env,
    common_store: heed::PolyDatabase,
    indexes_store: heed::Database<Str, Unit>,
    indexes: RwLock<HashMap<String, (Index, thread::JoinHandle<()>)>>,
    update_fn: Arc<ArcSwapFn>,
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
    MustStop,
}

pub type UpdateEvents = Receiver<UpdateEvent>;
pub type UpdateEventsEmitter = Sender<UpdateEvent>;

fn update_awaiter(
    receiver: UpdateEvents,
    env: heed::Env,
    index_uid: &str,
    update_fn: Arc<ArcSwapFn>,
    index: Index,
) {
    let mut receiver = receiver.into_iter();
    while let Some(UpdateEvent::NewUpdate) = receiver.next() {
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
            if status.error.is_none() {
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
                (callback)(index_uid, status);
            }
        }
    }

    debug!("update loop system stopped");
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
        let update_fn = Arc::new(ArcSwapFn::empty());

        // list all indexes that needs to be opened
        let mut must_open = Vec::new();
        let reader = env.read_txn()?;
        for result in indexes_store.iter(&reader)? {
            let (index_uid, _) = result?;
            must_open.push(index_uid.to_owned());
        }

        reader.abort();

        // open the previously aggregated indexes
        let mut indexes = HashMap::new();
        for index_uid in must_open {
            let (sender, receiver) = crossbeam_channel::bounded(100);
            let index = match store::open(&env, &index_uid, sender.clone())? {
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
            let index_clone = index.clone();
            let name_clone = index_uid.clone();
            let update_fn_clone = update_fn.clone();

            let handle = thread::spawn(move || {
                update_awaiter(
                    receiver,
                    env_clone,
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
            common_store,
            indexes_store,
            indexes: RwLock::new(indexes),
            update_fn,
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
                let name_clone = name.to_owned();
                let update_fn_clone = self.update_fn.clone();

                let handle = thread::spawn(move || {
                    update_awaiter(
                        receiver,
                        env_clone,
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
                store::clear(&mut writer, &index)?;
                writer.commit()?;

                // join the update loop thread to ensure it is stopped
                handle.join().unwrap();

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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::update::{ProcessedUpdateResult, UpdateStatus};
    use crate::DocumentId;
    use serde::de::IgnoredAny;
    use std::sync::mpsc;

    #[test]
    fn valid_updates() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path()).unwrap();
        let env = &database.env;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let schema = {
            let data = r#"
                identifier = "id"

                [attributes."name"]
                displayed = true
                indexed = true

                [attributes."description"]
                displayed = true
                indexed = true
            "#;
            toml::from_str(data).unwrap()
        };

        let mut writer = env.write_txn().unwrap();
        let _update_id = index.schema_update(&mut writer, schema).unwrap();
        writer.commit().unwrap();

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

        let mut writer = env.write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.into_iter().find(|id| *id == update_id);

        let reader = env.read_txn().unwrap();
        let result = index.update_status(&reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
    }

    #[test]
    fn invalid_updates() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path()).unwrap();
        let env = &database.env;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let schema = {
            let data = r#"
                identifier = "id"

                [attributes."name"]
                displayed = true
                indexed = true

                [attributes."description"]
                displayed = true
                indexed = true
            "#;
            toml::from_str(data).unwrap()
        };

        let mut writer = env.write_txn().unwrap();
        let _update_id = index.schema_update(&mut writer, schema).unwrap();
        writer.commit().unwrap();

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

        let mut writer = env.write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.into_iter().find(|id| *id == update_id);

        let reader = env.read_txn().unwrap();
        let result = index.update_status(&reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_some());
    }

    #[test]
    fn ignored_words_too_long() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path()).unwrap();
        let env = &database.env;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let schema = {
            let data = r#"
                identifier = "id"

                [attributes."name"]
                displayed = true
                indexed = true
            "#;
            toml::from_str(data).unwrap()
        };

        let mut writer = env.write_txn().unwrap();
        let _update_id = index.schema_update(&mut writer, schema).unwrap();
        writer.commit().unwrap();

        let mut additions = index.documents_addition();

        let doc1 = serde_json::json!({
            "id": 123,
            "name": "s̷̡̢̡̧̺̜̞͕͉͉͕̜͔̟̼̥̝͍̟̖͔͔̪͉̲̹̝̣̖͎̞̤̥͓͎̭̩͕̙̩̿̀̋̅̈́̌́̏̍̄̽͂̆̾̀̿̕̚̚͜͠͠ͅͅļ̵̨̨̨̰̦̻̳̖̳͚̬̫͚̦͖͈̲̫̣̩̥̻̙̦̱̼̠̖̻̼̘̖͉̪̜̠̙͖̙̩͔̖̯̩̲̿̽͋̔̿̍̓͂̍̿͊͆̃͗̔̎͐͌̾̆͗́̆̒̔̾̅̚̚͜͜ͅͅī̵̛̦̅̔̓͂͌̾́͂͛̎̋͐͆̽̂̋̋́̾̀̉̓̏̽́̑̀͒̇͋͛̈́̃̉̏͊̌̄̽̿̏̇͘̕̚̕p̶̧̛̛̖̯̗͕̝̗̭̱͙̖̗̟̟̐͆̊̂͐̋̓̂̈́̓͊̆͌̾̾͐͋͗͌̆̿̅͆̈́̈́̉͋̍͊͗̌̓̅̈̎̇̃̎̈́̉̐̋͑̃͘̕͘d̴̢̨̛͕̘̯͖̭̮̝̝̐̊̈̅̐̀͒̀́̈́̀͌̽͛͆͑̀̽̿͛̃̋̇̎̀́̂́͘͠͝ǫ̵̨̛̮̩̘͚̬̯̖̱͍̼͑͑̓̐́̑̿̈́̔͌̂̄͐͝ģ̶̧̜͇̣̭̺̪̺̖̻͖̮̭̣̙̻͒͊͗̓̓͒̀̀ͅ",
        });

        additions.update_document(doc1);

        let mut writer = env.write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.into_iter().find(|id| *id == update_id);

        let reader = env.read_txn().unwrap();
        let result = index.update_status(&reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
    }

    #[test]
    fn add_schema_attributes_at_end() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path()).unwrap();
        let env = &database.env;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let schema = {
            let data = r#"
                identifier = "id"

                [attributes."name"]
                displayed = true
                indexed = true

                [attributes."description"]
                displayed = true
                indexed = true
            "#;
            toml::from_str(data).unwrap()
        };

        let mut writer = env.write_txn().unwrap();
        let _update_id = index.schema_update(&mut writer, schema).unwrap();
        writer.commit().unwrap();

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

        let mut writer = env.write_txn().unwrap();
        let _update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        let schema = {
            let data = r#"
                identifier = "id"

                [attributes."name"]
                displayed = true
                indexed = true

                [attributes."description"]
                displayed = true
                indexed = true

                [attributes."age"]
                displayed = true
                indexed = true

                [attributes."sex"]
                displayed = true
                indexed = true
            "#;
            toml::from_str(data).unwrap()
        };

        let mut writer = env.write_txn().unwrap();
        let update_id = index.schema_update(&mut writer, schema).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.iter().find(|id| *id == update_id);

        // check if it has been accepted
        let reader = env.read_txn().unwrap();
        let result = index.update_status(&reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());
        reader.abort();

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

        let mut writer = env.write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.iter().find(|id| *id == update_id);

        // check if it has been accepted
        let reader = env.read_txn().unwrap();
        let result = index.update_status(&reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());

        // even try to search for a document
        let results = index.query_builder().query(&reader, "21 ", 0..20).unwrap();
        assert_matches!(results.len(), 1);

        reader.abort();

        // try to introduce attributes in the middle of the schema
        let schema = {
            let data = r#"
                identifier = "id"

                [attributes."name"]
                displayed = true
                indexed = true

                [attributes."description"]
                displayed = true
                indexed = true

                [attributes."city"]
                displayed = true
                indexed = true

                [attributes."age"]
                displayed = true
                indexed = true

                [attributes."sex"]
                displayed = true
                indexed = true
            "#;
            toml::from_str(data).unwrap()
        };

        let mut writer = env.write_txn().unwrap();
        let update_id = index.schema_update(&mut writer, schema).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.iter().find(|id| *id == update_id);

        // check if it has been accepted
        let reader = env.read_txn().unwrap();
        let result = index.update_status(&reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_some());
    }

    #[test]
    fn deserialize_documents() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path()).unwrap();
        let env = &database.env;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let schema = {
            let data = r#"
                identifier = "id"

                [attributes."name"]
                displayed = true
                indexed = true

                [attributes."description"]
                displayed = true
                indexed = true
            "#;
            toml::from_str(data).unwrap()
        };

        let mut writer = env.write_txn().unwrap();
        let _update_id = index.schema_update(&mut writer, schema).unwrap();
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

        let mut writer = env.write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.into_iter().find(|id| *id == update_id);

        let reader = env.read_txn().unwrap();
        let result = index.update_status(&reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());

        let document: Option<IgnoredAny> = index.document(&reader, None, DocumentId(25)).unwrap();
        assert!(document.is_none());

        let document: Option<IgnoredAny> = index
            .document(&reader, None, DocumentId(7900334843754999545))
            .unwrap();
        assert!(document.is_some());

        let document: Option<IgnoredAny> = index
            .document(&reader, None, DocumentId(8367468610878465872))
            .unwrap();
        assert!(document.is_some());
    }

    #[test]
    fn partial_document_update() {
        let dir = tempfile::tempdir().unwrap();

        let database = Database::open_or_create(dir.path()).unwrap();
        let env = &database.env;

        let (sender, receiver) = mpsc::sync_channel(100);
        let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
            sender.send(update.update_id).unwrap()
        };
        let index = database.create_index("test").unwrap();

        database.set_update_callback(Box::new(update_fn));

        let schema = {
            let data = r#"
                identifier = "id"

                [attributes."id"]
                displayed = true

                [attributes."name"]
                displayed = true
                indexed = true

                [attributes."description"]
                displayed = true
                indexed = true
            "#;
            toml::from_str(data).unwrap()
        };

        let mut writer = env.write_txn().unwrap();
        let _update_id = index.schema_update(&mut writer, schema).unwrap();
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

        let mut writer = env.write_txn().unwrap();
        let update_id = additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.iter().find(|id| *id == update_id);

        let reader = env.read_txn().unwrap();
        let result = index.update_status(&reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());

        let document: Option<IgnoredAny> = index.document(&reader, None, DocumentId(25)).unwrap();
        assert!(document.is_none());

        let document: Option<IgnoredAny> = index
            .document(&reader, None, DocumentId(7900334843754999545))
            .unwrap();
        assert!(document.is_some());

        let document: Option<IgnoredAny> = index
            .document(&reader, None, DocumentId(8367468610878465872))
            .unwrap();
        assert!(document.is_some());

        reader.abort();

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

        let mut writer = env.write_txn().unwrap();
        let update_id = partial_additions.finalize(&mut writer).unwrap();
        writer.commit().unwrap();

        // block until the transaction is processed
        let _ = receiver.iter().find(|id| *id == update_id);

        let reader = env.read_txn().unwrap();
        let result = index.update_status(&reader, update_id).unwrap();
        assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());

        let document: Option<serde_json::Value> = index
            .document(&reader, None, DocumentId(7900334843754999545))
            .unwrap();

        let new_doc1 = serde_json::json!({
            "id": 123,
            "name": "Marvin",
            "description": "I am the new Marvin",
        });
        assert_eq!(document, Some(new_doc1));

        let document: Option<serde_json::Value> = index
            .document(&reader, None, DocumentId(8367468610878465872))
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

        let database = Database::open_or_create(dir.path()).unwrap();
        let _index = database.create_index("test").unwrap();

        let deleted = database.delete_index("test").unwrap();
        assert!(deleted);

        let result = database.open_index("test");
        assert!(result.is_none());
    }
}
