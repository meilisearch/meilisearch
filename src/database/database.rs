use std::sync::{Arc, Mutex};
use std::error::Error;
use std::path::Path;

use rocksdb::rocksdb_options::{DBOptions, IngestExternalFileOptions, ColumnFamilyOptions};
use rocksdb::rocksdb::{Writable, Snapshot};
use rocksdb::{DB, DBVector, MergeOperands};
use crossbeam::atomic::ArcCell;

use crate::database::index::{self, Index, Positive};
use crate::database::{DatabaseView, Update, Schema};
use crate::database::{DATA_INDEX, DATA_SCHEMA};

pub struct Database {
    // DB is under a Mutex to sync update ingestions and separate DB update locking
    // and DatabaseView acquiring locking in other words:
    // "Block readers the minimum possible amount of time"
    db: Mutex<Arc<DB>>,

    // This view is updated each time the DB ingests an update
    view: ArcCell<DatabaseView<Arc<DB>>>,
}

impl Database {
    pub fn create<P: AsRef<Path>>(path: P, schema: Schema) -> Result<Database, Box<Error>> {
        let path = path.as_ref();
        if path.exists() {
            return Err(format!("File already exists at path: {}, cannot create database.",
                                path.display()).into())
        }

        let path = path.to_string_lossy();
        let mut opts = DBOptions::new();
        opts.create_if_missing(true);
        // opts.error_if_exists(true); // FIXME pull request that

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data-index merge operator", merge_indexes);

        let db = DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        let mut schema_bytes = Vec::new();
        schema.write_to_bin(&mut schema_bytes)?;
        db.put(DATA_SCHEMA, &schema_bytes)?;

        let db = Arc::new(db);
        let snapshot = Snapshot::new(db.clone());
        let view = ArcCell::new(Arc::new(DatabaseView::new(snapshot)?));

        Ok(Database { db: Mutex::new(db), view })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Database, Box<Error>> {
        let path = path.as_ref().to_string_lossy();

        let mut opts = DBOptions::new();
        opts.create_if_missing(false);

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data-index merge operator", merge_indexes);

        let db = DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        // FIXME create a generic function to do that !
        let _schema = match db.get(DATA_SCHEMA)? {
            Some(value) => Schema::read_from_bin(&*value)?,
            None => return Err(String::from("Database does not contain a schema").into()),
        };

        let db = Arc::new(db);
        let snapshot = Snapshot::new(db.clone());
        let view = ArcCell::new(Arc::new(DatabaseView::new(snapshot)?));

        Ok(Database { db: Mutex::new(db), view })
    }

    pub fn ingest_update_file(&self, update: Update) -> Result<Arc<DatabaseView<Arc<DB>>>, Box<Error>> {
        let snapshot = {
            // We must have a mutex here to ensure that update ingestions and compactions
            // are done atomatically and in the right order.
            // This way update ingestions will block other update ingestions without blocking view
            // creations while doing the "data-index" compaction
            let db = match self.db.lock() {
                Ok(db) => db,
                Err(e) => return Err(e.to_string().into()),
            };

            let path = update.path().to_string_lossy();
            let mut options = IngestExternalFileOptions::new();
            // options.move_files(move_update);

            let cf_handle = db.cf_handle("default").expect("\"default\" column family not found");
            db.ingest_external_file_optimized(&cf_handle, &options, &[&path])?;

            // Compacting to trigger the merge operator only one time
            // while ingesting the update and not each time searching
            db.compact_range(Some(DATA_INDEX), Some(DATA_INDEX));

            Snapshot::new(db.clone())
        };

        let view = Arc::new(DatabaseView::new(snapshot)?);
        self.view.set(view.clone());

        Ok(view)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Box<Error>> {
        self.view().get(key)
    }

    pub fn flush(&self) -> Result<(), Box<Error>> {
        match self.db.lock() {
            Ok(db) => Ok(db.flush(true)?),
            Err(e) => Err(e.to_string().into()),
        }
    }

    pub fn view(&self) -> Arc<DatabaseView<Arc<DB>>> {
        self.view.get()
    }
}

fn merge_indexes(key: &[u8], existing: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    assert_eq!(key, DATA_INDEX, "The merge operator only supports \"data-index\" merging");

    let mut index: Option<Index> = None;

    for bytes in existing.into_iter().chain(operands) {
        let bytes_len = bytes.len();
        let bytes = Arc::new(bytes.to_vec());
        let operand = Index::from_shared_bytes(bytes, 0, bytes_len);
        let operand = operand.expect("BUG: could not deserialize index");

        let merged = match index {
            Some(ref index) => index.merge(&operand).expect("BUG: could not merge index"),
            None            => operand,
        };

        index.replace(merged);
    }

    let index = index.unwrap_or_default();
    let mut bytes = Vec::new();
    index.write_to_bytes(&mut bytes);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    use serde_derive::{Serialize, Deserialize};
    use tempfile::tempdir;

    use crate::database::schema::{SchemaBuilder, STORED, INDEXED};
    use crate::database::update::UpdateBuilder;
    use crate::tokenizer::DefaultBuilder;

    #[test]
    fn ingest_update_file() -> Result<(), Box<Error>> {
        let dir = tempdir()?;

        let rocksdb_path = dir.path().join("rocksdb.rdb");

        #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
        struct SimpleDoc {
            id: u64,
            title: String,
            description: String,
            timestamp: u64,
        }

        let schema = {
            let mut builder = SchemaBuilder::with_identifier("id");
            builder.new_attribute("id", STORED);
            builder.new_attribute("title", STORED | INDEXED);
            builder.new_attribute("description", STORED | INDEXED);
            builder.new_attribute("timestamp", STORED);
            builder.build()
        };

        let database = Database::create(&rocksdb_path, schema.clone())?;
        let tokenizer_builder = DefaultBuilder::new();

        let update_path = dir.path().join("update.sst");

        let doc0 = SimpleDoc {
            id: 0,
            title: String::from("I am a title"),
            description: String::from("I am a description"),
            timestamp: 1234567,
        };
        let doc1 = SimpleDoc {
            id: 1,
            title: String::from("I am the second title"),
            description: String::from("I am the second description"),
            timestamp: 7654321,
        };

        let docid0;
        let docid1;
        let mut update = {
            let mut builder = UpdateBuilder::new(update_path, schema);

            docid0 = builder.update_document(&doc0).unwrap();
            docid1 = builder.update_document(&doc1).unwrap();

            builder.build()?
        };

        database.ingest_update_file(update)?;
        let view = database.view();

        let de_doc0: SimpleDoc = view.document_by_id(docid0)?;
        let de_doc1: SimpleDoc = view.document_by_id(docid1)?;

        assert_eq!(doc0, de_doc0);
        assert_eq!(doc1, de_doc1);

        Ok(dir.close()?)
    }
}
