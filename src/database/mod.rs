use std::error::Error;
use std::path::Path;

use rocksdb::rocksdb_options::{DBOptions, IngestExternalFileOptions, ColumnFamilyOptions};
use rocksdb::{DB, MergeOperands};
use rocksdb::rocksdb::Writable;

pub use crate::database::database_view::DatabaseView;
use crate::index::update::Update;
use crate::index::schema::Schema;
use crate::blob::{self, Blob};

mod document_key;
mod database_view;
mod deserializer;

const DATA_INDEX:  &[u8] = b"data-index";
const DATA_SCHEMA: &[u8] = b"data-schema";

pub struct Database(DB);

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

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data-index merge operator", merge_indexes);

        let db = DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        let mut schema_bytes = Vec::new();
        schema.write_to(&mut schema_bytes)?;
        db.put(DATA_SCHEMA, &schema_bytes)?;

        Ok(Database(db))
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
            Some(value) => Schema::read_from(&*value)?,
            None => return Err(String::from("Database does not contain a schema").into()),
        };

        Ok(Database(db))
    }

    pub fn ingest_update_file(&self, update: Update) -> Result<(), Box<Error>> {
        let move_update = update.can_be_moved();
        let path = update.into_path_buf();
        let path = path.to_string_lossy();

        let mut options = IngestExternalFileOptions::new();
        options.move_files(move_update);

        let cf_handle = self.0.cf_handle("default").unwrap();
        self.0.ingest_external_file_optimized(&cf_handle, &options, &[&path])?;

        // compacting to avoid calling the merge operator
        self.0.compact_range(Some(DATA_INDEX), Some(DATA_INDEX));

        Ok(())
    }

    pub fn view(&self) -> Result<DatabaseView, Box<Error>> {
        let snapshot = self.0.snapshot();
        DatabaseView::new(snapshot)
    }
}

fn merge_indexes(key: &[u8], existing_value: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    if key != DATA_INDEX { panic!("The merge operator only supports \"data-index\" merging") }

    let capacity = {
        let remaining = operands.size_hint().0;
        let already_exist = usize::from(existing_value.is_some());
        remaining + already_exist
    };

    let mut op = blob::OpBuilder::with_capacity(capacity);
    if let Some(existing_value) = existing_value {
        let blob = bincode::deserialize(existing_value).expect("BUG: could not deserialize data-index");
        op.push(Blob::Positive(blob));
    }

    for bytes in operands {
        let blob = bincode::deserialize(bytes).expect("BUG: could not deserialize blob");
        op.push(blob);
    }

    let blob = op.merge().expect("BUG: could not merge blobs");
    bincode::serialize(&blob).expect("BUG: could not serialize merged blob")
}
